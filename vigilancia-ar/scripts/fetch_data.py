"""Download the raw Dengue/Zika and RENAPER files and (optionally) sync them to the
Unity Catalog Volume that `read_files()` reads from.

    uv run --group ingest python scripts/fetch_data.py                 # download only
    uv run --group ingest python scripts/fetch_data.py --upload /Volumes/workspace/bronze/raw_data

Layout written under --out (mirrors the Volume):
    dengue_zika/legacy/    2018-2022 files; .xls/.xlsx also get a `.converted.csv`
    dengue_zika/current/   2023+ files (`-se-1-` in the name)
    renaper_population/    department-level cuts

Only files whose name or size differ from what is already in the Volume are uploaded,
so a run with nothing new is a no-op. When running in GitHub Actions the number of
uploaded files is exposed as the `uploaded` step output.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
# datos.salud.gob.ar serves the wrong intermediate certificate, so default verification
# fails. The right one (Sectigo DV R36, public) is shipped here and added to the system
# trust store: verification stays on.
SALUD_CA = HERE / "certs" / "sectigo_dv_r36.pem"
SALUD_PACKAGE = "https://datos.salud.gob.ar/api/3/action/package_show?id=vigilancia-de-dengue-y-zika"
# The federated catalog lists the RENAPER files; the files themselves are on RENAPER's host.
RENAPER_PACKAGE = "https://datos.gob.ar/api/3/action/package_show?id=estructura-de-poblacion-argentina"
RENAPER_FILE = re.compile(r"poblacion_identificada_departamento_[a-z]+_\d{4}\.csv$")


def _context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(cafile=SALUD_CA)
    return ctx


def _get(url: str, retries: int = 3) -> bytes:
    url = re.sub(r"^http://", "https://", url)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "vigilancia-ar-fetch/1.0"})
            with urllib.request.urlopen(req, context=_context(), timeout=120) as r:
                return r.read()
        except OSError:
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
    raise AssertionError("unreachable")


def _resources(package_url: str) -> list[str]:
    return [r["url"] for r in json.loads(_get(package_url))["result"]["resources"]]


def _name(url: str) -> str:
    return urllib.parse.unquote(url.rsplit("/", 1)[-1])


def _to_csv(path: Path) -> Path:
    """Excel -> UTF-8 comma CSV, every cell as text (keeps zero-padded INDEC codes)."""
    import pandas as pd  # ingest group only

    out = path.with_suffix(".converted.csv")
    pd.read_excel(path, dtype=str).to_csv(out, index=False)
    return out


def download(out: Path) -> list[Path]:
    targets: list[tuple[Path, str]] = []
    for url in _resources(SALUD_PACKAGE):
        name = _name(url)
        if not name.lower().endswith((".csv", ".xls", ".xlsx")):
            continue
        era = "current" if "-se-1-" in name else "legacy"
        targets.append((out / "dengue_zika" / era / name, url))
    for url in _resources(RENAPER_PACKAGE):
        if RENAPER_FILE.search(url):
            targets.append((out / "renaper_population" / _name(url), url))

    files: list[Path] = []
    for dest, url in targets:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(_get(url))
        print(f"downloaded {dest.relative_to(out)} ({dest.stat().st_size:,} bytes)")
        files.append(dest)
        if dest.suffix.lower() in (".xls", ".xlsx"):
            files.append(_to_csv(dest))
    # staging only reads CSVs; the Excel originals stay local
    return [f for f in files if f.suffix.lower() == ".csv"]


def upload(files: list[Path], out: Path, volume: str) -> int:
    """Copy to the Volume the files that are new or whose size changed."""
    if not shutil.which("databricks"):
        sys.exit("databricks CLI not found")
    remote_sizes: dict[str, int] = {}
    for sub in {f.parent.relative_to(out) for f in files}:
        res = subprocess.run(
            ["databricks", "fs", "ls", f"dbfs:{volume}/{sub}", "--output", "json"],
            capture_output=True, text=True,
        )
        if res.returncode == 0 and res.stdout.strip():
            for e in json.loads(res.stdout):
                remote_sizes[f"{sub}/{e['name']}"] = e.get("size", -1)

    sent = 0
    for f in files:
        rel = f.relative_to(out).as_posix()
        if remote_sizes.get(rel) == f.stat().st_size:
            continue
        subprocess.run(
            ["databricks", "fs", "cp", str(f), f"dbfs:{volume}/{rel}", "--overwrite"],
            check=True,
        )
        print(f"uploaded {rel}")
        sent += 1
    return sent


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--out", type=Path, default=HERE.parent / "data" / "raw")
    ap.add_argument("--upload", metavar="VOLUME_PATH", help="e.g. /Volumes/workspace/bronze/raw_data")
    args = ap.parse_args()

    files = download(args.out)
    sent = upload(files, args.out, args.upload.rstrip("/")) if args.upload else 0
    print(f"{len(files)} csv files, {sent} uploaded")
    if gh := os.environ.get("GITHUB_OUTPUT"):
        with open(gh, "a") as f:
            f.write(f"uploaded={sent}\n")


if __name__ == "__main__":
    main()
