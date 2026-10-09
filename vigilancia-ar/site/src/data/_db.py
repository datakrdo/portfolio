"""Shared Databricks access for the Parquet data loaders.

Auth: DATABRICKS_HOST + DATABRICKS_HTTP_PATH, plus DATABRICKS_TOKEN (CI) or, when
no token is set, browser OAuth (local `npm run dev`). Marts live in
`<catalog>.<VIGILANCIA_SCHEMA>_marts` (default dev_vigilancia_ar, as in the CI profile).
"""

import os
import sys
from pathlib import Path

import pyarrow.parquet as pq
from databricks import sql

CATALOG = os.environ.get("VIGILANCIA_CATALOG", "workspace")
MARTS = f"{CATALOG}.{os.environ.get('VIGILANCIA_SCHEMA', 'dev_vigilancia_ar')}_marts"
COMPILED_ANALYSES = (
    Path(__file__).resolve().parents[3] / "target/compiled/vigilancia_ar/analyses"
)


def _connect():
    kw = {
        "server_hostname": os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/"),
        "http_path": os.environ["DATABRICKS_HTTP_PATH"],
    }
    if token := os.environ.get("DATABRICKS_TOKEN"):
        kw["access_token"] = token
    else:
        kw["auth_type"] = "databricks-oauth"
    return sql.connect(**kw)


def emit(query: str) -> None:
    """Run a query and stream the result to stdout as Parquet (the loader contract)."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(query)
        pq.write_table(cur.fetchall_arrow(), sys.stdout.buffer)


def emit_analysis(name: str) -> None:
    """Run an already-compiled dbt analysis (written by `dbt docs generate`)."""
    emit((COMPILED_ANALYSES / f"{name}.sql").read_text().strip().rstrip(";"))
