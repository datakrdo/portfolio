#!/usr/bin/env python3
"""Write a machine-readable exploratory summary for the PBC dataset."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data import load_pbc_data  # noqa: E402
from src.eda import summarize_dataset  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="data/raw/pbc.csv")
    parser.add_argument("--output", default="outputs/eda_summary.json")
    args = parser.parse_args()

    summary = summarize_dataset(load_pbc_data(args.data))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
