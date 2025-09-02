#!/usr/bin/env python3
# etl_extraction_first.py
"""
ETL pipeline that computes SHA-256 hashes *immediately after extraction* (i.e. on the raw CSV bytes).
Those extraction hashes are the canonical proof-of-source values you can anchor on-chain.

It still performs transform/load steps and saves an audit ledger.
"""
from pathlib import Path
import hashlib
import json
from datetime import datetime, timezone
import argparse
import io
import csv
import pandas as pd
from typing import List, Optional, Tuple

def sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()

def sha256_file(path: Path, chunk_size: int = 1024*1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def read_raw_bytes(path: Path) -> bytes:
    return path.read_bytes()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = (
        df.columns.str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
        .str.lower()
    )
    df = df.copy()
    df.columns = cols
    return df

def strip_string_cells(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype("string").str.strip()
    return df

def deterministic_csv_bytes(df: pd.DataFrame) -> bytes:
    # stable column ordering and row sorting
    df_sorted_cols = df.reindex(sorted(df.columns), axis=1)
    sort_cols = list(df_sorted_cols.columns)
    if sort_cols:
        df_sorted = df_sorted_cols.sort_values(by=sort_cols, kind="mergesort", na_position="first")
    else:
        df_sorted = df_sorted_cols
    df_out = df_sorted.convert_dtypes()
    buf = io.StringIO(newline="")
    df_out.to_csv(buf, index=False, lineterminator="\n", na_rep="", quoting=csv.QUOTE_MINIMAL, date_format="%Y-%m-%dT%H:%M:%S%z")
    return buf.getvalue().encode("utf-8")

def etl(csv1: Path, csv2: Path, mode: str="union", join_key: Optional[str]=None, date_columns: Optional[List[str]]=None, output: Path=Path("output/clean.csv"), ledger: Path=Path("output/ledger.json")) -> None:
    # --- EXTRACT (and immediate extraction-hash generation) ---
    raw1 = read_raw_bytes(csv1)
    raw2 = read_raw_bytes(csv2)
    extraction_hash1 = sha256_bytes(raw1)
    extraction_hash2 = sha256_bytes(raw2)

    # Print extraction hashes (these are the proof-of-source hashes)
    print("=== EXTRACTION (proof-of-source) ===")
    print(f"{csv1}: sha256 = {extraction_hash1}")
    print(f"{csv2}: sha256 = {extraction_hash2}")
    print("===================================")

    # load into pandas for transform (we already recorded the raw bytes above)
    df1 = pd.read_csv(io.BytesIO(raw1))
    df2 = pd.read_csv(io.BytesIO(raw2))

    # Transform
    df1 = strip_string_cells(normalize_columns(df1))
    df2 = strip_string_cells(normalize_columns(df2))
    if date_columns:
        for c in date_columns:
            if c in df1.columns:
                df1[c] = pd.to_datetime(df1[c], errors="coerce", utc=True)
            if c in df2.columns:
                df2[c] = pd.to_datetime(df2[c], errors="coerce", utc=True)

    # Combine
    if mode == "union":
        df = pd.concat([df1, df2], axis=0, ignore_index=True, sort=False)
    else:
        if not join_key:
            raise ValueError("join_key required for join mode")
        df = df1.merge(df2, on=join_key, how="inner", suffixes=("_1", "_2"))

    df = df.drop_duplicates().reset_index(drop=True)

    # Optionally compute dataset hash (this is NOT the extraction hash)
    dataset_bytes = deterministic_csv_bytes(df)
    dataset_hash = sha256_bytes(dataset_bytes)

    # Save deterministic output CSV and ledger
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(dataset_bytes)

    # Append to ledger (record extraction hashes explicitly)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger_entries = []
    if ledger.exists():
        try:
            ledger_entries = json.loads(ledger.read_text())
        except Exception:
            ledger_entries = []

    entry = {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_paths": [str(csv1), str(csv2)],
        # **extraction hashes** — produced immediately on extraction of raw bytes
        "extraction_hashes": [
            {"path": str(csv1), "sha256": extraction_hash1},
            {"path": str(csv2), "sha256": extraction_hash2},
        ],
        "etl": {
            "mode": mode,
            "join_key": join_key,
            "date_columns": date_columns or [],
        },
        "output": {
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "deterministic_output_path": str(output),
            "deterministic_dataset_sha256": dataset_hash,  # optional separate hash
        }
    }
    ledger_entries.append(entry)
    ledger.write_text(json.dumps(ledger_entries, indent=2))

    print("ETL completed. Deterministic output and ledger written.")
    print(f"Deterministic dataset hash (not the extraction-proof): {dataset_hash}")
    print(f"Ledger: {ledger.resolve()}")
    print("Extraction hashes (anchor these on-chain):")
    print(f" - {csv1.name}: {extraction_hash1}")
    print(f" - {csv2.name}: {extraction_hash2}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv1", required=True, type=Path)
    p.add_argument("--csv2", required=True, type=Path)
    p.add_argument("--mode", choices=["union","join"], default="union")
    p.add_argument("--join-key", default=None)
    p.add_argument("--date-columns", nargs="*", default=None)
    p.add_argument("--output", type=Path, default=Path("output/clean.csv"))
    p.add_argument("--ledger", type=Path, default=Path("output/ledger.json"))
    args = p.parse_args()
    etl(args.csv1, args.csv2, mode=args.mode, join_key=args.join_key, date_columns=args.date_columns, output=args.output, ledger=args.ledger)

if __name__ == "__main__":
    main()
