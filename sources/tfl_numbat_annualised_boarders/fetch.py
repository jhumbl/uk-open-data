"""TfL NUMBAT annualised boarders by line: estimated passengers boarding each
Underground, Overground, DLR and Elizabeth line line in each NUMBAT year from
2016. Published by TfL as a single small workbook at the top of the "NUMBAT"
folder of its open bucket, https://crowding.data.tfl.gov.uk/ — named after
the years it covers ('NBT16-25_AnnualisedBoarders_Outputs.xlsx'), so it is
found by listing and its name changes each year.

Raw layout: one 'multi-year' sheet with title rows, a header row ('Mode',
'Line Code', 'Line Name', then one column per year), one row per line, and
mode subtotal rows below (mostly Excel SUMIFS formulas, not values). The
parquet is the line rows only, wide -> long; the subtotals are sums of them
and stay in the raw xlsx.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    categorical_value_sets,
    check_parquet_schema,
    download,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)
from lib.tfl_bucket import LANDING_URL, key_url, list_keys

STEM = "tfl_numbat_annualised_boarders"
XLSX_NAME = f"{STEM}.xlsx"
PARQUET_NAME = f"{STEM}.parquet"
KEY_RE = re.compile(r"^NUMBAT/NBT(\d{2})-(\d{2})_AnnualisedBoarders_Outputs\.xlsx$", re.IGNORECASE)
FIRST_YEAR = 2016
DUE_BY_MONTH = 12  # same rule as tfl_numbat_station_demand
LEAD = ["Mode", "Line Code", "Line Name"]
CATEGORICAL_COLUMNS = ["mode", "line_code", "line_name"]

FETCHED_FROM: dict[str, str] = {}


def find_key() -> str:
    """The single annualised-boarders workbook's key. Raises if there are
    none or several (TfL normally replaces it each year)."""
    keys = [k for k in list_keys("NUMBAT/") if KEY_RE.match(k)]
    if len(keys) != 1:
        raise RuntimeError(f"expected one NBTyy-yy_AnnualisedBoarders_Outputs.xlsx under "
                           f"{LANDING_URL}NUMBAT/, found {keys}")
    return keys[0]


def parse(xlsx_path: Path):
    """The long table. Raises on layout surprises; shared by tidy and validate."""
    import openpyxl
    import pandas as pd

    book = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    if len(book.sheetnames) != 1:
        raise ValueError(f"expected one sheet, got {book.sheetnames}")
    rows = list(book[book.sheetnames[0]].iter_rows(values_only=True))
    book.close()

    header_at = [i for i, r in enumerate(rows) if [str(v).strip() if v else v for v in r[:3]] == LEAD]
    if len(header_at) != 1:
        raise ValueError(f"can't find the single header row {LEAD}")
    header = rows[header_at[0]]
    year_cols = [(i, int(v)) for i, v in enumerate(header) if i >= len(LEAD) and isinstance(v, (int, float))]
    years = [y for _, y in year_cols]
    if not years or years != list(range(years[0], years[0] + len(years))):
        raise ValueError(f"year columns {years} are not a contiguous run")

    records = []
    for r in rows[header_at[0] + 1:]:
        mode, code, name = (str(v).strip() if v is not None else None for v in r[:3])
        if code is None:
            continue  # blank, or a mode subtotal row (no line code)
        if mode is None or name is None:
            raise ValueError(f"line row {r[:3]} lacks a mode or name")
        for i, year in year_cols:
            value = r[i]
            if value is not None and not isinstance(value, (int, float)):
                raise ValueError(f"{code} {year}: non-numeric cell {value!r}")
            records.append({"numbat_year": year, "mode": mode, "line_code": code,
                            "line_name": name, "boarders": None if value is None else float(value)})
    df = pd.DataFrame(records)
    return df.sort_values(["numbat_year", "mode", "line_code"], kind="stable").reset_index(drop=True)


def tidy_to_parquet(xlsx_path: Path, dest: Path) -> Path | None:
    try:
        df = parse(xlsx_path)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["numbat_year"] = df["numbat_year"].astype("int64")
        df["boarders"] = df["boarders"].astype("float64")
        for col in ("mode", "line_code", "line_name"):
            df[col] = df[col].astype(object)
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw xlsx still published.")
        return None


def validate(files: list[Path]) -> None:
    import pandas as pd

    by_name = {p.name: p for p in files}
    xlsx = by_name.get(XLSX_NAME)
    if xlsx is None:
        raise ValueError(f"expected {XLSX_NAME} was not produced")
    if xlsx.read_bytes()[:2] != b"PK":
        raise ValueError(f"{XLSX_NAME} is not an xlsx (zip) file")
    if not (5_000 < xlsx.stat().st_size < 2_000_000):
        raise ValueError(f"{XLSX_NAME} is {xlsx.stat().st_size:,} bytes — implausible (~24 KB in 2026)")

    df = parse(xlsx)
    years = sorted(df["numbat_year"].unique().tolist())
    if years[0] != FIRST_YEAR:
        raise ValueError(f"series now starts {years[0]}, expected {FIRST_YEAR}")
    today = date.today()
    due = today.year - 1 if today.month >= DUE_BY_MONTH else today.year - 2
    if years[-1] < due:
        raise ValueError(f"latest year is {years[-1]}, but TfL normally publishes {due}'s by now")
    if df.duplicated(["numbat_year", "line_code"]).any():
        raise ValueError("duplicate (year, line_code) rows")
    if not (12 <= df["line_code"].nunique() <= 40):
        raise ValueError(f"{df['line_code'].nunique()} lines — implausible (19 in 2026)")
    if (df["boarders"] < 0).any():
        raise ValueError("negative boarders")
    lu = df[df["mode"] == "LU"].groupby("numbat_year")["boarders"].sum()
    if not lu.between(2e8, 5e9).all():
        raise ValueError(f"annual LU boarders implausible: {lu.round().to_dict()}")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
        if pd.read_parquet(parquet).shape[0] != len(df):
            raise ValueError(f"{PARQUET_NAME} row count differs from the xlsx")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    file_stats = parquet_stats(parquet, "numbat_year")
    file_stats["categorical_values"] = categorical_value_sets(parquet, CATEGORICAL_COLUMNS)
    return {parquet.name: file_stats}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    key = find_key()
    url = key_url(key)
    print(f"  Downloading {key!r}")
    xlsx_path = download(url, out_dir / XLSX_NAME)
    FETCHED_FROM[XLSX_NAME] = url

    produced = [xlsx_path]
    parquet = tidy_to_parquet(xlsx_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = url
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
