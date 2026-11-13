"""Santander Cycles: number of hires per day, London-wide, from 30 July 2010.
Published by TfL on the London Datastore as "Number of Bicycle Hires":
https://data.london.gov.uk/dataset/number-bicycle-hires

The Datastore's download URL embeds a resource UUID and a filename that
changes with each monthly update ('tfl-daily-cycle-hires-sep26-update.xlsx'),
so we resolve the current spreadsheet through the dataset API each run.

Raw layout: an xlsx with a 'Metadata' sheet and a 'Data' sheet holding
several side-by-side tables under one header row — Day / hires, Month /
hires, Year / hires, and Month / average hire time — plus grand totals.
The monthly and yearly hire tables are sums of the daily one (validate()
checks they agree exactly), so the parquet is the daily table; everything
else, including average hire time, stays in the raw xlsx.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    check_parquet_schema,
    download,
    get_page,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)

STEM = "tfl_santander_cycles_daily_hires"
XLSX_NAME = f"{STEM}.xlsx"
PARQUET_NAME = f"{STEM}.parquet"
API_URL = "https://data.london.gov.uk/api/v2/dataset/number-bicycle-hires"

DATA_SHEET = "Data"
FIRST_DATE = "2010-07-30"  # the scheme's first day of public hire
MAX_STALENESS_DAYS = 120   # TfL updates monthly, ~2 weeks after month end

FETCHED_FROM: dict[str, str] = {}


def find_xlsx_url() -> str:
    """The current spreadsheet resource's download URL (newest check_timestamp)."""
    api = json.loads(get_page(API_URL))
    sheets = [r for r in (api.get("resources") or {}).values()
              if str(r.get("url", "")).lower().endswith(".xlsx")]
    if not sheets:
        raise RuntimeError(f"No .xlsx resource at {API_URL} — the Datastore API layout may have changed.")
    return max(sheets, key=lambda r: str(r.get("check_timestamp") or ""))["url"]


def _side_tables(xlsx_path: Path) -> dict[str, "pd.DataFrame"]:
    """The Data sheet's day, month and year hire tables, located by their
    header labels rather than fixed cells (TfL's layout drifts)."""
    import pandas as pd

    sheet = pd.read_excel(xlsx_path, sheet_name=DATA_SHEET, header=None, dtype=object)
    header_rows = sheet.index[sheet.eq("Day").any(axis=1)]
    if len(header_rows) != 1:
        raise ValueError(f"expected one header row containing 'Day' on the {DATA_SHEET!r} sheet")
    header = sheet.loc[header_rows[0]].tolist()
    body = sheet.loc[header_rows[0] + 1:]

    tables = {}
    for label in ("Day", "Month", "Year"):
        # The first column with this label whose right-hand neighbour is the
        # hire count ('Month' also heads the average-hire-time table).
        col = next((i for i, v in enumerate(header)
                    if v == label and i + 1 < len(header)
                    and str(header[i + 1]).strip() == "Number of Bicycle Hires"), None)
        if col is None:
            raise ValueError(f"no '{label}' / 'Number of Bicycle Hires' table on the {DATA_SHEET!r} sheet")
        t = body.iloc[:, [col, col + 1]].dropna(how="all")
        t.columns = ["period", "hires"]
        tables[label] = t
    return tables


def parse(xlsx_path: Path):
    """The daily table (hire_date ISO, hires int). Raises on layout surprises."""
    import pandas as pd

    day = _side_tables(xlsx_path)["Day"]
    if day.isna().any().any():
        raise ValueError("daily table has a date without a count, or a count without a date")
    out = pd.DataFrame({
        "hire_date": pd.to_datetime(day["period"], errors="raise").dt.strftime("%Y-%m-%d"),
        "hires": pd.to_numeric(day["hires"], errors="raise"),
    })
    if (out["hires"] % 1 != 0).any():
        raise ValueError("non-integer daily hire counts")
    out["hires"] = out["hires"].astype("int64")
    return out.sort_values("hire_date").reset_index(drop=True)


def tidy_to_parquet(xlsx_path: Path, dest: Path) -> Path | None:
    try:
        df = parse(xlsx_path)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["hire_date"] = df["hire_date"].astype(object)
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
    size = xlsx.stat().st_size
    if not (50_000 < size < 5_000_000):
        raise ValueError(f"{XLSX_NAME} is {size:,} bytes — outside the plausible range (~150 KB in 2026)")

    df = parse(xlsx)
    if df["hire_date"].iloc[0] != FIRST_DATE:
        raise ValueError(f"series now starts {df['hire_date'].iloc[0]}, expected {FIRST_DATE}")
    dates = pd.to_datetime(df["hire_date"])
    gaps = pd.date_range(dates.min(), dates.max()).difference(dates)
    if df["hire_date"].duplicated().any() or len(gaps):
        raise ValueError(f"daily series has duplicates or {len(gaps)} missing days")
    if (df["hires"] < 0).any():
        raise ValueError("negative hire counts")
    recent = df["hires"].tail(365)
    if not (5_000 < recent.median() < 200_000):
        raise ValueError(f"median daily hires over the last year is {recent.median()} — expected ~25k")
    if (date.today() - dates.max().date()).days > MAX_STALENESS_DAYS:
        raise ValueError(f"latest day is {dates.max().date()} — TfL appears to have stopped updating")

    # TfL's own monthly and yearly tables must equal sums of the daily one:
    # a mismatch means the sheet was misread (or TfL's tables disagree).
    tables = _side_tables(xlsx)
    daily = df.set_index(dates)["hires"]
    for label, freq in (("Month", "M"), ("Year", "Y")):
        published = tables[label]
        keys = (pd.to_datetime(published["period"]).dt.to_period("M") if label == "Month"
                else pd.PeriodIndex(published["period"].astype(int).astype(str), freq="Y"))
        published = pd.Series(pd.to_numeric(published["hires"]).values, index=keys)
        summed = daily.groupby(daily.index.to_period(freq)).sum()
        if not summed.reindex(published.index).eq(published).all():
            raise ValueError(f"TfL's {label.lower()}ly totals don't match the sum of the daily table")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
        if pd.read_parquet(parquet).shape[0] != len(df):
            raise ValueError(f"{PARQUET_NAME} row count differs from the xlsx")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    return {parquet.name: parquet_stats(parquet, "hire_date")}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    url = find_xlsx_url()
    print(f"  Downloading {url}")
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
