"""TfL Congestion Charge zone camera captures and confirmed vehicles, by
month, from July 2010. Published by TfL on the London Datastore:
https://data.london.gov.uk/dataset/camera-captures-and-confirmed-vehicles-seen-in-the-congestion-ch-2r88d

The Datastore's per-resource download URLs embed a resource UUID that can
change when the file is replaced, so we resolve the current CSV through the
dataset API each run (same pattern as tfl_journeys_by_mode).

Raw layout: one row per month, columns 'Month' ('Jul-10'), 'CC Camera
Captures during Charging Hours', 'CC Confirmed Vehicles observed during
Charging Hours' (blank before October 2016), 'Number of Charging Day in
Month', 'Notes' (free text naming non-charging days).

We publish the CSV as downloaded, plus one parquet: the same rows with ISO
months, integer counts and short column names. Nothing is added or derived.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
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

STEM = "tfl_ccz_camera_captures"
CSV_NAME = f"{STEM}.csv"
PARQUET_NAME = f"{STEM}.parquet"
API_URL = "https://data.london.gov.uk/api/v2/dataset/camera-captures-and-confirmed-vehicles-seen-in-the-congestion-ch-2r88d"

# Raw column -> published column. Closed: a renamed or added column
# quarantines the run for a human to map it.
COLUMNS = {
    "Month": "month",
    "CC Camera Captures during Charging Hours": "camera_captures",
    "CC Confirmed Vehicles observed during Charging Hours": "confirmed_vehicles",
    "Number of Charging Day in Month": "charging_days",
    "Notes": "notes",
}
FIRST_MONTH = "2010-07"
# Months TfL publishes no row for. April 2020: the charge was suspended from
# 23 March to 18 May 2020 (Covid-19), so TfL's file skips the month. Any
# other missing month quarantines the run.
KNOWN_MISSING_MONTHS = {"2020-04"}
# TfL adds each month about six weeks after it ends; quarantine (harmless —
# HF keeps the previous data) if the series looks abandoned.
MAX_STALENESS_DAYS = 150

FETCHED_FROM: dict[str, str] = {}


def find_csv_url() -> str:
    """The current CSV resource's download URL (newest check_timestamp)."""
    api = json.loads(get_page(API_URL))
    csvs = [r for r in (api.get("resources") or {}).values()
            if str(r.get("format", "")).lower() == "csv" and r.get("url")]
    if not csvs:
        raise RuntimeError(f"No CSV resource at {API_URL} — the Datastore API layout may have changed.")
    return max(csvs, key=lambda r: str(r.get("check_timestamp") or ""))["url"]


def _int_or_none(value: str) -> int | None:
    value = value.strip().replace(",", "")
    return int(value) if value else None


def parse(csv_path: Path):
    """The tidy table. Raises on layout surprises; shared by tidy and validate."""
    import pandas as pd

    raw = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    raw.columns = [c.strip() for c in raw.columns]
    if list(raw.columns) != list(COLUMNS):
        raise ValueError(
            f"columns changed: new {sorted(set(raw.columns) - set(COLUMNS))}, "
            f"missing {sorted(set(COLUMNS) - set(raw.columns))} (or reordered)"
        )
    raw = raw[raw["Month"].str.strip() != ""]  # tolerate trailing blank lines

    out = pd.DataFrame({
        "month": [datetime.strptime(m.strip(), "%b-%y").strftime("%Y-%m") for m in raw["Month"]],
        "camera_captures": [_int_or_none(v) for v in raw["CC Camera Captures during Charging Hours"]],
        "confirmed_vehicles": [_int_or_none(v) for v in raw["CC Confirmed Vehicles observed during Charging Hours"]],
        "charging_days": [_int_or_none(v) for v in raw["Number of Charging Day in Month"]],
        "notes": [v.strip() or None for v in raw["Notes"]],
    })
    for col in ("camera_captures", "confirmed_vehicles", "charging_days"):
        out[col] = out[col].astype("Int64")
    return out.sort_values("month").reset_index(drop=True)


def tidy_to_parquet(csv_path: Path, dest: Path) -> Path | None:
    try:
        df = parse(csv_path)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["month"] = df["month"].astype(object)
        df["notes"] = df["notes"].astype(object)
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw CSV still published.")
        return None


def validate(files: list[Path]) -> None:
    import pandas as pd

    by_name = {p.name: p for p in files}
    csv = by_name.get(CSV_NAME)
    if csv is None:
        raise ValueError(f"expected {CSV_NAME} was not produced")
    size = csv.stat().st_size
    if not (3_000 < size < 1_000_000):
        raise ValueError(f"{CSV_NAME} is {size:,} bytes — outside the plausible range (~8 KB in 2026)")

    df = parse(csv)  # raises on column / month-format changes
    if df["month"].iloc[0] != FIRST_MONTH:
        raise ValueError(f"series now starts {df['month'].iloc[0]}, expected {FIRST_MONTH}")
    if df["month"].duplicated().any():
        raise ValueError("duplicate months")
    expected = [m for m in pd.period_range(FIRST_MONTH, df["month"].iloc[-1], freq="M").strftime("%Y-%m")
                if m not in KNOWN_MISSING_MONTHS]
    if df["month"].tolist() != expected:
        missing = sorted(set(expected) - set(df["month"]))
        raise ValueError(f"months are no longer contiguous (missing {missing[:5]})")
    if df["camera_captures"].isna().any():
        raise ValueError("a month has no camera_captures figure")
    if not (1_000_000 < df["camera_captures"].median() < 10_000_000):
        raise ValueError(f"median monthly captures {df['camera_captures'].median()} — expected ~3m (units changed?)")
    if ((df["charging_days"] < 0) | (df["charging_days"] > 31)).any():
        raise ValueError("charging_days outside 0..31 — columns shifted?")
    month_end = (pd.Period(df["month"].iloc[-1], freq="M").end_time).date()
    if (date.today() - month_end).days > MAX_STALENESS_DAYS:
        raise ValueError(f"latest month is {df['month'].iloc[-1]} — TfL appears to have stopped updating")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
        if pd.read_parquet(parquet).shape[0] != len(df):
            raise ValueError(f"{PARQUET_NAME} row count differs from the CSV")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    return {parquet.name: parquet_stats(parquet, "month")}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    url = find_csv_url()
    print(f"  Downloading {url}")
    csv_path = download(url, out_dir / CSV_NAME)
    FETCHED_FROM[CSV_NAME] = url

    produced = [csv_path]
    parquet = tidy_to_parquet(csv_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = url
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
