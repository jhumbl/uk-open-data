"""TfL network daily journeys: total Underground and bus journeys across
London per day, from 1 January 2019, rounded by TfL to the nearest thousand.
Published in the open "Network Demand" folder of
https://crowding.data.tfl.gov.uk/ as Journeys_*.csv, alongside the station
footfall files (source tfl_station_footfall) — same layout, same overlap
quirk; see lib/tfl_network_demand.py (latest period wins).

We publish:

- tfl_network_daily_journeys_raw.zip   every Journeys_*.csv exactly as
                                       published, in one deterministic zip
                                       (TfL revises in place, keeps no history).
- tfl_network_daily_journeys.parquet   one row per day, overlaps resolved.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    canonicalize_categorical,
    categorical_value_sets,
    check_parquet_schema,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)
from lib.tfl_network_demand import (
    DAYS_OF_WEEK,
    FOLDER_URL,
    build_zip,
    download_all,
    iso_date,
    list_period_files,
    read_zip_series,
)

STEM = "tfl_network_daily_journeys"
SERIES = "Journeys"
ZIP_NAME = f"{STEM}_raw.zip"
PARQUET_NAME = f"{STEM}.parquet"

FIRST_DATE = "2019-01-01"
MAX_STALENESS_DAYS = 120  # see tfl_station_footfall/fetch.py
CATEGORICAL_COLUMNS = ["day_of_week"]

FETCHED_FROM: dict[str, str] = {}


def tidy_to_parquet(zip_path: Path, parquet_path: Path) -> Path | None:
    """Best-effort: build the deduplicated daily table from the raw zip.
    On any failure, warn and return None — the raw zip is still published."""
    try:
        import pandas as pd

        df = read_zip_series(zip_path, SERIES, key_columns=["traveldate"])
        out = pd.DataFrame(
            {
                "travel_date": iso_date(df["traveldate"]),
                "day_of_week": canonicalize_categorical(
                    df["dayofweek"].str.strip(), DAYS_OF_WEEK
                ),
                "tube_journey_count": pd.to_numeric(df["tubejourneycount"], errors="raise").astype("int64"),
                "bus_journey_count": pd.to_numeric(df["busjourneycount"], errors="raise").astype("int64"),
                "source_file": df["source_file"],
            }
        )
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        out = out[[f["name"] for f in fields]]
        out = out.sort_values("travel_date", kind="stable").reset_index(drop=True)
        for col in ("travel_date", "day_of_week", "source_file"):
            out[col] = out[col].astype(object)
        return write_parquet(out, parquet_path)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw zip still published.")
        return None


def validate(files: list[Path]) -> None:
    """Shape checks; raising quarantines this source's output."""
    import zipfile

    by_name = {p.name: p for p in files}
    zip_path = by_name.get(ZIP_NAME)
    if zip_path is None:
        raise ValueError(f"expected raw bundle {ZIP_NAME} was not produced")
    size = zip_path.stat().st_size
    if not (20_000 < size < 10_000_000):
        raise ValueError(f"{ZIP_NAME} is {size:,} bytes — implausible (~40 KB in 2026).")
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        if zf.testzip() is not None:
            raise ValueError(f"{ZIP_NAME} has a corrupt entry")
    if len(names) < 8 or not all(n.startswith(f"{SERIES}_") for n in names):
        raise ValueError(f"{ZIP_NAME} entries look wrong: {names}")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is None:
        return  # tidy step degraded; the raw zip alone is still valid output

    import pandas as pd

    check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
    df = pd.read_parquet(parquet)

    if df["travel_date"].duplicated().any():
        raise ValueError(f"{parquet.name} has duplicate travel_date rows — overlap rule broke.")
    unexpected = sorted(set(df["day_of_week"].unique()) - set(DAYS_OF_WEEK))
    if unexpected:
        raise ValueError(
            f"{parquet.name} has unexpected day_of_week value(s) {unexpected} — "
            "TfL changed the labels; review tidy_to_parquet()."
        )
    dates = pd.to_datetime(df["travel_date"], format="%Y-%m-%d")
    if (dates.dt.day_name() != df["day_of_week"]).any():
        raise ValueError(f"{parquet.name}: day_of_week disagrees with travel_date — columns shifted?")
    if df["travel_date"].min() != FIRST_DATE:
        raise ValueError(f"{parquet.name} starts {df['travel_date'].min()}, expected {FIRST_DATE}.")
    if len(df) != len(pd.date_range(dates.min(), dates.max())):
        raise ValueError(f"{parquet.name} has gaps between {dates.min().date()} and {dates.max().date()}.")
    newest = dates.max().date()
    if newest < date.today() - timedelta(days=MAX_STALENESS_DAYS):
        raise ValueError(f"{parquet.name}'s newest travel_date is {newest} — TfL may have stopped updating.")

    # Plausibility of magnitude (shape, not values): London runs millions of
    # journeys a day; single digits would mean TfL switched units.
    for col in ("tube_journey_count", "bus_journey_count"):
        median = df[col].median()
        if not (100_000 < median < 20_000_000):
            raise ValueError(f"{parquet.name} median {col} is {median:,.0f} — units changed?")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    file_stats = parquet_stats(parquet, "travel_date")
    file_stats["categorical_values"] = categorical_value_sets(parquet, CATEGORICAL_COLUMNS)
    return {parquet.name: file_stats}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    out_dir.mkdir(parents=True, exist_ok=True)
    period_files = list_period_files(SERIES)
    downloads = download_all(period_files, out_dir)

    zip_path = build_zip([(f.filename, path) for f, path in downloads], out_dir / ZIP_NAME)
    for _, path in downloads:
        path.unlink()
    FETCHED_FROM[ZIP_NAME] = FOLDER_URL

    produced = [zip_path]
    parquet = tidy_to_parquet(zip_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = FOLDER_URL
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
