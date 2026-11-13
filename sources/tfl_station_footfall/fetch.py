"""TfL station footfall: entry and exit taps per station per day, for ~440
Underground, Elizabeth line, DLR, Overground and some National Rail gate
lines, from 1 January 2019. Published by TfL in the open "Network Demand"
folder of https://crowding.data.tfl.gov.uk/ as StationFootfall_*.csv.

TfL's files overlap (yearly files to 2024, then rolling two-year files) and
the current one is rewritten in place about weekly with late corrections —
see lib/tfl_network_demand.py for the overlap rule (latest period wins) and
the filename/header quirks. We publish:

- tfl_station_footfall_raw.zip   every StationFootfall_*.csv exactly as
                                 published, in one deterministic zip. TfL
                                 keeps no history, so this bundle is the
                                 only archive of figures it later revises.
- tfl_station_footfall.parquet   one row per station per day, overlaps
                                 resolved, ISO dates, integer counts, with
                                 the TfL file each row came from.

The tidy step only: lower-cases/strips headers, strips station names (2019-22
files have 'Cannon Street ' with a trailing space), canonicalises
day_of_week, converts dates to ISO and counts to integers. Station names are
otherwise verbatim — an OPEN vocabulary (stations open and are renamed, e.g.
'Woolwich EL' -> 'Woolwich Elizabeth Line' on 2025-05-28), recorded in
stats() so renames show up as catalog.json diffs.

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
    unrecognised_files,
)

STEM = "tfl_station_footfall"
SERIES = "StationFootfall"
ZIP_NAME = f"{STEM}_raw.zip"
PARQUET_NAME = f"{STEM}.parquet"

# The series has started on this date since TfL first published it; a later
# minimum means files have gone missing.
FIRST_DATE = "2019-01-01"

# If the newest travel_date is older than this, TfL has stopped updating the
# series (normal lag is ~1-2 weeks). Quarantining is harmless — HF keeps the
# previous, identical data — and turns silent staleness into a red run.
MAX_STALENESS_DAYS = 120

# Low-cardinality categorical columns whose value sets go into catalog.json.
# station is ~440 names — larger than most, but renames are exactly the drift
# worth a reviewable diff.
CATEGORICAL_COLUMNS = ["day_of_week", "station"]

FETCHED_FROM: dict[str, str] = {}
# Files in TfL's folder matching neither series, recorded in catalog stats
# (see lib/tfl_bucket.unrecognised_keys). Filled by fetch().
UNRECOGNISED: list[str] = []


def tidy_to_parquet(zip_path: Path, parquet_path: Path) -> Path | None:
    """Best-effort: build the deduplicated daily table from the raw zip.
    On any failure, warn and return None — the raw zip is still published."""
    try:
        import pandas as pd

        df = read_zip_series(zip_path, SERIES, key_columns=["traveldate", "station"])
        out = pd.DataFrame(
            {
                "travel_date": iso_date(df["traveldate"]),
                "day_of_week": canonicalize_categorical(
                    df["dayofweek"].str.strip(), DAYS_OF_WEEK
                ),
                "station": df["station"],
                "entry_tap_count": pd.to_numeric(df["entrytapcount"], errors="raise").astype("int64"),
                "exit_tap_count": pd.to_numeric(df["exittapcount"], errors="raise").astype("int64"),
                "source_file": df["source_file"],
            }
        )
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        out = out[[f["name"] for f in fields]]
        out = out.sort_values(["travel_date", "station"], kind="stable").reset_index(drop=True)
        for col in ("travel_date", "day_of_week", "station", "source_file"):
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
    if not (5_000_000 < size < 300_000_000):
        raise ValueError(f"{ZIP_NAME} is {size:,} bytes — implausible (~15 MB in 2026).")
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

    if not (1_000_000 < len(df) < 5_000_000):
        raise ValueError(f"{parquet.name} has {len(df):,} rows — implausible (~1.2m in 2026).")
    if df.duplicated(["travel_date", "station"]).any():
        raise ValueError(f"{parquet.name} has duplicate (travel_date, station) rows — overlap rule broke.")

    n_stations = df["station"].nunique()
    if not (350 < n_stations < 700):
        raise ValueError(f"{parquet.name} has {n_stations} stations — implausible (~440 in 2026).")

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
        raise ValueError(
            f"{parquet.name} starts {df['travel_date'].min()}, expected {FIRST_DATE} — "
            "an early file may be missing from the bucket."
        )
    expected_days = pd.date_range(dates.min(), dates.max())
    gaps = expected_days.difference(pd.DatetimeIndex(dates.unique()))
    if len(gaps):
        raise ValueError(f"{parquet.name} has {len(gaps)} missing dates, e.g. {gaps[:3].date.tolist()}")

    newest = dates.max().date()
    if newest < date.today() - timedelta(days=MAX_STALENESS_DAYS):
        raise ValueError(
            f"{parquet.name}'s newest travel_date is {newest} — more than "
            f"{MAX_STALENESS_DAYS} days old; TfL may have stopped updating the series."
        )

    if (df[["entry_tap_count", "exit_tap_count"]] < 0).any().any():
        raise ValueError(f"{parquet.name} has negative tap counts")


def stats(files: list[Path]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    if UNRECOGNISED:
        result[ZIP_NAME] = {"unrecognised_bucket_files": list(UNRECOGNISED)}
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is not None:
        file_stats = parquet_stats(parquet, "travel_date")
        file_stats["categorical_values"] = categorical_value_sets(parquet, CATEGORICAL_COLUMNS)
        result[parquet.name] = file_stats
    return result


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    out_dir.mkdir(parents=True, exist_ok=True)
    UNRECOGNISED[:] = unrecognised_files()
    if UNRECOGNISED:
        print(f"  WARNING: unrecognised file(s) in TfL's folder — a renamed series? {UNRECOGNISED}")
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
