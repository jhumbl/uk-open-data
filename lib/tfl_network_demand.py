"""Shared helpers for the two sources built from TfL's open "Network Demand"
folder (https://crowding.data.tfl.gov.uk/ -> Network Demand/):

    tfl_station_footfall        StationFootfall_*.csv  (taps per station per day)
    tfl_network_daily_journeys  Journeys_*.csv         (network totals per day)

Both file series share one layout quirk, which is why the logic lives here
rather than being copied into each fetch.py:

- TfL publishes one file per calendar year up to 2024, then rolling
  two-year files (StationFootfall_2024_2025, StationFootfall_2025_2026, ...).
  Periods OVERLAP, and the overlaps are not always identical: the newer file
  carries small late corrections (66 station-days differed between the
  2024_2025 and 2025_2026 files when checked in November 2026). So every
  (date[, station]) key is taken from the file with the LATEST PERIOD — not
  the most recently uploaded file: StationFootfall_2024.csv was re-uploaded
  in April 2026, after the 2024_2025 file, so upload time is not a reliable
  proxy for "most revised".
- The current file is rewritten in place roughly weekly and TfL keeps no
  history, so the raw bundle we publish is the only archive of superseded
  figures.
- Filenames are not tidy: the two-year files have a stray space before
  ".csv" ("StationFootfall_2025_2026 .csv"). Never hard-code them — they
  are discovered from the bucket listing.
- Headers drift: older files have a UTF-8 BOM and spell "DayOFWeek". Columns
  are matched case-insensitively after stripping.
"""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

from lib.common import download
from lib.tfl_bucket import LANDING_URL, key_url, list_keys

FOLDER = "Network Demand/"
# Human-facing landing page for the folder (recorded as provenance).
FOLDER_URL = LANDING_URL

# Canonical day names. TfL's DayOfWeek column is a closed vocabulary.
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


@dataclass(frozen=True)
class PeriodFile:
    """One published file in the series, e.g. 'StationFootfall_2025_2026 .csv'."""

    key: str  # full S3 key, e.g. 'Network Demand/StationFootfall_2025_2026 .csv'
    first_year: int
    last_year: int

    @property
    def filename(self) -> str:
        """TfL's own filename, exactly as published (stray spaces and all)."""
        return self.key[len(FOLDER):]

    @property
    def url(self) -> str:
        return key_url(self.key)

    @property
    def rank(self) -> tuple[int, int]:
        """Higher rank wins an overlap: latest period end, then latest start."""
        return (self.last_year, self.first_year)


def list_period_files(series: str) -> list[PeriodFile]:
    """List the bucket folder and return every file of one series
    ('StationFootfall' or 'Journeys'), sorted oldest period first.

    Raises if nothing matches — that means TfL moved or renamed the series.
    """
    pattern = re.compile(
        rf"^{re.escape(FOLDER)}{re.escape(series)}_(\d{{4}})(?:_(\d{{4}}))?\s*\.csv$"
    )
    files = []
    for key in list_keys(FOLDER):
        match = pattern.match(key)
        if match:
            first = int(match.group(1))
            last = int(match.group(2) or first)
            files.append(PeriodFile(key=key, first_year=first, last_year=last))
    if not files:
        raise RuntimeError(
            f"No {series}_*.csv files found under {LANDING_URL}{FOLDER} — "
            "TfL may have moved or renamed the series; check the bucket."
        )
    if len({f.rank for f in files}) != len(files):
        raise RuntimeError(
            f"Two {series} files cover the same period ({[f.filename for f in files]}) — "
            "the overlap rule can't pick a winner; review lib/tfl_network_demand.py."
        )
    return sorted(files, key=lambda f: f.rank)


def download_all(files: list[PeriodFile], work_dir: Path) -> list[tuple[PeriodFile, Path]]:
    """Download each file into work_dir, returning (file, local path) pairs."""
    out = []
    for f in files:
        print(f"  Downloading {f.filename!r} ({f.url})")
        # Local name avoids the stray space; the zip keeps TfL's real name.
        local = work_dir / f"_download_{f.filename.replace(' ', '')}"
        out.append((f, download(f.url, local)))
    return out


def build_zip(entries: list[tuple[str, Path]], dest: Path) -> Path:
    """Bundle raw files into one byte-deterministic zip (sorted entries, fixed
    timestamps/permissions/create_system, pinned compression) — so identical
    content always hashes identically on every OS. Same recipe as the FSA
    source; see the determinism gotcha in CLAUDE.md."""
    with zipfile.ZipFile(dest, "w") as zf:
        for arcname, path in sorted(entries):
            info = zipfile.ZipInfo(arcname, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            info.create_system = 3
            zf.writestr(info, path.read_bytes(), compresslevel=9)
    return dest


def read_zip_series(zip_path: Path, series: str, key_columns: list[str]):
    """Read every CSV of a series out of the raw zip into one DataFrame,
    resolving overlaps by period rank (newest period wins per key).

    Column names are lower-cased and stripped (older files: BOM, 'DayOFWeek');
    all values are read as strings — the caller types them. A `source_file`
    column records which TfL file each surviving row came from.
    key_columns are given in that lower-cased form, e.g. ['traveldate'].
    """
    import pandas as pd

    pattern = re.compile(rf"^{re.escape(series)}_(\d{{4}})(?:_(\d{{4}}))?\s*\.csv$")
    frames = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            match = pattern.match(name)
            if not match:
                continue
            rank = (int(match.group(2) or match.group(1)), int(match.group(1)))
            with zf.open(name) as fh:
                df = pd.read_csv(fh, dtype=str, encoding="utf-8-sig", keep_default_na=False)
            df.columns = [c.strip().lower() for c in df.columns]
            df["source_file"] = name
            df["_rank"] = [rank] * len(df)
            frames.append(df)
    if not frames:
        raise ValueError(f"no {series}_*.csv entries in {zip_path.name}")

    combined = pd.concat(frames, ignore_index=True)
    missing = [c for c in key_columns if c not in combined.columns]
    if missing:
        raise ValueError(f"{series} files lack expected column(s) {missing}; got {list(combined.columns)}")
    for col in key_columns:
        combined[col] = combined[col].str.strip()
    combined = combined.sort_values("_rank", kind="stable")
    combined = combined.drop_duplicates(subset=key_columns, keep="last")
    return combined.drop(columns="_rank")


def iso_date(yyyymmdd: "pd.Series") -> "pd.Series":
    """'20250101' -> '2025-01-01'. Raises if any value isn't 8 digits, so a
    format change surfaces in the tidy step instead of as garbage dates."""
    bad = ~yyyymmdd.str.fullmatch(r"\d{8}")
    if bad.any():
        raise ValueError(f"unexpected TravelDate values, e.g. {yyyymmdd[bad].head(3).tolist()}")
    return yyyymmdd.str[0:4] + "-" + yyyymmdd.str[4:6] + "-" + yyyymmdd.str[6:8]
