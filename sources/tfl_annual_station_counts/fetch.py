"""TfL Annual Station Counts: typical-day and annualised entries and exits at
every Underground, Overground, DLR, Elizabeth line (and, in 2021, tram)
station, one workbook per year from 2017. Published by TfL in the
"Annual Station Counts" folder of its open bucket,
https://crowding.data.tfl.gov.uk/ (one subfolder per year).

Each year's folder holds an annualised workbook ('AnnualisedEntryExit_2017.xlsx'
... 'AC2025_AnnualisedEntryExit_public.xlsx' — the naming changes almost every
year, so files are found by listing, never hard-coded) and, for 2017-2021
only, a quarter-hour workbook we don't republish.

The workbook layout drifts too:
- 2017: 'Weekday (Mon-Fri)', Saturday, Sunday entries and exits + annualised,
  with two unlabelled all-zero spacer columns.
- 2018-2021: 'Weekday (Mon-Thu)', Friday, Saturday, Sunday + annualised.
- 2022-: Monday, 'Midweek (Tue-Thu)', Friday, Saturday, Sunday + weekly,
  12-week and annualised totals.
- The day-type and entries/exits header rows swap order in 2024; the
  direction labels are 'entries' / 'Entries' / 'Entry' (and 'en/ex', 'En/Ex').
- The id columns are headed NLC/ASC to 2021 and MNLC/MASC from 2022.

So the tidy step finds the header band by its 'Station' label and reads each
value column's day type and direction from the band, whatever its order.
We publish:

- tfl_annual_station_counts_raw.zip   every year's annualised workbook exactly
                                      as published, as '<year>/<TfL filename>'.
- tfl_annual_station_counts.parquet   one row per year x station row x
                                      published column (wide -> long).

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import re
import sys
import zipfile
from datetime import date
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    canonicalize_categorical,
    categorical_value_sets,
    check_parquet_schema,
    download,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)
from lib.tfl_bucket import LANDING_URL, key_url, list_keys, unrecognised_keys
from lib.tfl_network_demand import build_zip

STEM = "tfl_annual_station_counts"
ZIP_NAME = f"{STEM}_raw.zip"
PARQUET_NAME = f"{STEM}.parquet"
FOLDER = "Annual Station Counts/"
KEY_RE = re.compile(rf"^{re.escape(FOLDER)}(\d{{4}})/([^/]*annualised[^/]*\.xlsx)$", re.IGNORECASE)
# Quarter-hour workbooks (2017-2021) we know about and deliberately skip;
# anything matching neither pattern is reported as unrecognised.
SKIPPED_RE = re.compile(rf"^{re.escape(FOLDER)}\d{{4}}/[^/]*ByQhr[^/]*\.xlsx$", re.IGNORECASE)
FIRST_YEAR = 2017
# TfL has published each year's workbook by October of the following year
# (2022's in Oct 2023, 2023's in Aug 2024, 2025's in Aug 2026). From
# 1 December of year Y+1, year Y must be present; if not, quarantine (harmless
# — HF keeps the previous data) so a missed or renamed release goes red
# within weeks rather than years.
DUE_BY_MONTH = 12

# Files in TfL's folder matching neither pattern, recorded in catalog stats
# (see lib/tfl_bucket.unrecognised_keys). Filled by fetch().
UNRECOGNISED: list[str] = []

# Closed vocabularies: TfL's day-type labels, and its direction labels mapped
# to one canonical spelling each. A new label degrades the tidy step (and
# validate() then quarantines) for a human to map it.
DAY_TYPES = [
    "Weekday (Mon-Fri)", "Weekday (Mon-Thu)", "Monday", "Midweek (Tue-Thu)",
    "Friday", "Saturday", "Sunday", "Weekly", "12-week", "Annualised",
]
MEASURES = {"entries": "entries", "entry": "entries",
            "exits": "exits", "exit": "exits",
            "en/ex": "entries_and_exits"}
ID_HEADERS = [("Mode",), ("NLC", "MNLC"), ("ASC", "MASC"), ("Station",), ("Coverage",), ("Source",)]
CATEGORICAL_COLUMNS = ["mode", "coverage", "source", "day_type", "measure"]

FETCHED_FROM: dict[str, str] = {}


def expected_latest_year(today: date) -> int:
    """The newest count year that TfL should have published by `today`."""
    return today.year - 1 if today.month >= DUE_BY_MONTH else today.year - 2


def list_workbooks() -> dict[int, str]:
    """{year: S3 key} of each year's annualised workbook. Raises if a year
    has two candidates or the series looks moved. Also records any file in
    the folder that matches neither known pattern in UNRECOGNISED."""
    keys = list_keys(FOLDER)
    UNRECOGNISED[:] = unrecognised_keys(keys, [KEY_RE, SKIPPED_RE])
    if UNRECOGNISED:
        print(f"  WARNING: unrecognised file(s) in TfL's folder — a renamed workbook? {UNRECOGNISED}")
    found: dict[int, list[str]] = {}
    for key in keys:
        match = KEY_RE.match(key)
        if match:
            found.setdefault(int(match.group(1)), []).append(key)
    if not found:
        raise RuntimeError(f"No annualised workbooks under {LANDING_URL}{FOLDER} — TfL may have moved the series.")
    ambiguous = {y: k for y, k in found.items() if len(k) > 1}
    if ambiguous:
        raise RuntimeError(f"More than one annualised workbook for a year: {ambiguous}")
    return {year: keys[0] for year, keys in sorted(found.items())}


def _clean(value) -> str | None:
    """Cell -> stripped string, or None for blank/NaN."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    text = str(value).strip()
    return text or None


def _number(value) -> float | None:
    """A count cell -> float; '---' placeholders and blanks -> None."""
    text = _clean(value)
    if text is None or set(text) == {"-"}:
        return None
    return float(text)  # raises on any other text — a layout surprise


def parse_workbook(fh, year: int, arcname: str) -> list[dict]:
    """Long rows for one year's workbook. Raises on any layout surprise."""
    import pandas as pd

    book = pd.ExcelFile(fh)
    sheets = [s for s in book.sheet_names if s.strip().lower() != "note"]
    if len(sheets) != 1:
        raise ValueError(f"{arcname}: expected one data sheet besides 'Note', got {book.sheet_names}")
    grid = pd.read_excel(book, sheet_name=sheets[0], header=None, dtype=object)

    header_rows = grid.index[grid[3].map(_clean) == "Station"].tolist()
    if len(header_rows) != 1:
        raise ValueError(f"{arcname}: can't find the single header row labelled 'Station'")
    hdr = header_rows[0]
    for col, allowed in enumerate(ID_HEADERS):
        if _clean(grid.iat[hdr, col]) not in allowed:
            raise ValueError(f"{arcname}: column {col} is headed {grid.iat[hdr, col]!r}, expected one of {allowed}")

    # Each value column's day type and direction sit in the rows just around
    # the header row, in an order that varies by year. The band is whichever
    # of those rows carry such labels — not the 'Typical' title row above, and
    # not the first data row below.
    day_lookup = {d.casefold(): d for d in DAY_TYPES}
    known = set(day_lookup) | set(MEASURES)

    def is_label_row(r: int) -> bool:
        cells = (_clean(v) for v in grid.iloc[r, len(ID_HEADERS):])
        return any(c is not None and c.casefold() in known for c in cells)

    band = [r for r in (hdr - 1, hdr, hdr + 1) if 0 <= r < len(grid) and is_label_row(r)]
    if hdr not in band and hdr + 1 not in band:
        raise ValueError(f"{arcname}: no day-type / direction labels next to the header row")
    columns = {}
    for col in range(len(ID_HEADERS), grid.shape[1]):
        labels = [_clean(grid.iat[r, col]) for r in band]
        days = [day_lookup[l.casefold()] for l in labels if l and l.casefold() in day_lookup]
        measures = [MEASURES[l.casefold()] for l in labels if l and l.casefold() in MEASURES]
        other = [l for l in labels if l and l.casefold() not in day_lookup and l.casefold() not in MEASURES]
        if other:
            raise ValueError(f"{arcname}: unrecognised header label(s) {other} in column {col} — map them")
        if not days and not measures:
            columns[col] = None  # an unlabelled spacer column (2017)
            continue
        if len(days) != 1 or len(measures) != 1:
            raise ValueError(f"{arcname}: column {col} header band {labels} is ambiguous")
        columns[col] = (days[0], measures[0])

    rows = []
    for r in range(max(band + [hdr]) + 1, len(grid)):
        ids = [_clean(grid.iat[r, c]) for c in range(len(ID_HEADERS))]
        if not any(ids):
            continue
        if ids[0] is None or ids[3] is None:
            raise ValueError(f"{arcname}: row {r + 1} has values but no mode or station — footer or layout change?")
        for col, label in columns.items():
            value = _number(grid.iat[r, col])
            if label is None:
                if value not in (None, 0.0):
                    raise ValueError(f"{arcname}: unlabelled column {col} has data in row {r + 1}")
                continue
            rows.append({
                "year": year,
                "mode": ids[0],
                "nlc": ids[1],
                "asc": ids[2],
                "station": ids[3],
                "coverage": ids[4],
                "source": ids[5],
                "day_type": label[0],
                "measure": label[1],
                "count": value,
                "source_file": arcname,
                "sheet_row": r + 1,
            })
    return rows


def parse_zip(zip_path: Path):
    import pandas as pd

    rows = []
    with zipfile.ZipFile(zip_path) as zf:
        for arcname in sorted(zf.namelist()):
            year = int(arcname.split("/", 1)[0])
            with zf.open(arcname) as fh:
                rows.extend(parse_workbook(fh, year, arcname))
    df = pd.DataFrame(rows)
    df["day_type"] = canonicalize_categorical(df["day_type"], DAY_TYPES)
    day_order = {d: i for i, d in enumerate(DAY_TYPES)}
    df["_day"] = df["day_type"].map(day_order)
    df = df.sort_values(["year", "sheet_row", "measure", "_day"], kind="stable")
    return df.drop(columns="_day").reset_index(drop=True)


def tidy_to_parquet(zip_path: Path, dest: Path) -> Path | None:
    try:
        df = parse_zip(zip_path)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["year"] = df["year"].astype("int64")
        df["sheet_row"] = df["sheet_row"].astype("int64")
        df["count"] = df["count"].astype("float64")
        for col in ("mode", "nlc", "asc", "station", "coverage", "source", "day_type", "measure", "source_file"):
            df[col] = df[col].astype(object)
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw zip still published.")
        return None


def validate(files: list[Path]) -> None:
    """Shape checks; raising quarantines this source's output."""
    by_name = {p.name: p for p in files}
    zip_path = by_name.get(ZIP_NAME)
    if zip_path is None:
        raise ValueError(f"expected raw bundle {ZIP_NAME} was not produced")
    size = zip_path.stat().st_size
    if not (200_000 < size < 20_000_000):
        raise ValueError(f"{ZIP_NAME} is {size:,} bytes — implausible (~0.8 MB in 2026)")
    with zipfile.ZipFile(zip_path) as zf:
        if zf.testzip() is not None:
            raise ValueError(f"{ZIP_NAME} has a corrupt entry")
        years = sorted(int(n.split("/", 1)[0]) for n in zf.namelist())
    if years != list(range(FIRST_YEAR, years[-1] + 1)):
        raise ValueError(f"{ZIP_NAME} years {years} are not contiguous from {FIRST_YEAR}")
    due = expected_latest_year(date.today())
    if years[-1] < due:
        hint = f" Unrecognised files in the folder: {UNRECOGNISED}" if UNRECOGNISED else ""
        raise ValueError(
            f"latest workbook is for {years[-1]}, but TfL normally publishes {due}'s by "
            f"now — check {LANDING_URL} for a renamed or moved file.{hint}"
        )

    parquet = by_name.get(PARQUET_NAME)
    if parquet is None:
        return  # tidy step degraded; the raw zip alone is still valid output

    import pandas as pd

    check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
    df = pd.read_parquet(parquet)
    if sorted(df["year"].unique().tolist()) != years:
        raise ValueError(f"{parquet.name} years don't match the workbooks in the zip")
    unexpected_days = sorted(set(df["day_type"]) - set(DAY_TYPES))
    unexpected_measures = sorted(set(df["measure"]) - set(MEASURES.values()))
    if unexpected_days or unexpected_measures:
        raise ValueError(f"unexpected day_type {unexpected_days} / measure {unexpected_measures} — map them")
    if df.duplicated(["year", "sheet_row", "day_type", "measure"]).any():
        raise ValueError("duplicate (year, sheet_row, day_type, measure) rows")
    if (df["count"] < 0).any():
        raise ValueError("negative counts")

    per_year = df.groupby("year")["sheet_row"].nunique()
    if not per_year.between(400, 700).all():
        raise ValueError(f"station rows per year outside 400..700: {per_year.to_dict()}")
    annual_lu = (df[(df["mode"] == "LU") & (df["day_type"] == "Annualised")]
                 .groupby("year")["count"].sum())
    if not annual_lu.between(3e8, 6e9).all():
        raise ValueError(f"annualised LU entries+exits per year implausible: {annual_lu.to_dict()}")


def stats(files: list[Path]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    if UNRECOGNISED:
        result[ZIP_NAME] = {"unrecognised_bucket_files": list(UNRECOGNISED)}
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is not None:
        file_stats = parquet_stats(parquet, "year")
        file_stats["categorical_values"] = categorical_value_sets(parquet, CATEGORICAL_COLUMNS)
        result[parquet.name] = file_stats
    return result


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    out_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for year, key in list_workbooks().items():
        filename = key.rsplit("/", 1)[1]
        print(f"  Downloading {key!r}")
        local = download(key_url(key), out_dir / f"_download_{year}.xlsx")
        entries.append((f"{year}/{filename}", local))

    zip_path = build_zip(entries, out_dir / ZIP_NAME)
    for _, path in entries:
        path.unlink()
    FETCHED_FROM[ZIP_NAME] = LANDING_URL

    produced = [zip_path]
    parquet = tidy_to_parquet(zip_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = LANDING_URL
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
