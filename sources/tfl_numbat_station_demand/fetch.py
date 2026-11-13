"""TfL NUMBAT station demand: modelled entries, exits, boarders and alighters
at every Underground, Overground, DLR and Elizabeth line station — boarders
and alighters per line, direction and platform — for each quarter-hour of a
typical autumn day of each day type, every year from 2016. Published by TfL
in the "NUMBAT" folder of its open bucket, https://crowding.data.tfl.gov.uk/,
as one workbook per year per day type ('NUMBAT 2025/NBT25TWT_Outputs.xlsx').

NUMBAT is TfL's rail demand model: it assigns each smartcard journey to a
route, scaled to gateline counts. It is the only open source that separates
Elizabeth line from Underground use at shared stations (Tottenham Court Road,
Bond Street, ...): the platform-level boarder/alighter rows do; TfL's Annual
Station Counts and daily footfall do not.

Each workbook has nine sheets; we republish four — Station_Entries,
Station_Exits, Station_Boarders, Station_Alighters — as one long table. The
rest (link loads and frequencies, line boarders, station flows) stay in the
raw zip. Each sheet row carries a daily Total, six time-band sums and 96
quarter-hour values; the Total and bands are sums of the quarter-hours
(tidy_to_parquet checks the Total exactly), so the parquet keeps only the
quarter-hours.

The workbooks are ~474 MB and almost never change, so they are cached under
the gitignored repo-root cache/ keyed by each file's S3 ETag: a steady-state
run lists the bucket and downloads nothing (see police_crime_london for the
same pattern). A cache miss only costs a re-download, never correctness.

Day types change: 2016-17 MTT (Mon-Thu), SAT, SUN; 2018-21 adds FRI;
from 2022 MON, TWT (Tue-Thu), FRI, SAT, SUN. Codes are published verbatim.

We publish:

- tfl_numbat_station_demand_raw.zip   every year's output workbooks exactly
                                      as published, as '<year>/<TfL filename>'.
- tfl_numbat_station_demand.parquet   one row per year x day type x sheet
                                      row x quarter-hour.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import json
import re
import sys
import zipfile
from datetime import date
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    REPO_ROOT,
    categorical_value_sets,
    check_parquet_schema,
    download,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)
from lib.tfl_bucket import LANDING_URL, key_url, list_objects, unrecognised_keys
from lib.tfl_network_demand import build_zip

STEM = "tfl_numbat_station_demand"
ZIP_NAME = f"{STEM}_raw.zip"
PARQUET_NAME = f"{STEM}.parquet"
FOLDER = "NUMBAT/"
CACHE_DIR = REPO_ROOT / "cache" / STEM
MANIFEST = CACHE_DIR / "etags.json"

OUTPUT_RE = re.compile(r"^NUMBAT/NUMBAT (\d{4})/NBT(\d{2})([A-Z]{3})_outputs\.xlsx$", re.IGNORECASE)
# Everything else TfL keeps in the folder that we know about and don't
# republish here; anything matching none of these is reported as unrecognised.
OTHER_KNOWN_RE = [
    re.compile(r"^NUMBAT/Intro_to_NUMBAT\.pdf$"),
    re.compile(r"^NUMBAT/NBT\d{2}-\d{2}_AnnualisedBoarders_Outputs\.xlsx$"),  # its own source
    re.compile(r"^NUMBAT/NUMBAT \d{4}/[^/]*definitions[^/]*$", re.IGNORECASE),
    re.compile(r"^NUMBAT/NUMBAT \d{4}/[^/]*OD[ _]Data/", re.IGNORECASE),
]

FIRST_YEAR = 2016
# TfL has published each year's NUMBAT by November of the following year
# (2022's in Jun 2023, 2024's in Nov 2025, 2025's in Aug 2026).
DUE_BY_MONTH = 12

DAY_TYPE_ORDER = ["MTT", "MON", "TWT", "FRI", "SAT", "SUN"]
# The only day-type sets TfL has used; a new combination quarantines.
DAY_TYPE_SETS = [{"MTT", "SAT", "SUN"}, {"MTT", "FRI", "SAT", "SUN"},
                 {"MON", "TWT", "FRI", "SAT", "SUN"}]
SHEETS = {  # sheet -> (measure, expected leading headers)
    "Station_Entries": ("entries", ["NLC", "ASC", "Station", "Fare Zone", "Total"]),
    "Station_Exits": ("exits", ["NLC", "ASC", "Station", "Fare Zone", "Total"]),
    "Station_Boarders": ("boarders", ["ID", "NLC", "ASC", "Station", "Mode", "Line", "Dir", "Platform", "Total"]),
    "Station_Alighters": ("alighters", ["ID", "NLC", "ASC", "Station", "Mode", "Line", "Dir", "Platform", "Total"]),
}
TIME_BANDS = ["Early", "AM Peak", "Midday", "PM Peak", "Evening", "Late"]
QHR_LABEL_RE = re.compile(r"^\d{4}-\d{4}$")
FIRST_QHR, N_QHR = 21, 96  # TfL's quarter-hour numbers 21 (05:00) .. 116 (04:45)
CATEGORICAL_COLUMNS = ["day_type", "measure", "mode", "line", "direction"]
# TfL's 2019-2021 workbooks hold 62 small negative values (min -31.5), all
# exits/alighters at tram stops — published as given (see caveats). More, or
# deeper, than this means something else is wrong.
MAX_NEGATIVE_VALUES = 200
MIN_VALUE = -100.0

FETCHED_FROM: dict[str, str] = {}
UNRECOGNISED: list[str] = []


def expected_latest_year(today: date) -> int:
    """The newest NUMBAT year TfL should have published by `today`."""
    return today.year - 1 if today.month >= DUE_BY_MONTH else today.year - 2


# --- fetch: list, cache, bundle ---------------------------------------------

def list_workbooks() -> list[dict]:
    """The output workbooks in TfL's folder, as list_objects() dicts plus
    year and day_type, sorted. Records unrecognised files as a side effect."""
    objects = list_objects(FOLDER)
    UNRECOGNISED[:] = unrecognised_keys([o["key"] for o in objects], [OUTPUT_RE, *OTHER_KNOWN_RE])
    if UNRECOGNISED:
        print(f"  WARNING: unrecognised file(s) in TfL's folder — a renamed workbook? {UNRECOGNISED}")
    books = []
    for obj in objects:
        match = OUTPUT_RE.match(obj["key"])
        if not match:
            continue
        year, yy, day_type = int(match.group(1)), match.group(2), match.group(3).upper()
        if f"{year % 100:02d}" != yy:
            raise RuntimeError(f"{obj['key']}: filename year NBT{yy} disagrees with folder {year}")
        books.append({**obj, "year": year, "day_type": day_type,
                      "filename": obj["key"].rsplit("/", 1)[1]})
    if not books:
        raise RuntimeError(f"No NUMBAT output workbooks under {LANDING_URL}{FOLDER} — TfL may have moved them.")
    keys = [(b["year"], b["day_type"]) for b in books]
    if len(set(keys)) != len(keys):
        raise RuntimeError(f"Two workbooks for the same year and day type: {sorted(keys)}")
    return sorted(books, key=lambda b: (b["year"], DAY_TYPE_ORDER.index(b["day_type"])
                                        if b["day_type"] in DAY_TYPE_ORDER else 99))


def cached_download(book: dict) -> Path:
    """The workbook's local copy, downloading only if TfL's ETag changed."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}
    local = CACHE_DIR / str(book["year"]) / book["filename"]
    if local.exists() and manifest.get(book["key"]) == book["etag"] and local.stat().st_size == book["size"]:
        return local
    print(f"  Downloading {book['key']!r}")
    local.parent.mkdir(parents=True, exist_ok=True)
    download(key_url(book["key"]), local)
    manifest[book["key"]] = book["etag"]
    MANIFEST.write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding="utf-8")
    return local


# --- tidy: workbook -> long table ---------------------------------------------

def _code(value) -> str | None:
    """An id cell as a string: 750 -> '750', 750.0 -> '750', 'ABRd' -> 'ABRd'."""
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    return text or None


def parse_sheet(rows: list[tuple], sheet: str, where: str):
    """One station sheet -> long DataFrame (ids repeated per quarter-hour).
    Raises on any layout surprise."""
    import numpy as np
    import pandas as pd

    measure, lead = SHEETS[sheet]
    hours, qhr_numbers, header = rows[0], rows[1], rows[2]
    labels = [str(h).strip() if h is not None else None for h in header]
    if labels[:len(lead)] != lead:
        raise ValueError(f"{where}: leading headers {labels[:len(lead)]} != {lead}")
    bands = labels[len(lead):len(lead) + len(TIME_BANDS)]
    if bands != TIME_BANDS:
        raise ValueError(f"{where}: time-band headers {bands} != {TIME_BANDS}")
    q0 = len(lead) + len(TIME_BANDS)
    qhr_labels = labels[q0:q0 + N_QHR]
    if len(qhr_labels) != N_QHR or not all(l and QHR_LABEL_RE.match(l) for l in qhr_labels):
        raise ValueError(f"{where}: expected {N_QHR} quarter-hour columns after the time bands")
    if [qhr_numbers[q0 + i] for i in range(N_QHR)] != list(range(FIRST_QHR, FIRST_QHR + N_QHR)):
        raise ValueError(f"{where}: quarter-hour numbers are not {FIRST_QHR}..{FIRST_QHR + N_QHR - 1}")
    if any(v is not None for row in rows[3:] for v in row[q0 + N_QHR:]):
        raise ValueError(f"{where}: data beyond the last quarter-hour column")

    body = [r for r in rows[3:] if any(v is not None for v in r[:len(lead)])]
    ids = [[_code(v) for v in r[:len(lead) - 1]] for r in body]
    if any(i[lead.index("Station")] is None for i in ids):
        raise ValueError(f"{where}: a row has no station name")
    values = np.array([[np.nan if v is None else float(v) for v in r[q0:q0 + N_QHR]] for r in body])
    totals = np.array([float(r[len(lead) - 1] or 0) for r in body])
    mismatch = ~np.isclose(np.nansum(values, axis=1), totals, rtol=1e-6, atol=1e-6)
    if mismatch.any():
        raise ValueError(f"{where}: {mismatch.sum()} row(s) whose Total isn't the sum of their quarter-hours")

    n = len(body)
    col = {name: [i[k] for i in ids] for k, name in enumerate(lead[:-1])}
    platform_sheet = "ID" in col
    frame = {
        "measure": np.repeat(measure, n * N_QHR),
        "point_id": np.repeat(col["ID"] if platform_sheet else [None] * n, N_QHR),
        "nlc": np.repeat(col["NLC"], N_QHR),
        "asc": np.repeat(col["ASC"], N_QHR),
        "station": np.repeat(col["Station"], N_QHR),
        "fare_zone": np.repeat([None] * n if platform_sheet else col["Fare Zone"], N_QHR),
        "mode": np.repeat(col["Mode"] if platform_sheet else [None] * n, N_QHR),
        "line": np.repeat(col["Line"] if platform_sheet else [None] * n, N_QHR),
        "direction": np.repeat(col["Dir"] if platform_sheet else [None] * n, N_QHR),
        "platform": np.repeat(col["Platform"] if platform_sheet else [None] * n, N_QHR),
        "qhr": np.tile(np.arange(FIRST_QHR, FIRST_QHR + N_QHR, dtype="int64"), n),
        "quarter_hour": np.tile(np.array(qhr_labels, dtype=object), n),
        "value": values.reshape(-1),
    }
    return pd.DataFrame(frame)


def parse_workbook(fh, year: int, day_type: str, arcname: str):
    import openpyxl
    import pandas as pd

    book = openpyxl.load_workbook(fh, read_only=True, data_only=True)
    missing = [s for s in SHEETS if s not in book.sheetnames]
    if missing:
        raise ValueError(f"{arcname}: missing sheet(s) {missing}")
    frames = []
    for sheet in SHEETS:
        rows = list(book[sheet].iter_rows(values_only=True))
        frames.append(parse_sheet(rows, sheet, f"{arcname} {sheet}"))
    book.close()
    out = pd.concat(frames, ignore_index=True)
    out.insert(0, "year", year)
    out.insert(1, "day_type", day_type)
    out["source_file"] = arcname
    return out


def parse_zip(zip_path: Path):
    import pandas as pd

    frames = []
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        def order(name: str):
            year, filename = name.split("/", 1)
            match = OUTPUT_RE.match(f"NUMBAT/NUMBAT {year}/{filename}")
            return int(year), DAY_TYPE_ORDER.index(match.group(3).upper())
        for name in sorted(names, key=order):
            year, filename = name.split("/", 1)
            day_type = OUTPUT_RE.match(f"NUMBAT/NUMBAT {year}/{filename}").group(3).upper()
            with zf.open(name) as fh:
                frames.append(parse_workbook(fh, int(year), day_type, name))
    return pd.concat(frames, ignore_index=True)


def tidy_to_parquet(zip_path: Path, dest: Path) -> Path | None:
    try:
        df = parse_zip(zip_path)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["year"] = df["year"].astype("int64")
        df["value"] = df["value"].astype("float64")
        for f in fields:
            if f["type"] == "string":
                df[f["name"]] = df[f["name"]].astype(object)
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw zip still published.")
        return None


# --- checks and stats ----------------------------------------------------------

def validate(files: list[Path]) -> None:
    """Shape checks; raising quarantines this source's output."""
    by_name = {p.name: p for p in files}
    zip_path = by_name.get(ZIP_NAME)
    if zip_path is None:
        raise ValueError(f"expected raw bundle {ZIP_NAME} was not produced")
    size = zip_path.stat().st_size
    if not (200_000_000 < size < 2_000_000_000):
        raise ValueError(f"{ZIP_NAME} is {size:,} bytes — implausible (~470 MB in 2026)")
    with zipfile.ZipFile(zip_path) as zf:
        if zf.testzip() is not None:
            raise ValueError(f"{ZIP_NAME} has a corrupt entry")
        years = sorted({int(n.split("/", 1)[0]) for n in zf.namelist()})
    if years != list(range(FIRST_YEAR, years[-1] + 1)):
        raise ValueError(f"{ZIP_NAME} years {years} are not contiguous from {FIRST_YEAR}")
    due = expected_latest_year(date.today())
    if years[-1] < due:
        hint = f" Unrecognised files in the folder: {UNRECOGNISED}" if UNRECOGNISED else ""
        raise ValueError(f"latest NUMBAT year is {years[-1]}, but TfL normally publishes {due}'s by "
                         f"now — check {LANDING_URL} for a renamed or moved file.{hint}")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is None:
        return  # tidy step degraded; the raw zip alone is still valid output

    import pandas as pd

    check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
    df = pd.read_parquet(parquet, columns=["year", "day_type", "measure", "nlc", "point_id", "value"])
    if sorted(df["year"].unique().tolist()) != years:
        raise ValueError(f"{parquet.name} years don't match the workbooks in the zip")
    for year, group in df.groupby("year"):
        if set(group["day_type"].unique()) not in DAY_TYPE_SETS:
            raise ValueError(f"{year}: day types {sorted(group['day_type'].unique())} are a new combination — "
                             "check TfL's NUMBAT folder and update DAY_TYPE_SETS")
    if set(df["measure"]) != {m for m, _ in SHEETS.values()}:
        raise ValueError(f"unexpected measures {sorted(set(df['measure']))}")
    negative = df["value"] < 0
    if negative.sum() > MAX_NEGATIVE_VALUES or df["value"].min() < MIN_VALUE:
        raise ValueError(f"{negative.sum()} negative values (min {df['value'].min()}) — "
                         f"more than TfL's known handful at tram stops")
    rows = df.groupby(["year", "day_type", "measure"]).size() / 96
    station_rows = rows[rows.index.get_level_values("measure").isin(["entries", "exits"])]
    platform_rows = rows[rows.index.get_level_values("measure").isin(["boarders", "alighters"])]
    if not station_rows.between(400, 700).all() or not platform_rows.between(900, 1600).all():
        raise ValueError("station or platform row counts per workbook are outside the plausible range")
    weekday = df[df["day_type"].isin(["MTT", "TWT"]) & (df["measure"] == "entries")]
    entries = weekday.groupby("year")["value"].sum()
    if not entries.between(5e5, 2e7).all():
        raise ValueError(f"typical-weekday entries per year implausible: {entries.round().to_dict()}")


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
    books = list_workbooks()
    entries = [(f"{b['year']}/{b['filename']}", cached_download(b)) for b in books]
    print(f"  {len(entries)} workbooks ready; bundling the raw zip")
    zip_path = build_zip(entries, out_dir / ZIP_NAME)
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
