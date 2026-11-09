"""ORR Table 1415: time series of estimated passenger entries and exits
(1415a) and interchanges (1415b) at every mainline station in Great
Britain, annually by financial year (April to March) from 1997-98.
Published by the Office of Rail and Road on its data portal:
https://dataportal.orr.gov.uk/statistics/usage/estimates-of-station-usage

ORR publishes once a year (November/December). The .ods link has changed
between releases, so fetch() finds it on the landing page.

Layout (December 2025 release): sheets Cover_sheet, Notes, 1415a_Entries_
and_Exits, 1415b_Interchanges — plus a dozen junk sheets left over from
ORR's own workbook links, which we ignore. Each 1415 sheet has 3 title/notes
rows, then a header: Sort, Station name, Three Letter Code(TLC), National
Location Code (NLC), Region, Local authority, then one column per year
('Apr 1997 to Mar 1998', ...), some with suffixes: '[b]' = break in series,
'[note N]' = see the Notes sheet. Cells hold a number (about 5% fractional —
these are model estimates), '[x]' (not available) or '[z]' (not applicable:
station not open / closed). The 71 closed stations have '[z]' in place of
their codes and region. Both sheets list the same stations in the same order.

We publish:

- orr_1415_station_usage.ods       the workbook exactly as downloaded.
- orr_1415_station_usage.parquet   long format: one row per station per
        financial year with a value in either table, both measures as
        columns, a series_break flag from the header markers.

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
    canonicalize_categorical,
    categorical_value_sets,
    check_parquet_schema,
    download,
    get_page,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)

STEM = "orr_1415_station_usage"
ODS_NAME = f"{STEM}.ods"
PARQUET_NAME = f"{STEM}.parquet"
PORTAL = "https://dataportal.orr.gov.uk"
LANDING_PAGE = f"{PORTAL}/statistics/usage/estimates-of-station-usage"
ODS_LINK_RE = re.compile(r'href="([^"]*table-1415[^"]*\.ods)"', re.IGNORECASE)

SHEET_EE = "1415a_Entries_and_Exits"
SHEET_INT = "1415b_Interchanges"
ID_COLUMNS = {
    "Station name": "station_name",
    "Three Letter Code(TLC)": "tlc",
    "National Location Code (NLC)": "nlc",
    "Region": "region",
    "Local authority: district or unitary": "local_authority",
}
YEAR_RE = re.compile(r"^Apr (\d{4}) to Mar (\d{4})\s*(.*)$")
MISSING_MARKERS = {"[x]", "[z]"}
FIRST_YEAR = 1997

# Closed vocabulary: the 11 ITL1 regions of Great Britain (ORR's spelling).
REGIONS = [
    "East Midlands", "East of England", "London", "North East", "North West",
    "Scotland", "South East", "South West", "Wales", "West Midlands",
    "Yorkshire and The Humber",
]
CATEGORICAL_COLUMNS = ["region"]

FETCHED_FROM: dict[str, str] = {}


def find_ods_url() -> str:
    links = sorted(set(ODS_LINK_RE.findall(get_page(LANDING_PAGE))))
    if len(links) != 1:
        raise RuntimeError(
            f"Expected exactly one Table 1415 .ods link on {LANDING_PAGE}, found "
            f"{links}. The page layout may have changed — check ODS_LINK_RE."
        )
    return PORTAL + links[0] if links[0].startswith("/") else links[0]


def _read_sheet(ods_path: Path, sheet: str):
    """One 1415 sheet as a DataFrame with the real header row, all cells raw."""
    import pandas as pd

    raw = pd.read_excel(ods_path, sheet_name=sheet, header=None, dtype=object)
    header_rows = [i for i in range(min(len(raw), 20)) if "Station name" in raw.iloc[i].astype(str).tolist()]
    if not header_rows:
        raise ValueError(f"{sheet}: no header row containing 'Station name' in the first 20 rows")
    h = header_rows[0]
    table = raw.iloc[h + 1:].reset_index(drop=True)
    table.columns = [str(c).strip() for c in raw.iloc[h]]
    return table.dropna(how="all")


def _year_columns(table, sheet: str) -> list[tuple[str, str, bool]]:
    """(column, 'YYYY-YY', is_break) per year column; checks they're contiguous."""
    years = []
    for col in table.columns:
        m = YEAR_RE.match(col)
        if m:
            start, end = int(m.group(1)), int(m.group(2))
            if end != start + 1:
                raise ValueError(f"{sheet}: odd year column {col!r}")
            years.append((col, f"{start}-{str(end)[2:]}", start, "[b]" in m.group(3)))
    starts = [y[2] for y in years]
    if not starts or starts[0] != FIRST_YEAR or starts != list(range(FIRST_YEAR, FIRST_YEAR + len(starts))):
        raise ValueError(f"{sheet}: year columns are not contiguous from {FIRST_YEAR}: {starts}")
    return [(col, label, brk) for col, label, _, brk in years]


def _value(cell, where: str) -> float | None:
    """A numeric cell, or None for ORR's [x]/[z] markers and blanks. Anything
    else is a format change — raise rather than guess."""
    if cell is None or (isinstance(cell, float) and cell != cell):
        return None
    if isinstance(cell, str):
        if cell.strip() in MISSING_MARKERS:
            return None
        raise ValueError(f"{where}: unexpected cell value {cell!r}")
    value = float(cell)
    if value < 0:
        raise ValueError(f"{where}: negative value {value}")
    return value


def _attr(cell) -> str | None:
    """An identifying cell as a string, None for markers/blanks; NLCs come
    back as ints and are kept as strings (they're codes, not quantities)."""
    if cell is None or (isinstance(cell, float) and cell != cell):
        return None
    s = str(int(cell)) if isinstance(cell, (int, float)) and not isinstance(cell, bool) else str(cell).strip()
    return None if s in MISSING_MARKERS or not s else s


def parse(ods_path: Path):
    """The long table from both sheets. Raises on any layout surprise; used
    by both the tidy step and validate()."""
    import pandas as pd

    ee = _read_sheet(ods_path, SHEET_EE)
    it = _read_sheet(ods_path, SHEET_INT)
    for sheet, table in ((SHEET_EE, ee), (SHEET_INT, it)):
        missing = set(ID_COLUMNS) - set(table.columns)
        if missing:
            raise ValueError(f"{sheet}: missing columns {sorted(missing)}")
    ee_years = _year_columns(ee, SHEET_EE)
    it_years = _year_columns(it, SHEET_INT)
    if [y[1] for y in ee_years] != [y[1] for y in it_years]:
        raise ValueError("1415a and 1415b cover different years")
    ids = list(ID_COLUMNS)
    if len(ee) != len(it) or not (ee[ids].astype(str).values == it[ids].astype(str).values).all():
        raise ValueError("1415a and 1415b no longer list the same stations in the same order")

    rows = []
    for i, (ee_row, it_row) in enumerate(zip(ee.to_dict("records"), it.to_dict("records"))):
        attrs = {out: _attr(ee_row[col]) for col, out in ID_COLUMNS.items()}
        if not attrs["station_name"]:
            raise ValueError(f"{SHEET_EE} row {i}: no station name")
        for (col, label, brk), (it_col, _, _) in zip(ee_years, it_years):
            where = f"{attrs['station_name']} {label}"
            entries_exits = _value(ee_row[col], f"{SHEET_EE} {where}")
            interchanges = _value(it_row[it_col], f"{SHEET_INT} {where}")
            if entries_exits is None and interchanges is None:
                continue
            rows.append({"financial_year": label, **attrs, "entries_exits": entries_exits,
                         "interchanges": interchanges, "series_break": brk})
    df = pd.DataFrame(rows)
    df["region"] = canonicalize_categorical(df["region"], REGIONS)
    return df.sort_values(["financial_year", "station_name", "tlc"], na_position="first").reset_index(drop=True)


def tidy_to_parquet(ods_path: Path, dest: Path) -> Path | None:
    """Best-effort: a layout change degrades to raw-only, never fails the run."""
    try:
        df = parse(ods_path)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["entries_exits"] = df["entries_exits"].astype("float64")
        df["interchanges"] = df["interchanges"].astype("float64")
        df["series_break"] = df["series_break"].astype(bool)
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw ODS still published.")
        return None


def validate(files: list[Path]) -> None:
    """Shape checks on the raw workbook (via parse(), which raises on layout
    surprises) and on the parquet. Raising quarantines this run's files."""
    import pandas as pd

    by_name = {p.name: p for p in files}
    ods = by_name.get(ODS_NAME)
    if ods is None:
        raise ValueError(f"expected {ODS_NAME} was not produced")
    if ods.read_bytes()[:2] != b"PK":
        raise ValueError(f"{ODS_NAME} is not an ODS (zip) file")
    size = ods.stat().st_size
    if not (300_000 < size < 30_000_000):
        raise ValueError(f"{ODS_NAME} is {size:,} bytes — outside the plausible range (~1.5 MB)")

    df = parse(ods)
    stations = df["station_name"].nunique()
    if not (2_400 <= stations <= 3_500):
        raise ValueError(f"{stations} stations — expected ~2,600")
    if df.duplicated(["financial_year", "station_name", "tlc"]).any():
        raise ValueError("duplicate station-year rows")
    bad_regions = set(df["region"].dropna()) - set(REGIONS)
    if bad_regions:
        raise ValueError(f"unexpected region values {sorted(bad_regions)} — new or re-spelled region")

    latest = df["financial_year"].max()
    latest_end = date(int(latest[:4]) + 1, 3, 31)
    if (date.today() - latest_end).days > 730:
        raise ValueError(f"latest year is {latest} — ORR appears to have stopped publishing (or the file is stale)")
    total = df.loc[df["financial_year"] == latest, "entries_exits"].sum()
    if not (1e9 < total < 10e9):
        raise ValueError(f"GB entries+exits in {latest} total {total:,.0f} — outside 1bn..10bn")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
        n = pd.read_parquet(parquet, columns=["financial_year"]).shape[0]
        if n != len(df):
            raise ValueError(f"{PARQUET_NAME} has {n} rows; the workbook gives {len(df)}")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    file_stats = parquet_stats(parquet, "financial_year")
    file_stats["categorical_values"] = categorical_value_sets(parquet, CATEGORICAL_COLUMNS)
    return {parquet.name: file_stats}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    url = find_ods_url()
    print(f"  Downloading {url}")
    ods_path = download(url, out_dir / ODS_NAME)
    FETCHED_FROM[ODS_NAME] = url

    produced = [ods_path]
    parquet = tidy_to_parquet(ods_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = url
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
