"""VEH0132: Licensed ultra low emission vehicles (ULEVs) at the end of the
quarter, by fuel type, keepership and local authority. Published by DfT/DVLA.

The direct .ods download URL changes with every release (it contains an
attachment id), but the landing page URL is stable. So we scrape the landing
page for the current VEH0132 link, then download it.

We publish two files: the unmodified ODS as downloaded (the archival record)
and a long/tidy parquet — one row per geography x fuel x keepership x
quarter — whose schema is declared in datapackage.json (the serving
artifact).

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
Optional extras used here: FETCHED_FROM (filename -> resolved URL, recorded
in catalog.json as provenance) and stats(files) (per-file computed stats for
catalog.json).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    download,
    get_page,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)

LANDING_PAGE = (
    "https://www.gov.uk/government/statistical-data-sets/"
    "vehicle-licensing-statistics-data-tables"
)

# Matches e.g. https://assets.publishing.service.gov.uk/.../veh0132.ods
ODS_LINK_RE = re.compile(
    r'href="(https://assets\.publishing\.service\.gov\.uk/[^"]*veh0132[^"]*\.ods)"',
    re.IGNORECASE,
)

ODS_NAME = "dft_veh0132_ulev_by_la.ods"
PARQUET_NAME = "dft_veh0132_ulev_by_la.parquet"

# Wide-sheet header -> tidy column name (matched case-insensitively).
ID_COLUMNS = {
    "ons code": "ons_code",
    "ons geography": "ons_geography",
    "ons sort": "ons_sort",
    "fuel": "fuel",
    "keepership": "keepership",
}

# DfT's quarter columns look like "2025 Q4".
QUARTER_RE = re.compile(r"^\d{4} Q[1-4]$")

# Resolved download URL per published file, read by run_all.py after fetch()
# and recorded in catalog.json as `fetched_from`.
FETCHED_FROM: dict[str, str] = {}


def find_ods_url() -> str:
    html = get_page(LANDING_PAGE)
    match = ODS_LINK_RE.search(html)
    if not match:
        raise RuntimeError(
            f"Could not find a VEH0132 .ods link on {LANDING_PAGE}. "
            "The page layout may have changed — check the regex in fetch.py."
        )
    return match.group(1)


def tidy_to_parquet(ods_path: Path, parquet_path: Path) -> Path | None:
    """Best-effort conversion of the raw ODS into a long/tidy parquet.

    DfT statistical ODS files have title/notes rows above the header, and
    the layout occasionally changes between releases. So this is deliberately
    defensive: if anything about the structure surprises us, we log a warning
    and return None — the raw ODS is still published either way, so the
    pipeline never fails just because the tidy step did.

    The output schema is declared in datapackage.json (the single source of
    truth); validate() enforces it.
    """
    try:
        import pandas as pd

        sheets = pd.read_excel(ods_path, engine="odf", sheet_name=None, header=None)
        # The data sheet is usually the largest one whose name mentions the
        # table id; fall back to the largest sheet overall.
        candidates = [n for n in sheets if "0132" in n] or list(sheets)
        sheet_name = max(candidates, key=lambda n: sheets[n].shape[0])
        raw = sheets[sheet_name]

        # Find the header row: the first row containing at least two cells
        # that exactly match known column-header labels. We match individual
        # cells (not the row's concatenated text) and ignore long cells, so
        # the title row above the table — which is one long sentence that
        # happens to contain phrases like "local authority" — can't be
        # mistaken for the header.
        HEADER_LABELS = {
            "ons code", "ons geography", "ons sort", "fuel",
            "keepership", "local authority", "region",
        }
        header_idx = None
        for i in range(min(len(raw), 15)):
            cells = {
                str(v).strip().lower()
                for v in raw.iloc[i].tolist()
                if pd.notna(v) and len(str(v)) < 40
            }
            if len(cells & HEADER_LABELS) >= 2:
                header_idx = i
                break
        if header_idx is None:
            raise ValueError("could not locate header row")

        wide = pd.read_excel(
            ods_path, engine="odf", sheet_name=sheet_name, header=header_idx
        )
        wide = wide.dropna(how="all").dropna(axis=1, how="all")
        wide.columns = [str(c).strip() for c in wide.columns]

        # Map the id columns case-insensitively to tidy names; any other
        # non-quarter column (e.g. the constant "Units") is dropped.
        rename = {
            c: ID_COLUMNS[c.lower()] for c in wide.columns if c.lower() in ID_COLUMNS
        }
        missing = set(ID_COLUMNS.values()) - set(rename.values())
        if missing:
            raise ValueError(f"expected id column(s) missing: {sorted(missing)}")
        wide = wide.rename(columns=rename)

        quarter_cols = [c for c in wide.columns if QUARTER_RE.match(c)]
        if len(quarter_cols) < 10:
            raise ValueError(
                f"only {len(quarter_cols)} 'YYYY Qn' columns found — layout surprise"
            )

        # Footnote rows below the table have no ONS code.
        wide = wide[wide["ons_code"].notna()]

        # DfT indents geography names to show hierarchy (LAs under regions);
        # strip that presentation whitespace — ons_code and ons_sort already
        # carry identity and order, and '   Westminster' would break every
        # consumer's equality lookup.
        for col in ("ons_code", "ons_geography", "fuel", "keepership"):
            wide[col] = wide[col].str.strip()

        df = wide.melt(
            id_vars=list(ID_COLUMNS.values()),
            value_vars=quarter_cols,
            var_name="quarter",
            value_name="count",
        )

        # DfT suppresses some cells as "[x]" -> null. Deliberately NOT
        # to_numeric(errors="coerce"): if DfT introduces a marker we don't
        # know, astype must raise (degrading with a warning) rather than
        # silently publishing nulls.
        suppressed = df["count"].astype(str).str.strip() == "[x]"
        df["count"] = df["count"].mask(suppressed).astype("Int64")
        df["ons_sort"] = df["ons_sort"].astype("Int64")

        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df.sort_values(
            ["ons_sort", "ons_code", "fuel", "keepership", "quarter"],
            ignore_index=True,
        )[[f["name"] for f in fields]]

        return write_parquet(df, parquet_path)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy parquet step skipped ({exc}). Raw ODS still published.")
        return None


def validate(files: list[Path]) -> None:
    """Sanity-check this run's output. Raise (with a clear message) if the
    publisher appears to have changed the format — run_all.py will then
    quarantine the files and flag the source, and the HF repo keeps
    serving the previous good version.

    Keep checks cheap and about *shape*, not values: magic bytes, size
    bounds, the declared schema, plausible row counts.
    """
    ods = next(p for p in files if p.suffix == ".ods")

    # ODS files are zip containers -> must start with "PK". Catches the
    # classic silent failure of an HTML error page saved as .ods.
    with open(ods, "rb") as fh:
        if fh.read(2) != b"PK":
            raise ValueError(f"{ods.name} is not a valid ODS file (bad magic bytes)")

    size = ods.stat().st_size
    if not (500_000 < size < 50_000_000):
        raise ValueError(
            f"{ods.name} is {size:,} bytes — outside the plausible range for "
            "VEH0132 (~2 MB). Publisher may have changed or truncated the file."
        )

    # If the tidy parquet was produced, it must match the schema declared in
    # datapackage.json and look like the long LA-level table we expect.
    parquet = next((p for p in files if p.suffix == ".parquet"), None)
    if parquet is not None:
        import pandas as pd

        from lib.common import check_parquet_schema

        check_parquet_schema(
            parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        )

        df = pd.read_parquet(parquet)

        # ~6,500 wide rows x ~57 quarters ≈ 375k; leave headroom for growth.
        if not (200_000 < len(df) < 800_000):
            raise ValueError(
                f"{parquet.name} has {len(df):,} rows — outside the plausible "
                "range; review tidy_to_parquet()."
            )

        bad_quarters = df.loc[~df["quarter"].str.match(QUARTER_RE), "quarter"]
        if not bad_quarters.empty:
            raise ValueError(
                f"{parquet.name} has quarter value(s) like "
                f"{bad_quarters.iloc[0]!r} that don't look like 'YYYY Qn'."
            )

        if df["ons_code"].str.match(r"^[EWSNK]\d{8}$", na=False).sum() < 100:
            raise ValueError(
                f"{parquet.name} contains almost no ONS geography codes — "
                "the table layout has probably changed; review tidy_to_parquet()."
            )

        if not (3 <= df["fuel"].nunique() <= 12):
            raise ValueError(
                f"{parquet.name} has {df['fuel'].nunique()} fuel values — "
                "implausible; review tidy_to_parquet()."
            )
        if not (2 <= df["keepership"].nunique() <= 5):
            raise ValueError(
                f"{parquet.name} has {df['keepership'].nunique()} keepership "
                "values — implausible; review tidy_to_parquet()."
            )

        null_fraction = df["count"].isna().mean()
        if null_fraction > 0.25:
            raise ValueError(
                f"{parquet.name}: {null_fraction:.0%} of counts are null — far "
                "more suppression than expected (~1%); review tidy_to_parquet()."
            )


def stats(files: list[Path]) -> dict[str, dict]:
    """Computed per-file stats for catalog.json (row count, quarter span)."""
    parquet = next((p for p in files if p.suffix == ".parquet"), None)
    if parquet is None:
        return {}
    return {parquet.name: parquet_stats(parquet, "quarter")}


def fetch(out_dir: Path) -> list[Path]:
    url = find_ods_url()
    print(f"  Downloading {url}")
    FETCHED_FROM.clear()
    ods_path = download(url, out_dir / ODS_NAME)
    FETCHED_FROM[ODS_NAME] = url

    produced = [ods_path]
    parquet_path = tidy_to_parquet(ods_path, out_dir / PARQUET_NAME)
    if parquet_path:
        produced.append(parquet_path)
        FETCHED_FROM[PARQUET_NAME] = url
    return produced


if __name__ == "__main__":
    files = fetch(Path("data"))
    for f in files:
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
