"""GLA London Public Realm Trees: location, species, size/age (where
recorded) and 2090 climate suitability for ~1.14 million publicly maintained
trees in London — street trees, park trees, and some school/housing land —
collated by GiGL for the GLA from all 32 boroughs, the City of London, TfL,
the Royal Parks, the LLDC and Quintain (Wembley Park).

New editions are occasional (2018, 2021, November 2025). Each edition is
uploaded to the London Datastore as a NEW resource with a new UUID and a
dated filename, so we never hard-code a download URL: we query the
Datastore's dataset API and download whichever CSV resource has the newest
check_timestamp. Most weekly runs will therefore re-download an identical
file and report it unchanged — that is normal.

We publish two files: the unmodified CSV as downloaded (the archival record,
UTF-8 BOM and all) and a typed parquet (the serving artifact) whose schema is
declared in datapackage.json. The parquet keeps the publisher's column names
and categorical text exactly as published; the tidy step only types the
coordinates and id, trims whitespace, nulls empty cells and fixes a
deterministic row order. The three size columns (canopy_m, height_m,
girth_dbh) stay strings deliberately: boroughs supply a mix of exact values
("8 m") and bands ("05 to 10 m", "<5 m", "20+ m"), so a numeric cast would
null roughly half the recorded data.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
Optional extras used here: FETCHED_FROM (filename -> resolved URL, recorded
in catalog.json as provenance) and stats(files) (per-file computed stats for
catalog.json).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import download, get_page, load_resource_schema, write_parquet

# The Datastore's machine-readable index of this dataset. Stable across
# editions (unlike the per-resource download URLs it points at).
API_URL = "https://data.london.gov.uk/api/dataset/local-authority-maintained-trees"

CSV_NAME = "gla_public_realm_trees.csv"
PARQUET_NAME = "gla_public_realm_trees.parquet"

# Resolved download URL per published file, read by run_all.py after fetch()
# and recorded in catalog.json as `fetched_from`.
FETCHED_FROM: dict[str, str] = {}


def find_csv_url() -> str:
    """Return the download URL of the newest CSV resource in the dataset.

    The API's `resources` mapping (uuid -> resource) carries a
    check_timestamp per resource; newest wins. The `order` field is display
    ordering, not recency — don't be tempted by it.
    """
    api = json.loads(get_page(API_URL))
    csvs = [
        r
        for r in (api.get("resources") or {}).values()
        if str(r.get("format", "")).lower() == "csv" and r.get("url")
    ]
    if not csvs:
        raise RuntimeError(
            f"No CSV resources found at {API_URL}. "
            "The Datastore API layout may have changed — check fetch.py."
        )
    latest = max(csvs, key=lambda r: str(r.get("check_timestamp") or ""))
    print(f"  Newest CSV resource: {latest.get('title')} "
          f"({latest.get('check_timestamp')})")
    return latest["url"]


def tidy_to_parquet(csv_path: Path, parquet_path: Path) -> Path | None:
    """Best-effort conversion of the raw CSV into a typed parquet.

    Deliberately defensive: if the header or the value formats surprise us,
    log a warning and return None — the raw CSV is still published either
    way, so the pipeline never fails just because the tidy step did.

    The output schema is declared in datapackage.json (the single source of
    truth); validate() enforces it.
    """
    try:
        import pandas as pd

        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        expected = [f["name"] for f in fields]

        df = pd.read_csv(
            csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig"
        )
        if list(df.columns) != expected:
            raise ValueError(
                f"CSV columns {list(df.columns)} != declared schema {expected}"
            )

        for col in df.columns:
            df[col] = df[col].str.strip()
        df = df.replace("", pd.NA)

        # Deliberately errors="raise" (not "coerce"): if a future edition
        # puts junk in these columns, the tidy step must degrade with a
        # warning rather than silently publish nulls.
        df["lat"] = pd.to_numeric(df["lat"], errors="raise").astype("float64")
        df["lon"] = pd.to_numeric(df["lon"], errors="raise").astype("float64")
        df["uniqueid"] = df["uniqueid"].astype("Int64")

        # Cluster by borough so borough-filtered reads touch few row groups;
        # uniqueid (unique in the 2025 edition) makes the order total.
        df = df.sort_values(
            ["borough", "uniqueid"], na_position="last", ignore_index=True
        )[expected]

        return write_parquet(df, parquet_path)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy parquet step skipped ({exc}). Raw CSV still published.")
        return None


def validate(files: list[Path]) -> None:
    """Sanity-check this run's output. Raise (with a clear message) if the
    publisher appears to have changed the format — run_all.py will then
    quarantine the files and flag the source, and the HF repo keeps serving
    the previous good version.

    Keep checks cheap and about *shape*, not values.
    """
    csv = next(p for p in files if p.suffix == ".csv")

    with open(csv, "rb") as fh:
        head = fh.read(200).lstrip(b"\xef\xbb\xbf")  # tolerate the UTF-8 BOM
    if not head.startswith(b"borough,lat,lon,uniqueid,"):
        raise ValueError(
            f"{csv.name} does not start with the expected header — "
            "the Datastore file layout has probably changed."
        )

    size = csv.stat().st_size
    if not (100_000_000 < size < 500_000_000):
        raise ValueError(
            f"{csv.name} is {size:,} bytes — outside the plausible range for "
            "this dataset (~209 MB in the 2025 edition). The API may have "
            "matched the wrong resource, or the file is truncated."
        )

    parquet = next((p for p in files if p.suffix == ".parquet"), None)
    if parquet is not None:
        import pandas as pd

        from lib.common import check_parquet_schema

        check_parquet_schema(
            parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        )

        df = pd.read_parquet(parquet, columns=["borough", "lat"])

        # ~1.14M rows in the 2025 edition; leave headroom either way.
        if not (800_000 < len(df) < 2_500_000):
            raise ValueError(
                f"{parquet.name} has {len(df):,} rows — outside the plausible "
                "range; review tidy_to_parquet()."
            )

        n_boroughs = df["borough"].nunique()
        if not (25 <= n_boroughs <= 45):
            raise ValueError(
                f"{parquet.name} has {n_boroughs} borough values — expected "
                "~33; the borough column has probably changed meaning."
            )

        # This repo exists to serve Westminster questions — its rows going
        # missing is exactly the kind of quiet regression to catch.
        westminster = (df["borough"] == "Westminster").sum()
        if westminster < 10_000:
            raise ValueError(
                f"{parquet.name} has only {westminster:,} Westminster rows "
                "(~34,000 expected) — borough naming or coverage has changed."
            )

        stray = ((df["lat"] < 51.0) | (df["lat"] > 52.1)).mean()
        if stray > 0.01:
            raise ValueError(
                f"{parquet.name}: {stray:.1%} of latitudes fall outside "
                "London's plausible range — coordinate columns may have "
                "been swapped or re-projected."
            )


def stats(files: list[Path]) -> dict[str, dict]:
    """Computed per-file stats for catalog.json. This dataset has no period
    column (it is a current-state register), so row count only."""
    parquet = next((p for p in files if p.suffix == ".parquet"), None)
    if parquet is None:
        return {}
    import pyarrow.parquet as pq

    return {parquet.name: {"row_count": pq.ParquetFile(parquet).metadata.num_rows}}


def fetch(out_dir: Path) -> list[Path]:
    url = find_csv_url()
    print(f"  Downloading {url}")
    FETCHED_FROM.clear()
    csv_path = download(url, out_dir / CSV_NAME)
    FETCHED_FROM[CSV_NAME] = url

    produced = [csv_path]
    parquet_path = tidy_to_parquet(csv_path, out_dir / PARQUET_NAME)
    if parquet_path:
        produced.append(parquet_path)
        FETCHED_FROM[PARQUET_NAME] = url
    return produced


if __name__ == "__main__":
    files = fetch(Path("data"))
    for f in files:
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
