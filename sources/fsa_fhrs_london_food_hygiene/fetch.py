"""FHRS: Food hygiene ratings for all 33 London local authorities (the 32
boroughs plus the City of London Corporation). Published by the Food
Standards Agency at https://ratings.food.gov.uk/open-data.

The FSA publishes one XML file per local authority — there is no national
file — regenerated daily at a stable, predictable URL (no scraping needed).
We publish two files: a single zip bundling the 33 raw XMLs exactly as
published (the archival record), and a merged London-wide parquet table
(the serving artifact).

The FSA keeps no public history of these files: each day's extract
overwrites the last. The weekly commits and monthly archive-YYYY-MM tags
on this family's HF repo are therefore a longitudinal record that exists
nowhere else. Note the XML header embeds the daily ExtractDate, so the
zip's hash changes every weekly run even if no rating changed.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
Optional extras used here: FETCHED_FROM (filename -> source URL, recorded in
catalog.json as provenance) and stats(files) (per-file computed stats for
catalog.json). The merged table's schema is declared in datapackage.json.
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import download, load_resource_schema, parquet_stats, write_parquet

STEM = "fsa_fhrs_london_food_hygiene"
ZIP_NAME = f"{STEM}_raw_xml.zip"
PARQUET_NAME = f"{STEM}.parquet"
URL_TEMPLATE = "https://ratings.food.gov.uk/OpenDataFiles/FHRS{code}en-GB.xml"

# Resolved download URL per published file, read by run_all.py after fetch()
# and recorded in catalog.json as `fetched_from`. Both published files are
# built from all 33 per-authority downloads, so we record the URL template
# rather than one arbitrary URL (or 33 of them).
FETCHED_FROM: dict[str, str] = {}

# FHRS local-authority code -> (filename slug, FSA authority name).
# Taken from https://api.ratings.food.gov.uk/Authorities (RegionName ==
# "London") in July 2026. These codes are stable; validate() will complain
# loudly if a file stops looking like its authority.
LONDON_AUTHORITIES: dict[int, tuple[str, str]] = {
    501: ("barking_and_dagenham", "Barking and Dagenham"),
    502: ("barnet", "Barnet"),
    503: ("bexley", "Bexley"),
    504: ("brent", "Brent"),
    505: ("bromley", "Bromley"),
    506: ("camden", "Camden"),
    507: ("croydon", "Croydon"),
    508: ("city_of_london", "City of London Corporation"),
    509: ("ealing", "Ealing"),
    510: ("enfield", "Enfield"),
    511: ("greenwich", "Greenwich"),
    512: ("hackney", "Hackney"),
    513: ("hammersmith_and_fulham", "Hammersmith and Fulham"),
    514: ("haringey", "Haringey"),
    515: ("harrow", "Harrow"),
    516: ("havering", "Havering"),
    517: ("hillingdon", "Hillingdon"),
    518: ("hounslow", "Hounslow"),
    519: ("islington", "Islington"),
    520: ("kensington_and_chelsea", "Kensington and Chelsea"),
    521: ("kingston_upon_thames", "Kingston-Upon-Thames"),
    522: ("lambeth", "Lambeth"),
    523: ("lewisham", "Lewisham"),
    524: ("merton", "Merton"),
    525: ("newham", "Newham"),
    526: ("redbridge", "Redbridge"),
    527: ("richmond_upon_thames", "Richmond-Upon-Thames"),
    528: ("southwark", "Southwark"),
    529: ("sutton", "Sutton"),
    530: ("tower_hamlets", "Tower Hamlets"),
    531: ("waltham_forest", "Waltham Forest"),
    532: ("wandsworth", "Wandsworth"),
    533: ("westminster", "Westminster"),
}


def arcname_for(code: int) -> str:
    """Name of an authority's file inside the zip — the FSA's own filename,
    so the bundle stays byte-for-byte what the publisher published."""
    return f"FHRS{code}en-GB.xml"


def build_zip(xml_files: list[tuple[str, Path]], dest: Path) -> Path:
    """Bundle the raw XMLs into one deterministic zip: sorted entries, fixed
    timestamps and permissions, pinned compression level — so identical
    content always produces identical bytes and the catalog only reports a
    change when the data actually changed."""
    with zipfile.ZipFile(dest, "w") as zf:
        for arcname, path in sorted(xml_files):
            info = zipfile.ZipInfo(arcname, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            info.create_system = 3  # ZipInfo defaults to 0 on Windows, 3 on
            # Unix — pin it or the same content hashes differently per OS
            zf.writestr(info, path.read_bytes(), compresslevel=9)
    return dest


def establishment_rows(la_name: str, xml_bytes: bytes) -> list[dict[str, str]]:
    """Flatten one authority's FHRS XML into merged-table rows."""
    root = ET.fromstring(xml_bytes)
    rows = []
    for est in root.iter("EstablishmentDetail"):
        get = lambda tag: (est.findtext(tag) or "").strip()  # noqa: E731
        rows.append(
            {
                "local_authority": la_name,
                "fhrs_id": get("FHRSID"),
                "local_authority_business_id": get("LocalAuthorityBusinessID"),
                "business_name": get("BusinessName"),
                "business_type": get("BusinessType"),
                "business_type_id": get("BusinessTypeID"),
                "address_line_1": get("AddressLine1"),
                "address_line_2": get("AddressLine2"),
                "address_line_3": get("AddressLine3"),
                "address_line_4": get("AddressLine4"),
                "post_code": get("PostCode"),
                "rating_value": get("RatingValue"),
                "rating_key": get("RatingKey"),
                "rating_date": get("RatingDate"),
                "new_rating_pending": get("NewRatingPending"),
                "score_hygiene": get("Scores/Hygiene"),
                "score_structural": get("Scores/Structural"),
                "score_confidence_in_management": get("Scores/ConfidenceInManagement"),
                "longitude": get("Geocode/Longitude"),
                "latitude": get("Geocode/Latitude"),
            }
        )
    return rows


def tidy_merge(zip_path: Path, out_dir: Path) -> list[Path]:
    """Best-effort merge of all authorities into one parquet.

    Same philosophy as the VEH0132 tidy step: if the FSA changes the XML
    shape, we log a warning and publish nothing derived — the raw zip is
    still published either way, so the run never fails just because the
    merge did. Reading back from the zip also proves the bundle is intact.
    """
    try:
        import pandas as pd

        rows: list[dict[str, str]] = []
        with zipfile.ZipFile(zip_path) as zf:
            for code, (slug, la_name) in sorted(LONDON_AUTHORITIES.items()):
                rows.extend(establishment_rows(la_name, zf.read(arcname_for(code))))
        if not rows:
            raise ValueError("no EstablishmentDetail elements found in any file")

        # Column order comes from the schema declared in datapackage.json —
        # the single source of truth, enforced again by validate(). All
        # values are kept as strings: RatingValue can be "Exempt"/
        # "AwaitingInspection", and string columns keep the parquet output
        # deterministic.
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = pd.DataFrame(rows, columns=[f["name"] for f in fields])
        return [write_parquet(df, out_dir / PARQUET_NAME)]
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: merge step skipped ({exc}). Raw XML zip still published.")
        return []


def _norm(s: str) -> str:
    """Lowercase letters only — for fuzzy authority-name comparison."""
    return re.sub(r"[^a-z]", "", s.lower())


def validate(files: list[Path]) -> None:
    """Sanity-check this run's output. Raise (with a clear message) if the
    FSA appears to have changed the format — run_all.py will then quarantine
    the files and the HF repo keeps serving the previous good version.
    Checks are about *shape*, not values.
    """
    by_name = {p.name: p for p in files}

    zip_path = by_name.get(ZIP_NAME)
    if zip_path is None:
        raise ValueError(f"expected raw bundle {ZIP_NAME} was not produced")

    size = zip_path.stat().st_size
    if not (2_000_000 < size < 50_000_000):
        raise ValueError(
            f"{ZIP_NAME} is {size:,} bytes — outside the plausible range "
            "(~10 MB for 33 London authorities)."
        )

    total = 0
    with zipfile.ZipFile(zip_path) as zf:
        expected = {arcname_for(code) for code in LONDON_AUTHORITIES}
        actual = set(zf.namelist())
        if actual != expected:
            raise ValueError(
                f"{ZIP_NAME} entries don't match the 33 expected authorities "
                f"(missing: {sorted(expected - actual)}, "
                f"unexpected: {sorted(actual - expected)})."
            )

        for code, (slug, la_name) in sorted(LONDON_AUTHORITIES.items()):
            root = ET.fromstring(zf.read(arcname_for(code)))
            if root.tag != "FHRSEstablishment":
                raise ValueError(
                    f"{arcname_for(code)}: unexpected XML root <{root.tag}>"
                )

            establishments = root.findall(".//EstablishmentDetail")
            if len(establishments) < 200:
                raise ValueError(
                    f"{arcname_for(code)} has only {len(establishments)} "
                    f"establishments — implausibly few for {la_name}; "
                    "file may be truncated."
                )
            total += len(establishments)

            xml_la = establishments[0].findtext("LocalAuthorityName") or ""
            if _norm(slug) not in _norm(xml_la):
                raise ValueError(
                    f"{arcname_for(code)} says its authority is {xml_la!r}, "
                    f"expected {la_name!r} — the FSA's code-to-authority "
                    "mapping may have changed; review LONDON_AUTHORITIES "
                    "in fetch.py."
                )

    if not (50_000 < total < 200_000):
        raise ValueError(
            f"{total:,} establishments across London — outside the plausible "
            "range (~80k in 2026). The FSA may have changed file contents."
        )

    # If the merged layer was produced, it must match the schema declared in
    # datapackage.json and agree with the raw files.
    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        import pandas as pd

        from lib.common import check_parquet_schema

        check_parquet_schema(
            parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        )

        df = pd.read_parquet(parquet)
        if len(df) != total:
            raise ValueError(
                f"{parquet.name} has {len(df):,} rows but the raw XML files "
                f"contain {total:,} establishments — merge is dropping data."
            )
        if df["local_authority"].nunique() != len(LONDON_AUTHORITIES):
            raise ValueError(
                f"{parquet.name} covers {df['local_authority'].nunique()} "
                f"authorities, expected {len(LONDON_AUTHORITIES)}."
            )


def stats(files: list[Path]) -> dict[str, dict]:
    """Computed per-file stats for catalog.json. min/max_period is the span
    of inspection dates present (rating_date), not the extract date."""
    parquet = next((p for p in files if p.suffix == ".parquet"), None)
    if parquet is None:
        return {}
    return {parquet.name: parquet_stats(parquet, "rating_date")}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    xml_files: list[tuple[str, Path]] = []
    for code, (slug, la_name) in sorted(LONDON_AUTHORITIES.items()):
        url = URL_TEMPLATE.format(code=code)
        print(f"  Downloading {la_name} ({url})")
        xml_files.append((arcname_for(code), download(url, out_dir / arcname_for(code))))

    zip_path = build_zip(xml_files, out_dir / ZIP_NAME)
    for _, path in xml_files:
        path.unlink()  # only the bundle is published
    FETCHED_FROM[ZIP_NAME] = URL_TEMPLATE

    produced = [zip_path]
    for path in tidy_merge(zip_path, out_dir):
        produced.append(path)
        FETCHED_FROM[path.name] = URL_TEMPLATE
    return produced


if __name__ == "__main__":
    files = fetch(Path("data"))
    for f in files:
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
