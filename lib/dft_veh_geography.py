"""Region codes for the geography rows of DfT's VEH local-authority tables.

Shared by dft_veh0105_licensed_by_la and dft_veh0132_ulev_by_la.

DfT's sheets carry no region column: the hierarchy is presentational. Within
each block of rows (one block per body type x fuel x keepership combination)
the geographies run United Kingdom, Great Britain, England, then each region
(E12...) followed by its counties and local authorities, then Wales, Scotland
and Northern Ireland (W92/S92/N92) followed by theirs. `ons_sort` is only the
row's position within its block, so it is no help. We derive region_code by
walking the rows in sheet order, which is exactly the hierarchy DfT
publishes, and check_region_codes() then verifies the result against facts
that do not depend on row order (code prefixes, the 'unknown within <region>'
row names, London's 33 boroughs), so a reshuffled sheet quarantines rather
than publishing wrong regions.

The nations stand in as regions for Wales, Scotland and Northern Ireland,
because that is where DfT's table places them.
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

# Rows that start a region: England's regions, and the three other nations.
REGION_PREFIXES = ("E12", "W92", "S92", "N92")
# Rows above region level: United Kingdom, Great Britain, England.
ABOVE_REGION_PREFIXES = ("K02", "K03", "E92")
# Local authority and county codes -> the prefix their region must have.
EXPECTED_REGION_PREFIX = {
    "E06": "E12", "E07": "E12", "E08": "E12", "E09": "E12",
    "E10": "E12", "E11": "E12", "E13": "E12",
    "W06": "W92", "S12": "S92", "N09": "N92",
}
LONDON = "E12000007"
LONDON_BOROUGHS = 33  # 32 boroughs + the City of London, all E09


def assign_region_codes(codes: Iterable[str], names: Iterable[str],
                        blocks: Iterable[tuple]) -> list[str | None]:
    """region_code for each row, given the rows' ONS codes, geography names
    and block keys (the non-geography id columns) in sheet order.

    A region or nation row gets its own code; rows above region level get
    None; a local authority or county gets the region it sits under. DfT's
    '[z]' rows get the current region only if their name says so
    ('Local Authority unknown within London', 'District unknown within
    Kent'); the national ones ('Region or county unknown', 'Vehicle under
    disposal previously GB') get None — some blocks place those straight
    after a region, so carrying the region forward would be wrong.
    """
    out: list[str | None] = []
    current: str | None = None
    previous_block = None
    for code, name, block in zip(codes, names, blocks):
        if block != previous_block:
            current, previous_block = None, block
        if code.startswith(REGION_PREFIXES):
            current = code
            out.append(code)
        elif code.startswith(ABOVE_REGION_PREFIXES):
            current = None
            out.append(None)
        elif code == "[z]" and " within " not in name:
            out.append(None)
        else:
            out.append(current)
    return out


def check_region_codes(df: pd.DataFrame, filename: str) -> None:
    """Raise if df's region_code column (with ons_code and ons_geography)
    looks wrong. Checks only facts that do not depend on row order."""
    geo = df[["ons_code", "ons_geography", "region_code"]].drop_duplicates()
    region_rows = geo[geo["ons_code"].str.startswith(REGION_PREFIXES)]
    region_names = dict(zip(region_rows["ons_code"], region_rows["ons_geography"]))

    def fail(message: str, rows: pd.DataFrame) -> None:
        sample = rows.head(3)[["ons_code", "ons_geography", "region_code"]].to_dict("records")
        raise ValueError(f"{filename}: {message}, e.g. {sample} — "
                         "DfT's geography layout may have changed; review lib/dft_veh_geography.py")

    if not 10 <= len(region_names) <= 14:
        fail(f"{len(region_names)} region/nation rows, expected 12", region_rows)
    unknown = geo[geo["region_code"].notna() & ~geo["region_code"].isin(list(region_names))]
    if not unknown.empty:
        fail("region_code that is not a region row of the table", unknown)
    if (region_rows["region_code"] != region_rows["ons_code"]).any():
        fail("a region row whose region_code is not its own code", region_rows)
    above = geo[geo["ons_code"].str.startswith(ABOVE_REGION_PREFIXES)]
    if above["region_code"].notna().any():
        fail("a UK/GB/England row with a region_code", above)
    national = geo[(geo["ons_code"] == "[z]") & ~geo["ons_geography"].str.contains(" within ")]
    if national["region_code"].notna().any():
        fail("a national 'unknown'/'under disposal' row with a region_code", national)

    coded = geo[geo["ons_code"] != "[z]"]
    per_code = coded.groupby("ons_code")["region_code"].nunique(dropna=False)
    if (per_code > 1).any():
        fail("a geography placed in more than one region", coded[coded["ons_code"].isin(per_code[per_code > 1].index)])
    expected = coded["ons_code"].str[:3].map(EXPECTED_REGION_PREFIX)
    wrong = coded[expected.notna() & (coded["region_code"].fillna("").str[:3] != expected)]
    if not wrong.empty:
        fail("a local authority whose region does not match its code prefix", wrong)
    boroughs = coded[coded["ons_code"].str.startswith("E09")]
    if len(boroughs) != LONDON_BOROUGHS or (boroughs["region_code"] != LONDON).any():
        fail(f"{len(boroughs)} E09 codes, or some outside London; expected {LONDON_BOROUGHS} in {LONDON}", boroughs)

    # 'Local Authority unknown within North East' must sit in North East.
    named = geo[geo["ons_geography"].str.startswith("Local Authority unknown within ")]
    claimed = named["ons_geography"].str.removeprefix("Local Authority unknown within ")
    if (claimed != named["region_code"].map(region_names)).any():
        fail("an 'unknown within <region>' row placed in another region", named)
