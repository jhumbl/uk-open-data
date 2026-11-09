"""NaPTAN rail stations: every National Rail station record in the
National Public Transport Access Nodes dataset (DfT), with its CRS
(three-letter) code, TIPLOC, NaPTAN stop area and location.
https://www.data.gov.uk/dataset/ff93ffc1-6656-47d8-9155-85ea0b8f2251/naptan

NaPTAN groups stops by ATCO area; area 910 is the national rail network
(Great Britain). The API's CSV output omits the rail references (CRS,
TIPLOC), so we download the XML, which carries them in AnnotatedRailRef.

Raw layout: <NaPTAN><StopPoints> one <StopPoint> per station record (an
'access area'; large stations have several, one per TIPLOC, sharing a CRS),
then <StopAreas> (one GRLS group per station). Locations are OS National
Grid easting/northing; NaPTAN also gives WGS84 latitude/longitude for most
records but omits it for a few (e.g. the central Elizabeth line stations).
Where it is missing we convert easting/northing ourselves (OSGB36 -> WGS84,
Helmert, ~5 m) and flag the row — validate() checks that conversion against
every record where NaPTAN does give both.

We publish the XML as downloaded, plus one parquet: one row per StopPoint.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import math
import re
import sys
import xml.etree.ElementTree as ET
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

STEM = "dft_naptan_rail_stations"
XML_NAME = f"{STEM}.xml"
PARQUET_NAME = f"{STEM}.parquet"
API_URL = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=xml&atcoAreaCodes=910"
NS = {"n": "http://www.naptan.org.uk/"}
ATCO_RE = re.compile(r"^9100[A-Z0-9]+$")

# Closed vocabularies (NaPTAN codes). A new value quarantines for a human.
# RLY = rail station access area, RPL = rail platform, BCQ = bus/coach stop
# recorded under the rail area (e.g. rail-replacement stops).
STOP_TYPES = ["RLY", "RPL", "BCQ"]
STATUSES = ["active", "inactive"]
CATEGORICAL_COLUMNS = ["stop_type", "status"]

# Greater-than-GB bounding boxes, for sanity checks.
EN_BOX = (0, 700_000, 0, 1_300_000)          # easting min/max, northing min/max
LATLON_BOX = (49.0, 61.0, -9.0, 2.0)          # lat min/max, lon min/max
MAX_CONVERSION_ERROR_M = 2.0   # 99th-percentile disagreement allowed (measured ~0.2 m median)

FETCHED_FROM: dict[str, str] = {}


def osgb_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """OS National Grid easting/northing -> WGS84 (lat, lon), via inverse
    Transverse Mercator on Airy 1830 and a 7-parameter Helmert transform.
    Accurate to ~5 m (Ordnance Survey, 'A guide to coordinate systems in
    Great Britain'); stdlib only."""
    a, b = 6377563.396, 6356256.909                 # Airy 1830
    f0, lat0, lon0 = 0.9996012717, math.radians(49), math.radians(-2)
    n0, e0 = -100_000.0, 400_000.0
    e2 = 1 - (b * b) / (a * a)
    n = (a - b) / (a + b)

    def meridional_arc(lat: float) -> float:
        dl, sl = lat - lat0, lat + lat0
        return b * f0 * (
            (1 + n + 5 / 4 * n**2 + 5 / 4 * n**3) * dl
            - (3 * n + 3 * n**2 + 21 / 8 * n**3) * math.sin(dl) * math.cos(sl)
            + (15 / 8 * n**2 + 15 / 8 * n**3) * math.sin(2 * dl) * math.cos(2 * sl)
            - 35 / 24 * n**3 * math.sin(3 * dl) * math.cos(3 * sl)
        )

    lat, m = lat0, 0.0
    while True:
        lat = (northing - n0 - m) / (a * f0) + lat
        m = meridional_arc(lat)
        if abs(northing - n0 - m) < 1e-5:
            break
    s, c, t = math.sin(lat), math.cos(lat), math.tan(lat)
    nu = a * f0 / math.sqrt(1 - e2 * s * s)
    rho = a * f0 * (1 - e2) / (1 - e2 * s * s) ** 1.5
    eta2 = nu / rho - 1
    de = easting - e0
    vii = t / (2 * rho * nu)
    viii = t / (24 * rho * nu**3) * (5 + 3 * t * t + eta2 - 9 * t * t * eta2)
    ix = t / (720 * rho * nu**5) * (61 + 90 * t * t + 45 * t**4)
    x = 1 / (c * nu)
    xi = 1 / (c * 6 * nu**3) * (nu / rho + 2 * t * t)
    xii = 1 / (c * 120 * nu**5) * (5 + 28 * t * t + 24 * t**4)
    xiia = 1 / (c * 5040 * nu**7) * (61 + 662 * t * t + 1320 * t**4 + 720 * t**6)
    lat_osgb = lat - vii * de**2 + viii * de**4 - ix * de**6
    lon_osgb = lon0 + x * de - xi * de**3 + xii * de**5 - xiia * de**7

    # OSGB36 geodetic -> cartesian (Airy), Helmert to WGS84, -> geodetic (GRS80).
    s, c = math.sin(lat_osgb), math.cos(lat_osgb)
    nu = a / math.sqrt(1 - e2 * s * s)
    x1, y1, z1 = nu * c * math.cos(lon_osgb), nu * c * math.sin(lon_osgb), (1 - e2) * nu * s
    tx, ty, tz = 446.448, -125.157, 542.060
    sc = -20.4894e-6
    rx, ry, rz = (math.radians(v / 3600) for v in (0.1502, 0.2470, 0.8421))
    x2 = tx + (1 + sc) * x1 - rz * y1 + ry * z1
    y2 = ty + rz * x1 + (1 + sc) * y1 - rx * z1
    z2 = tz - ry * x1 + rx * y1 + (1 + sc) * z1
    a2, b2 = 6378137.0, 6356752.3141                 # GRS80 / WGS84
    e2w = 1 - (b2 * b2) / (a2 * a2)
    p = math.hypot(x2, y2)
    lat = math.atan2(z2, p * (1 - e2w))
    for _ in range(10):
        nu = a2 / math.sqrt(1 - e2w * math.sin(lat) ** 2)
        lat = math.atan2(z2 + e2w * nu * math.sin(lat), p)
    return math.degrees(lat), math.degrees(math.atan2(y2, x2))


def _text(el, path: str) -> str | None:
    value = el.findtext(path, namespaces=NS)
    value = value.strip() if value is not None else None
    return value or None


def _number(el, path: str) -> float | None:
    value = _text(el, path)
    return float(value) if value is not None else None


def parse(xml_path: Path):
    """The StopPoint table (raw NaPTAN values, before canonicalisation).
    Raises on layout surprises; shared by tidy and validate."""
    import pandas as pd

    root = ET.parse(xml_path).getroot()
    if root.tag != f"{{{NS['n']}}}NaPTAN":
        raise ValueError(f"root element is {root.tag}, expected NaPTAN")
    points = root.find("n:StopPoints", NS)
    if points is None:
        raise ValueError("no <StopPoints> element")

    rows = []
    for sp in points:
        atco = _text(sp, "n:AtcoCode")
        rail_refs = sp.findall("n:StopClassification/n:OffStreet/n:Rail/n:AnnotatedRailRef", NS)
        if len(rail_refs) > 1:
            raise ValueError(f"{atco}: {len(rail_refs)} AnnotatedRailRefs — expected at most one")
        rail = rail_refs[0] if rail_refs else None
        areas = [a.text.strip() for a in sp.findall("n:StopAreas/n:StopAreaRef", NS) if a.text]
        if len(areas) > 1:
            raise ValueError(f"{atco}: in {len(areas)} stop areas — expected at most one")
        loc = sp.find("n:Place/n:Location", NS)
        if loc is None:
            raise ValueError(f"{atco}: no <Location>")
        # Two published shapes: <Translation> with E/N and lat/lon, or bare E/N.
        tr = loc.find("n:Translation", NS)
        src = tr if tr is not None else loc
        rows.append({
            "atco_code": atco,
            "common_name": _text(sp, "n:Descriptor/n:CommonName"),
            "stop_type": _text(sp, "n:StopClassification/n:StopType"),
            "status": sp.get("Status"),
            "crs": _text(rail, "n:CrsRef") if rail is not None else None,
            "tiploc": _text(rail, "n:TiplocRef") if rail is not None else None,
            "station_name": _text(rail, "n:StationName") if rail is not None else None,
            "stop_area_code": areas[0] if areas else None,
            "easting": _number(src, "n:Easting"),
            "northing": _number(src, "n:Northing"),
            "naptan_lat": _number(src, "n:Latitude"),
            "naptan_lon": _number(src, "n:Longitude"),
            "locality_code": _text(sp, "n:Place/n:NptgLocalityRef"),
            "admin_area_code": _text(sp, "n:AdministrativeAreaRef"),
            "modified": sp.get("ModificationDateTime"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("no StopPoints")
    if df[["easting", "northing"]].isna().any().any():
        bad = df.loc[df[["easting", "northing"]].isna().any(axis=1), "atco_code"].tolist()
        raise ValueError(f"records without easting/northing: {bad[:10]}")
    return df.sort_values("atco_code").reset_index(drop=True)


def with_coordinates(df):
    """Add lat/lon: NaPTAN's own where given, else converted from E/N (flagged)."""
    df = df.copy()
    df["coords_derived"] = df["naptan_lat"].isna() | df["naptan_lon"].isna()
    converted = [osgb_to_wgs84(e, n) if d else (None, None)
                 for e, n, d in zip(df["easting"], df["northing"], df["coords_derived"])]
    df["lat"] = [c[0] if d else v for c, d, v in zip(converted, df["coords_derived"], df["naptan_lat"])]
    df["lon"] = [c[1] if d else v for c, d, v in zip(converted, df["coords_derived"], df["naptan_lon"])]
    return df


def tidy_to_parquet(xml_path: Path, dest: Path) -> Path | None:
    try:
        df = with_coordinates(parse(xml_path))
        df["stop_type"] = canonicalize_categorical(df["stop_type"], STOP_TYPES)
        df["status"] = canonicalize_categorical(df["status"], STATUSES)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["easting"] = df["easting"].round().astype("int64")
        df["northing"] = df["northing"].round().astype("int64")
        df["lat"] = df["lat"].astype("float64").round(6)
        df["lon"] = df["lon"].astype("float64").round(6)
        df["coords_derived"] = df["coords_derived"].astype(bool)
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw XML still published.")
        return None


def validate(files: list[Path]) -> None:
    import pandas as pd

    by_name = {p.name: p for p in files}
    xml = by_name.get(XML_NAME)
    if xml is None:
        raise ValueError(f"expected {XML_NAME} was not produced")
    size = xml.stat().st_size
    if not (5_000_000 < size < 200_000_000):
        raise ValueError(f"{XML_NAME} is {size:,} bytes — outside the plausible range (~27 MB)")
    if b"<NaPTAN" not in xml.read_bytes()[:1000]:
        raise ValueError(f"{XML_NAME} does not start like a NaPTAN XML document")

    df = parse(xml)  # raises on layout surprises
    if not (2_000 <= len(df) <= 5_000):
        raise ValueError(f"{len(df)} stop points — expected ~2,800")
    if df["atco_code"].isna().any() or df["atco_code"].duplicated().any():
        raise ValueError("missing or duplicate ATCO codes")
    bad_atco = [a for a in df["atco_code"] if not ATCO_RE.match(a)]
    if bad_atco:
        raise ValueError(f"ATCO codes outside area 910: {bad_atco[:10]}")
    for col, allowed in (("stop_type", STOP_TYPES), ("status", STATUSES)):
        df[col] = canonicalize_categorical(df[col], allowed)  # re-casing is absorbed; new codes are not
        bad = set(df[col].dropna()) - set(allowed)
        if bad or df[col].isna().any():
            raise ValueError(f"unexpected {col} values {sorted(bad)} (or nulls) — new NaPTAN code?")
    active_crs = df.loc[(df["status"] == "active") & df["crs"].notna(), "crs"].nunique()
    if not (2_400 <= active_crs <= 3_500):
        raise ValueError(f"{active_crs} distinct active CRS codes — expected ~2,600")
    bad_crs = [c for c in df["crs"].dropna() if not re.fullmatch(r"[A-Z0-9]{3}", c)]
    if bad_crs:
        raise ValueError(f"malformed CRS codes {bad_crs[:10]}")
    e_lo, e_hi, n_lo, n_hi = EN_BOX
    if not (df["easting"].between(e_lo, e_hi).all() and df["northing"].between(n_lo, n_hi).all()):
        raise ValueError("easting/northing outside the National Grid")

    # The conversion used to fill missing lat/lon must agree with NaPTAN's own.
    # Judged on the 99th percentile, not the max: a handful of NaPTAN records
    # have E/N and lat/lon that disagree with each other (Horden by ~100 m in
    # Nov 2026) — publisher data-entry, not a conversion error.
    both = df.dropna(subset=["naptan_lat", "naptan_lon"])
    errors = sorted(
        math.hypot((clat - lat) * 111_200, (clon - lon) * 111_200 * math.cos(math.radians(lat)))
        for (clat, clon), lat, lon in zip(
            (osgb_to_wgs84(e, n) for e, n in zip(both["easting"], both["northing"])),
            both["naptan_lat"], both["naptan_lon"])
    )
    p99 = errors[int(0.99 * (len(errors) - 1))]
    if p99 > MAX_CONVERSION_ERROR_M:
        raise ValueError(f"E/N -> lat/lon conversion differs from NaPTAN's by {p99:.1f} m at the 99th percentile")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
        published = pd.read_parquet(parquet)
        if len(published) != len(df):
            raise ValueError(f"{PARQUET_NAME} has {len(published)} rows; the XML gives {len(df)}")
        lat_lo, lat_hi, lon_lo, lon_hi = LATLON_BOX
        if not (published["lat"].between(lat_lo, lat_hi).all() and published["lon"].between(lon_lo, lon_hi).all()):
            raise ValueError("a station lies outside Great Britain (or has no coordinates)")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    file_stats = parquet_stats(parquet, "modified")
    file_stats["categorical_values"] = categorical_value_sets(parquet, CATEGORICAL_COLUMNS)
    return {parquet.name: file_stats}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    print(f"  Downloading {API_URL}")
    xml_path = download(API_URL, out_dir / XML_NAME)
    FETCHED_FROM[XML_NAME] = API_URL

    produced = [xml_path]
    parquet = tidy_to_parquet(xml_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = API_URL
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
