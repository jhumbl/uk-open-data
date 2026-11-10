"""Station crosswalk for TfL station footfall: links each station label in
tfl_station_footfall (TfL's own strings, e.g. 'Kings Cross St Pancras',
'Woolwich EL', 'Edgware Road C&H') to TfL's StopPoint station records —
NaPTAN IDs, TfL hub IDs, modes, lines, coordinates — and to National Rail
CRS codes for joining ORR station usage. GitHub issue #1.

This table is CURATED BY THIS PROJECT, not published by TfL: TfL publishes
no link between its footfall labels and its station records. The mapping is
made in two steps, both visible in the output's `match` column:

1. automatic — the label (minus a TfL suffix: DLR, NR, EL, Elizabeth Line,
   LO, which restricts the mode) is normalised and matched against
   StopPoint station names (with and without bracketed parts). A label
   without a suffix prefers the Underground station, then TfL's other gated
   modes (DLR, Overground, Elizabeth line), then National Rail only; tram
   stops are never candidates (no gates). Exactly one candidate is
   required; anything else is left for an override.
2. override — overrides.csv (committed, reviewed, one line of reasoning per
   entry) settles the labels automatic matching cannot: TfL abbreviations,
   split stations, duplicate records. An override with no naptan_id links
   the label to no station (TfL's 'Unknown').

A label neither step resolves is published with match = 'unmatched' and
listed under unmatched_labels in catalog.json stats, with a warning — never
a quarantine, so a new station never blocks anything. (Only a large number
of unmatched labels, which means matching itself broke, fails validate().)

Inputs:
- TfL StopPoint API, /StopPoint/Mode/{mode} for the five TfL rail modes:
  published raw, as downloaded, in a deterministic zip. Its per-stop
  facility lists come back in varying order, so the raw zip changes most
  weeks even when nothing real has; the parquet does not.
- The footfall labels, from this run's tfl_station_footfall.parquet if it
  was produced (sources run in name order, so it runs first), otherwise the
  published copy on Hugging Face.
- CRS codes, from dft_naptan_rail_stations.parquet the same way: TfL's
  rail station IDs (910G...) are NaPTAN stop area codes.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import csv
import json
import re
import sys
import zipfile
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    categorical_value_sets,
    check_parquet_schema,
    download,
    hf_resolve_url,
    load_resource_schema,
    write_parquet,
)
from lib.tfl_network_demand import build_zip  # generic deterministic-zip recipe

STEM = "tfl_stoppoint_footfall_crosswalk"
ZIP_NAME = f"{STEM}_raw.zip"
PARQUET_NAME = f"{STEM}.parquet"
API_BASE = "https://api.tfl.gov.uk/StopPoint/Mode/"
MODES = ["tube", "dlr", "overground", "elizabeth-line", "tram"]
OVERRIDES_PATH = Path(__file__).parent / "overrides.csv"

FOOTFALL_FILE, FOOTFALL_FAMILY = "tfl_station_footfall.parquet", "tfl-network-demand"
NAPTAN_FILE, NAPTAN_FAMILY = "dft_naptan_rail_stations.parquet", "dft-naptan"

STATION_TYPES = {"NaptanMetroStation", "NaptanRailStation"}
HUB_TYPE = "TransportInterchange"
SUFFIX_MODES = {
    "DLR": {"dlr"},
    "NR": {"national-rail", "overground"},
    "EL": {"elizabeth-line"},
    "Elizabeth Line": {"elizabeth-line"},
    "LO": {"overground"},
}
SUFFIX_RE = re.compile(r"^(.*?)\s+(DLR|NR|EL|Elizabeth Line|LO)$")
# TfL's type suffixes on StopPoint names, stripped for display names.
NAME_SUFFIX_RE = re.compile(r"\s+(Underground Station|DLR Station|Rail Station|Tram Stop|Station)$")

MATCH_VALUES = ["automatic", "override", "unmatched"]
MAX_UNMATCHED = 25        # more than this means matching broke, not new stations
LONDON_BOX = (51.2, 51.8, -1.1, 0.4)   # lat min/max, lon min/max (incl. Reading)

FETCHED_FROM: dict[str, str] = {}


# --- inputs -----------------------------------------------------------------

def _published_or_local(out_dir: Path, filename: str, family: str, scratch: Path) -> Path:
    """This run's copy of one of our own published files if it exists, else
    the latest published copy from Hugging Face."""
    local = out_dir / filename
    if local.exists():
        return local
    url = hf_resolve_url(family, filename)
    print(f"  {filename} not produced this run — reading the published copy")
    return download(url, scratch / filename)


def footfall_labels(path: Path) -> list[str]:
    import pyarrow.parquet as pq

    labels = pq.read_table(path, columns=["station"]).column("station").to_pylist()
    return sorted({s for s in labels if s})


def naptan_crs(path: Path) -> dict[str, str]:
    """NaPTAN stop area code (910G...) -> CRS, active records only."""
    import pandas as pd

    df = pd.read_parquet(path, columns=["stop_area_code", "crs", "status"])
    df = df[(df["status"] == "active") & df["stop_area_code"].notna() & df["crs"].notna()]
    return dict(zip(df["stop_area_code"], df["crs"]))


def load_overrides(path: Path = OVERRIDES_PATH) -> dict[str, str | None]:
    """station label -> naptan_id (None = deliberately no station)."""
    with open(path, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows or set(rows[0]) != {"station", "naptan_id", "reason"}:
        raise ValueError(f"{path.name}: expected columns station,naptan_id,reason")
    out: dict[str, str | None] = {}
    for row in rows:
        label = row["station"].strip()
        if label in out:
            raise ValueError(f"{path.name}: duplicate override for {label!r}")
        if not row["reason"].strip():
            raise ValueError(f"{path.name}: override for {label!r} has no reason")
        out[label] = row["naptan_id"].strip() or None
    return out


# --- StopPoint index --------------------------------------------------------

def read_stoppoints(zip_path: Path) -> tuple[dict[str, dict], dict[str, dict]]:
    """(stations, hubs) from the raw zip. stations: naptan_id -> record with
    name, hub, modes, lines, lat, lon. hubs: hub id -> name + member station
    ids. Hub children count as stations too (that is how National Rail
    stations inside a TfL hub, e.g. Woolwich Arsenal, are reached)."""
    stations: dict[str, dict] = {}
    hubs: dict[str, dict] = {}

    def add_station(sp: dict) -> None:
        rec = stations.setdefault(sp["naptanId"], {
            "name": sp["commonName"], "hub": sp.get("hubNaptanCode"),
            "modes": set(), "lines": set(), "lat": sp.get("lat"), "lon": sp.get("lon"),
        })
        rec["modes"] |= set(sp.get("modes") or [])
        rec["lines"] |= {line["id"] for line in sp.get("lines") or []}
        rec["hub"] = rec["hub"] or sp.get("hubNaptanCode")

    with zipfile.ZipFile(zip_path) as zf:
        names = sorted(zf.namelist())
        if names != sorted(f"{m}.json" for m in MODES):
            raise ValueError(f"raw zip holds {names}, expected one JSON per mode {MODES}")
        for name in names:
            doc = json.loads(zf.read(name).decode("utf-8"))
            points = doc.get("stopPoints") if isinstance(doc, dict) else None
            if not isinstance(points, list) or not points:
                raise ValueError(f"{name}: no stopPoints list — the StopPoint API layout may have changed")
            if doc.get("total") not in (None, len(points)):
                raise ValueError(f"{name}: {len(points)} of {doc.get('total')} stop points — response is paged")
            for sp in points:
                if sp.get("stopType") in STATION_TYPES:
                    add_station(sp)
                elif sp.get("stopType") == HUB_TYPE:
                    hub = hubs.setdefault(sp["naptanId"], {"name": sp["commonName"], "members": set()})
                    for child in sp.get("children") or []:
                        if child.get("stopType") in STATION_TYPES:
                            hub["members"].add(child["naptanId"])
                            add_station({**child, "hubNaptanCode": sp["naptanId"]})
    return stations, hubs


def norm(name: str) -> str:
    """Comparison key for station names: case, punctuation, '&', a leading
    'London ' and TfL's type suffixes removed."""
    s = name.lower().replace("&", " and ").replace("'", "").replace(".", "")
    s = re.sub(r"\b(underground|dlr|rail|tram|elizabeth line|overground)?\s*(station|stop)\b", " ", s)
    s = re.sub(r"^london ", "", s.strip())
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def name_keys(name: str) -> set[str]:
    """Index keys for a StopPoint name: with and without its bracketed part,
    so 'Kensington (Olympia)' matches 'Kensington Olympia' and 'Cambridge
    Heath (London)' matches 'Cambridge Heath'."""
    return {norm(name), norm(re.sub(r"\(.*?\)", " ", name))}


def build_index(stations: dict[str, dict]) -> dict[str, list[str]]:
    index: dict[str, list[str]] = {}
    for sid in sorted(stations):
        for key in name_keys(stations[sid]["name"]):
            index.setdefault(key, []).append(sid)
    return index


TFL_RAIL_MODES = {"dlr", "overground", "elizabeth-line"}


def _preference(modes: set[str]) -> int | None:
    """Rank for a label with no suffix: Underground first, then TfL's other
    gated modes, then National Rail only. Tram-only stops have no gates, so
    they never carry footfall and are never candidates."""
    if "tube" in modes:
        return 0
    if modes & TFL_RAIL_MODES:
        return 1
    if "national-rail" in modes:
        return 2
    return None


def auto_match(label: str, stations: dict[str, dict], index: dict[str, list[str]]) -> str | None:
    """The single StopPoint station a label names, or None if not exactly one."""
    m = SUFFIX_RE.match(label)
    base, suffix = (m.group(1), m.group(2)) if m else (label, None)
    candidates = index.get(norm(base), [])
    if suffix:
        wanted = SUFFIX_MODES[suffix]
        candidates = [c for c in candidates if stations[c]["modes"] & wanted]
        if suffix in ("EL", "Elizabeth Line"):
            candidates = [c for c in candidates if c.startswith("910G")]
    else:
        ranked = [(r, c) for c in candidates if (r := _preference(stations[c]["modes"])) is not None]
        best = min((r for r, _ in ranked), default=None)
        candidates = [c for r, c in ranked if r == best]
    return candidates[0] if len(candidates) == 1 else None


def display_name(name: str) -> str:
    return NAME_SUFFIX_RE.sub("", name).strip()


# --- the table --------------------------------------------------------------

def build(zip_path: Path, labels: list[str], crs_by_area: dict[str, str],
          overrides: dict[str, str | None]):
    """One row per footfall label. Raises if an override names a station
    StopPoint doesn't have (a mistake to fix, not a new label)."""
    import pandas as pd

    stations, hubs = read_stoppoints(zip_path)
    unknown = sorted(i for i in overrides.values() if i and i not in stations)
    if unknown:
        raise ValueError(f"overrides.csv names station IDs StopPoint does not list: {unknown}")
    index = build_index(stations)

    rows = []
    for label in labels:
        if label in overrides:
            sid, match = overrides[label], "override"
        else:
            sid = auto_match(label, stations, index)
            match = "automatic" if sid else "unmatched"
        st = stations.get(sid) if sid else None
        hub_id = st["hub"] if st and st["hub"] in hubs else None
        members = sorted(hubs[hub_id]["members"]) if hub_id else ([sid] if sid else [])
        rows.append({
            "station": label,
            "naptan_id": sid,
            "naptan_name": st["name"] if st else None,
            "hub_naptan_id": hub_id,
            "station_group_id": hub_id or sid,
            "station_group_name": (hubs[hub_id]["name"] if hub_id else display_name(st["name"])) if st else None,
            "modes": sorted(st["modes"]) if st else [],
            "lines": sorted(st["lines"]) if st else [],
            "crs": sorted({crs_by_area[m] for m in members if m in crs_by_area}),
            "lat": round(float(st["lat"]), 6) if st and st["lat"] is not None else None,
            "lon": round(float(st["lon"]), 6) if st and st["lon"] is not None else None,
            "match": match,
        })
    return pd.DataFrame(rows)


def tidy_to_parquet(zip_path: Path, dest: Path, labels: list[str], crs_by_area: dict[str, str]) -> Path | None:
    try:
        df = build(zip_path, labels, crs_by_area, load_overrides())
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]].sort_values("station").reset_index(drop=True)
        df["lat"] = df["lat"].astype("float64")
        df["lon"] = df["lon"].astype("float64")
        unmatched = df.loc[df["match"] == "unmatched", "station"].tolist()
        if unmatched:
            print(f"  WARNING: {len(unmatched)} footfall label(s) not linked to a station "
                  f"(add to overrides.csv): {unmatched}")
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw StopPoint zip still published.")
        return None


def validate(files: list[Path]) -> None:
    import pandas as pd

    by_name = {p.name: p for p in files}
    zip_path = by_name.get(ZIP_NAME)
    if zip_path is None:
        raise ValueError(f"expected {ZIP_NAME} was not produced")
    size = zip_path.stat().st_size
    if not (200_000 < size < 50_000_000):
        raise ValueError(f"{ZIP_NAME} is {size:,} bytes — outside the plausible range (~2 MB)")
    stations, hubs = read_stoppoints(zip_path)  # raises on layout surprises
    if not (400 <= len(stations) <= 1_000):
        raise ValueError(f"{len(stations)} StopPoint stations — expected ~600")
    overrides = load_overrides()
    unknown = sorted(i for i in overrides.values() if i and i not in stations)
    if unknown:
        raise ValueError(f"overrides.csv names station IDs StopPoint does not list: {unknown}")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is None:
        return
    check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
    df = pd.read_parquet(parquet)
    if not (300 <= len(df) <= 1_000):
        raise ValueError(f"{len(df)} footfall labels — expected ~440")
    if df["station"].duplicated().any():
        raise ValueError("duplicate station labels")
    bad = set(df["match"]) - set(MATCH_VALUES)
    if bad:
        raise ValueError(f"unexpected match values {sorted(bad)}")
    linked = df["naptan_id"].notna()
    if (linked & (df["match"] == "unmatched")).any() or (~linked & (df["match"] == "automatic")).any():
        raise ValueError("match column inconsistent with naptan_id")
    if not df.loc[linked, "naptan_id"].isin(list(stations)).all():
        raise ValueError("a linked naptan_id is not a StopPoint station")
    unmatched = int((df["match"] == "unmatched").sum())
    if unmatched > MAX_UNMATCHED:
        raise ValueError(f"{unmatched} labels unmatched — automatic matching has broken "
                         "(StopPoint names or footfall labels changed format?)")
    lat_lo, lat_hi, lon_lo, lon_hi = LONDON_BOX
    located = df.dropna(subset=["lat", "lon"])
    if len(located) != linked.sum():
        raise ValueError("a linked station has no coordinates")
    if not (located["lat"].between(lat_lo, lat_hi).all() and located["lon"].between(lon_lo, lon_hi).all()):
        raise ValueError("a linked station lies outside the London area")


def stats(files: list[Path]) -> dict[str, dict]:
    import pandas as pd

    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    df = pd.read_parquet(parquet)
    labels = set(df["station"])
    return {parquet.name: {
        "row_count": len(df),
        "categorical_values": {
            **categorical_value_sets(parquet, ["match"]),
            "modes": sorted({m for ms in df["modes"] for m in ms}),
            "lines": sorted({line for ls in df["lines"] for line in ls}),
        },
        "unmatched_labels": sorted(df.loc[df["match"] == "unmatched", "station"]),
        "stale_overrides": sorted(set(load_overrides()) - labels),
    }}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch = out_dir / f"_{STEM}_inputs"
    scratch.mkdir(exist_ok=True)
    try:
        entries = []
        for mode in MODES:
            url = API_BASE + mode
            print(f"  Downloading {url}")
            entries.append((f"{mode}.json", download(url, scratch / f"{mode}.json")))
        zip_path = build_zip(entries, out_dir / ZIP_NAME)
        FETCHED_FROM[ZIP_NAME] = API_BASE + ",".join(MODES)

        labels = footfall_labels(_published_or_local(out_dir, FOOTFALL_FILE, FOOTFALL_FAMILY, scratch))
        crs_by_area = naptan_crs(_published_or_local(out_dir, NAPTAN_FILE, NAPTAN_FAMILY, scratch))
    finally:
        for p in scratch.iterdir():
            p.unlink()
        scratch.rmdir()

    produced = [zip_path]
    parquet = tidy_to_parquet(zip_path, out_dir / PARQUET_NAME, labels, crs_by_area)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = FETCHED_FROM[ZIP_NAME]
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
