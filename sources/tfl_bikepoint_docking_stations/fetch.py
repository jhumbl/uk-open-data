"""TfL BikePoint: every Santander Cycles docking station in London, from
TfL's Unified API (https://api.tfl.gov.uk/BikePoint), one call, no key.

The API returns current state only. When TfL removes, renames or moves a
docking station it simply vanishes from (or changes in) the list — there is
no public history, so the weekly copies and monthly archive tags of this
source are the record. (An analysis that matches old journey files to
stations needs exactly that history: TfL has also reused old BikePoint IDs
for new stations elsewhere.)

We publish:

- tfl_bikepoint_docking_stations_raw.zip   the API response exactly as
        returned (BikePoint.json), in a deterministic zip. It includes the
        live bike/empty-dock counts and per-property `modified` timestamps,
        so it changes on every fetch.
- tfl_bikepoint_docking_stations.parquet   one row per docking station:
        the INVENTORY only (id, terminal, name, position, flags, install/
        removal dates, total docks). The live availability counts are
        deliberately left out — a single reading at fetch time each week is
        not a meaningful series — so this file changes only when the network
        itself changes, and a 'changed' parquet means something.

Station names are kept verbatim (an open vocabulary; TfL's names include
stray spaces before commas, e.g. 'River Street , Clerkenwell', and the same
spelling appears in TfL's cycle hire journey files, so normalising would
break matching).

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
from datetime import datetime, timezone
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    categorical_value_sets,
    check_parquet_schema,
    download,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)
from lib.tfl_network_demand import build_zip  # generic deterministic-zip recipe

STEM = "tfl_bikepoint_docking_stations"
ZIP_NAME = f"{STEM}_raw.zip"
PARQUET_NAME = f"{STEM}.parquet"
API_URL = "https://api.tfl.gov.uk/BikePoint"
RAW_ARCNAME = "BikePoint.json"

ID_PATTERN = re.compile(r"^BikePoints_(\d+)$")
FLAG_KEYS = {"Installed": "installed", "Locked": "locked", "Temporary": "temporary"}
REQUIRED_PROPERTIES = {"TerminalName", "InstallDate", "RemovalDate", "NbDocks", *FLAG_KEYS}
# Generous box around Greater London; a point outside it means bad data.
LAT_RANGE = (51.25, 51.75)
LON_RANGE = (-0.55, 0.35)

FETCHED_FROM: dict[str, str] = {}


def _properties(place: dict) -> dict[str, str]:
    return {p["key"]: p["value"] for p in place.get("additionalProperties", [])}


def _flag(value: str, key: str, place_id: str) -> bool:
    """TfL sends booleans as the strings 'true'/'false' (closed vocabulary).
    Anything else is a format change: raise rather than guess."""
    v = value.strip().lower()
    if v not in ("true", "false"):
        raise ValueError(f"{place_id}: {key}={value!r}, expected 'true' or 'false'")
    return v == "true"


def _epoch_ms_to_iso(value: str, key: str, place_id: str) -> str | None:
    """InstallDate/RemovalDate are epoch milliseconds as strings, or empty."""
    v = value.strip()
    if not v:
        return None
    if not v.isdigit():
        raise ValueError(f"{place_id}: {key}={value!r} is not epoch milliseconds")
    return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_raw(zip_path: Path) -> list[dict]:
    with zipfile.ZipFile(zip_path) as zf:
        return json.loads(zf.read(RAW_ARCNAME).decode("utf-8"))


def tidy_to_parquet(zip_path: Path, dest: Path) -> Path | None:
    """Best-effort: one row per docking station, inventory columns only,
    sorted by the numeric part of the BikePoint id."""
    try:
        import pandas as pd

        rows = []
        for place in _read_raw(zip_path):
            pid = place["id"]
            props = _properties(place)
            rows.append(
                {
                    "bikepoint_id": pid,
                    "terminal_name": props["TerminalName"].strip(),
                    "name": place["commonName"],
                    "lat": float(place["lat"]),
                    "lon": float(place["lon"]),
                    "nb_docks": int(props["NbDocks"]),
                    **{col: _flag(props[key], key, pid) for key, col in FLAG_KEYS.items()},
                    "install_date": _epoch_ms_to_iso(props["InstallDate"], "InstallDate", pid),
                    "removal_date": _epoch_ms_to_iso(props["RemovalDate"], "RemovalDate", pid),
                }
            )
        rows.sort(key=lambda r: int(ID_PATTERN.match(r["bikepoint_id"]).group(1)))
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = pd.DataFrame(rows, columns=[f["name"] for f in fields])
        df["nb_docks"] = df["nb_docks"].astype("int64")
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw zip still published.")
        return None


def validate(files: list[Path]) -> None:
    """Shape checks; raising quarantines this run's files."""
    by_name = {p.name: p for p in files}
    zip_path = by_name.get(ZIP_NAME)
    if zip_path is None:
        raise ValueError(f"expected raw bundle {ZIP_NAME} was not produced")
    size = zip_path.stat().st_size
    if not (30_000 < size < 20_000_000):
        raise ValueError(f"{ZIP_NAME} is {size:,} bytes — outside the plausible range (~150 KB)")

    places = _read_raw(zip_path)
    if not isinstance(places, list) or not (500 <= len(places) <= 1500):
        n = len(places) if isinstance(places, list) else type(places).__name__
        raise ValueError(f"API returned {n} docking stations — expected ~800")

    ids = set()
    for place in places:
        pid = place.get("id", "")
        if not ID_PATTERN.match(pid) or place.get("placeType") != "BikePoint":
            raise ValueError(f"unexpected record: id={pid!r} placeType={place.get('placeType')!r}")
        if pid in ids:
            raise ValueError(f"duplicate BikePoint id {pid}")
        ids.add(pid)
        props = _properties(place)
        missing = REQUIRED_PROPERTIES - props.keys()
        if missing:
            raise ValueError(f"{pid} is missing properties {sorted(missing)}")
        # Closed vocabularies / formats: assert on the raw data, so a change
        # quarantines for a human instead of silently skipping the parquet.
        for key in FLAG_KEYS:
            _flag(props[key], key, pid)
        for key in ("InstallDate", "RemovalDate"):
            _epoch_ms_to_iso(props[key], key, pid)
        if not props["NbDocks"].strip().isdigit():
            raise ValueError(f"{pid}: NbDocks={props['NbDocks']!r} is not a whole number")
        if not (LAT_RANGE[0] < place["lat"] < LAT_RANGE[1] and LON_RANGE[0] < place["lon"] < LON_RANGE[1]):
            raise ValueError(f"{pid} at ({place['lat']}, {place['lon']}) is outside London")
        if not str(place.get("commonName", "")).strip():
            raise ValueError(f"{pid} has no name")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        import pandas as pd

        check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
        df = pd.read_parquet(parquet)
        if len(df) != len(places):
            raise ValueError(f"{PARQUET_NAME} has {len(df)} rows; the API returned {len(places)}")
        if (df["nb_docks"] < 0).any() or df["nb_docks"].max() > 200:
            raise ValueError("nb_docks outside 0..200 — TfL may have changed the field's meaning")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    file_stats = parquet_stats(parquet, "install_date")
    file_stats["categorical_values"] = categorical_value_sets(parquet, list(FLAG_KEYS.values()))
    return {parquet.name: file_stats}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"  Downloading {API_URL}")
    raw = download(API_URL, out_dir / f"_download_{RAW_ARCNAME}")
    zip_path = build_zip([(RAW_ARCNAME, raw)], out_dir / ZIP_NAME)
    raw.unlink()
    FETCHED_FROM[ZIP_NAME] = API_URL

    produced = [zip_path]
    parquet = tidy_to_parquet(zip_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = API_URL
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
