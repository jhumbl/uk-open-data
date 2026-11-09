"""TfL public transport journeys by type of transport: millions of journeys
per four-week TfL reporting period by bus, Underground, DLR, tram,
Overground, cable car and Elizabeth line (labelled 'TfL Rail'), London-wide,
from April 2010. Published by TfL on the London Datastore:
https://data.london.gov.uk/dataset/public-transport-journeys-by-type-of-transport-ep8ow

The Datastore's per-resource download URLs embed a resource UUID that can
change when the file is replaced, so we resolve the current CSV through the
dataset API each run (same pattern as gla_public_realm_trees).

Raw layout: one row per period, columns 'Period and Financial year'
('07_26/27'), 'Reporting Period' (1-13), 'Days in period', 'Period
beginning'/'Period ending' ('dd-Mon-yy'), then one '<Mode> journeys (m)'
column per mode. Known quirk: the 2020/21 periods are labelled '_19/20', so
financial_year is derived from each period's start date, never the label.

We publish the CSV as downloaded, plus one long parquet: one row per period
per mode. The set of mode columns is a closed vocabulary — a new or renamed
column quarantines the run for a human to map it, rather than silently
dropping a mode.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.common import (
    categorical_value_sets,
    check_parquet_schema,
    download,
    get_page,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)

STEM = "tfl_journeys_by_mode"
CSV_NAME = f"{STEM}.csv"
PARQUET_NAME = f"{STEM}.parquet"
API_URL = "https://data.london.gov.uk/api/v2/dataset/public-transport-journeys-by-type-of-transport-ep8ow"

PERIOD_COLUMNS = ["Period and Financial year", "Reporting Period", "Days in period",
                  "Period beginning", "Period ending"]
# Raw column -> published mode name. Closed: validate() requires exactly these.
MODE_COLUMNS = {
    "Bus journeys (m)": "Bus",
    "Underground journeys (m)": "Underground",
    "DLR Journeys (m)": "DLR",
    "Tram Journeys (m)": "Tram",
    "Overground Journeys (m)": "Overground",
    "London Cable Car Journeys (m)": "London Cable Car",
    "TfL Rail Journeys (m)": "Elizabeth line",
}
CATEGORICAL_COLUMNS = ["mode"]

FETCHED_FROM: dict[str, str] = {}


def find_csv_url() -> str:
    """The current CSV resource's download URL (newest check_timestamp)."""
    api = json.loads(get_page(API_URL))
    csvs = [r for r in (api.get("resources") or {}).values()
            if str(r.get("format", "")).lower() == "csv" and r.get("url")]
    if not csvs:
        raise RuntimeError(f"No CSV resource at {API_URL} — the Datastore API layout may have changed.")
    return max(csvs, key=lambda r: str(r.get("check_timestamp") or ""))["url"]


def _iso(value: str) -> str:
    return datetime.strptime(value.strip(), "%d-%b-%y").date().isoformat()


def parse(csv_path: Path):
    """The long table. Raises on layout surprises; shared by tidy and validate."""
    import pandas as pd

    raw = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    raw.columns = [c.strip() for c in raw.columns]
    expected = PERIOD_COLUMNS + list(MODE_COLUMNS)
    if list(raw.columns) != expected:
        raise ValueError(
            f"columns changed: new {sorted(set(raw.columns) - set(expected))}, "
            f"missing {sorted(set(expected) - set(raw.columns))} (or reordered) — "
            "map any new mode column in MODE_COLUMNS"
        )

    rows = []
    for rec in raw.to_dict("records"):
        start, end = _iso(rec["Period beginning"]), _iso(rec["Period ending"])
        start_d = date.fromisoformat(start)
        fy = start_d.year - (start_d.month < 4)  # label is wrong for 2020/21
        base = {
            "financial_year": f"{fy}-{str(fy + 1)[2:]}",
            "period": int(rec["Reporting Period"]),
            "period_start": start,
            "period_end": end,
            "days_in_period": int(rec["Days in period"]),
            "tfl_period_label": rec["Period and Financial year"].strip(),
        }
        for col, mode in MODE_COLUMNS.items():
            cell = rec[col].strip()
            rows.append({**base, "mode": mode, "journeys_millions": float(cell) if cell else None})
    df = pd.DataFrame(rows)
    return df.sort_values(["period_start", "mode"]).reset_index(drop=True)


def tidy_to_parquet(csv_path: Path, dest: Path) -> Path | None:
    try:
        df = parse(csv_path)
        fields = load_resource_schema(Path(__file__).parent, PARQUET_NAME)
        df = df[[f["name"] for f in fields]]
        df["journeys_millions"] = df["journeys_millions"].astype("float64")
        return write_parquet(df, dest)
    except Exception as exc:  # noqa: BLE001 — this step must never kill the run
        print(f"  WARNING: tidy step skipped ({exc}). Raw CSV still published.")
        return None


def validate(files: list[Path]) -> None:
    import pandas as pd

    by_name = {p.name: p for p in files}
    csv = by_name.get(CSV_NAME)
    if csv is None:
        raise ValueError(f"expected {CSV_NAME} was not produced")
    size = csv.stat().st_size
    if not (5_000 < size < 2_000_000):
        raise ValueError(f"{CSV_NAME} is {size:,} bytes — outside the plausible range (~15 KB)")

    df = parse(csv)  # raises on column / date-format changes
    periods = df.drop_duplicates("period_start").sort_values("period_start")
    if periods["period_start"].iloc[0] != "2010-04-01":
        raise ValueError(f"series now starts {periods['period_start'].iloc[0]}, expected 2010-04-01")
    gaps = (pd.to_datetime(periods["period_start"]).iloc[1:].values
            - pd.to_datetime(periods["period_end"]).iloc[:-1].values)
    if (gaps != pd.Timedelta(days=1)).any():
        raise ValueError("periods are no longer contiguous (gap or overlap between consecutive periods)")
    if not periods["period"].between(1, 13).all():
        raise ValueError("a reporting period number is outside 1..13")
    if df.duplicated(["period_start", "mode"]).any():
        raise ValueError("duplicate period-mode rows")
    if (df["journeys_millions"] < 0).any():
        raise ValueError("negative journey counts")
    bus = df.loc[df["mode"] == "Bus", "journeys_millions"]
    if not (50 < bus.median() < 400):
        raise ValueError(f"median bus journeys per period is {bus.median()}m — expected ~150m (units changed?)")
    latest_end = date.fromisoformat(periods["period_end"].iloc[-1])
    if (date.today() - latest_end).days > 150:
        raise ValueError(f"latest period ends {latest_end} — TfL appears to have stopped updating")

    parquet = by_name.get(PARQUET_NAME)
    if parquet is not None:
        check_parquet_schema(parquet, load_resource_schema(Path(__file__).parent, PARQUET_NAME))
        published = pd.read_parquet(parquet)
        if len(published) != len(df):
            raise ValueError(f"{PARQUET_NAME} has {len(published)} rows; the CSV gives {len(df)}")
        if set(published["mode"]) != set(MODE_COLUMNS.values()):
            raise ValueError(f"unexpected mode values {sorted(set(published['mode']))}")


def stats(files: list[Path]) -> dict[str, dict]:
    parquet = next((p for p in files if p.name == PARQUET_NAME), None)
    if parquet is None:
        return {}
    file_stats = parquet_stats(parquet, "period_start")
    file_stats["categorical_values"] = categorical_value_sets(parquet, CATEGORICAL_COLUMNS)
    return {parquet.name: file_stats}


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    url = find_csv_url()
    print(f"  Downloading {url}")
    csv_path = download(url, out_dir / CSV_NAME)
    FETCHED_FROM[CSV_NAME] = url

    produced = [csv_path]
    parquet = tidy_to_parquet(csv_path, out_dir / PARQUET_NAME)
    if parquet is not None:
        produced.append(parquet)
        FETCHED_FROM[PARQUET_NAME] = url
    return produced


if __name__ == "__main__":
    for f in fetch(Path("data")):
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
