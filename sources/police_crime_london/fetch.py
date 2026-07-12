"""data.police.uk: street-level crime, outcomes and stop & search recorded by
London's two territorial forces (Metropolitan Police Service and City of
London Police), December 2010 to present.

Upstream publishes a ~1.6 GB zip every month at a permanent dated URL
(https://data.police.uk/data/archive/YYYY-MM.zip), each containing per-month,
per-force CSVs for the trailing ~36 months (the oldest zips reach further
back: 2013-12 spans Dec 2010-Dec 2013 and 2016-12 spans Dec 2010-Dec 2016).
Months are revised by forces until they roll out of the newest zip's window,
so full history is reconstructed by "tiling": a fixed covering set of zips
three years apart plus the newest zip, each contributing only the months
after the previous zip's last month, so every month comes from the newest
zip that contains it (the most-revised version).

Two deliberate deviations from the usual source pattern, both because the
inputs total ~9 GB:

* No raw zip is republished. data.police.uk itself retains every monthly
  zip forever at a stable dated URL — better provenance than a copy. The
  newest zip consumed is recorded in FETCHED_FROM.
* London-filtered per-zip intermediates are cached under the gitignored
  repo-root cache/ directory (NEVER under data/, whose contents run_all.py
  publishes), keyed by zip month + the MD5 the archive page
  publishes for it. The covering zips are immutable, so in steady state a
  run downloads nothing (all cached) or just the newest zip (~monthly).
  The cache is purely a performance optimisation: a cache miss re-downloads
  and re-derives byte-identical output.

Contract (same for every source): expose

    fetch(out_dir: Path) -> list[Path]

which writes one or more files into out_dir and returns their paths.
Raise on failure — run_all.py handles logging and keeps other sources going.
Optional extras used here: validate(files), stats(files) and FETCHED_FROM.
The three tables' schemas are declared in datapackage.json.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import sys
import zipfile
from collections import namedtuple
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

# Allow "python sources/<name>/fetch.py" to find lib/ when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from lib.common import (
    REPO_ROOT,
    download,
    get_page,
    load_resource_schema,
    parquet_stats,
    write_parquet,
)

STEM = "police_crime_london"
CRIMES_NAME = f"{STEM}.parquet"
OUTCOMES_NAME = f"{STEM}_outcomes.parquet"
STOP_SEARCH_NAME = f"{STEM}_stop_search.parquet"

SOURCE_DIR = Path(__file__).parent
ARCHIVE_PAGE = "https://data.police.uk/data/archive/"

FIRST_MONTH = "2010-12"  # start of published upstream history
COVERING_FIRST = "2013-12"  # oldest dated zip on the archive page
COVERING_STEP_MONTHS = 36  # covering zips are three years apart

# dataset key -> (zip member-name suffix, published filename, period column).
# Dict order is publication order (crimes first — the primary resource).
DATASETS: dict[str, tuple[str, str, str]] = {
    "street": ("street", CRIMES_NAME, "month"),
    "outcomes": ("outcomes", OUTCOMES_NAME, "month"),
    "stop_search": ("stop-and-search", STOP_SEARCH_NAME, "date"),
}

# Bump when the intermediate-building logic changes (normalisation,
# filtering): every cache key misses and the next run rebuilds from fresh
# downloads. A cache miss is only a performance cost, never a correctness one.
CACHE_VERSION = 1
CACHE_DIR = REPO_ROOT / "cache" / STEM

# Dated links on the archive page are relative: href="/data/archive/2026-05.zip".
# The page shows a bare 32-hex MD5 in the text shortly after each link.
ZIP_LINK_RE = re.compile(r'href="(/data/archive/(\d{4}-\d{2})\.zip)"')
MD5_RE = re.compile(r"\b([0-9a-f]{32})\b")

# Zip members look like "2026-05/2026-05-metropolitan-street.csv".
MEMBER_RE = re.compile(
    r"^(\d{4}-\d{2})/\1-(city-of-london|metropolitan)-"
    r"(street|outcomes|stop-and-search)\.csv$"
)

# Resolved download URL per published file, read by run_all.py after fetch()
# and recorded in catalog.json as `fetched_from`. All three tables are built
# from the same set of zips; the newest zip is what a run actually reacts
# to, so that is the URL recorded.
FETCHED_FROM: dict[str, str] = {}


# One upstream zip and the inclusive month range it contributes. A plain
# namedtuple, not a dataclass: run_all.py's module loader doesn't register
# sources in sys.modules, and dataclass processing needs the owning module
# to be findable there. Fields: month (the zip's own YYYY-MM label), url,
# md5 (from the archive page), first/last (months this zip contributes;
# last == month).
ZipJob = namedtuple("ZipJob", ["month", "url", "md5", "first", "last"])


# ---- month arithmetic ------------------------------------------------------
# "YYYY-MM" strings compare lexicographically in temporal order, so range
# checks are plain string comparisons; only stepping needs arithmetic.


def _month_index(month: str) -> int:
    year, mon = month.split("-")
    return int(year) * 12 + int(mon) - 1


def _add_months(month: str, n: int) -> str:
    i = _month_index(month) + n
    return f"{i // 12:04d}-{i % 12 + 1:02d}"


def _months_between(first: str, last: str) -> list[str]:
    return [
        _add_months(first, n)
        for n in range(_month_index(last) - _month_index(first) + 1)
    ]


# ---- planning: what zips to use and which months each contributes ----------


def parse_archive_page(html: str) -> dict[str, tuple[str, str]]:
    """Map month -> (absolute zip URL, MD5) from the archive page HTML."""
    matches = list(ZIP_LINK_RE.finditer(html))
    if not matches:
        raise RuntimeError(
            f"No dated zip links found on {ARCHIVE_PAGE} — the page layout "
            "may have changed; check ZIP_LINK_RE in fetch.py."
        )
    archive: dict[str, tuple[str, str]] = {}
    for i, match in enumerate(matches):
        month = match.group(2)
        if month in archive:
            raise RuntimeError(
                f"Archive page lists {month}.zip twice — page layout may "
                "have changed; check ZIP_LINK_RE in fetch.py."
            )
        # The MD5 belongs to the text between this link and the next one.
        end = matches[i + 1].start() if i + 1 < len(matches) else len(html)
        md5 = MD5_RE.search(html, match.end(), end)
        if md5 is None:
            raise RuntimeError(
                f"No MD5 checksum found next to {month}.zip on the archive "
                "page — page layout may have changed; check MD5_RE in fetch.py."
            )
        archive[month] = (urljoin(ARCHIVE_PAGE, match.group(1)), md5.group(1))
    return archive


def plan_zips(archive: dict[str, tuple[str, str]]) -> list[ZipJob]:
    """Pick the covering zips + the newest zip and assign each its months.

    The covering months are computed (COVERING_FIRST + multiples of
    COVERING_STEP_MONTHS) rather than hardcoded so the set automatically
    gains 2028-12 when the newest zip's window no longer reaches back to
    2025-12 — a hardcoded list would develop a coverage gap in early 2029.
    """
    newest = max(archive)
    if newest < COVERING_FIRST:
        raise RuntimeError(
            f"Newest zip on the archive page is {newest} — earlier than "
            f"{COVERING_FIRST}; the page is not showing its full listing."
        )

    covering = []
    month = COVERING_FIRST
    while month <= newest:
        covering.append(month)
        month = _add_months(month, COVERING_STEP_MONTHS)
    missing = [m for m in covering if m not in archive]
    if missing:
        raise RuntimeError(
            f"Covering zip(s) {missing} are not on the archive page — "
            "upstream removed a permanent zip or the page layout changed."
        )

    jobs = []
    first = FIRST_MONTH
    for month in covering:
        url, md5 = archive[month]
        jobs.append(ZipJob(month, url, md5, first, month))
        first = _add_months(month, 1)
    if newest > covering[-1]:
        url, md5 = archive[newest]
        jobs.append(ZipJob(newest, url, md5, first, newest))

    # Belt and braces: the ranges must tile FIRST_MONTH..newest exactly.
    expect = FIRST_MONTH
    for job in jobs:
        if job.first != expect or job.last < job.first:
            raise RuntimeError(
                f"Zip plan does not tile contiguously at {job.month} "
                f"(expected range to start {expect}, got {job.first})."
            )
        expect = _add_months(job.last, 1)
    if expect != _add_months(newest, 1):
        raise RuntimeError(f"Zip plan stops at {expect}, expected {newest}.")

    for job in jobs:
        state = "cached" if is_cached(job) else "to download"
        print(f"  plan: {job.month}.zip -> {job.first}..{job.last} ({state})")
    return jobs


# ---- the intermediate cache ------------------------------------------------


def cache_key(job: ZipJob) -> str:
    return f"v{CACHE_VERSION}_{job.month}_{job.md5}"


def intermediate_paths(job: ZipJob) -> dict[str, Path]:
    entry = CACHE_DIR / cache_key(job)
    return {ds: entry / f"{ds}.parquet" for ds in DATASETS}


def is_cached(job: ZipJob) -> bool:
    # All three files are always written (empty ones included), so "all
    # three exist" is a complete hit test; entries land via an atomic
    # directory rename, so a half-written entry cannot exist.
    return all(path.exists() for path in intermediate_paths(job).values())


def prune_cache(needed: set[str]) -> None:
    """Drop cache entries this run didn't need: superseded newest-month
    entries, old CACHE_VERSIONs, and any leftover tmp/. Called only after a
    fully successful build, so an interrupted run never loses useful work."""
    if not CACHE_DIR.exists():
        return
    for child in CACHE_DIR.iterdir():
        if child.name not in needed:
            print(f"  pruning stale cache entry: {child.name}")
            shutil.rmtree(child)


def _md5_file(path: Path) -> str:
    md5 = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            md5.update(chunk)
    return md5.hexdigest()


# ---- turning one zip into cached intermediates -----------------------------


def _snake(header: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", header.strip().lower()).strip("_")


def _read_member(zf: zipfile.ZipFile, name: str, columns: list[str]) -> pd.DataFrame:
    """Read one member CSV with every value as a string ('' for empty —
    never NaN, which keeps the output byte-deterministic), normalise headers
    to the declared schema, tolerate old-vintage drift (missing columns
    become '', unknown columns are dropped with a note)."""
    with zf.open(name) as fh:
        df = pd.read_csv(fh, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = [_snake(c) for c in df.columns]
    unknown = [c for c in df.columns if c not in columns]
    if unknown:
        print(f"  NOTE: {name}: dropping unrecognised column(s) {unknown}")
        df = df.drop(columns=unknown)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df[columns]


def build_intermediates(zip_path: Path, job: ZipJob) -> None:
    """Filter one zip down to its assigned months' London files and write
    the three per-dataset intermediates into the cache (atomically)."""
    schema_columns = {
        ds: [f["name"] for f in load_resource_schema(SOURCE_DIR, filename)]
        for ds, (_, filename, _) in DATASETS.items()
    }
    members: dict[str, list[str]] = {ds: [] for ds in DATASETS}
    street_months: dict[str, set[str]] = {"metropolitan": set(), "city-of-london": set()}
    suffix_to_ds = {suffix: ds for ds, (suffix, _, _) in DATASETS.items()}

    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            match = MEMBER_RE.match(name.replace("\\", "/"))
            if match is None:
                continue
            month, force, suffix = match.groups()
            # The tiling filter. Critical: the oldest zips span far more
            # than this zip's assigned range (2016-12.zip reaches back to
            # Dec 2010), and months outside the range belong to other zips.
            if not (job.first <= month <= job.last):
                continue
            members[suffix_to_ds[suffix]].append(name)
            if suffix == "street":
                street_months[force].add(month)

        # The Met has street data for every month of the published history,
        # so a hole there means the zip layout changed, not a data gap.
        expected = set(_months_between(job.first, job.last))
        met_missing = sorted(expected - street_months["metropolitan"])
        if met_missing:
            raise RuntimeError(
                f"{job.month}.zip has no metropolitan street file for "
                f"{met_missing} — member layout may have changed; check "
                "MEMBER_RE in fetch.py."
            )
        # City of London gaps happen upstream; outcomes/stop-search coverage
        # gaps are normal too (they start later and lag by force/month).
        col_missing = expected - street_months["city-of-london"]
        if col_missing:
            print(
                f"  NOTE: {job.month}.zip: no City of London street file "
                f"for {len(col_missing)} month(s) (upstream gap)."
            )

        tmp_dir = CACHE_DIR / "tmp" / cache_key(job)
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True)
        for ds, columns in schema_columns.items():
            frames = [_read_member(zf, n, columns) for n in sorted(members[ds])]
            if frames:
                df = pd.concat(frames, ignore_index=True)
            else:
                # Normal for old zips: no London outcomes before 2012, no
                # London stop-and-search before Mar 2015. Written anyway
                # (typed, empty) so is_cached() stays a complete test.
                df = pd.DataFrame({c: pd.Series([], dtype=str) for c in columns})
            write_parquet(df, tmp_dir / f"{ds}.parquet")
            print(f"  {job.month}.zip: {ds} -> {len(df):,} rows")

    final_dir = CACHE_DIR / cache_key(job)
    if final_dir.exists():
        shutil.rmtree(final_dir)
    os.replace(tmp_dir, final_dir)


def ensure_intermediates(job: ZipJob) -> None:
    """Make sure a zip's intermediates are in the cache, downloading and
    processing the zip if needed. Only ever one zip on disk at a time — the
    CI runner has ~14 GB of disk and each zip is ~1.6 GB."""
    if is_cached(job):
        print(f"  cache hit: {cache_key(job)}")
        return
    (CACHE_DIR / "tmp").mkdir(parents=True, exist_ok=True)
    zip_path = CACHE_DIR / "tmp" / f"{job.month}.zip"
    print(f"  Downloading {job.url} (~1.6 GB — this takes a while)")
    download(job.url, zip_path)
    try:
        digest = _md5_file(zip_path)
        if digest != job.md5:
            raise RuntimeError(
                f"{job.month}.zip MD5 mismatch (page says {job.md5}, file is "
                f"{digest}) — truncated download or a half-published upstream "
                "zip; the next run will retry."
            )
        build_intermediates(zip_path, job)
    finally:
        zip_path.unlink(missing_ok=True)


# ---- binding intermediates into the published tables ------------------------


def bind_and_write(jobs: list[ZipJob], dataset: str, out_dir: Path) -> Path:
    """Concatenate one dataset's intermediates across all zips, sort
    deterministically, and write the published parquet."""
    _, filename, period = DATASETS[dataset]
    fields = load_resource_schema(SOURCE_DIR, filename)
    columns = [f["name"] for f in fields]
    newest = jobs[-1].last

    df = pd.concat(
        [pd.read_parquet(intermediate_paths(job)[dataset]) for job in jobs],
        ignore_index=True,
    )

    # Defensive re-filter to the planned range (a no-op given the build-time
    # tiling filter, but guarantees a stale-logic intermediate can't leak
    # out-of-range rows). Rows with an empty period value are kept.
    period_month = df[period].str[:7]
    df = df[(period_month == "") | ((period_month >= FIRST_MONTH) & (period_month <= newest))]

    # Deterministic full-row sort: crime IDs are empty on ASB rows and true
    # duplicate rows exist upstream (and are deliberately kept, so row
    # counts match the published files), so every column is a sort key —
    # identical content can then only ever produce identical bytes.
    df = df.sort_values(
        [period] + [c for c in columns if c != period], ignore_index=True
    )
    return write_parquet(df[columns], out_dir / filename)


# ---- the source contract ----------------------------------------------------


def fetch(out_dir: Path) -> list[Path]:
    FETCHED_FROM.clear()
    archive = parse_archive_page(get_page(ARCHIVE_PAGE))
    jobs = plan_zips(archive)
    for job in jobs:
        ensure_intermediates(job)
    prune_cache({cache_key(job) for job in jobs})

    produced = []
    for dataset in DATASETS:
        path = bind_and_write(jobs, dataset, out_dir)
        print(f"  wrote {path.name} ({path.stat().st_size:,} bytes)")
        produced.append(path)

    newest_url = jobs[-1].url
    for path in produced:
        FETCHED_FROM[path.name] = newest_url
    return produced


def _today_month() -> str:
    return date.today().strftime("%Y-%m")


def validate(files: list[Path]) -> None:
    """Sanity-check this run's output — shape, not values. Raise (clearly)
    if upstream appears to have changed format; run_all.py then quarantines
    the files and the HF repo keeps serving the previous good version.

    Unlike sources with a raw fallback, all three parquets ARE the outputs,
    so all three are mandatory. Checks use footer metadata and single-column
    reads — never a full read of a 13M-row table.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from lib.common import check_parquet_schema

    by_name = {p.name: p for p in files}
    for name in (CRIMES_NAME, OUTCOMES_NAME, STOP_SEARCH_NAME):
        if name not in by_name:
            raise ValueError(f"expected output {name} was not produced")
        check_parquet_schema(by_name[name], load_resource_schema(SOURCE_DIR, name))

    forces = {"Metropolitan Police Service", "City of London Police"}

    def read_column(name: str, column: str):
        return pq.read_table(by_name[name], columns=[column]).column(column)

    def check_rows(name: str, low: int, high: int, expected: str) -> None:
        rows = pq.ParquetFile(by_name[name]).metadata.num_rows
        if not (low < rows < high):
            raise ValueError(
                f"{name} has {rows:,} rows — outside the plausible range "
                f"({expected})."
            )

    def check_month_span(
        name: str, column: str, min_low: str, min_high: str, max_lag_months: int
    ) -> None:
        """The column's values (truncated to YYYY-MM) must start in
        [min_low, min_high] and reach to within max_lag_months of today.
        Empty values are ignored (stop-and-search tolerates them)."""
        col = read_column(name, column)
        nonempty = col.filter(pc.not_equal(col, ""))
        months = pc.utf8_slice_codeunits(nonempty, 0, 7)
        span = pc.min_max(months).as_py()
        if not (min_low <= span["min"] <= min_high):
            raise ValueError(
                f"{name}: history starts {span['min']}, expected between "
                f"{min_low} and {min_high} — covering-set tiling may be broken."
            )
        stale_floor = _add_months(_today_month(), -max_lag_months)
        if span["max"] < stale_floor:
            raise ValueError(
                f"{name}: newest period {span['max']} is stale (expected >= "
                f"{stale_floor}) — the newest zip may have been missed or "
                "upstream stopped publishing."
            )

    def check_format(name: str, column: str, pattern: str, allow_empty: bool) -> None:
        col = read_column(name, column)
        if allow_empty:
            col = col.filter(pc.not_equal(col, ""))
        if not pc.all(pc.match_substring_regex(col, pattern)).as_py():
            raise ValueError(
                f"{name}: not every {column} value matches {pattern!r} — "
                "upstream format may have changed."
            )

    def check_forces(name: str, exact: bool) -> None:
        seen = set(pc.unique(read_column(name, "reported_by")).to_pylist())
        wrong = (seen != forces) if exact else not (seen and seen <= forces)
        if wrong:
            raise ValueError(
                f"{name}: reported_by values {sorted(seen)} don't match the "
                f"two London forces {sorted(forces)} — force filter broken."
            )

    # Crimes: ~13.5M rows observed on first build (2026-07).
    check_rows(CRIMES_NAME, 8_000_000, 30_000_000, "~13M in 2026")
    check_format(CRIMES_NAME, "month", r"^\d{4}-\d{2}$", allow_empty=False)
    check_month_span(CRIMES_NAME, "month", FIRST_MONTH, FIRST_MONTH, 6)
    check_forces(CRIMES_NAME, exact=True)
    if len(pc.unique(read_column(CRIMES_NAME, "month"))) < 180:
        raise ValueError(f"{CRIMES_NAME}: fewer than 180 distinct months.")

    # Outcomes: London data begins 2012-01 (observed on the first build,
    # 2026-07, from the immutable 2013-12 covering zip — so the start month
    # can only move if tiling breaks); ~10M rows observed.
    check_rows(OUTCOMES_NAME, 5_000_000, 25_000_000, "~10M in 2026")
    check_format(OUTCOMES_NAME, "month", r"^\d{4}-\d{2}$", allow_empty=False)
    check_month_span(OUTCOMES_NAME, "month", "2011-12", "2012-06", 6)
    check_forces(OUTCOMES_NAME, exact=False)  # not every force returns outcomes

    # Stop & search: London data begins 2015-03 (observed on the first
    # build, from the immutable 2016-12 covering zip); ~2M rows observed;
    # publication lags more than crimes. No force column exists in this
    # table — force membership is implied by which member files were read.
    check_rows(STOP_SEARCH_NAME, 500_000, 10_000_000, "~2M in 2026")
    check_format(STOP_SEARCH_NAME, "date", r"^\d{4}-\d{2}-\d{2}T", allow_empty=True)
    check_month_span(STOP_SEARCH_NAME, "date", "2014-12", "2015-06", 9)


def stats(files: list[Path]) -> dict[str, dict]:
    """Computed per-file stats for catalog.json: row counts and the span of
    months (crimes/outcomes) or stop dates (stop & search) present."""
    by_name = {p.name: p for p in files}
    return {
        filename: parquet_stats(by_name[filename], period)
        for _, filename, period in DATASETS.values()
        if filename in by_name
    }


if __name__ == "__main__":
    files = fetch(Path("data"))
    for f in files:
        print(f"Wrote {f} ({f.stat().st_size:,} bytes)")
