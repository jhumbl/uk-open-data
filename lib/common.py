"""Shared helpers used by every source's fetch.py and by run_all.py.

Keep this module small and dependency-light. If a helper is only needed by
one source, put it in that source's fetch.py instead.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Repo root = parent of the lib/ directory this file lives in.
REPO_ROOT = Path(__file__).resolve().parent.parent
CATALOG_PATH = REPO_ROOT / "catalog.json"
FAMILIES_PATH = REPO_ROOT / "families.json"

# The Hugging Face namespace all dataset repos live under (DESIGN.md §3).
# Set once when the HF account is created; changing it is a URL migration
# for every consumer (DESIGN.md §7) — deliberate, never casual.
HF_NAMESPACE = "jhumbl"

# Be a polite client: identify ourselves and never hang forever.
USER_AGENT = "uk-open-data (github.com/jhumbl/uk-open-data; automated weekly fetch)"
DEFAULT_TIMEOUT = 120  # seconds

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = (10, 30)  # sleep before attempt 2, attempt 3


def hf_repo_id(family: str) -> str:
    """The HF dataset repo id for a family, e.g. 'ns/dft-vehicle-licensing'."""
    return f"{HF_NAMESPACE}/{family}"


def hf_repo_url(family: str) -> str:
    return f"https://huggingface.co/datasets/{hf_repo_id(family)}"


def hf_resolve_url(family: str, filename: str, revision: str = "main") -> str:
    """The direct-download URL consumers use (CORS + Range verified weekly).

    revision may be 'main' (latest), an 'archive-YYYY-MM' tag, or a commit sha.
    """
    return f"{hf_repo_url(family)}/resolve/{revision}/{filename}"


def _with_retries(what: str, attempt_fn):
    """Run attempt_fn up to RETRY_ATTEMPTS times, backing off between tries.

    Retries transient failures only: connection errors, timeouts, and HTTP
    5xx/429. Anything else (404 = the link moved, etc.) raises immediately —
    retrying a structural failure just delays the correct fetch_failed.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return attempt_fn()
        except requests.exceptions.RequestException as exc:
            response = getattr(exc, "response", None)
            transient = response is None or (
                response.status_code >= 500 or response.status_code == 429
            )
            if not transient or attempt == RETRY_ATTEMPTS:
                raise
            wait = RETRY_BACKOFF_SECONDS[attempt - 1]
            print(
                f"  retrying {what} in {wait}s "
                f"(attempt {attempt}/{RETRY_ATTEMPTS} failed: {exc})"
            )
            time.sleep(wait)


def download(url: str, dest: Path) -> Path:
    """Stream a URL to dest, creating parent dirs, retrying transient
    failures with backoff. Returns dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    def attempt() -> Path:
        with requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=DEFAULT_TIMEOUT,
            stream=True,
        ) as resp:
            resp.raise_for_status()
            # "wb" inside the attempt so a retry truncates any partial file.
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
        return dest

    return _with_retries(url, attempt)


def get_page(url: str) -> str:
    """Fetch a web page as text (used for scraping download links),
    retrying transient failures with backoff."""

    def attempt() -> str:
        resp = requests.get(
            url, headers={"User-Agent": USER_AGENT}, timeout=DEFAULT_TIMEOUT
        )
        resp.raise_for_status()
        return resp.text

    return _with_retries(url, attempt)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_catalog() -> dict:
    if CATALOG_PATH.exists():
        with open(CATALOG_PATH) as fh:
            return json.load(fh)
    return {"datasets": {}, "families": {}, "sources": {}}


def save_catalog(catalog: dict) -> None:
    with open(CATALOG_PATH, "w") as fh:
        json.dump(catalog, fh, indent=2, sort_keys=True)
        fh.write("\n")


def load_datapackage(source_dir: Path) -> dict:
    with open(source_dir / "datapackage.json") as fh:
        return json.load(fh)


def load_families() -> dict:
    """families.json: family-level card metadata (title, description) that
    has no per-source home. Every hf_repo named by a source's
    datapackage.json must have an entry here."""
    with open(FAMILIES_PATH) as fh:
        return json.load(fh)


def write_parquet(df, dest: Path) -> Path:
    """Write a DataFrame to parquet with byte-deterministic output.

    Identical content must produce identical bytes on every OS and every run
    (see the determinism gotcha in CLAUDE.md), so we pin everything pandas or
    pyarrow would otherwise decide for us: the pandas schema-metadata blob is
    stripped (it embeds the pandas version), and compression / row-group size
    are set explicitly. The footer's created_by field (the pyarrow version)
    cannot be removed — the exact pyarrow== pin in requirements.txt is the
    backstop for that one.

    The CALLER is responsible for deterministic row and column order.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.replace_schema_metadata(None)
    pq.write_table(table, dest, compression="snappy", row_group_size=1 << 20)
    return dest


def load_resource_schema(source_dir: Path, filename: str) -> list[dict]:
    """Return the Frictionless schema.fields list for the resource whose
    path equals filename in source_dir/datapackage.json.

    Raises KeyError if the resource or its schema block is missing — schema
    blocks on derived tabular resources are load-bearing: fetch.py builds
    its column order from them and validate() enforces them.
    """
    package = load_datapackage(source_dir)
    for resource in package.get("resources", []):
        if resource.get("path") == filename:
            return resource["schema"]["fields"]
    raise KeyError(f"no resource with path {filename!r} in {source_dir / 'datapackage.json'}")


def check_parquet_schema(path: Path, fields: list[dict]) -> None:
    """Assert a parquet file's column names, order and types match the
    Frictionless fields declared in datapackage.json. Reads only the file
    footer, so it is cheap regardless of table size.

    Raises ValueError naming the first mismatch. Types are checked
    pragmatically: string -> arrow string/large_string, integer -> any
    arrow integer, number -> floating, boolean -> boolean.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    type_checks = {
        "string": lambda t: pa.types.is_string(t) or pa.types.is_large_string(t),
        "integer": pa.types.is_integer,
        "number": pa.types.is_floating,
        "boolean": pa.types.is_boolean,
    }

    schema = pq.read_schema(path)
    expected_names = [f["name"] for f in fields]
    if schema.names != expected_names:
        raise ValueError(
            f"{path.name} columns {schema.names} != declared schema "
            f"{expected_names} (datapackage.json is the source of truth)"
        )
    for field in fields:
        declared = field.get("type", "string")
        check = type_checks.get(declared)
        if check is None:
            raise ValueError(
                f"{path.name}: declared type {declared!r} for column "
                f"{field['name']!r} is not one of {sorted(type_checks)}"
            )
        actual = schema.field(field["name"]).type
        if not check(actual):
            raise ValueError(
                f"{path.name} column {field['name']!r} is {actual}, but the "
                f"declared schema says {declared!r}"
            )


def parquet_stats(path: Path, period_column: str) -> dict:
    """Computed catalog stats for a parquet file: row count (from the footer,
    no data read) and the min/max of period_column (a single-column read),
    skipping null and empty values. The period column's values must be
    strings whose lexicographic order equals temporal order (e.g. '2025 Q4',
    ISO dates).
    """
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    row_count = pf.metadata.num_rows
    column = pf.read(columns=[period_column]).column(period_column)
    values = [v for v in column.to_pylist() if v]
    stats = {"row_count": row_count}
    if values:
        stats["min_period"] = min(values)
        stats["max_period"] = max(values)
    return stats
