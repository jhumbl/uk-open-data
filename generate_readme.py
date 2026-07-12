"""Regenerate README.md's "## Datasets" table from catalog.json and every
sources/*/datapackage.json.

Usage:
    python generate_readme.py

Rewrites only the content between the BEGIN/END marker comments in
README.md; never hand-edit that table directly, it will be overwritten.
A source only appears once it has at least one file with a download_url in
catalog.json (i.e. a successful sync to HF) — otherwise its URLs wouldn't
resolve yet.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from lib.common import REPO_ROOT, hf_repo_url, load_catalog

SOURCES_DIR = REPO_ROOT / "sources"
README_PATH = REPO_ROOT / "README.md"

BEGIN_MARKER = "<!-- BEGIN GENERATED DATASETS TABLE (generate_readme.py) -->"
END_MARKER = "<!-- END GENERATED DATASETS TABLE -->"

REQUIRED_FIELDS = ("provider", "cadence", "title", "resources", "hf_repo")


def build_row(source_dir: Path) -> str:
    pkg = json.loads((source_dir / "datapackage.json").read_text(encoding="utf-8"))
    for field in REQUIRED_FIELDS:
        if field not in pkg:
            sys.exit(
                f"generate_readme.py: {source_dir.name}/datapackage.json is "
                f"missing required field '{field}'"
            )

    resources = pkg["resources"]
    primary = resources[0]["path"]
    # Label each sibling by what distinguishes it from the primary file:
    # its extension when only that differs (`.parquet`), its suffix when
    # the names share the source-name stem (`_outcomes.parquet`), or the
    # full name as a last resort — three parquet siblings must not all
    # collapse into the same bare extension.
    stem = primary.split(".", 1)[0]
    extra = [
        r["path"].removeprefix(stem) if r["path"].startswith(stem) else r["path"]
        for r in resources[1:]
    ]
    file_cell = f"`{primary}`"
    if extra:
        file_cell += " (+ " + ", ".join(f"`{label}`" for label in extra) + ")"

    family = pkg["hf_repo"]
    data_cell = f"[{family}]({hf_repo_url(family)})"
    return (
        f"| {file_cell} | {pkg['title']} | {pkg['provider']} "
        f"| {pkg['cadence']} | {data_cell} |"
    )


def build_table() -> str:
    catalog = load_catalog()
    sources_with_urls = {
        entry["source"]
        for entry in catalog["datasets"].values()
        if "download_url" in entry
    }

    rows = []
    for source_dir in sorted(SOURCES_DIR.iterdir()):
        if not source_dir.is_dir() or not (source_dir / "datapackage.json").exists():
            continue
        if source_dir.name not in sources_with_urls:
            print(
                f"generate_readme.py: skipping {source_dir.name} — "
                "not yet synced to HF",
                file=sys.stderr,
            )
            continue
        rows.append(build_row(source_dir))

    header = (
        "| File | Description | Source | Cadence | Data |\n"
        "| --- | --- | --- | --- | --- |"
    )
    return "\n".join([header, *rows])


def main() -> int:
    text = README_PATH.read_text(encoding="utf-8")
    table = build_table()
    pattern = re.compile(
        re.escape(BEGIN_MARKER) + r".*?" + re.escape(END_MARKER), re.DOTALL
    )
    new_text, count = pattern.subn(f"{BEGIN_MARKER}\n{table}\n{END_MARKER}", text)
    if count != 1:
        sys.exit(
            "generate_readme.py: could not find the marker pair "
            f"({BEGIN_MARKER!r} / {END_MARKER!r}) exactly once in README.md"
        )

    if new_text != text:
        README_PATH.write_text(new_text, encoding="utf-8")
        print("README.md updated")
    else:
        print("README.md already up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
