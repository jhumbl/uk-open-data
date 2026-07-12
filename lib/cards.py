"""Render each family's HF dataset card (the repo's README.md) from
families.json plus the member sources' datapackage.json files.

One metadata source, two rendered faces (DESIGN.md §7): this module renders
the Hugging Face face; generate_readme.py renders the pipeline-repo face.
Never hand-edit a card on the HF website — the next weekly run overwrites it.

Cards must be byte-deterministic and MUST NOT embed timestamps or run
metadata: an unchanged card is skipped by the uploader, but a card that
says "generated on <date>" would create an HF commit every week and break
the run-twice-nothing-changes invariant (DESIGN.md §6).

The YAML frontmatter carries the `configs:` block that drives HF's dataset
viewer: one config per derived parquet, named after the file's stem, so
every table in a family renders as its own tab in the viewer.
"""

from __future__ import annotations

import json

from lib.common import hf_repo_id, hf_resolve_url


def _yaml_str(value: str) -> str:
    """A safely quoted YAML scalar. JSON string escaping is a YAML subset."""
    return json.dumps(value)


def _md_cell(value: str) -> str:
    """Escape a string for use inside a markdown table cell."""
    return value.replace("|", "\\|").replace("\n", " ")


def _parquet_resources(package: dict) -> list[dict]:
    return [r for r in package.get("resources", []) if r.get("format") == "parquet"]


def _frontmatter(family_meta: dict, packages: list[dict]) -> str:
    """The YAML block HF parses: license, pretty_name, viewer configs."""
    licenses = {json.dumps(p["licenses"][0], sort_keys=True) for p in packages}
    if len(licenses) != 1:
        raise ValueError(
            "sources in one family declare different licenses — that breaks "
            "the one-honest-card test (DESIGN.md §5); split the family"
        )
    license_ = json.loads(licenses.pop())

    lines = [
        "---",
        # HF's license taxonomy has no OGL entry; 'other' + name + link is
        # the documented way to declare one.
        "license: other",
        f"license_name: {_yaml_str(license_['name'].lower())}",
        f"license_link: {_yaml_str(license_['path'])}",
        f"pretty_name: {_yaml_str(family_meta['title'])}",
        "configs:",
    ]
    for package in packages:
        for resource in _parquet_resources(package):
            lines += [
                f"- config_name: {_yaml_str(resource['path'].removesuffix('.parquet'))}",
                "  data_files:",
                "  - split: train",
                f"    path: {_yaml_str(resource['path'])}",
            ]
    lines.append("---")
    return "\n".join(lines)


def _table_section(family: str, package: dict) -> str:
    """One '## <table>' section of the card body, per member source."""
    out = [f"## {package['title']}", "", package["description"], ""]

    out += [
        f"- **Provider:** {package['provider']}",
        f"- **Update cadence:** {package['update_cadence']}",
        f"- **Homepage:** {package['homepage']}",
        f"- **Licence:** [{package['licenses'][0]['title']}]({package['licenses'][0]['path']})",
    ]
    citations = "; ".join(f"[{s['title']}]({s['path']})" for s in package.get("sources", []))
    if citations:
        out.append(f"- **Source:** {citations}")
    out.append("")

    out += ["### Files", ""]
    for resource in package.get("resources", []):
        url = hf_resolve_url(family, resource["path"])
        out.append(f"- [`{resource['path']}`]({url}) — {resource['description']}")
    out.append("")

    for resource in _parquet_resources(package):
        out += [f"### Columns of `{resource['path']}`", ""]
        out += ["| column | type | description |", "| --- | --- | --- |"]
        for field in resource["schema"]["fields"]:
            out.append(
                f"| `{field['name']}` | {field.get('type', 'string')} "
                f"| {_md_cell(field.get('description', ''))} |"
            )
        out.append("")

    caveats = package.get("caveats", [])
    if caveats:
        out += ["### Caveats", ""]
        out += [f"- {c}" for c in caveats]
        out.append("")

    return "\n".join(out)


def build_card(
    family: str, family_meta: dict, packages: list[dict], pipeline_repo_url: str
) -> str:
    """The full README.md for a family's HF dataset repo.

    packages: the datapackage.json dicts of every member source that has
    published data, in a deterministic (sorted-by-name) order.
    """
    example = next(
        (r["path"] for p in packages for r in _parquet_resources(p)), None
    )

    head = [
        _frontmatter(family_meta, packages),
        "",
        f"# {family_meta['title']}",
        "",
        family_meta["description"],
        "",
        f"Curated by [uk-open-data]({pipeline_repo_url}): a weekly pipeline "
        "re-downloads each publisher file, tidies it into analysis-ready "
        "parquet, validates it against a declared schema, and uploads it "
        "here. `main` is always the latest data; a git tag `archive-YYYY-MM` "
        "snapshots every month, so silent revisions by the publisher stay "
        "visible. The raw file as published is stored alongside every tidy "
        "parquet.",
        "",
        "## How to use",
        "",
        "Files are served with CORS and HTTP range support (verified on "
        "every pipeline run), so browsers can query them directly — "
        "DuckDB-Wasm, or plain `fetch()`:",
        "",
        "```",
        f"latest:    {hf_resolve_url(family, '<filename>')}",
        f"snapshot:  {hf_resolve_url(family, '<filename>', 'archive-YYYY-MM')}",
        f"duckdb:    hf://datasets/{hf_repo_id(family)}/<filename>",
        "```",
    ]
    if example:
        head += [
            "",
            "```sql",
            f"SELECT * FROM 'hf://datasets/{hf_repo_id(family)}/{example}' LIMIT 10;",
            "```",
        ]
    head.append("")

    sections = [_table_section(family, p) for p in packages]
    return "\n".join(head) + "\n" + "\n".join(sections)
