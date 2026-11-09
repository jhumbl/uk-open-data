"""Render each family's HF dataset card (the repo's README.md) from
families.json plus the member sources' datapackage.json files.

One metadata source, two rendered faces: this module renders
the Hugging Face face; generate_readme.py renders the pipeline-repo face.
Never hand-edit a card on the HF website — the next weekly run overwrites it.

Cards must be byte-deterministic and MUST NOT embed timestamps or run
metadata: an unchanged card is skipped by the uploader, but a card that
says "generated on <date>" would create an HF commit every week and break
the run-twice-nothing-changes invariant (the determinism gotcha in
CLAUDE.md).

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
            "the one-honest-card test (CLAUDE.md, the dataset-family "
            "model); split the family"
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
    # Cards describe the data and where it comes from — nothing about how
    # this project uses it (browsers, CORS, dashboards). That belongs in
    # the pipeline repo's README, where smoke_test.py backs the claim.
    example = next(
        (r["path"] for p in packages for r in _parquet_resources(p)), None
    )
    providers = list(dict.fromkeys(p["provider"] for p in packages))
    repo_id = hf_repo_id(family)
    # police_crime_london republishes no raw files (see its caveats), so
    # only claim "original kept alongside" when every member does keep one.
    keeps_raw = all(
        any(r.get("format") != "parquet" for r in p.get("resources", []))
        for p in packages
    )

    head = [
        _frontmatter(family_meta, packages),
        "",
        f"# {family_meta['title']}",
        "",
        family_meta["description"],
        "",
        "## About this copy",
        "",
        f"An unofficial republication of open data published by "
        f"{', '.join(providers)}, under the licence shown below. Each table "
        "is the publisher's data tidied into a single parquet file"
        + (
            "; the file as originally published is kept alongside it. "
            if keeps_raw
            else ". "
        )
        + "The publisher's files are re-checked weekly and changes picked "
        "up automatically. `main` holds the latest "
        "version and a monthly git tag `archive-YYYY-MM` preserves each "
        f"month's state, so earlier versions remain available. Maintained "
        f"by [uk-open-data]({pipeline_repo_url}).",
        "",
        "## Loading the data",
        "",
    ]
    if example:
        head += [
            "```python",
            "import pandas as pd",
            f'df = pd.read_parquet("hf://datasets/{repo_id}/{example}")',
            "```",
            "",
            "```sql",
            f"SELECT * FROM 'hf://datasets/{repo_id}/{example}' LIMIT 10;  -- DuckDB",
            "```",
            "",
        ]
    head += [
        "Direct download, latest and a monthly snapshot:",
        "",
        "```",
        hf_resolve_url(family, "<filename>"),
        hf_resolve_url(family, "<filename>", "archive-YYYY-MM"),
        "```",
        "",
    ]

    sections = [_table_section(family, p) for p in packages]
    return "\n".join(head) + "\n" + "\n".join(sections)
