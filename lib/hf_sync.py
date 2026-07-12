"""Publish each family's files to its Hugging Face dataset repo.

One dataset repo per family (DESIGN.md §5). The hub compares content hashes
before committing: unchanged files are dropped from the commit, and if
nothing changed at all no commit is created — that server-side skip is what
makes byte-determinism pay off (DESIGN.md §6) and what the run-twice check
relies on. huggingface_hub is imported lazily so `run_all.py --no-hf` works
without it installed.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from lib.common import hf_repo_id


def get_api():
    """An authenticated HfApi, or a clear error telling you where the token
    comes from. CI provides HF_TOKEN as an Actions secret; locally either
    export HF_TOKEN or store the token once with `hf auth login`."""
    from huggingface_hub import HfApi, get_token

    token = os.environ.get("HF_TOKEN") or get_token()
    if not token:
        raise RuntimeError(
            "No Hugging Face token found. Create a WRITE token at "
            "https://huggingface.co/settings/tokens, then either set the "
            "HF_TOKEN environment variable or run `hf auth login` — or run "
            "with --no-hf to skip uploading."
        )
    return HfApi(token=token, library_name="uk-open-data")


def sync_family(api, family: str, files: list[Path], card: str) -> dict:
    """Upload this run's files plus the generated card to the family repo.

    Creates the repo if it doesn't exist (adding a source never involves the
    HF UI). Returns {"revision": <main sha after sync>, "committed": bool}.
    committed=False means the hub found every byte identical — the normal
    outcome for most weekly runs.
    """
    from huggingface_hub import CommitOperationAdd

    repo_id = hf_repo_id(family)
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True)
    before = api.repo_info(repo_id, repo_type="dataset").sha

    operations = [
        CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=card.encode("utf-8"))
    ] + [
        CommitOperationAdd(path_in_repo=path.name, path_or_fileobj=str(path))
        for path in sorted(files)
    ]
    info = api.create_commit(
        repo_id,
        operations,
        repo_type="dataset",
        commit_message=f"refresh {datetime.now(timezone.utc):%Y-%m-%d}",
    )
    # On an all-unchanged run create_commit skips the commit and returns a
    # CommitInfo pointing at the existing head, so oid == before.
    return {"revision": info.oid, "committed": info.oid != before}


def ensure_monthly_tag(api, family: str) -> str:
    """Tag the repo `archive-YYYY-MM` for the current month if not already
    tagged (DESIGN.md §7 step 3). exist_ok means the first run of the month
    wins and later runs never move the tag — snapshots stay immutable.
    """
    tag = f"archive-{datetime.now(timezone.utc):%Y-%m}"
    api.create_tag(hf_repo_id(family), tag=tag, repo_type="dataset", exist_ok=True)
    return tag
