"""TfL's open data bucket, https://crowding.data.tfl.gov.uk/ — a public S3
bucket holding several unrelated series in top-level folders ("Network
Demand/", "Annual Station Counts/", "NUMBAT/", "BUSTO/", ...).

TfL renames files freely between releases (stray spaces, changing prefixes,
"_public" vs "_Public"), so sources never hard-code a key: they list a folder
and pick files by pattern. This module is just that listing, shared by every
source that reads the bucket.
"""

from __future__ import annotations

import re
import urllib.parse
import xml.etree.ElementTree as ET

from lib.common import get_page

BUCKET_URL = "https://s3-eu-west-1.amazonaws.com/crowding.data.tfl.gov.uk"
# Human-facing landing page for the bucket (recorded as provenance).
LANDING_URL = "https://crowding.data.tfl.gov.uk/"

_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def list_keys(prefix: str) -> list[str]:
    """Every object key under prefix (e.g. 'Network Demand/'), via the public
    S3 ListObjects XML, following pagination markers. Folder placeholder keys
    (ending '/') are included as S3 returns them; callers filter by pattern."""
    keys: list[str] = []
    marker = ""
    while True:
        query = urllib.parse.urlencode({"prefix": prefix, "marker": marker})
        root = ET.fromstring(get_page(f"{BUCKET_URL}/?{query}"))
        page = [el.text or "" for el in root.findall("s3:Contents/s3:Key", _S3_NS)]
        keys.extend(page)
        truncated = (root.findtext("s3:IsTruncated", "false", _S3_NS) or "").lower() == "true"
        if not truncated or not page:
            break
        marker = page[-1]
    return keys


def unrecognised_keys(keys: list[str], patterns: list[re.Pattern]) -> list[str]:
    """Files among keys (folder placeholders ending '/' excluded) that match
    none of patterns, sorted.

    The tripwire for a publisher moving a series without breaking it: if TfL
    starts publishing under a new filename, our pattern stops matching, the
    old files keep validating, and nothing goes red. Sources record this list
    in their catalog stats (and print a warning), so a newcomer in a folder
    we read shows up as a reviewable catalog.json diff. Pass every pattern a
    folder's files are expected to match — including files we deliberately
    don't republish — so the list stays empty in the normal case.
    """
    return sorted(
        k for k in keys
        if not k.endswith("/") and not any(p.match(k) for p in patterns)
    )


def key_url(key: str) -> str:
    """Direct download URL for an object key."""
    return f"{BUCKET_URL}/{urllib.parse.quote(key)}"
