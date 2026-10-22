#!/usr/bin/env python3
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

"""Compare exported v1/v2 committee parents without contacting or writing either system.

Inputs are complete CSV snapshots taken as close together as possible:
  --v1        committee_sfid,parent_sfid (platform.collaboration__c.sfid,committee_id)
  --v2        committee_uid,parent_uid (v2 committee base, not committee members)
  --mappings  committee_sfid,committee_uid (live committee.sfid.* mappings only)

Outputs one CSV row for every mapped committee plus unmapped v1 committees.
Neither absence nor update timestamps establish which system's parent is correct.
"""

import argparse
import csv
import sys
from pathlib import Path

FIELDS = (
    "status",
    "committee_sfid",
    "committee_uid",
    "v1_parent_sfid",
    "v1_parent_uid",
    "v2_parent_uid",
    "v2_parent_sfid",
)


def load_rows(path: Path, key: str, required: set[str]) -> dict[str, dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError(f"{path}: required columns: {', '.join(sorted(required))}")
        rows = {}
        for line, raw in enumerate(reader, start=2):
            if any(raw.get(column) is None for column in required):
                raise ValueError(f"{path}:{line}: missing required CSV cell")
            row = {
                column: (value or "").strip() for column, value in raw.items() if column
            }
            identifier = row[key]
            if not identifier or identifier in rows:
                raise ValueError(
                    f"{path}:{line}: empty or duplicate {key}: {identifier!r}"
                )
            rows[identifier] = row
        return rows


def compare(
    v1: dict[str, dict[str, str]],
    v2: dict[str, dict[str, str]],
    mappings: dict[str, dict[str, str]],
):
    forward = {sfid: row["committee_uid"] for sfid, row in mappings.items()}
    if any(not uid for uid in forward.values()) or len(set(forward.values())) != len(
        forward
    ):
        raise ValueError("mappings must have distinct non-empty committee_uid values")
    reverse = {uid: sfid for sfid, uid in forward.items()}

    for sfid in sorted(set(v1) | set(forward)):
        uid = forward.get(sfid, "")
        parent_sfid = v1.get(sfid, {}).get("parent_sfid", "")
        parent_uid = v2.get(uid, {}).get("parent_uid", "")
        resolved_v1_parent = forward.get(parent_sfid, "")
        resolved_v2_parent = reverse.get(parent_uid, "")

        if sfid not in v1:
            status = "missing_v1_record"
        elif not uid:
            status = "unmapped_committee"
        elif uid not in v2:
            status = "missing_v2_record"
        elif parent_sfid and not resolved_v1_parent:
            status = "unmapped_v1_parent"
        elif parent_uid and not resolved_v2_parent:
            status = "unmapped_v2_parent"
        elif resolved_v1_parent == parent_uid:
            status = "both_empty" if not parent_uid else "match"
        elif resolved_v1_parent and not parent_uid:
            status = "v1_only"
        elif parent_uid and not parent_sfid:
            status = "v2_only"
        else:
            status = "different_parents"

        yield {
            "status": status,
            "committee_sfid": sfid,
            "committee_uid": uid,
            "v1_parent_sfid": parent_sfid,
            "v1_parent_uid": resolved_v1_parent,
            "v2_parent_uid": parent_uid,
            "v2_parent_sfid": resolved_v2_parent,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v1", type=Path, required=True)
    parser.add_argument("--v2", type=Path, required=True)
    parser.add_argument("--mappings", type=Path, required=True)
    args = parser.parse_args()

    try:
        v1 = load_rows(args.v1, "committee_sfid", {"committee_sfid", "parent_sfid"})
        v2 = load_rows(args.v2, "committee_uid", {"committee_uid", "parent_uid"})
        mappings = load_rows(
            args.mappings, "committee_sfid", {"committee_sfid", "committee_uid"}
        )
        writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(compare(v1, v2, mappings))
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    sys.exit(main())
