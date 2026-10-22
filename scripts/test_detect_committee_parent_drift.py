# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

import csv
import tempfile
import unittest
from pathlib import Path

from detect_committee_parent_drift import compare, load_rows


class DetectCommitteeParentDriftTests(unittest.TestCase):
    def test_all_disagreement_categories(self):
        v1 = {
            "parent": {"parent_sfid": ""},
            "other": {"parent_sfid": ""},
            "match": {"parent_sfid": "parent"},
            "v1-only": {"parent_sfid": "parent"},
            "v2-only": {"parent_sfid": ""},
            "different": {"parent_sfid": "parent"},
            "unmapped-parent": {"parent_sfid": "missing"},
            "unmapped-child": {"parent_sfid": "parent"},
            "missing-v2": {"parent_sfid": ""},
            "orphan-v2-parent": {"parent_sfid": ""},
        }
        mappings = {
            sfid: {"committee_uid": "uid-" + sfid}
            for sfid in v1
            if sfid != "unmapped-child"
        }
        v2 = {
            row["committee_uid"]: {"parent_uid": ""}
            for row in mappings.values()
            if row["committee_uid"] != "uid-missing-v2"
        }
        v2["uid-match"]["parent_uid"] = "uid-parent"
        v2["uid-v2-only"]["parent_uid"] = "uid-parent"
        v2["uid-different"]["parent_uid"] = "uid-other"
        v2["uid-orphan-v2-parent"]["parent_uid"] = "uid-not-mapped"

        rows = {row["committee_sfid"]: row for row in compare(v1, v2, mappings)}
        self.assertEqual(
            {sfid: row["status"] for sfid, row in rows.items()},
            {
                "parent": "both_empty",
                "other": "both_empty",
                "match": "match",
                "v1-only": "v1_only",
                "v2-only": "v2_only",
                "different": "different_parents",
                "unmapped-parent": "unmapped_v1_parent",
                "unmapped-child": "unmapped_committee",
                "missing-v2": "missing_v2_record",
                "orphan-v2-parent": "unmapped_v2_parent",
            },
        )
        self.assertEqual(rows["different"]["v2_parent_sfid"], "other")

    def test_rejects_duplicate_and_missing_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "v1.csv"
            with path.open("w", newline="") as out:
                writer = csv.writer(out)
                writer.writerows(
                    [["committee_sfid", "parent_sfid"], ["same", ""], ["same", ""]]
                )
            with self.assertRaisesRegex(ValueError, "duplicate"):
                load_rows(path, "committee_sfid", {"committee_sfid", "parent_sfid"})
            with self.assertRaisesRegex(ValueError, "required columns"):
                load_rows(path, "committee_sfid", {"committee_sfid", "missing"})

    def test_rejects_short_rows_but_allows_explicit_empty_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "v1.csv"
            path.write_text(
                "committee_sfid,parent_sfid\nchild-sfid\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "missing required CSV cell"):
                load_rows(path, "committee_sfid", {"committee_sfid", "parent_sfid"})
            path.write_text(
                "committee_sfid,parent_sfid\nchild-sfid,\n", encoding="utf-8"
            )
            self.assertEqual(
                load_rows(path, "committee_sfid", {"committee_sfid", "parent_sfid"})[
                    "child-sfid"
                ]["parent_sfid"],
                "",
            )


if __name__ == "__main__":
    unittest.main()
