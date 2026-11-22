"""The name of a downloaded file: readable, unique, and safe both as a path and as a URL.

`export_<uuid>.<ext>` told the user nothing and made two exports of the same table
indistinguishable on disk. The name is now the table (or the application) plus the minute; the
tests below pin the part that used to go wrong — what happens to a name that is too long, full of
characters a URL or a filesystem refuses, or empty once cleaned.
"""

import re
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from lib.exportnames import RESERVED, export_name, slug  # noqa: E402

AT = datetime(2026, 11, 22, 13, 26)


class SlugTests(unittest.TestCase):

    def test_an_accented_name_keeps_its_letters(self):
        self.assertEqual(slug("Ventes été"), "Ventes_ete")

    def test_a_wildcard_index_loses_its_meta_characters(self):
        # `nyx_api*` is a legal Elasticsearch index pattern and a legal argument of
        # `/generic_search`; `*` is neither a URL character nor a portable file name.
        self.assertEqual(slug("nyx_api*"), "nyx_api")

    def test_everything_outside_the_safe_set_becomes_an_underscore(self):
        self.assertEqual(slug('a b/c\\d:e?"f"<g>|h#i%j'), "a_b_c_d_e_f_g_h_i_j")

    def test_a_name_is_bounded_whatever_it_started_as(self):
        self.assertEqual(len(slug("x" * 300)), 80)

    def test_a_name_is_never_left_empty_or_with_a_leading_dot(self):
        self.assertEqual(slug(""), "")
        self.assertEqual(slug("..."), "")
        self.assertEqual(slug("réserver"), "reserver")


class ExportNameTests(unittest.TestCase):

    def test_the_name_carries_the_table_and_the_minute(self):
        self.assertEqual(export_name("airport_alert-2026.11", "xlsx", at=AT, token="a1b2c3"),
                         "airport_alert-2026.11_20261122-1326_a1b2c3.xlsx")

    def test_two_exports_of_the_same_table_in_the_same_minute_differ(self):
        first = export_name("sites", "csv", at=AT)
        second = export_name("sites", "csv", at=AT)

        self.assertNotEqual(first, second)

    def test_an_unnamed_export_still_has_a_name(self):
        self.assertTrue(export_name("", "csv", at=AT, token="a1b2c3").startswith("export_"))

    def test_a_windows_reserved_name_is_prefixed(self):
        # Downloaded to a Windows machine, `con.csv` cannot be created whatever the path.
        for name in sorted(RESERVED)[:3]:
            self.assertTrue(export_name(name, "csv", at=AT, token="a1b2c3")
                            .startswith("export_" + name + "_"))

    def test_the_extension_is_a_whitelist_not_a_choice(self):
        self.assertTrue(export_name("sites", "pdf", at=AT, token="a1b2c3").endswith(".csv"))
        self.assertTrue(export_name("sites", "../../etc/passwd", at=AT, token="a1b2c3")
                        .endswith(".csv"))
        self.assertTrue(export_name("sites", ".XLSX", at=AT, token="a1b2c3").endswith(".xlsx"))

    def test_the_name_is_a_safe_path_segment_and_a_safe_url(self):
        name = export_name("Ventes été (2026) #1", "xlsx", at=AT, token="a1b2c3")

        self.assertRegex(name, r"^[A-Za-z0-9._-]+$", "no character may need URL quoting")
        self.assertLess(len(name), 255, "NAME_MAX on the volume that holds the exports")

    def test_a_name_cut_for_length_stays_unique(self):
        first = export_name("x" * 300, "csv", at=AT, token="a1b2c3")
        second = export_name("y" * 300, "csv", at=AT, token="d4e5f6")

        self.assertNotEqual(first, second)
        self.assertEqual(first.count("x"), 80, "the stem is cut, not the rest of the name")
        self.assertTrue(first.endswith("_20261122-1326_a1b2c3.csv"))


if __name__ == "__main__":
    unittest.main()
