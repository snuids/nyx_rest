import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from helpers.disk_helper import (
    can_access_file_app, can_access_logs, get_all_file_paths, list_dir,
    normalize_log_path, resolve_under_root,
)


class FileAccessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "data"
        self.root.mkdir()
        self.outside = Path(self.temp.name) / "data-private"
        self.outside.mkdir()
        (self.root / "inside.log").write_text("inside")
        (self.outside / "private.log").write_text("outside")
        (self.root / "external.log").symlink_to(self.outside / "private.log")
        (self.root / "external-dir").symlink_to(self.outside, target_is_directory=True)

    def test_resolved_paths_reject_siblings_absolute_paths_and_symlinks(self):
        self.assertEqual(resolve_under_root(self.root, "inside.log"), str((self.root / "inside.log").resolve()))
        for path in (
            "../data-private/private.log",
            str(self.outside / "private.log"),
            "external.log",
            "external-dir/private.log",
        ):
            with self.subTest(path=path), self.assertRaises(ValueError):
                resolve_under_root(self.root, path)

    def test_listing_and_zip_enumeration_skip_symlinks_outside_root(self):
        names = {item["name"] for item in list_dir(str(self.root), "/", "", str(self.root))["data"]}
        self.assertEqual(names, {"inside.log"})
        paths = get_all_file_paths(str(self.root), str(self.root))
        self.assertEqual(paths, [str(self.root / "inside.log")])

    def test_upload_destination_cannot_escape_root(self):
        with self.assertRaises(ValueError):
            resolve_under_root(self.root, "../data-private/private.log")
        with self.assertRaises(ValueError):
            resolve_under_root(self.root, "external.log")
        self.assertEqual((self.outside / "private.log").read_text(), "outside")

    def test_app_privileges_and_log_privileges(self):
        app = {"privileges": ["files"]}
        self.assertFalse(can_access_file_app(app, {"privileges": ["user"]}))
        self.assertTrue(can_access_file_app(app, {"privileges": ["files"]}))
        self.assertTrue(can_access_file_app(app, {"privileges": ["admin"]}))
        self.assertFalse(can_access_logs({"privileges": ["user"]}))
        self.assertTrue(can_access_logs({"privileges": ["logs"]}))

    def test_legacy_log_paths_stay_within_log_root(self):
        with tempfile.TemporaryDirectory() as logs:
            self.assertEqual(resolve_under_root(logs, normalize_log_path("/logs")),
                             str(Path(logs).resolve()))
            self.assertEqual(resolve_under_root(logs, normalize_log_path("/logs/api.log")),
                             str((Path(logs) / "api.log").resolve()))
            self.assertEqual(resolve_under_root(logs, normalize_log_path("api.log")),
                             str((Path(logs) / "api.log").resolve()))
            for path in ("/etc/passwd", "/logs-private/secret", "/logs/../../etc/passwd",
                         "/logs//etc/passwd"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    resolve_under_root(logs, normalize_log_path(path))


if __name__ == "__main__":
    unittest.main()
