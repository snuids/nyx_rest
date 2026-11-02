"""File routes: path confinement, seen from the route.

`helpers/disk_helper.py` has its own tests (85 %). What was missing is the wiring: whether
`listdir` and `streamfile` actually apply those guards before touching the disk, with the
`rec_id` and the `path` the interface really sends. The applications do not exist on the test
disk, so an accepted path produces an intercepted `OSError`, not a leak.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS   # noqa: E402

if HAVE_ROUTE_DEPS:
    import routes.files as files_routes                     # noqa: E402
else:                                                       # pragma: no cover
    files_routes = None


APPS = {
    "files-yul": {"type": "file-system", "title": "Fichiers YUL", "privileges": ["YUL"],
                  "config": {"rootpath": "/data/nyx/files", "regex": ""}},
    "files-admin": {"type": "file-system", "title": "Fichiers admin",
                    "privileges": ["admin"], "config": {"rootpath": "/data/nyx/admin"}},
    "un-tableau": {"type": "generic-table", "title": "Pas un repertoire", "privileges": []},
}


@unittest.skipUnless(files_routes is not None, "routes.files non importable")
class ListDirTests(RouteTestCase):
    route_modules = (files_routes,)
    documents = {"nyx_app": APPS}

    def setUp(self):
        super().setUp()
        self.simple = self.login_as("alice@example.com", ["YUL"])
        self.logs_user = self.login_as("logreader@example.com", ["logs"])
        self.admin = self.login_as("bob@example.com", ["admin"])

    def _spy_on_list_dir(self):
        """Intercepts `list_dir`: we test the guard being passed, not the sandbox disk."""
        calls = []
        original = files_routes.list_dir

        def fake(dirpath, path, regex, prepath):
            calls.append(dirpath)
            return {"error": "", "entries": []}

        files_routes.list_dir = fake
        self.addCleanup(setattr, files_routes, "list_dir", original)
        return calls

    def test_the_logs_directory_is_closed_without_the_privilege(self):
        calls = self._spy_on_list_dir()
        code, body, _ = self.call("post", "/api/v1/listdir", self.simple,
                                  {"rec_id": -1, "path": ""})
        self.assertEqual(body["error"], "not allowed")
        self.assertEqual(calls, [], "le disque ne doit pas etre touche quand le garde refuse")

    def test_the_logs_directory_opens_with_the_logs_privilege(self):
        # the root of /logs is the empty string: that is what the console sends (`path=""`),
        # not "/", which is an absolute path and is refused by resolve_under_root
        calls = self._spy_on_list_dir()
        code, body, _ = self.call("post", "/api/v1/listdir", self.logs_user,
                                  {"rec_id": -1, "path": ""})
        self.assertEqual(body["error"], "")
        self.assertEqual(calls, ["/logs"], "le privilege logs doit suffire a ouvrir /logs")

    def test_the_legacy_logs_prefix_is_still_accepted(self):
        calls = self._spy_on_list_dir()
        code, body, _ = self.call("post", "/api/v1/listdir", self.logs_user,
                                  {"rec_id": -1, "path": "/logs/nginx"})
        self.assertEqual(body["error"], "")
        self.assertEqual(calls, ["/logs/nginx"])

    def test_a_slash_is_not_the_root_of_the_logs_directory(self):
        # current contract: "/" is an absolute path, hence refused (even for an admin).
        # Changing that would widen the root without anything signalling it.
        code, body, _ = self.call("post", "/api/v1/listdir", self.admin,
                                  {"rec_id": -1, "path": "/"})
        self.assertEqual(body["error"], "not allowed")

    def test_an_app_reserved_to_admins_is_unknown_for_a_simple_account(self):
        # `retrieve_app_info` returns (None, None): the account does not even learn that the
        # app exists
        code, body, _ = self.call("post", "/api/v1/listdir", self.simple,
                                  {"rec_id": "files-admin", "path": "/"})
        self.assertEqual(body["error"], "unknown app")

    def test_an_unknown_app_is_refused(self):
        code, body, _ = self.call("post", "/api/v1/listdir", self.simple,
                                  {"rec_id": "nexiste-pas", "path": "/"})
        self.assertEqual(body["error"], "unknown app")

    def test_an_index_that_is_not_a_directory_is_refused(self):
        code, body, _ = self.call("post", "/api/v1/listdir", self.simple,
                                  {"rec_id": "un-tableau", "path": "/"})
        self.assertEqual(body["error"], "unknown app")

    def test_a_traversal_out_of_the_app_root_is_refused(self):
        for path in ("../../etc", "..", "/etc"):
            code, body, _ = self.call("post", "/api/v1/listdir", self.simple,
                                      {"rec_id": "files-yul", "path": path})
            self.assertEqual(body["error"], "not allowed", path)

    def test_a_missing_parameter_is_refused(self):
        code, body, _ = self.call("post", "/api/v1/listdir", self.simple, {"rec_id": "files-yul"})
        self.assertTrue(body["error"].startswith("MISSING_PARAM"))


@unittest.skipUnless(files_routes is not None, "routes.files non importable")
class StreamFileTests(RouteTestCase):
    route_modules = (files_routes,)
    documents = {"nyx_app": APPS}

    def setUp(self):
        super().setUp()
        self.simple = self.login_as("alice@example.com", ["YUL"])
        self.admin = self.login_as("bob@example.com", ["admin"])

    def test_a_path_outside_the_allowed_roots_is_refused(self):
        for path in ("/etc/passwd", "../../etc/passwd", "/root/.ssh/id_rsa"):
            code, body, _ = self.call("get", "/api/v1/streamfile?file=" + path, self.simple)
            self.assertEqual(body["error"], "not allowed", path)
            self.assertIn(body["data"], (None, ""))

    def test_the_logs_are_closed_without_the_privilege(self):
        code, body, _ = self.call("get", "/api/v1/streamfile?file=/logs/nginx/access.log",
                                  self.simple)
        self.assertEqual(body["error"], "not allowed")

    def test_an_error_does_not_leak_the_filesystem_layout(self):
        code, body, _ = self.call("get", "/api/v1/streamfile?file=/etc/passwd", self.simple)
        self.assertNotIn("/etc", body["error"])
        self.assertNotIn("passwd", body["error"].lower())

    def test_a_missing_file_parameter_is_refused(self):
        code, body, _ = self.call("get", "/api/v1/streamfile", self.simple)
        self.assertNotEqual(body.get("error"), "", "un appel sans fichier ne doit rien rendre")


if __name__ == "__main__":
    unittest.main()
