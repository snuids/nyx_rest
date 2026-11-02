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
        self.assertEqual(calls, [], "the disk must not be touched when the guard refuses")

    def test_the_logs_directory_opens_with_the_logs_privilege(self):
        # the root of /logs is the empty string: that is what the console sends (`path=""`),
        # not "/", which is an absolute path and is refused by resolve_under_root
        calls = self._spy_on_list_dir()
        code, body, _ = self.call("post", "/api/v1/listdir", self.logs_user,
                                  {"rec_id": -1, "path": ""})
        self.assertEqual(body["error"], "")
        self.assertEqual(calls, ["/logs"], "the logs privilege must be enough to open /logs")

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
        self.assertNotEqual(body.get("error"), "", "a call with no file must return nothing")


@unittest.skipUnless(files_routes is not None, "routes.files non importable")
class DownloadAndUploadTests(RouteTestCase):
    """`/files` never had a test: the download turns a directory into a zip, the upload writes
    to disk, and both take the file name from the client. So the refusals come first, and the
    happy paths follow — a guard that refuses everything would pass a suite of refusals alone."""

    route_modules = (files_routes,)

    def setUp(self):
        super().setUp()
        import os
        import tempfile
        self.root = tempfile.mkdtemp(prefix="nyx-files-")
        self.addCleanup(self._remove_root)
        os.makedirs(os.path.join(self.root, "sous-dossier"), exist_ok=True)
        with open(os.path.join(self.root, "rapport.csv"), "w", encoding="utf-8") as handle:
            handle.write("a,b\n1,2\n")
        with open(os.path.join(self.root, "sous-dossier", "interne.txt"), "w", encoding="utf-8") as handle:
            handle.write("contenu interne\n")
        # the class-level APPS point at /data/nyx/... which does not exist on the test host
        self.es.docs["nyx_app"] = {
            "files-yul": {"type": "file-system", "privileges": ["YUL"],
                          "config": {"rootpath": self.root, "regex": ""}},
            "files-admin": {"type": "file-system", "privileges": ["admin"],
                            "config": {"rootpath": self.root}},
        }
        self.simple = self.login_as("alice@example.com", ["YUL"])

    def _remove_root(self):
        import os
        for base, dirs, files in os.walk(self.root, topdown=False):
            for name in files:
                os.unlink(os.path.join(base, name))
            for name in dirs:
                os.rmdir(os.path.join(base, name))
        os.rmdir(self.root)

    def download(self, query, token=None):
        return self.client.get("/api/v1/files?" + query,
                               headers={"Authorization": "Bearer " + (token or self.simple)})

    def test_a_single_file_comes_back_as_is(self):
        response = self.download("rec_id=files-yul&path=/&files=rapport.csv")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"a,b\n1,2\n")

    def test_a_directory_comes_back_as_a_zip(self):
        import os
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            response = self.download("rec_id=files-yul&path=/&files=sous-dossier")
        finally:
            os.chdir(cwd)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data[:2], b"PK", "a directory must be zipped, not streamed")

    def test_a_traversing_file_name_is_refused(self):
        response = self.download("rec_id=files-yul&path=/&files=../../etc/passwd")
        self.assertEqual(response.get_json()["error"], "not allowed")

    def test_an_empty_file_list_is_refused(self):
        response = self.download("rec_id=files-yul&path=/&files=")
        self.assertEqual(response.get_json()["error"], "error in file format")

    def test_an_app_the_account_cannot_open_is_refused(self):
        response = self.download("rec_id=files-admin&path=/&files=rapport.csv")
        self.assertEqual(response.get_json()["error"], "unknown app")

    def test_a_file_is_written_under_the_root(self):
        import base64
        import os
        code, body, _ = self.call("post", "/api/v1/files?rec_id=files-yul&path=/", self.simple,
                                  {"files": [{"file_name": "nouveau.txt",
                                              "data": base64.b64encode(b"bonjour").decode()}]})
        self.assertEqual(body["error"], "")
        written = os.path.join(self.root, "nouveau.txt")
        self.assertTrue(os.path.isfile(written))
        with open(written, "rb") as handle:
            self.assertEqual(handle.read(), b"bonjour")

    def test_a_traversing_file_name_writes_nothing_outside(self):
        import base64
        import os
        code, body, _ = self.call("post", "/api/v1/files?rec_id=files-yul&path=/", self.simple,
                                  {"files": [{"file_name": "../evasion.txt",
                                              "data": base64.b64encode(b"non").decode()}]})
        self.assertEqual(body["error"], "not allowed")
        self.assertFalse(os.path.exists(os.path.join(os.path.dirname(self.root), "evasion.txt")),
                         "nothing must be written outside the app root")

    def test_uploading_to_an_unknown_app_is_refused(self):
        import base64
        code, body, _ = self.call("post", "/api/v1/files?rec_id=nexiste-pas&path=/", self.simple,
                                  {"files": [{"file_name": "a.txt",
                                              "data": base64.b64encode(b"x").decode()}]})
        self.assertEqual(body["error"], "unknown app")


@unittest.skipUnless(files_routes is not None, "routes.files non importable")
class FileHelperTests(unittest.TestCase):
    """The helpers the routes lean on, without a request around them."""

    def test_a_file_shorter_than_the_window_is_read_whole(self):
        import os
        import tempfile
        with tempfile.NamedTemporaryFile("wb", suffix=".log", delete=False) as handle:
            handle.write(b"court")
            path = handle.name
        try:
            self.assertEqual(files_routes.read_last_bytes(path, 32000), b"court")
        finally:
            os.unlink(path)

    def test_only_the_tail_of_a_long_file_comes_back(self):
        import os
        import tempfile
        with tempfile.NamedTemporaryFile("wb", suffix=".log", delete=False) as handle:
            handle.write(b"x" * 1000 + b"FIN")
            path = handle.name
        try:
            self.assertEqual(files_routes.read_last_bytes(path, 3), b"FIN")
        finally:
            os.unlink(path)

    def test_a_random_name_has_the_requested_length(self):
        name = files_routes.randomString(12)
        self.assertEqual(len(name), 12)
        self.assertTrue(name.isalpha())


if __name__ == "__main__":
    unittest.main()
