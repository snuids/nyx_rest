"""Routes de fichiers : le confinement des chemins, vu depuis la route.

`helpers/disk_helper.py` a ses propres tests (85 %). Ce qui manquait, c'est le câblage : est-ce
que `listdir` et `streamfile` appliquent bien ces gardes avant de toucher au disque, avec le
`rec_id` et le `path` que l'interface envoie vraiment. Les applications n'existent pas sur le
disque de test, donc un chemin accepté produit une erreur d'`OSError` interceptée, pas une fuite.
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
        """Intercepte `list_dir` : on teste le passage du garde, pas le disque du bac à sable."""
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
        # la racine de /logs, c'est la chaine vide : c'est ce que la console envoie
        # (`path=""`), pas "/" qui est un chemin absolu et se fait refuser par resolve_under_root
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
        # contrat actuel : "/" est un chemin absolu, donc refuse (meme pour un admin). Le
        # changer elargirait la racine sans que rien ne le signale.
        code, body, _ = self.call("post", "/api/v1/listdir", self.admin,
                                  {"rec_id": -1, "path": "/"})
        self.assertEqual(body["error"], "not allowed")

    def test_an_app_reserved_to_admins_is_unknown_for_a_simple_account(self):
        # `retrieve_app_info` renvoie (None, None) : le compte n'apprend pas meme que l'app existe
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
