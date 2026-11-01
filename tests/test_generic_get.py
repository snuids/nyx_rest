"""Lecture d'un document par identifiant : un absent n'est pas une erreur.

C'est le chemin qu'emprunte la création d'un utilisateur dans l'interface : avant d'ouvrir le
formulaire, elle demande au serveur si l'adresse est libre. Un 404 y est donc une réponse
attendue, pas un incident — et pourtant il entrait dans le compteur d'erreurs.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))


class NotFoundError(Exception):
    """Reproduit l'exception du client (opensearchpy et elasticsearch-py la nomment pareil)."""
    status_code = 404


class ClusterDown(Exception):
    status_code = 503


class FakeES:
    def __init__(self, documents=None, failures=None, search_hits=None):
        self.documents = documents or {}
        self.failures = failures or {}
        self.search_hits = search_hits or []
        self.calls = []

    def get(self, index, id=None, doc_type=None):
        self.calls.append(("get", index, id))
        if id in self.failures:
            raise self.failures[id]
        if id in self.documents:
            return {"_id": id, "_index": index, "_source": self.documents[id]}
        raise NotFoundError(f"{index}/{id} not found")

    def search(self, index, body=None, doc_type=None):
        self.calls.append(("search", index, body))
        return {"hits": {"hits": self.search_hits}}


class FetchDocumentTest(unittest.TestCase):
    def setUp(self):
        import es_read   # noqa: E402  (le chemin est posé plus haut)
        self.data = es_read

    def test_a_present_document_is_returned(self):
        es = FakeES({"bob@example.com": {"login": "bob@example.com"}})
        error, doc = self.data.fetch_document(es, "nyx_user", "bob@example.com", 8)
        self.assertEqual(error, "")
        self.assertEqual(doc["_id"], "bob@example.com")

    def test_a_missing_document_is_not_an_error(self):
        """Le cœur du défaut : trois « erreurs » par création de compte, toutes des 404."""
        es = FakeES()
        error, doc = self.data.fetch_document(es, "nyx_user", "nouveau@example.com", 8)
        self.assertEqual(error, "", "un document absent ne doit pas remplir le compteur d'erreurs")
        self.assertIsNone(doc)

    def test_a_real_failure_stays_an_error(self):
        es = FakeES(failures={"bob@example.com": ClusterDown("cluster unavailable")})
        error, doc = self.data.fetch_document(es, "nyx_user", "bob@example.com", 8)
        self.assertEqual(error, "unable to get data")
        self.assertIsNone(doc)

    def test_a_user_indexed_under_another_id_is_found_by_login(self):
        """Huit comptes : e-mail en _id, nom court en `login`. Sans ce repli, doublon possible."""
        es = FakeES(search_hits=[{"_id": "amarchand", "_index": "nyx_user",
                                  "_source": {"login": "amarchand"}}])
        error, doc = self.data.fetch_document(es, "nyx_user", "amarchand", 8)
        self.assertEqual(error, "")
        self.assertEqual(doc["_id"], "amarchand")
        self.assertEqual(es.calls[-1][0], "search")

    def test_the_login_lookup_is_not_attempted_on_other_indices(self):
        es = FakeES()
        error, doc = self.data.fetch_document(es, "nyx_indice", "inconnu", 8)
        self.assertEqual((error, doc), ("", None))
        self.assertEqual([c[0] for c in es.calls], ["get"], "aucune recherche ne doit être tentée")

    def test_a_legacy_cluster_keeps_its_doc_type(self):
        es = FakeES({"bob": {"login": "bob"}})
        error, doc = self.data.fetch_document(es, "nyx_user", "bob", 6, doc_type="doc")
        self.assertEqual(error, "")
        self.assertEqual(doc["_id"], "bob")


if __name__ == "__main__":
    unittest.main()
