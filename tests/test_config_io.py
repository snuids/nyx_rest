"""
Route-level tests for the configuration bundle endpoints.

They run the real routes through a Flask test client, with a fake Elasticsearch in place of
``state.es``. Flask, Flask-RESTX and cachetools are only needed for this file: the suite is
skipped when they are absent (the same way the other tests in this folder are optional).
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:
    from flask import Flask
    from flask_restx import Api

    HAVE_WEB_STACK = True
except Exception:  # pragma: no cover - depends on the machine running the tests
    HAVE_WEB_STACK = False

import state
from config_bundle import BUNDLE_FORMAT, BUNDLE_VERSION


class FakeIndices:
    def __init__(self):
        self.refreshed = []

    def refresh(self, index=None):
        self.refreshed.append(index)


class FakeES:
    """Just enough of the Elasticsearch client for the bundle routes."""

    def __init__(self, documents=None):
        self.documents = documents or {}
        self.bulk_body = None
        self.indices = FakeIndices()

    def search(self, index=None, body=None, doc_type=None):
        if index not in self.documents:
            raise Exception("index_not_found_exception")
        return {"hits": {"hits": self.documents[index]}}

    def bulk(self, body):
        self.bulk_body = body
        items = []
        for line in body.strip().split("\n"):
            operation = json.loads(line)
            if "index" in operation:
                items.append(
                    {
                        "index": {
                            "_index": operation["index"]["_index"],
                            "_id": operation["index"]["_id"],
                            "status": 201,
                        }
                    }
                )
            elif "delete" in operation:
                items.append(
                    {
                        "delete": {
                            "_index": operation["delete"]["_index"],
                            "_id": operation["delete"]["_id"],
                            "status": 200,
                        }
                    }
                )
        return {"items": items}


def bundle(indices, api_version="3.22.0"):
    return {
        "format": BUNDLE_FORMAT,
        "formatVersion": BUNDLE_VERSION,
        "platform": {"apiVersion": api_version, "elasticVersion": 7, "instance": "dev"},
        "indices": indices,
    }


@unittest.skipUnless(HAVE_WEB_STACK, "flask / flask-restx not installed")
class ConfigBundleRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from routes.configio import register

        cls.app = Flask(__name__)
        api = Api(cls.app, doc="/api/doc/")
        namespace = api.namespace("api/v1")
        register(cls.app, api, namespace)
        cls.client = cls.app.test_client()

        state.VERSION = "3.22.0"
        state.elkversion = 7

    def setUp(self):
        state.tokens.clear()
        state.tokens["admin-token"] = {
            "login": "admin",
            "id": "admin",
            "privileges": ["admin"],
            "language": "en",
        }
        state.tokens["user-token"] = {
            "login": "plain",
            "id": "plain",
            "privileges": ["user"],
            "language": "en",
        }
        self.es = FakeES(
            {
                "nyx_app": [
                    {
                        "_id": "deliveries",
                        "_source": {
                            "title": "Deliveries",
                            "type": "generic-table",
                            "category": "Ops",
                            "config": {"index": "deliveries*"},
                        },
                    }
                ],
                "nyx_indice": [
                    {
                        "_id": "deliveries",
                        "_source": {"indicepattern": "deliveries.*", "privileges": ["ops"]},
                    }
                ],
            }
        )
        state.es = self.es

    # -- export -----------------------------------------------------------

    def test_export_returns_a_bundle_of_the_design_indices(self):
        response = self.client.get("/api/v1/config/export?token=admin-token")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["error"], "")
        self.assertEqual(payload["counts"]["nyx_app"], 1)
        self.assertEqual(payload["bundle"]["format"], BUNDLE_FORMAT)
        self.assertEqual(payload["bundle"]["platform"]["apiVersion"], "3.22.0")
        self.assertEqual(payload["bundle"]["indices"]["nyx_app"][0]["_id"], "deliveries")
        # indices that do not exist on the instance are exported empty, not as an error
        self.assertEqual(payload["bundle"]["indices"]["nyx_translation"], [])

    def test_export_accepts_a_subset_of_indices(self):
        response = self.client.get("/api/v1/config/export?token=admin-token&indices=nyx_app")
        payload = response.get_json()
        self.assertEqual(list(payload["bundle"]["indices"]), ["nyx_app"])

    def test_export_refuses_an_index_that_is_not_portable(self):
        response = self.client.get("/api/v1/config/export?token=admin-token&indices=nyx_user")
        payload = response.get_json()
        self.assertEqual(payload["error"], "UNKNOWN_INDEX")
        self.assertEqual(payload["details"], ["nyx_user"])

    def test_export_needs_an_admin_token(self):
        self.assertEqual(
            self.client.get("/api/v1/config/export?token=user-token").get_json()["error"],
            "NO_PRIVILEGE",
        )
        self.assertEqual(
            self.client.get("/api/v1/config/export").get_json()["error"], "NO_TOKEN"
        )

    # -- import -----------------------------------------------------------

    def test_import_without_apply_only_returns_the_plan(self):
        payload = bundle(
            {
                "nyx_app": [
                    {
                        "_id": "deliveries",
                        "_source": {
                            "title": "Deliveries (prod)",
                            "type": "generic-table",
                            "category": "Ops",
                            "config": {"index": "deliveries*"},
                        },
                    },
                    {"_id": "activity", "_source": {"title": "Activity", "type": "internal"}},
                ]
            }
        )
        response = self.client.post("/api/v1/config/import?token=admin-token", json=payload)
        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(result["error"], "")
        self.assertTrue(result["dry_run"])
        self.assertFalse(result["applied"])
        self.assertEqual(result["summary"]["update"], 1)
        self.assertEqual(result["summary"]["create"], 1)
        self.assertIsNone(self.es.bulk_body, "a dry run must not write anything")
        changed = [c for c in result["changes"] if c["action"] == "update"][0]
        self.assertEqual(changed["fieldsChanged"], ["title"])

    def test_import_with_apply_writes_a_bulk_payload(self):
        payload = bundle({"nyx_app": [{"_id": "activity", "_source": {"title": "Activity"}}]})
        response = self.client.post("/api/v1/config/import?token=admin-token&apply=1", json=payload)
        result = response.get_json()
        self.assertEqual(result["error"], "")
        self.assertTrue(result["applied"])
        self.assertEqual(result["operations"], 1)
        self.assertIn('"_id": "activity"', self.es.bulk_body)
        self.assertIsInstance(result["appliedAt"], str)
        self.assertEqual(self.es.indices.refreshed, ["nyx_app"])

    def test_import_prune_deletes_what_the_bundle_omits(self):
        payload = bundle({"nyx_indice": []})
        response = self.client.post(
            "/api/v1/config/import?token=admin-token&apply=1&prune=1", json=payload
        )
        result = response.get_json()
        self.assertEqual(result["summary"]["delete"], 1)
        self.assertIn('"delete"', self.es.bulk_body)
        self.assertIn('"deliveries"', self.es.bulk_body)

    def test_import_refuses_a_non_portable_index(self):
        payload = bundle({"nyx_user": [{"_id": "x", "_source": {"password": "hash"}}]})
        result = self.client.post("/api/v1/config/import?token=admin-token", json=payload).get_json()
        self.assertEqual(result["error"], "INVALID_BUNDLE")
        self.assertTrue(any("nyx_user" in detail for detail in result["details"]))

    def test_import_refuses_a_newer_format(self):
        payload = bundle({"nyx_app": []})
        payload["formatVersion"] = BUNDLE_VERSION + 1
        result = self.client.post("/api/v1/config/import?token=admin-token", json=payload).get_json()
        self.assertEqual(result["error"], "INVALID_BUNDLE")

    def test_import_rejects_a_body_that_is_not_json(self):
        result = self.client.post(
            "/api/v1/config/import?token=admin-token", data="not json"
        ).get_json()
        self.assertEqual(result["error"], "UNABLE_TO_DECODE_BODY")

    def test_import_needs_an_admin_token(self):
        payload = bundle({"nyx_app": []})
        result = self.client.post("/api/v1/config/import?token=user-token", json=payload).get_json()
        self.assertEqual(result["error"], "NO_PRIVILEGE")

    def test_import_that_changes_nothing_reports_no_operation(self):
        payload = bundle(
            {
                "nyx_app": [
                    {
                        "_id": "deliveries",
                        "_source": {
                            "title": "Deliveries",
                            "type": "generic-table",
                            "category": "Ops",
                            "config": {"index": "deliveries*"},
                        },
                    }
                ]
            }
        )
        result = self.client.post(
            "/api/v1/config/import?token=admin-token&apply=1", json=payload
        ).get_json()
        self.assertEqual(result["error"], "")
        self.assertFalse(result["applied"])
        self.assertEqual(result["summary"]["unchanged"], 1)
        self.assertIsNone(self.es.bulk_body)


if __name__ == "__main__":
    unittest.main()
