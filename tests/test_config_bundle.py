import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from config_bundle import (
    BUNDLE_FORMAT,
    BUNDLE_VERSION,
    build_bulk_lines,
    build_bundle,
    clean_source,
    compute_plan,
    describe_plan,
    summarise,
    validate_bundle,
)


def make_bundle(indices):
    return build_bundle(indices, api_version="3.22.0", elastic_version=7, instance="dev")


APP = {
    "_id": "deliveries",
    "_source": {
        "category": "Operations",
        "title": "Deliveries",
        "type": "generic-table",
        "config": {"index": "deliveries*", "pkey": "_id"},
    },
}
INDICE = {"_id": "deliveries", "_source": {"indicepattern": "deliveries.*", "privileges": ["ops"]}}


class BundleShapeTests(unittest.TestCase):
    def test_bundle_is_sorted_and_carries_its_format(self):
        bundle = make_bundle(
            {"nyx_app": [APP, {"_id": "activity", "_source": {"title": "Activity"}}]}
        )
        self.assertEqual(bundle["format"], BUNDLE_FORMAT)
        self.assertEqual(bundle["formatVersion"], BUNDLE_VERSION)
        self.assertEqual(bundle["platform"]["apiVersion"], "3.22.0")
        self.assertEqual([d["_id"] for d in bundle["indices"]["nyx_app"]], ["activity", "deliveries"])

    def test_read_time_fields_are_not_exported(self):
        bundle = make_bundle(
            {"nyx_app": [{"_id": "x", "_source": {"title": "X", "rec_id": "x", "_index": "nyx_app"}}]}
        )
        self.assertEqual(bundle["indices"]["nyx_app"][0]["_source"], {"title": "X"})
        self.assertEqual(clean_source({"a": 1, "_id": "z"}), {"a": 1})


class ValidationTests(unittest.TestCase):
    def test_a_valid_bundle_has_no_errors(self):
        self.assertEqual(validate_bundle(make_bundle({"nyx_app": [APP]})), [])

    def test_wrong_format_is_rejected(self):
        bundle = make_bundle({"nyx_app": [APP]})
        bundle["format"] = "something-else"
        self.assertIn("format", " ".join(validate_bundle(bundle)))

    def test_newer_format_version_is_refused(self):
        bundle = make_bundle({"nyx_app": [APP]})
        bundle["formatVersion"] = BUNDLE_VERSION + 1
        self.assertTrue(any("newer" in error for error in validate_bundle(bundle)))

    def test_secrets_and_instance_state_are_refused(self):
        for index in ("nyx_user", "nyx_apikey", "nyx_reportdef", "nyx_config"):
            with self.subTest(index=index):
                bundle = make_bundle({"nyx_app": [APP]})
                bundle["indices"][index] = [{"_id": "leak", "_source": {"password": "hash"}}]
                errors = validate_bundle(bundle)
                self.assertTrue(any(index in error for error in errors), errors)

    def test_unknown_index_is_refused(self):
        bundle = make_bundle({"nyx_app": [APP]})
        bundle["indices"]["my_own_index"] = [{"_id": "a", "_source": {}}]
        self.assertTrue(any("my_own_index" in error for error in validate_bundle(bundle)))

    def test_duplicate_and_missing_ids_are_reported(self):
        bundle = make_bundle({"nyx_app": [APP]})
        bundle["indices"]["nyx_app"] = [
            {"_id": "a", "_source": {}},
            {"_id": "a", "_source": {}},
            {"_source": {}},
        ]
        errors = validate_bundle(bundle)
        self.assertTrue(any("duplicate" in error for error in errors), errors)
        self.assertTrue(any("no valid _id" in error for error in errors), errors)


class PlanTests(unittest.TestCase):
    def test_create_update_unchanged_and_delete(self):
        bundle = make_bundle(
            {
                "nyx_app": [
                    APP,
                    {"_id": "changed", "_source": {"title": "New title"}},
                    {"_id": "same", "_source": {"title": "Same"}},
                ]
            }
        )
        current = {
            "nyx_app": [
                {"_id": "changed", "_source": {"title": "Old title", "order": 1}},
                {"_id": "same", "_source": {"title": "Same"}},
                {"_id": "gone", "_source": {"title": "Only here"}},
            ]
        }
        plan = compute_plan(bundle, current, prune=False)
        actions = {(c["index"], c["id"]): c["action"] for c in plan["changes"]}
        self.assertEqual(actions[("nyx_app", "deliveries")], "create")
        self.assertEqual(actions[("nyx_app", "changed")], "update")
        self.assertEqual(actions[("nyx_app", "same")], "unchanged")
        self.assertNotIn(("nyx_app", "gone"), actions)
        self.assertEqual(plan["summary"]["create"], 1)
        self.assertEqual(plan["summary"]["update"], 1)
        self.assertEqual(plan["summary"]["delete"], 0)

    def test_prune_plans_the_deletions(self):
        bundle = make_bundle({"nyx_app": [APP]})
        current = {"nyx_app": [{"_id": "gone", "_source": {"title": "Only here"}}]}
        plan = compute_plan(bundle, current, prune=True)
        actions = {(c["id"], c["action"]) for c in plan["changes"]}
        self.assertIn(("gone", "delete"), actions)
        self.assertEqual(plan["summary"]["delete"], 1)

    def test_an_update_lists_the_changed_fields(self):
        bundle = make_bundle({"nyx_app": [{"_id": "x", "_source": {"title": "B", "order": 2}}]})
        current = {"nyx_app": [{"_id": "x", "_source": {"title": "A", "order": 2}}]}
        plan = compute_plan(bundle, current)
        change = [c for c in plan["changes"] if c["action"] == "update"][0]
        self.assertEqual(change["fieldsChanged"], ["title"])

    def test_key_order_is_not_a_change(self):
        bundle = make_bundle({"nyx_app": [{"_id": "x", "_source": {"a": 1, "b": 2}}]})
        current = {"nyx_app": [{"_id": "x", "_source": {"b": 2, "a": 1}}]}
        plan = compute_plan(bundle, current)
        self.assertEqual(plan["summary"]["update"], 0)
        self.assertEqual(plan["summary"]["unchanged"], 1)

    def test_an_index_absent_from_the_target_is_created_wholesale(self):
        bundle = make_bundle({"nyx_translation": [{"_id": "t1", "_source": {"area": "menus"}}]})
        plan = compute_plan(bundle, {}, prune=True)
        self.assertEqual(plan["summary"]["create"], 1)
        self.assertEqual(plan["summary"]["delete"], 0)

    def test_summary_and_description(self):
        changes = [
            {"index": "nyx_app", "id": "a", "action": "update", "fieldsChanged": ["title"]},
            {"index": "nyx_app", "id": "b", "action": "unchanged", "fieldsChanged": []},
        ]
        summary = summarise(changes)
        self.assertEqual(summary, {"create": 0, "update": 1, "unchanged": 1, "delete": 0, "total": 2, "pending": 1})
        text = describe_plan({"changes": changes})
        self.assertIn("update", text)
        self.assertNotIn("unchanged", text)


class BulkTests(unittest.TestCase):
    def test_bulk_lines_carry_the_documents_to_write(self):
        bundle = make_bundle({"nyx_app": [APP]})
        plan = compute_plan(bundle, {})
        body, operations = build_bulk_lines(plan["changes"], bundle)
        self.assertEqual(operations, 1)
        lines = body.strip().split("\n")
        self.assertEqual(len(lines), 2)
        self.assertIn('"_index": "nyx_app"', lines[0])
        self.assertIn('"_id": "deliveries"', lines[0])
        self.assertIn('"title": "Deliveries"', lines[1])
        self.assertNotIn("_type", lines[0])

    def test_nothing_to_write_produces_an_empty_payload(self):
        bundle = make_bundle({"nyx_app": [APP]})
        plan = compute_plan(bundle, {"nyx_app": [APP]})
        body, operations = build_bulk_lines(plan["changes"], bundle)
        self.assertEqual((body, operations), ("", 0))

    def test_deletions_are_planned_with_the_right_action(self):
        bundle = make_bundle({"nyx_app": [APP]})
        current = {"nyx_app": [{"_id": "gone", "_source": {}}]}
        plan = compute_plan(bundle, current, prune=True)
        body, operations = build_bulk_lines(plan["changes"], bundle)
        self.assertIn('"delete"', body)
        self.assertEqual(operations, 2)  # one create + one delete


if __name__ == "__main__":
    unittest.main()
