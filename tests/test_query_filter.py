"""Facets: one aggregation per selector, and only for an index the account may read.

`queryFilter` is what fills the drop-downs of a console screen. It reads `nyx_app`, builds an
aggregation for every `queryselecter`, and returns the buckets. Two rules matter here: a `text`
field is faceted on its `.keyword` sub-field (otherwise every value is analysed and the list is
useless), and a facet must never be built on an index the account may not open — a bucket lists
the distinct values of a field, which is data, even though no document is returned.
"""

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS   # noqa: E402

if HAVE_ROUTE_DEPS:
    import state                                           # noqa: E402
    import routes.data as data_routes                      # noqa: E402
else:                                                      # pragma: no cover
    data_routes = state = None


APPS = {
    "facets": {
        "type": "generic-table",
        "privileges": ["YUL", "admin"],
        "config": {
            "index": "dossier",
            "timefield": "date",
            "queryfilters": [
                {"type": "queryselecter", "field": "statut", "querySize": "7"},
                {"type": "queryselecter", "field": "ville"},
            ],
        },
    },
    "no-facets": {
        "type": "generic-table",
        "privileges": ["YUL"],
        "config": {"index": "dossier"},
    },
}

MAPPINGS = {"dossier": {"statut": "text", "ville": "keyword", "date": "date"}}

AGGREGATIONS = {
    "dossier": {
        "statut.keyword": [{"key": "actif", "doc_count": 3}, {"key": "clos", "doc_count": 1}],
        "ville": [{"key": "Bruxelles", "doc_count": 2}],
    }
}


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class UnmappedIndexTests(RouteTestCase):
    """An index created without a mapping: the facets are built on the raw field names."""

    route_modules = (data_routes,)

    def setUp(self):
        super().setUp()
        self.es.docs["nyx_app"] = APPS
        # `get_mapping` returns {"mappings": {}} — no `properties` at all, which used to raise a
        # KeyError out of `get_mappings` and answer a 500.
        self.es.indices.get_mapping = lambda index=None, **kwargs: {index: {"mappings": {}}}
        # the fake cluster answers under the field names the route asks for
        self.es.aggregations.update({"dossier": {
            "statut": [{"key": "actif", "doc_count": 3}],
            "ville": [{"key": "Bruxelles", "doc_count": 2}],
        }})
        self.user = self.login_as("alice@example.com", ["YUL"])

    def test_a_screen_on_an_unmapped_index_answers_and_facets_the_raw_fields(self):
        code, body, _ = self.call("post", "/api/v1/queryFilter/facets", self.user, {})

        self.assertEqual(code, 200, body)
        # one search per selector: without a mapping, each is faceted on the field itself
        faceted = [list(body["aggregations"])[0]
                   for index, body in self.es.searches if index == "dossier"]
        self.assertEqual(sorted(faceted), ["statut", "ville"])


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class QueryFilterTests(RouteTestCase):
    route_modules = (data_routes,)

    def setUp(self):
        super().setUp()
        self.es.docs["nyx_app"] = APPS
        # FakeIndices shares this dict, so the mapping lookup sees what the test declares
        self.es.mappings.update(MAPPINGS)
        self.es.aggregations.update(AGGREGATIONS)
        self.user = self.login_as("alice@example.com", ["YUL"])

    def filter(self, body=None, rec_id="facets", token=None):
        return self.call("post", "/api/v1/queryFilter/" + rec_id, token or self.user, body or {})

    def facets(self, body=None, **kwargs):
        code, response, _ = self.filter(body, **kwargs)
        return response.get("queryfilters") or response

    def aggregation_of(self, field):
        """The aggregation clause the route sent for a field, or None."""
        for index, body in self.es.searches:
            if index != "dossier":
                continue
            aggregations = (body or {}).get("aggregations") or {}
            if field in aggregations:
                return aggregations[field]
        return None

    def test_an_app_without_query_filters_says_so(self):
        code, body, _ = self.filter(rec_id="no-facets")
        self.assertEqual(body["error"], "NO QUERY FILTERS")
        self.assertEqual(self.es.searches, [], "nothing to facet means nothing to search")

    def test_a_text_field_is_faceted_on_its_keyword_sub_field(self):
        facets = self.facets()
        self.assertEqual(facets[0]["buckets"][0], {"key": "actif", "doc_count": 3})
        self.assertIsNotNone(self.aggregation_of("statut.keyword"),
                             "an analysed field must be faceted on its .keyword sub-field")
        self.assertIsNone(self.aggregation_of("statut"),
                          "the analysed field itself would return one bucket per token")

    def test_a_keyword_field_is_faceted_as_is(self):
        self.facets()
        self.assertIsNotNone(self.aggregation_of("ville"))

    def test_the_size_comes_from_the_screen_configuration(self):
        self.facets()
        self.assertEqual(self.aggregation_of("statut.keyword")["terms"]["size"], 7)
        self.assertEqual(self.aggregation_of("ville")["terms"]["size"], 200,
                         "a selector with no querySize keeps the default")

    def test_a_selected_value_narrows_the_facets_built_after_it(self):
        self.facets({"selected": ["actif", ""]})
        # the second search is the one that carries the first selection
        second = [body for index, body in self.es.searches if index == "dossier"][1]
        sent = str(second["query"])
        self.assertIn('statut.keyword:"actif"', sent)

    def test_a_timerange_is_sent_as_a_range_clause(self):
        self.facets({"timerange": ["2026-01-01T00:00:00", "2026-02-01T00:00:00"]})
        bodies = [body for index, body in self.es.searches if index == "dossier"]
        must = bodies[-1]["query"]["bool"]["must"]
        ranges = [clause for clause in must if "range" in clause]
        self.assertEqual(len(ranges), 1, "the time field must bound the facets too")
        self.assertEqual(ranges[0]["range"]["date"]["format"], "epoch_millis")

    def test_an_account_without_right_on_the_index_gets_no_facet(self):
        self.es.docs["nyx_indice"] = {
            "rule-1": {"indicepattern": "dossier", "privileges": ["admin"]},
        }
        state.last_indices_refresh = datetime.now() - timedelta(hours=1)
        facets = self.facets()
        self.assertEqual([f["buckets"] for f in facets], [[], []],
                         "a bucket lists the distinct values of a field: it is data")
        self.assertEqual([body for index, body in self.es.searches if index == "dossier"], [],
                         "nothing may be searched on an index the account cannot read")

    def test_the_administration_account_still_gets_its_facets(self):
        self.es.docs["nyx_indice"] = {
            "rule-1": {"indicepattern": "dossier", "privileges": ["admin"]},
        }
        state.last_indices_refresh = datetime.now() - timedelta(hours=1)
        admin = self.login_as("bob@example.com", ["admin"])
        facets = self.facets(token=admin)
        self.assertEqual(facets[0]["buckets"][0]["key"], "actif")


if __name__ == "__main__":
    unittest.main()
