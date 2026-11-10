"""Tests for the Kibana URL helpers and the menu builder.

`kibana_helpers.py` was covered at 15 %: a console menu is built by `computeMenus`, which asks the
cluster for every application, drops what the account may not see, computes a Kibana embed URL per
dashboard and rewrites what does not match the stored document. None of that had a test, and the URL
is the one piece where a mistake is visible to every user at once.
"""

import copy
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import kibana_helpers                                                          # noqa: E402
import state                                                                   # noqa: E402


class CleanUrlTests(unittest.TestCase):
    def test_an_old_embed_code_becomes_a_bare_url(self):
        html = ('<iframe src="https://energy.example/kibana/app/dashboards#/view/abc" '
                'height="600" width="800"></iframe>')

        self.assertEqual(
            kibana_helpers.clean_kibana_url_0(html),
            "energy.example/kibananyx/app/dashboards#/view/abc",
        )

    def test_a_url_without_embed_code_is_left_alone(self):
        self.assertEqual(
            kibana_helpers.clean_kibana_url_0("energy.example/kibana/app/x"),
            "energy.example/kibananyx/app/x",
        )


class CleanQueryTests(unittest.TestCase):
    def test_an_empty_query_is_replaced_by_the_user_filter(self):
        url = "dash?embed=true&_g=(query:'')"

        cleaned = kibana_helpers.clean_kibana_url(url, "tenant", ["a", "b"])

        self.assertIn("query:'(tenant:a OR tenant:b)'", cleaned)

    def test_an_existing_query_keeps_its_terms_and_the_filter_is_and_ed(self):
        url = "dash?_g=(query:'status:ok')"

        cleaned = kibana_helpers.clean_kibana_url(url, "tenant", ["a"])

        self.assertIn("query:'status:ok AND (tenant:a)'", cleaned)

    def test_a_url_without_query_is_returned_unchanged(self):
        url = "dash?embed=true&_g=(refreshInterval:(pause:!t,value:0))"

        self.assertEqual(kibana_helpers.clean_kibana_url(url, "tenant", ["a"]), url)


class DictDashboardTests(unittest.TestCase):
    def test_the_dashboards_are_keyed_by_the_id_after_the_type_prefix(self):
        es = mock.Mock()
        es.search.return_value = {"hits": {"hits": [
            {"_id": "dashboard:one", "_source": {}},
            {"_id": "dashboard:two", "_source": {}},
        ]}}

        dashboards = kibana_helpers.get_dict_dashboards(es)

        self.assertEqual(sorted(dashboards), ["one", "two"])
        self.assertEqual(es.search.call_args.kwargs.get("size"), 10000)
        self.assertEqual(es.search.call_args.kwargs.get("index"), ".kibana*")


def a_dashboard(namespace="energy", version="7.0.0"):
    return {
        "_id": "dashboard:abc",
        "_source": {
            "dashboard": {
                "description": "Conso",
                # True/False must travel as Kibana's !t/!f
                "optionsJSON": json.dumps({"useMargins": True, "hidePanelTitles": False}),
                "panelsJSON": json.dumps([
                    {"id": "p1", "type": "visualization", "panelRefName": "panel_0"},
                    {"id": "p2", "type": "visualization",
                     "embeddableConfig": {"colors": {"a b%": "#ffffff"},
                                          "vis": {"colors": {"c d": "#000000"}}}},
                ]),
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({"query": {"query_string": {"query": "status:ok"}}})
                },
            },
            "references": [{"name": "panel_0", "id": "ref-1", "type": "visualization"}],
            "migrationVersion": {"dashboard": version},
            "namespace": namespace,
        },
    }


def an_app(**config):
    base = {"kibanaId": "abc", "title": "Conso",
            "url": "/dashboard/abc", "shorturl": "/app/dashboards#/view/abc"}
    base.update(config)
    return {"config": base}


class ComputeKibanaUrlTests(unittest.TestCase):
    def test_an_app_without_a_dashboard_keeps_its_stored_url(self):
        appl = {"config": {"kibanaId": None, "url": "https://grafana.example/x"}}

        self.assertEqual(kibana_helpers.compute_kibana_url({}, appl), "https://grafana.example/x")

    def test_an_unknown_dashboard_is_refused(self):
        with self.assertLogs(level="ERROR"):
            url, shorturl = kibana_helpers.compute_kibana_url({}, an_app())

        self.assertEqual((url, shorturl), ("INVALIDURL", "INVALIDURL"))

    def test_the_embed_url_carries_the_time_range_the_panels_and_the_space(self):
        dashboards = {"abc": a_dashboard()}

        url, shorturl = kibana_helpers.compute_kibana_url(dashboards, an_app())

        self.assertTrue(url.startswith("./kibananyx/s/energy/app/kibana#/dashboard/abc?embed=true"),
                        url)
        self.assertIn("time:(from:now-7d,mode:quick,to:now)", url)
        self.assertIn("refreshInterval:(pause:!t,value:0)", url)
        self.assertIn("options:(useMargins:!t,hidePanelTitles:!f)", url)
        self.assertIn("ref-1", url, "the panel reference of a 7.x dashboard is resolved")
        self.assertIn("a%20b%25", url, "a colour key is percent-escaped")
        self.assertIn("c%20d", url)
        self.assertIn("status:ok", url, "the saved query of the dashboard is kept")
        self.assertTrue(shorturl.startswith("./kibananyx/s/energy/app/dashboards#/view/abc"),
                        shorturl)

    def test_a_default_space_is_not_prefixed(self):
        dashboards = {"abc": a_dashboard(namespace="default")}

        url, _ = kibana_helpers.compute_kibana_url(dashboards, an_app())

        self.assertTrue(url.startswith("./kibananyx/app/kibana#"), url)

    def test_the_configured_time_range_and_refresh_win(self):
        dashboards = {"abc": a_dashboard()}
        appl = an_app(kibanaTime="from:now-30d,mode:quick,to:now")
        appl["timeRefresh"] = True
        appl["timeRefreshValue"] = "30"

        url, _ = kibana_helpers.compute_kibana_url(dashboards, appl)

        self.assertIn("time:(from:now-30d,mode:quick,to:now)", url)
        self.assertIn("refreshInterval:(pause:!f,value:30)", url)

    def test_a_numeric_refresh_value_makes_the_computation_fail(self):
        """`'refreshInterval' in appl["timeRefreshValue"]` on an int raises.

        The value is tested for membership before being converted, so a numeric interval — the
        natural thing for a form field to send — never reaches the `str()` branch. In the console
        the exception is caught (`computeMenus` keeps the stored url and logs), so the effect is
        "the refresh does not apply", not a crash. Pinned as it is: accepting a number would mean
        changing the test to a `isinstance` check, which is a behaviour change.
        """
        appl = an_app()
        appl["timeRefresh"] = True
        appl["timeRefreshValue"] = 30

        with self.assertRaises(TypeError):
            kibana_helpers.compute_kibana_url({"abc": a_dashboard()}, appl)

    def test_a_refresh_interval_given_whole_is_taken_as_it_is(self):
        dashboards = {"abc": a_dashboard()}
        appl = an_app()
        appl["timeRefresh"] = True
        appl["timeRefreshValue"] = "refreshInterval:(pause:!f,value:300000)"

        url, _ = kibana_helpers.compute_kibana_url(dashboards, appl)

        self.assertIn("refreshInterval:(pause:!f,value:300000)", url)

    def test_an_older_dashboard_does_not_go_through_the_references(self):
        dashboards = {"abc": a_dashboard(version="6.8.0")}

        url, _ = kibana_helpers.compute_kibana_url(dashboards, an_app())

        self.assertIn("id:p1", url)


class FakeMenuES:
    """The cluster as `computeMenus` reads it: applications, dashboards, and the index call."""

    def __init__(self, apps, dashboards=()):
        self.apps = apps
        self.dashboards = list(dashboards)
        self.searches = []
        self.indexed = []

    def search(self, index=None, body=None, size=None, **kwargs):
        self.searches.append(index)
        if index == "nyx_app":
            return {"hits": {"hits": copy.deepcopy(self.apps)}}
        return {"hits": {"hits": copy.deepcopy(self.dashboards)}}

    def index(self, index=None, id=None, body=None, **kwargs):
        self.indexed.append((index, id, body))
        return {"result": "updated"}


def an_app_document(identifier, title, category="cat", **fields):
    source = {"title": title, "category": category, "type": "generic"}
    source.update(fields)
    return {"_id": identifier, "_index": "nyx_app", "_source": source}


class ComputeMenusTests(unittest.TestCase):
    def setUp(self):
        self.saved_es = getattr(state, "es", None)
        self.saved_translations = getattr(state, "translations", None)
        self.addCleanup(self.restore)
        state.translations = {
            "fr": {"menus": {"cat": "Catégorie", "sub": "Sous-menu", "App A": "Application A"}},
        }

    def restore(self):
        state.es = self.saved_es
        state.translations = self.saved_translations

    def user(self, privileges=("admin",), filters=(), language="fr"):
        return {"_source": {"privileges": list(privileges), "filters": list(filters),
                            "language": language}}

    def build(self, es, usr, apptag="console"):
        state.es = es
        with mock.patch("es_helpers.refresh_translations") as refresh:
            menus = kibana_helpers.computeMenus(usr, "token", apptag)
        refresh.assert_called_once()
        return menus

    def test_a_console_menu_groups_by_category_and_subcategory(self):
        es = FakeMenuES([
            an_app_document("a1", "App A", subcategory="sub", apptags=["console"], icon="chart"),
            an_app_document("a2", "App B", subcategory="sub", apptags=["console"]),
            an_app_document("a3", "App C", apptags=["console"]),
        ])

        menus = self.build(es, self.user())

        self.assertEqual([menu["category"] for menu in menus], ["cat"])
        self.assertEqual(menus[0]["loc_category"], "Catégorie")
        submenus = menus[0]["submenus"]
        self.assertEqual([sub["title"] for sub in submenus], ["sub", ""])
        self.assertEqual([app["title"] for app in submenus[0]["apps"]], ["App A", "App B"])
        self.assertEqual(submenus[0]["loc_title"], "Sous-menu")
        self.assertEqual(submenus[0]["apps"][0]["loc_title"], "Application A")
        self.assertEqual(submenus[0]["icon"], "chart", "the first application gives the icon")
        self.assertNotIn("icon", submenus[1])

    def test_the_display_fields_do_not_travel_to_the_console(self):
        es = FakeMenuES([
            an_app_document("a1", "App A", subcategory="sub", apptags=["console"],
                            order=3, privileges=["P1"]),
        ])

        app = self.build(es, self.user()) [0]["submenus"][0]["apps"][0]

        for field in ("category", "subcategory", "order", "privileges"):
            self.assertNotIn(field, app)

    def test_an_application_the_account_may_not_use_is_dropped(self):
        es = FakeMenuES([an_app_document("a1", "App A", apptags=["console"], privileges=["P1"])])

        self.assertEqual(self.build(es, self.user(privileges=("P2",))), [])

    def test_the_application_stays_when_one_of_its_privileges_is_held(self):
        es = FakeMenuES([an_app_document("a1", "App A", apptags=["console"],
                                         privileges=["P1", "P2"])])

        menus = self.build(es, self.user(privileges=("P2",)))

        self.assertEqual(len(menus), 1)

    def test_a_non_console_menu_only_lists_the_application_tagged_for_it(self):
        es = FakeMenuES([
            an_app_document("a1", "App A", apptags=["mobile"]),
            an_app_document("a2", "App B", apptags=["console"]),
            an_app_document("a3", "App C"),
        ])

        menus = self.build(es, self.user(), apptag="mobile")

        titles = [app["title"] for sub in menus[0]["submenus"] for app in sub["apps"]]
        self.assertEqual(titles, ["App A"])

    def test_a_kibana_application_gets_its_url_computed(self):
        es = FakeMenuES(
            [an_app_document("a1", "Conso", apptags=["console"], type="kibana",
                             config={"kibanaId": "abc", "url": "/old", "shorturl": "/old2"})],
            dashboards=[{"_id": "dashboard:abc", "_source": a_dashboard()["_source"]}],
        )

        app = self.build(es, self.user())[0]["submenus"][0]["apps"][0]

        self.assertTrue(app["config"]["url"].startswith("./kibananyx/s/energy/app/kibana#"))
        self.assertEqual(len(es.indexed), 1, "the stale url is written back")

    def test_an_application_whose_url_is_already_right_is_not_rewritten(self):
        expected = kibana_helpers.compute_kibana_url(
            {"abc": a_dashboard()}, an_app()
        )
        es = FakeMenuES(
            [an_app_document("a1", "Conso", apptags=["console"], type="kibana",
                             config={"kibanaId": "abc", "url": expected[0],
                                     "shorturl": expected[1]})],
            dashboards=[{"_id": "dashboard:abc", "_source": a_dashboard()["_source"]}],
        )

        self.build(es, self.user())

        self.assertEqual(es.indexed, [], "an up-to-date document is not rewritten")

    def test_a_stale_url_is_written_back_to_the_cluster(self):
        es = FakeMenuES(
            [an_app_document("a1", "Conso", apptags=["console"], type="kibana",
                             config={"kibanaId": "abc", "url": "/dashboard/abc",
                                     "shorturl": "/app/dashboards#/view/abc"})],
            dashboards=[{"_id": "dashboard:abc", "_source": a_dashboard()["_source"]}],
        )

        with self.assertLogs(level="WARNING") as logs:
            self.build(es, self.user())

        self.assertTrue(any("desync" in line for line in logs.output))
        self.assertEqual(len(es.indexed), 1)
        index, identifier, body = es.indexed[0]
        self.assertEqual((index, identifier), ("nyx_app", "a1"))
        self.assertNotIn("rec_id", body, "the display field is not persisted")

    def test_an_unknown_dashboard_replaces_the_stored_url_with_invalidurl(self):
        """`compute_kibana_url` refuses by *returning* `INVALIDURL`, it does not raise.

        The `try/except` around it in `computeMenus` therefore never runs on that path: the value
        is assigned to the config, and the desync branch below writes it back to Elasticsearch. A
        dashboard removed from `.kibana*` thus wipes the URL the console had for that application.
        Pinned as it is — keeping the old url would mean checking the return value, a decision.
        """
        es = FakeMenuES([an_app_document("a1", "Conso", apptags=["console"], type="kibana",
                                         config={"kibanaId": "abc", "url": "/kept",
                                                 "shorturl": "/kept2"})])

        with self.assertLogs(level="ERROR"):
            app = self.build(es, self.user())[0]["submenus"][0]["apps"][0]

        self.assertEqual(app["config"]["url"], "INVALIDURL")
        self.assertEqual(len(es.indexed), 1, "et la valeur refusee part au cluster")
        self.assertEqual(es.indexed[0][2]["config"]["url"], "INVALIDURL")

    def test_the_user_filter_is_applied_to_the_computed_url(self):
        es = FakeMenuES(
            [an_app_document("a1", "Conso", apptags=["console"], type="kibana",
                             config={"kibanaId": "abc", "url": "/old", "shorturl": "/old2",
                                     "filtercolumn": "tenant"})],
            dashboards=[{"_id": "dashboard:abc", "_source": a_dashboard()["_source"]}],
        )

        app = self.build(es, self.user(filters=("a", "b")))[0]["submenus"][0]["apps"][0]

        self.assertIn("tenant:a OR tenant:b", app["config"]["url"])


if __name__ == "__main__":
    unittest.main()
