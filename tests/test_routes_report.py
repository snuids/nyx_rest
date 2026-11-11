"""Report submission routes: /report/generate and /report/regenerate.

The point of these two routes is what they **refuse** to take from the caller: no credentials, no
definition body, no identifier. The tests below therefore assert mostly on the job that reaches the
broker — the session comes from the token, the definition from the cluster, the id from the server —
and on the answers given to a caller who tries to send a forged one.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS          # noqa: E402

if HAVE_ROUTE_DEPS:
    import routes.reports as report_routes                        # noqa: E402
else:                                                             # pragma: no cover
    report_routes = None


def definitions():
    return {
        "id_report1": {
            "title": "Monthly energy",
            "reportType": "jasper",
            "jasper": "./jasperdef/energy.jrxml",
            "datasource": "sites",
            "output": ["pdf"],
            "privileges": [],
            "parameters": [
                {"name": "period", "title": "Period", "type": "interval", "value": ["a", "b"]},
                {"name": "site", "title": "Site", "type": "text", "value": "all"},
            ],
        },
        "id_reserved": {
            "title": "Payroll",
            "reportType": "python",
            "exec": "./reports/pythondef/payroll.py",
            "output": ["xlsx"],
            "privileges": ["payroll"],
            "parameters": [],
        },
    }


def tasks():
    return {
        "nyx_reporttask-2026.11": {
            "id_task1": {
                "id": "id_task1",
                "creds": {"user": {"id": "alice@example.com", "login": "alice"}},
                "report": {"title": "Monthly energy", "reportType": "jasper",
                           "jasper": "./jasperdef/energy.jrxml", "output": ["pdf"],
                           "privileges": [],
                           "parameters": [{"name": "site", "type": "text", "value": "Brussels"}]},
                "task": {"mailingList": ["boss@example.com"], "mailSubject": "Monthly",
                         "mailTemplate": "Here: $URL$"},
                "treatment": {"status": "Finished"},
            },
            "id_task3": {
                "id": "id_task3",
                "creds": {"user": {"id": "alice@example.com", "login": "alice"}},
                "report": {"title": "Payroll", "reportType": "python", "privileges": ["payroll"]},
                "task": {},
            },
            "id_task2": {
                "id": "id_task2",
                "creds": {"user": {"id": "bob@example.com", "login": "bob"}},
                "report": {"title": "Payroll", "reportType": "python", "privileges": ["payroll"]},
                "task": {},
            },
        }
    }


@unittest.skipUnless(report_routes is not None, "routes.reports non importable")
class GenerateTests(RouteTestCase):
    route_modules = (report_routes,)

    def setUp(self):
        super().setUp()
        self.state.es.docs["nyx_reportdef"] = definitions()
        self.state.es.docs.update(tasks())

    # -- helpers -----------------------------------------------------------
    def submitted(self):
        """The job that left for /queue/NYX_REPORT_STEP1."""
        sent = self.state.conn.sent_to("/queue/NYX_REPORT_STEP1")
        self.assertEqual(len(sent), 1, "exactly one job should have been published")
        return json.loads(sent[0])

    # -- the job is rebuilt from what the server trusts ---------------------
    def test_publishes_a_job_carrying_the_authenticated_session(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/generate", token=token,
                                    body={"report": "id_report1"})

        self.assertEqual(status, 200)
        self.assertEqual(body["error"], "")
        job = self.submitted()
        self.assertEqual(job["id"], body["id"])
        self.assertTrue(job["id"].startswith("id_"))
        self.assertEqual(job["creds"]["token"], token)
        self.assertEqual(job["creds"]["user"]["login"], "alice@example.com")
        self.assertEqual(job["privileges"], ["YUL"])
        self.assertEqual(job["report"]["jasper"], "./jasperdef/energy.jrxml")

    def test_ignores_credentials_and_definition_the_caller_tries_to_impose(self):
        token = self.login_as("alice@example.com", ["YUL"])

        self.call("post", "/api/v1/report/generate", token=token,
                  body={"report": "id_report1",
                        "creds": {"token": "stolen", "user": {"login": "bob", "privileges": ["admin"]}},
                        "privileges": ["admin"],
                        "exec": "/bin/sh", "reportType": "python"})

        job = self.submitted()
        self.assertEqual(job["creds"]["token"], token)
        self.assertEqual(job["creds"]["user"]["login"], "alice@example.com")
        self.assertEqual(job["privileges"], ["YUL"])
        self.assertEqual(job["report"]["reportType"], "jasper")
        self.assertNotIn("exec", job["report"])

    # -- parameter values, and only values ---------------------------------
    def test_applies_the_submitted_parameter_values(self):
        token = self.login_as("alice@example.com", ["YUL"])

        self.call("post", "/api/v1/report/generate", token=token,
                  body={"report": "id_report1",
                        "parameters": [{"name": "site", "value": "Ghent"}]})

        parameters = {p["name"]: p for p in self.submitted()["report"]["parameters"]}
        self.assertEqual(parameters["site"]["value"], "Ghent")
        self.assertEqual(parameters["site"]["title"], "Site", "the definition keeps its metadata")
        self.assertEqual(parameters["period"]["value"], ["a", "b"], "untouched parameters keep theirs")

    def test_refuses_a_parameter_the_definition_does_not_declare(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/generate", token=token,
                                    body={"report": "id_report1",
                                          "parameters": [{"name": "shell", "value": "rm -rf /"}]})

        self.assertEqual(status, 400)
        self.assertIn("shell", body["error"])
        self.assertEqual(self.state.conn.sent_to("/queue/NYX_REPORT_STEP1"), [])

    def test_refuses_a_parameter_without_a_name(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/generate", token=token,
                                    body={"report": "id_report1", "parameters": ["period"]})

        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "each parameter needs a name")

    # -- mail settings -----------------------------------------------------
    def test_keeps_the_mail_settings_and_drops_the_rest(self):
        token = self.login_as("alice@example.com", ["YUL"])

        self.call("post", "/api/v1/report/generate", token=token,
                  body={"report": "id_report1",
                        "task": {"mailingList": ["a@example.com", 42],
                                 "mailSubject": "Monthly", "mailTemplate": "see $URL$",
                                 "destination": "/queue/EVIL", "creds": {"token": "x"}}})

        task = self.submitted()["task"]
        self.assertEqual(task["mailingList"], ["a@example.com", "42"])
        self.assertEqual(task["mailSubject"], "Monthly")
        self.assertEqual(task["mailTemplate"], "see $URL$")
        self.assertNotIn("destination", task)
        self.assertNotIn("creds", task)
        self.assertEqual(task["mailingListCc"], [])

    def test_caps_the_recipient_list(self):
        token = self.login_as("alice@example.com", ["YUL"])

        self.call("post", "/api/v1/report/generate", token=token,
                  body={"report": "id_report1",
                        "task": {"mailingList": ["u%d@example.com" % i for i in range(500)]}})

        self.assertEqual(len(self.submitted()["task"]["mailingList"]), report_routes.MAX_RECIPIENTS)

    # -- refusals ----------------------------------------------------------
    def test_unknown_definition_is_a_404(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/generate", token=token,
                                    body={"report": "id_does_not_exist"})

        self.assertEqual(status, 404)
        self.assertEqual(body["error"], "unknown report")

    def test_a_report_reserved_to_a_privilege_is_refused_to_others(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/generate", token=token,
                                    body={"report": "id_reserved"})

        self.assertEqual(status, 403)
        self.assertEqual(body["error"], "forbidden")
        self.assertEqual(self.state.conn.sent_to("/queue/NYX_REPORT_STEP1"), [])

    def test_a_report_reserved_to_a_privilege_is_open_to_its_holders(self):
        token = self.login_as("carol@example.com", ["payroll"])

        status, _, _ = self.call("post", "/api/v1/report/generate", token=token,
                                 body={"report": "id_reserved"})

        self.assertEqual(status, 200)

    def test_an_administrator_may_run_any_report(self):
        token = self.login_as("bob@example.com", ["admin"])

        status, _, _ = self.call("post", "/api/v1/report/generate", token=token,
                                 body={"report": "id_reserved"})

        self.assertEqual(status, 200)

    def test_a_missing_report_is_a_400(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/generate", token=token, body={})

        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "report is required")

    def test_without_a_token_nothing_is_published(self):
        status, body, _ = self.call("post", "/api/v1/report/generate", body={"report": "id_report1"})

        self.assertEqual(body["error"], "NO_TOKEN")
        self.assertEqual(self.state.conn.sent_to("/queue/NYX_REPORT_STEP1"), [])


@unittest.skipUnless(report_routes is not None, "routes.reports non importable")
class RegenerateTests(RouteTestCase):
    route_modules = (report_routes,)

    def setUp(self):
        super().setUp()
        self.state.es.docs["nyx_reportdef"] = definitions()
        self.state.es.docs.update(tasks())

    def submitted(self):
        sent = self.state.conn.sent_to("/queue/NYX_REPORT_STEP1")
        self.assertEqual(len(sent), 1)
        return json.loads(sent[0])

    def test_replays_the_definition_snapshot_of_the_task(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/regenerate", token=token,
                                    body={"task": "id_task1"})

        self.assertEqual(status, 200)
        job = self.submitted()
        self.assertEqual(job["id"], body["id"], "a new identifier, not the old one")
        self.assertNotEqual(job["id"], "id_task1")
        # the snapshot, not the current definition: the parameters of the previous run are kept
        self.assertEqual(job["report"]["parameters"][0]["value"], "Brussels")
        self.assertEqual(job["task"]["mailSubject"], "Monthly")
        self.assertEqual(job["creds"]["user"]["login"], "alice@example.com")

    def test_the_index_can_be_given_to_avoid_a_wildcard_search(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, _, _ = self.call("post", "/api/v1/report/regenerate", token=token,
                                 body={"task": "id_task1", "index": "nyx_reporttask-2026.11"})

        self.assertEqual(status, 200)
        self.assertEqual(self.state.es.searches[-1][0], "nyx_reporttask-2026.11")

    def test_someone_elses_task_is_refused(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/regenerate", token=token,
                                    body={"task": "id_task2"})

        self.assertEqual(status, 403)
        self.assertEqual(body["error"], "forbidden")

    def test_an_administrator_may_replay_someone_elses_task(self):
        token = self.login_as("bob@example.com", ["admin"])

        status, _, _ = self.call("post", "/api/v1/report/regenerate", token=token,
                                 body={"task": "id_task2"})

        self.assertEqual(status, 200)

    def test_her_own_task_whose_snapshot_needs_a_privilege_she_lost(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/regenerate", token=token,
                                    body={"task": "id_task3"})

        self.assertEqual(status, 403)
        self.assertEqual(self.state.conn.sent_to("/queue/NYX_REPORT_STEP1"), [])

    def test_her_own_task_whose_snapshot_needs_a_privilege_she_holds(self):
        token = self.login_as("alice@example.com", ["YUL", "payroll"])

        status, _, _ = self.call("post", "/api/v1/report/regenerate", token=token,
                                 body={"task": "id_task3"})

        self.assertEqual(status, 200)

    def test_an_unknown_task_is_a_404(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/regenerate", token=token,
                                    body={"task": "id_nope"})

        self.assertEqual(status, 404)
        self.assertEqual(body["error"], "unknown task")

    def test_a_missing_task_is_a_400(self):
        token = self.login_as("alice@example.com", ["YUL"])

        status, body, _ = self.call("post", "/api/v1/report/regenerate", token=token, body={})

        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "task is required")


if __name__ == "__main__":
    unittest.main()
