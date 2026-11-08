"""
Export of a Kibana dashboard — the direct path, which nothing exercised.

`common.kibanaData` runs the msearch nginx stashed for the token, turns the responses into
DataFrames and writes one file. Above 3 000 hits it hands the work to the queue and answers
`{"type": "mail"}`; below, it writes the file itself and answers `{"type": "direct"}`.

That second branch had **no test**, and it had stopped working: it called `writer.save()`, removed
in pandas 2.0, while the image has run 2.2.3 since v3.21.11 — so every dashboard export under
3 000 rows answered 500 (the exception travelled up through the route). The same branch also
always built an Excel file, whatever `output` asked for, and let pandas write the DataFrame index
as a first, unnamed column.

These tests run the real function against a fake cluster (only `msearch` is called) and read the
file it produces: the point is not the DataFrame logic, it is that a user gets a file they can
open, in the format they asked for.
"""

import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:
    import pandas        # noqa: F401
    import xlsxwriter    # noqa: F401  (engine of the xlsx branch)
    import openpyxl      # noqa: F401  (reads the file back in the assertions)

    HAVE_DEPS = True
    EXPORT_DEPS_MISSING = ""
except Exception as exc:  # pragma: no cover - depends on the machine running the tests
    HAVE_DEPS = False
    EXPORT_DEPS_MISSING = type(exc).__name__ + ": " + str(exc)[:80]

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS, DEPS_MISSING  # noqa: E402
from routes import data as data_routes                                # noqa: E402

import common  # noqa: E402


class FakeBroker:
    """Double of the ActiveMQ client: what would leave for the queue is kept."""

    def __init__(self):
        self.messages = []

    def send_message(self, destination, body, headers=None):
        self.messages.append((destination, body))
        return True

    def sent_to(self, destination):
        return [body for dest, body in self.messages if dest == destination]


class FakeCluster:
    """Double of the Elasticsearch client: `msearch` only, the one call the export makes."""

    def __init__(self, responses=None, error=None):
        self.responses = responses or []
        self.error = error
        self.matchrequests = []

    def msearch(self, matchrequest):
        self.matchrequests.append(matchrequest)
        if self.error is not None:
            raise self.error
        return {"responses": self.responses}


def response(*documents, index="nyx_energy"):
    """One sub-response of the msearch: hits with `_id`, `_index` and a `_source`."""
    return {
        "hits": {
            "hits": [
                {"_id": str(position), "_index": index, "_source": dict(document)}
                for position, document in enumerate(documents)
            ]
        }
    }


@unittest.skipUnless(HAVE_DEPS, "export dependencies missing (" + EXPORT_DEPS_MISSING + ")")
class DashboardExportTests(unittest.TestCase):
    def setUp(self):
        self.cluster = FakeCluster()
        self.broker = FakeBroker()
        self.folder = tempfile.mkdtemp(prefix="nyx-export-")
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        import shutil
        shutil.rmtree(self.folder, ignore_errors=True)

    def export(self, outputformat="xlsx", is_rest_api=True):
        return common.kibanaData(
            self.cluster, self.broker, '{"query":{}}\n', {"id": "someone"},
            outputformat, is_rest_api, "https://host/outputs/", self.folder + "/",
        )

    # -- the direct branch ------------------------------------------------

    def test_an_xlsx_export_is_written_and_opens(self):
        self.cluster.responses = [response({"device": "pump", "kwh": 12.5},
                                           {"device": "valve", "kwh": 3.0})]

        result = self.export("xlsx")

        self.assertEqual(result["error"], "")
        self.assertEqual(result["type"], "direct")
        self.assertEqual(result["records"], 2)
        path = Path(self.folder) / result["fileName"]
        self.assertTrue(path.exists(), "the export must leave a file behind")

        frame = pandas_read(path)
        self.assertEqual(len(frame), 2)
        self.assertEqual(sorted(frame["device"]), ["pump", "valve"])
        self.assertNotIn("Unnamed: 0", list(frame.columns),
                         "the DataFrame index must not be exported as a column")

    def test_a_csv_export_writes_csv(self):
        self.cluster.responses = [response({"device": "pump", "kwh": 12.5})]

        result = self.export("csv")

        self.assertEqual(result["error"], "")
        path = Path(self.folder) / result["fileName"]
        self.assertTrue(path.name.endswith(".csv"), "the name must match the requested format")
        with open(path, newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["device"], "pump")

    def test_every_response_of_the_msearch_is_exported(self):
        self.cluster.responses = [
            response({"device": "pump", "kwh": 1.0}),
            response({"device": "valve", "kwh": 2.0}, {"device": "meter", "kwh": 3.0}),
        ]

        result = self.export("xlsx")

        sheets = pandas_read_sheets(Path(self.folder) / result["fileName"])
        devices = sorted(device for frame in sheets.values() for device in frame["device"])
        self.assertEqual(devices, ["meter", "pump", "valve"],
                         "one sheet per panel, and no panel left behind")

    def test_a_response_with_no_hit_does_not_break_the_export(self):
        self.cluster.responses = [response()]

        result = self.export("xlsx")

        self.assertEqual(result["error"], "", "an empty dashboard is not a failure")
        self.assertEqual(result["records"], 0)

    def test_the_msearch_is_run_without_the_size_zero_panels(self):
        self.cluster.responses = [response({"device": "pump"})]

        self.export("xlsx")

        self.assertEqual(len(self.cluster.matchrequests), 1)
        self.assertNotIn('"size":0', self.cluster.matchrequests[0],
                         "a size:0 panel returns no rows to export")

    # -- the queued branch ------------------------------------------------

    def test_above_the_threshold_the_export_is_mailed(self):
        self.cluster.responses = [response(*[{"n": position} for position in range(3001)])]

        result = self.export("xlsx")

        self.assertEqual(result["type"], "mail")
        self.assertEqual(result["total"], 3001)
        self.assertEqual(len(self.broker.sent_to("/queue/REST_LOAD_KIBANA")), 1)
        self.assertEqual([name for name in Path(self.folder).iterdir()], [],
                         "nothing is written when the worker takes over")

    def test_the_threshold_does_not_apply_to_the_worker(self):
        """`nyx_rest_helper` calls this with `is_rest_api=False`; it must not re-queue."""
        self.cluster.responses = [response(*[{"n": position} for position in range(3001)])]

        result = self.export("xlsx", is_rest_api=False)

        self.assertEqual(result["type"], "direct")
        self.assertEqual(self.broker.sent_to("/queue/REST_LOAD_KIBANA"), [])

    # -- failures --------------------------------------------------------

    def test_a_failed_msearch_returns_an_error_instead_of_none(self):
        self.cluster.error = ConnectionError("cluster down")

        result = self.export("xlsx")

        self.assertIsNotNone(result, "the route would answer 500 with an empty body for None")
        self.assertNotEqual(result["error"], "")


@unittest.skipUnless(HAVE_ROUTE_DEPS, "route dependencies missing (" + DEPS_MISSING + ")")
class KibanaLoadRouteTests(RouteTestCase):
    """`POST /api/v1/kibana_load` reads the msearch nginx stashed for the token."""

    route_modules = (data_routes,)

    def test_a_token_without_a_pending_export_answers_an_error(self):
        # `state.redisserver.get(...)` answered None and `.decode()` on it raised AttributeError:
        # the API answered 500 with an empty body instead of a readable refusal.
        token = self.login_as("someone", ["user"])

        status, payload, _ = self.call("post", "/api/v1/kibana_load?output=xlsx", token=token)

        self.assertEqual(status, 200)
        self.assertEqual(payload["error"], "NO_EXPORT_REQUEST")

    def test_the_stashed_msearch_is_handed_to_the_export(self):
        token = self.login_as("someone", ["user"])
        self.redis.set("nyx_kib_msearch" + token, '{"query":{"match_all":{}}}\n', None)
        captured = {}
        original = data_routes.kibanaData

        def fake(*args, **kwargs):
            captured["args"] = args
            return {"error": "", "type": "direct", "records": 0}

        data_routes.kibanaData = fake
        self.addCleanup(setattr, data_routes, "kibanaData", original)

        status, payload, _ = self.call("post", "/api/v1/kibana_load?output=csv", token=token)

        self.assertEqual(payload["error"], "")
        self.assertEqual(captured["args"][2], '{"query":{"match_all":{}}}\n')
        self.assertEqual(captured["args"][4], "csv", "the requested format must travel")


def pandas_read(path):
    """Reads an exported workbook back, with the engine the image provides."""
    return __import__("pandas").read_excel(path, engine="openpyxl")


def pandas_read_sheets(path):
    """Reads every sheet of an exported workbook: one sheet per panel of the dashboard."""
    return __import__("pandas").read_excel(path, engine="openpyxl", sheet_name=None)


if __name__ == "__main__":
    unittest.main()
