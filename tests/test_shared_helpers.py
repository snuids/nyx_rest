"""Tests for the helpers every route shares.

`common.py`, `pg_common.py` and `auth/auth_ad.py` carry the logic the routes call through: the
privileges filter, the flattening of an Elasticsearch hit into a row, the export writer, the
paged read (`loadData`), the SQL Server connection pool and the Active Directory bind. They were
the largest measured hole in this repository — 34 %, 36 % and 33 % — and nothing but a route test
touched them.

The doubles are local on purpose: `pyodbc` is not installed outside the image, so the module-level
name is replaced; `ldap3` is installed, so its `Server`/`Connection` are replaced in the module's
namespace; the cluster is a class that answers page by page.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import common                                                                  # noqa: E402
import pg_common                                                               # noqa: E402
from auth import auth_ad                                                       # noqa: E402

ADMIN = {"id": "admin@example.com", "privileges": ["admin"]}
ALICE = {"id": "alice@example.com", "privileges": ["R1", "R2"]}


class ElasticVersionTests(unittest.TestCase):
    def test_a_modern_cluster_reports_its_major(self):
        es = mock.Mock()
        es.info.return_value = {"version": {"number": "8.19.23"}}

        self.assertEqual(common.getELKVersion(es), 8)

    def test_an_opensearch_cluster_is_read_as_seven(self):
        # OpenSearch announces 2.x and answers like Elasticsearch 7; the routes gate on 7.
        es = mock.Mock()
        es.info.return_value = {"version": {"number": "2.19.0"}}

        self.assertEqual(common.getELKVersion(es), 7)

    def test_the_info_call_carries_a_timeout(self):
        # Without it, an unreachable cluster holds the request open until the socket gives up.
        es = mock.Mock()
        es.info.return_value = {"version": {"number": "8.1.0"}}

        common.get_es_info(es)

        self.assertEqual(es.info.call_args.kwargs.get("request_timeout"), 5)


class FilterReportsTests(unittest.TestCase):
    def records(self):
        return [
            {"_id": "one", "_source": {"creds": {"user": {"user": "alice@example.com"}}}},
            {"_id": "two", "_source": {"creds": {"user": {"user": "bob@example.com"}}}},
            {"_id": "three", "_source": {"creds": {"user": {}}}},
        ]

    def test_an_admin_sees_every_report(self):
        self.assertEqual(len(common.filterReports(self.records(), ADMIN)), 3)

    def test_a_user_sees_only_the_reports_they_ran(self):
        kept = common.filterReports(self.records(), ALICE)

        self.assertEqual([record["_id"] for record in kept], ["one"])

    def test_a_report_without_a_user_belongs_to_nobody(self):
        # The document is stamped "NA" when the reporter did not set one; a user whose id is "NA"
        # would otherwise inherit every report the scheduler ran on its own behalf.
        kept = common.filterReports(self.records(), {"id": "NA", "privileges": []})

        self.assertEqual([record["_id"] for record in kept], ["three"])


class FilterReportDefsTests(unittest.TestCase):
    def records(self):
        return [
            {"_id": "shared", "_source": {"privileges": ["R1", "R9"]}},
            {"_id": "other", "_source": {"privileges": ["R9"]}},
            {"_id": "open", "_source": {}},
        ]

    def test_an_admin_sees_every_definition(self):
        self.assertEqual(len(common.filterReportDefs(self.records(), ADMIN)), 3)

    def test_a_definition_is_kept_when_one_of_its_privileges_is_held(self):
        kept = common.filterReportDefs(self.records(), ALICE)

        self.assertEqual([record["_id"] for record in kept], ["shared"])

    def test_a_definition_without_privileges_is_not_shared(self):
        kept = common.filterReportDefs(self.records(), ALICE)

        self.assertNotIn("open", [record["_id"] for record in kept])


class ApplyPrivilegesTests(unittest.TestCase):
    def test_an_admin_sees_every_record(self):
        records = [{"_source": {"readPriv": "R9"}}]

        self.assertEqual(common.applyPrivileges(records, ADMIN, "readPriv"), records)

    def test_a_record_is_kept_when_the_user_holds_its_privilege(self):
        records = [
            {"_source": {"readPriv": "R1"}},
            {"_source": {"readPriv": "R9"}},
            {"_source": {"other": "R1"}},
        ]

        self.assertEqual(common.applyPrivileges(records, ALICE, "readPriv"), [records[0]])

    def test_a_record_listing_several_privileges_is_read_on_the_first_one(self):
        """The filter only looks at the first value of the list.

        `for priv2 in rec["_source"][column].split(','): if priv2 in privhash: append; break` — the
        `break` sits in the loop, after the `if`, so the list is cut at its first element whatever
        it is. A row "R9,R1" is therefore refused to a user holding R1. This test pins the actual
        behaviour: it is restrictive (fail-closed), not permissive, and changing it is a decision,
        not a test correction.
        """
        records = [{"_source": {"readPriv": "R9,R1"}}]

        self.assertEqual(common.applyPrivileges(records, ALICE, "readPriv"), [])


class FlattenTests(unittest.TestCase):
    def test_nested_objects_and_lists_flatten_into_one_row(self):
        document = {
            "name": "sensor",
            "reading": {"value": 21.5, "unit": "C"},
            "samples": [{"at": 1}, {"at": 2}],
        }

        self.assertEqual(
            common.flattenJson(document),
            {
                "name": "sensor",
                "reading$value": 21.5,
                "reading$unit": "C",
                "samples$0#at": 1,
                "samples$1#at": 2,
            },
        )

    def test_an_empty_document_flattens_to_nothing(self):
        self.assertEqual(common.flattenJson({}), {})

    def test_the_cleaning_keeps_the_hit_identity(self):
        response = {
            "hits": {
                "hits": [
                    {"_id": "a1", "_index": "nyx_data", "_source": {"v": 1, "nested": {"x": 2}}},
                ]
            }
        }

        self.assertEqual(
            common.cleanElasticRecords(response),
            [{"v": 1, "nested$x": 2, "_id": "a1", "_index": "nyx_data"}],
        )

    def test_a_response_without_hits_yields_nothing(self):
        self.assertEqual(common.cleanElasticRecords({"aggregations": {}}), [])
        self.assertEqual(common.cleanElasticRecords({"hits": {}}), [])


class WriteExportTests(unittest.TestCase):
    def frames(self):
        import pandas as pd

        return [
            pd.DataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
            pd.DataFrame([{"a": 3, "b": "z"}]),
        ]

    def test_a_csv_export_stacks_the_frames_into_one_file(self):
        # A CSV has no sheets: the sub-responses of an msearch are stacked.
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "export.csv")

            common.write_export(self.frames(), path, "csv")

            self.assertTrue(os.path.isfile(path))
            with open(path, encoding="utf8") as handle:
                lines = handle.read().strip().split("\n")
            self.assertEqual(lines[0], "a,b", "the pandas index must not become a column")
            self.assertEqual(len(lines), 4, "three rows and the header")

    def test_an_xlsx_export_puts_each_frame_on_its_own_sheet(self):
        from openpyxl import load_workbook

        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "export.xlsx")

            common.write_export(self.frames(), path, "xlsx")

            workbook = load_workbook(path)
            self.assertEqual(workbook.sheetnames, ["Sheet1", "Sheet2"])
            self.assertEqual(workbook["Sheet1"]["A2"].value, 1)
            self.assertEqual(workbook["Sheet2"]["A2"].value, 3)

    def test_an_empty_export_still_produces_a_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "empty.csv")

            common.write_export([], path, "csv")

            self.assertTrue(os.path.isfile(path))


class FakePyodbc:
    """`pyodbc` is not installed outside the image: the module-level name is replaced."""

    def __init__(self, fail=False):
        self.connections = []
        self.connection_strings = []
        self.fail = fail

    def connect(self, connection_string):
        self.connection_strings.append(connection_string)
        if self.fail:
            raise RuntimeError("driver: cannot open server")
        connection = mock.Mock(name="pyodbc connection %d" % len(self.connections))
        self.connections.append(connection)
        return connection


class SqlServerConnectionTests(unittest.TestCase):
    def setUp(self):
        pg_common.sql_server_connections.clear()
        self.addCleanup(pg_common.sql_server_connections.clear)
        for name, value in (("SQLSERVER_HOST", "sql.example"), ("SQLSERVER_LOGIN", "sa"),
                            ("SQLSERVER_PASSWORD", "s3cr3t"), ("SQLSERVER_PORT", "1433")):
            patcher = mock.patch.object(pg_common.settings, name, value, create=True)
            patcher.start()
            self.addCleanup(patcher.stop)

    def app(self, database="nyx", kind="sqlserver"):
        return {"_source": {"config": {"databaseType": kind, "database": database}}}

    def test_the_connection_string_names_the_database_and_the_server(self):
        driver = FakePyodbc()
        with mock.patch.object(pg_common, "pyodbc", driver, create=True):
            connection = pg_common.create_sql_server_connection({"database": "nyx"})

        self.assertIs(connection, driver.connections[0])
        string = driver.connection_strings[0]
        self.assertIn("SERVER=sql.example", string)
        self.assertIn("DATABASE=nyx", string)
        self.assertIn("PORT=1433", string)
        self.assertIn("TrustServerCertificate=yes", string)

    def test_a_driver_failure_is_logged_and_yields_no_connection(self):
        driver = FakePyodbc(fail=True)
        with mock.patch.object(pg_common, "pyodbc", driver, create=True), \
                self.assertLogs(level="ERROR") as logs:
            connection = pg_common.create_sql_server_connection({"database": "nyx"})

        self.assertIsNone(connection)
        self.assertTrue(any("Error creating SQL Server connection" in line for line in logs.output))

    def test_the_connection_is_kept_per_database(self):
        driver = FakePyodbc()
        with mock.patch.object(pg_common, "pyodbc", driver, create=True):
            first = pg_common.get_sql_server_connection(self.app("nyx"))
            again = pg_common.get_sql_server_connection(self.app("nyx"))
            other = pg_common.get_sql_server_connection(self.app("reporting"))

        self.assertIs(first, again, "one connection per database, not one per request")
        self.assertIsNot(first, other)
        self.assertEqual(len(driver.connection_strings), 2)

    def test_a_sqlserver_app_without_a_database_is_refused(self):
        driver = FakePyodbc()
        with mock.patch.object(pg_common, "pyodbc", driver, create=True):
            connection = pg_common.get_sql_server_connection(self.app(""))

        self.assertIsNone(connection)
        self.assertEqual(driver.connection_strings, [])

    def test_another_database_type_gets_no_connection(self):
        driver = FakePyodbc()
        with mock.patch.object(pg_common, "pyodbc", driver, create=True):
            connection = pg_common.get_sql_server_connection(self.app("nyx", kind="postgres"))

        self.assertIsNone(connection)
        self.assertEqual(driver.connection_strings, [])


class GetAppByIdTests(unittest.TestCase):
    def test_the_app_document_is_read_once_and_kept(self):
        es = mock.Mock()
        es.get.return_value = {"_id": "id_1", "_source": {"title": "Conso"}}

        first = pg_common.getAppByID(es, "id_cache_ttl")
        second = pg_common.getAppByID(es, "id_cache_ttl")

        self.assertEqual(first["_source"]["title"], "Conso")
        self.assertEqual(second, first)
        self.assertEqual(es.get.call_count, 1, "the document is cached for 30 s")
        es.get.assert_called_with("nyx_app", id="id_cache_ttl")


class FakeAdConnection:
    def __init__(self, bound=True, entries=None):
        self.bound = bound
        self.entries = entries or []
        self.searched = []
        self.unbinds = 0

    def search(self, **kwargs):
        self.searched.append(kwargs)
        return True

    def unbind(self):
        self.unbinds += 1


class ActiveDirectoryTests(unittest.TestCase):
    def configure(self, servers=("ldaps://ad1.example",), base="DC=example,DC=com",
                  domain="EXAMPLE"):
        for name, value in (("AD_SERVERS", list(servers)), ("AD_BASE_DN", base),
                            ("AD_DOMAIN", domain)):
            patcher = mock.patch.object(auth_ad, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def entry(self, attributes):
        entry = mock.Mock()
        entry.entry_attributes_as_dict = dict(attributes)
        entry.entry_dn = "CN=Alice,DC=example,DC=com"
        return entry

    def test_an_unconfigured_instance_refuses_without_touching_the_network(self):
        for name, value in (("AD_SERVERS", []), ("AD_BASE_DN", ""), ("AD_DOMAIN", None)):
            patcher = mock.patch.object(auth_ad, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        server = mock.patch.object(auth_ad, "Server").start()
        self.addCleanup(mock.patch.stopall)

        success, info = auth_ad.authenticate_ad("alice", "secret")

        self.assertEqual((success, info), (False, {}))
        self.assertFalse(server.called)

    def test_a_short_login_binds_with_the_domain_and_searches_on_the_account_name(self):
        self.configure()
        connection = FakeAdConnection(entries=[self.entry({"mail": ["alice@example.com"]})])
        with mock.patch.object(auth_ad, "Server") as server, \
                mock.patch.object(auth_ad, "Connection", return_value=connection) as constructor:
            success, info = auth_ad.authenticate_ad("alice", "secret")

        self.assertTrue(success)
        self.assertEqual(constructor.call_args.kwargs["user"], "EXAMPLE\\alice")
        self.assertEqual(connection.searched[0]["search_filter"], "(sAMAccountName=alice)")
        self.assertEqual(connection.searched[0]["search_base"], "DC=example,DC=com")
        self.assertEqual(info["mail"], ["alice@example.com"])
        self.assertEqual(info["distinguishedName"], "CN=Alice,DC=example,DC=com")
        self.assertEqual(connection.unbinds, 1)
        self.assertTrue(server.called)

    def test_a_upn_login_is_reduced_to_its_account_name(self):
        self.configure()
        connection = FakeAdConnection(entries=[self.entry({})])
        with mock.patch.object(auth_ad, "Server"), \
                mock.patch.object(auth_ad, "Connection", return_value=connection) as constructor:
            auth_ad.authenticate_ad("alice@example.com", "secret")

        self.assertEqual(constructor.call_args.kwargs["user"], "EXAMPLE\\alice")
        self.assertEqual(connection.searched[0]["search_filter"], "(sAMAccountName=alice)")

    def test_a_bound_connection_without_entries_is_a_success_without_attributes(self):
        self.configure()
        connection = FakeAdConnection(entries=[])
        with mock.patch.object(auth_ad, "Server"), \
                mock.patch.object(auth_ad, "Connection", return_value=connection), \
                self.assertLogs(level="WARNING"):
            success, info = auth_ad.authenticate_ad("alice", "secret")

        self.assertEqual((success, info), (True, {}))
        self.assertEqual(connection.unbinds, 1)

    def test_a_failing_server_leaves_room_for_the_next_one(self):
        self.configure(servers=("ldaps://ad1.example", "ldaps://ad2.example"))
        connection = FakeAdConnection(entries=[self.entry({"mail": ["alice@example.com"]})])
        constructor = mock.Mock(side_effect=[RuntimeError("unreachable"), connection])
        with mock.patch.object(auth_ad, "Server"), \
                mock.patch.object(auth_ad, "Connection", constructor), \
                self.assertLogs(level="ERROR"):
            success, info = auth_ad.authenticate_ad("alice", "secret")

        self.assertTrue(success)
        self.assertEqual(constructor.call_count, 2)
        self.assertEqual(info["mail"], ["alice@example.com"])

    def test_every_server_failing_is_a_refusal(self):
        self.configure(servers=("ldaps://ad1.example", "ldaps://ad2.example"))
        with mock.patch.object(auth_ad, "Server"), \
                mock.patch.object(auth_ad, "Connection", side_effect=RuntimeError("no route")), \
                self.assertLogs(level="ERROR"):
            success, info = auth_ad.authenticate_ad("alice", "secret")

        self.assertEqual((success, info), (False, {}))


class FakeScrollES:
    """A cluster that answers page by page: `search`, then `scroll` until the empty page."""

    def __init__(self, pages, mappings=None):
        self.pages = list(pages)
        self.mappings = mappings or {}
        self.searches = []
        self.scrolls = []
        self.cleared = []
        self.indices = self

    def search(self, index=None, body=None, scroll=None, **kwargs):
        import json as _json

        self.searches.append((index, _json.loads(body), scroll))
        return self.pages[0]

    def scroll(self, scroll_id=None, scroll=None, **kwargs):
        self.scrolls.append(scroll_id)
        self.pages.pop(0)
        return self.pages[0]

    def clear_scroll(self, body=None, **kwargs):
        self.cleared.append(body)
        return {"succeeded": True}

    def get_mapping(self, index=None, **kwargs):
        # the shape a real client returns, which `common.get_mappings` reads
        properties = {field: {"type": kind} for field, kind in self.mappings.get(index, {}).items()}
        return {index: {"mappings": {"properties": properties}}}


def a_page(hits, scroll_id="scroll-1", total=None, aggregations=None):
    page = {
        "_scroll_id": scroll_id,
        "hits": {"hits": hits, "total": {"value": len(hits) if total is None else total}},
    }
    if aggregations is not None:
        page["aggregations"] = aggregations
    return page


class LoadDataTests(unittest.TestCase):
    """`loadData`: the paged read, the conversions, the export and the delegation."""

    def test_a_small_result_is_returned_and_the_scroll_released(self):
        hits = [{"_id": "a", "_index": "idx", "_source": {"v": 1}},
                {"_id": "b", "_index": "idx", "_source": {"v": 2}}]
        es = FakeScrollES([a_page(hits), a_page([], total=2)])

        result = common.loadData(
            es, None, "idx", {"size": 10}, None, False, (None, None, None),
            True, {"id": "alice", "privileges": []}, "csv", "", "",
        )

        self.assertEqual(result["error"], "")
        self.assertEqual(result["total"], 2)
        self.assertEqual(len(result["records"]), 2)
        self.assertEqual(es.scrolls, ["scroll-1"], "the next page is asked for")
        self.assertEqual(es.cleared, [{"scroll_id": ["scroll-1", "scroll-1"]}])

    def test_the_page_number_becomes_an_offset(self):
        es = FakeScrollES([a_page([{"_id": "x", "_index": "idx", "_source": {}}])])

        common.loadData(
            es, None, "idx", {"size": 10, "extra": {"currentpage": 3, "pagesize": 10}}, None,
            False, (None, None, None), True, {"id": "alice", "privileges": []}, "csv", "", "",
        )

        index, body, scroll = es.searches[0]
        self.assertEqual(body["from"], 20)
        self.assertNotIn("extra", body)
        self.assertIsNone(scroll, "a precise page asks for no cursor")
        self.assertEqual(es.scrolls, [])

    def test_a_date_histogram_interval_becomes_a_fixed_interval(self):
        # `interval` was removed from the date_histogram aggregation in Elasticsearch 8: sent as
        # is, every chart query answered 400.
        es = FakeScrollES([a_page([])])

        common.loadData(
            es, None, "idx", {"aggs": {"per_day": {"date_histogram": {"interval": "30d"}}}}, None,
            False, (None, None, None), True, {"id": "alice", "privileges": []}, "csv", "", "",
        )

        aggs = es.searches[0][1]["aggs"]["per_day"]["date_histogram"]
        self.assertEqual(aggs["fixed_interval"], "30d")
        self.assertNotIn("interval", aggs)

    def test_a_sort_on_a_text_field_uses_its_keyword_sub_field(self):
        # Sorting a text field directly is refused by the cluster; the `.keyword` sub-field is
        # what carries the doc values.
        es = FakeScrollES([a_page([])], mappings={"idx": {"title": "text", "count": "long"}})

        common.loadData(
            es, None, "idx", {"sort": [{"title": {}}, {"count": {}}]}, None, False,
            (None, None, None), True, {"id": "alice", "privileges": []}, "csv", "", "",
        )

        sort = es.searches[0][1]["sort"]
        self.assertIn("title.keyword", sort[0])
        self.assertIn("count", sort[1], "a numeric field sorts on itself")

    def test_a_screen_on_report_tasks_is_filtered_by_its_owner(self):
        mine = {"_id": "mine", "_index": "nyx_reporttask",
                "_source": {"creds": {"user": {"user": "alice"}}}}
        theirs = {"_id": "theirs", "_index": "nyx_reporttask",
                  "_source": {"creds": {"user": {"user": "bob"}}}}
        es = FakeScrollES([a_page([mine, theirs]), a_page([], total=2)])

        result = common.loadData(
            es, None, "nyx_reporttask", {}, None, False, (None, None, None), True,
            {"id": "alice", "privileges": []}, "csv", "", "",
        )

        self.assertEqual([record["_id"] for record in result["records"]], ["mine"])

    def test_an_export_is_written_and_its_url_returned(self):
        hits = [{"_id": "a", "_index": "idx", "_source": {"v": 1, "title": "x"}}]
        es = FakeScrollES([a_page(hits), a_page([], total=1)])

        with tempfile.TemporaryDirectory() as folder:
            result = common.loadData(
                es, None, "idx", {}, None, True, (None, None, None), True,
                {"id": "alice", "privileges": []}, "csv",
                "https://energy.example/generated/", folder + "/",
            )

            self.assertEqual(result["type"], "direct")
            self.assertTrue(result["url"].startswith("https://energy.example/generated/export_"))
            self.assertTrue(os.path.isfile(result["file"]))
            with open(result["file"], encoding="utf8") as handle:
                self.assertIn("title", handle.readline())

    def test_an_export_above_five_thousand_rows_is_delegated_to_the_helper(self):
        hits = [{"_id": "h%d" % position, "_index": "idx", "_source": {"v": position}}
                for position in range(5001)]
        es = FakeScrollES([a_page(hits, total=5001), a_page([], total=5001)])
        broker = mock.Mock()

        result = common.loadData(
            es, broker, "idx", {"size": 10000}, None, True, (None, None, None), True,
            {"id": "alice", "privileges": []}, "csv", "https://x/", "/tmp/",
        )

        self.assertEqual(result["type"], "mail")
        self.assertEqual(broker.send_message.call_count, 1)
        destination, body = broker.send_message.call_args.args
        self.assertEqual(destination, "/queue/REST_LOAD_DATA")
        self.assertIn('"index": "idx"', body)

    def test_the_export_columns_are_renamed_as_asked(self):
        hits = [{"_id": "a", "_index": "idx", "_source": {"raw": 1, "drop": 2}}]
        es = FakeScrollES([a_page(hits), a_page([], total=1)])

        with tempfile.TemporaryDirectory() as folder:
            result = common.loadData(
                es, None, "idx", {"extra": {"exportColumns": "raw->Name, drop"}}, None, True,
                (None, None, None), True, {"id": "alice", "privileges": []}, "csv",
                "https://x/", folder + "/",
            )

            with open(result["file"], encoding="utf8") as handle:
                header = handle.readline().strip()
            # `loadData` writes `df.to_csv(path)` without `index=False`, unlike `write_export`:
            # the first column of the file is the pandas index, unnamed. Pinned as it is.
            self.assertEqual(header, ",Name,drop")


if __name__ == "__main__":
    unittest.main()
