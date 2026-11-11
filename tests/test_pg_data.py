"""Tests for `pg_common.loadPGData`, the read path of the SQL applications.

The function builds the statement from what the application declares (its SQL, its time field, its
columns), runs a count, an optional histogram, then the page itself, and either returns the rows or
writes the export. It was the last large hole of this repository — 45 % against `pg_common.py`,
essentially this one function — and it carries the two places where a value used to travel into the
SQL as text: the page size and the sort column.

The driver is doubled rather than installed: `pyodbc` only exists in the image, so the SQL Server
branch replaces `pg_common.get_sql_server_connection`; the PostgreSQL branch takes a connection
object whose cursor records the statements it is given.
"""

import datetime
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import pg_common                                                               # noqa: E402

CONFIG = {
    "sql": "select id, name from my_table",
    "databaseType": "postgres",
    "headercolumns": [{"field": "id"}, {"field": "name"}],
    "pkey": "id",
    "index": "my_table",
}

DESCRIPTION = [("id", 23, None, None, None), ("name", 25, None, None, None)]


class FakeCursor:
    """A cursor that answers by the shape of the statement it was given."""

    def __init__(self, count=0, records=(), histogram=(), groups=(), description=None):
        self.count = count
        self.records = list(records)
        self.histogram = list(histogram)
        self.groups = list(groups)
        self.description = description or DESCRIPTION
        self.statements = []
        self.last = ""

    def execute(self, statement, *params):
        self.statements.append((statement, params))
        self.last = statement

    def fetchone(self):
        return (self.count,)

    def fetchall(self):
        lowered = self.last.lower()
        if "date_trunc" in lowered or "dateadd" in lowered:
            return list(self.histogram)
        if "count(*)" in lowered:
            return list(self.groups)
        return list(self.records)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def statements_matching(self, fragment):
        return [statement for statement, _ in self.statements if fragment in statement]


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


class FakeAppES:
    """`getAppByID` reads `nyx_app` once per (es, id): each test gets its own object."""

    def __init__(self, config=None, **extra):
        source = {"config": dict(config or CONFIG)}
        source.update(extra)
        self.document = {"_id": "app-1", "_source": source}

    def get(self, index=None, id=None, **kwargs):
        return self.document


class PgDataTestCase(unittest.TestCase):
    def setUp(self):
        pg_common.sql_server_connections.clear()
        self.addCleanup(pg_common.sql_server_connections.clear)

    def load(self, cursor, es=None, data=None, download=False, outputformat="csv",
             folder=None, conn=None, is_rest_api=True):
        return pg_common.loadPGData(
            es or FakeAppES(), "app-1", FakeConnection(cursor), conn, data or {},
            download, is_rest_api, {"id": "alice", "privileges": []},
            outputformat, "https://energy.example/generated/", folder or "/tmp/",
        )


class ReadTests(PgDataTestCase):
    def test_the_rows_are_returned_with_their_columns(self):
        cursor = FakeCursor(count=2, records=[(1, "a"), (2, "b")])

        result = self.load(cursor)

        self.assertEqual(result["error"], "")
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["records"], [
            {"id": 1, "name": "a", "_id": 1},
            {"id": 2, "name": "b", "_id": 2},
        ])
        self.assertEqual(result["colnames"], [{"col": "id", "type": 23},
                                              {"col": "name", "type": 25}])
        self.assertIsNone(result["aggs"])

    def test_the_count_statement_is_built_from_the_application_sql(self):
        cursor = FakeCursor(count=0)

        self.load(cursor)

        self.assertIn("select count(*) from my_table", cursor.statements[0][0])
        self.assertIn("LIMIT 1000", cursor.statements[-1][0],
                      "the default page size bounds the read")

    def test_the_page_and_the_size_become_a_limit_and_an_offset(self):
        cursor = FakeCursor(count=0, records=[])

        self.load(cursor, data={"size": 10, "page": 3})

        self.assertIn("LIMIT 10 OFFSET 20", cursor.statements[-1][0])

    def test_a_worthless_page_size_falls_back_on_the_default(self):
        cursor = FakeCursor(count=0)

        self.load(cursor, data={"size": "10; drop table users", "page": "x"})

        statement = cursor.statements[-1][0]
        self.assertIn("LIMIT 1000", statement)
        self.assertNotIn("drop table", statement)

    def test_a_declared_column_can_be_sorted_on(self):
        cursor = FakeCursor(count=0)

        self.load(cursor, data={"sort": {"column": "name", "order": "descending"}})

        self.assertIn('ORDER BY "name" DESC', cursor.statements[-1][0])

    def test_a_column_the_application_does_not_declare_is_not_sorted_on(self):
        cursor = FakeCursor(count=0)

        self.load(cursor, data={"sort": {"column": "password", "order": "descending"}})

        statement = cursor.statements[-1][0]
        self.assertNotIn("password", statement)
        self.assertNotIn("ORDER BY", statement)


class TimeRangeTests(PgDataTestCase):
    def app_with_time_field(self):
        config = dict(CONFIG, timefield="ts")
        return FakeAppES(config)

    def test_a_range_becomes_a_where_clause_on_the_time_field(self):
        cursor = FakeCursor(count=0)

        self.load(cursor, es=self.app_with_time_field(),
                  data={"range": {"gte": 0, "lte": 7200000}})

        statement = cursor.statements[-1][0]
        self.assertIn("where ts >= '", statement)
        self.assertIn("and ts <= '", statement)

    def test_an_existing_where_clause_keeps_its_terms(self):
        config = dict(CONFIG, timefield="ts", sql="select id, name from my_table where active = 1")
        cursor = FakeCursor(count=0)

        self.load(cursor, es=FakeAppES(config), data={"range": {"gte": 0, "lte": 60000}})

        self.assertIn("AND (ts >=", cursor.statements[-1][0])

    def test_a_grouped_query_gets_the_range_before_its_group_by(self):
        config = dict(CONFIG, timefield="ts",
                      sql="select name, count(*) from my_table where active = 1 group by name")
        cursor = FakeCursor(count=0)

        self.load(cursor, es=FakeAppES(config), data={"range": {"gte": 0, "lte": 60000}})

        statement = cursor.statements[-1][0]
        self.assertLess(statement.index("AND (ts"), statement.lower().index("group by"))

    def test_no_time_field_means_no_range_clause(self):
        cursor = FakeCursor(count=0)

        self.load(cursor, data={"range": {"gte": 0, "lte": 60000}})

        self.assertNotIn(" ts ", cursor.statements[-1][0])


class FilterAndErrorTests(PgDataTestCase):
    def test_the_search_term_is_bound_not_written_into_the_statement(self):
        config = dict(CONFIG, sql="select id, name from my_table where name like ${FILTER}")
        cursor = FakeCursor(count=0)

        self.load(cursor, es=FakeAppES(config), data={"query": "pump' OR '1'='1"})

        statement, params = cursor.statements[-1]
        self.assertNotIn("pump", statement, "the term never reaches the SQL text")
        self.assertEqual(params, (["%pump' OR '1'='1%"],))

    def test_a_filter_token_that_cannot_be_bound_is_refused(self):
        config = dict(CONFIG, sql="select id from my_table where name = 'x-${FILTER}'")
        cursor = FakeCursor(count=0)

        result = self.load(cursor, es=FakeAppES(config), data={"query": "pump"})

        self.assertEqual(result, {"error": "invalid search query", "records": [], "aggs": None,
                                  "total": 0, "colnames": []})
        self.assertEqual(cursor.statements, [], "nothing is sent to the database")


class AggregationTests(PgDataTestCase):
    def app_with_graphic(self):
        config = dict(CONFIG, timefield="ts")
        return FakeAppES(config, graphicChecked=True)

    def test_a_charted_application_gets_a_histogram(self):
        cursor = FakeCursor(count=0, histogram=[(3, datetime.datetime(2026, 11, 9, 10, 0))])

        result = self.load(cursor, es=self.app_with_graphic(),
                           data={"range": {"gte": 0, "lte": 60000}})

        self.assertIn("date_trunc('second',ts)", " ".join(cursor.statements_matching("date_trunc")))
        self.assertEqual(result["aggtime"], "1s")
        self.assertEqual(result["aggtimeval"], 1)
        self.assertEqual(result["aggs"]["2"]["buckets"][0]["doc_count"], 3)

    def test_a_long_range_is_grouped_by_hour(self):
        # < 5 min -> seconde, < 4 h -> minute, < 5 jours -> heure, au-dela -> semaine
        cursor = FakeCursor(count=0, histogram=[])

        result = self.load(cursor, es=self.app_with_graphic(),
                           data={"range": {"gte": 0, "lte": 6 * 60 * 60 * 1000}})

        self.assertIn("date_trunc('hour',ts)", " ".join(cursor.statements_matching("date_trunc")))
        self.assertEqual(result["aggtime"], "1h")
        self.assertEqual(result["aggtimeval"], 3600)

    def test_a_week_and_beyond_is_grouped_by_week(self):
        cursor = FakeCursor(count=0)

        result = self.load(cursor, es=self.app_with_graphic(),
                           data={"range": {"gte": 0, "lte": 90 * 24 * 60 * 60 * 1000}})

        self.assertIn("date_trunc('week',ts)", " ".join(cursor.statements_matching("date_trunc")))
        self.assertEqual(result["aggtime"], "1w")


class ExportTests(PgDataTestCase):
    def test_a_small_export_is_written_and_its_url_returned(self):
        cursor = FakeCursor(count=1, records=[(1, "a")])

        with tempfile.TemporaryDirectory() as folder:
            result = self.load(cursor, download=True, folder=folder + "/")

            self.assertEqual(result["type"], "direct")
            self.assertTrue(result["url"].startswith("https://energy.example/generated/export_"))
            self.assertTrue(os.path.isfile(result["file"]))
            with open(result["file"], encoding="utf8") as handle:
                self.assertIn("name", handle.readline())

    def test_an_export_above_five_thousand_rows_is_delegated(self):
        records = [(position, "n%d" % position) for position in range(5001)]
        cursor = FakeCursor(count=5001, records=records)
        broker = mock.Mock()

        result = self.load(cursor, download=True, conn=broker)

        self.assertEqual(result["type"], "mail")
        destination, body = broker.send_message.call_args.args
        self.assertEqual(destination, "/queue/REST_LOAD_PGDATA")
        self.assertIn('"appid": "app-1"', body)

    def test_the_declared_export_columns_filter_the_file(self):
        config = dict(CONFIG, exportColumns="name")
        cursor = FakeCursor(count=1, records=[(1, "a")])

        with tempfile.TemporaryDirectory() as folder:
            result = self.load(cursor, es=FakeAppES(config), download=True, folder=folder + "/")

            with open(result["file"], encoding="utf8") as handle:
                header = handle.readline().strip()
            # `df.to_csv(...)` without `index=False`: the first column is the pandas index.
            self.assertEqual(header, ",name")

    def test_a_date_column_reaches_the_file_as_a_date(self):
        # 1082 is PostgreSQL's `date`: the cell must not stay an integer offset. The column has to
        # be declared by the application (`headercolumns`), which is what builds the row.
        config = dict(CONFIG, headercolumns=[{"field": "id"}, {"field": "day"}])
        cursor = FakeCursor(
            count=1, records=[(1, datetime.datetime(2026, 11, 9, 10, 30))],
            description=[("id", 23, None, None, None), ("day", 1082, None, None, None)],
        )

        with tempfile.TemporaryDirectory() as folder:
            result = self.load(cursor, es=FakeAppES(config), download=True, folder=folder + "/")

            with open(result["file"], encoding="utf8") as handle:
                rows = handle.read().strip().split("\n")
        self.assertEqual(len(cursor.statements), 2, "a count, then the page")
        self.assertIn("2026-11-09", rows[1])


class SqlServerTests(PgDataTestCase):
    def sqlserver_app(self):
        config = dict(CONFIG, databaseType="sqlserver", database="nyx")
        return FakeAppES(config)

    def test_the_rows_are_read_with_top_and_the_display_size_of_the_driver(self):
        cursor = FakeCursor(count=1, records=[(1, "a")], description=DESCRIPTION)
        connection = FakeConnection(cursor)

        with mock.patch.object(pg_common, "get_sql_server_connection", return_value=connection):
            result = self.load(cursor, es=self.sqlserver_app())

        self.assertIn("select TOP 1000", cursor.statements[-1][0])
        self.assertEqual(result["colnames"][0]["type"], None, "the type comes from the 5th field")
        self.assertEqual(connection.commits, 1, "the SQL Server write is committed")

    def test_a_later_page_uses_offset_fetch(self):
        cursor = FakeCursor(count=0, records=[], description=DESCRIPTION)
        connection = FakeConnection(cursor)

        with mock.patch.object(pg_common, "get_sql_server_connection", return_value=connection):
            self.load(cursor, es=self.sqlserver_app(), data={"size": 10, "page": 2})

        statement = cursor.statements[-1][0]
        self.assertIn("OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY", statement)
        self.assertIn("order by 1", statement.lower(), "a paged read needs an order")


if __name__ == "__main__":
    unittest.main()
