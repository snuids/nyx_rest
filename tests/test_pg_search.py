"""
The SQL a `pg_search` runs: what the request body may and may not become.

`pg_common.loadPGData` pasted two values of the body into the statement — the free-text search
(`${FILTER}`) and the sort column (`ORDER BY ` + column). The console's search box of any SQL
application was therefore a way of reading or modifying the table behind it:

    {"query": "pump' OR '1'='1"}
    {"sort": {"column": "kwh; DROP TABLE cars"}}

`lib/pg_search.py` now binds the filter and only accepts a declared column as a sort key. These
tests hold both halves: the pure functions, and the statements the engine actually hands to the
cursor — read from a fake cursor, because what matters is the text and the parameters, not the
rows the database would return.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from lib.pg_search import (  # noqa: E402
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    bind_filter,
    page_values,
    sort_clause,
)

try:
    import pg_common          # noqa: F401  (imports pandas, psycopg2)
    HAVE_PG = True
    PG_MISSING = ""
except Exception as exc:      # pragma: no cover - depends on the machine running the tests
    HAVE_PG = False
    PG_MISSING = type(exc).__name__ + ": " + str(exc)[:80]


PAYLOAD = "pump' OR '1'='1"
SORT_PAYLOAD = "kwh; DROP TABLE cars"


class BindFilterTests(unittest.TestCase):
    def test_the_usual_quoted_token_becomes_a_parameter(self):
        sql, params = bind_filter("select * from cars where name like '%${FILTER}%'", "pump", "postgres")

        self.assertEqual(sql, "select * from cars where name like %s")
        self.assertEqual(params, ["%pump%"])

    def test_the_quotes_are_part_of_what_is_replaced(self):
        # Replacing the token alone would leave `'%%s%'`: the marker inside a literal, and the
        # parameter never consumed.
        sql, _ = bind_filter("select * from t where name like '%${FILTER}%'", "a", "postgres")

        self.assertEqual(sql, "select * from t where name like %s")
        self.assertNotIn("'", sql, "no quote may survive around the placeholder")

    def test_one_wildcard_or_none_are_handled(self):
        for template in ("select * from t where n like '%${FILTER}'",
                         "select * from t where n like '${FILTER}'"):
            sql, params = bind_filter(template, "x", "postgres")
            self.assertEqual(sql, "select * from t where n like %s")
            self.assertEqual(params, ["%x%"])

    def test_a_bare_token_is_bound_too(self):
        sql, params = bind_filter("select * from t where n like ${FILTER}", "x", "postgres")

        self.assertEqual(sql, "select * from t where n like %s")
        self.assertEqual(params, ["%x%"])

    def test_the_search_term_never_reaches_the_statement(self):
        sql, params = bind_filter("select * from t where n like '%${FILTER}%'", PAYLOAD, "postgres")

        self.assertNotIn(PAYLOAD, sql)
        self.assertNotIn("OR '1'='1", sql)
        self.assertEqual(params, ["%" + PAYLOAD + "%"])

    def test_every_occurrence_gets_its_parameter(self):
        sql, params = bind_filter(
            "select * from t where a like '%${FILTER}%' or b like '%${FILTER}%'", "x", "postgres"
        )

        self.assertEqual(sql.count("%s"), 2)
        self.assertEqual(params, ["%x%", "%x%"])

    def test_the_parameters_follow_the_placeholders_of_the_statement(self):
        # Mixed spellings: the parameters must be in the order the placeholders appear, or the
        # second token would be compared with the first value.
        sql, params = bind_filter(
            "select * from t where a like ${FILTER} and b like '%${FILTER}%'", "x", "postgres"
        )

        self.assertEqual(sql, "select * from t where a like %s and b like %s")
        self.assertEqual(params, ["%x%", "%x%"])

    def test_a_quoted_token_is_replaced_with_its_quotes(self):
        sql, _ = bind_filter("select * from t where n like '%${FILTER}%' or m = 1", "x", "postgres")

        self.assertEqual(sql, "select * from t where n like %s or m = 1")

    def test_a_query_without_the_token_is_left_alone(self):
        sql, params = bind_filter("select * from cars", "anything", "postgres")

        self.assertEqual(sql, "select * from cars")
        self.assertEqual(params, [], "no parameters: the statement is executed untouched")

    def test_the_other_percent_signs_are_escaped_for_psycopg2(self):
        # psycopg2 formats the whole statement when parameters are passed: a `%` left as is in
        # `to_char(ts, '%Y')` becomes an unknown format character and the query dies.
        sql, params = bind_filter(
            "select to_char(ts, '%Y-%m'), x from t where n like '%${FILTER}%'", "pump", "postgres"
        )

        self.assertIn("'%%Y-%%m'", sql)
        self.assertIn("like %s", sql)
        self.assertEqual(params, ["%pump%"])

    def test_sql_server_keeps_its_percent_signs(self):
        sql, _ = bind_filter(
            "select to_char(ts, '%Y'), x from t where n like '%${FILTER}%'", "pump", "sqlserver"
        )

        self.assertIn("'%Y'", sql, "pyodbc does not format the statement")
        self.assertIn("like ?", sql)

    def test_an_empty_search_matches_everything(self):
        _, params = bind_filter("select * from t where n like '%${FILTER}%'", "", "postgres")

        self.assertEqual(params, ["%%"], "an empty search must still be a LIKE pattern")

    def test_sql_server_uses_its_own_marker(self):
        sql, params = bind_filter("select * from t where n like '%${FILTER}%'", "x", "sqlserver")

        self.assertEqual(sql, "select * from t where n like ?")
        self.assertEqual(params, ["%x%"])

    def test_a_token_inside_a_longer_literal_is_refused(self):
        # `'ref-${FILTER}-x'` cannot be bound without leaving the marker inside a literal: better
        # an error than a statement that silently lost its quotes.
        with self.assertRaises(ValueError):
            bind_filter("select * from t where ref like 'ref-${FILTER}-x'", "x", "postgres")


class PageValuesTests(unittest.TestCase):
    def test_the_defaults(self):
        self.assertEqual(page_values(None, None), (DEFAULT_PAGE_SIZE, 1))

    def test_the_size_is_an_integer(self):
        self.assertEqual(page_values(50, 2), (50, 2))
        self.assertEqual(page_values("50", "2"), (50, 2))

    def test_a_size_that_is_not_a_number_is_not_a_page_size(self):
        self.assertEqual(page_values(SORT_PAYLOAD, 1)[0], DEFAULT_PAGE_SIZE)
        self.assertEqual(page_values("10; DROP TABLE cars", 1)[0], DEFAULT_PAGE_SIZE)

    def test_the_bounds(self):
        self.assertEqual(page_values(0, 1)[0], 1)
        self.assertEqual(page_values(-10, 1)[0], 1)
        self.assertEqual(page_values(MAX_PAGE_SIZE * 10, 1)[0], MAX_PAGE_SIZE)
        self.assertEqual(page_values(10, 0)[1], 1)
        self.assertEqual(page_values(10, "deux")[1], 1)


class SortClauseTests(unittest.TestCase):
    columns = ["kwh", "device", "id"]

    def test_a_declared_column_is_quoted(self):
        self.assertEqual(sort_clause({"column": "kwh"}, self.columns, "postgres"),
                         ' ORDER BY "kwh"')
        self.assertEqual(sort_clause({"column": "kwh", "order": "descending"}, self.columns, "postgres"),
                         ' ORDER BY "kwh" DESC')

    def test_the_quoting_depends_on_the_database(self):
        self.assertEqual(sort_clause({"column": "kwh"}, self.columns, "sqlserver"),
                         " ORDER BY [kwh]")

    def test_an_undeclared_column_is_ignored(self):
        self.assertEqual(sort_clause({"column": "salary"}, self.columns, "postgres"), "")

    def test_an_injected_column_is_ignored(self):
        self.assertEqual(sort_clause({"column": SORT_PAYLOAD}, self.columns, "postgres"), "")
        self.assertEqual(sort_clause({"column": 'kwh" DESC, (select 1)'}, self.columns, "postgres"), "")

    def test_nothing_asked_is_nothing_added(self):
        for sort in (None, {}, {"column": ""}, {"column": 3}, "kwh"):
            self.assertEqual(sort_clause(sort, self.columns, "postgres"), "")


class FakeCursor:
    """Records the statements and returns the rows the test decided."""

    def __init__(self, rows, description, count=0):
        self.rows = rows
        self.description = description
        self.count = count
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchone(self):
        return (self.count,)

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class FakeCluster:
    def __init__(self, app):
        self.app = app

    def get(self, index, id):        # noqa: A002 - signature of the client
        return {"_id": id, "_source": self.app}


def postgres_app(**overrides):
    config = {
        "databaseType": "postgres",
        "pkey": "id",
        "headercolumns": [{"field": "kwh"}, {"field": "device"}],
        "sql": "select kwh, device from cars where name like '%${FILTER}%'",
    }
    config.update(overrides)
    return {"title": "Cars", "type": "pgsql-generic-table", "config": config}


@unittest.skipUnless(HAVE_PG, "pg dependencies missing (" + PG_MISSING + ")")
class LoadPGDataTests(unittest.TestCase):
    """The engine, with a fake cursor: what it would send, not what the database would answer."""

    def search(self, body=None, app=None):
        # `getAppByID` caches on the (client, id) pair: a fresh FakeCluster per call is a fresh
        # key, so the constant app id cannot leak one test's document into another.
        cursor = FakeCursor(rows=[(1.5, "pump", 7)],
                            description=[("kwh", 701), ("device", 1043), ("id", 23)], count=1)
        connection = FakeConnection(cursor)
        es = FakeCluster(app or postgres_app())
        result = pg_common.loadPGData(es, "cars-app", connection, object(), body, False, True,
                                      {"id": "tester"}, "csv", "http://out/", "/tmp/")
        return result, cursor, connection

    def test_the_search_term_is_bound_in_every_statement(self):
        _, cursor, _ = self.search({"query": PAYLOAD})

        self.assertGreaterEqual(len(cursor.statements), 2, "count then select")
        for sql, params in cursor.statements:
            self.assertNotIn(PAYLOAD, sql, "the term must never be in the statement")
            self.assertIn("%" + PAYLOAD + "%", params)
            self.assertIn("%s", sql)

    def test_the_count_and_the_select_share_the_parameters(self):
        _, cursor, _ = self.search({"query": "pump"})

        count_sql, count_params = cursor.statements[0]
        select_sql, select_params = cursor.statements[-1]
        self.assertIn("count(*)", count_sql.lower())
        self.assertEqual(count_params, select_params)

    def test_a_declared_sort_column_is_applied(self):
        _, cursor, _ = self.search({"sort": {"column": "kwh", "order": "descending"}})

        self.assertIn('ORDER BY "kwh" DESC', cursor.statements[-1][0])

    def test_an_injected_sort_column_is_dropped(self):
        _, cursor, _ = self.search({"sort": {"column": SORT_PAYLOAD}})

        statement = cursor.statements[-1][0]
        self.assertNotIn("ORDER BY", statement)
        self.assertNotIn("DROP", statement)

    def test_an_injected_page_size_is_not_a_page_size(self):
        _, cursor, _ = self.search({"size": "10; DROP TABLE cars"})

        statement = cursor.statements[-1][0]
        self.assertNotIn("DROP", statement)
        self.assertIn("LIMIT " + str(DEFAULT_PAGE_SIZE), statement)

    def test_the_page_number_becomes_an_offset(self):
        _, cursor, _ = self.search({"size": 10, "page": 3})

        self.assertIn("LIMIT 10 OFFSET 20", cursor.statements[-1][0])

    def test_an_app_without_the_token_still_runs_without_parameters(self):
        app = postgres_app(sql="select kwh, device from cars where kwh > 0")

        result, cursor, _ = self.search({"query": "pump"}, app=app)

        self.assertEqual(result["error"], "")
        for sql, params in cursor.statements:
            self.assertIsNone(params, "a query without ${FILTER} must reach the driver untouched")

    def test_an_app_without_database_type_is_read_as_postgres(self):
        app = postgres_app()
        del app["config"]["databaseType"]

        result, cursor, _ = self.search({"query": "pump"}, app=app)

        self.assertEqual(result["error"], "")
        self.assertIn("%s", cursor.statements[-1][0])

    def test_the_rows_come_back_shaped_as_the_console_expects(self):
        result, _, connection = self.search({"query": "pump"})

        self.assertEqual(result["error"], "")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["records"][0]["device"], "pump")
        self.assertEqual(result["records"][0]["_id"], 7)
        self.assertEqual([column["col"] for column in result["colnames"]],
                         ["kwh", "device", "id"])
        self.assertEqual(connection.commits, 1)

    def test_a_query_without_a_range_does_not_crash_the_graph_block(self):
        app = postgres_app()
        app["graphicChecked"] = True

        result, _, _ = self.search({"query": "pump"}, app=app)

        self.assertEqual(result["error"], "", "graphicChecked without a range used to raise")

    def test_an_unbindable_filter_is_refused(self):
        app = postgres_app(sql="select * from cars where ref like 'ref-${FILTER}-x'")

        result, cursor, _ = self.search({"query": "pump"}, app=app)

        self.assertEqual(result["error"], "invalid search query")
        self.assertEqual(cursor.statements, [], "nothing may reach the database")


if __name__ == "__main__":
    unittest.main()
