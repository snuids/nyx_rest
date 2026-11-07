"""SQL record routes: the placeholder key and the shared connection.

One bad `DELETE` used to be enough to break a whole worker. A row whose key column is NULL
is sent by the console as the string `null`; bound to an integer column that is
`invalid input syntax for type integer: "null"`, the statement aborted the transaction of
the connection every request of that worker shares, and every later `pg_search` answered
500 with `InFailedSqlTransaction` until the health check reconnected ten seconds later.

These tests pin the three things that stop it: the placeholder never reaches the database,
a failed statement is rolled back on the connection that ran it, and the read route answers
an error instead of raising through Flask.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from route_harness import RouteTestCase, HAVE_ROUTE_DEPS   # noqa: E402

if HAVE_ROUTE_DEPS:
    import routes.data as data_routes                      # noqa: E402
else:                                                      # pragma: no cover
    data_routes = None


class FakePGError(Exception):
    """Stands in for psycopg2's `InvalidTextRepresentation`/`InFailedSqlTransaction`."""


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None

    def execute(self, query, params=None):
        self.connection.executed.append((query, params))
        if self.connection.execute_error is not None:
            raise self.connection.execute_error
        self.description = [("brand", 1043), ("year", 23)]

    def fetchone(self):
        return ("Ford", 1964)

    def fetchall(self):
        return [("Ford", 1964)]

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakePGConnection:
    """Double of the shared psycopg2 connection: records what the route does to it."""

    def __init__(self, execute_error=None, rollback_error=None):
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.autocommit = False
        self.execute_error = execute_error
        self.rollback_error = rollback_error

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1
        if self.rollback_error is not None:
            raise self.rollback_error

    def close(self):
        self.closed += 1

    def get_dsn_parameters(self):
        return {"user": "postgres", "dbname": "nyx"}


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class PlaceholderKeyTests(RouteTestCase):
    route_modules = (data_routes,)

    def setUp(self):
        super().setUp()
        self.token = self.login_as("alice@example.com", ["YUL"])
        self.pg = FakePGConnection()
        self._install("pg_connection", self.pg)
        self._patch_connection(self.pg)

    def _patch_connection(self, connection):
        original = data_routes.get_postgres_connection
        data_routes.get_postgres_connection = lambda: connection
        self.addCleanup(setattr, data_routes, "get_postgres_connection", original)

    def test_a_null_key_is_refused_before_the_database(self):
        # exactly what the console sends for a row whose `pkey` column is NULL
        code, body, _ = self.call("delete", "/api/v1/pg_generic/cars/year/null", self.token)
        self.assertEqual(body["error"], "invalid record identifier")
        self.assertEqual(self.pg.executed, [],
                         "no statement may reach a database that cannot honour it")

    def test_every_spelling_of_a_missing_key_is_refused(self):
        for value in ("null", "NULL", "None", "undefined", "nan", "%20"):
            with self.subTest(value=value):
                code, body, _ = self.call(
                    "delete", "/api/v1/pg_generic/cars/year/" + value, self.token)
                self.assertEqual(body["error"], "invalid record identifier")
        self.assertEqual(self.pg.executed, [])

    def test_a_real_key_is_still_deleted(self):
        code, body, _ = self.call("delete", "/api/v1/pg_generic/cars/year/1964", self.token)
        self.assertEqual(body["error"], "")
        self.assertEqual(self.pg.executed[0][0], 'DELETE FROM "cars" WHERE "year" = %s')
        self.assertEqual(self.pg.executed[0][1], ("1964",))
        self.assertEqual(self.pg.commits, 1)


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class FailedStatementTests(RouteTestCase):
    route_modules = (data_routes,)

    def setUp(self):
        super().setUp()
        self.token = self.login_as("alice@example.com", ["YUL"])

    def _patch_connection(self, connection):
        original = data_routes.get_postgres_connection
        data_routes.get_postgres_connection = lambda: connection
        self.addCleanup(setattr, data_routes, "get_postgres_connection", original)

    def test_a_failed_delete_is_rolled_back_on_the_shared_connection(self):
        pg = FakePGConnection(execute_error=FakePGError("invalid input syntax for type integer"))
        self._install("pg_connection", pg)
        self._patch_connection(pg)

        code, body, _ = self.call("delete", "/api/v1/pg_generic/cars/year/1964", self.token)
        self.assertEqual(body["error"], "unable to delete record")
        self.assertEqual(pg.rollbacks, 1, "the aborted transaction must be closed")
        self.assertEqual(pg.commits, 0)
        self.assertIs(self.state.pg_connection, pg,
                      "a connection that rolled back is still usable: keep it")

    def test_a_failed_save_is_rolled_back_too(self):
        pg = FakePGConnection(execute_error=FakePGError("value too long"))
        self._install("pg_connection", pg)
        self._patch_connection(pg)

        code, body, _ = self.call(
            "post", "/api/v1/pg_generic/cars/year/1964", self.token,
            body={"record": [{"key": "brand", "value": "x" * 400}]})
        self.assertEqual(body["error"], "unable to save record")
        self.assertEqual(pg.rollbacks, 1)

    def test_a_connection_that_cannot_roll_back_is_dropped(self):
        # a dead socket: rollback itself fails, so the next call must reconnect rather
        # than answer 500 on the same broken connection until the ten-second health check
        pg = FakePGConnection(execute_error=FakePGError("server closed the connection"),
                              rollback_error=FakePGError("connection already closed"))
        self._install("pg_connection", pg)
        self._patch_connection(pg)

        self.call("delete", "/api/v1/pg_generic/cars/year/1964", self.token)
        self.assertIsNone(self.state.pg_connection)
        self.assertEqual(pg.closed, 1)

    def test_a_failing_read_answers_an_error_instead_of_a_500(self):
        def boom(*args, **kwargs):
            raise FakePGError("current transaction is aborted")

        pg = FakePGConnection()
        self._install("pg_connection", pg)
        self._patch_connection(pg)
        original = data_routes.loadPGData
        data_routes.loadPGData = boom
        self.addCleanup(setattr, data_routes, "loadPGData", original)

        code, body, _ = self.call(
            "post", "/api/v1/pg_search/id_30934302", self.token,
            body={"size": 100, "page": 1, "sort": {}})
        self.assertEqual(code, 200)
        self.assertEqual(body["error"], "unable to load data")
        self.assertEqual(body["records"], [])
        self.assertEqual(pg.rollbacks, 1)


@unittest.skipUnless(data_routes is not None, "routes.data non importable")
class PlaceholderHelperTests(unittest.TestCase):
    def test_the_placeholder_predicate_reads_what_javascript_produces(self):
        # `null` for a NULL key column, `undefined` for a missing one, `NaN` from a number
        for value in (None, "", "null", "NULL", " None ", "undefined", "nan"):
            with self.subTest(value=value):
                self.assertTrue(data_routes.is_placeholder_key(value))
        for value in ("1964", "0", "false", "abc"):
            with self.subTest(value=value):
                self.assertFalse(data_routes.is_placeholder_key(value))


if __name__ == "__main__":
    unittest.main()
