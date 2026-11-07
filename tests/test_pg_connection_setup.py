"""The shared PostgreSQL connection: autocommit, and how it is replaced.

One connection serves every request of a gunicorn worker. Left in a transaction, a single
failed statement made the connection answer `InFailedSqlTransaction` to every later request
— on any SQL app — and left the reads holding an open snapshot between requests. Autocommit
is what stops that; `discard_postgres_connection` is the fallback for a connection that
cannot be reused at all.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:
    import psycopg2                                            # noqa: F401
    HAVE_PSYCOPG = True
except Exception:                                              # pragma: no cover
    HAVE_PSYCOPG = False

if HAVE_PSYCOPG:
    import db_postgres
    import state
else:                                                          # pragma: no cover
    db_postgres = None


class FakeConnection:
    def __init__(self):
        self.autocommit = False
        self.closed = 0
        self.executed = []

    def cursor(self):
        connection = self

        class Cursor:
            description = None

            def execute(self, query, params=None):
                connection.executed.append(query)

            def fetchone(self):
                return ("PostgreSQL 14.17",)

            def close(self):
                pass

        return Cursor()

    def get_dsn_parameters(self):
        return {"user": "postgres", "database": "nyx", "password": "secret"}

    def close(self):
        self.closed += 1


@unittest.skipUnless(HAVE_PSYCOPG, "psycopg2 non importable")
class ConnectionSetupTests(unittest.TestCase):
    def setUp(self):
        self.saved_connection = state.pg_connection
        self.saved_thread = state.pg_thread
        state.pg_connection = None
        # a sentinel: the ping thread must not be started by a unit test
        state.pg_thread = object()
        self.addCleanup(setattr, state, "pg_connection", self.saved_connection)
        self.addCleanup(setattr, state, "pg_thread", self.saved_thread)

    def _patch_connect(self, connection):
        self.addCleanup(setattr, db_postgres.psycopg2, "connect",
                        db_postgres.psycopg2.connect)
        db_postgres.psycopg2.connect = lambda **kwargs: connection

    def test_the_connection_is_opened_in_autocommit(self):
        fake = FakeConnection()
        self._patch_connect(fake)

        result = db_postgres.get_postgres_connection()

        self.assertIs(result, fake)
        self.assertTrue(fake.autocommit,
                        "a shared connection must not keep a transaction open between requests")

    def test_the_same_connection_is_reused_across_requests(self):
        fake = FakeConnection()
        self._patch_connect(fake)

        self.assertIs(db_postgres.get_postgres_connection(),
                      db_postgres.get_postgres_connection())

    def test_discarding_closes_and_clears_the_connection(self):
        fake = FakeConnection()
        state.pg_connection = fake

        db_postgres.discard_postgres_connection()

        self.assertIsNone(state.pg_connection)
        self.assertEqual(fake.closed, 1)
        # idempotent: the health check may call it when there is nothing to drop
        db_postgres.discard_postgres_connection()


if __name__ == "__main__":
    unittest.main()
