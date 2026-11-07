"""
PostgreSQL connection management: connection pooling, health-check thread,
and get_postgres_connection() helper.
"""

import time
import logging
import threading
import concurrent.futures

import psycopg2

from config import settings
import state

logger = logging.getLogger()


def check_pg():
    while True:
        time.sleep(10)
        try:
            logger.info("Check PG...")
            cur = state.pg_connection.cursor()
            cur.execute('SELECT 1')
            cur.close()
        except Exception as e:
            logger.error("Unable to check postgresql", exc_info=True)
            discard_postgres_connection()
            get_postgres_connection()


def discard_postgres_connection():
    """Drop the shared connection so the next call reconnects.

    One connection serves every request of a worker: when it becomes unusable (server
    restart, broken socket) the whole worker answers 500 until something replaces it.
    The health check does that on its own, but it only looks once every ten seconds, so
    a request that fails on a dead connection keeps failing for that long. Any caller
    that sees an error it cannot classify can call this instead of waiting for the thread.
    """
    conn = state.pg_connection
    state.pg_connection = None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            logger.warning("Unable to close the PostgreSQL connection", exc_info=True)


def get_postgres_connection():
    if state.pg_connection is not None:
        return state.pg_connection
    logger.info(">>> Create PG Connection")
    try:
        def _connect():
            return psycopg2.connect(
                user=settings.PG_LOGIN,
                password=settings.PG_PASSWORD,
                host=settings.PG_HOST,
                port=settings.PG_PORT,
                database=settings.PG_DATABASE,
                connect_timeout=5,
                gssencmode="disable",
                options="-c statement_timeout=5000",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _executor:
            _future = _executor.submit(_connect)
            try:
                state.pg_connection = _future.result(timeout=6)
            except concurrent.futures.TimeoutError:
                raise Exception("PostgreSQL connection timed out after 6 seconds")

        # One connection serves every request of the worker, so a transaction that stays
        # open is shared too: a statement that fails leaves it aborted and every later
        # request of that worker answers `InFailedSqlTransaction` — on any SQL app, not
        # just the one that failed — until the health check replaces the connection ten
        # seconds later. In autocommit no transaction is left behind, so the failed
        # statement is the only casualty; the explicit `commit()` calls still work (they
        # become no-ops) and the connection no longer sits `idle in transaction`, which
        # also stopped the reads from holding a snapshot between requests.
        state.pg_connection.autocommit = True

        cursor = state.pg_connection.cursor()
        dsn_params = state.pg_connection.get_dsn_parameters()
        dsn_params.pop("password", None)
        logger.info(dsn_params)
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        logger.info("Connected to - %s" % (record,))

        if state.pg_thread is None:
            logger.info("Creating PG ping thread.")
            state.pg_thread = threading.Thread(target=check_pg)
            state.pg_thread.start()

        return state.pg_connection
    except (Exception, psycopg2.Error) as error:
        logger.error("Error while connecting to PostgreSQL", error)
    return None
