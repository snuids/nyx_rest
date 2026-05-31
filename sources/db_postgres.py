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
            state.pg_connection = None
            get_postgres_connection()


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

        cursor = state.pg_connection.cursor()
        logger.info(state.pg_connection.get_dsn_parameters())
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
