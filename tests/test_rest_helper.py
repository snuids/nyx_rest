"""Tests for `nyx_rest_helper.py`, the export worker.

The file is a service: importing it configures logging, opens the broker link and builds the
cluster client. The suite therefore imports it **once**, in `setUpModule`, with both constructors
doubled and a `logs/` directory in a temporary working directory — the same way the container
starts, minus the network.

What matters here is the part no running worker shows: an export request is only served when the
account behind it still has the right to the index, and the mail goes to the address the cluster
knows, not to the one the message carries.
"""

import os
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

helper = None
_temporary = None
_previous_cwd = None


def setUpModule():
    """Import the worker with its two clients doubled, outside the repository tree."""
    global helper, _temporary, _previous_cwd
    _previous_cwd = os.getcwd()
    _temporary = tempfile.TemporaryDirectory()
    os.chdir(_temporary.name)
    os.makedirs("logs", exist_ok=True)

    with mock.patch("amqstompclient.amqstompclient.AMQClient", return_value=mock.Mock()), \
            mock.patch("opensearchpy.OpenSearch", return_value=mock.Mock()):
        import nyx_rest_helper

    helper = nyx_rest_helper


def tearDownModule():
    global _temporary
    os.chdir(_previous_cwd)
    _temporary.cleanup()


ALICE = {"id": "alice@example.com", "_id": "alice@example.com", "_source": {
    "id": "alice@example.com", "firstname": "Alice", "lastname": "Marchand",
    "privileges": ["admin"], "login": "alice",
}}


class HelperTestCase(unittest.TestCase):
    def setUp(self):
        self.saved = {name: getattr(helper, name, None)
                      for name in ("es", "conn", "pg_connection", "pg_thread",
                                   "SMTP_ADDRESS", "SMTP_USER", "SMTP_PASSWORD", "SMTP_FROM",
                                   "SMTP_PORT", "OUTPUT_FOLDER", "OUTPUT_URL")}
        self.addCleanup(self.restore)
        helper.conn = mock.Mock()
        helper.pg_connection = None
        helper.pg_thread = None
        helper.SMTP_ADDRESS = "smtp.example"
        helper.SMTP_USER = ""
        helper.SMTP_PASSWORD = ""
        helper.SMTP_FROM = "nyx@example.com"
        helper.SMTP_PORT = 25
        helper.OUTPUT_FOLDER = "/tmp/"
        helper.OUTPUT_URL = "https://energy.example/generated/"

    def restore(self):
        for name, value in self.saved.items():
            setattr(helper, name, value)

    def cluster(self, accounts=None):
        es = mock.Mock()
        store = accounts or {"alice@example.com": ALICE}

        def get(index=None, id=None, **kwargs):
            if id in store:
                return store[id]
            raise LookupError(id)

        es.get.side_effect = get
        helper.es = es
        return es


class AccountBehindTests(HelperTestCase):
    def test_the_account_is_read_from_the_cluster_by_the_id_the_message_carries(self):
        es = self.cluster()

        user = helper.account_behind({"user": {"id": "alice@example.com"}})

        self.assertEqual(user["firstname"], "Alice")
        self.assertEqual(user["id"], "alice@example.com")
        es.get.assert_called_with(index="nyx_user", id="alice@example.com")

    def test_a_message_without_an_account_is_dropped(self):
        self.cluster()

        self.assertIsNone(helper.account_behind({}))
        self.assertIsNone(helper.account_behind({"user": {}}))

    def test_an_unknown_account_is_dropped(self):
        self.cluster()

        self.assertIsNone(helper.account_behind({"user": {"id": "stranger@example.com"}}))

    def test_without_a_cluster_client_everything_is_dropped(self):
        helper.es = None

        self.assertIsNone(helper.account_behind({"user": {"id": "alice@example.com"}}))


class MessageDispatchTests(HelperTestCase):
    def message(self, **fields):
        base = {"user": {"id": "alice@example.com"}, "index": "nyx_data", "data": {},
                "doc_type": None, "download": True, "cui": (None, None, None),
                "outputformat": "csv", "appid": "app-1", "matchrequest": {}}
        base.update(fields)
        return base

    def test_an_export_the_account_may_not_run_is_refused(self):
        self.cluster()
        with mock.patch.object(helper, "load_rules", return_value={}), \
                mock.patch.object(helper, "may_export", return_value=False) as allowed, \
                mock.patch.object(helper, "remoteLoadData") as load, \
                mock.patch.object(helper, "sendMail") as mail:
            helper.messageReceived("/queue/REST_LOAD_DATA", __import__("json").dumps(self.message()),
                                   {})

        self.assertTrue(allowed.called)
        self.assertFalse(load.called)
        self.assertFalse(mail.called)

    def test_an_allowed_export_is_run_and_mailed_to_the_account_of_the_cluster(self):
        self.cluster()
        task = {"total": 3, "took": 1, "file": "/tmp/x.csv"}
        with mock.patch.object(helper, "load_rules", return_value={}), \
                mock.patch.object(helper, "may_export", return_value=True), \
                mock.patch.object(helper, "remoteLoadData", return_value=task) as load, \
                mock.patch.object(helper, "sendMail") as mail:
            helper.messageReceived("/queue/REST_LOAD_DATA", __import__("json").dumps(self.message()),
                                   {})

        message = load.call_args.args[0]
        self.assertEqual(message["user"]["id"], "alice@example.com",
                         "the recipient is the account of the cluster, not the one claimed")
        mail.assert_called_once_with(task, message)

    def test_a_sql_export_is_refused_for_an_unknown_account(self):
        self.cluster()
        with mock.patch.object(helper, "remoteLoadPGData") as load, \
                mock.patch.object(helper, "sendMail") as mail:
            helper.messageReceived("/queue/REST_LOAD_PGDATA",
                                   __import__("json").dumps(self.message(user={"id": "bob"})), {})

        self.assertFalse(load.called)
        self.assertFalse(mail.called)

    def test_a_kibana_export_needs_every_index_of_the_dashboard(self):
        self.cluster()
        with mock.patch.object(helper, "load_rules", return_value={}), \
                mock.patch.object(helper, "kibana_indices", return_value=["a", "b"]), \
                mock.patch.object(helper, "may_export", side_effect=[True, False]), \
                mock.patch.object(helper, "remoteKibanaData") as load:
            helper.messageReceived("/queue/REST_LOAD_KIBANA",
                                   __import__("json").dumps(self.message()), {})

        self.assertFalse(load.called, "one refused index refuses the whole export")

    def test_a_kibana_export_is_summarised_for_the_mail(self):
        self.cluster()
        produced = {"records": 12, "fileName": "export_x.xlsx",
                    "timing": {"load": 2, "export": 3, "process": 1}}
        with mock.patch.object(helper, "load_rules", return_value={}), \
                mock.patch.object(helper, "kibana_indices", return_value=["a"]), \
                mock.patch.object(helper, "may_export", return_value=True), \
                mock.patch.object(helper, "remoteKibanaData", return_value=produced), \
                mock.patch.object(helper, "sendMail") as mail:
            helper.messageReceived("/queue/REST_LOAD_KIBANA",
                                   __import__("json").dumps(self.message()), {})

        task = mail.call_args.args[0]
        self.assertEqual(task["total"], 12)
        self.assertEqual(task["file"], "./outputs/export_x.xlsx")
        self.assertEqual(task["took"], 6, "the three timings are added up")

    def test_a_kibana_export_without_rules_or_index_is_refused(self):
        self.cluster()
        for rules, indices in ((None, ["a"]), ({}, [])):
            with self.subTest(rules=rules, indices=indices), \
                    mock.patch.object(helper, "load_rules", return_value=rules), \
                    mock.patch.object(helper, "kibana_indices", return_value=indices), \
                    mock.patch.object(helper, "remoteKibanaData") as load:
                helper.messageReceived("/queue/REST_LOAD_KIBANA",
                                       __import__("json").dumps(self.message()), {})
                self.assertFalse(load.called)

    def test_a_message_for_another_queue_is_ignored(self):
        self.cluster()
        with mock.patch.object(helper, "remoteLoadData") as load:
            helper.messageReceived("/queue/SOMETHING_ELSE", "{}", {})

        self.assertFalse(load.called)


class SendMailTests(HelperTestCase):
    def task(self, folder, name="export.csv"):
        path = os.path.join(folder, name)
        with open(path, "w", encoding="utf8") as handle:
            handle.write("a,b\n1,2\n")
        return {"total": 1, "took": 12, "file": path}

    def message(self):
        return {"user": {"id": "alice@example.com", "firstname": "Alice", "lastname": "Marchand"}}

    def test_the_export_is_zipped_attached_sent_and_removed(self):
        with tempfile.TemporaryDirectory() as folder:
            task = self.task(folder)
            server = mock.Mock()
            with mock.patch.object(helper.smtplib, "SMTP", return_value=server) as smtp:
                helper.sendMail(task, self.message())

            smtp.assert_called_once_with("smtp.example", 25)
            message = server.send_message.call_args.args[0]
            self.assertEqual(message["To"], "alice@example.com")
            self.assertEqual(message["Subject"], "Data From NYX")
            body = message.get_payload()[0].get_payload()
            self.assertIn("Alice Marchand", body)
            self.assertIn("Records: 1", body)
            self.assertTrue(server.quit.called)
            # the archive is written in the working directory and removed afterwards
            self.assertEqual([name for name in os.listdir(".") if name.endswith(".zip")], [])

    def test_the_archive_really_contains_the_export(self):
        with tempfile.TemporaryDirectory() as folder:
            task = self.task(folder)
            server = mock.Mock()
            seen = {}

            def keep(message):
                attachment = message.get_payload()[-1]
                payload = attachment.get_payload(decode=True)
                seen["zip"] = payload
                return {}

            server.send_message.side_effect = keep
            with mock.patch.object(helper.smtplib, "SMTP", return_value=server):
                helper.sendMail(task, self.message())

        archive = zipfile.ZipFile(__import__("io").BytesIO(seen["zip"]))
        self.assertEqual(len(archive.namelist()), 1)
        self.assertIn("a,b", archive.read(archive.namelist()[0]).decode("utf8"))

    def test_an_ssl_server_is_opened_and_the_account_logged_in_when_configured(self):
        helper.SMTP_USER = "nyx"
        helper.SMTP_PASSWORD = "s3cr3t"
        with tempfile.TemporaryDirectory() as folder, \
                mock.patch.object(helper.settings, "SMTP_SSL", True), \
                mock.patch.object(helper.settings, "SMTP_TLS", True):
            server = mock.Mock()
            with mock.patch.object(helper.smtplib, "SMTP_SSL", return_value=server) as smtp:
                helper.sendMail(self.task(folder), self.message())

        self.assertTrue(smtp.called)
        self.assertTrue(server.ehlo.called)
        self.assertTrue(server.starttls.called)
        server.login.assert_called_once_with("nyx", "s3cr3t")

    def test_a_missing_export_is_reported_and_raised(self):
        with tempfile.TemporaryDirectory() as folder:
            task = {"total": 0, "took": 1, "file": os.path.join(folder, "absent.csv")}
            with mock.patch.object(helper.smtplib, "SMTP") as smtp, \
                    self.assertLogs(level="ERROR"):
                with self.assertRaises(FileNotFoundError):
                    helper.sendMail(task, self.message())

        self.assertFalse(smtp.return_value.send_message.called)


class PostgresConnectionTests(HelperTestCase):
    def connection(self):
        connection = mock.Mock()
        connection.get_dsn_parameters.return_value = {"user": "nyx", "password": "s3cr3t"}
        connection.cursor.return_value.fetchone.return_value = ("PostgreSQL 16",)
        return connection

    def test_an_existing_connection_is_reused(self):
        existing = mock.Mock()
        helper.pg_connection = existing

        self.assertIs(helper.get_postgres_connection(), existing)

    def test_the_connection_is_autocommit_and_its_password_stays_out_of_the_log(self):
        connection = self.connection()
        with mock.patch.object(helper.psycopg2, "connect", return_value=connection), \
                mock.patch.object(helper.threading, "Thread") as thread, \
                self.assertLogs(level="INFO") as logs:
            helper.get_postgres_connection()

        self.assertTrue(connection.autocommit)
        self.assertFalse(any("s3cr3t" in line for line in logs.output))
        self.assertTrue(thread.return_value.start.called, "the ping thread is started")
        self.assertIs(helper.pg_connection, connection)

    def test_a_connection_that_fails_leaves_no_connection(self):
        with mock.patch.object(helper.psycopg2, "connect", side_effect=RuntimeError("no route")), \
                self.assertLogs(level="ERROR"):
            self.assertIsNone(helper.get_postgres_connection())

        self.assertIsNone(helper.pg_connection)

    def test_discarding_closes_the_connection(self):
        connection = mock.Mock()
        helper.pg_connection = connection

        helper.discard_pg_connection()

        self.assertIsNone(helper.pg_connection)
        self.assertTrue(connection.close.called)

    def test_discarding_survives_a_connection_that_refuses_to_close(self):
        connection = mock.Mock()
        connection.close.side_effect = RuntimeError("already gone")
        helper.pg_connection = connection

        with self.assertLogs(level="WARNING"):
            helper.discard_pg_connection()

        self.assertIsNone(helper.pg_connection)

    def test_a_failed_statement_is_rolled_back(self):
        connection = mock.Mock()

        helper.rollback_pg_connection(connection)

        self.assertTrue(connection.rollback.called)

    def test_a_rollback_that_fails_discards_the_connection(self):
        connection = mock.Mock()
        connection.rollback.side_effect = RuntimeError("broken")
        helper.pg_connection = connection

        with self.assertLogs(level="WARNING"):
            helper.rollback_pg_connection(connection)

        self.assertIsNone(helper.pg_connection, "the next message reconnects instead of failing")

    def test_rolling_back_nothing_does_nothing(self):
        self.assertIsNone(helper.rollback_pg_connection(None))

    def test_the_ping_drops_a_connection_that_does_not_answer(self):
        connection = mock.Mock()
        connection.cursor.side_effect = RuntimeError("server closed")
        helper.pg_connection = connection
        sleeps = {"count": 0}

        def sleep_once(seconds):
            sleeps["count"] += 1
            if sleeps["count"] > 1:
                raise TimeoutError("stop")

        with mock.patch.object(helper.time, "sleep", side_effect=sleep_once), \
                self.assertLogs(level="ERROR"):
            with self.assertRaises(TimeoutError):
                helper.check_pg()

        self.assertIsNone(helper.pg_connection)

    def test_a_failing_sql_export_rolls_the_connection_back(self):
        connection = mock.Mock()
        helper.pg_connection = connection
        with mock.patch.object(helper, "loadPGData", side_effect=RuntimeError("bad SQL")), \
                self.assertLogs(level="ERROR"):
            with self.assertRaises(RuntimeError):
                helper.remoteLoadPGData({"appid": "app-1", "data": {}, "download": True,
                                         "user": ALICE, "outputformat": "csv"})

        self.assertTrue(connection.rollback.called)
        self.assertIs(helper.pg_connection, connection,
                      "a successful rollback keeps the connection for the next message")


class MoreDispatchTests(HelperTestCase):
    """Les chemins qui ecrivent: un export SQL servi, et les deux telechargements directs."""

    def test_a_sql_export_is_run_and_mailed(self):
        self.cluster()
        task = {"total": 2, "took": 3, "file": "/tmp/x.csv"}
        message = {"user": {"id": "alice@example.com"}, "appid": "app-1", "data": {},
                   "download": True, "outputformat": "csv"}
        with mock.patch.object(helper, "remoteLoadPGData", return_value=task) as load, \
                mock.patch.object(helper, "sendMail") as mail:
            helper.messageReceived("/queue/REST_LOAD_PGDATA", __import__("json").dumps(message), {})

        self.assertEqual(load.call_args.args[0]["user"]["id"], "alice@example.com")
        mail.assert_called_once_with(task, load.call_args.args[0])

    def test_the_direct_downloads_call_the_shared_loaders(self):
        helper.es = mock.Mock()
        helper.conn = mock.Mock()
        with mock.patch.object(helper, "loadData", return_value="data") as load:
            result = helper.remoteLoadData({"index": "idx", "data": {}, "doc_type": None,
                                            "download": True, "cui": (None, None, None),
                                            "user": ALICE, "outputformat": "csv"})
        self.assertEqual(result, "data")
        self.assertFalse(load.call_args.args[7], "the worker is not the API: is_rest_api is False")

        with mock.patch.object(helper, "kibanaData", return_value="kibana") as kibana:
            self.assertEqual(
                helper.remoteKibanaData({"matchrequest": {}, "user": ALICE, "outputformat": "xlsx"}),
                "kibana",
            )
        self.assertEqual(kibana.call_args.args[5], False)


class CleanupTests(HelperTestCase):
    def message(self):
        return {"user": {"id": "alice@example.com", "firstname": "Alice", "lastname": "Marchand"}}

    def test_a_ping_that_answers_keeps_the_connection(self):
        connection = mock.Mock()
        helper.pg_connection = connection
        sleeps = {"count": 0}

        def sleep_once(seconds):
            sleeps["count"] += 1
            if sleeps["count"] > 1:
                raise TimeoutError("stop")

        with mock.patch.object(helper.time, "sleep", side_effect=sleep_once):
            with self.assertRaises(TimeoutError):
                helper.check_pg()

        self.assertTrue(connection.cursor.return_value.execute.called)
        self.assertIs(helper.pg_connection, connection)

    def test_a_mail_whose_attachment_cannot_be_removed_is_still_sent(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "export.csv")
            open(path, "w").close()
            task = {"total": 1, "took": 1, "file": path}
            with mock.patch.object(helper.smtplib, "SMTP"), \
                    mock.patch.object(helper.os, "remove", side_effect=RuntimeError("busy")), \
                    self.assertLogs(level="ERROR") as logs:
                helper.sendMail(task, self.message())

            # la suppression est justement empechee: l'archive reste, on la retire pour les
            # autres tests qui comptent les fichiers du repertoire courant.
            for name in os.listdir("."):
                if name.endswith(".zip"):
                    Path(name).unlink()

        self.assertTrue(any("Failed to remove zip file" in line for line in logs.output))

    def test_a_broker_that_refuses_to_close_is_reported(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "export.csv")
            open(path, "w").close()
            task = {"total": 1, "took": 1, "file": path}
            server = mock.Mock()
            server.quit.side_effect = RuntimeError("gone")
            with mock.patch.object(helper.smtplib, "SMTP", return_value=server), \
                    self.assertLogs(level="ERROR") as logs:
                helper.sendMail(task, self.message())

        self.assertTrue(any("Failed to close SMTP connection" in line for line in logs.output))


class StartupTests(unittest.TestCase):
    """Le demarrage du conteneur: clients construits, journal configure, boucle de signe de vie."""

    def test_the_module_starts_with_tls_and_a_logstash_appender(self):
        import runpy
        import logging

        saved_cwd = os.getcwd()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.addCleanup(os.chdir, saved_cwd)
        os.chdir(temporary.name)
        os.makedirs("logs", exist_ok=True)

        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        self.addCleanup(root.handlers.__setitem__, slice(None), saved_handlers)

        from config import settings as source_settings

        client = mock.Mock()
        sleeps = {"count": 0}

        def sleep_once(seconds):
            sleeps["count"] += 1
            if sleeps["count"] > 1:
                raise TimeoutError("stop")

        with mock.patch.object(source_settings, "ELK_SSL", True), \
                mock.patch.object(source_settings, "USE_LOGSTASH", True), \
                mock.patch.object(source_settings, "ELK_URL", "elastic.example"), \
                mock.patch.object(source_settings, "ELK_PORT", "9200"), \
                mock.patch("amqstompclient.amqstompclient.AMQClient", return_value=client), \
                mock.patch("opensearchpy.OpenSearch") as cluster, \
                mock.patch("logstash_async.handler.AsynchronousLogstashHandler",
                           side_effect=lambda *a, **k: logging.NullHandler()), \
                mock.patch("time.sleep", side_effect=sleep_once):
            with self.assertRaises(TimeoutError):
                runpy.run_path(str(Path(__file__).resolve().parents[1] / "sources"
                                    / "nyx_rest_helper.py"), run_name="__main__")

        self.assertTrue(cluster.called)
        self.assertEqual(cluster.call_args.args[0], ["https://elastic.example:9200"],
                         "the TLS branch prefixes the host it was given")
        self.assertTrue(client.send_life_sign.called)

    def test_an_url_that_already_carries_its_scheme_is_used_as_it_is(self):
        import runpy
        import logging

        saved_cwd = os.getcwd()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.addCleanup(os.chdir, saved_cwd)
        os.chdir(temporary.name)
        os.makedirs("logs", exist_ok=True)

        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        self.addCleanup(root.handlers.__setitem__, slice(None), saved_handlers)

        from config import settings as source_settings

        client = mock.Mock()
        # the first life sign fails: the loop must log and keep going, not stop the worker
        client.send_life_sign.side_effect = [RuntimeError("broker hiccup")]
        sleeps = {"count": 0}

        def sleep_once(seconds):
            sleeps["count"] += 1
            if sleeps["count"] > 1:
                raise TimeoutError("stop")

        with mock.patch.object(source_settings, "ELK_SSL", True), \
                mock.patch.object(source_settings, "USE_LOGSTASH", False), \
                mock.patch.object(source_settings, "ELK_URL", "https://elastic.example:9200"), \
                mock.patch("amqstompclient.amqstompclient.AMQClient", return_value=client), \
                mock.patch("opensearchpy.OpenSearch") as cluster, \
                mock.patch("logstash_async.handler.AsynchronousLogstashHandler",
                           side_effect=lambda *a, **k: logging.NullHandler()), \
                mock.patch("time.sleep", side_effect=sleep_once):
            with self.assertLogs(level="ERROR") as logs:
                with self.assertRaises(TimeoutError):
                    runpy.run_path(str(Path(__file__).resolve().parents[1] / "sources"
                                        / "nyx_rest_helper.py"), run_name="__main__")

        self.assertEqual(cluster.call_args.args[0], ["https://elastic.example:9200"],
                         "a url with its scheme is not prefixed again")
        self.assertTrue(any("Unable to send life sign" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
