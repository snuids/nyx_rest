"""The settings the queue worker reads at import time.

`nyx_rest_helper.py` builds its Elasticsearch client from `settings` when the module loads, so a
field that disappeared from `config.py` is not a broken feature — it is a process that cannot
start, and nobody notices until someone rebuilds its image. That is exactly what happened with
`ELK_PORT`: declared in the compose files, dropped from the pydantic model, read by the worker.

The module itself is not imported here: its import opens the broker and the cluster, which no unit
test should do. The fields it reads are what these tests pin.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

try:
    from config import Settings
    CONFIG_IMPORTABLE = True
    IMPORT_ERROR = ""
except Exception as exc:  # missing runtime dependency, as on a bare interpreter
    Settings = None
    CONFIG_IMPORTABLE = False
    IMPORT_ERROR = type(exc).__name__ + ": " + str(exc)[:80]


@unittest.skipUnless(CONFIG_IMPORTABLE, "runtime dependencies missing (" + IMPORT_ERROR + ")")
class SettingsFieldsTest(unittest.TestCase):
    def test_elk_port_is_declared(self):
        self.assertIn("ELK_PORT", Settings.model_fields)
        self.assertIn("ELK_URL", Settings.model_fields)

    def test_elk_port_has_a_default(self):
        # The transport defaults to 9200 for an http URL; the worker builds its own URL.
        self.assertEqual(Settings.model_fields["ELK_PORT"].default, "9200")


if __name__ == "__main__":
    unittest.main()
