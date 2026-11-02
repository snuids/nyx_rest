"""
Logging handler for the REST API.

gunicorn runs the API with three workers (``-w 3``) and every one of them configures the same
``TimedRotatingFileHandler`` on ``logs/nyx_rest_api.log``. Each worker decides on its own that the
day has changed, so at midnight they roll over at the same moment:

* the worker that arrives first renames the file and opens a fresh one — fine;
* the workers that arrive second find the file gone. The standard handler then does the worst
  thing possible: ``doRollover`` removes the archive that the first worker just created
  (``if os.path.exists(dfn): os.remove(dfn)``) and renames the *empty new* file over it, so the
  day's log is destroyed. When the timing is slightly different the rename raises
  ``FileNotFoundError`` and the logging module prints, on every record until the next midnight::

      --- Logging error ---
      Traceback (most recent call last):
      FileNotFoundError: [Errno 2] No such file or directory:
          '/opt/sources/logs/nyx_rest_api.log' -> '/opt/sources/logs/nyx_rest_api.log.2026-10-25'

  The losing worker also keeps its handle open on the renamed file, so its records keep landing in
  the archive, and ``rolloverAt`` is never advanced because the exception interrupts
  ``doRollover`` before reaching it.

The class below serialises the rollover with an exclusive ``flock`` on a lock file next to the log,
then checks whether another worker already did the work. Only one process ever renames; the others
just reopen the live file and move their own deadline forward. It stays inside the standard library
on purpose: the API image installs a pinned requirements file, and pulling in
``concurrent-log-handler`` for this would be a dependency for a problem that a lock solves.
"""

import fcntl
import os
import time
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


class MultiProcessTimedRotatingFileHandler(TimedRotatingFileHandler):
    """``TimedRotatingFileHandler`` that survives several processes sharing one log file."""

    def doRollover(self):
        with self._rollover_lock():
            if self._rotated_by_another_process():
                self._adopt_rotation()
                return
            try:
                super().doRollover()
            except FileNotFoundError:
                # The rename lost a race the lock did not cover (another host, a removed file).
                self._adopt_rotation()

    def _rollover_lock(self):
        handle = open(self.baseFilename + ".lock", "a")
        fcntl.flock(handle, fcntl.LOCK_EX)
        return _Release(handle)

    def _rotated_by_another_process(self):
        """True when somebody already renamed the file for this period."""
        try:
            mtime = os.stat(self.baseFilename).st_mtime
        except FileNotFoundError:
            return True
        return int(mtime) >= self.rolloverAt

    def _adopt_rotation(self):
        """Reopen the live file and move our own deadline to the next period."""
        if self.stream is not None:
            try:
                self.stream.close()
            except OSError:
                pass
            self.stream = None
        self.rolloverAt = self.computeRollover(int(time.time()))


class MultiProcessRotatingFileHandler(RotatingFileHandler):
    """``RotatingFileHandler`` par taille, sûr entre plusieurs processus.

    ``logs/RestAPIG.log`` et ``logs/gunicorn.log`` sont écrits par les trois workers à la fois,
    sans aucune rotation : 359 Mo et 73 Mo en une journée, jusqu'au jour où le disque se remplit.
    La rotation par taille se heurte au même problème que la rotation quotidienne — deux workers
    renomment le même fichier — donc elle réutilise le verrou.
    """

    def doRollover(self):
        with self._rollover_lock():
            try:
                size = os.stat(self.baseFilename).st_size
            except FileNotFoundError:
                self._reopen()          # un autre worker a déjà renommé le fichier
                return
            if size < self.maxBytes:
                self._reopen()          # rotation déjà faite pendant qu'on attendait le verrou
                return
            try:
                super().doRollover()
            except FileNotFoundError:
                self._reopen()

    def _reopen(self):
        if self.stream is not None:
            try:
                self.stream.close()
            except OSError:
                pass
            self.stream = None
        self.stream = self._open()

    def _rollover_lock(self):
        handle = open(self.baseFilename + ".lock", "a")
        fcntl.flock(handle, fcntl.LOCK_EX)
        return _Release(handle)


class _Release:
    """Context manager closing the lock file, which releases the ``flock``."""

    def __init__(self, handle):
        self.handle = handle

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        fcntl.flock(self.handle, fcntl.LOCK_UN)
        self.handle.close()
        return False
