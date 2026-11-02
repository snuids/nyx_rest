"""Brute-force guard for ``POST /api/v1/cred/login``.

A wrong password leaves no trace today: the call has no token yet, so nothing reaches
``nyx_apicalls``. Two Redis counters, one per account and one per IP, fill that hole:

* ``loginfail:user:<login>`` — an attacker can change IP without changing target;
* ``loginfail:ip:<ip>``      — an attacker can change account without changing source.

Both keys live for ``WINDOW`` seconds. Past ``MAX_ATTEMPTS``, the route answers 429 for as
long as the key lasts. An account that crosses the ``BAN_LADDER`` steps is frozen for longer,
but **never permanently**: a permanent block turns an attack into a denial of service against
its own user.

The module is deliberately kept apart from the route: the functions take a Redis client and
return integers, which makes them testable without Redis or Flask.
"""

import logging
import random
import time

logger = logging.getLogger(__name__)

WINDOW = 300                    # observation window, in seconds
MAX_ATTEMPTS = 5                # free attempts before the first freeze
BAN_LADDER = ((10, 300), (20, 3600))   # failure threshold -> extra freeze, in seconds
MIN_FAILURE_DELAY = 0.3         # response floor on failure, in seconds
JITTER = 0.15                   # added noise, to blur the timing


def user_key(login):
    return "loginfail:user:" + (login or "").lower()


def ip_key(ip):
    return "loginfail:ip:" + (ip or "unknown")


def ban_key(login):
    return "loginban:" + (login or "").lower()


def client_ip(headers, remote_addr):
    """Real client IP behind nginx.

    The production configuration sets ``X-Real-IP`` (not ``X-Forwarded-For``) on ``/api/``.
    Both are accepted, in that order, and we fall back on the socket.
    """
    real = (headers or {}).get("X-Real-IP")
    if real:
        return real.strip()
    forwarded = (headers or {}).get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return remote_addr or "unknown"


def lock_seconds(attempts):
    """Freeze applicable to a counter holding ``attempts`` failures. Pure function."""
    if attempts < MAX_ATTEMPTS:
        return 0
    longest = 0
    for threshold, seconds in BAN_LADDER:
        if attempts >= threshold:
            longest = seconds
    # between MAX_ATTEMPTS and the first step: wait for the window to expire
    return longest or WINDOW


def retry_after(conn, login, ip):
    """Seconds to wait before retrying, 0 if the call is allowed.

    Redis unavailable: we let it through. Protecting the login must not close the door on
    everyone when the cache goes down.
    """
    try:
        ban = conn.ttl(ban_key(login))
        if ban and int(ban) > 0:
            return int(ban)
        worst = 0
        for key in (user_key(login), ip_key(ip)):
            attempts = int(conn.get(key) or 0)
            if attempts >= MAX_ATTEMPTS:
                worst = max(worst, max(int(conn.ttl(key) or WINDOW), 1))
        return worst
    except Exception:
        logger.warning("login_guard: Redis unreachable, check skipped", exc_info=True)
        return 0


def register_failure(conn, login, ip):
    """Records a failure and returns the freeze now applicable."""
    try:
        for key in (user_key(login), ip_key(ip)):
            conn.incr(key)
            conn.expire(key, WINDOW)
        attempts = int(conn.get(user_key(login)) or 0)
        for threshold, seconds in BAN_LADDER:
            if attempts >= threshold:
                conn.setex(ban_key(login), seconds, 1)
        return retry_after(conn, login, ip)
    except Exception:
        logger.warning("login_guard: failure not recorded", exc_info=True)
        return 0


def reset(conn, login, ip):
    """Clears the counters after a successful authentication."""
    try:
        conn.delete(user_key(login), ip_key(ip), ban_key(login))
    except Exception:
        logger.warning("login_guard: counters not cleared", exc_info=True)


def delay():
    """Response floor to oppose to a failure.

    ``pbkdf2_sha256`` is expensive, but only if the account exists: without this floor the
    difference in response time is enough to enumerate accounts. The noise keeps the
    measurement of the floor itself from becoming a signal again.
    """
    time.sleep(MIN_FAILURE_DELAY + random.random() * JITTER)


def failure_message():
    """Single message for "unknown account" and "wrong password".

    Two distinct messages make enumeration free, whatever care is taken over the response
    time.
    """
    return "Bad Credentials"
