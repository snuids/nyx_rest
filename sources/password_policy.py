"""Password policy for NYX accounts.

Until now, the interface created every account with the **same** pbkdf2 hash hard-coded in
`User.vue`. Every new account therefore shared the same secret until someone thought of
resetting it — a shared password that nobody knows is a door left open.

Two rules replace that practice:

* an account created without a password receives a **random password that nobody knows**,
  and the ``mustResetPassword`` flag;
* while the flag is set, the token obtained at login only opens the password change route
  and the logout (see ``middleware.token_required``).

The rest of the API is therefore unreachable with a provisional password: the change is not a
courtesy of the interface, it is a condition of access.
"""

import secrets

# 18 bytes give 24 url-safe characters: 144 bits of entropy, out of reach of an exhaustive
# search, and with no ambiguous character to copy from a screen or a message.
TEMPORARY_BYTES = 18

# Routes reachable with a provisional password. Everything else is refused.
ALLOWED_BEFORE_CHANGE = (
    "/api/v1/cred/changepassword",
    "/api/v1/cred/logout",
    "/api/v1/healthz",
    "/api/v1/metrics",
)

MIN_LENGTH = 12


def generate_temporary_password():
    """Random password, never displayed nor transmitted: the account is unusable as it is."""
    return secrets.token_urlsafe(TEMPORARY_BYTES)


def is_allowed_before_change(path):
    """True if the route stays open as long as the password has not been chosen."""
    return (path or "").rstrip("/") in ALLOWED_BEFORE_CHANGE


def must_change_password(usr, path):
    """True if the call must be refused because the password has not been chosen yet."""
    if not usr or not usr.get("mustResetPassword"):
        return False
    return not is_allowed_before_change(path)


def check_new_password(password, login=""):
    """Minimal checks on a password chosen by the user.

    Length only: composition rules (upper case, digit, symbol) are now discouraged by ANSSI
    and NIST, they produce predictable passwords written down on a piece of paper. Length is
    what matters.
    """
    if not password or len(password) < MIN_LENGTH:
        return f"The password must contain at least {MIN_LENGTH} characters."
    if login and password.lower() == str(login).lower():
        return "The password cannot be the same as the login."
    return ""
