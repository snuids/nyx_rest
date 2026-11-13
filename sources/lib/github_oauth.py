"""GitHub as an identity provider — the exchange, and nothing else.

The console sends back the `code` GitHub puts on the redirect, and the route that receives it is
public by nature. What must not be guessed lives here: the URL the browser is sent to, how the code
becomes a token, and which of the returned e-mails identifies the account.

Two rules the module exists to keep:

* **The GitHub token never leaves it.** The platform mints its own session token (`finalize_login`);
  storing a provider token in Redis would add a second secret, with a one-hour life and no way to
  revoke it on our side. Only the identity crosses.
* **A refusal is an answer, not an exception.** `GithubError` says GitHub said no (bad code, revoked
  app, rate limit); the route turns that into a login failure and an audit line, never a 500.

Every function that talks HTTP takes the session as an argument, so the tests describe GitHub's
answers (JSON bodies, error payloads) without a network and without mocking `requests`.
"""

import logging
from urllib.parse import quote

logger = logging.getLogger(__name__)

GITHUB_AUTHORIZE = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN = "https://github.com/login/oauth/access_token"
GITHUB_USER = "https://api.github.com/user"
GITHUB_EMAILS = "https://api.github.com/user/emails"

SCOPE = "read:user user:email"
TIMEOUT = 10


class GithubError(Exception):
    """GitHub refused the exchange; the caller answers with a failed sign-in."""


def authorize_url(client_id, redirect_uri, state):
    """The URL the browser is redirected to, with a `state` it must give back."""
    if not client_id or not redirect_uri or not state:
        raise ValueError("client id, redirect uri and state are required")
    return (
        GITHUB_AUTHORIZE
        + "?client_id=" + quote(str(client_id), safe="")
        + "&redirect_uri=" + quote(str(redirect_uri), safe="")
        + "&scope=" + quote(SCOPE, safe="")
        + "&state=" + quote(str(state), safe="")
    )


def exchange_code(session, client_id, client_secret, code, redirect_uri=None):
    """The access token behind `code`, or `GithubError`.

    `Accept: application/json` is what makes GitHub answer a JSON body instead of a form-encoded
    one — the form is what the stub this replaces parsed with `split("=")`, which loses everything
    the moment a value contains an `&`. The token is asked for here, server-side: the browser never
    sees the client secret.
    """
    payload = {"client_id": client_id, "client_secret": client_secret, "code": code}
    if redirect_uri:
        payload["redirect_uri"] = redirect_uri
    try:
        response = session.post(GITHUB_TOKEN, data=payload,
                                headers={"Accept": "application/json"}, timeout=TIMEOUT)
        body = response.json()
    except Exception as error:
        logger.error("GitHub token exchange failed", exc_info=True)
        raise GithubError(str(error))

    token = (body or {}).get("access_token")
    if not token:
        logger.warning("GitHub refused the code: %s", (body or {}).get("error"))
        raise GithubError((body or {}).get("error") or "no access token")
    return token


def fetch_profile(session, access_token):
    """What GitHub says about the identity: login, name, and the address it can be matched on.

    `name` is the free-form field of the profile: null when the person left it empty, which is
    common on machine accounts. It is split on the last space, because GitHub does not distinguish
    a first name from a last one — "Arnaud Marchand" gives `arnaud`/`marchand`, "Cher" gives
    `cher`/"".

    `email` is the **verified** address, the only one an account may be matched on or filed under.
    `public_email` is the address the person chose to publish, which may be unverified: it is
    reported to the caller so a refusal can say what to look for, never used as a key.

    There is **no phone number**: GitHub has no such field, on the profile or in the API. A caller
    that needs one has to ask the person.
    """
    headers = {"Authorization": "Bearer " + str(access_token),
               "Accept": "application/vnd.github+json"}
    try:
        user = session.get(GITHUB_USER, headers=headers, timeout=TIMEOUT)
        if user.status_code != 200:
            raise GithubError("unable to read the GitHub profile")
        payload = user.json() or {}
        login = payload.get("login") or ""
    except GithubError:
        raise
    except Exception as error:
        logger.error("GitHub profile unreachable", exc_info=True)
        raise GithubError(str(error))

    if not login:
        raise GithubError("the GitHub profile carries no login")

    firstname, lastname = split_name(payload.get("name"))
    profile = {
        "login": login,
        "name": (payload.get("name") or "").strip(),
        "firstname": firstname,
        "lastname": lastname,
        "email": "",
        "public_email": (payload.get("email") or "").strip(),
    }

    try:
        emails = session.get(GITHUB_EMAILS, headers=headers, timeout=TIMEOUT)
        if emails.status_code == 200:
            profile["email"] = pick_primary_email(emails.json())
    except Exception:
        # The scope `user:email` is enough for this call, but a token issued by an older app
        # configuration may not carry it: the login is still usable, the account match is not.
        logger.warning("GitHub e-mails unreadable, matching on the login only", exc_info=True)
    return profile


def fetch_identity(session, access_token):
    """`(login, email)` for the token. The e-mail may be empty; the login may not.

    Kept as the narrow contract for callers that only need to recognise the account; `fetch_profile`
    carries the rest (name, published address) for the paths that file a new one.
    """
    profile = fetch_profile(session, access_token)
    return profile["login"], profile["email"]


def split_name(name):
    """`("Arnaud", "Marchand")` for "Arnaud Marchand", `("Cher", "")` for one word, `("", "")` for none."""
    words = [word for word in str(name or "").split() if word]
    if not words:
        return "", ""
    if len(words) == 1:
        return words[0], ""
    return words[0], " ".join(words[1:])


def pick_primary_email(entries):
    """The address an account can be recognised by: primary **and** verified, else a verified one.

    An unverified address is an address somebody typed into GitHub: matching an account on it
    would let anyone claim a NYX account by adding its e-mail to their GitHub profile.
    """
    if not isinstance(entries, list):
        return ""
    verified = [entry for entry in entries
                if isinstance(entry, dict) and entry.get("verified") and entry.get("email")]
    for entry in verified:
        if entry.get("primary"):
            return entry["email"]
    return verified[0]["email"] if verified else ""


def candidates(login, email):
    """The identifiers to look for in `nyx_user`, most specific first.

    Eight production accounts are indexed under their e-mail while their `login` field holds a short
    name (`amarchand@icloud.com` -> `amarchand`); the caller tries each candidate as a document id
    and as a `login.keyword`, which is the same pair of lookups the password route performs.
    """
    out = []
    for value in (email, login):
        value = (value or "").strip()
        if value and value not in out:
            out.append(value)
    return out
