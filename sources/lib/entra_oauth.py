"""Microsoft Entra ID as an identity provider — the exchange, and nothing else.

The console sends back the `code` Entra puts on the redirect, and the route that receives it is
public by nature. What must not be guessed lives here: the URL the browser is sent to, how the code
becomes an identity, and which of the claims identifies the account.

Three rules the module exists to keep:

* **The tokens never leave it.** Entra answers an `access_token` and an `id_token`; neither becomes
  the session token (`finalize_login` mints its own) and neither is stored — a provider token in
  Redis would be a second secret, with a one-hour life and no way to revoke it on our side. Only the
  identity crosses, as a profile dictionary shaped like the one `github_oauth` returns.
* **An identity from another directory is refused.** The directory is the security boundary: the ID
  token must come from the configured tenant (`tid`), be issued for this application (`aud`), be
  inside its validity window and answer the `nonce` this browser was given. Without the `tid` check,
  any Entra tenant could present somebody holding the address of one of our accounts.
* **A refusal is an answer, not an exception.** `EntraError` says Entra said no, or said something
  that cannot be trusted; the route turns that into a login failure and an audit line, never a 500.

`ENTRA_TENANT_ID` is the **directory id** (the GUID the portal shows as "Directory (tenant) ID"),
not the domain name: the id is what the claims and the token endpoint are checked against, and a
name would leave the comparison with nothing to hold. A configuration that carries a name is read as
"not configured", so the button stays hidden instead of half-working.

Every function that talks HTTP takes the session as an argument, so the tests describe Entra's
answers (JSON bodies, error payloads, ID tokens) without a network and without mocking `requests`.
"""

import base64
import json
import logging
import re
import time
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

AUTHORIZE = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize"
TOKEN = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
ISSUER = "https://login.microsoftonline.com/{tenant}/v2.0"

# Nothing beyond the identity is read: no Graph call is made, so no `User.Read` is asked for and the
# consent screen shows a sign-in and nothing else.
SCOPE = "openid profile email"

TIMEOUT = 10
CLOCK_SKEW = 300        # seconds of tolerance on `exp` / `nbf`, for a clock that drifts a little

TENANT_ID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class EntraError(Exception):
    """Entra refused the exchange, or answered an identity that cannot be trusted."""


def is_tenant_id(value):
    """True for a directory id — the form every claim is compared against."""
    return bool(TENANT_ID.match(str(value or "").strip()))


def configured(tenant, client_id, client_secret, redirect_uri):
    """True when the four settings the sign-in needs are present and usable.

    The tenant has to be a directory id: a domain name would sign the person in and then fail the
    `tid` comparison, which looks like a wrong password to whoever is at the keyboard.
    """
    return bool(client_id and client_secret and redirect_uri and is_tenant_id(tenant))


def _endpoint(template, tenant):
    """The tenant's URL for `template`, or `ValueError` when the tenant cannot be used."""
    tenant = str(tenant or "").strip()
    if not is_tenant_id(tenant):
        raise ValueError("the tenant must be a directory id (a GUID), not a domain name")
    return template.format(tenant=tenant.lower())


def authorize_url(tenant, client_id, redirect_uri, state, nonce):
    """The URL the browser is redirected to, with a `state` and a `nonce` it must give back."""
    for name, value in (("client id", client_id), ("redirect uri", redirect_uri),
                        ("state", state), ("nonce", nonce)):
        if not value:
            raise ValueError(name + " is required")
    query = urlencode([
        ("client_id", client_id),
        ("response_type", "code"),
        ("redirect_uri", redirect_uri),
        ("response_mode", "query"),
        ("scope", SCOPE),
        ("state", state),
        ("nonce", nonce),
        # Entra otherwise reuses the browser's Microsoft session silently: the person is signed in
        # as whoever last opened Outlook on that machine, which is exactly the wrong account when
        # two people share a laptop. `select_account` asks.
        ("prompt", "select_account"),
    ])
    return _endpoint(AUTHORIZE, tenant) + "?" + query


def exchange_code(session, tenant, client_id, client_secret, code,
                  redirect_uri=None, nonce=None):
    """The identity behind `code`, or `EntraError`. The tokens stay inside this call.

    The secret is used here and the browser never sees it: the exchange is server-side, which is
    also why the redirect URI must be registered as a **Web** platform on the application — a "Single
    page application" registration has no secret and would refuse this call.
    """
    if not code:
        raise EntraError("no authorization code")

    endpoint = _endpoint(TOKEN, tenant)
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "scope": SCOPE,
    }
    if redirect_uri:
        payload["redirect_uri"] = redirect_uri

    try:
        response = session.post(endpoint, data=payload, timeout=TIMEOUT)
        body = response.json()
    except Exception as error:
        logger.error("Entra token exchange failed", exc_info=True)
        raise EntraError(str(error))

    if not isinstance(body, dict):
        raise EntraError("unexpected token answer")
    if body.get("error"):
        # A used code, a wrong secret, a redirect URI that does not match the registration: they
        # all arrive here, and `error_description` is what tells them apart in the log.
        logger.warning("Entra refused the code: %s (%s)",
                       body.get("error"), str(body.get("error_description") or "")[:300])
        raise EntraError(str(body.get("error")))

    id_token = body.get("id_token")
    if not id_token:
        raise EntraError("no identity token")
    claims = read_claims(id_token, client_id=client_id, tenant=tenant, nonce=nonce)
    return profile_from_claims(claims)


def read_claims(id_token, client_id=None, tenant=None, nonce=None):
    """The claims of an identity token, checked — never trusted as they arrive.

    The signature is not verified, and does not need to be: the token was fetched by this
    confidential client on a direct TLS call to the tenant's token endpoint, which is the server
    validation OpenID Connect Core accepts in place of a signature check (§3.1.3.7, item 6). Note
    that this holds **only** for a token received this way — an `id_token` arriving from a browser
    would need its signature checked, which is why nothing here is called on a request body.

    What is checked is what a token from elsewhere, or replayed, would get wrong: the audience, the
    directory, the issuer, the validity window, and the `nonce` this browser was given.
    """
    segments = str(id_token or "").split(".")
    if len(segments) < 2:
        raise EntraError("the identity token is not a JWT")
    try:
        claims = json.loads(_decode(segments[1]))
    except Exception:
        raise EntraError("the identity token cannot be read")
    if not isinstance(claims, dict):
        raise EntraError("the identity token carries no claims")

    if client_id and str(claims.get("aud") or "") != str(client_id):
        raise EntraError("the identity token was issued for another application")

    directory = str(claims.get("tid") or "")
    if tenant:
        if directory.lower() != str(tenant).strip().lower():
            raise EntraError("the identity token comes from another directory")
        if str(claims.get("iss") or "").lower() != ISSUER.format(tenant=directory.lower()):
            # The issuer repeats the directory, so a token whose issuer disagrees with its own
            # `tid` cannot be read as coming from either.
            raise EntraError("the identity token has no consistent issuer")

    now = time.time()
    expiry = _timestamp(claims, "exp")
    if expiry is not None and expiry + CLOCK_SKEW < now:
        raise EntraError("the identity token has expired")
    not_before = _timestamp(claims, "nbf")
    if not_before is not None and not_before - CLOCK_SKEW > now:
        raise EntraError("the identity token is not valid yet")

    if nonce and str(claims.get("nonce") or "") != str(nonce):
        raise EntraError("the identity token answers another sign-in")
    return claims


def profile_from_claims(claims):
    """What Entra says about the identity, in the shape `github_oauth.fetch_profile` answers.

    The address is the key an account is matched on, so it must be one the directory vouches for:
    the `email` claim when the directory sets one, the `preferred_username` (the sign-in name, an
    address inside an organisational directory) when it does not. There is no third source — an
    identity that carries neither is refused rather than matched on a guess, because a short name
    derived from the address would let an identity land on an account whose address it does not
    hold.

    There is no phone number either: Entra has a `mobilePhone` attribute, but it is empty on most
    accounts and never part of the token. An account filed from an identity is completed by hand.
    """
    upn = str(claims.get("preferred_username") or claims.get("upn") or "").strip()
    email = str(claims.get("email") or "").strip()
    if "@" not in email:
        email = upn if "@" in upn else ""
    if "@" not in email:
        raise EntraError("the identity carries no address")

    firstname = str(claims.get("given_name") or claims.get("givenName") or "").strip()
    lastname = str(claims.get("family_name") or claims.get("familyName")
                   or claims.get("surname") or "").strip()
    name = str(claims.get("name") or "").strip()
    if not firstname and not lastname and name:
        firstname, lastname = split_name(name)

    return {
        "login": local_part(upn or email),
        "email": email,
        "upn": upn,
        "name": name or " ".join(part for part in (firstname, lastname) if part),
        "firstname": firstname,
        "lastname": lastname,
        "oid": str(claims.get("oid") or ""),
        "tid": str(claims.get("tid") or ""),
        "source": "entra",
    }


def candidates(profile):
    """The addresses to look for in `nyx_user`, most specific first.

    Both are full addresses the directory vouches for (`email`, then the sign-in name). Nothing
    derived is tried: a local part, a display name or an object id would match an account on
    something its holder never proved.
    """
    out = []
    for value in (profile.get("email"), profile.get("upn")):
        value = (value or "").strip()
        if value and value.lower() not in [seen.lower() for seen in out]:
            out.append(value)
    return out


def local_part(address):
    """`arnaud.marchand` for `arnaud.marchand@contoso.com` — the short name NYX accounts carry."""
    return str(address or "").split("@")[0]


def split_name(name):
    """`("Arnaud", "Marchand")` for one full name, `("Cher", "")` for a single word."""
    words = [word for word in str(name or "").split() if word]
    if not words:
        return "", ""
    if len(words) == 1:
        return words[0], ""
    return words[0], " ".join(words[1:])


def _decode(segment):
    """One base64url segment of a JWT, as text — with the padding the encoding leaves out."""
    padding = "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(str(segment) + padding).decode("utf-8")


def _timestamp(claims, name):
    """`claims[name]` as a number, or `None` when it is absent or not one."""
    try:
        return float(claims.get(name))
    except (TypeError, ValueError):
        return None
