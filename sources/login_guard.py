"""Garde contre le force brute sur ``POST /api/v1/cred/login``.

Un mot de passe faux ne laisse aucune trace aujourd'hui : l'appel n'a pas encore de token,
donc rien n'arrive dans ``nyx_apicalls``. Deux compteurs Redis, un par compte et un par IP,
pallient ce trou :

* ``loginfail:user:<login>`` — un attaquant peut changer d'IP sans changer de cible ;
* ``loginfail:ip:<ip>``      — un attaquant peut changer de compte sans changer de source.

Les deux clés vivent ``WINDOW`` secondes. Au-delà de ``MAX_ATTEMPTS``, la route répond 429 le
temps que la clé expire. Un compte qui franchit les paliers de ``BAN_LADDER`` est gelé plus
longtemps, mais **jamais définitivement** : un blocage permanent transforme une attaque en
déni de service contre son propre utilisateur.

Le module est volontairement séparé de la route : les fonctions prennent un client Redis et
renvoient des entiers, ce qui les rend testables sans Redis ni Flask.
"""

import logging
import random
import time

logger = logging.getLogger(__name__)

WINDOW = 300                    # fenêtre d'observation, en secondes
MAX_ATTEMPTS = 5                # tentatives libres avant le premier gel
BAN_LADDER = ((10, 300), (20, 3600))   # seuil d'échecs -> gel supplémentaire, en secondes
MIN_FAILURE_DELAY = 0.3         # plancher de réponse en cas d'échec, en secondes
JITTER = 0.15                   # bruit ajouté, pour brouiller le chronométrage


def user_key(login):
    return "loginfail:user:" + (login or "").lower()


def ip_key(ip):
    return "loginfail:ip:" + (ip or "unknown")


def ban_key(login):
    return "loginban:" + (login or "").lower()


def client_ip(headers, remote_addr):
    """IP réelle du client derrière nginx.

    La configuration de prod pose ``X-Real-IP`` (pas ``X-Forwarded-For``) sur ``/api/``.
    On accepte les deux, dans cet ordre, et on retombe sur la socket.
    """
    real = (headers or {}).get("X-Real-IP")
    if real:
        return real.strip()
    forwarded = (headers or {}).get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return remote_addr or "unknown"


def lock_seconds(attempts):
    """Gel applicable à un compteur ayant ``attempts`` échecs. Fonction pure."""
    if attempts < MAX_ATTEMPTS:
        return 0
    longest = 0
    for threshold, seconds in BAN_LADDER:
        if attempts >= threshold:
            longest = seconds
    # entre MAX_ATTEMPTS et le premier palier : on attend l'expiration de la fenêtre
    return longest or WINDOW


def retry_after(conn, login, ip):
    """Secondes à attendre avant de retenter, 0 si l'appel est autorisé.

    Redis indisponible : on laisse passer. Protéger le login ne doit pas fermer la porte
    à tout le monde quand le cache tombe.
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
        logger.warning("login_guard : Redis injoignable, contrôle ignoré", exc_info=True)
        return 0


def register_failure(conn, login, ip):
    """Enregistre un échec et renvoie le gel désormais applicable."""
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
        logger.warning("login_guard : échec non enregistré", exc_info=True)
        return 0


def reset(conn, login, ip):
    """Efface les compteurs après une authentification réussie."""
    try:
        conn.delete(user_key(login), ip_key(ip), ban_key(login))
    except Exception:
        logger.warning("login_guard : compteurs non effacés", exc_info=True)


def delay():
    """Délai plancher à opposer à un échec.

    ``pbkdf2_sha256`` coûte cher, mais seulement si le compte existe : sans ce plancher
    la différence de temps de réponse suffit à énumérer les comptes. Le bruit évite que
    la mesure du plancher lui-même ne redevienne un signal.
    """
    time.sleep(MIN_FAILURE_DELAY + random.random() * JITTER)


def failure_message():
    """Message unique pour « compte inconnu » et « mot de passe faux ».

    Deux messages distincts rendent l'énumération gratuite, quel que soit le soin
    apporté au temps de réponse.
    """
    return "Bad Credentials"
