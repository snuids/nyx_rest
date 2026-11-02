"""Agrégation des compteurs entre les workers gunicorn.

Chaque worker tient ses compteurs en mémoire : ``state.requests_total`` vaut trois choses
différentes selon celui qui répond au scrape. Prometheus réutilise sa connexion HTTP, donc il
interroge toujours le même worker et ne voit qu'un tiers du trafic.

Chaque worker publie donc ses compteurs dans un hachage Redis (``HSET nyx_counters <pid>``), et
``/metrics`` additionne ce que tous les workers ont publié récemment. Un worker mort disparaît de
la somme au bout de ``STALE_AFTER`` secondes : sans cela, un redéploiement laisserait ses derniers
chiffres figés dans le total pour toujours.

L'agrégation est une fonction pure, testable sans Redis.
"""

import json
import time

HASH_KEY = "nyx_counters"
STALE_AFTER = 30        # un worker muet depuis plus longtemps est considéré mort
PUBLISH_TTL = 120       # le hachage entier expire si plus personne ne publie

COUNTERS = (
    "requests_total",
    "errors_total",
    "auth_failures_total",
    "auth_blocked_total",
    "active_tokens",
)


def snapshot(state):
    """Valeurs à publier pour ce worker."""
    return {name: int(getattr(state, name, 0) or 0) for name in COUNTERS}


def publish(conn, pid, counters, now=None):
    """Écrit l'état de ce worker dans Redis. Silencieux en cas d'échec : la métrique n'est pas
    une fonction vitale de l'API."""
    try:
        payload = {"at": int(now if now is not None else time.time())}
        payload.update({name: int(value) for name, value in counters.items()})
        conn.hset(HASH_KEY, str(pid), json.dumps(payload))
        conn.expire(HASH_KEY, PUBLISH_TTL)
    except Exception:
        pass


def aggregate(entries, now=None, stale_after=STALE_AFTER):
    """Somme les compteurs des workers vivants.

    ``entries`` : liste de couples ``(champ, valeur_brute)`` telle que la renvoie ``HGETALL``,
    ou de dictionnaires déjà décodés. Les entrées trop anciennes ou illisibles sont ignorées.
    """
    now = int(now if now is not None else time.time())
    totals = {name: 0 for name in COUNTERS}
    workers = 0
    for entry in entries or []:
        payload = entry
        if isinstance(entry, (list, tuple)) and len(entry) == 2:
            try:
                payload = json.loads(entry[1])
            except (TypeError, ValueError):
                continue
        if not isinstance(payload, dict):
            continue
        published_at = int(payload.get("at") or 0)
        if now - published_at > stale_after:
            continue          # worker disparu : ses chiffres ne doivent pas se figer dans le total
        workers += 1
        for name in COUNTERS:
            try:
                totals[name] += int(payload.get(name) or 0)
            except (TypeError, ValueError):
                pass
    totals["workers"] = workers
    return totals


def read_aggregate(conn, now=None, stale_after=STALE_AFTER):
    """Lit le hachage Redis puis agrège. ``None`` si Redis est injoignable."""
    try:
        raw = conn.hgetall(HASH_KEY)
    except Exception:
        return None
    if not raw:
        return None
    entries = []
    for key, value in raw.items():
        entries.append((key, value))
    totals = aggregate(entries, now=now, stale_after=stale_after)
    if totals["workers"] == 0:
        return None
    return totals
