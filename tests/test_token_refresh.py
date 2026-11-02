"""Copie du jeton en mémoire : elle doit pouvoir être ignorée quand elle est périmée.

Chaque worker gunicorn garde le payload du jeton dans `state.tokens` (un `TTLCache` de cinq
minutes) ; seule une déconnexion le vide plus tôt. Le changement de mot de passe écrit le
nouveau payload dans Redis et dans le worker qui a servi la requête. Les autres workers
répondaient donc encore MUST_CHANGE_PASSWORD à un utilisateur qui venait de choisir son mot de
passe — mesuré en production : 3 appels sur 12, pendant jusqu'à cinq minutes.

`getUserFromToken(..., fresh=True)` est la relecture qui manquait.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import state
from middleware import getUserFromToken

TOKEN = "11111111-2222-3333-4444-555555555555"


class FakeRedis:
    def __init__(self, payload=None):
        self.payload = payload
        self.reads = 0

    def get(self, key):
        self.reads += 1
        if self.payload is None:
            return None
        return json.dumps(self.payload).encode()


def payload(must_reset):
    return {"login": "bob@example.com", "id": "bob@example.com",
            "privileges": ["YUL"], "mustResetPassword": must_reset}


class TokenCacheTests(unittest.TestCase):
    def setUp(self):
        self.addCleanup(setattr, state, "tokens", state.tokens)
        self.addCleanup(setattr, state, "redisserver", getattr(state, "redisserver", None))
        state.tokens = {}

    def test_a_cached_payload_is_served_without_asking_redis(self):
        state.tokens[TOKEN] = payload(True)
        redis = FakeRedis(payload(False))
        state.redisserver = redis
        self.assertTrue(getUserFromToken(None, TOKEN)["mustResetPassword"])
        self.assertEqual(redis.reads, 0, "le cache local doit eviter l'appel a Redis")

    def test_fresh_ignores_the_stale_copy_and_refreshes_it(self):
        # le worker avait memorise le jeton bride ; le mot de passe vient d'etre change ailleurs
        state.tokens[TOKEN] = payload(True)
        state.redisserver = FakeRedis(payload(False))
        refreshed = getUserFromToken(None, TOKEN, fresh=True)
        self.assertFalse(refreshed["mustResetPassword"])
        self.assertFalse(state.tokens[TOKEN]["mustResetPassword"],
                         "la copie locale doit etre remplacee par la version fraiche")

    def test_fresh_still_returns_none_when_the_token_is_revoked(self):
        state.tokens[TOKEN] = payload(True)
        state.redisserver = FakeRedis(None)
        self.assertIsNone(getUserFromToken(None, TOKEN, fresh=True))
        self.assertNotIn(TOKEN, state.tokens)

    def test_fresh_populates_the_cache_when_the_worker_had_nothing(self):
        state.redisserver = FakeRedis(payload(False))
        self.assertFalse(getUserFromToken(None, TOKEN, fresh=True)["mustResetPassword"])
        self.assertIn(TOKEN, state.tokens)

    def test_fresh_also_covers_a_redis_outage(self):
        # Redis tombe : on garde la session en cours plutôt que de renvoyer tout le monde au
        # login. La copie mémoire fait foi jusqu'au retour du cache.
        class Broken:
            def get(self, key):
                raise ConnectionError("redis is down")

        state.tokens[TOKEN] = payload(True)
        state.redisserver = Broken()
        self.assertTrue(getUserFromToken(None, TOKEN, fresh=True)["mustResetPassword"])


if __name__ == "__main__":
    unittest.main()
