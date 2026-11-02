"""Lecture de `nyx_user` : un compte ordinaire ne voit que sa propre fiche.

L'index porte les condensats pbkdf2 de tous les comptes, et `can_use_indice` ne reçoit qu'un
nom d'index — il ne peut donc pas savoir *quel* document est demandé. Deux contrôles sont
nécessaires et doivent refuser la même chose, sinon la restriction se contourne par l'autre
route :

* `can_use_indice` ajoute une clause `bool.filter` (« son propre compte ») à toute recherche
  ou agrégation sur l'index, donc à `generic_search`, `queryFilter` et aux exports ;
* `can_read_nyx_user` garde la lecture directe `GET /generic/nyx_user/<id>`.

Un compte `admin` n'est pas restreint : l'écran d'administration des utilisateurs en a besoin.
"""

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

import state
from es_helpers import (
    can_read_nyx_user,
    can_use_indice,
    is_admin,
    is_nyx_user_indice,
    nyx_user_read_filter,
    own_account_ids,
    requires_admin_for_write,
)


def rule(pattern, **fields):
    return {"_source": {"indicepattern": pattern, **fields}}


def user(login="bob@example.com", privileges=(), filters=(), **extra):
    return {"login": login, "privileges": list(privileges),
            "filters": list(filters), **extra}


def filters_of(query):
    return (query.get("bool") or {}).get("filter") or []


class NyxUserIndexDetectionTests(unittest.TestCase):
    def test_the_index_and_its_reindex_alias_are_recognised(self):
        # la production sert un alias : nommer `reindexed-nyx_user-v8` dans generic_search
        # doit tomber sous la même règle que `nyx_user`
        self.assertTrue(is_nyx_user_indice("nyx_user"))
        self.assertTrue(is_nyx_user_indice("reindexed-nyx_user-v8"))

    def test_a_neighbouring_index_is_not_recognised(self):
        for name in ("nyx_user_history", "nyx_users", "nyx_userfoo", "not_nyx_user", ""):
            self.assertFalse(is_nyx_user_indice(name), name)

    def test_a_wildcard_that_designates_nyx_user_is_recognised(self):
        # l'interface liste les comptes par `generic_search/nyx_user*` : sans cette forme,
        # la restriction se contournait par un astérisque
        for name in ("nyx_user*", "nyx_us*", "nyx_*", "*", "?yx_user", "reindexed-nyx_user*"):
            self.assertTrue(is_nyx_user_indice(name), name)

    def test_a_wildcard_on_another_family_is_left_alone(self):
        # quatre écrans de production lisent une famille de cette façon
        for name in ("nyx_apicalls*", "nyx_energy*", "nyx_alert*", "airport_udropstatus*",
                     "deliveries-*"):
            self.assertFalse(is_nyx_user_indice(name), name)

    def test_a_missing_index_name_is_not_a_crash(self):
        self.assertFalse(is_nyx_user_indice(None))


class OwnAccountIdentityTests(unittest.TestCase):
    def test_the_login_and_the_document_id_are_both_kept(self):
        # `_id` porte l'e-mail et `login` le nom court sur huit comptes : l'un des deux
        # suffit à lire sa fiche, donc les deux sont conservés
        self.assertEqual(own_account_ids({"id": "bob@example.com", "login": "bob"}),
                         ["bob@example.com", "bob"])

    def test_a_token_without_identity_yields_no_ids(self):
        self.assertEqual(own_account_ids({}), [])
        self.assertEqual(own_account_ids(None), [])
        self.assertEqual(own_account_ids({"login": "", "id": None}), [])

    def test_the_filter_matches_the_id_or_the_login(self):
        clause = nyx_user_read_filter({"id": "bob@example.com", "login": "bob"})
        should = clause["bool"]["should"]
        self.assertIn({"ids": {"values": ["bob@example.com", "bob"]}}, should)
        self.assertIn({"terms": {"login.keyword": ["bob@example.com", "bob"]}}, should)
        self.assertEqual(clause["bool"]["minimum_should_match"], 1)

    def test_no_filter_without_identity(self):
        self.assertIsNone(nyx_user_read_filter({}))


class WriteRestrictionTests(unittest.TestCase):
    """Les trois indices qui portent des droits sont en lecture seule pour un non-admin."""

    def test_the_account_and_configuration_indices_are_protected(self):
        for name in ("nyx_user", "reindexed-nyx_user-v8", "nyx_app", "nyx_privilege",
                     "nyx_privileges", "nyx_privilege-history"):
            self.assertTrue(requires_admin_for_write(name), name)

    def test_data_indices_are_not_protected(self):
        # une app métier doit rester éditable par ses utilisateurs
        for name in ("airport_alert-2026.11", "nyx_energy-2026.11", "nyx_lambda",
                     "nyx_user_history", "deliveries-2026"):
            self.assertFalse(requires_admin_for_write(name), name)

    def test_the_guard_does_not_crash_on_a_missing_name(self):
        self.assertFalse(requires_admin_for_write(None))
        self.assertFalse(requires_admin_for_write(""))

    def test_only_the_admin_privilege_counts(self):
        self.assertTrue(is_admin({"privileges": ["admin"]}))
        self.assertFalse(is_admin({"privileges": ["YUL", "report"]}))
        self.assertFalse(is_admin({}))
        self.assertFalse(is_admin(None))


class SearchRestrictionTests(unittest.TestCase):
    def setUp(self):
        state.last_indices_refresh = datetime.now()
        state.indices_refresh_seconds = 60
        state.indices = []

    def test_a_non_admin_reading_nyx_user_is_restricted_to_their_own_account(self):
        allowed, query, _ = can_use_indice("nyx_user", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(filters_of(query), [nyx_user_read_filter(user())])

    def test_the_restriction_also_covers_the_reindex_alias(self):
        allowed, query, _ = can_use_indice("reindexed-nyx_user-v8", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(len(filters_of(query)), 1)

    def test_the_restriction_also_covers_the_wildcard_form(self):
        # `generic_search/nyx_user*` : la forme qu'utilise l'interface pour lister les
        # comptes, et celle qui laissait passer les 21 condensats
        allowed, query, _ = can_use_indice("nyx_user*", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(len(filters_of(query)), 1)

    def test_an_admin_keeps_the_whole_index(self):
        # sans cette exception, l'écran d'administration des comptes n'afficherait que soi-même
        allowed, query, _ = can_use_indice("nyx_user", user(privileges=["admin"]), None)
        self.assertTrue(allowed)
        self.assertEqual(filters_of(query), [])

    def test_a_token_without_identity_is_refused_rather_than_broadened(self):
        # échouer en fermé : un jeton incomplet ne doit pas rendre l'index entier
        allowed, _, _ = can_use_indice("nyx_user", {"privileges": ["YUL"]}, None)
        self.assertFalse(allowed)

    def test_another_index_is_left_untouched(self):
        allowed, query, _ = can_use_indice("deliveries-2026", user(), None)
        self.assertTrue(allowed)
        self.assertEqual(filters_of(query), [])

    def test_the_restriction_survives_the_row_filter_branch(self):
        # la branche `filtercolumn` réécrit la query_string puis retourne : le filtre ajouté
        # avant la boucle de règles doit donc être porté par `bool.filter`, pas par la chaîne
        state.indices = [rule("nyx_user", filtercolumn="site")]
        allowed, query, _ = can_use_indice("nyx_user", user(filters=["A"]), None)
        self.assertTrue(allowed)
        self.assertIn("site:A", query["bool"]["must"][0]["query_string"]["query"])
        self.assertEqual(len(filters_of(query)), 1)

    def test_the_restriction_survives_a_user_query(self):
        query = {"bool": {"must": [{"query_string": {"query": "privileges:admin",
                                                     "default_field": "*"}}]}}
        allowed, result, _ = can_use_indice("nyx_user", user(), query)
        self.assertTrue(allowed)
        self.assertEqual(result["bool"]["must"][0]["query_string"]["query"], "privileges:admin")
        self.assertEqual(len(filters_of(result)), 1, "le filtre doit être and-é, pas substitué")

    def test_an_existing_filter_list_is_kept_and_completed(self):
        query = {"bool": {"must": [{"match_all": {}}],
                          "filter": [{"term": {"active": True}}]}}
        allowed, result, _ = can_use_indice("nyx_user", user(), query)
        self.assertTrue(allowed)
        self.assertEqual(len(filters_of(result)), 2)


class DirectReadTests(unittest.TestCase):
    def test_a_non_admin_reads_their_own_document(self):
        u = {"id": "bob@example.com", "login": "bob", "privileges": ["YUL"]}
        self.assertTrue(can_read_nyx_user(u, "bob@example.com"))
        self.assertTrue(can_read_nyx_user(u, "bob"))

    def test_a_non_admin_cannot_read_another_account(self):
        u = {"id": "bob@example.com", "login": "bob", "privileges": ["YUL"]}
        self.assertFalse(can_read_nyx_user(u, "alice@example.com"))
        self.assertFalse(can_read_nyx_user(u, ""))

    def test_an_admin_reads_any_account(self):
        u = {"id": "root@example.com", "login": "root", "privileges": ["admin"]}
        self.assertTrue(can_read_nyx_user(u, "alice@example.com"))

    def test_no_user_reads_nothing(self):
        self.assertFalse(can_read_nyx_user(None, "alice@example.com"))
        self.assertFalse(can_read_nyx_user({}, "alice@example.com"))


if __name__ == "__main__":
    unittest.main()
