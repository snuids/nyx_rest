"""Politique de mot de passe des comptes NYX.

Jusqu'ici, l'interface créait chaque compte avec le **même** condensat pbkdf2 écrit en dur dans
`User.vue`. Tous les comptes neufs partageaient donc le même secret jusqu'à ce que quelqu'un pense
à le réinitialiser — un mot de passe partagé que personne ne connaît est une porte laissée ouverte.

Deux règles remplacent cette pratique :

* un compte créé sans mot de passe reçoit un mot de passe **aléatoire que personne ne connaît**,
  et le drapeau ``mustResetPassword`` ;
* tant que le drapeau est levé, le jeton obtenu à la connexion n'ouvre que la route de changement
  de mot de passe et la déconnexion (voir ``middleware.token_required``).

Le reste de l'API n'est donc pas atteignable avec un mot de passe provisoire : le changement n'est
pas une politesse de l'interface, c'est une condition d'accès.
"""

import secrets

# 18 octets donnent 24 caractères url-safe : 144 bits d'entropie, hors de portée d'une recherche
# exhaustive, et sans caractère ambigu à recopier depuis un écran ou un message.
TEMPORARY_BYTES = 18

# Routes accessibles avec un mot de passe provisoire. Tout le reste est refusé.
ALLOWED_BEFORE_CHANGE = (
    "/api/v1/cred/changepassword",
    "/api/v1/cred/logout",
    "/api/v1/healthz",
    "/api/v1/metrics",
)

MIN_LENGTH = 12


def generate_temporary_password():
    """Mot de passe aléatoire, jamais affiché ni transmis : le compte est inutilisable tel quel."""
    return secrets.token_urlsafe(TEMPORARY_BYTES)


def is_allowed_before_change(path):
    """Vrai si la route reste ouverte tant que le mot de passe n'a pas été choisi."""
    return (path or "").rstrip("/") in ALLOWED_BEFORE_CHANGE


def must_change_password(usr, path):
    """Vrai si l'appel doit être refusé parce que le mot de passe n'a pas encore été choisi."""
    if not usr or not usr.get("mustResetPassword"):
        return False
    return not is_allowed_before_change(path)


def check_new_password(password, login=""):
    """Contrôles minimaux sur un mot de passe choisi par l'utilisateur.

    Longueur seulement : les règles de composition (majuscule, chiffre, symbole) sont
    aujourd'hui déconseillées par l'ANSSI et le NIST, elles produisent des mots de passe
    prévisibles et notés sur un papier. La longueur est ce qui compte.
    """
    if not password or len(password) < MIN_LENGTH:
        return f"Le mot de passe doit contenir au moins {MIN_LENGTH} caractères."
    if login and password.lower() == str(login).lower():
        return "Le mot de passe ne peut pas être identique à l'identifiant."
    return ""
