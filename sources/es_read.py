"""Lecture d'un document par identifiant.

Ce module ne dépend ni de Flask ni du client Elasticsearch : il prend un objet `es` et renvoie
un couple `(erreur, document)`. C'est ce qui le rend testable avec un faux client, sans monter
d'application ni installer pandas.

Il existe parce qu'un document absent n'est pas une panne : la création d'un utilisateur commence
par un GET sur son futur identifiant, et répondre « unable to get data » y faisait entrer une
opération normale dans le compteur d'erreurs de l'API.
"""

import logging

logger = logging.getLogger(__name__)


def is_not_found(exc):
    """Vrai si l'exception traduit un document absent, et non une panne."""
    return (getattr(exc, "status_code", None) == 404
            or type(exc).__name__ == "NotFoundError")

def find_user_by_login(es, login, elkversion):
    """Retrouve un utilisateur par son login quand son _id ne le vaut pas.

    Huit comptes de la prod sont indexés sous leur adresse e-mail alors que leur champ
    `login` porte un nom court (`amarchand@icloud.com` -> `amarchand`). Sans ce repli,
    ces comptes sont introuvables par login — et l'interface, qui vérifie la disponibilité
    d'un e-mail avant de créer un compte, en conclut qu'il est libre et en crée un doublon.
    """
    body = {"size": 1, "query": {"term": {"login.keyword": str(login).lower()}}}
    try:
        if elkversion >= 7:
            res = es.search(index="nyx_user", body=body)
        else:
            res = es.search(index="nyx_user", doc_type="doc", body=body)
    except Exception:
        logger.warning("recherche par login impossible", exc_info=True)
        return None
    hits = res.get("hits", {}).get("hits", [])
    return hits[0] if hits else None

def fetch_document(es, index, object_id, elkversion, doc_type="doc"):
    """Lit un document par identifiant. Un document absent n'est pas une erreur d'API.

    La création d'un utilisateur commence par un GET sur son futur identifiant : le
    document n'existe évidemment pas encore, et l'interface s'appuie sur cette absence
    pour autoriser la création. Répondre « unable to get data » faisait donc entrer une
    opération normale dans le compteur d'erreurs — trois par création de compte.
    """
    try:
        if elkversion >= 7:
            return "", es.get(index=index, id=object_id)
        return "", es.get(index=index, id=object_id, doc_type=doc_type)
    except Exception as exc:
        if not is_not_found(exc):
            logger.error("unable to get data", exc_info=True)
            return "unable to get data", None
    if index == "nyx_user":
        return "", find_user_by_login(es, object_id, elkversion)
    return "", None
