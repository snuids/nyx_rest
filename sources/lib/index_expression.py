"""How an index expression designates an index.

Shared by the API and by the queue worker: both decide whether a query reaches a protected index,
and they must recognise the same spellings. A rule enforced in one place and skipped in the other
is not a rule — the worker reads Elasticsearch directly, so it re-applies the check itself.

Elasticsearch accepts a **comma-separated list** wherever a single index name is accepted
(`_search/nyx_app,nyx_user`). Deciding on the whole string treated `nyx_app,nyx_user` as a name
that simply is not `nyx_user`, so the own-record filter was skipped on a query that did return
every account, hashes included.
"""

import re


def has_wildcard(expression):
    return "*" in expression or "?" in expression


def _reaches_single(expression, root):
    name = str(expression or "").strip().lower()
    if not name:
        return False
    if not has_wildcard(name):
        return (name == root or name.startswith(root + "-") or name.startswith(root + "."))
    prefix = re.split(r"[*?]", name, 1)[0]
    return root.startswith(prefix) or prefix.startswith(root)


def expression_reaches(expression, root):
    """True if this index expression may designate the `root` index.

    Three spellings lead to the same index and must be treated alike: the exact name
    (`nyx_user`), the reindexing alias (`reindexed-nyx_user-v8`, suffix after `-` or `.`)
    and the pattern (`nyx_user*`, `nyx_*`, `*`). A pattern is compared by prefix: as soon
    as it may cover the root, it is considered to target the index — `nyx_apicalls*` is
    not concerned, `nyx_*` is.

    A comma-separated list reaches the root as soon as **one** of its parts does.
    """
    return any(_reaches_single(part, root) for part in str(expression or "").split(","))
