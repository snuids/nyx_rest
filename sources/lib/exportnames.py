"""Names for the files the console downloads: `export_<uuid>.<ext>` told nobody anything.

A download is named after what it contains — the index, the SQL application — and the minute it was
asked for, because two exports of the same table were otherwise indistinguishable once on disk.

The name *is* the URL: the console opens it in a browser (`window.open`) and nginx serves a static
file, so there is no `Content-Disposition` that could carry a prettier one. It therefore has to be a
URL-safe path segment as well as a file name that the visitor's own machine will accept. Everything
outside `[A-Za-z0-9._-]` is replaced, the stem is bounded, and the extension comes from a whitelist —
never from the request.
"""

import re
import secrets
from datetime import datetime
from unicodedata import normalize

# What a download may carry: URL-safe (no quoting needed where the name becomes a URL) and portable
# to every filesystem the browser on the other side might write to.
UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")
REPEATED = re.compile(r"_{2,}")

# Long enough for the longest index of the cluster (66 characters seen in production: the internal
# Elasticsearch ones), short enough that the whole name stays far below the 255 bytes a name may
# hold, with room for the timestamp and the suffix.
MAX_STEM = 80

# The only formats this API writes. A value outside the list is not an extension, it is a name
# somebody tried to choose.
EXTENSIONS = ("csv", "xlsx")

# Windows refuses these names on every one of its filesystems, whatever the extension: a download
# that lands there would be renamed or refused by the browser.
RESERVED = {"con", "prn", "aux", "nul"} \
    | {"com%d" % number for number in range(1, 10)} \
    | {"lpt%d" % number for number in range(1, 10)}


def slug(value):
    """The part of a name that survives: ASCII, safe characters, bounded, never empty."""
    text = normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    text = REPEATED.sub("_", UNSAFE.sub("_", text)).strip("._-")
    return text[:MAX_STEM].strip("._-")


def export_name(stem, extension, at=None, token=None):
    """`<stem>_<yyyymmdd-hhmm>_<6 hex>.<ext>` — unique without a uuid, readable without a lookup.

    `at` and `token` exist for the tests; in production the clock and `secrets` choose both, which
    is what makes two exports of the same table asked for in the same minute distinguishable.
    """
    extension = str(extension or "").lower().strip(".")
    if extension not in EXTENSIONS:
        extension = "csv"

    base = slug(stem) or "export"
    if base.split(".")[0].lower() in RESERVED:
        base = "export_" + base

    moment = at or datetime.now()
    suffix = token if token is not None else secrets.token_hex(3)
    return "%s_%s_%s.%s" % (base, moment.strftime("%Y%m%d-%H%M"), suffix, extension)
