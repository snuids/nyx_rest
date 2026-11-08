"""What a search term and a sort may become in the SQL of a `pg_search`.

Two values of the request body used to be pasted into the statement by `loadPGData`:

* the free-text search, through `query.replace("${FILTER}", "%" + data["query"] + "%")`;
* the sort column, through `" ORDER BY " + data["sort"]["column"]`.

Both come from the console's search box, so any account holding a token could read or modify a
table it was allowed to read (`pump' OR '1'='1`, `kwh; DROP TABLE cars`). The generic CRUD route
was parameterised in 3.21.8; this engine was not.

The filter is now a **bound parameter**, which raises one question the old code never asked: where
does the token sit? Three spellings exist in the `nyx_app` documents of the platform, and only some
of them can be bound:

    where name like '%${FILTER}%'      the usual form: the wildcards come from the template
    where name like '%${FILTER}'       one wildcard
    where name like ${FILTER}          no quotes at all (invalid SQL before, valid now)

A quoted occurrence — with or without the `%` — is therefore replaced **as a whole** (quotes
included) by the placeholder, and the parameter carries `%term%`. The token is never interpolated
again: a spelling this module cannot bind is refused with `ValueError`, which the route answers as
an error instead of running a statement that may have lost its quotes.
"""

import re

from helpers.sql_queries import quote_identifier

FILTER_TOKEN = "${FILTER}"

DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 50000

# A quoted occurrence — the quotes are part of the match, so they go away with the token — or a
# bare one, which the statement is expected to compare directly.
_TOKEN = re.compile(r"'%?\$\{FILTER\}%?'|\$\{FILTER\}")

# Stands in for a token while the literal `%` of the query are escaped. It cannot appear in a SQL
# text: a NUL byte ends a C string.
_SENTINEL = "\x00FILTER\x00"


def placeholder_for(db_type):
    """The parameter marker of the driver: `%s` for psycopg2, `?` for pyodbc."""
    if db_type == "sqlserver":
        return "?"
    if db_type == "postgres":
        return "%s"
    raise ValueError("Unsupported database type")


def bind_filter(template, term, db_type):
    """Return `(sql, params)` with every `${FILTER}` bound to `%term%`."""
    placeholder = placeholder_for(db_type)
    template = template or ""
    value = "%" + ("" if term is None else str(term)) + "%"

    # The parameters must follow the placeholders, in the order of the statement: the tokens are
    # therefore scanned once, left to right.
    params = []
    for match in _TOKEN.finditer(template):
        if match.group(0) == FILTER_TOKEN and template.count("'", 0, match.start()) % 2 == 1:
            # Inside a longer literal (`'ref-${FILTER}-x'`): binding it would leave the marker
            # inside the string and the parameter unused. Refuse rather than guess.
            raise ValueError("the ${FILTER} token of this query cannot be bound")
        params.append(value)

    if not params:
        # No token: the statement is executed as it is, without parameters — exactly what the
        # engine did before. This keeps every application that does not use `${FILTER}` intact.
        return template, []

    # psycopg2 formats the **whole** statement when parameters are passed: every other `%` of the
    # query (`to_char(ts, '%Y')`, a hardcoded `LIKE '%abc%'`) has to be doubled, or it is read as
    # a format character. pyodbc does not format at all and must not see those doublings.
    sql = _TOKEN.sub(_SENTINEL, template)
    if db_type == "postgres":
        sql = sql.replace("%", "%%")
    return sql.replace(_SENTINEL, placeholder), params


def page_values(size, page):
    """`(size, page)` as integers inside sane bounds.

    `data["size"]` was interpolated into `LIMIT`/`TOP` as-is: a string there was a second way of
    writing arbitrary SQL into the statement. A value that is not a number is not a page size.
    """
    try:
        size = int(size)
    except (TypeError, ValueError):
        size = DEFAULT_PAGE_SIZE
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 1
    return max(1, min(size, MAX_PAGE_SIZE)), max(1, page)


def sort_clause(sort, columns, db_type):
    """The `ORDER BY` fragment for `sort`, or `""` when the request asks for nothing usable.

    Only a column this application declares (`headercolumns`, plus `pkey`) may be sorted on, and it
    is quoted by the same helper the CRUD route uses: the console has no reason to send anything
    else, and a column name cannot be bound as a parameter.
    """
    if not isinstance(sort, dict):
        return ""
    column = sort.get("column")
    if not isinstance(column, str) or not column:
        return ""

    allowed = [name for name in (columns or []) if isinstance(name, str)]
    if column not in allowed:
        return ""

    direction = " DESC" if sort.get("order") == "descending" else ""
    return " ORDER BY " + quote_identifier(column, db_type) + direction
