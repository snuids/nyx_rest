# Client extensions

A module in this directory is loaded **only** when its name appears in the `EXT_MODULES` setting of
the API, comma separated:

```shell
EXT_MODULES=woop            # one module
EXT_MODULES=woop,onfleet    # several
                            # empty (the default): nothing is loaded
```

A module must expose the signature the loader calls:

```python
def config(api, conn, es, redis, token_required):
    ...
```

## Why nothing is discovered any more

Until 3.22.23 the loader scanned `lib/` for any file whose name contained `ext` and called its
`config()` at startup. Whatever was dropped there was mounted, with no authentication of its own:
`lib/ext_woop.py` had been answering `POST /api/v1/woop/deliveries/` (create an Onfleet task) and
`DELETE /api/v1/woop/deliveries/<task_id>` (delete it) unauthenticated on the production instance
since 2023. An extension is a decision, so it is now a setting.

## Adding one

1. put the module here, `extensions/<name>.py`, with `config(...)` as above;
2. decorate every resource with `token_required()` — an extension does not inherit the API's
   discipline, it has to apply it;
3. declare it in the environment (`EXT_MODULES=<name>`) and restart the container: the loader runs
   at import.

A module that fails to import is logged at ERROR with its traceback, and does not stop the API.

## What is parked here

`woop.py` — the WOOP/Onfleet delivery integration. It now requires a token on both resources, and
it is no longer loaded. Its organisation and team identifiers are still hard-coded, its calls still
ignore their HTTP status: it is kept as the starting point of the integration, not as maintained
code.
