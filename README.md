# Nyx Rest API

![badge](https://img.shields.io/badge/made%20with-python-blue.svg?style=flat-square)
![badge](https://img.shields.io/github/languages/code-size/snuids/nyx_rest)
![badge](https://img.shields.io/github/last-commit/snuids/nyx_rest)

NYX Rest API (version 3.22.0).

The API is the only component that reads and writes the platform configuration and the customer
data. Its contract is described in the deployment repository under `docs/api.md`; this file covers
what is specific to running and extending the API itself.

## Configuration bundle (promotion)

The platform configuration — screens (`nyx_app`), index rules (`nyx_indice`), privileges
(`nyx_privilege`), filters (`nyx_filter`), translations (`nyx_translation`) and named queries
(`nyx_datasource`) — can be exported from one instance and applied to another. That is how a
change designed in a development instance reaches production: export, commit the JSON, review,
apply.

Deliberately **not** part of a bundle: `nyx_user` (password hashes), `nyx_apikey`, `nyx_config`
(instance stylesheet and password rules), the report definitions and lambdas, and every runtime
index. A bundle that contains them is rejected.

### Endpoints

| Method and path | Query | Effect |
| --- | --- | --- |
| `GET /api/v1/config/export` | `indices=nyx_app,nyx_indice` (optional) | returns `{error, counts, bundle}` |
| `POST /api/v1/config/import` | `apply=1` to write, `prune=1` to delete what the bundle omits | returns the plan, or the plan plus what was applied |

`import` is a **dry run by default**: it answers with the plan
(`create` / `update` / `unchanged` / `delete`, per document, with the list of changed fields) and
writes nothing. An import that would change nothing is a no-op. Both endpoints require an
`admin` token.

```shell
# what would change on the target? (no write)
curl -sk -X POST "https://prod/api/v1/config/import?token=$TOKEN" \
  -H 'Content-Type: application/json' --data @config/bundle.json

# apply it, and delete the documents the bundle no longer contains
curl -sk -X POST "https://prod/api/v1/config/import?token=$TOKEN&apply=1&prune=1" \
  -H 'Content-Type: application/json' --data @config/bundle.json
```

### CLI

`tools/nyx_config.py` wraps the two endpoints with the standard library only, so it runs on an
operator machine without the API dependencies:

```shell
export NYX_TOKEN=...                     # admin token, kept out of the process arguments

tools/nyx_config.py export --url https://dev.example.com -o config/bundle.json
tools/nyx_config.py plan   config/bundle.json --url https://prod.example.com
tools/nyx_config.py apply  config/bundle.json --url https://prod.example.com --yes
```

`apply` without `--yes` prints the plan and exits with code 2, so a pipeline cannot write by
accident. `--prune` also deletes what the bundle omits — use it only once the target has no
local-only screens left.

### Reviewing a promotion

1. `export` from the source instance and commit the bundle (it sorts documents and fields, so a
   diff is readable).
2. `plan` against the target and read the `fieldsChanged` lists — this is the review.
3. `apply --yes`, then reload the console (`/api/v1/reloadconfig` happens at login; the menus are
   recomputed from the imported documents).
4. Keep the bundle in git: the history of the platform design becomes reviewable.

## Behaviour notes

File listing, downloads, uploads, and ZIPs stay within the selected file-system app's root and
require the app's privileges. For `rec_id=-1`, paths may be relative to `/logs` or begin with
`/logs` (as used by the logs UI), and require the `logs` or `admin` privilege. `/streamfile`
likewise checks app privileges; configured extra stream roots are available to admins.

Generic SQL CRUD requests bind values as query parameters and safely quote table and column names
for both PostgreSQL and SQL Server.

Two-factor login rejects missing or expired verification codes and consumes a valid code after
use. Verification codes are never written to the log.

`/api/v1/files` requires a valid `token` query parameter for both downloads (GET) and uploads
(POST).

`/api/v1/streamfile` allows files under `/logs` by default. To stream files from other
directories, set `STREAM_ALLOWED_ROOTS` to a comma-separated list of allowed roots (include
`/logs` if log streaming should remain available). File-system app roots are also allowed for
users with access to the app. Requests still require a valid token and the corresponding
privileges.

## Authentication

Two ways to present the token, checked in this order:

```shell
curl -sk -H "Authorization: Bearer $TOKEN" https://host/api/v1/status   # preferred
curl -sk "https://host/api/v1/status?token=$TOKEN"                      # still accepted
```

The query parameter is the historical form: it ends up in nginx access logs, browser history and
`Referer` headers. It keeps working because the console and the mobile app were built that way;
enable the API log at `DEBUG` to see which routes still receive it (`Token passed as a query
parameter on …`).

## Probes and metrics

| Endpoint | Token | Answer |
| --- | --- | --- |
| `GET /api/v1/healthz` | none | `{status, version, elasticVersion, checks{elasticsearch,redis,broker}, time}`, **503** when a dependency is down |
| `GET /api/v1/metrics` | none | Prometheus text: `nyx_up`, `nyx_info`, `nyx_elasticsearch_up`, `nyx_redis_up`, `nyx_broker_up`, `nyx_broker_life_sign_age_seconds`, `nyx_requests_total`, `nyx_errors_total`, `nyx_active_tokens` |

`/api/v1/status` requires a token, so it cannot be a liveness probe: use `/healthz`. The broker
is considered up while its life sign is younger than 30 s (the API publishes one every 5 s), which
catches a worker that is running but has lost its STOMP connection.

The counters are per worker: gunicorn runs three of them, so a Prometheus scrape of the service
sees whichever one answers. Sum them (`sum by (instance)`) or scrape each worker by name.

## Tests

```shell
python -m unittest discover -s tests
```

`tests/test_config_bundle.py` covers the bundle logic without any dependency.
`tests/test_config_io.py` runs the real routes through a Flask test client with a fake
Elasticsearch; it skips itself when Flask is not installed.

## Run

create a startrest.sh file with the following content:

```
#!/bin/sh
echo "STARTING NYX API"
echo "================"

export REDIS_IP="localhost"
export AMQC_URL="YOUR_NYX_SERVER"
export AMQC_LOGIN="admin"
export AMQC_PASSWORD="activemq_pass"
export AMQC_PORT=61613

export ELK_URL="localhost"
export ELK_LOGIN=""
export ELK_PASSWORD=""
export ELK_PORT=9200
export ELK_SSL=true

export USE_LOGSTASH=false

export OUTPUT_URL="https://YOUR_NYX_SERVER/outputs/"
export OUTPUT_FOLDER="./outputs/"

export WELCOMEMESSAGE="Welcome to Nyx"
export ICON="anchor"

export PG_LOGIN=nyx
export PG_PASSWORD=POSTGRES_PASS
export PG_HOST=YOUR_NYX_SERVER
export PG_PORT=5444
export PG_DATABASE=nyx

echo "Variables SET"
python nyx_rest_api_plus.py 
```
