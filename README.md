# Nyx Rest API

![badge](https://img.shields.io/badge/made%20with-python-blue.svg?style=flat-square)
![badge](https://img.shields.io/github/languages/code-size/snuids/nyx_rest)
![badge](https://img.shields.io/github/last-commit/snuids/nyx_rest)

NYX Rest API (version 3.21.12).

File listing, downloads, uploads, and ZIPs stay within the selected file-system
app's root and require the app's privileges. For `rec_id=-1`, paths may be
relative to `/logs` or begin with `/logs` (as used by the logs UI), and require
the `logs` or `admin` privilege. `/streamfile` likewise
checks app privileges; configured extra stream roots are available to admins.

Generic SQL CRUD requests now bind values as query parameters and safely quote
table and column names for both PostgreSQL and SQL Server.

Two-factor login now rejects missing or expired verification codes and consumes
a valid code after use. Verification codes are no longer written to the log.

The `/api/v1/files` endpoint requires a valid `token` query parameter for both
downloads (GET) and uploads (POST).

`/api/v1/streamfile` allows files under `/logs` by default. To stream files
from other directories, set `STREAM_ALLOWED_ROOTS` to a comma-separated list
of allowed roots (include `/logs` if log streaming should remain available).
File-system app roots are also allowed for users with access to the app. Requests
still require a valid token and the corresponding privileges.

# Run

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
