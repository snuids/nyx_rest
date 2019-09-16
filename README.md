# Nyx Rest API

![badge](https://img.shields.io/badge/made%20with-python-blue.svg?style=flat-square)
![badge](https://img.shields.io/github/languages/code-size/snuids/nyx_rest)
![badge](https://img.shields.io/github/last-commit/snuids/nyx_rest)

NYX Rest API

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

export ELK_URL="YOUR_NYX_SERVER"
export ELK_LOGIN="user"
export ELK_PASSWORD="ELK_PASS"
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

