
# Snowflake -> ElastiSearch Ingestion

Create python virtual environment.

```shell
python3 -m venv venv
```

Activate venv.

```shell
source venv/bin/activate
```

Initialize environment configuration:

```shell
cp env.sh.shadow env.sh
```

Update env values in env.sh then:

```shell
source env.sh
```

Run any `*_es` script:

```shell
python push_es.py
python request_es.py
python query_request_es.py
python delete_es_idx.py
```


To run multiple python script parallelly.

```shell
sh run.sh
```

Update `run.sh` as per need.

## P.S. Genereate public-private key & add public key to Snowflake
## Refer: [Key Pair Authentication](https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-key-pair-authentication)