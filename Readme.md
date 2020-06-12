
# Snowflake -> ElastiSearch Ingestion
Ingests data queried from snowflake to elasticsearch. It will ingest one month of data into index of same month at a time. After one month index is ingested it will create new index for next month & start ingesting that

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

Install requirements:

```shell
python -m pip install -r requirements.txt
```

Run script:

```shell
python index.py
```
OR
```shell
nohup venv/bin/python -u index.py > no_log.log &
```


## P.S. Genereate public-private key & add public key to Snowflake
## Refer: [Key Pair Authentication](https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-key-pair-authentication)