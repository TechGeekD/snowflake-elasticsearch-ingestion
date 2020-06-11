import os

ES_INDEX = os.environ.get('ES_INDEX', '')
ES_DOMAIN = os.environ.get('ES_DOMAIN', '')

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

SNOWFLAKE_PRIVATE_KEY_PATH = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', '')
SNOWFLAKE_KEY_PASSPHRASE = os.environ.get('SNOWFLAKE_KEY_PASSPHRASE', None)
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')

SNOWFLAKE_USER_NAME = os.environ.get('SNOWFLAKE_USER_NAME', '')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD', '')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', '')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', '')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', '')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', '')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE', '')

INGEST_CONFIG = {
    'START_DATE': os.environ.get('START_DATE', None),
    'END_DATE': os.environ.get('END_DATE', None),
    'CHUNK_LIMIT': int(os.environ.get('CHUNK_LIMIT', 2000000)),
    'OFFSET': int(os.environ.get('OFFSET', 0)),
    'MAX_DATA_LIMIT': int(os.environ.get('MAX_DATA_LIMIT', 0)),
    'NO_OF_PROCESSES': int(os.environ.get('NO_OF_PROCESSES', 18)),
}

ELASTICSEARCH_HTTP_COMPRESS = os.environ.get('ELASTICSEARCH_HTTP_COMPRESS', True) == 1
ELASTICSEARCH_MAXSIZE = int(os.environ.get('ELASTICSEARCH_MAXSIZE', 12))
ELASTICSEARCH_TIMEOUT = int(os.environ.get('ELASTICSEARCH_TIMEOUT', 60))
ELASTICSEARCH_RETRY_ON_TIMEOUT = os.environ.get('ELASTICSEARCH_RETRY_ON_TIMEOUT', True) == 1
ELASTICSEARCH_MAX_RETRIES = int(os.environ.get('ELASTICSEARCH_MAX_RETRIES', 1))
