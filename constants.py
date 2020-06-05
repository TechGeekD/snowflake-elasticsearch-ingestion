import os

ES_INDEX = os.environ.get('ES_INDEX', '')
ES_DOMAIN = os.environ.get('ES_DOMAIN', '')

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')

AWS_ES_USERNAME = os.environ.get('AWS_ES_USERNAME', '')
AWS_ES_PASSWORD = os.environ.get('AWS_ES_PASSWORD', '')

SNOWFLAKE_PRIVATE_KEY_PATH = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', '')
SNOWFLAKE_KEY_PASSPHRASE = os.environ.get('SNOWFLAKE_KEY_PASSPHRASE', '')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')

SNOWFLAKE_USER_NAME = os.environ.get('SNOWFLAKE_USER_NAME', '')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', '')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', '')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', '')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', '')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE', '')
KINESIS_FIREHOSE = os.environ.get('KINESIS_FIREHOSE', '')