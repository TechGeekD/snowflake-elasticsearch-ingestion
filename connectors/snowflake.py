import snowflake.connector

from utils.pkey import PRIVATE_KEY
from constants import SNOWFLAKE_USER_NAME, SNOWFLAKE_ACCOUNT, \
                      SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, \
                      SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, \
                      SNOWFLAKE_PASSWORD

snowflake_ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER_NAME,
        account=SNOWFLAKE_ACCOUNT,
        password=SNOWFLAKE_PASSWORD,
        private_key=PRIVATE_KEY,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
      )

snowflake_cursor = snowflake_ctx.cursor()
