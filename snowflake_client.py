import os
import snowflake.connector
from pkey import private_key
from constants import SNOWFLAKE_USER_NAME, SNOWFLAKE_ACCOUNT, \
                      SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, \
                      SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE

ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER_NAME,
        account=SNOWFLAKE_ACCOUNT,
        private_key=private_key,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
      )

cs = ctx.cursor()