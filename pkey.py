import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, ENVIRONMENT, \
                    SNOWFLAKE_KEY_PASSPHRASE, SNOWFLAKE_PRIVATE_KEY_PATH

# import pdb
# pdb.set_trace()

if ENVIRONMENT == 'dev' and SNOWFLAKE_KEY_PASSPHRASE:
    # Snowflake key
    if not SNOWFLAKE_PRIVATE_KEY_PATH:
        SNOWFLAKE_PRIVATE_KEY_PATH = '{}/.ssh/snowflake/rsa_key.p8'.format(os.environ['HOME'])

    with open(SNOWFLAKE_PRIVATE_KEY_PATH, 'rb') as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=SNOWFLAKE_KEY_PASSPHRASE.encode(),
            backend=default_backend()
        )

    private_key = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
else:
    private_key = None