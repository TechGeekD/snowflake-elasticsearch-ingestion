import boto3
from requests_aws4auth import AWS4Auth

from constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, \
    AWS_REGION

credentials = boto3.Session().get_credentials()

awsauth = AWS4Auth(AWS_ACCESS_KEY_ID,
                   AWS_SECRET_ACCESS_KEY,
                   AWS_REGION,
                   'es',
                   session_token=credentials.token)
