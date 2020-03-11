import os
import boto3
import requests
from requests_aws4auth import AWS4Auth
from constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

credentials = boto3.Session().get_credentials()

awsauth = AWS4Auth(AWS_ACCESS_KEY_ID,
                   AWS_SECRET_ACCESS_KEY,
                   "us-east-1",
                   "es",
                   session_token=credentials.token)

# response = requests.get(domain, auth=awsauth)

# print('***********************************************')
# print(json.dumps(json.loads(response.text), indent=4))
# print('***********************************************')

# esClient = boto3.client('es')
# data = esClient.list_domain_names()
# print(json.dumps(data, indent=4, sort_keys=True))
