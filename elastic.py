
from elasticsearch import Elasticsearch, RequestsHttpConnection, ElasticsearchException
from creds import awsauth
from constants import ES_DOMAIN, AWS_ES_USERNAME, AWS_ES_PASSWORD


es = Elasticsearch(
        [ES_DOMAIN],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # http_auth=(AWS_ES_USERNAME, AWS_ES_PASSWORD),

# print('***********************************************')
# print(json.dumps(es.info(), indent=4))
# print('***********************************************')
