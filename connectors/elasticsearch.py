
from elasticsearch import Elasticsearch, RequestsHttpConnection, ElasticsearchException
from utils.creds import awsauth

from constants import ES_DOMAIN, ELASTICSEARCH_HTTP_COMPRESS, \
    ELASTICSEARCH_MAXSIZE, ELASTICSEARCH_TIMEOUT, \
    ELASTICSEARCH_RETRY_ON_TIMEOUT, ELASTICSEARCH_MAX_RETRIES

elasticsearch_client = Elasticsearch(
        [ES_DOMAIN],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        http_compress=ELASTICSEARCH_HTTP_COMPRESS,
        maxsize=ELASTICSEARCH_MAXSIZE,
        timeout=ELASTICSEARCH_TIMEOUT,
        retry_on_timeout=ELASTICSEARCH_RETRY_ON_TIMEOUT,
        max_retries=ELASTICSEARCH_MAX_RETRIES,
    )
