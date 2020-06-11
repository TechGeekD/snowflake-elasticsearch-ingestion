from utils.logger import Logger
from connectors.elasticsearch import elasticsearch_client, ElasticsearchException

from constants import ES_INDEX

try:
    index_name = f'{ES_INDEX}-*'
    res = elasticsearch_client.count(index=index_name, body={
            'query': {
                'match_all': {}
            },
        })

    total = res['count']
    Logger(f'Total Document count in es: {total}').log_info()

except ElasticsearchException as error:
    Logger(index_name).log_exception(error)
