import sys
from elastic import es, ElasticsearchException
from constants import ES_INDEX
from utils import Pretty

try:
    size = 1000

    request_cache = not bool(len(sys.argv) > 1)
    print(f'request_cache: {request_cache}')

    res = es.search(index=ES_INDEX, request_cache=request_cache, body={
            "size": size,
            "query": {
                "match_all": {}
            },
        })

    # scrollId = res['_scroll_id']
    # res2 = es.scroll(scroll_id = scrollId, scroll = '1m')

    print('********************* print_queried **************************')
    total = res['hits']['total']
    print(Pretty(f'data in es {total}').print())

    # print('********************* print_queried_as_table **************************')
    # print("isrc     streams_7_days      streams_7_days_growth       timestamp")
    # for hit in res['hits']['hits']:
    #     print("%(isrc)s     %(streams_7_days)s     %(streams_7_days_growth)s        %(timestamp)s" % hit["_source"])

    print(f'request_cache: {request_cache}')

except ElasticsearchException:
    print('********************* print_query_error **************************')
    print(f'{ES_INDEX} not found')