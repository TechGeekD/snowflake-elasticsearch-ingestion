import sys
from elastic import es, ElasticsearchException
from constants import ES_INDEX
from utils import Pretty

# try:
size = 100

request_cache = not bool(len(sys.argv) > 1)
print(f'request_cache: {request_cache}')

# body = {
#     "query": {
#         "bool": {
#             "must": {
#                 "match": {
#                     "product_id": 1733637
#                 }
#             },
#         }
#     },
#     "size": size
# }

# body = {
#     "aggs": {
#         "by_label_id": {
#             "filters": {
#                 "filters": {
#                     "label_ids": {
#                         "match": {
#                             "label_id": 8673
#                         }
#                     }
#                 }
#             },
#             "aggs": {

#                 "top_hits_label_id": {
#                     "top_hits": {},
#                 },
#                 "sum_streams_all_time": {
#                     "bucket_script": {
#                         "bucket_path": {
#                             "streams_all_time": "top_hits_label_id['streams_all_time']"
#                         },
#                         "script": "params.streams_all_time"
#                     }
#                 }
#             }
#         }
#     }
# }


# body = {
#         "size": size,
#         "query": {
#             "match_all": {}
#         },
#     }

body = {
    "query": {
        "bool": {
            "must": {
                "term": {
                    "feed_id": [1, 2, 3]
                }
            }
        }
    }
}

# body = {
#     "aggs": {
#         "by_isrc": {
#             "terms": {
#                     "field": "isrc.keyword"
#             },
#             "aggs": {
#                 "sum_streams_all_time": {
#                     "sum": {
#                         "field": "streams_all_time"
#                     }
#                 }
#             }
#         },
#     }
# }
            # "aggs": {
            #     "isrc_data": {
            #         "top_hits": {
            #         }
            #     }
            # }
                        # "sort": [
                        #     {
                        #         "duration": {
                        #             "order": "asc"
                        #         }
                        #     }
                        # ],
                        # "size": 10,
                        # "from": 0


print(ES_INDEX, Pretty(body).print())
# res = es.indices.clear_cache(index=ES_INDEX);

# print('********************* print_clear_cache **************************')
# print(Pretty(res).print())
# res = es.indices.put_mapping(index=ES_INDEX, doc_type='document', body={
#   "properties": {
#     "isrc": {
#       "type": "text",
#       "fielddata": True
#     }
#   }
# })
# print(Pretty(res).print())

# res = es.indices.get_mapping(index=ES_INDEX)
# print(Pretty(res).print())
res = es.search(
        index=ES_INDEX,
        request_cache=request_cache,
        body=body,
        size=0
    )

# scrollId = res['_scroll_id']
# res2 = es.scroll(scroll_id = scrollId, scroll = '1m')

print('********************* print_queried **************************')
total = res['hits']['total']
print(Pretty(f'data in es {total}').print())
print(Pretty(res).print())

# print('********************* print_ping **************************')
# res = es.ping()
# print(Pretty(res).print())
# print('********************* print_queried_as_table **************************')
# print("isrc     streams_7_days      streams_7_days_growth       timestamp")
# for hit in res['hits']['hits']:
#     print("%(isrc)s     %(streams_7_days)s     %(streams_7_days_growth)s        %(timestamp)s" % hit["_source"])

print(f'request_cache: {request_cache}')

# except ElasticsearchException:
#     print('********************* print_query_error **************************')
#     print(f'{ES_INDEX} not found')
