import sys
import json
from datetime import datetime
from elastic import es, ElasticsearchException
from snowflake_client import ctx, cs
from utils import Pretty
from constants import ES_DOMAIN, ES_INDEX
from elasticsearch.helpers import parallel_bulk
import time
# from collections import deque

jsonFromRowData = []

try:
    offset = 0
    limit = 400000

    if(len(sys.argv) > 1):
        offset=int(sys.argv[1])

    # try:
    #     res = es.search(index=ES_INDEX, body={
    #         "size": 1,
    #         "sort": { "timestamp": "desc" },
    #         "query": {
    #             "match_all": {}
    #         }
    #     })
    #     print('********************* print_last_doc **************************')
    #     print(Pretty(res).print())

    #     offset = int(res['hits']['hits'][0].get('_id'))
    # except ElasticsearchException:
    #     print('********************* print_get_last_doc_error **************************')
    #     print(f'{ES_INDEX} not found')
    # finally:
    #     print('********************* print_offset **************************')
    #     print(offset)

    start = time.time()
    cs.execute(f"""
       WITH top_sound AS (
            SELECT *
            FROM STREAMS_BY_TRACK_FEED_ROLLUP
            LIMIT {limit}
            OFFSET {offset}
       )
       SELECT
            OBJECT_CONSTRUCT(
                'isrc', ts.isrc,
                'feed_id',ts.feed_id,
                'store_id',ts.store_id,
                'label_id',ts.label_id,
                'track_id',ts.track_id,
                'product_id',ts.product_id,
                'streams_7_days',ts.streams_7_days,
                'streams_7_days_prev',ts.streams_7_days_prev,
                'streams_28_days',ts.streams_28_days,
                'streams_all_time',ts.streams_all_time
            )
        FROM top_sound as ts
    """)

    all_rows = cs.fetchall()

    query_seconds = time.time() - start
    print("it took for query " + str(query_seconds) + " seconds.")

    # i = offset
    # print('********************* print_last_index **************************')
    # print(f'last index pushed: {i}')
    print(f'Snowflake data length: {len(all_rows)}')

    def gen_data():
        for row in all_rows:
            jsonFromRow = json.loads(row[0])
            # print(Pretty(jsonFromRow).print())
            jsonFromRow['timestamp'] = datetime.now()

            yield {
                "_index": ES_INDEX,
                "_type": "document",
                "_source": jsonFromRow,
            }

    # for row in all_rows:
    #     i += 1
    #     jsonFromRow = json.loads(row[0])
    #     jsonFromRow['timestamp'] = datetime.now()
    #     res = es.index(index=ES_INDEX, id=i, body=jsonFromRow)

    #     print('********************* print_ingested **************************')
    #     print(Pretty(res).print())

    start = time.time()
    for success, info in parallel_bulk(es, gen_data(), thread_count=10, chunk_size=10000):
        if not success:
            print('********************* print_ingest_error **************************')
            print('A document failed:', Pretty(info).print())
        # if success:
        #     print('********************* print_ingest **************************')
        #     print('A document success:', Pretty(info).print())

    seconds = time.time() - start
    print("it took for parallel_bulk " + str(seconds) + " seconds.")
    print("total time "+ str(query_seconds + seconds ))

finally:
    cs.close()

ctx.close()
