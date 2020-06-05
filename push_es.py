import sys
import json
import boto3
from botocore.exceptions import ClientError

from datetime import datetime
from elastic import es, ElasticsearchException
from snowflake_client import ctx, cs
from utils import Pretty
from constants import ES_DOMAIN, ES_INDEX, KINESIS_FIREHOSE
from elasticsearch.helpers import parallel_bulk
import time
# from collections import deque

jsonFromRowData = []

try:
    offset = 0
    limit = 1000000

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
    # except ElasticsearchException as e:
    #     print('********************* print_query_error **************************')
    #     print(f'{ES_INDEX} not found')
    #     print(e)
    # finally:
    #     print('********************* print_offset **************************')
    #     print(offset)

    start = time.time()
    # cs.execute(f"""
    #    WITH streams_by_track_playlist AS (
    #         SELECT *
    #         FROM STREAMS_BY_TRACK_PLAYLIST_COUNTRY_FEED_ROLLUP
    #         LIMIT {limit}
    #         OFFSET {offset}
    #    )
    #    SELECT
    #         OBJECT_CONSTRUCT(
    #             'track_id', ts.track_id,
    #             'playlist_id',ts.playlist_id,
    #             'country_code',ts.country_code,
    #             'feed_id',ts.feed_id,
    #             'store_playlist_id',ts.store_playlist_id,
    #             'label_id',ts.label_id,
    #             'store_id',ts.store_id,
    #             'product_id',ts.product_id,
    #             'isrc',ts.isrc,
    #             'streams_7_days',ts.streams_7_days
    #         )
    #     FROM streams_by_track_playlist as ts
    # """)

    cs.execute(f"""
       WITH top_sound AS (
            SELECT *
            FROM STREAMS_BY_TRACK_PLAYLIST_COUNTRY_DAILY
            LIMIT {limit}
            OFFSET {offset}
       )
       SELECT
            OBJECT_CONSTRUCT(
                'isrc', ts.isrc,
                'feed_id', ts.feed_id,
                'store_id', ts.store_id,
                'label_id', ts.label_id,
                'track_id', ts.track_id,
                'product_id', ts.product_id,
                'country_code', ts.country_code,
                'playlist_id', ts.playlist_id,
                'download_activity_date', ts.download_activity_date,
                'store_playlist_id', ts.store_playlist_id,
                'playlist_name', ts.playlist_name,
                'playlist_url', ts.playlist_url,
                'playlist_image', ts.playlist_image,
                'streams', ts.streams
            )
        FROM top_sound as ts
    """)
    all_rows = cs.fetchall()

    query_seconds = time.time() - start
    print("it took for query " + str(query_seconds) + " seconds.")

    # i = offset
    # print('********************* print_last_index **************************')
    # print(f'last index pushed: {i}')
    # print(f'Snowflake data length: {len(all_rows)}')
    data_start = time.time()
    for row in all_rows:
        jsonFromRow = json.loads(row[0])
        jsonFromRow['timestamp'] = datetime.now().timestamp()
        jsonFromRowData.append(jsonFromRow)
        # res = es.index(index=ES_INDEX, id=i, body=jsonFromRow)
    data_seconds = time.time() - data_start
    print("it took for data " + str(data_seconds) + " seconds.")
    jj
    # def gen_data():
    #     for row in all_rows:
    #         jsonFromRow = json.loads(row[0])
    #         # print(Pretty(jsonFromRow).print())
    #         jsonFromRow['timestamp'] = datetime.now()

    #         yield {
    #             "_index": ES_INDEX,
    #             "_type": "document",
    #             "_source": jsonFromRow,
    #         }

    # # #     print('********************* print_ingested **************************')
    # # #     print(Pretty(res).print())

    start = time.time()
    # for success, info in parallel_bulk(es, gen_data(), thread_count=10, chunk_size=10000):
    #     if not success:
    #         print('********************* print_ingest_error **************************')
    #         print('A document failed:', Pretty(info).print())
    #     # if success:
    #     #     print('********************* print_ingest **************************')
    #     #     print('A document success:', Pretty(info).print())

    def firehose_deliver(client, stream_name, records):

        if len(records) == 0:
            raise Exception("Record list is empty")

        if isinstance(records, dict):
            raise Exception("Single record given, array is required")

        # payload = base64.b64encode(records.encode('utf8'))
        payload = records
        # import pdb
        # pdb.set_trace()
       # try:
      #      response = client.put_record_batch(
     #           DeliveryStreamName=stream_name,
    #            Records=payload
   #         )
  #          return response
 #       except ClientError as c:
#            print(c)

    try:
        chunkSize = 500
        firehose_client = boto3.client('firehose')

        for i in range(0, len(jsonFromRowData), chunkSize):
            recordsDict = []
            for data in jsonFromRowData[i:i+chunkSize]:
                recordsDict.append({'Data': json.dumps(data)})

            res = firehose_deliver(
                firehose_client,
                KINESIS_FIREHOSE,
                recordsDict
            )

            # print('xxxxxxxxxxxx')
            # print(i)
            # print('xxxxxxxxxxxx')
            # print(Pretty(res).print())
            # print('xxxxxxxxxxxx')

    except ClientError as e:
        print('**********')
        print(e)
        exit(1)

    seconds = time.time() - start
    print("it took for firehose_bulk " + str(seconds) + " seconds.")
    print("total time for " + str(offset)  + ' is '+ str(query_seconds + seconds ))
finally:
    cs.close()

ctx.close()
