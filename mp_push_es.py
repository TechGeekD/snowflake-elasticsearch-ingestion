import sys
import json
from datetime import datetime
from elastic import es, ElasticsearchException
from snowflake_client import ctx, cs
from utils import Pretty
from constants import ES_DOMAIN, ES_INDEX
from elasticsearch.helpers import parallel_bulk
import time
from multiprocessing import Pool

def push_to_es(offset=0):
    jsonFromRowData = []

    try:
        limit = 20000000

        start = time.time()

        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        print('Before Query')
        print('Current Timestamp: ', timestampStr)

        cs.execute(f"""
            WITH top_sound AS (
                SELECT stp.*, pf.followers
                FROM STREAMS_BY_TRACK_PLAYLIST_COUNTRY_FEED_DAILY AS stp
                LEFT JOIN MAPPINGS_DIM_PLAYLIST_TO_FOLLOWERS AS pf USING(stp.playlist_id)
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
                    'followers',ts.followers,
                    'product_id',ts.product_id,
                    'country_code',ts.country_code,
                    'playlist_id',ts.playlist_id,
                    'download_activity_date',ts.download_activity_date,
                    'store_playlist_id',ts.store_playlist_id,
                    'playlist_name',ts.playlist_name,
                    'playlist_url',ts.playlist_url,
                    'playlist_image',ts.playlist_image,
                    'streams',ts.streams
                )
            FROM top_sound as ts
        """)


        all_rows = cs.fetchall()

        query_seconds = time.time() - start
        print("it took for query " + str(query_seconds) + " seconds.")

        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        print('After Query & Before Ingestion')
        print('Current Timestamp : ', timestampStr)

        def gen_data():
            for row in all_rows:
                jsonFromRow = json.loads(row[0])

                yield {
                    "_index": ES_INDEX,
                    "_type": "_doc",
                    "_source": jsonFromRow,
                }

        start = time.time()
        for success, info in parallel_bulk(es, gen_data(), thread_count=5, queue_size=10,
            max_chunk_bytes=20971520, chunk_size=1000):
            if not success:
                print('********************* print_ingest_error **************************')
                print('A document failed:', Pretty(info).print())
            else:
                print('********************* print_ingest_success **************************')
                print('A document success:', Pretty(info).print())

                dateTimeObj = datetime.now()
                timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
                print('After Ingestion success')
                print('Current Timestamp : ', timestampStr)

        seconds = time.time() - start
        print("it took for parallel_bulk " + str(seconds) + " seconds.")
        print("total time "+ str(query_seconds + seconds ))

        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        print('After Ingestion')
        print('Current Timestamp : ', timestampStr)
    finally:
        pass
        # cs.close()

        # ctx.close()

if __name__ == '__main__':
    chunk_limit = 20000000
    data_start = int(sys.argv[1])
    data_end = int(sys.argv[2])
    offser_list_generator = (x for x in range(data_start, data_end, chunk_limit))
    no_of_processes = 20

    with Pool(no_of_processes) as p:
        p.map(push_to_es, offser_list_generator)

    cs.close()
    ctx.close()
