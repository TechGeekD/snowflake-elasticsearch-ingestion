import sys
import json
import calendar
from datetime import datetime
from elastic import es, ElasticsearchException
from snowflake_client import ctx, cs
from utils import Pretty
from constants import ES_DOMAIN, ES_INDEX
from elasticsearch.helpers import parallel_bulk
import time
from multiprocessing import Pool

print('elasticsearch-ingest:')
try:
    start_dateStr = sys.argv[3]
    end_dateStr = sys.argv[4]
except:
    print("Provide Date Range: start_date & end_date")

try:
    chunk_limit = int(sys.argv[2])
except:
    print('Provide Values: limit')

def push_to_es(offset=0):
    print(f"\n{offset}: PUSH_TO_ES_START")

    jsonFromRowData = []

    try:
        limit = chunk_limit

        start = time.time()

        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        print(f'\n{offset}: Before Query Current Timestamp: ', timestampStr)

        cs.execute(f"""
            WITH top_sound AS (
                SELECT stp.*, pf.followers
                FROM STREAMS_BY_TRACK_PLAYLIST_COUNTRY_FEED_DAILY AS stp
                LEFT JOIN MAPPINGS_DIM_PLAYLIST_TO_FOLLOWERS AS pf USING(stp.playlist_id)
                WHERE stp.DOWNLOAD_ACTIVITY_DATE between '{start_dateStr}' and '{end_dateStr}'
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
        print(f"\n{offset}: it took for query {str(query_seconds)} seconds for {len(all_rows)} rows.")

        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        print(f'{offset}: After Query & Before Ingestion {timestampStr} :')

        def gen_data():
            for row in all_rows:
                jsonFromRow = json.loads(row[0])
                parsed_date = datetime.strptime(jsonFromRow['download_activity_date'], "%Y-%m-%d")
                index_date = f"{parsed_date.year}-{parsed_date.month}"
                index_name = f'{ES_INDEX}-{index_date}'
                # print(f"\n{index_name}: is index name")

                yield {
                    "_index": index_name,
                    "_type": "_doc",
                    "_source": jsonFromRow,
                }

        start = time.time()
        for success, info in parallel_bulk(es, gen_data(), thread_count=5, queue_size=10,
            max_chunk_bytes=20971520, chunk_size=1000):
            if not success:
                print('********************* print_ingest_error **************************')
                print('A document failed:', Pretty(info).print())
            # else:
                # print('********************* print_ingest_success **************************')
                # print('A document success:', Pretty(info).print())

                # dateTimeObj = datetime.now()
                # timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
                # print('After Ingestion success')
                # print('Current Timestamp : ', timestampStr)

        seconds = time.time() - start

        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        print(f"\n{offset}: it took for parallel_bulk {str(seconds)} seconds.")
        print(f"{offset}: total time " + str(query_seconds + seconds))
        print(f'{offset}: After Ingestion {timestampStr}')

        print(f"\n{offset}: PUSH_TO_ES_END")

    except Exception as err:
        print(f'\n{offset}: PUSH_TO_ES_Exception')
        print(err)


def delete_index_to_be_ingested(start_dateTimeObj, end_dateTimeObj):
    print(f"DELETE_INDEX_TO_BE_INGESTED_STARTED")

    if(end_dateTimeObj < start_dateTimeObj):
        print(f"\nEND_DATE: '{end_dateTimeObj}' should be >= START_DATE: '{start_dateTimeObj}'\n")
        sys.exit()

    idx_start_month = start_dateTimeObj.month
    idx_start_year = start_dateTimeObj.year

    idx_end_month = end_dateTimeObj.month
    idx_end_year = end_dateTimeObj.year

    last_date_of_month = calendar.monthrange(idx_end_year, idx_end_month)[1]

    start_dateStr = datetime(
        idx_start_year,
        idx_start_month,
        1
    ).strftime("%Y-%m-%d")

    end_dateStr = datetime(
        idx_end_year,
        idx_end_month,
        last_date_of_month
    ).strftime("%Y-%m-%d")

    while True:
        index_date = f'{idx_start_year}-{idx_start_month}'
        index_name = f'{ES_INDEX}-{index_date}'
        try:
            res = es.indices.delete(index=index_name, ignore_unavailable=True)
            print(res, f"\tDeletion of index : {index_name}")
        except Exception as error:
            print("DELETE_INDEXES_Exception")
            print(error)

        try:
            res = es.indices.create(
                index=index_name,
                body={
                    'settings': {
                        'number_of_shards': 5,
                        'number_of_replicas': 0,
                        'refresh_interval': '-1'
                    },
                })
            print(res, f"\creation of index : {index_name}")
        except Exception as error:
            print("CREATE_INDEXES_Exception")
            print(error)

        if(idx_start_month == idx_end_month and idx_start_year == idx_end_year):
            print("break")
            break

        idx_start_month += 1
        if((idx_start_month % 13) == 0):
            idx_start_year += 1
            idx_start_month = 1

    print(f"DELETE_INDEX_TO_BE_INGESTED_ENDED")

if __name__ == '__main__':
    full_start = time.time()
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print(f"Start full run {timestampStr}")

    delete_index_to_be_ingested(
        datetime.strptime(start_dateStr, "%Y-%m-%d"),
        datetime.strptime(end_dateStr, "%Y-%m-%d")
    )

    cs.execute(f"""
        SELECT COUNT(*)
        FROM STREAMS_BY_TRACK_PLAYLIST_COUNTRY_FEED_DAILY AS stp
        WHERE stp.DOWNLOAD_ACTIVITY_DATE BETWEEN '{start_dateStr}' AND '{end_dateStr}'
    """)

    data_count = cs.fetchone()[0]
    print('Total document count :', data_count)

    try:
        data_start = int(sys.argv[1])
    except:
        print('Provide Values: offset')
        sys.exit()

    data_end = data_count
    offser_list_generator = (x for x in range(data_start, data_end, chunk_limit))
    no_of_processes = 20

    try:
        print(f'POOL_START_{data_start}_{data_end}_{chunk_limit}')

        with Pool(no_of_processes) as p:
            p.map(push_to_es, offser_list_generator)

        print(f'POOL_END_{data_start}_{data_end}_{chunk_limit}')

        index_name = f'{ES_INDEX}-*'
        try:
            res = es.indices.put_settings(
                index=index_name,
                body={
                'index': {
                    'number_of_replicas': 1,
                    'refresh_interval': None
                },
            })
            print(res, f"\nreset index for query : {index_name}")
        except Exception as error:
            print("RESET_INDEXES_Exception")
            print(error)

    except Exception as error:
        print('\nPOOL_Exception')
        print(error)

    cs.close()
    ctx.close()
    full_seconds = time.time() - full_start
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print(f"Start full run {timestampStr}")
    print(f"\nTime for full run {str(full_seconds)} seconds.")
