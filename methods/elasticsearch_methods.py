import sys
import time
import calendar
from datetime import datetime
import json

from elasticsearch.helpers import parallel_bulk

from utils.logger import Logger
from utils.pretty import Pretty

from connectors.elasticsearch import elasticsearch_client
from connectors.snowflake import snowflake_cursor

from constants import ES_INDEX, INGEST_CONFIG

def push_to_es(offset_gen):
    offset = offset_gen['offset']
    start_dateStr = offset_gen['start_dateStr']
    end_dateStr = offset_gen['end_dateStr']

    Logger(f'{offset}: PUSH_TO_ES_START').log_info()

    try:
        limit = INGEST_CONFIG['CHUNK_LIMIT']

        start = time.time()

        Logger(f'{offset}: Before Query Current Timestamp').log_time()

        snowflake_cursor.execute(f"""
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


        all_rows = snowflake_cursor.fetchall()

        query_seconds = time.time() - start

        Logger(f'{offset}: it took for query {str(query_seconds)} seconds for {len(all_rows)} rows.').log_info()
        Logger(f'{offset}: After Query & Before Ingestion').log_time()

        def gen_data():
            for row in all_rows:
                jsonFromRow = json.loads(row[0])
                parsed_date = datetime.strptime(jsonFromRow['download_activity_date'], '%Y-%m-%d')

                index_date = f'{parsed_date.year}-{parsed_date.month}'
                index_name = f'{ES_INDEX}-{index_date}'

                yield {
                    '_index': index_name,
                    '_type': '_doc',
                    '_source': jsonFromRow,
                }

        start = time.time()
        for success, info in parallel_bulk(elasticsearch_client, gen_data(), thread_count=5, queue_size=10,
            max_chunk_bytes=20971520, chunk_size=1000):
            if not success:
                error = Pretty(info).print()
                Logger(f'{offset}: A document failed s {error} A document failed e').log_info()

        seconds = time.time() - start

        full_push_time = query_seconds + seconds
        Logger(f'{offset}: it took for parallel_bulk {str(seconds)} seconds.').log_info()
        Logger(f'{offset}: total time {full_push_time}').log_info()

        Logger(f'{offset}: After Ingestion').log_time()

        Logger(f'{offset}: PUSH_TO_ES_END').log_info()

    except Exception as error:
        Logger(f'{offset}: PUSH_TO_ES').log_exception(error)

def recreate_index(index_name):
    Logger(f'START: recreate_index: {index_name}').log_info()
    try:
        res = elasticsearch_client.indices.delete(index=index_name, ignore_unavailable=True)
        Logger(f'\t{res} Deletion of index : {index_name}').log_info()
    except Exception as error:
        Logger('DELETE_INDEXES').log_exception(error)

    try:
        res = elasticsearch_client.indices.create(
            index=index_name,
            body={
                'settings': {
                    'number_of_shards': 5,
                    'number_of_replicas': 0,
                    'refresh_interval': '-1',
                    'codec': 'best_compression',
                }
            })
        Logger(f'\t{res} Creation of index : {index_name}').log_info()
    except Exception as error:
        Logger('CREATE_INDEXES').log_exception(error)

def prepare_index_to_be_ingested(start_dateTimeObj, end_dateTimeObj):
    Logger('PREPARE_INDEX_TO_BE_INGESTED_START').log_info()

    if(end_dateTimeObj < start_dateTimeObj):
        Logger(f'END_DATE: {end_dateTimeObj} should be >= START_DATE: {start_dateTimeObj}').log_info()
        sys.exit()

    idx_start_month = start_dateTimeObj.month
    idx_start_year = start_dateTimeObj.year

    idx_end_month = end_dateTimeObj.month
    idx_end_year = end_dateTimeObj.year

    index_date = f'{idx_start_year}-{idx_start_month}'
    index_name = f'{ES_INDEX}-{index_date}'

    recreate_index(index_name)

    Logger('PREPARE_INDEX_TO_BE_INGESTED_END').log_info()

def fetch_data_count_from_snowflake(start_dateStr, end_dateStr):
    snowflake_cursor.execute(f"""
        SELECT COUNT(*)
        FROM STREAMS_BY_TRACK_PLAYLIST_COUNTRY_FEED_DAILY AS stp
        WHERE stp.DOWNLOAD_ACTIVITY_DATE BETWEEN '{start_dateStr}' AND '{end_dateStr}'
    """)

    document_count = snowflake_cursor.fetchone()[0]
    Logger(f'Total document count: {document_count}').log_info()

    return document_count

def reset_index_settings():
    index_name = f'{ES_INDEX}-*'
    try:
        res = elasticsearch_client.indices.put_settings(
            index=index_name,
            body={
            'index': {
                'number_of_replicas': 1,
                'refresh_interval': None
            },
        })

        Logger(f'{res} Reset index for query : {index_name}').log_info()

    except Exception as error:
        Logger('RESET_INDEXES').log_exception(error)
