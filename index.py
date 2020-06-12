import time
import calendar
from datetime import datetime
from multiprocessing import Pool

from utils.logger import Logger
from connectors.elasticsearch import elasticsearch_client
from connectors.snowflake import snowflake_ctx, snowflake_cursor

from constants import ES_DOMAIN, ES_INDEX, INGEST_CONFIG

from  methods import elasticsearch_methods as es_methods

def ingest_data(start_dateStr, end_dateStr):

    chunk_limit = INGEST_CONFIG['CHUNK_LIMIT']
    offset_start = INGEST_CONFIG['OFFSET']
    no_of_processes = INGEST_CONFIG['NO_OF_PROCESSES']

    es_methods.prepare_index_to_be_ingested(
        datetime.strptime(start_dateStr, '%Y-%m-%d'),
        datetime.strptime(end_dateStr, '%Y-%m-%d')
    )

    Logger(f'Start date {start_dateStr}, End date {end_dateStr}').log_info()

    if INGEST_CONFIG['MAX_DATA_LIMIT'] > 0:
        data_count_to_ingest = INGEST_CONFIG['MAX_DATA_LIMIT']
    else:
        data_count_to_ingest = es_methods.fetch_data_count_from_snowflake(
            start_dateStr,
            end_dateStr
            )

    offset_end = data_count_to_ingest

    offset_list_generator = ({
        'offset': offset,
        'start_dateStr': start_dateStr,
        'end_dateStr': end_dateStr
        } for offset in range(offset_start, offset_end, chunk_limit))

    try:
        Logger(f'POOL_START_{offset_start}_{chunk_limit}_{offset_end}').log_info()

        with Pool(no_of_processes) as p:
            p.map(es_methods.push_to_es, offset_list_generator)

        Logger(f'POOL_END_{offset_start}_{chunk_limit}_{offset_end}').log_info()

        es_methods.reset_index_settings()

    except Exception as error:
        Logger('POOL').log_exception(error)

def ingest_by_month(start_dateStr, end_dateStr):
    start_dateTimeObj = datetime.strptime(start_dateStr, '%Y-%m-%d')
    end_dateTimeObj = datetime.strptime(end_dateStr, '%Y-%m-%d')

    idx_start_month = start_dateTimeObj.month
    idx_start_year = start_dateTimeObj.year
    # idx_start_day = start_dateTimeObj.day

    idx_end_month = end_dateTimeObj.month
    idx_end_year = end_dateTimeObj.year
    # idx_end_day = end_dateTimeObj.day

    while True:
        last_date_of_month = calendar.monthrange(idx_start_year, idx_start_month)[1]

        start_dateStr = f'{idx_start_year}-{idx_start_month}-01'
        end_dateStr = f'{idx_start_year}-{idx_start_year}-{last_date_of_month}'

        Logger(f'**** START: ingest_data {start_dateStr} {end_dateStr} ****').log_info()
        ingest_data(start_dateStr, end_dateStr)
        Logger(f'**** END: ingest_data {start_dateStr} {end_dateStr} ****').log_info()

        if(idx_start_month == idx_end_month and idx_start_year == idx_end_year):
            break

        # +1 till 12 month then +1 year & reset month to 1
        idx_start_month += 1
        if((idx_start_month % 13) == 0):
            idx_start_year += 1
            idx_start_month = 1

def main():
    full_start = time.time()
    Logger('START: elasticsearch-analytics-ingest:').log_info()
    Logger('Start full run').log_time()

    start_dateStr = INGEST_CONFIG['START_DATE']
    end_dateStr = INGEST_CONFIG['END_DATE']

    if not start_dateStr and not end_dateStr:
         Logger(f'Provide valid value: Start date & End date').log_info()

    ingest_by_month(start_dateStr, end_dateStr)

    full_seconds = time.time() - full_start
    Logger('End full run').log_time()
    Logger(f'Time for full run {str(full_seconds)} seconds.').log_info()

    snowflake_cursor.close()
    snowflake_ctx.close()

    full_seconds = time.time() - full_start
    Logger(f'Time for script {str(full_seconds)} seconds.').log_info()
    Logger('END: elasticsearch-analytics-ingest:').log_info()

if __name__ == '__main__':
    main()
