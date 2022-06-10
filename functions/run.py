import pandas as pd
import logging
import time

from .empty_the_existing_data_in_db import empty_the_existing_data_in_db
from .query_metadata import query_metadata
from .calculate_padding_query_detect import calculate_padding_query_detect
from .batch_processing import batch_processing

def run(cf, 
        log_file_name= "logfile.log", 
        log_level=logging.INFO, 
        batch_size_max = "3H",
        batch_size_min = "5T",
        delay_size = "1T",
        time_second_for_sleep = 60):

    batch_time_size_max = pd.Timedelta(batch_size_max)
    batch_time_size_min = pd.Timedelta(batch_size_min),
    delay_time_size = pd.Timedelta(delay_size),

    # initialisation
    
    logging.basicConfig(format='%(message)s', filename=log_file_name, level=log_level)

    empty_the_existing_data_in_db(cf.influxdb, cf.postgresdb)

    df_meta = query_metadata(cf.postgresdb, cf.meta_config, cf.default_config,
                               columns_for_join=cf.meta_columns_for_join, 
                               columns_for_pivot=cf.columns_for_pivot)

    padding_query_detect = calculate_padding_query_detect(df_meta, 
                                                          cf.resample_padding_size, 
                                                          cf.resample_freq)

    start_time = pd.Timestamp(cf.start_time_str, tz="UTC")
    batch_time_size = batch_time_size_max

    start_row_value = None
    df_buffered_rows_for_next_query = None

    # loop 

    while True:
        
        print('batch_time_size: ', batch_time_size)
        print('delay_time_size: ', delay_time_size)
        
        if pd.Timestamp.now(tz='UTC') - (start_time + padding_query_detect) > (batch_time_size+delay_time_size):

            time_range = (start_time, start_time + batch_time_size)

            start_row_value, \
            df_buffered_rows_for_next_query \
            = batch_processing(cf.influxdb, 
                                 cf.postgresdb, 
                                 df_meta, 
                                 time_range, 
                                 cf.resample_freq, 
                                 padding_query_detect,
                                 columns_for_pivot=cf.columns_for_pivot, 
                                 column_for_detect=cf.column_for_detect, 
                                 iot_columns_for_join=cf.iot_columns_for_join, 
                                 meta_columns_for_join=cf.meta_columns_for_join, 
                                 start_row_value=start_row_value, 
                                 df_buffered_rows_for_next_query=df_buffered_rows_for_next_query)

            start_time = start_time + batch_time_size

        else:
            batch_time_size = batch_time_size_min
            time.sleep(time_second_for_sleep)