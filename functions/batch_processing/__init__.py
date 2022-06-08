import pandas as pd
import numpy as np
import logging

from .db_read_query import db_read_query
from .scan_on_off_from_queried_data import scan_on_off_from_queried_data
from .save_on_off_to_db import save_on_off_to_db


def add_start_end_rows(df_queried_data, query_end_time, start_row_value=None, column_for_detect='W'):
    
    # add starting row
    
    df_queried_data_with_start = pd.concat([start_row_value, df_queried_data])

    # add ending row

    df_end_row = df_queried_data_with_start.tail(1)

    df_end_row.index = [query_end_time] # the name of index will be removed at this step
    df_end_row.index.name = df_queried_data_with_start.index.name 
    
    df_end_row.loc[df_end_row.index,column_for_detect] = np.NaN

    df_queried_data_with_start_end = pd.concat([df_queried_data_with_start, df_end_row])
    
    # pdb.set_trace()
    
    return df_queried_data_with_start_end 

def update_buffer(df_queried_data_with_start_end, next_query_start_time, 
                     time_column = "time",
                     column_for_detect = 'W',
                     columns_for_pivot = ['site_name', 'asset_type']):
    
    
    index_for_remove = df_queried_data_with_start_end.index < next_query_start_time
    index_for_buffer = df_queried_data_with_start_end.index >= next_query_start_time
    
    df_queried_data_for_remove = df_queried_data_with_start_end.loc[index_for_remove]
    
    df_last_values = df_queried_data_for_remove.sort_index(ascending = True).groupby(columns_for_pivot).last()
    
    df_last_values[time_column] = next_query_start_time
    
    df_last_values_with_time = df_last_values.reset_index().set_index([time_column])

    start_row_value = df_last_values_with_time[[column_for_detect, *columns_for_pivot]]
    
    df_buffered_rows_for_next_query = df_queried_data_with_start_end.loc[index_for_buffer]

    return start_row_value, df_buffered_rows_for_next_query

def batch_processing(influxdb, postgresdb, df_meta, time_range, 
                     resample_freq,
                     padding_query_detect, 
                    #  cref=130,
                    #  expiration_time_margins=[20, 240],
                     columns_for_pivot=['site_name', 'asset_type'], 
                     column_for_detect='W', 
                     iot_columns_for_join=['nid', 'channel'],
                     meta_columns_for_join=['nid', 'channel_number'],
                     start_row_value=None, 
                     df_buffered_rows_for_next_query=None):
    
    detect_start_time, detect_end_time = time_range
    
    print(time_range[0].strftime("%Y-%m-%d %H:%M:%S"), end=", ") # "%Y-%m-%d %H:%M:%S"
    
    log_text = time_range[0].strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(log_text)
    
    query_end_time = detect_end_time + padding_query_detect
    
    if df_buffered_rows_for_next_query is not None: # check whether df_buffered_rows_for_next_query has been defined
        query_start_time = df_buffered_rows_for_next_query.index.max()
        df_queried_data = db_read_query(influxdb, query_start_time, query_end_time, df_meta, 
                                          meta_columns_for_join=meta_columns_for_join, 
                                          iot_columns_for_join=iot_columns_for_join)
        df_queried_data = pd.concat([df_buffered_rows_for_next_query, df_queried_data])
    else:
        query_start_time = detect_start_time - padding_query_detect
        df_queried_data = db_read_query(influxdb, query_start_time, query_end_time, df_meta, 
                                          meta_columns_for_join=meta_columns_for_join, 
                                          iot_columns_for_join=iot_columns_for_join)

    if start_row_value is not None: # check whether start_row_value has been defined
        df_queried_data_with_start_end = add_start_end_rows(df_queried_data, query_end_time, 
                                                              start_row_value=start_row_value, 
                                                              column_for_detect=column_for_detect) # where to use the query_start_time
    else:
        df_queried_data_with_start_end = add_start_end_rows(df_queried_data, query_end_time, 
                                                              column_for_detect=column_for_detect)

    df_scanned_on_off_actions = scan_on_off_from_queried_data(df_queried_data_with_start_end, detect_start_time, detect_end_time, 
                    # cref = cref,
                    # expiration_time_margins = expiration_time_margins,
                    resample_freq=resample_freq, 
                    columns_for_pivot=columns_for_pivot, 
                    column_for_detect=column_for_detect)
    
    if df_scanned_on_off_actions.shape[0] > 0:
        
        save_on_off_to_db(influxdb, postgresdb, df_scanned_on_off_actions, columns_for_pivot)
        
        log_text_df = df_scanned_on_off_actions.to_string()
    
        logging.info('\n' + log_text_df + '\n')
        
    next_query_start_time = detect_end_time - padding_query_detect
     
    start_row_value, df_buffered_rows_for_next_query = update_buffer(df_queried_data_with_start_end, next_query_start_time, 
                                                                     column_for_detect = column_for_detect,
                                                                     columns_for_pivot = columns_for_pivot)
    
    return start_row_value, df_buffered_rows_for_next_query
