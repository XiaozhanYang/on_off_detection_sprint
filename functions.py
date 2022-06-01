import pandas as pd # import the data as dataframe and manipulate the data

import numpy as np
from influxdb import InfluxDBClient, DataFrameClient
import time

import pdb
import logging

from .query_metadata import query_metadata

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
    
    df_last_values = df_queried_data_for_remove.sort_index(ascending = True).groupby(["asset_type"]).last()
    
    df_last_values["time"] = next_query_start_time
    
    df_last_values_with_time = df_last_values.reset_index().set_index([time_column])

    start_row_value = df_last_values_with_time[[column_for_detect, *columns_for_pivot]]
    
    df_buffered_rows_for_next_query = df_queried_data_with_start_end.loc[index_for_buffer]

    return start_row_value, df_buffered_rows_for_next_query
    
def query_iot_data(db, query_start_time, query_end_time, nid, channels_list=None):
    
    query_start_time_str = query_start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    query_end_time_str   = query_end_time.strftime("%Y-%m-%d %H:%M:%S.%f")

    client_gen3 = InfluxDBClient(host=db.host, port=db.port, database=db.database)
    
    try:
        channels_str = "|".join(channels_list)
        q = f""" SELECT W, nid, channel FROM "GEN3/C" WHERE ("nid" = '{nid}') and ("channel" =~ /^({channels_str})$/) and (time >= '{query_start_time_str}') and (time < '{query_end_time_str}')"""

    except:
        
        q = f""" SELECT W, nid, channel FROM "GEN3/C" WHERE ("nid" = '{nid}') and (time >= '{query_start_time_str}') and (time < '{query_end_time_str}')"""
    
    results_gen3 = client_gen3.query(q)

    df = pd.DataFrame(results_gen3.get_points())
    
    # df_ti = df.set_index('time')
    if df.shape[0] > 0:
        return df
    else:
        df = pd.DataFrame({'time': pd.Series(dtype='str'),
                       'W': pd.Series(dtype='float'),
                       'nid': pd.Series(dtype='str'),
                          'channel':pd.Series(dtype='str')})
        return df


def db_read_query(db, query_start_time, query_end_time, df_meta, 
                  iot_columns_for_join = ['nid', 'channel'],
                  meta_columns_for_join = ['nid', 'channel_number']):
    
    nids_groups = df_meta.groupby([meta_columns_for_join[0]])
    
    df_iot_all = pd.DataFrame([])
    
    for nid, groups in nids_groups:
        
        df_iot_nid = query_iot_data(db, query_start_time, query_end_time, nid, channels_list=groups[meta_columns_for_join[1]].to_list())
        df_iot_all = pd.concat([df_iot_all, df_iot_nid])
        
    
    columns_for_join = iot_columns_for_join+meta_columns_for_join
    df_joined = df_iot_all.merge(df_meta, left_on=iot_columns_for_join, right_on=meta_columns_for_join).drop(columns=columns_for_join)


    df_joined.time = pd.to_datetime(df_joined.time)
    df_joined_ti = df_joined.set_index('time')

    return df_joined_ti

def scan_on_off_from_queried_data(df_queried_data_with_start_end, detect_start_time, detect_end_time, 
                resample_freq="1T", 
                columns_for_pivot=['site_name', 'asset_type'], 
                column_for_detect="W"):

    df_pivot_and_resampled_data = pivot_and_resample(df_queried_data_with_start_end, 
                                                      resample_freq = resample_freq, 
                                                      columns_for_pivot = columns_for_pivot, 
                                                      column_for_detect = column_for_detect)


    main_assets_with_config = df_pivot_and_resampled_data.columns.tolist()
    columns_for_pivot = df_pivot_and_resampled_data.columns.names[:-3]

    df_on_off_actions_export = pd.DataFrame([])

    for asset_name_with_config in main_assets_with_config:

        sr_watt = df_pivot_and_resampled_data[asset_name_with_config]

        sr_on_off_actions_scanned = detect_on_off(sr_watt, detect_start_time, detect_end_time)
        if sr_on_off_actions_scanned.shape[0] > 0:
            df_on_off_actions_scanned = sr_to_df(sr_on_off_actions_scanned, columns_for_pivot=columns_for_pivot)
            df_on_off_actions_export = pd.concat([df_on_off_actions_export, df_on_off_actions_scanned])

    return df_on_off_actions_export

def pivot_and_resample(df_joined_ti_selected_concat, 
              resample_freq="1T", 
              columns_for_pivot=['site_name', 'asset_type'],
              columns_for_config=['cref','expiration_time_low','expiration_time_high'], 
              column_for_detect="W"):
    
    # filling missing grouper windows by adding resample() function
    # we need to do this, because the groupby with additional columns will not produce the missing data points
    df_joined_pivot_selected_concat = df_joined_ti_selected_concat.groupby([pd.Grouper(freq=resample_freq), 
                                                                            *columns_for_pivot, *columns_for_config]
                                                                          )[column_for_detect] \
                                        .max().unstack(level=0) \
                                        .T.resample(resample_freq).ffill().ffill()
    # pdb.set_trace()
    return df_joined_pivot_selected_concat

def detect_on_off(sr_watt, start_time, end_time):

    # Get configuration from the sr_watt series
    cref = sr_watt.name[-3]    
    expiration_time_margins = [int(exp_value) for exp_value in sr_watt.name[-2:]]
    
    sr_watt_gt = sr_watt.gt(cref).astype('int32')
   
    sr_watt_gt_rmax_shift_rmin = sr_watt_gt.rolling(expiration_time_margins[1]).max()\
                                              .shift(1-expiration_time_margins[1])\
                                              .rolling(expiration_time_margins[1]).min()
    
    sr_watt_gt_rmax_shift_rmin_rmin_shift_rmax = sr_watt_gt_rmax_shift_rmin.rolling(expiration_time_margins[0]).min()\
                                                           .shift(1-expiration_time_margins[0])\
                                                           .rolling(expiration_time_margins[0]).max()

    sr_watt_gt_rmax_shift_rmin_rmin_shift_rmax_diff = sr_watt_gt_rmax_shift_rmin_rmin_shift_rmax.diff()
    
    index_on_off =  (sr_watt_gt_rmax_shift_rmin_rmin_shift_rmax_diff != 0)
    sr_on_off_actions = sr_watt_gt_rmax_shift_rmin_rmin_shift_rmax_diff.loc[index_on_off].dropna() # select only the on and off actions
    
    index_scan_window = (sr_on_off_actions.index >= start_time) & (sr_on_off_actions.index < end_time)
    sr_on_off_actions_scanned = sr_on_off_actions.loc[index_scan_window]
    
    # To be removed (Was used for debugging)
    if sr_on_off_actions_scanned.shape[0]>0:
        
        global df_tests
        df_test = sr_on_off_actions
        # pdb.set_trace()
    
    return sr_on_off_actions_scanned

def sr_to_df(sr_on_off_actions_scanned, columns_for_pivot=['site_name', 'asset_type'], action_column="action"):
    
    df_on_off_actions_frame = sr_on_off_actions_scanned.to_frame()
    time_column = sr_on_off_actions_scanned.index.name

    df_on_off_actions_frame.columns = df_on_off_actions_frame.columns.droplevel([-3,-2,-1]).set_names(columns_for_pivot)
    
    print("\n", df_on_off_actions_frame, "\n")

    df_on_off_actions_export = df_on_off_actions_frame.T.stack().rename(action_column).reset_index()[[time_column, action_column, *columns_for_pivot]]
    
    return df_on_off_actions_export.set_index(time_column)

def save_on_off_to_db(db, df_for_export, columns_for_tag):

    client_test = DataFrameClient(host=db.host, port=db.port, database=db.database)
    client_test.write_points(df_for_export, db.sink_table, 
                                    tag_columns=columns_for_tag, 
                                    batch_size=10000,
                                    time_precision='ms')

def batch_processing(db, df_meta, time_range, 
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
        df_queried_data = db_read_query(db, query_start_time, query_end_time, df_meta, 
                                          meta_columns_for_join=meta_columns_for_join, 
                                          iot_columns_for_join=iot_columns_for_join)
        df_queried_data = pd.concat([df_buffered_rows_for_next_query, df_queried_data])
    else:
        query_start_time = detect_start_time - padding_query_detect
        df_queried_data = db_read_query(db, query_start_time, query_end_time, df_meta, 
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
        
        save_on_off_to_db(db, df_scanned_on_off_actions, columns_for_pivot)
        
        log_text_df = df_scanned_on_off_actions.to_string()
    
        logging.info('\n' + log_text_df + '\n')
        
    next_query_start_time = detect_end_time - padding_query_detect
     
    start_row_value, df_buffered_rows_for_next_query = update_buffer(df_queried_data_with_start_end, next_query_start_time)
    
    return start_row_value, df_buffered_rows_for_next_query
