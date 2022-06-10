import pandas as pd

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