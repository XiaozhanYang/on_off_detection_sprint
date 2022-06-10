import pandas as pd

def calculate_padding_query_detect(df_meta, resample_padding_size, resample_freq):

    expiration_time_margins = [int(df_meta.expiration_time_low.max()), int(df_meta.expiration_time_high.max())]
    scan_padding_size = sum(expiration_time_margins) + 2 # expiration_time_margins are for the rolling window function, 1 is for diff function

    padding_query_detect = (scan_padding_size + resample_padding_size) * pd.Timedelta(resample_freq)
    
    return padding_query_detect