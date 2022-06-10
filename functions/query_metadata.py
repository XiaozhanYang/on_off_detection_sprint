import psycopg2
import pandas as pd


def extract_columns(meta_config):
    columns_for_query = set()
    for meta_config_record in meta_config:
        for select_slice in meta_config_record['select']:
            columns_for_query = columns_for_query.union(set(select_slice))
        for exclude_slice in meta_config_record['exclude']:
            columns_for_query = columns_for_query.union(set(exclude_slice))
            
    return list(columns_for_query)

def meta_select_config(df_meta, meta_config, default_config):
    
    index_whole = False

    for meta_config_record in meta_config:
        # can be a generic function in the next step
        index_select = False
        for select_slice in meta_config_record['select']:
            index_select_slice = True
            for column in select_slice:
                index_select_slice &= (df_meta[column].isin(select_slice[column]))
            index_select |= index_select_slice   

        index_exclude = False
        for exclude_slice in meta_config_record['exclude']:
            index_exclude_slice = True
            for column in exclude_slice:
                index_exclude_slice &= (df_meta[column].isin(exclude_slice[column]))
            index_exclude |= index_exclude_slice

        index_record = index_select & (~index_exclude)

        for param in default_config:
            if param in meta_config_record['config']:
            # also add the config from default
                df_meta.loc[index_record, param] = meta_config_record['config'][param]
            elif param in df_meta.columns:
                df_meta.loc[index_record, param] = df_meta.loc[index_record, param].fillna(default_config[param])
            else:
                df_meta.loc[index_record, param] = default_config[param]

        index_whole |= index_record

    df_meta_select_config = df_meta.loc[index_whole]

    for param in default_config:
        if df_meta_select_config[param].mod(1).max() == 0:
            df_meta_select_config.loc[df_meta_select_config.index, param] = df_meta_select_config[param].astype(int)
    
    return df_meta_select_config

def query_metadata(db, meta_config, default_config,
                   columns_for_pivot = ['site_name', 'circuit_description'],
                   columns_for_join = ['nid', 'channel_number']):
    
    # assets_list = []
    # for assets_from_selected_site in concerned_assets_from_sites:
    #     assets_from_selected_site["site_name"]
    #     for asset_type in assets_from_selected_site["asset_type"]:
    #         assets_list.append(f"""('{assets_from_selected_site["site_name"]}', '{asset_type}')""")
            
    columns_for_config = extract_columns(meta_config)

    columns_for_query = set(columns_for_config).union(set(columns_for_pivot),set(columns_for_join))
    columns_for_query_str = ', '.join(columns_for_query)
        
    # "Dishwasher", "Pizza Oven", "Intruder Alarm"
    query  = f"""SELECT {columns_for_query_str} from {db.table_name};"""

    conn = psycopg2.connect(db.CONNECTION)

    data_cursor = conn.cursor()

    data_cursor.execute(query)
    resp = data_cursor.fetchall()

    df_meta = pd.DataFrame(resp, columns=columns_for_query)

    conn.close()
    
    for column in columns_for_join:
        df_meta[column] = df_meta[column].astype(str)
    
    df_meta_select_config = meta_select_config(df_meta, meta_config, default_config)

    return df_meta_select_config[[*columns_for_join, *columns_for_pivot, *default_config]]
