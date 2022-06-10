from influxdb import DataFrameClient
from sqlalchemy import create_engine

def save_on_off_to_db(influxdb, postgresdb, df_for_export, columns_for_tag):

    # for influxdb
    client_test = DataFrameClient(host=influxdb.host, port=influxdb.port, database=influxdb.database)
    client_test.write_points(df_for_export, influxdb.sink_table, 
                                    tag_columns=columns_for_tag, 
                                    batch_size=10000,
                                    time_precision='ms')
    # for postgresdb
    engine = create_engine(postgresdb.ENGINE)
    df_for_export.to_sql(postgresdb.table_name_sink, con=engine, if_exists='append', index=True)
