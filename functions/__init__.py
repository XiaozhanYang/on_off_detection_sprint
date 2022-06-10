from .query_metadata import query_metadata
from .batch_processing import batch_processing

# For testing purposes:
from .batch_processing.db_read_query import db_read_query
from .batch_processing.scan_on_off_from_queried_data import scan_on_off_from_queried_data
from .batch_processing.save_on_off_to_db import save_on_off_to_db

from influxdb import InfluxDBClient
import psycopg2

def empty_the_existing_data_in_db(influxdb, postgresdb):

    # empty the data in influxdb
    
    client_gen3 = InfluxDBClient(host=influxdb.host, port=influxdb.port, database=influxdb.database)

    q = f""" DROP MEASUREMENT "{influxdb.sink_table}" """

    client_gen3.query(q)

    # empty the data in postgresdb
  
    conn = psycopg2.connect(postgresdb.CONNECTION)
    cur = conn.cursor()

    # check whether the table exists

    query_check_existing_tables  = """SELECT table_name 
                                      FROM information_schema.tables
                                      WHERE table_schema = 'public'
                                   """

    cur.execute(query_check_existing_tables)
    list_tables = cur.fetchall()
    table_name_list  = [table_name[0] for table_name in list_tables]

    if postgresdb.table_name_sink in table_name_list:

        # delete existing data
        query_empty_hypertable = f"DELETE FROM {postgresdb.table_name_sink};"
        cur.execute(query_empty_hypertable)
        
    else:
        
        # turn on the timescaledb extension
        list_the_extensions = "CREATE EXTENSION IF NOT EXISTS timescaledb;" # enable the timescaledb extension, so function create_hypertable can be used
        cur.execute(list_the_extensions)
        conn.commit()

        # create timescaledb hypertable
        query_create_normaltable = f"""CREATE TABLE IF NOT EXISTS {postgresdb.table_name_sink} (
                                               time TIMESTAMPTZ NOT NULL,
                                               action INTEGER,
                                               site_name TEXT NULL,
                                               circuit_description TEXT NULL
                                               );"""

        query_create_hypertable = f"SELECT create_hypertable('{postgresdb.table_name_sink}', 'time');"

        cur.execute(query_create_normaltable) 
        cur.execute(query_create_hypertable)

    conn.commit()
    cur.close()