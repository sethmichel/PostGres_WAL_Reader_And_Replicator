from typing import List, Dict, Any
import psycopg
from pathlib import Path
from Sql_Commands import Insert_Into_Cdc_Events, Create_Cdv_Events_Table


sink_db_conn = None # connection to the sink pg db


# returns connection to the sink table
def Get_Sink_Db_Conn(dsn):
    global sink_db_conn
    
    try:
        sink_db_conn = psycopg.connect(dsn)

        return sink_db_conn
                
    except Exception as e:
        raise Exception(f"Failed to connect to sink database: {e}")


# create table if it doesn't already exist
def Create_Cdc_Table():
    global sink_db_conn
    
    if sink_db_conn is None:
        raise Exception("Sink database connection not initialized. Call Get_Sink_Db_Conn() first.")
    
    sql_command = Create_Cdv_Events_Table()
    
    try:
        with sink_db_conn.cursor() as cur:
            cur.execute(sql_command)

        sink_db_conn.commit()

    except Exception as e:
        raise Exception(f"Failed to create CDC table: {e}")


# example: upsert into a pg server, keyed by (table, primary_key, commit_lsn)
# data should already be formatted, and this transforms the wal data into insert statements
# data: List[Dict[str, Any]]
# sql uses "ON CONFLICT DO NOTHING"
# this is basically a staging table, I'll need to choose if I handle each event myself but they'll all be there
async def Apply_Postgres(dsn, data):
    insert_sql = Insert_Into_Cdc_Events()

    async with await psycopg.AsyncConnection.connect(dsn) as cx:
        async with cx.cursor() as cur:
            for event in data:
                if event["type"] == "insert":
                    # example upsert; adapt to your schema
                    await cur.execute(insert_sql,
                                     (event["table"], event["pk"], event["commit_lsn"], event["payload_json"]))
            await cx.commit()
