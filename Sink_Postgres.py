from typing import List, Dict, Any
import psycopg
from pathlib import Path
from Sql_Commands import Insert_Into_Cdc_Events, Create_Cdv_Events_Table



# create table if it doesn't already exist
def Create_Cdc_Table(dsn):
    sql_command = Create_Cdv_Events_Table()
    
    try:
        with psycopg.connect(dsn) as cx:
            with cx.cursor() as cur:
                cur.execute(sql_command)
            cx.commit()
            
    except Exception as e:
        raise Exception(f"Failed to create CDC table: {e}")


# example: upsert into a pg server, keyed by (table, primary_key, commit_lsn)
# data should already be formatted, and this transforms the wal data into insert statements
# data: List[Dict[str, Any]]
# sql uses "ON CONFLICT DO NOTHING"
# this is basically a staging table, I'll need to choose if I handle each event myself but they'll all be there
def Apply_Postgres(dsn, data):
    insert_sql = Insert_Into_Cdc_Events()

    #print(f"DEBUG: Connecting to Sink DB with DSN: {dsn.replace(dsn.split('password=')[1].split()[0], '*****') if 'password=' in dsn else dsn}")
    try:
        # Use synchronous connection to avoid ProactorEventLoop issues on Windows
        with psycopg.connect(dsn, connect_timeout=5) as cx:
            with cx.cursor() as cur:
                for event in data:
                    if event["type"] == "insert":
                        # example upsert; adapt to your schema
                        cur.execute(insert_sql,
                                         (event["table"], event["pk"], event["commit_lsn"], event["payload_json"]))
                cx.commit()

    except psycopg.OperationalError as e:
        print(f"ERROR: Failed to connect to Sink DB: {e}")
        raise e

    except Exception as e:
        print(f"ERROR: Unexpected error in Apply_Postgres: {e}")
        raise e
