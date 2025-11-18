import sqlite3
from pathlib import Path
from typing import Optional
from Sql_Commands import Create_LSN_Offset_Table, Get_Last_Applied_Lsn, Set_Last_Applied_Lsn


'''
this file handles the last_applied_lsn. it moves it around and saves/loads it

"Offset store" is the tiny local database or file that remembers the last wal change I successfully processed. 
Like an sql lite with 1 table 2 columns. Python updates this table every time it sends data. Then when I 
restart I read that number back

this file will store and get my last_applied_lsn
'''

last_lsn_db_conn = None # connection to sqlite where we store the last applied lsn


# returns connection to the sqlite table holding the lsn values
# creates the table if it doesn't exist (safe to run at startup)
def Get_Lsn_Table_Conn(passed_path):
    global last_lsn_db_conn

    Path(passed_path).parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # this creates the file if it doesn't exist
        last_lsn_db_conn = sqlite3.connect(passed_path)
        
        # Create table if it doesn't exist
        create_offset_table_sql = Create_LSN_Offset_Table()
        last_lsn_db_conn.execute(create_offset_table_sql)
        last_lsn_db_conn.commit()
                
    except sqlite3.Error as e:
        raise Exception(f"Failed to connect to SQLite database at {passed_path}: {e}")


# gets most recent lsn for a given replication slot
# called at startup/restart
def Get_Last_Applied_Lsn(slot_name):
    global last_lsn_db_conn
    
    get_lsn_sql = Get_Last_Applied_Lsn(slot_name)
    row = last_lsn_db_conn.execute(get_lsn_sql, (slot_name,)).fetchone()

    if (row):
        return row[0]
    else:
        return None


# save lsn after using it
# called everyt ime my sink successfully commits a batch
def Set_Last_Applied_Lsn(slot_name, lsn):
    global last_lsn_db_conn
    
    set_lsn_sql = Set_Last_Applied_Lsn()
    last_lsn_db_conn.execute(set_lsn_sql, (slot_name, lsn))
    last_lsn_db_conn.commit()
