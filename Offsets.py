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


# make sqllite table if it doesn't exist
def Ensure_Offsets_Store(passed_path):
    create_offset_table_sql = Create_LSN_Offset_Table()

    Path(passed_path).parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(passed_path) as cx:
        cx.execute(create_offset_table_sql)
        cx.commit()


# gets most recent lsn for a given replication slot
# called at startup/restart
def Get_Last_Applied_Lsn(passed_path, slot_name):
    get_lsn_sql = Get_Last_Applied_Lsn(slot_name)

    with sqlite3.connect(passed_path) as cx:
        row = cx.execute(get_lsn_sql, (slot_name,)).fetchone()

        if (row):
            return row[0]
        else:
            return None


# save lsn after using it
# called everyt ime my sink successfully commits a batch
def Set_Last_Applied_Lsn(passed_path, slot_name, lsn):
    set_lsn_sql = Set_Last_Applied_Lsn()

    with sqlite3.connect(passed_path) as cx:
        cx.execute(set_lsn_sql, (slot_name, lsn))
        cx.commit()
