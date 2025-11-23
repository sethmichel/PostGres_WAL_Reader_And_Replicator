
# NOTE: use placeholders in sql instead of using f strings to pass in all parameters is better 
#       for preventing sql injection attacks

# globals ---------------
lsn_offset_table = "Lsn_Offsets"

# -----------------------

# store last applied lsn
# slot_name	        last_applied_lsn
# slot_wal_reader	0/16B6C50


# get last applied lsn
# slot_name	        last_applied_lsn
# slot_wal_reader	0/16B6C50


# create lsn offset table
def Create_LSN_Offset_Table():
    return f"""
            CREATE TABLE IF NOT EXISTS {lsn_offset_table} (
                slot_name TEXT PRIMARY KEY,
                last_applied_lsn TEXT NOT NULL
            )
           """

def Create_Test_Data_Table_Sql():
    return """
            CREATE TABLE IF NOT EXISTS test_data (
                id SERIAL PRIMARY KEY,
                counter INTEGER NOT NULL,
                message TEXT,
                value NUMERIC(10,2),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
            
def Get_Last_Applied_Lsn_Sql():
    return f"SELECT last_applied_lsn FROM {lsn_offset_table} WHERE slot_name = ?"


def Set_Last_Applied_Lsn():
    return f"""
            INSERT INTO {lsn_offset_table}(slot_name, last_applied_lsn)
            VALUES(?, ?)
            ON CONFLICT(slot_name) DO UPDATE SET last_applied_lsn = excluded.last_applied_lsn
            """


def Create_Cdv_Events_Table():
    return """
            CREATE TABLE IF NOT EXISTS cdc_events (
                table_fqn TEXT NOT NULL,
                pk TEXT NOT NULL,
                commit_lsn TEXT NOT NULL,
                payload JSONB,
                PRIMARY KEY (table_fqn, pk, commit_lsn));
           """


def Insert_Into_Cdc_Events():
    return """
           INSERT INTO cdc_events(table_fqn, pk, commit_lsn, payload)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (table_fqn, pk, commit_lsn) DO NOTHING
           """







