from dataclasses import dataclass


# holds a pg connection parameters
@dataclass
class Pg_Conn_Info:
    host: str
    port: int
    user: str
    password: str
    dbname: str


# app settings
@dataclass
class App_Config:
    primary: Pg_Conn_Info
    publication: str
    slot_name: str
    plugin: str                  # 'pgoutput' or 'wal2json'
    start_from_beginning: bool   # if no offset yet
    batch_size: int
    max_retries: int
    backoff_seconds: float
    status_interval_seconds: float
    offsets_path: str            # ex) "offsets.sqlite"
