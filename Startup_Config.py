from dataclasses import dataclass
from dotenv import load_dotenv
from pathlib import Path
import os
import sys

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


# load database connection info from the .env files
def Load_Env_Config(env_file):
    # Load the specific .env file
    env_path = Path(__file__).parent / "Docker_Connections" / env_file
    load_dotenv(env_path)

    conn_info = Pg_Conn_Info(
        host="localhost",  # localhost is just for when I'm connecting from host machine
        port=int(os.getenv("POSTGRES_PORT")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB")
    )

    if (conn_info.host == None or conn_info.port == None or conn_info.user == None or
        conn_info.password == None or conn_info.dbname == None):
        print(f"Error: missing environment variables in file: {env_file}")
        sys.exit(1)
    else:
        return conn_info
    