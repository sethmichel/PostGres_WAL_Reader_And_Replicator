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
def Load_Docker_Env_Config(env_file):
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


# load app settings .env files
def Load_App_Env_Config(env_file, primary_config):
    # Load the specific .env file
    load_dotenv(env_file)

    app_info = App_Config(
        primary = primary_config,
        publication = os.getenv("publication_name"),
        slot_name = os.getenv("slot_name"),
        plugin = os.getenv("plugin"),
        start_from_beginning = os.getenv("start_from_beginning").lower(),
        batch_size = int(os.getenv("batch_size")),
        max_retries = int(os.getenv("max_retries")),
        backoff_seconds = float(os.getenv("backoff_seconds")),
        status_interval_seconds = float(os.getenv("status_interval_seconds")),
        offsets_path = os.getenv("offsets_path")
    )
    if (app_info.start_from_beginning == "false"):
        app_info.start_from_beginning = False
    else:
        app_info.start_from_beginning = True

    # if any memeber variable is None
    if any(value is None for value in vars(app_info).values()):
        print(f"Error: missing environment variables in file: {env_file}")
        sys.exit(1)
    else:
        return app_info
    