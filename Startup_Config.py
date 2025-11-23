from dataclasses import dataclass
from dotenv import load_dotenv
from pathlib import Path
import os
import sys

# holds a pg connection parameters
@dataclass
class Pg_Conn_Info:
    host: str
    host_name: str
    port: int
    user: str
    password: str
    dbname: str


# app settings
@dataclass
class App_Config:
    primary: Pg_Conn_Info
    publication_name: str
    subscription_name: str
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
    load_dotenv(env_path, override=True) # must use override

    # Map env file to host port (since we're connecting from Windows host, not inside container)
    # Docker port mapping: HOST_PORT:CONTAINER_PORT (e.g., "5434:5432")
    port_mapping = {
        "Primary.env": 5434,   # primary container accessible at localhost:5434
        "primary.env": 5434,
        "Standby.env": 5435,   # standby container accessible at localhost:5435
        "standby.env": 5435,
        "Sink.env": 5436,      # sink container accessible at localhost:5436
        "sink.env": 5436
    }
    
    host_port = port_mapping.get(env_file, int(os.getenv("POSTGRES_PORT").strip()))

    conn_info = Pg_Conn_Info(
        host="localhost",   # localhost is for when connecting from host machine
        host_name=os.getenv("HOST_NAME").strip(),
        port=host_port,     # use the HOST port, not the container port
        user=os.getenv("POSTGRES_USER").strip(),
        password=os.getenv("POSTGRES_PASSWORD").strip(),
        dbname=os.getenv("POSTGRES_DB").strip()
    )
    #print(f"DEBUG: Loaded config from {env_file}: user={conn_info.user}, host_port={conn_info.port}")

    if (conn_info.host == None or conn_info.host_name == None or conn_info.port == None or conn_info.user == None or
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
        publication_name = os.getenv("publication_name").strip(),
        subscription_name = os.getenv("subscription_name").strip(),
        slot_name = os.getenv("slot_name").strip(),
        plugin = os.getenv("plugin").strip(),
        start_from_beginning = os.getenv("start_from_beginning").strip().lower(),
        batch_size = int(os.getenv("batch_size").strip()),
        max_retries = int(os.getenv("max_retries").strip()),
        backoff_seconds = float(os.getenv("backoff_seconds").strip()),
        status_interval_seconds = float(os.getenv("status_interval_seconds").strip()),
        offsets_path = os.getenv("offsets_path").strip()
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
    