import asyncio
import os
import sys
from pathlib import Path
from typing import Dict, Any
from Startup_Config import App_Config, Pg_Conn_Info, Load_Env_Config
from Offsets import Ensure_Offsets_Store, Get_Last_Applied_Lsn, Set_Last_Applied_Lsn
from Source_Pg import Ensure_Publication, Ensure_Slot, Wal2Json_Via_Pg_Recvlogical, Get_Current_Lsn
from Apply_Manager import Run_Apply_Loop
from Sink_Postgres import Apply_Postgres


# return Dict[str, Any]
def Make_Dsn_Params_Dict(pg):
    return {
        "host": pg.host,
        "port": pg.port,
        "user": pg.user,
        "password": pg.password,
        "dbname": pg.dbname,
    }


def Make_Dsn(pg: Pg_Conn_Info):
    return f"host={pg.host} port={pg.port} user={pg.user} password={pg.password} dbname={pg.dbname}"


# check if Docker_Connections folder exists, create if not, and verify required files
def Check_Docker_Connections(): 
    # Get the directory where Main.py is located
    main_dir = Path(__file__).parent
    docker_connections_dir = main_dir / "Docker_Connections"
    
    # Check if folder exists, create if not
    if not docker_connections_dir.exists():
        docker_connections_dir.mkdir()
        print(f"Created Docker_Connections folder at {docker_connections_dir}")
        return
    
    # If folder exists, check for required files
    required_files = ["docker-compose.yml", "primary.env", "standby.env"]
    missing_files = []
    
    for file_name in required_files:
        file_path = docker_connections_dir / file_name
        if not file_path.exists():
            missing_files.append(file_name)
    
    if missing_files:
        print("Error: The following required files are missing from Docker_Connections folder:")
        for file_name in missing_files:
            print(f"  - {file_name}")
        print(f"\nPlease ensure all required files exist in: {docker_connections_dir}")
        sys.exit(1)
    
    # Check if app.env exists
    app_env_path = main_dir / "app.env"
    if not app_env_path.exists():
        print(f"Error: app.env is missing from {main_dir}")
        sys.exit(1)


async def Main():
    # check folders and load env files. these all close the program if they fail
    Check_Docker_Connections()
    primary_config = Load_Env_Config('Docker_Connections/primary.env')
    standy_config = Load_Env_Config('Docker_Connections/standby.env')
    app_config = Load_Env_Config('app.env')

    # make dsn strings
    primary_dsn = Make_Dsn(primary_config)

    # check stuff exists or create it
    Ensure_Offsets_Store(app_config.offsets_path)                       # make sqllite lsn table if it doesn't exist
    Ensure_Publication(primary_dsn, app_config.publication)             # check the publication is still up. if not create one on primary
    Ensure_Slot(primary_dsn, app_config.slot_name, app_config.plugin)   # cleck for a slot, if not create one

    # get the most recent lsn that we successfully processed
    most_recent_successful_lsn = Get_Last_Applied_Lsn(app_config.offsets_path, app_config.slot_name)

    if most_recent_successful_lsn == None and not app_config.start_from_beginning:
        most_recent_successful_lsn = Get_Current_Lsn(primary_dsn)

    # 
    source = Wal2Json_Via_Pg_Recvlogical(
        dsn_params=Make_Dsn_Params_Dict(primary_config),
        slot=app_config.slot_name,
        publication=app_config.publication,
        start_lsn=most_recent_successful_lsn,
        status_interval_seconds=app_config.status_interval_seconds
    )

    async def Apply_Batch(events):
        # choose a sink; stdout for dev, postgres or others for prod
        # await apply_postgres(other_dsn, events)
        for _ in []:  # placeholder to keep the function async even if stdout
            pass

    def Persist_Lsn(lsn: str):
        Set_Last_Applied_Lsn(app_config.offsets_path, app_config.slot_name, lsn)

    await Run_Apply_Loop(
        source=source,
        batch_size=app_config.batch_size,
        apply_batch=Apply_Batch,
        persist_lsn=Persist_Lsn,
        max_retries=app_config.max_retries,
        backoff_seconds=app_config.backoff_seconds
    )


if __name__ == "__main__":
    asyncio.run(Main())
