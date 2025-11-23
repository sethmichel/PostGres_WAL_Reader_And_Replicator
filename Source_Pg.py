import asyncio
import json
import os
from typing import AsyncIterator, Dict, Any, Tuple, Optional
import psycopg

'''
This is the logical replication source aka the WAL reader. it connects to pg and streams change events
from the logical replication slot. Wal2Json_Via_Pg_Recvlogical() is a async iterator for getting wal updates.
it passes this info to the apply_manager.py

it's the input for the data pipeline, it transforms the binary wal stream into something useable

Main.py calls Wal2Json_Via_Pg_Recvlogical() to get an async iterator of WAL events

data flow of this file
pg -> WAL -> logical decoding plugin -> pg_recvlogical -> stdout JSON -> Python yields event
'''

# asks pg for its current WAL position 
# use with this the last_applied_lsn to figure out where I should move to
# returns pg_lsn as text like '0/16B6C50'
def Get_Current_Lsn(dsn):
    with psycopg.connect(dsn) as cx:
        with cx.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            (lsn,) = cur.fetchone()

            return lsn


# check the publication is still up. if not create one on primary
# primary should be doing the publishing
def Check_Publication(dsn, publication):
    with psycopg.connect(dsn) as cx:
        with cx.cursor() as cur:
            cur.execute("""SELECT 1 FROM pg_publication WHERE pubname = %s""", (publication,))
            exists = cur.fetchone()

            if not exists:
                cur.execute(f"CREATE PUBLICATION {publication} FOR ALL TABLES")
                cx.commit()


# check the logicall replication SLOT exists, if not create one with the decoding plugin (pgoutput, wal2json, ...)
def Check_Replication_Slot(dsn, slot, plugin):
    with psycopg.connect(dsn) as cx:
        with cx.cursor() as cur:
            cur.execute("""SELECT 1 FROM pg_replication_slots WHERE slot_name = %s""", (slot,))
            exists = cur.fetchone()
            if not exists:
                cur.execute("SELECT * FROM pg_create_logical_replication_slot(%s, %s)",
                                  (slot, plugin))
                cx.commit()


# check if subscription exists on standby, if not create one that connects to primary
def Check_Subscription(standby_dsn, primary_config, app_config):
    """
    Create a subscription on the standby server that subscribes to the primary's publication.
    
    Args:
        standby_dsn: Connection string to standby server (from host machine)
        primary_docker_host: Docker container hostname for primary (e.g., 'pg_primary')
        primary_user: Primary database user
        primary_password: Primary database password
        primary_dbname: Primary database name
        subscription_name: Name of the subscription to create
        publication_name: Name of the publication on primary to subscribe to
    """
    with psycopg.connect(standby_dsn, autocommit=True) as cx:
        with cx.cursor() as cur:
            # Check if subscription already exists
            cur.execute("""SELECT 1 FROM pg_subscription WHERE subname = %s""", (app_config.subscription_name,))
            exists = cur.fetchone()
            
            if not exists:
                # Build connection string for primary using Docker network hostname
                # The subscription runs inside the standby container, so it needs to use
                # the Docker network hostname not localhost
                connection_string = (
                    f"host={primary_config.host_name} " # not localhost
                    f"port=5432 "      # Use docker container port, not host port
                    f"user={primary_config.user} "
                    f"password={primary_config.password} "
                    f"dbname={primary_config.dbname}"
                )
                
                # Create subscription - must run outside transaction block
                # Note: We use string formatting here because subscription name and publication name 
                # cannot be parameterized in CREATE SUBSCRIPTION
                cur.execute(
                    f"CREATE SUBSCRIPTION {app_config.subscription_name} "
                    f"CONNECTION '{connection_string}' "
                    f"PUBLICATION {app_config.publication_name} "
                    f"WITH (copy_data = false)"
                )
                print(f"Created subscription '{app_config.subscription_name}' on standby server")
            else:
                print(f"Subscription '{app_config.subscription_name}' already exists on standby server")


''' 
- decoder: launch a subprocess using pg_recvlogical to stream transformed WAL output that's readable
- the messages are produced by wal2json over pg_recvlogical
- this is a asynchronous generator that yields (lsn, event_json) pairs
- since args is a command line command, this is basically running a subprocess that does a command line prompt 
  to get wal data continuously. it's called in the main data loop

- how the generator fits into the whole system. the generator gets a continuous wal data stream, when it gets data it
  yields (lsn, data) which returns and is batched. when the batch reaches it's max size it's processed, sent to the 
  sink, and the lsn is saved (if it worked), then it returns to the yield here and continues the loop

paras: dsn_params: Dict[str, Any] | start_lsn: Optional[str]
returns: AsyncIterator[Tuple[str, Dict[str, Any]]] '''
async def Wal2Json_Via_Pg_Recvlogical(dsn_params, slot, publication, start_lsn, status_interval_seconds):
    # args is a command line command that's done in a subprocess
    args = [
        "pg_recvlogical",
        "-h", dsn_params["host"],
        "-p", str(dsn_params["port"]),
        "-U", dsn_params["user"],
        "-d", dsn_params["dbname"],
        #"-S", slot,
        "-o", f"pretty-print=0",       # -o means formatting
        #"-o", f"add-tables=*",        # wal2json fails if we use add-tables=*
        "-o", f"include-xids=1",
        "-o", f"include-timestamp=1",
        "-o", f"include-lsn=1",
        "--slot", slot,                # replication slot name
        "--plugin", "wal2json",
        "--start",
        "-f", "-",                     # write to stdout
        #"--no-loop",
        "--status-interval", str(int(status_interval_seconds)),
    ]

    if start_lsn:
        args += ["--startpos", start_lsn]
    
    #print(f"DEBUG: Starting pg_recvlogical with command:")
    #print(f"  {' '.join(args)}")
    #print(f"  Starting from LSN: {start_lsn if start_lsn else 'beginning'}")

    # output is newline-delimited json objects (transaction chunks)
    # user must have replication privileges, and pg_hba.conf must allow replication connection (not just host connections)
    # PGPASSWORD is the standard PostgreSQL environment variable for passing passwords to CLI tools
    # We must copy the existing environment (os.environ.copy()) so Windows can find pg_recvlogical in PATH
    env = os.environ.copy()
    env["PGPASSWORD"] = dsn_params["password"]
    
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env
    )

    # make sure proc exists. assert will make the code fail very noticably
    assert proc.stdout is not None
    assert proc.stderr is not None
    
    # Read stderr in a separate task to see errors
    async def log_stderr():
        async for line in proc.stderr:
            error_msg = line.decode("utf-8").strip()
            if error_msg:
                print(f"pg_recvlogical STDERR: {error_msg}")
    
    stderr_task = asyncio.create_task(log_stderr())
    
    # "async for" does await internally. the slow parts is waiting for the data to arrive from pg from the subprocess
    async for raw in proc.stdout:
        line = raw.decode("utf-8").strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        # wal2json emits objects with an array of changes and metadata including lsn
        lsn = obj.get("lsn") or obj.get("nextlsn") or obj.get("last_lsn")
        if not lsn:
            # if missing per-chunk lsn, you can emit the commit lsn after collecting
            lsn = obj.get("commit_lsn") or obj.get("xid")  # fallback, not preferred

        yield lsn, obj

    # Cancel stderr task and wait for subprocess to finish
    # this'll wait for the subprocess to finish which means 1) when I close the program, 
    # 2) the wal stream ends, 3) it gets an error
    stderr_task.cancel()
    try:
        await stderr_task
    except asyncio.CancelledError:
        pass
    await proc.wait() 
