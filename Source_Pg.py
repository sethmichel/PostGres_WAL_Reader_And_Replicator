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
        "-S", slot,
        "-o", f"pretty-print=0",       # -o means formatting
        "-o", f"add-tables=*",         # subset if desired
        "-o", f"include-xids=1",
        "-o", f"include-timestamp=1",
        "-o", f"include-lsn=1",
        "--slot", slot,                # replication slot name
        "--plugin", "wal2json",
        "--start",
        "--no-loop",
        "--status-interval", str(int(status_interval_seconds)),
    ]

    if start_lsn:
        args += ["--startpos", start_lsn]

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

    # this'll wait for the subprocess to finish which means 1) when I close the program, 
    # 2) the wal stream ends, 3) it gets an error
    await proc.wait() 
