import asyncio
import json
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
    with psycopg.AsyncConnection.connect(dsn) as cx:
        with cx.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            (lsn,) = cur.fetchone()

            return lsn


# check the publication is still up. if not create one on primary
# primary should be doing the publishing
def Ensure_Publication(dsn, publication):
    with psycopg.connect(dsn) as cx:
        with cx.cursor() as cur:
            cur.execute("""SELECT 1 FROM pg_publication WHERE pubname = %s""", (publication,))
            exists = cur.fetchone()

            if not exists:
                cur.execute(f"CREATE PUBLICATION {publication} FOR ALL TABLES")
                cx.commit()


# check the logicall replication SLOT exists, if not create one with the decoding plugin (pgoutput, wal2json, ...)
def Ensure_Slot(dsn, slot, plugin):
    with psycopg.connect(dsn) as cx:
        with cx.cursor() as cur:
            cur.execute("""SELECT 1 FROM pg_replication_slots WHERE slot_name = %s""", (slot,))
            exists = cur.fetchone()
            if not exists:
                cur.execute("SELECT * FROM pg_create_logical_replication_slot(%s, %s)",
                                  (slot, plugin))
                cx.commit()


# decoder: launch a subprocess using pg_recvlogical to stream transformed WAL output that's readable
# the messages are produced by wal2json over pg_recvlogical
# this is a asynchronous generator that yields (lsn, event_json) pairs
# paras: dsn_params: Dict[str, Any] | start_lsn: Optional[str]
# returns: AsyncIterator[Tuple[str, Dict[str, Any]]]
async def Wal2Json_Via_Pg_Recvlogical(dsn_params, slot, publication, start_lsn, status_interval_seconds):
    # pg_recvlogical requires connection paras individually (not a dsn string)
    args = [
        "pg_recvlogical",
        "-h", dsn_params["host"],
        "-p", str(dsn_params["port"]),
        "-U", dsn_params["user"],
        "-d", dsn_params["dbname"],
        "-S", slot,
        "-o", f"pretty-print=0",
        "-o", f"add-tables=*",            # subset if desired
        "-o", f"include-xids=1",
        "-o", f"include-timestamp=1",
        "-o", f"include-lsn=1",
        "--slot", slot,
        "--plugin", "wal2json",
        "--start",
        "--no-loop",
        "--status-interval", str(int(status_interval_seconds)),
    ]

    if start_lsn:
        args += ["--startpos", start_lsn]

    # output is newline-delimited json objects (transaction chunks)
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={"PGPASSWORD": dsn_params["password"]}
    )

    assert proc.stdout is not None
    
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

    await proc.wait()
