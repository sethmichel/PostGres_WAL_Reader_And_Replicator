import asyncio
import os
from typing import Dict, Any
from Config import App_Config, Pg_Conn_Info
from Offsets import Ensure_Offsets_Store, Get_Last_Applied_Lsn, Set_Last_Applied_Lsn
from Source_Pg import Ensure_Publication, Ensure_Slot, Wal2Json_Via_Pg_Recvlogical, Get_Current_Lsn
from Apply_Manager import Run_Apply_Loop
from Sink_Postgres import Apply_Postgres


# return Dict[str, Any]
def Dsn_Params(pg: Pg_Conn_Info):
    return {
        "host": pg.host,
        "port": pg.port,
        "user": pg.user,
        "password": pg.password,
        "dbname": pg.dbname,
    }


def Make_Dsn(pg: Pg_Conn_Info):
    return f"host={pg.host} port={pg.port} user={pg.user} password={pg.password} dbname={pg.dbname}"


async def Main():
    def Persist_Lsn(lsn: str):
        Set_Last_Applied_Lsn(cfg.offsets_path, cfg.slot_name, lsn)

    cfg = App_Config(
        primary=Pg_Conn_Info(host="pg_primary", port=5432, user="postgres", password="postgres", dbname="app"),
        publication="demo_pub",
        slot_name="slot_wal_reader",
        plugin="wal2json",                     # or 'pgoutput' if you implement a decoder
        start_from_beginning=False,
        batch_size=100,
        max_retries=8,
        backoff_seconds=0.5,
        status_interval_seconds=10.0,
        offsets_path="data/offsets.sqlite",
    )

    Ensure_Offsets_Store(cfg.offsets_path)

    primary_dsn = Make_Dsn(cfg.primary)
    await Ensure_Publication(primary_dsn, cfg.publication)
    await Ensure_Slot(primary_dsn, cfg.slot_name, cfg.plugin)

    last = Get_Last_Applied_Lsn(cfg.offsets_path, cfg.slot_name)

    if not last and not cfg.start_from_beginning:
        last = await Get_Current_Lsn(primary_dsn)

    source = Wal2Json_Via_Pg_Recvlogical(
        dsn_params=Dsn_Params(cfg.primary),
        slot=cfg.slot_name,
        publication=cfg.publication,
        start_lsn=last,
        status_interval_seconds=cfg.status_interval_seconds
    )

    async def Apply_Batch(events):
        # choose a sink; stdout for dev, postgres or others for prod
        # await apply_postgres(other_dsn, events)
        for _ in []:  # placeholder to keep the function async even if stdout
            pass

    await Run_Apply_Loop(
        source=source,
        batch_size=cfg.batch_size,
        apply_batch=Apply_Batch,
        persist_lsn=Persist_Lsn,
        max_retries=cfg.max_retries,
        backoff_seconds=cfg.backoff_seconds
    )


if __name__ == "__main__":
    asyncio.run(Main())
