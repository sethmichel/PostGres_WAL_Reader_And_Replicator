from typing import List, Dict, Any
import psycopg
from Sql_Commands import Insert_Into_Cdc_Events


# example: upsert into a staging table, keyed by (table, primary_key, commit_lsn)
# this assumes events are normalized to a simple dict format in the apply manager
# events: List[Dict[str, Any]]
async def Apply_Postgres(dsn, events):
    insert_sql = Insert_Into_Cdc_Events()

    async with await psycopg.AsyncConnection.connect(dsn) as cx:
        async with cx.cursor() as cur:
            for ev in events:
                if ev["type"] == "insert":
                    # example upsert; adapt to your schema
                    await cur.execute(insert_sql,
                                     (ev["table"], ev["pk"], ev["commit_lsn"], ev["payload_json"]))
            await cx.commit()
