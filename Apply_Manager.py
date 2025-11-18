import asyncio
from typing import AsyncIterator, Tuple, Dict, Any, List, Callable


'''
purpose: this is the data pipeline "manager". it connects the source (original wal logs from source_pg.py) to the sink (sink_stdout.py). it enforces safety and consistancy

it decides
    when to process a batch
    when to rety processing a failed batch
    when to update offsets (last_applied_lsn)
    how to transform raw WAL data

- beacuse it saves lsn (this file doesn't do that, it just approves it in a sense) only if the whole batch is successfully processed - it prevents data loss.
  because if we crash we'll get the previous lsn as a starting point and reprocess that failed batch, so we can't miss data
- data cannot be dulicated with this design
'''


''' goal: Makes all events uniform, regardless of schema
- takes 1 wal2json message (which should be multiple wal changes) and splits it into individual events
- these events have fields like: table, type, pk, commit_lsn, payload_json

return: List[Dict[str, Any]]
paras: obj: Dict[str, Any] '''
def Normalize_Wal2Json(obj):
    out = []
    commit_lsn = obj.get("lsn") or obj.get("commit_lsn")
    
    for ch in obj.get("changes", []):
        out.append({
            "commit_lsn": commit_lsn,
            "type": ch.get("kind"),          # insert, update, delete
            "table": f'{ch.get("schema")}.{ch.get("table")}',
            "pk": ch.get("oldkeys", {}).get("keyvalues") or ch.get("columnvalues"),
            "payload_json": ch,              # store raw; sinks may shape it
        })

    return out


''' the cental async loop
- reads events from the wal source stream (binary)
- buffers them into a list
- once a batch is fully made, it calls the function to process the batch
'''
async def Run_Apply_Loop(source: AsyncIterator[Tuple[str, Dict[str, Any]]], batch_size,
                         apply_batch: Callable[[List[Dict[str, Any]]], "asyncio.Future[None]"],
                         persist_lsn: Callable[[str], None], max_retries,
                         backoff_seconds):

    buffer = [] # List[Tuple[str, Dict[str, Any]]]

    # source is a async generator
    # "async for" handles "await" internally
    async for lsn, obj in source:
        buffer.append((lsn, obj))
        if len(buffer) >= batch_size:
            await Process_Batch(buffer, apply_batch, persist_lsn, max_retries, backoff_seconds)
            buffer.clear()

    if buffer:
        await Process_Batch(buffer, apply_batch, persist_lsn, max_retries, backoff_seconds)
        buffer.clear()
    else:
        pass


''' handles retries, backoff, and gives the ok to save the lsn (persist_lsn)
- calls the sink (apply_batch) to apply the data
- if batch is fully successfully processed, it then calls the function that saves the last_applied_lsn to the table
- if batch fails at all, it retries with exponential backoff up to max_retries
- what backoff means: when a batch fails to process, we don't retry right away. we wait an increasing amount of time before retrying
    this gives the systems / computer time to hopefully fix themselves or whatever caused the issue
'''
async def Process_Batch(buffer: List[Tuple[str, Dict[str, Any]]], apply_batch, persist_lsn, 
                        max_retries, backoff_seconds):
    
    # normalize and collect events
    last_lsn = None
    events: List[Dict[str, Any]] = []

    for lsn, obj in buffer:
        last_lsn = lsn
        events.extend(Normalize_Wal2Json(obj))

    # retry with backoff; sink must be idempotent
    attempt = 0
    while True:
        try:
            await apply_batch(events)
            if last_lsn:
                persist_lsn(last_lsn)

            return

        except Exception as exc:
            attempt += 1
            if attempt > max_retries:
                # in production: send to dead-letter and move on or halt based on policy
                raise
            await asyncio.sleep(backoff_seconds * attempt)
