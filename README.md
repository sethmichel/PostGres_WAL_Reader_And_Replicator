This is a Change Data Capture (CDC) pipeline that:
1) Reads PostgreSQL (pg) WAL (Write-Ahead Log) events with logical replication
2) Transforms them into normalized events that I can manipulate and move around the program
3) Sends them to a destination (sink) with guaranteed delivery
4) Tracks progress to survive crashes/restart without data loss, data duplication, and is idempotency (no variety in effects, everything happens the exact same each time)


**Data Flow**
PostgreSQL WAL → pg_recvlogical (subprocess) → Source_Pg.py (async iterator)
    ↓
Apply_Manager.py (batching + normalization + retry logic)
    ↓
Sink (Stdout for testing, Postgres for prod)
    ↓
Offsets.py (saves LSN to isolated/separate SQLite for crash recovery)

1) config.py holds my connections to pg and system configs

2) main.py handles the core loop, makes sure various pg and system components are correct or creates them. starts the data pipeline

3) source_pg.py is the bridge between my code and pg, it gets the actual decoded wal data from pg (pg decodes it via wal2json which is the plugin setting I chose).
   now I have readable wal updates. this is in json

4) these wal updates sent to apply_manager.py from main.py

5) apply_manager.py will normalize the decoded wal data so it's easy to work with, it batches them together. Then sends the batch to my sink in sink_stdout.py which processes it. sink_stdout.py is basically a test file right now so it'll print those WAL changes to the console. if sink_stdout.py does everything successfully, then the main data loop will save the last_applied_lsn to the lsn table. otherwise it will retry processing that batch a few times. we know if it worked out not because it return like normal or raise an error.


**Key Stability Features**
At Least Once Delivery - No Data Loss
- I only save the LSN is a WAL data batch is successfully processed
- If I crash mid-processing or anywhere, I restart from the last saved LSN and reprocess (most recent lsn is saved in separte sqlite database)
- Data re-tries processing on failure. it increases wait times between retries.

Idempotency - Consistancey & No Duplicate Data
- My sink (destination) uses "ON CONFLICT DO NOTHING" sql which means that if the data is already applied, then do nothing
- I track (commit_lsn, pk) pairs to detect duplicate events. If I encounter a duplicate of these then I skip that WAL event
- Reprocessing the same batch won't create duplicate data

System Crashes/Restarts/Disconnects
- Same solutions I've described. I can use the most recent saved LSN to return to the correct position in WAL, and my code will skip duplicates


***Files***

**config.py**
what it does: Holds all connection info and settings

classes
    pg_conn_info: holds a pg connection parameters
    app_config: app settings


**offests.py**
what it does: handles the last_applied_lsn. it moves it around and saves/loads it. we'd lose our place on restarts without this

getter/setter for replication offset (last_applied_lsn). it's so the consumer can start from the exact right place after a crash/restart/reconnect. you want to store this in a separate file or db outside the replication system. If I lose this value then I'll lose database data


**source_pg.py**
- purpose: This is the bidge between my code and pg. it's the wal reader and it decodes the wal from each logical replication slot into something I can read (so it's the logical replication source). Wal2Json_Via_Pg_Recvlogical() is a async iterator for getting wal updates.

- it's the input for the data pipeline, it transforms the binary wal stream into something usable

- Main.py calls Wal2Json_Via_Pg_Recvlogical() - notice that args is a command line command, so this is basically running a subprocess that does a command line prompt to get WAL results

- data flow of this file
pg -> WAL -> logical decoding plugin -> pg_recvlogical -> stdout JSON -> Python yields event


**sink_postgres.py**
- purpose: Send the transformed wal data to a destination and apply those changes

- a "sink" means the destination where python is sending the data. the analogy is data is flowing out of pg and going down the "sink" into the destination.
- this is the connection to the sink and how to send data to it (inserts/updates...). it ensures safe replays

- 


**sink_stdout.py**
- purpose: a testing sink. instead of writeing data somewhere, it prints the events to the console


**apply_manager.py**
- purpose: this is the data pipeline "manager". it connects the source (original wal logs from source_pg.py) to the sink (sink_stdout.py). it enforces safety and consistancy
- it decides
    - when to process a batch
    - when to rety processing a batch
    - when to update offsets (last_applied_lsn)
    - how to transform raw WAL data

- beacuse it saves lsn (this file doesn't do that, it just approves it in a sense) only if the whole batch is successfully processed - it prevents data loss.
  because if we crash we'll get the previous lsn as a starting point and reprocess that failed batch, so we can't miss data

- data cannot be dulicated with this design

- data gets "at-least-once delivery" meaning it's for sure at least sent once to the sink


**sqlite**
- this is basically just a file on disk, so we use the sqlite3 module and create it in the code if it's missing


**Main.py**
- purpose: app start and main data loops/configs
- does startup stuff like 
    - makes the config objects to be passed around the program
    - check tables exist or create them 
    - check the publisher is publishing
    - check the replication slots are correct
    - get the most recent lsn so we can figure out our starting place
    - start the WAL source stream by connecting to the db (start that async iterator)
    - check sink stuff is all correct
    - create the async generator which is used by apply_manager.py to actually get the wal results in a loop

the key is the run_apply_loop() code. that loop is basically the data pipeline



config.py (settings) -> main.py (builds configs, sets up slots, starts loop) -> source_pg.py (streams WAL, gets the new lsn, events)
-> apply_manager.py (organizes/processes decoded wal data) --1--> sink_postgres.py (send data to sink)
                                                           --2--> sink_stdout.py (our debugging sink)
-> offsets.py (save the new lsn)


- Main.py makes an app_config object (which has app settings), which is passed around to files.
- Main.py loads the last_applied_lsn on startup/restart/reconnect and passes it to teh wal stream reader (source_pg.py)
- Main.py calls source_pg.Wal2Json_Via_Pg_Recvlogical() to get an async iterator of WAL events
- each time the sink successfully saves a batch, it saves that lsn to the last_applied_lsn table






