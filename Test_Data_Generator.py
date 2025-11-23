"""
Generates test data for the db so the main app can get new WAL data

- Creates a test_data table if it doesn't exist
- Continuously alternates between 4 operations every second:
  1. INSERT - Adds a new row with random data
  2. UPDATE - Modifies a random existing row
  3. UPDATE COUNTER - Increments counter on all rows (generates multiple WAL entries)
  4. DELETE - Removes oldest row (only if there are more than 5 rows)
"""

import psycopg
import time
import random
from pathlib import Path
from Startup_Config import Load_Docker_Env_Config, Pg_Conn_Info


def Make_Dsn(pg: Pg_Conn_Info):
    return f"host={pg.host} port={pg.port} user={pg.user} password={pg.password} dbname={pg.dbname}"


def Generate_Changes(dsn: str, delay_seconds: float = 1.0):
    with psycopg.connect(dsn) as conn:        
        operation_count = 0
        operations = ['insert', 'update', 'delete', 'update_counter']
        
        print(f"\n{'='*60}")
        print("Starting continuous data generation...")
        print(f"Delay between operations: {delay_seconds} seconds")
        print(f"{'='*60}\n")
        
        try:
            while True:
                operation_count += 1
                operation = operations[operation_count % len(operations)]
                
                with conn.cursor() as cur:
                    if operation == 'insert':
                        # Insert a new row
                        message = f"Test message {operation_count}"
                        value = round(random.uniform(10.0, 1000.0), 2)
                        cur.execute("""
                            INSERT INTO test_data (counter, message, value)
                            VALUES (%s, %s, %s)
                            RETURNING id
                        """, (operation_count, message, value))
                        new_id = cur.fetchone()[0]
                        print(f"[{operation_count:04d}] INSERT -> Added row with id={new_id}, value={value}")
                    
                    elif operation == 'update':
                        # Update a random existing row
                        cur.execute("SELECT id FROM test_data ORDER BY RANDOM() LIMIT 1")
                        result = cur.fetchone()
                        if result:
                            row_id = result[0]
                            new_message = f"Updated at operation {operation_count}"
                            new_value = round(random.uniform(10.0, 1000.0), 2)
                            cur.execute("""
                                UPDATE test_data 
                                SET message = %s, value = %s, updated_at = NOW()
                                WHERE id = %s
                            """, (new_message, new_value, row_id))
                            print(f"[{operation_count:04d}] UPDATE -> Modified row id={row_id}, new_value={new_value}")
                    
                    elif operation == 'update_counter':
                        # Update the counter on all rows (generates multiple WAL entries)
                        cur.execute("UPDATE test_data SET counter = counter + 1, updated_at = NOW()")
                        rows_updated = cur.rowcount
                        print(f"[{operation_count:04d}] UPDATE -> Incremented counter on {rows_updated} rows")
                    
                    elif operation == 'delete':
                        # Delete the oldest row if we have more than 5 rows
                        cur.execute("SELECT COUNT(*) FROM test_data")
                        count = cur.fetchone()[0]
                        
                        if count > 5:
                            cur.execute("""
                                DELETE FROM test_data 
                                WHERE id = (SELECT id FROM test_data ORDER BY created_at ASC LIMIT 1)
                                RETURNING id
                            """)
                            deleted_id = cur.fetchone()[0]
                            print(f"[{operation_count:04d}] DELETE -> Removed row id={deleted_id} (keeping table size manageable)")
                        else:
                            print(f"[{operation_count:04d}] DELETE -> Skipped (only {count} rows, keeping minimum)")
                
                conn.commit()
                time.sleep(delay_seconds)
                
        except KeyboardInterrupt:
            print(f"\n\n{'='*60}")
            print(f"Stopped after {operation_count} operations")
            print(f"{'='*60}")


def Main():
    print("\n" + "="*60)
    print("WAL Test Data Generator")
    print("="*60)
    
    # Load primary database configuration
    try:
        primary_config = Load_Docker_Env_Config('Primary.env')
    except Exception as e:
        print(f"Error loading Primary.env: {e}")
        print("\nMake sure you're running this from the project root directory")
        print("and that Docker_Connections/Primary.env exists.")
        return
    
    primary_dsn = Make_Dsn(primary_config)
    
    print(f"\nConnecting to: {primary_config.host}:{primary_config.port}")
    print(f"Database: {primary_config.dbname}")
    print(f"User: {primary_config.user}\n")
    
    try:
        # Test connection first
        with psycopg.connect(primary_dsn) as conn:
            print("Database connection successful\n")
        
        # Start generating changes
        Generate_Changes(primary_dsn, delay_seconds=1.0)
        
    except psycopg.OperationalError as e:
        print(f"\n Failed to connect to database:")
        print(f"  {e}")
        print("\nMake sure:")
        print("  1. Docker containers are running (docker compose up -d)")
        print("  2. PostgreSQL is accepting connections")
        print("  3. Connection details in Primary.env are correct")
    except Exception as e:
        print(f"\n Error: {e}")


if __name__ == "__main__":
    Main()

