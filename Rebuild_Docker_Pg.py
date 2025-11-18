#!/usr/bin/env python3

"""
This should automatically remake the docker containers and pg servers so I don't have to do it for the 5th time
"""

import subprocess
import time
import sys
import os
import requests
import json
from pathlib import Path


def Load_Env_File(file_path):
    """Load environment variables from a .env file and return as dictionary"""
    env_vars = {}
    if not os.path.exists(file_path):
        print(f"Warning: Environment file not found: {file_path}")
        return env_vars
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            # Parse key=value pairs, handling quotes
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                env_vars[key] = value
    
    return env_vars


def Load_Configuration():
    """Load all configuration from env files"""
    script_dir = Path(__file__).parent
    docker_dir = script_dir / "Docker_Connections"
    
    # Load app.env
    app_config = Load_Env_File(script_dir / "app.env")
    
    # Load primary.env
    primary_config = Load_Env_File(docker_dir / "primary.env")
    
    # Load standby.env
    standby_config = Load_Env_File(docker_dir / "standby.env")
    
    config = {
        'docker_compose_path': str(docker_dir),
        'publication_name': app_config.get('publication_name'),
        'subscription_name': app_config.get('subscription_name'),
        'pgadmin_email': app_config.get('PGADMIN_EMAIL'),
        'pgadmin_password': app_config.get('PGADMIN_PASSWORD'),
        'pgadmin_url': app_config.get('PGADMIN_URL'),
        'primary': {
            'server_name': primary_config.get('SERVER_NAME'),
            'host': primary_config.get('HOST_NAME'),
            'user': primary_config.get('POSTGRES_USER'),
            'password': primary_config.get('POSTGRES_PASSWORD'),
            'database': primary_config.get('POSTGRES_DB'),
            'port': primary_config.get('POSTGRES_PORT')
        },
        'standby': {
            'server_name': standby_config.get('SERVER_NAME'),
            'host': standby_config.get('HOST_NAME'),
            'user': standby_config.get('POSTGRES_USER'),
            'password': standby_config.get('POSTGRES_PASSWORD'),
            'database': standby_config.get('POSTGRES_DB'),
            'port': standby_config.get('POSTGRES_PORT')
        }
    }
    
    return config


def Run_Command(cmd, cwd=None, capture_output=True, check=True):
    """Run a shell command and return the result"""
    print(f"\nRunning: {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            capture_output=capture_output,
            text=True,
            check=check
        )
        if capture_output and result.stdout:
            print(f"Output:\n{result.stdout}")
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        raise


def Wait_For_User(message="Press Enter to continue..."):
    """Wait for user input before continuing"""
    input(f"\n{message}")


def Wait_For_Postgres(container, user, max_attempts=30):
    """Wait for PostgreSQL to be ready"""
    print(f"\nWaiting for {container} to be ready...")
    for i in range(max_attempts):
        try:
            result = subprocess.run(
                f"docker exec {container} pg_isready -U {user}",
                shell=True,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"{container} is ready!")
                return True
            time.sleep(2)
        except Exception:
            time.sleep(2)
    print(f"Timeout waiting for {container}")
    return False


def Execute_Sql(container, user, database, sql_command):
    """Execute a SQL command in a container"""
    cmd = f'docker exec -i {container} psql -U {user} -d {database} -c "{sql_command}"'
    return Run_Command(cmd, check=False)


def Setup_Pgadmin_Server(session, server_name, host, port, username, password, db_name, pgadmin_url):
    """Setup a server in pgAdmin via API"""
    print(f"\nSetting up pgAdmin server: {server_name}")
    
    # Get server group ID (usually 1 for default)
    server_group_id = 1
    
    server_config = {
        "name": server_name,
        "group_id": server_group_id,
        "host": host,
        "port": port,
        "maintenance_db": db_name,
        "username": username,
        "ssl_mode": "prefer",
        "connect_now": True,
        "password": password
    }
    
    try:
        response = session.post(
            f"{pgadmin_url}/api/v1/servers",
            json=server_config,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code in [200, 201]:
            print(f"Server '{server_name}' created successfully!")
            return True
        else:
            print(f"Could not create server '{server_name}': {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"Error creating server '{server_name}': {e}")
        return False


def Main():
    print("=" * 70)
    print("PostgreSQL Replication Environment Setup")
    print("=" * 70)
    
    # Load configuration from env files
    config = Load_Configuration()
    
    # Change to Docker directory
    os.chdir(config['docker_compose_path'])
    print(f"\nChanged to directory: {os.getcwd()}")
    
    # Step 1: Start Docker Compose
    print("\n" + "=" * 70)
    print("STEP 1: Starting Docker Containers")
    print("=" * 70)
    Run_Command("docker compose up -d")
    
    # Wait for PostgreSQL containers to be ready
    print("\nWaiting for PostgreSQL containers to initialize (this may take a minute)...")
    time.sleep(5)
    
    if not Wait_For_Postgres(config['primary']['host'], config['primary']['user']):
        print("Primary server failed to start")
        sys.exit(1)
    
    if not Wait_For_Postgres(config['standby']['host'], config['standby']['user']):
        print("Standby server failed to start")
        sys.exit(1)
    
    # Step 2: Setup pgAdmin servers
    print("\n" + "=" * 70)
    print("STEP 2: Setting up pgAdmin Servers")
    print("=" * 70)
    
    print(f"\nWaiting for pgAdmin to be ready at {config['pgadmin_url']}...")
    time.sleep(10)
    
    # Try to setup pgAdmin servers via API
    try:
        session = requests.Session()
        
        # Login to pgAdmin
        print(f"\nAttempting to login to pgAdmin...")
        login_response = session.post(
            f"{config['pgadmin_url']}/login",
            data={
                "email": config['pgadmin_email'],
                "password": config['pgadmin_password']
            }
        )
        
        if login_response.status_code == 200:
            print("Successfully logged into pgAdmin!")
            
            # Create servers
            primary_created = Setup_Pgadmin_Server(
                session, 
                config['primary']['server_name'], 
                config['primary']['host'], 
                config['primary']['port'], 
                config['primary']['user'], 
                config['primary']['password'], 
                config['primary']['database'],
                config['pgadmin_url']
            )
            
            standby_created = Setup_Pgadmin_Server(
                session, 
                config['standby']['server_name'], 
                config['standby']['host'], 
                config['standby']['port'], 
                config['standby']['user'], 
                config['standby']['password'], 
                config['standby']['database'],
                config['pgadmin_url']
            )
            
            if not (primary_created and standby_created):
                print("\nSome servers could not be created automatically.")
                print(f"Please create them manually in pgAdmin at {config['pgadmin_url']}")
                print("\n   Primary Server:")
                print(f"     - Name: {config['primary']['server_name']}")
                print(f"     - Host: {config['primary']['host']}")
                print(f"     - Port: {config['primary']['port']}")
                print(f"     - Username: {config['primary']['user']}")
                print(f"     - Database: {config['primary']['database']}")
                print("\n   Standby Server:")
                print(f"     - Name: {config['standby']['server_name']}")
                print(f"     - Host: {config['standby']['host']}")
                print(f"     - Port: {config['standby']['port']}")
                print(f"     - Username: {config['standby']['user']}")
                print(f"     - Database: {config['standby']['database']}")
        else:
            raise Exception("Could not login to pgAdmin")
            
    except Exception as e:
        print(f"\nCould not automatically setup pgAdmin servers: {e}")
        print(f"Please create the servers manually in pgAdmin at {config['pgadmin_url']}")
        print("\n   Server configurations are shown above.")
    
    Wait_For_User("Press Enter once pgAdmin servers are configured (or if you want to skip pgAdmin setup)...")
    
    # Step 3: Create Publication on Primary
    print("\n" + "=" * 70)
    print("STEP 3: Creating Publication on Primary Server")
    print("=" * 70)
    
    sql = f"CREATE PUBLICATION {config['publication_name']} FOR ALL TABLES;"
    print(f"\nExecuting SQL on primary: {sql}")
    Execute_Sql(config['primary']['host'], config['primary']['user'], config['primary']['database'], sql)
    
    # Verify publication
    print("\nVerifying publication...")
    Execute_Sql(config['primary']['host'], config['primary']['user'], config['primary']['database'], "SELECT * FROM pg_publication;")
    
    # Step 4: Create Subscription on Standby
    print("\n" + "=" * 70)
    print("STEP 4: Creating Subscription on Standby Server")
    print("=" * 70)
    
    connection_string = (
        f"host={config['primary']['host']} port={config['primary']['port']} "
        f"user={config['primary']['user']} password={config['primary']['password']} dbname={config['primary']['database']}"
    )
    sql = f"CREATE SUBSCRIPTION {config['subscription_name']} CONNECTION '{connection_string}' PUBLICATION {config['publication_name']};"
    print(f"\nExecuting SQL on standby: {sql}")
    Execute_Sql(config['standby']['host'], config['standby']['user'], config['standby']['database'], sql)
    
    # Give it a moment to establish connection
    time.sleep(2)
    
    # Step 5: Check Replication Status
    print("\n" + "=" * 70)
    print("STEP 5: Checking Subscription Replication")
    print("=" * 70)
    
    response = input("\nDo you want to check replication status? (y/n): ").strip().lower()
    if response == 'y':
        print("\nChecking replication status on primary server...")
        Execute_Sql(config['primary']['host'], config['primary']['user'], config['primary']['database'], "SELECT * FROM pg_stat_replication;")
        
        print("\nChecking subscription status on standby server...")
        Execute_Sql(config['standby']['host'], config['standby']['user'], config['standby']['database'], "SELECT * FROM pg_stat_subscription;")
        
        Wait_For_User("Press Enter to continue after reviewing replication status...")
    
    # Step 6: WAL Inspection
    print("\n" + "=" * 70)
    print("STEP 6: WAL Inspection")
    print("=" * 70)
    
    response = input("\nDo you want to inspect the WAL files? (y/n): ").strip().lower()
    if response == 'y':
        print("\nInspecting WAL files on primary server...")
        wal_cmd = f"docker exec -it {config['primary']['host']} pg_waldump /var/lib/postgresql/data/pg_wal/000000010000000000000001"
        print(f"\nRunning: {wal_cmd}")
        subprocess.run(wal_cmd, shell=True)
    
    # Final Summary
    print("\n" + "=" * 70)
    print("Setup Complete!")
    print("=" * 70)
    print("\nQuick Reference:")
    print(f"  - pgAdmin:          {config['pgadmin_url']}")
    print(f"  - Primary DB:       localhost:5432")
    print(f"  - Standby DB:       localhost:5433")
    print(f"\nUseful Commands:")
    print(f"  - Enter primary shell:  docker exec -it {config['primary']['host']} bash")
    print(f"  - Enter standby shell:  docker exec -it {config['standby']['host']} bash")
    print(f"  - Primary psql:         docker exec -it {config['primary']['host']} psql -U {config['primary']['user']} -d {config['primary']['database']}")
    print(f"  - Standby psql:         docker exec -it {config['standby']['host']} psql -U {config['standby']['user']} -d {config['standby']['database']}")
    print(f"  - Stop containers:      docker compose down")
    print(f"  - View logs:            docker compose logs -f")
    print("\n")


if __name__ == "__main__":
    try:
        Main()
    except KeyboardInterrupt:
        print("\n\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

