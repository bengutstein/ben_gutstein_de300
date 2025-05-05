import boto3
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra_sigv4.auth import SigV4AuthProvider
import os
import time
import uuid
import csv

def connect_to_cluster():
    # TLS config
    cert_path = os.path.expanduser("~/sf-class2-root.crt")
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations(cert_path)
    ssl_context.verify_mode = CERT_REQUIRED

    # AWS auth
    boto_session  = boto3.Session(region_name="us-east-1")  # or "us-east-2"
    auth_provider = SigV4AuthProvider(boto_session)

    # Cassandra config
    exec_profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    cluster = Cluster(
        ["cassandra.us-east-1.amazonaws.com"],  # or use us-east-2
        port=9142,
        ssl_context=ssl_context,
        auth_provider=auth_provider,
        execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    )
    return cluster.connect()

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS de300_demo
        WITH replication = {'class': 'SingleRegionStrategy'};
    """)
    print("✅ Keyspace creation request sent.")

def use_keyspace(session):
    session.set_keyspace("de300_demo")
    print("✅ Switched to de300_demo keyspace.")

def create_github_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS github (
            id UUID PRIMARY KEY,
            name TEXT,
            username TEXT
        );
    """)
    print("✅ Table 'github' created.")

def insert_github_row(session, name, username):
    session.execute(
        "INSERT INTO github (id, name, username) VALUES (%s, %s, %s)",
        (uuid.uuid4(), name, username),
    )
    print(f"✅ Inserted: {name} ({username})")

def export_github_to_csv(session, filename="github_rows.csv"):
    rows = session.execute("SELECT * FROM github")
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "username"])
        for row in rows:
            writer.writerow([row.id, row.name, row.username])
    print(f"✅ Exported data to {filename}")

# --- MAIN LOGIC ---
if __name__ == "__main__":
    session = connect_to_cluster()

    # Uncomment one of the following at a time:

    # Step 1: Create the keyspace
    # create_keyspace(session)

    # Step 2: After ~10–20 sec delay, switch to it
    use_keyspace(session)

    create_github_table(session)
    insert_github_row(session, "Ben Gutstein", "bengutstein")

    export_github_to_csv(session)
