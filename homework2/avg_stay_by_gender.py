import os
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra_sigv4.auth import SigV4AuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
import boto3
from datetime import datetime

def connect_to_cluster():
    cert_path = os.path.expanduser("~/sf-class2-root.crt")
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations(cert_path)
    ssl_context.verify_mode = CERT_REQUIRED

    boto_session = boto3.Session(region_name="us-east-2")
    auth_provider = SigV4AuthProvider(boto_session)

    exec_profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    cluster = Cluster(
        ["cassandra.us-east-2.amazonaws.com"],
        port=9142,
        ssl_context=ssl_context,
        auth_provider=auth_provider,
        execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile}
    )
    return cluster.connect()

# --- MAIN EXECUTION ---
session = connect_to_cluster()
session.set_keyspace("de300_hw2")

# Step 1: Query all rows
rows = session.execute("SELECT gender, intime, outtime FROM stays_by_gender;")

# Step 2: Compute total duration and count by gender
from collections import defaultdict
import datetime

durations = defaultdict(float)
counts = defaultdict(int)

for row in rows:
    if row.intime and row.outtime:
        try:
            delta = row.outtime - row.intime
            days = delta.total_seconds() / (3600.0 * 24)
            durations[row.gender] += days
            counts[row.gender] += 1
        except Exception as e:
            print(f"⚠️ Error computing duration for row: {e}")

# Step 3: Print averages
print("\nAverage ICU Stay Duration by Gender (in days):")
for gender in durations:
    avg = durations[gender] / counts[gender] if counts[gender] else 0
    print(f"{gender}: {avg:.2f} days ({counts[gender]} stays)")

session.shutdown()
