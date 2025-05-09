import csv
import os
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra_sigv4.auth import SigV4AuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
import boto3

def connect_to_cluster():
    cert_path = os.path.expanduser("~/sf-class2-root.crt")
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations(cert_path)
    ssl_context.verify_mode = CERT_REQUIRED

    boto_session = boto3.Session(region_name="us-east-1")
    auth_provider = SigV4AuthProvider(boto_session)

    exec_profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    cluster = Cluster(
        ["cassandra.us-east-1.amazonaws.com"],
        port=9142,
        ssl_context=ssl_context,
        auth_provider=auth_provider,
        execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile}
    )
    return cluster.connect()

# Step 1: Connect
session = connect_to_cluster()
session.set_keyspace("de300_hw2")

# Step 2: Create merged table
session.execute("""
CREATE TABLE IF NOT EXISTS stays_by_gender (
    patient_id TEXT,
    gender TEXT,
    icustay_id TEXT,
    intime TIMESTAMP,
    outtime TIMESTAMP,
    PRIMARY KEY (gender, patient_id, icustay_id)
);
""")
print("✅ Table created.")

# Step 3: Load gender from PATIENTS.csv
gender_lookup = {}
with open("PATIENTS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        gender_lookup[row["subject_id"]] = row["gender"]

# Step 4: Load ICUSTAYS.csv and merge
inserted = 0
skipped = 0

with open("ICUSTAYS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        sid = row["subject_id"]
        gender = gender_lookup.get(sid)
        if gender:
            try:
                session.execute("""
                    INSERT INTO stays_by_gender (patient_id, gender, icustay_id, intime, outtime)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    sid, gender, row["icustay_id"], row["intime"], row["outtime"]
                ))
                inserted += 1
            except Exception as e:
                print(f"⚠️ Insert error: {e}")
                skipped += 1
        else:
            skipped += 1

print(f"✅ Inserted {inserted} rows. Skipped {skipped}.")

# Step 5: Query a few rows
rows = session.execute("SELECT * FROM stays_by_gender WHERE gender = 'F' LIMIT 10;")
print("📋 Sample rows for gender = 'F':")
for row in rows:
    print(row)

session.shutdown()

