import csv
import os
import time
from collections import defaultdict, Counter
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

def wait_for_table(session, keyspace, table, timeout=60):
    print(f"Waiting for table '{keyspace}.{table}' to become available...")
    for _ in range(timeout):
        rows = session.execute("""
            SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s
        """, [keyspace])
        if any(row.table_name == table for row in rows):
            print("Table is ready.")
            return
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for table '{table}' to be available.")

# Step 1: Connect
session = connect_to_cluster()
session.set_keyspace("de300_hw2")

# Step 2: Drop and create table
session.execute("""
    DROP TABLE IF EXISTS procedures_by_age;
""")
print("Dropped old table if existed.")

session.execute("""
    CREATE TABLE procedures_by_age (
        age_group TEXT,
        procedure_name TEXT,
        PRIMARY KEY (age_group, procedure_name)
    );
""")
print("Table created.")
wait_for_table(session, "de300_hw2", "procedures_by_age")

# Step 3: Load data
age_lookup = {}
with open("PATIENTS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        age_lookup[row["subject_id"]] = row["dob"]

procedure_lookup = {}
with open("D_ICD_PROCEDURES.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        procedure_lookup[row["icd9_code"]] = row["short_title"]

admits = {}
with open("ADMISSIONS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        admits[row["hadm_id"]] = {
            "subject_id": row["subject_id"],
            "admittime": row["admittime"]
        }

print("Loading lookups...")

# Step 4: Count procedures by age group
age_proc_count = defaultdict(Counter)

def calculate_age(dob, admit):
    try:
        dob_year = int(dob[:4])
        admit_year = int(admit[:4])
        return admit_year - dob_year
    except:
        return None

with open("PROCEDURES_ICD.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        hadm_id = row["hadm_id"]
        icd9 = row["icd9_code"]
        admit = admits.get(hadm_id)
        if admit and icd9 in procedure_lookup:
            sid = admit["subject_id"]
            dob = age_lookup.get(sid)
            if not dob:
                continue
            age = calculate_age(dob, admit["admittime"])
            if age is None:
                continue
            if age <= 19:
                age_group = "<=19"
            elif age <= 49:
                age_group = "20-49"
            elif age <= 79:
                age_group = "50-79"
            else:
                age_group = "80+"
            age_proc_count[age_group][procedure_lookup[icd9]] += 1

print("Finished counting. Skipped 3 bad rows.")

# Step 5: Insert top 3 for each age group
inserted = 0
for age_group, counter in age_proc_count.items():
    for proc_name, _ in counter.most_common(3):
        try:
            session.execute("""
                INSERT INTO procedures_by_age (age_group, procedure_name)
                VALUES (%s, %s)
            """, (age_group, proc_name))
            inserted += 1
        except Exception as e:
            print(f"Insert failed: {e}")

print(f"Inserted {inserted} rows.")

# Step 6: Verify
print("\nTop 3 procedures by age group:")
try:
    rows = session.execute("SELECT age_group, procedure_name FROM procedures_by_age")
    for row in rows:
        print(f"{row.age_group}: {row.procedure_name}")
except Exception as e:
    print(f"Query failed: {e}")

session.shutdown()

