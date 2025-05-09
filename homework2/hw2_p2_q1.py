import csv
import os
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

# Step 1: Connect
session = connect_to_cluster()
session.set_keyspace("de300_hw2")

# Step 2: Load ethnicity from ADMISSIONS.csv
ethnicity_lookup = {}
with open("ADMISSIONS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        ethnicity_lookup[row["subject_id"]] = row["ethnicity"]

# Step 3: Insert merged records into Keyspaces
inserted = 0
skipped = 0

with open("PRESCRIPTIONS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        sid = row["subject_id"]
        drug = row["drug"]
        ethnicity = ethnicity_lookup.get(sid)
        if ethnicity and drug:
            try:
                session.execute("""
                    INSERT INTO ethnicity_drug_prescriptions (subject_id, ethnicity, drug)
                    VALUES (%s, %s, %s)
                """, (sid, ethnicity, drug))
                inserted += 1
            except Exception as e:
                print(f"Insert error: {e}")
                skipped += 1
        else:
            skipped += 1

print(f"Inserted {inserted} rows. Skipped {skipped}.")

# Step 4: Query all rows and compute most common drug per ethnicity
rows = session.execute("SELECT ethnicity, drug FROM ethnicity_drug_prescriptions")
ethnicity_drug_counts = defaultdict(Counter)

for row in rows:
    ethnicity = row.ethnicity
    drug = row.drug
    ethnicity_drug_counts[ethnicity][drug] += 1

# Step 5: Print results
print("\nMost prescribed drug per ethnicity:")
for ethnicity, drug_counts in ethnicity_drug_counts.items():
    top_drug, count = drug_counts.most_common(1)[0]
    print(f"{ethnicity}: {top_drug} ({count} prescriptions)")

session.shutdown()

