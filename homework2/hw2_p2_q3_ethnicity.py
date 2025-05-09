import csv
import os
import time
from collections import defaultdict, Counter
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
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

# Step 1: Connect to Cassandra
session = connect_to_cluster()
session.set_keyspace("de300_hw2")

# Step 2: Create table
session.execute("""
CREATE TABLE IF NOT EXISTS stays_by_ethnicity (
    patient_id TEXT,
    ethnicity TEXT,
    icustay_id TEXT,
    intime TIMESTAMP,
    outtime TIMESTAMP,
    PRIMARY KEY (ethnicity, patient_id, icustay_id)
);
""")
print("Table ensured.")

# Step 3: Load ethnicity from ADMISSIONS.csv
demographics = {}
with open("ADMISSIONS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        demographics[row["subject_id"]] = row["ethnicity"]

# Step 4: Insert merged ICU data
inserted = 0
skipped = 0
with open("ICUSTAYS.csv", newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        sid = row["subject_id"]
        ethnicity = demographics.get(sid)
        if ethnicity:
            try:
                session.execute("""
                    INSERT INTO stays_by_ethnicity (patient_id, ethnicity, icustay_id, intime, outtime)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    sid, ethnicity, row["icustay_id"], row["intime"], row["outtime"]
                ))
                inserted += 1
            except Exception as e:
                skipped += 1
        else:
            skipped += 1

print(f"Inserted {inserted} rows. Skipped {skipped}.")

# Step 5: Compute average LOS per ethnicity
durations = defaultdict(float)
counts = defaultdict(int)

rows = session.execute("SELECT ethnicity, intime, outtime FROM stays_by_ethnicity")
for row in rows:
    if row.intime and row.outtime:
        try:
            delta = row.outtime - row.intime
            days = delta.total_seconds() / (3600.0 * 24)
            durations[row.ethnicity] += days
            counts[row.ethnicity] += 1
        except:
            continue

# Step 6: Build dataframe
ethnicities = []
avg_days = []
num_cases = []

for eth in durations:
    ethnicities.append(eth)
    avg_days.append(round(durations[eth] / counts[eth], 2))
    num_cases.append(counts[eth])

df_ethnicity = pd.DataFrame({
    "ethnicity": ethnicities,
    "avg_icu_los_days": avg_days,
    "num_cases": num_cases
}).sort_values(by="avg_icu_los_days", ascending=False)

# Step 7: Plot
plt.figure(figsize=(10, 6))
df_ethnicity.plot.bar(
    x='ethnicity',
    y='avg_icu_los_days',
    legend=False,
    color='mediumseagreen',
    figsize=(10, 6)
)
plt.title("Average ICU LOS by Ethnicity")
plt.xlabel("Ethnicity")
plt.ylabel("Average ICU LOS (days)")
plt.xticks(rotation=45, ha='right')
for idx, val in enumerate(df_ethnicity['avg_icu_los_days']):
    plt.text(idx, val + 0.1, str(val), ha='center')
plt.tight_layout()
plt.show()

session.shutdown()

