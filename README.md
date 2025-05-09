# DATA_ENG 300 HW2

This project performs both relational and non-relational analysis on ICU patient data to answer healthcare questions using SQL (DuckDB) and Cassandra (Amazon Keyspaces). The work was split into two parts.

---

## File Overview

- `*.csv` – Raw MIMIC-style ICU datasets (e.g., admissions, prescriptions, patients)
- `Homework2.ipynb` – Notebook orchestrating full assignment with full explanations for all parts
- `hw2_p2_q1.py`, `hw2_p2_q2.py`, `hw2_p2_q3.py`, `hw2_p2_q3_ethnicity.py` – Part II queries using Cassandra
- `avg_stay_by_gender.py` – Standalone script for ICU stay analysis by gender
- `Dockerfile` – Jupyter container setup for EC2
- `.crt` – SSL cert file for AWS connection

---

## Analysis Questions

### Part I (Relational / DuckDB)
1. **Total Drug Usage by Ethnicity**  
   - SQL summary query with aggregation by ethnicity  
   - Bar chart for top drugs per group

2. **Procedures by Age Group**  
   - SQL query to extract top 3 procedures for 5 age brackets  
   - Sorted by frequency, joined with ICD names

3. **ICU Stay Duration by Gender/Ethnicity**  
   - Calculated stay length using `ADMITTIME` and `DISCHTIME`  
   - Grouped by gender or ethnicity  
   - Outputs include sample table and optional plot

### Part II (Non-relational / Cassandra)
Reimplements all 3 queries using Keyspaces:
- Table schema design per analysis
- Data ingestion to Keyspaces
- Querying using CQL
- Extraction verified via Jupyter or printout
- Explanation in Homework2.ipynb for any discrepancies

---

## Running the Code

### Setup Instructions
1. **Docker & Jupyter**

   docker build -t my_jupyter
   docker run -p 8888:8888 -v $(pwd):/home/jovyan/ my_jupyter

