# Homework 3 – DE300: MapReduce and SVM

This project contains two main parts:

- **Part 1**: TF-IDF computation using PySpark and RDDs.
- **Part 2**: Soft-margin Support Vector Machine (SVM) loss and prediction implementation.

---

## How to Build and Run with Docker

1. **Build the Docker image**:
   ```bash
   docker build -t jupyter/pyspark-notebook .
   ```

2. **Run the container**:
   ```bash
   docker run -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/pyspark-notebook
   ```

3. **Open JupyterLab**:
   - Copy and past the given link with the token into your browser

---

## Files

- `hw3_mapreduce_docker.ipynb` – ipynb on the docker (does not contain outputs because I couldn't run part 1 on the EC2 instance)
- `homework3_de300_local.ipynb` – ipynb from local machine (does contain outputs)
- `agnews_clean.csv`, `w.csv`, `bias.csv`, `data_for_svm.csv` – Downloaded data files
- `Dockerfile` – Container definition to run Spark and JupyterLab
- `homework3_de300_writeup.pdf` – Written answers and explanations as well as outputs from local

---

## Summary of Work

### Part 1: TF-IDF with PySpark

- Grouped TF-IDF values by document and displayed the top 5 keywords.
- Calculated the scores using the equations provided on the sheet

### Part 2: SVM Loss and Prediction with NumPy

- Implemented the soft-margin SVM objective using the equation from the sheet
- Used matrix operations in NumPy for performance.
- Computed predictions using the SVM classifier against the true labels.

---

## Generative AI Disclosure

I used ChatGPT to:

- Understand how to use the `PipelinedRDD` structure (Part 1).
- How to use `collect()` to find the IDF values (Part 1)
- Understand and correctly apply the soft-margin SVM loss equation and prediction rule using NumPy (Part 2).
- Analyze the given equations (which were in a slightly confusing format due to the HTML; Both parts)


