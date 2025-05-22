# I used a very similar Dockerfile to Homework 2, I was nervous about messing up the Docker build, so I played it safe with what I knew worked.

FROM jupyter/base-notebook

# Set working directory
WORKDIR /home/jovyan

# Expose Jupyter port
EXPOSE 8888

# Prevent prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Copy all local files to image
COPY . .

# Install necessary system tools
USER root
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    bzip2 \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# OPTIONAL: Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip ./aws

# Install Python dependencies
RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    jupyterlab \
    psycopg2-binary \
    cassandra-driver \
    cassandra-sigv4 \
    duckdb \
    boto3 \
    matplotlib

USER jovyan

# Launch notebook server
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''", "--allow-root"]