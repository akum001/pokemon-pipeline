FROM apache/airflow:2.10.4

# Switch to root for installing system dependencies
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    vim \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


# Switch back to airflow user
USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /requirements.txt