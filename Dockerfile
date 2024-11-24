FROM apache/airflow:2.9.3

# Switch to root to install packages
USER root

# Install git
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to airflow user
USER airflow

# Copy requirements and install Python packages
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt