FROM apache/airflow:2.9.2-python3.12

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    git \
    gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for Python package installation
USER airflow

# Install Airflow providers and dependencies
RUN pip install --no-cache-dir \
    apache-airflow[amazon] \
    apache-airflow-providers-slack \
    apache-airflow[statsd] \
    soda-core-redshift \
    pandas \
    boto3 


# Copy requirements files
USER root
COPY requirements.txt /
COPY requirements_dbt_venv.txt /tmp/

# Install regular Python dependencies
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Install dbt into a virtual environment
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir --no-user -r /tmp/requirements_dbt_venv.txt && \
    deactivate