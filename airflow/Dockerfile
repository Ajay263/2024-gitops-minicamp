FROM apache/airflow:2.9.2-python3.12

# Install system dependencies and clean up in one layer
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    git \
    gosu \
    python3-distutils \
    libpq-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user and install Python packages
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow[amazon] \
    apache-airflow-providers-slack \
    apache-airflow[statsd] \
    pandas \
    boto3

# Copy and install requirements
COPY requirements.txt /
COPY requirements_dbt_venv.txt /tmp/

# Install requirements
RUN pip install --no-cache-dir -r /requirements.txt

# Create and populate dbt virtual environment
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir --no-user -r /tmp/requirements_dbt_venv.txt && \
    deactivate

# Create and populate soda virtual environment
RUN python -m venv soda_venv && \
    . soda_venv/bin/activate && \
    pip install -i https://pypi.cloud.soda.io soda-redshift && \
    deactivate

# Final user setting
USER airflow