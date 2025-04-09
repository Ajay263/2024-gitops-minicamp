FROM apache/airflow:2.9.2-python3.12

# Install system dependencies and Python packages in a single layer
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        make \
        git \
        gosu \
        python3-distutils \
        libpq-dev \
        postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    # Switch to airflow user for Python package installation
    gosu airflow pip install --no-cache-dir \
        apache-airflow[amazon] \
        apache-airflow-providers-slack \
        apache-airflow[statsd] \
        elementary-data[redshift] \
        pandas \
        boto3

# Copy requirements files
COPY --chown=airflow:airflow requirements.txt requirements_dbt_venv.txt /tmp/

# Install main requirements, create and populate venv environments in one layer
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    # Create and set up dbt environment
    python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir --no-user -r /tmp/requirements_dbt_venv.txt && \
    deactivate && \
    # Create and set up soda environment
    python -m venv soda_venv && \
    . soda_venv/bin/activate && \
    pip install --no-cache-dir -i https://pypi.cloud.soda.io soda-redshift && \
    deactivate

# Set correct permissions for elementary package
USER root
RUN chown -R airflow:airflow /opt/airflow/dbt_venv/lib/python3.12/site-packages/elementary && \
    chmod -R u+w /opt/airflow/dbt_venv/lib/python3.12/site-packages/elementary

# Final user setting
USER airflow