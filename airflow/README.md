1. create .env file  

`echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`


2. Create REdshift connections

`
Connection Id: redshift_conn
Connection Type: Amazon Redshift
Host: Your Redshift Serverless endpoint
Schema: nexabrands_datawarehouse (this is your dbname in the DAG)
Login: Your Redshift username
Password: Your Redshift password
Port: 5439 (default Redshift port)

`
