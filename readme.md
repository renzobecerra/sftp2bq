# GCP Pipeline: SFTP >> GCS >> Big Query

Pipeline triggered by an `SFTP` file dump.

#### Set up steps:
1. SFTP server
2. GCS bucket mirroring/sync on SFTP server
3. Webhook trigger using cloud functions
4. Airflow DAG (Composer)


#### Pipeline Instructions
A detailed walkthrough of how to set up a pipeline triggered by a file `PUT` to 
an sftp server can be found in 
[pipeline_instructions.ipynb](pipeline_instructions.ipynb)

- Cloud Function Trigger: [cloud_function_trigger.py](cloud_function_trigger.py)

- SFTP SSH permissions: [DAG_airflow_example.py](sftp_ssh_permissions.txt)

- Airflow DAG example file: [DAG_airflow_example.py](DAG_airflow_example.py)
