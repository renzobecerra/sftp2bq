import re
import os
import logging
import datetime
from datetime import timedelta

import airflow
from airflow import DAG
from airflow import models
from airflow import configuration
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.utils.dates import days_ago

from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import gcs_to_bq, gcs_to_gcs
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectUpdatedSensor

"""
GCS trigger (cloudfunction) >> GCS file to Big Query
Files dropped into gcs bucket will trigger this DAG and sink to big query table
"""
default_args={
    'owner': 'renzo',
    'depends_on_past': False,
    'start_date':days_ago(0),#datetime.datetime.now(),#days_ago(0),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'max_active_runs': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# Input Params
compression_type='GZIP', # or default value 'NONE' 
field_delimiter_type="\t",
source_bucket='bc_omniture'
destination_dataset='omniture'
destination_table='omniture_raw'
del_bucket = 'bc_omniture_del'
write_disposition_type='WRITE_APPEND' 
bq_schema=[
    {'name': 'username', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'date_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'visid_high', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'visid_low', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'post_visid_high', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'post_visid_low', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'hit_time_gmt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'hitid_high', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'hitid_low', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'first_hit_time_gmt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'last_hit_time_gmt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'visit_start_time_gmt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'visit_num', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'ip', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'purchaseid', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'exclude_hit', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'hit_source', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'browser_height', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'browser_width', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'user_agent', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'geo_city', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'geo_country', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'geo_region', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'geo_dma', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'geo_zip', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'page_url', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'pagename', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'ref_domain', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'visit_start_page_url', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'prop11', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_campaign', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'va_closer_detail', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'va_closer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'va_finder_detail', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'va_finder_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'post_evar5', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar15', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar18', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar19', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar21', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar22', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar24', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar28', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar36', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar39', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar42', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_evar62', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'event_list', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_list', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'post_product_list', 'type': 'STRING', 'mode': 'NULLABLE'}
]

def get_source(**kwargs):
	"""
	gets file_name from DAG context and xcom_pushes
	"""
	split = "\/(.*?)\/"
	context_id = kwargs["dag_run"].conf['id']
	gcs_file_name = re.search(split, context_id).group(1)
	task_instance = kwargs['task_instance']
	task_instance.xcom_push(value=gcs_file_name, key='file_name')

def branch(**kwargs):
    task_instance = kwargs['task_instance']
    file_name = task_instance.xcom_pull(task_ids='get_file', key='file_name')
    print("FILE NAME: "+file_name)
    if file_name.endswith('.tsv.gz'):
        return 'sink_bq'
    else:
        return 'move_meta_file'

with airflow.DAG(
    'omniture_gcs_to_bigquery',
    default_args=default_args,
    description='GCS trigger to bigquery',
    schedule_interval=None) as dag:
    
    # Runs get_source function to save file name to xcom
    get_file = PythonOperator(task_id="get_file", python_callable=get_source, provide_context=True)

    gcs_bucket_sensor = GoogleCloudStorageObjectUpdatedSensor(
    	task_id='gcs_bucket_sensor', 
    	bucket=source_bucket,
    	ts_func=context['dag'].context['execution_date'],
    	object="{{ task_instance.xcom_pull(task_ids='get_file', key='file_name') }}", 
    	provide_context=True)

    fork = BranchPythonOperator(task_id='fork', python_callable=branch, provide_context=True)

    sink_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='sink_bq',
        bucket=source_bucket,
        source_objects=["{{ task_instance.xcom_pull(task_ids='get_file', key='file_name') }}"], # pulls file_name
        destination_project_dataset_table='omniture.omniture_raw_2017_2027',
        schema_fields=bq_schema,
        compression=compression_type,
        field_delimiter=field_delimiter_type,
        write_disposition=write_disposition_type,
        provide_context=True
        )

    move_sink_file = gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='move_sink_file',
        source_bucket=source_bucket,
        source_object="{{ task_instance.xcom_pull(task_ids='get_file', key='file_name') }}",
        destination_bucket=del_bucket,
        destination_object='processed_sink_files/'+"{{ task_instance.xcom_pull(task_ids='get_file', key='file_name') }}",
        move_object=True,
        provide_context=True,
        trigger_rule='none_failed'
        )

    move_meta_file = gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id='move_meta_file',
        source_bucket=source_bucket,
        source_object="{{ task_instance.xcom_pull(task_ids='get_file', key='file_name') }}",
        destination_bucket=del_bucket,
        destination_object='processed_meta_files/'+"{{ task_instance.xcom_pull(task_ids='get_file', key='file_name') }}",
        move_object=True,
        provide_context=True,
        trigger_rule='none_failed'
        )

    job_complete = DummyOperator(task_id='job_complete', dag=dag, trigger_rule='none_failed')

    get_file >> gcs_bucket_sensor >> fork
    fork >> move_meta_file >> job_complete
    fork >> sink_bq >> move_sink_file >> job_complete

    


