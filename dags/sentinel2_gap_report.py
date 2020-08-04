"""
# Generate a gap report between sentinel-cogs and deafica-sentinel-2 buckets

This DAG runs once a month and creates a gap report in the folowing location:
s3://deafrica-sentinel-2/monthly-status-report
"""
import json
import csv
from datetime import datetime

from airflow import configuration
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.inventory import s3

default_args = {
    'owner': 'Airflow',
    "start_date": datetime(2020, 7, 24),
    'email': ['toktam.ebadi@ga.gov.au'],
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 0,
    'manifest_suffix': "manifest.json",
    'africa_conn_id': "deafrica_CPT",
    "us_conn_id": "deafrica_oregon",
    "dest_bucket_name": "s3://deafrica-sentinel-2-inventory",
    "src_bucket_name": "s3://sentinel-cogs-inventory",
    "reporting_bucket": "s3://deafrica-sentinel-2",
    "reporting_prefix": "monthly-status-report/",
    "schedule_interval": "@monthly"
}

def generate_bucket_diffs():
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2/monthly-status-report
    """
    url_source = default_args['src_bucket_name']
    url_destination = default_args['dest_bucket_name']
    suffix = default_args['manifest_suffix']

    source_keys = []
    destination_keys = []

    s3_inventory = s3(url_destination, default_args['us_conn_id'], 'af-south-1', suffix)
    for bucket, key, *rest in s3_inventory.list_keys():
            destination_keys.append(key)

    s3_inventory = s3(url_source, default_args['us_conn_id'], 'us-west-2', suffix)
    for bucket, key, *rest in s3_inventory.list_keys():
            source_keys.append(key)

    source_keys = set(source_keys)
    destination_keys = set(destination_keys)

    diff =  [x for x in (source_keys - destination_keys)]

    output_filename = datetime.today().strftime("%d/%m/%Y %H:%M:%S") + ".json"
    output_filename = output_filename.replace("/", "_").replace(":", "_").replace(" ", "_")

    reporting_bucket = default_args['reporting_bucket']
    key = default_args['reporting_prefix'] + output_filename

    print(f"{len(diff)} files are missing from {reporting_bucket}")
    s3_report = s3(reporting_bucket, default_args['africa_conn_id'], 'af-south-1')
    s3_report.s3.put_object(Bucket=s3_report.bucket, Key=key, Body=str(json.dumps({'Keys': diff})))

with DAG('sentinel-2_gap_detection', default_args=default_args,
         schedule_interval=default_args["schedule_interval"],
         tags=["Sentinel-2", "status"], catchup=False) as dag:

    READ_INVENTORIES = PythonOperator(
        task_id='read_inventories',
        python_callable=generate_bucket_diffs)

    READ_INVENTORIES
