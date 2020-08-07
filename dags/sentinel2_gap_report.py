"""
# Generate a gap report between sentinel-cogs and deafica-sentinel-2 buckets

This DAG runs once a month and creates a gap report in the folowing location:
s3://deafrica-sentinel-2/monthly-status-report
"""
import json
import csv
import sys
from pathlib import Path
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
    'africa_tiles': "data/africa-mgrs-tiles.csv",
    'africa_conn_id': "deafrica-prod-migration",
    "us_conn_id": "deafrica_migration_oregon",
    "dest_bucket_name": "s3://deafrica-sentinel-2-inventory",
    "src_bucket_name": "s3://sentinel-cogs-inventory",
    "reporting_bucket": "s3://deafrica-sentinel-2",
    "reporting_prefix": "monthly-status-report/",
    "schedule_interval": "@monthly"
}

def generate_buckets_diff():
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2/monthly-status-report
    """
    url_source = default_args['src_bucket_name']
    url_destination = default_args['dest_bucket_name']
    suffix = default_args['manifest_suffix']
    africa_tile_ids_path = Path(configuration.get('core', 'dags_folder')). \
                        parent.joinpath(default_args['africa_tiles'])
    cogs_folder = "sentinel-s2-l2a-cogs"
    diff = []
    destination_keys = []

    # Read Africa tile ids
    africa_tile_ids = []
    with open(africa_tile_ids_path, 'r') as file:
        ids = csv.reader(file)
        africa_tile_ids = [row[0] for row in ids]
        s3_inventory = s3(url_destination, default_args['africa_conn_id'], 'af-south-1', suffix)
        for bucket, key, *rest in s3_inventory.list_keys(cogs_folder):
            destination_keys.append(key)

    destination_keys = set(destination_keys)
    s3_inventory = s3(url_source, default_args['us_conn_id'], 'us-west-2', suffix)
    for bucket, key, *rest in s3_inventory.list_keys(cogs_folder):
        if key.startswith('sentinel-s2-l2a-cogs') and len(key.split("/")) > 2 and \
           key.split("/")[2].split("_")[1] in africa_tile_ids and key not in destination_keys:
           diff.append(key)

    diff = [[x] for x in set(diff)]
    output_filename = datetime.today().strftime("%d_%m_%Y_%H_%M_%S") + ".json"
    reporting_bucket = default_args['reporting_bucket']
    key = default_args['reporting_prefix'] + output_filename

    print(f"{len(diff)} files are missing from {reporting_bucket}")
    s3_report = s3(reporting_bucket, default_args['africa_conn_id'], 'af-south-1')
    s3_report.s3.put_object(Bucket=s3_report.bucket, Key=key, Body=str(json.dumps({'Keys': diff})))

with DAG('sentinel-2_gap_detection', default_args=default_args,
         schedule_interval=default_args["schedule_interval"],
         tags=["Sentinel-2", "status"], catchup=False) as dag:

    READ_INVENTORIES = PythonOperator(
        task_id='compare_s2_inventories',
        python_callable=generate_buckets_diff)

    READ_INVENTORIES
