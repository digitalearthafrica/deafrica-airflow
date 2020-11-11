"""
# Generate a gap report between sentinel-cogs and deafica-sentinel-2 buckets

This DAG runs once a month and creates a gap report in the folowing location:
s3://deafrica-sentinel-2/monthly-status-report
"""

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import re

import pandas as pd
from airflow import DAG, configuration
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

from utils.inventory import s3

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 7, 24),
    "email": ["toktam.ebadi@ga.gov.au"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
    "manifest_suffix": "manifest.json",
    "africa_tiles": "data/africa-mgrs-tiles.csv",
    "africa_conn_id": "deafrica-prod-migration",
    "us_conn_id": "deafrica_migration_oregon",
    "dest_bucket_name": "s3://deafrica-sentinel-2-inventory",
    "src_bucket_name": "s3://sentinel-cogs-inventory",
    "reporting_bucket": "s3://deafrica-sentinel-2",
    "reporting_prefix": "monthly-status-report/",
    "schedule_interval": "@monthly",
}


def generate_buckets_diff():
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2/monthly-status-report
    """
    url_source = default_args["src_bucket_name"]
    url_destination = default_args["dest_bucket_name"]
    suffix = default_args["manifest_suffix"]
    cogs_folder_name = "sentinel-s2-l2a-cogs"
    source_keys = set()

    africa_tile_ids = set(
        pd.read_csv(
            "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"
        ).values.ravel()
    )

    s3_inventory = s3(url_source, default_args["us_conn_id"], "us-west-2", suffix)
    print(f"Processing keys from the inventory file: {s3_inventory.url}")

    for bucket, key, *rest in s3_inventory.list_keys():
        if (
            ".json" in key
            and key.startswith(cogs_folder_name)
            and key.split("/")[-2].split("_")[1] in africa_tile_ids
            # We need to ensure we're ignoring the old format data
            and re.match("sentinel-s2-l2a-cogs\/\d{4}\/", key) is None
        ):
            source_keys.add(key)

    s3_inventory = s3(
        url_destination, default_args["africa_conn_id"], "af-south-1", suffix
    )
    print(f"Processing keys from the inventory file: {s3_inventory.url}")

    for bucket, key, *rest in s3_inventory.list_keys():
        if ".json" in key and key.startswith(cogs_folder_name):
            if key in source_keys:
                source_keys.remove(key)

    missing_scenes = [f"{default_args['src_bucket_name']}/{key}" for key in source_keys]

    output_filename = datetime.today().isoformat() + ".txt"
    reporting_bucket = default_args["reporting_bucket"]
    key = default_args["reporting_prefix"] + output_filename

    print(f"{len(missing_scenes)} files are missing from {reporting_bucket}")
    s3_report = s3(reporting_bucket, default_args["africa_conn_id"], "af-south-1")
    s3_report.s3.put_object(
        Bucket=s3_report.bucket, Key=key, Body="\n".join(missing_scenes)
    )
    print(f"Wrote inventory to: {default_args['reporting_bucket']}/{key}")


with DAG(
    "sentinel-2_gap_detection",
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    tags=["Sentinel-2", "status"],
    catchup=False,
) as dag:

    READ_INVENTORIES = PythonOperator(
        task_id="compare_s2_inventories", python_callable=generate_buckets_diff
    )

    READ_INVENTORIES
