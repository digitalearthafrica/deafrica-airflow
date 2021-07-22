"""
# Read report and generate messages to fill missing scenes
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from airflow.hooks.S3_hook import S3Hook
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from odc import aws
from odc.aws import inventory
from toolz import get_in

from infra.connections import CONN_LANDSAT_WRITE
from infra.s3_buckets import (
    LANDSAT_INVENTORY_BUCKET_NAME, 
    LANDSAT_SYNC_BUCKET_NAME
)

REPORTING_PREFIX = "status-report/"
# Dev does not need to be updated
SCHEDULE_INTERVAL = None

default_args = {
    "owner": "RODRIGO",
    "start_date": datetime(2021, 6, 7),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
    "version": "0.0.1",
}

MANIFEST_FOLDER = f's3://{LANDSAT_INVENTORY_BUCKET_NAME}/deafrica-landsat/deafrica-landsat-inventory/'
BUCKET_NAME = LANDSAT_SYNC_BUCKET_NAME
SUFFIX = '.json'
PREFIX = 'collection02/level-2/standard/'


def retrieve_value_mtl(mtl_key: str):
    """
    This function fetches the MTL json file and return the RMSE values.
    """

    info = aws.s3_fetch(mtl_key)
    mtl_dict = json.loads(info)

    rmse = float(
        get_in(
            ["LANDSAT_METADATA_FILE", "LEVEL1_PROCESSING_RECORD", "GEOMETRIC_RMSE_MODEL"],
            mtl_dict,
            default=float("nan")
        )
    )
    rmse_x = float(
        get_in(
            ["LANDSAT_METADATA_FILE", "LEVEL1_PROCESSING_RECORD", "GEOMETRIC_RMSE_MODEL_X"],
            mtl_dict,
            default=float("nan")
        )
    )
    rmse_y = float(
        get_in(
            ["LANDSAT_METADATA_FILE", "LEVEL1_PROCESSING_RECORD", "GEOMETRIC_RMSE_MODEL_Y"],
            mtl_dict,
            default=float("nan")
        )
    )

    return rmse, rmse_x, rmse_y


def update_stac_sr_st(stac_path: str, rmse: str, rmse_x: str, rmse_y: str) -> None:
    if aws.s3_head_object(url=stac_path):
        info_st = aws.s3_fetch(stac_path)
        stac_dict = json.loads(info_st)
        stac_dict['properties']["landsat:rms"] = rmse
        stac_dict['properties']["landsat:rmse_x"] = rmse_x
        stac_dict['properties']["landsat:rmse_y"] = rmse_y
        aws.s3_dump(
            data=bytes(json.dumps(stac_dict).encode("UTF-8")),
            url=stac_path,
            **{'ContentType': "application/json"}
        )
        logging.info(f"{stac_path} UPDATED!")
    else:
        logging.info(f"{stac_path} NOT FOUND!")


def update_stac(path: str) -> None:
    """
    Based on the path try to update ST and SR
    """

    try:
        scene = path.split('/')[-2]

        st_key = f's3://{BUCKET_NAME}/{path}{scene}_ST_stac.json'
        sr_key = f's3://{BUCKET_NAME}/{path}{scene}_SR_stac.json'
        mtl_key = f's3://{BUCKET_NAME}/{path}{scene}_MTL.json'

        if aws.s3_head_object(url=mtl_key):
            rmse, rmse_x, rmse_y = retrieve_value_mtl(mtl_key=mtl_key)
        else:
            raise Exception(f'Error MTL {mtl_key}, not found!')

        with ThreadPoolExecutor(max_workers=2) as executor:
            tasks = [
                executor.submit(update_stac_sr_st, st_key, rmse, rmse_x, rmse_y),
                executor.submit(update_stac_sr_st, sr_key, rmse, rmse_x, rmse_y)
            ]
            [future.result() for future in as_completed(tasks) if future.result()]
    except Exception as error:
        logging.error(error)


def update(field_to_update, **kwargs):
    """
    Start function to update RMSE on already transferred stacs
    """
    
    logging.info('Starting Process')
    logging.info(f'manifest folder {MANIFEST_FOLDER} - prefix {PREFIX}, suffix {SUFFIX}')

    # Get credentials from Airflow, Remove if moving the code to another tool
    cred = S3Hook(aws_conn_id=CONN_LANDSAT_WRITE).get_session().get_credentials()

    s3 = aws.s3_client(
        creds=cred,
        region_name='af-south-1',
    )

    # Retrieve paths for the files
    list_inventory = inventory.list_inventory(
        manifest=MANIFEST_FOLDER,
        s3=s3,
        prefix=PREFIX,
        suffix=SUFFIX,
        n_threads=500,
    )

    with ThreadPoolExecutor(max_workers=500) as executor:
        logging.info(f"Starting threads")

        tasks = [
            executor.submit(update_stac, path)
            for path in set(f"{namespace.Key.rsplit('/', 1)[0]}/" for namespace in list_inventory)
        ]

        [future.result() for future in as_completed(tasks) if future.result()]


with DAG(
    "landsat_update_RMSE",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Landsat_scenes", "UPDATE", "RMSE"],
    catchup=False,
) as dag:
    START = DummyOperator(task_id="start-tasks")

    UPDATE_RMSE = PythonOperator(
                task_id=f"Landsat_update_RMSE",
                python_callable=update,
                op_args=["{{ dag_run.conf.field_to_update }}"],
            )

    END = DummyOperator(task_id="end-tasks")

    START >> UPDATE_RMSE >> END
