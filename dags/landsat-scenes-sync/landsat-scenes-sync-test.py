"""
# TEST DAG
"""

# [START import_module]
import logging
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from utils.scenes_sync import ScenesSync

# [END import_module]


# [START default_args]
DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 2),
    "catchup": False,
    "version": "0.1"
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat-scenes-sync-tests",
    default_args=DEFAULT_ARGS,
    description="Sync Tests",
    schedule_interval=None,
    tags=["TESTs"],
)
# [END instantiate_dag]


def copy_file_to_s3():
    try:
        s3_hook = S3Hook(aws_conn_id=SYNC_LANDSAT_CONNECTION_ID)

        returned = s3_hook.get_conn().copy_object(
            Bucket="deafrica-landsat-dev",
            Key='collection02/level-2/standard/etm/1999/169/075/LE07_L2SP_169075_19991108_20200918_02_T1/LE07_L2SP_169075_19991108_20200918_02_T1_ST_QA.TIF',
            CopySource={
                "Bucket": "usgs-landsat",
                "Key": 'collection02/level-2/standard/etm/1999/169/075/LE07_L2SP_169075_19991108_20200918_02_T1/LE07_L2SP_169075_19991108_20200918_02_T1_ST_QA.TIF',
                "VersionId": None
            },
            ACL="public-read",
            RequestPayer='requester',
        )

        logging.info(f'RETURNED - {returned}')
    except Exception as error:
        logging.error(f'Error found: {error}')
        raise error


with dag:
    START = DummyOperator(task_id="start-tasks")

    process = PythonOperator(
        task_id=f"TEST",
        python_callable=copy_file_to_s3,
        op_kwargs=dict(),
        dag=dag,
    )

    END = DummyOperator(task_id="end-tasks")

    START >> process >> END
