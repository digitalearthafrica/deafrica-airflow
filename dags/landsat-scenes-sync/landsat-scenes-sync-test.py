"""
# TEST DAG
"""

# [START import_module]
import logging
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from infra.connections import SYNC_LANDSAT_CONNECTION_ID

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
    "version": "0.1",
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


def copy_file_to_s3(path: str):
    try:

        s3_hook = S3Hook(aws_conn_id=SYNC_LANDSAT_CONNECTION_ID)
        returned = s3_hook.get_conn().copy_object(
            Bucket="deafrica-landsat-dev",
            Key=path,
            CopySource={"Bucket": "usgs-landsat", "Key": path, "VersionId": None},
            ACL="public-read",
            RequestPayer="requester",
        )

        logging.info(f"RETURNED - {returned}")
    except Exception as error:
        logging.error(f"Error found: {error}")
        raise error


with dag:
    START = DummyOperator(task_id="start-tasks")

    path_list = [
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ANG.txt",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_B10.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_B7.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_QA_AEROSOL.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_B4.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_MTL.txt",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_thumb_large.jpeg",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_MTL.json",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_B6.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_B3.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_B1.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_thumb_small.jpeg",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_QA_RADSAT.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_CDIST.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_B5.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_QA_PIXEL.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_SR_B2.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_DRAD.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_ATRAN.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_MTL.xml",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_EMSD.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_TRAD.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_URAD.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_EMIS.TIF",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_QA.TIF",
    ]

    processes = []
    count = 0
    for path in path_list:
        processes.append(
            PythonOperator(
                task_id=f"TEST-{count}",
                python_callable=copy_file_to_s3,
                op_kwargs=dict(path=path),
                dag=dag,
            )
        )
        count += 1

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
