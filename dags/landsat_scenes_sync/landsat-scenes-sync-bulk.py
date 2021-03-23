"""
# Landsat Bulk Sync automation
DAG to retrieve Landsat 5, 7 and 8 GZIP bulk data from USGS, unzip, filter and send the right ones to our SQS.

"""
# [START import_module]
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import PythonOperator

from utils.scenes_sync import retrieve_bulk_data

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
    "landsat_scenes_sync-bulk",
    default_args=DEFAULT_ARGS,
    description="Sync bulk files",
    schedule_interval=None,
    tags=["Scene", "bulk"],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    processes = []
    files = {
        "landsat_8": "LANDSAT_OT_C2_L2.csv.gz",
        "landsat_7": "LANDSAT_ETM_C2_L2.csv.gz",
        "Landsat_4_5": "LANDSAT_TM_C2_L2.csv.gz",
    }

    for sat, file in files.items():
        processes.append(
            PythonOperator(
                task_id=sat,
                python_callable=retrieve_bulk_data,
                op_kwargs=dict(file_name=file),
                dag=dag,
            )
        )

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
