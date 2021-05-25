"""
# Landsat Scene Identification and Sync

Retrieves Landsat 5, 7 and 8 GZIP bulk CSVs from USGS, filters out the scenes we are interested in
by region and processing date, and submits them to our processing SQS.

"""
# [START import_module]
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import PythonOperator

from utils.landsat_scenes_identifying_logic import identifying_data

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
    "start_date": datetime(2021, 5, 8),
    "version": "0.16",
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat_scenes_identifying",
    default_args=DEFAULT_ARGS,
    description="Identify scenes and Sync",
    schedule_interval="@daily",
    catchup=True,
    tags=[
        "Scene",
    ],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    processes = []
    files = {
        "landsat_8": "LANDSAT_OT_C2_L2.csv.gz",
        # "landsat_7": "LANDSAT_ETM_C2_L2.csv.gz",
        # "Landsat_4_5": "LANDSAT_TM_C2_L2.csv.gz",
    }

    for sat, file in files.items():
        processes.append(
            PythonOperator(
                task_id=sat,
                python_callable=identifying_data,
                op_kwargs=dict(file_name=file, date_to_process="{{ ds }}"),
            )
        )

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
