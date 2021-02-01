# [START import_module]
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import PythonOperator
from dags.utils.rodrigo import retrieve_bulk_data

# [END import_module]

# [START default_args]
DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 27),
    # "schedule_interval": "@once",
    "us_conn_id": "prod-eks-s2-data-transfer",
    "africa_conn_id": "deafrica-prod-migration",
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat-scenes-sync-bulk",
    default_args=DEFAULT_ARGS,
    description="Sync bulk files",
    schedule_interval=timedelta(days=1),
    tags=["Scene", "bulk"],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    processes = []
    files = {
        'landsat_8': 'LANDSAT_OT_C2_L2.csv.gz',
        'landsat_7': 'LANDSAT_ETM_C2_L2.csv.gz',
        'Landsat_4_5': 'LANDSAT_TM_C2_L2.csv.gz'
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
