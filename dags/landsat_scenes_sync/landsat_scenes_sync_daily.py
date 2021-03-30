"""
# Landsat Bulk Sync automation
DAG to retrieve periodically Landsat 5, 7 and 8 JSON data from USGS API, filter and send the right ones to our SQS.
"""

# [START import_module]
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import PythonOperator

from utils.scenes_sync import retrieve_json_data_and_send_daily

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
    "start_date": datetime(2021, 3, 29),
    "catchup": False,
    "version": "0.2",
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat_scenes_sync_daily",
    default_args=DEFAULT_ARGS,
    description="Sync Daily",
    schedule_interval="@daily",  # Run once a day at midnight
    tags=["Scene", "Daily", "API"],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    start_date = datetime.now() - timedelta(days=12)
    today = datetime.now()
    requested_date = start_date
    processes = []

    count_tasks = (today - start_date).days + 1
    while count_tasks > 0:
        processes.append(
            PythonOperator(
                task_id=f"Task-Day-{requested_date.date().isoformat()}",
                python_callable=retrieve_json_data_and_send_daily,
                op_kwargs=dict(date=requested_date),
            )
        )
        requested_date += timedelta(days=1)
        count_tasks -= 1

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
