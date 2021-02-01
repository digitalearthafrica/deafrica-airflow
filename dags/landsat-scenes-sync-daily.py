# [START import_module]
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import PythonOperator

from utils.scenes_sync import retrieve_json_data_and_send

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
    "landsat-scenes-sync-daily",
    default_args=DEFAULT_ARGS,
    description="Sync Daily",
    schedule_interval=timedelta(days=1),
    tags=["Scene", "Daily", "API"],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    # Test Start and End dates
    start_date = datetime.now().replace(day=28, month=1, year=2021)
    end_date = datetime.now()

    # start_date = DEFAULT_ARGS['start_date']
    # end_date = datetime.now()
    requested_date = start_date
    processes = []

    if not start_date or not end_date:
        count_tasks = ((end_date - start_date).days + 1)
        while count_tasks > 0:
            processes.append(
                PythonOperator(
                    task_id=f'Task-Day-{requested_date.date().isoformat()}',
                    python_callable=retrieve_json_data_and_send,
                    op_kwargs=dict(date=requested_date),
                    dag=dag,
                )
            )
            requested_date -= timedelta(days=1)
            count_tasks -= 1
    else:
        raise Exception('Start_date and End_date are required for daily JSON request.')

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
