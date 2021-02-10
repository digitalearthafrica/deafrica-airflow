# [START import_module]
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from utils.scenes_sync_process import process

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
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat-scenes-process",
    default_args=DEFAULT_ARGS,
    description="Process Queue Messages",
    schedule_interval=None,
    tags=["Scene"],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    retrieve_messages = PythonOperator(
        task_id=f"Read_messages",
        python_callable=process,
        op_kwargs=dict(),
        dag=dag,
    )

    # processes = []
    #
    # count_tasks = ((end_date - start_date).days + 1)
    # while count_tasks > 0:
    #     processes.append(
    #         PythonOperator(
    #             task_id=f'Task-Day-{requested_date.date().isoformat()}',
    #             python_callable=retrieve_json_data_and_send,
    #             op_kwargs=dict(date=requested_date),
    #             dag=dag,
    #         )
    #     )
    #     requested_date -= timedelta(days=1)
    #     count_tasks -= 1

    END = DummyOperator(task_id="end-tasks")

    START >> retrieve_messages >> END
