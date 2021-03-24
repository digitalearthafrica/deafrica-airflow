"""
# Landsat Process automation
DAG to periodically retrieve Landsat 5, 7 and 8 data from SQS, process, migrate data from USGS to Africa,
generate stac 1.0 and send to SNS.

"""

# [START import_module]
import time
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from utils.scenes_sync_process import process
from utils.url_request_utils import time_process

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
    "limit_of_processes": 30,
    "version": "0.3",
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


def terminate(ti, start_timer, **kwargs):
    successful_msg_counts = 0
    failed_msg_counts = 0

    for idx in range(DEFAULT_ARGS["limit_of_processes"]):
        successful_msg_counts += ti.xcom_pull(
            key="successful", task_ids=f"data_transfer_{idx}"
        )
        failed_msg_counts += ti.xcom_pull(key="failed", task_ids=f"data_transfer_{idx}")

    print(
        f"{successful_msg_counts} were successfully processed, and {failed_msg_counts} failed"
    )
    print(f"Message processed and sent in {time_process(start=start_timer)}")


with dag:

    START = DummyOperator(task_id="start-tasks")

    END = PythonOperator(
        task_id="end-tasks",
        python_callable=terminate,
        op_kwargs=dict(start_timer=time.time()),
        provide_context=True,
    )

    retrieve_messages = [
        PythonOperator(
            task_id=f"Processing-Messages-DEAfrica-{idx}",
            python_callable=process,
            op_kwargs=dict(),
            dag=dag,
        )
        for idx in range(DEFAULT_ARGS["limit_of_processes"])
    ]

    START >> retrieve_messages >> END
