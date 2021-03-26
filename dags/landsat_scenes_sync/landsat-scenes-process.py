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


# maximum number of active runs for this DAG. The scheduler will not create new active DAG runs once this limit is hit.
# Defaults to core.max_active_runs_per_dag if not set
MAX_ACTIVE_RUNS = 15

# the number of task instances allowed to run concurrently across all active runs of the DAG this is set on.
# Defaults to core.dag_concurrency if not set
CONCURRENCY = 50 * MAX_ACTIVE_RUNS


# [START default_args]

DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),  # start_date is always yesterday
    "catchup": False,
    "limit_of_processes": 30,
    "version": "0.3",
}
# [END default_args]


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


# [START instantiate_dag]
dag = DAG(
    "landsat-scenes-process",
    default_args=DEFAULT_ARGS,
    description="Process Queue Messages",
    concurrency=CONCURRENCY,
    max_active_runs=MAX_ACTIVE_RUNS,
    schedule_interval="0 */12 * * *",
    tags=["Scene"],
)


# [END instantiate_dag]

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
