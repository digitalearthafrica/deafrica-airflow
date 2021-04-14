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
from utils.sync_utils import time_process

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
    "start_date": datetime(2021, 4, 14),
    "catchup": True,
    "limit_of_processes": 30,
    "version": "0.6.2",
}
# [END default_args]


def terminate(start_timer, **kwargs):
    """
    Function to present the processed time
    :param start_timer:
    :param kwargs:
    :return:
    """
    print(f"Message processed and sent in {time_process(start=start_timer)}")


# [START instantiate_dag]
dag = DAG(
    "landsat_scenes_processing",
    default_args=DEFAULT_ARGS,
    description="Process Landsat Queue Messages",
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
        )
        for idx in range(DEFAULT_ARGS["limit_of_processes"])
    ]

    START >> retrieve_messages >> END
