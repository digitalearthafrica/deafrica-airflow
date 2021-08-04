"""test_slackoperator.py

An example of using the Slack Alert airflow function with a simple single-task DAG.
It uses the PythonOperator to simulate a coin flip. If the coin flips "tails" it
raises an exception, forcing a failed task Slack alert. If the coin flips "heads"
it passes and calls the succeess task Slack alert.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from test.slack_operator import task_fail_slack_alert, task_success_slack_alert
import random

def coin_flip():
    """
    This is the simple coin flip code that raises an exception "half the time"
    Args:
        None
    Returns:
        True only if coin flips "heads"
    Raises:
        ValueError: If coin flips "tails"
    """
    flip = random.random() > 0.5
    if not flip:
        raise ValueError("Coin flipped tails. We lose!")
    print("Coin flipped heads. We win!")
    return True

default_args = {
    "owner": "Nikita Gandhi",
    # "depends_on_past": False,
    "start_date": datetime(2021, 8, 1),
    "email": ["nikita.gandhi@ga.gov.au"],
    # "email_on_failure": True,
    # "email_on_retry": True,
    "retries": 0,
    # Here we show an example of setting a failure callback applicable to all
    # tasks in the DAG
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    "test_slackoperator",
    description="Test DAG",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)


with dag:
    t1 = PythonOperator(
        task_id="coin_flip",
        python_callable=coin_flip,
        # Here we show assigning success callback just for this task
        on_success_callback=task_success_slack_alert,
    )