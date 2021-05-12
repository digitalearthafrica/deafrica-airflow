"""
SMTP infra setup testing dag
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator


default_args = {
    "owner": "Pin Jin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["pin.jin@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": None,
}

dag = DAG(
    "test_emailoperator",
    description="Simple Test DAG",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)

email = EmailOperator(
    task_id="send_email",
    to="pin.jin@ga.gov.au",
    subject="Airflow Alert",
    html_content=None,
    dag=dag,
)

email