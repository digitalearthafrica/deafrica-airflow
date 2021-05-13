"""
SMTP infra setup testing dag
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator


default_args = {
    "owner": "Pin Jin",
    # "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["nikita.gandhi@ga.gov.au"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "retries": 1,
}

dag = DAG(
    "test_emailoperator",
    description="Simple Test DAG",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)

email = EmailOperator(
    mime_charset="utf-8",
    task_id="send_email",
    to="nikita.gandhi@ga.gov.au",
    subject="Templated Subject: start_date {{ ds }}",
    params={"content1": "random"},
    html_content="Templated Content: content1 - {{ params.content1 }}  task_key - {{ task_instance_key_str }} test_mode - {{ test_mode }} task_owner - {{ task.owner}} hostname - {{ ti.hostname }}",
    dag=dag,
)

email
