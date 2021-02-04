from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.aws_sqs_publish_operator import SQSPublishOperator
from datetime import timedelta, datetime
from airflow import DAG


def test_ssm_conn_and_var():
    sqs_hook = SQSHook(aws_conn_id="sync_landsat_scenes")
    print(sqs_hook.get_conn())
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName="deafrica-dev-eks-sync-landsat-scene")
    queue.send_messages(Entries="testing")


DEFAULT_ARGS = {
    "owner": "Pin Jin",
    "email": ["pin.jin@ga.gov.au"],
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
    "Test-SSM-Conn-and-Var",
    default_args=DEFAULT_ARGS,
    description="Test SSM",
    schedule_interval=None,
    tags=["Test", "Infra"],
)

with dag:
    TEST = PythonOperator(
        task_id="test-conn",
        python_callable=test_ssm_conn_and_var,
        dag=dag,
    )

    # TEST = SQSPublishOperator(
    #     task_id="test-conn",
    #     aws_conn_id="sync_landsat_scenes",
    #     sqs_queue="deafrica-dev-eks-sync-landsat-scene",
    #     message_content="test",
    # )
