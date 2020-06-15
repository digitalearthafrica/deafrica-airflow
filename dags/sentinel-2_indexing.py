"""
# Sentinel-2 indexing automation

DAG to periodically/one-shot index Sentinel=2 data. Eventually it could 
update explorer and ows schemas in RDS after a given Dataset has been 
indexed.

This DAG uses k8s executors and pre-existing pods in cluster with relevant tooling
and configuration installed.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    "owner": "Alex Leith",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["alex.leith@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "AWS_DEFAULT_REGION": "us-west-2",
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": "database-write.local",
        "DB_DATABASE": "ows-index",
    },
    # Use Ku secrets to send DB Creds
    # Lift secrets into environment variables for datacube
    "secrets": [
        Secret("env", "DB_USERNAME", "ows-db", "postgres-username"),
        Secret("env", "DB_PASSWORD", "ows-db", "postgres-password"),
    ],
}

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.7"
OWS_IMAGE = "opendatacube/ows:0.14.1"
EXPLORER_IMAGE = "opendatacube/dashboard:2.1.6"

dag = DAG(
    "sentinel-2_indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s"]
)


with dag:
    START = DummyOperator(task_id="sentinel-2_indexing")

    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        cmds=["sqs-to-dc"],
        annotations={"iam.amazonaws.com/role": "svc-deafrica-processing-prod"},
        arguments=["--stac", "deafrica-prod-eks-sentinel-2-indexing" "s2_l2a"],
        labels={"step": "sqs-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
    )

    # TODO: implement
    UPDATE_RANGES = KubernetesPodOperator(
        namespace="processing",
        image=OWS_IMAGE,
        cmds=["datacube-ows-update"],
        arguments=["--help"],
        labels={"step": "ows"},
        name="ows-update-ranges",
        task_id="update-ranges-task",
        get_logs=True,
    )

    # TODO: implement
    EXPLORER_SUMMARY = KubernetesPodOperator(
        namespace="processing",
        image=EXPLORER_IMAGE,
        cmds=["cubedash-gen"],
        arguments=["--help"],
        labels={"step": "explorer"},
        name="explorer-summary",
        task_id="explorer-summary-task",
        get_logs=True,
    )

    COMPLETE = DummyOperator(task_id="all_done")

    START >> INDEXING
    INDEXING >> UPDATE_RANGES
    INDEXING >> EXPLORER_SUMMARY
    UPDATE_RANGES >> COMPLETE
    EXPLORER_SUMMARY >> COMPLETE
