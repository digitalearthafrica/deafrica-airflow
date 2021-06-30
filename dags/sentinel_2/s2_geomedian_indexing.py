"""
# Sentinel-2 geomedian product indexing automation

DAG to index Sentinel-2 geomedian backlog data.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret

from infra.images import INDEXER_IMAGE
from infra.s3_buckets import DEAFRICA_SERVICES_BUCKET_NAME
from infra.variables import SECRET_ODC_WRITER_NAME, DB_HOSTNAME

DEFAULT_ARGS = {
    "owner": "Toktam Ebadi",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["toktam.ebadi@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "schedule_interval": "@once",
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": DB_HOSTNAME,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_ODC_WRITER_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_ODC_WRITER_NAME, "postgres-password"),
        Secret("env", "DB_DATABASE", SECRET_ODC_WRITER_NAME, "database-name"),
    ],
}

dag = DAG(
    "Sentinel-2-geomedian-indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["k8s", "Sentinel-2-indexing"],
)

with DAG(
    "Sentinel-2-geomedian-backlog-indexing",
    default_args=DEFAULT_ARGS,
    tags=["Sentinel-2", "geomedian-indexing"],
    schedule_interval=DEFAULT_ARGS["schedule_interval"],
    catchup=False,
) as dag:

    # This needs to be updated in the future in case more zones have been added
    for index in range(153, 247):
        INDEXING = KubernetesPodOperator(
            namespace="processing",
            image=INDEXER_IMAGE,
            image_pull_policy="Always",
            labels={"backlog": "s3-to-dc"},
            cmds=["s3-to-dc"],
            arguments=[
                "s3-to-dc",
                "--stac",
                "--no-sign-request",
                f"s3://{DEAFRICA_SERVICES_BUCKET_NAME}/gm_s2_annual/1-0-0/x{index}/**/*.json",
                "gm_s2_annual",
            ],
            name="datacube-index",
            task_id=f"Sentinel-2-geomedian-backlog-indexing-task-x{index}",
            get_logs=True,
            is_delete_operator_pod=True,
        )
