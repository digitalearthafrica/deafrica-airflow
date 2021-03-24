"""
# odc database in RDS backup and store to s3

DAG to periodically backup ODC database data.

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.
"""
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from textwrap import dedent

from airflow.models import Variable

from infra.variables import (
    DB_DUMP_S3_BUCKET,
    DB_DUMP_S3_ROLE,
    SECRET_DBA_ADMIN_NAME,
    DB_DATABASE,
    DB_HOSTNAME,
    SECRET_AWS_NAME,
)

from infra.podconfig import NODE_AFFINITY
from infra.images import INDEXER_IMAGE

DAG_NAME = "utility_odc_db_dump_to_s3"

# DAG CONFIGURATION
DEFAULT_ARGS = {
    "owner": "Pin Jin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["pin.jin@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_DATABASE": DB_DATABASE,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_DBA_ADMIN_NAME, "postgres-username"),
        Secret("env", "PGPASSWORD", SECRET_DBA_ADMIN_NAME, "postgres-password"),
        Secret(
            "env", "AWS_DEFAULT_REGION", "indexing-aws-creds-prod", "AWS_DEFAULT_REGION"
        ),
    ],
}


DUMP_TO_S3_COMMAND = [
    "bash",
    "-c",
    dedent(
        """
            pg_dump -Fc -h $(DB_HOSTNAME) -U $(DB_USERNAME) -d $(DB_DATABASE) > {0}
            ls -la | grep {0}
            aws s3 cp --acl bucket-owner-full-control {0} s3://{1}/deafrica-dev/{0} --region af-south-1
        """
    ).format(f"africa_{date.today().strftime('%Y_%m_%d')}.pgdump", DB_DUMP_S3_BUCKET),
]

# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="@weekly",  # weekly
    catchup=False,
    tags=["k8s", "developer_support", "rds", "s3", "db"],
)

with dag:
    DB_DUMP = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        arguments=DUMP_TO_S3_COMMAND,
        annotations={"iam.amazonaws.com/role": DB_DUMP_S3_ROLE},
        labels={"step": "ds-arch"},
        name="dump-odc-db",
        task_id="dump-odc-db",
        get_logs=True,
        affinity=NODE_AFFINITY,
        is_delete_operator_pod=True,
    )
