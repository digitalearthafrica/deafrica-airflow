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
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from textwrap import dedent
from infra.podconfig import ONDEMAND_NODE_AFFINITY
from infra.images import INDEXER_IMAGE
from infra.variables import SECRET_DBA_ADMIN_NAME, DB_DATABASE, DB_WRITER, DB_PORT
from infra.variables import REGION
from infra.s3_buckets import DB_DUMP_S3_BUCKET
from infra.iam_roles import DB_DUMP_S3_ROLE

DAG_NAME = "utility_odc_db_dump_to_s3"
DB_DUMP_MOUNT_PATH = '/dbdump'

odc_db_dump_volume_mount = VolumeMount(
    name="odc-db-dump-volume",
    mount_path=DB_DUMP_MOUNT_PATH,
    sub_path=None,
    read_only=False,
)

odc_db_dump_volume_config = {
    "persistentVolumeClaim": {"claimName": "odc-db-dump-volume"}
}

odc_db_dump_volume = Volume(
    name="odc-db-dump-volume", configs=odc_db_dump_volume_config
)

# DAG CONFIGURATION
DEFAULT_ARGS = {
    "owner": "Nikita Gandhi",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["nikita.gandhi@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "DB_HOSTNAME": DB_WRITER,
        "DB_DATABASE": DB_DATABASE,
        "DB_PORT": DB_PORT,
        "DB_DUMP_MOUNT_PATH": DB_DUMP_MOUNT_PATH,
        "AWS_DEFAULT_REGION": REGION,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_DBA_ADMIN_NAME, "postgres-username"),
        Secret("env", "PGPASSWORD", SECRET_DBA_ADMIN_NAME, "postgres-password"),
    ],
}


DUMP_TO_S3_COMMAND = [
    "bash",
    "-c",
    dedent(
        """
            pg_dump -Fc -h $(DB_HOSTNAME) -U $(DB_USERNAME) -d $(DB_DATABASE) > $(DB_DUMP_MOUNT_PATH)/{0}
            ls -la $(DB_DUMP_MOUNT_PATH) | grep {0}
            aws s3 cp --acl bucket-owner-full-control --only-show-errors $(DB_DUMP_MOUNT_PATH)/{0} s3://{1}/deafrica-dev/{0}
            echo "SUCCESS writing to s3://{1}/deafrica-dev/{0}"
            
            rm -f $(DB_DUMP_MOUNT_PATH)/{0}
        """
    ).format(f"odc_{date.today().strftime('%Y_%m_%d')}.pgdump", DB_DUMP_S3_BUCKET),
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
        affinity=ONDEMAND_NODE_AFFINITY,
        volumes=[odc_db_dump_volume],
        volume_mounts=[odc_db_dump_volume_mount],
        is_delete_operator_pod=True,
    )
