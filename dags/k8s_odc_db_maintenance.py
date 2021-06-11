"""### AWS ODC datacube db maintenance job

Daily DAG for AWS ODC datacube db maintenance operations
"""

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from textwrap import dedent
from infra.images import INDEXER_IMAGE
from infra.podconfig import ONDEMAND_NODE_AFFINITY
from infra.variables import DB_HOSTNAME, DB_PORT, DB_DATABASE
from infra.variables import SECRET_ODC_ADMIN_NAME, SECRET_EXPLORER_ADMIN_NAME

# Templated DAG arguments
DEFAULT_ARGS = {
    "owner": "Nikita Gandhi",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 11),
    "email": ["nikita.gandhi@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_PORT": DB_PORT,
        "DB_DATABASE": DB_DATABASE
    }
}

# Lift secrets database connectivity
ODC_SCHEMA_ADMIN_SECRET = [
   Secret("env", "DB_ADMIN_USER", SECRET_ODC_ADMIN_NAME, "postgres-username"),
   Secret("env", "PGPASSWORD", SECRET_ODC_ADMIN_NAME, "postgres-password"),
]

CUBEDASH_SCHEMA_ADMIN_SECRET = [
    Secret("env", "DB_ADMIN_USER", SECRET_EXPLORER_ADMIN_NAME, "postgres-username"),
    Secret("env", "PGPASSWORD", SECRET_EXPLORER_ADMIN_NAME, "postgres-password"),
]


dag = DAG(
    "k8s_odc_db_maintenance",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=["k8s", "db", "odc"],
    schedule_interval="@weekly",
    dagrun_timeout=timedelta(minutes=60 * 4),
)

affinity = ONDEMAND_NODE_AFFINITY

ODC_SCHEMA_MAINTENANCE_SCRIPT = [
    "bash",
    "-c",
    dedent(
        """
            # vaccum analyze agdc schema
            psql -h $(DB_HOSTNAME) -U $(DB_ADMIN_USER) -d $(DB_DATABASE) -c \
            "vacuum verbose analyze agdc.dataset, agdc.dataset_type, agdc.dataset_source, agdc.metadata_type, agdc.dataset_location;"
        """
    ),
]


CUBEDASH_SCHEMA_MAINTENANCE_SCRIPT = [
    "bash",
    "-c",
    dedent(
        """
            # vaccum analyze cubedash schema
            psql -h $(DB_HOSTNAME) -U $(DB_ADMIN_USER) -d $(DB_DATABASE) -c \
            "vacuum verbose cubedash.dataset_spatial, cubedash.product, cubedash.region, cubedash.time_overview;"
        """
    ),
]

with dag:
    START = DummyOperator(task_id="start")

    ODC_SCHEMA_MAINTENANCE = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        arguments=ODC_SCHEMA_MAINTENANCE_SCRIPT,
        secrets=ODC_SCHEMA_ADMIN_SECRET,
        image_pull_policy="Always",
        labels={"step": "odc-schema-maintenance"},
        name="odc-schema-maintenance",
        task_id="odc-schema-maintenance",
        get_logs=True,
        is_delete_operator_pod=True,
        affinity=affinity,
    )

    CUBEDASH_SCHEMA_MAINTENANCE = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        arguments=CUBEDASH_SCHEMA_MAINTENANCE_SCRIPT,
        secrets=CUBEDASH_SCHEMA_ADMIN_SECRET,
        image_pull_policy="Always",
        labels={"step": "cubedash-schema-maintenance"},
        name="cubedash-schema-maintenance",
        task_id="cubdash-schemab-maintenance",
        get_logs=True,
        is_delete_operator_pod=True,
        affinity=affinity,
    )

    # Task complete
    COMPLETE = DummyOperator(task_id="done")

    START >> ODC_SCHEMA_MAINTENANCE
    ODC_SCHEMA_MAINTENANCE >> CUBEDASH_SCHEMA_MAINTENANCE
    CUBEDASH_SCHEMA_MAINTENANCE >> COMPLETE
