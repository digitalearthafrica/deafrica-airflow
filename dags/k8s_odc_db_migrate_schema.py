# -*- coding: utf-8 -*-

"""
### DEA ODC prod database - migrate schema

DAG to manually migrate schema for ODC DB when new version of explorer is
deployed.

NOTE: explorer_admin user permission boundary is bound to `cubedash` schema only.
  So, when schema upgrade require to create agdc additional index,
  those steps handled outside manually using odc_admin user.
"""

import pendulum
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from infra.images import EXPLORER_IMAGE
from infra.variables import DB_WRITER, DB_DATABASE, DB_PORT, REGION
from infra.variables import SECRET_EXPLORER_ADMIN_NAME
from infra.podconfig import ONDEMAND_NODE_AFFINITY

local_tz = pendulum.timezone("Africa/Johannesburg")

DEFAULT_ARGS = {
    "owner": "Nikita Gandhi",
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 10, tzinfo=local_tz),
    "email": ["nikita.gandhi@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "AWS_DEFAULT_REGION": REGION,
        "DB_HOSTNAME": DB_WRITER,
        "DB_DATABASE": DB_DATABASE,
        "DB_PORT": DB_PORT,
    },
    # Use K8S secrets to send DB Creds
    # Lift secrets into environment variables for datacube database connectivity
    # Use this db-users to run cubedash update-summary
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_EXPLORER_ADMIN_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_EXPLORER_ADMIN_NAME, "postgres-password"),
    ],
}

dag = DAG(
    "k8s_odc_db_migrate_schema",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=["k8s"],
    schedule_interval=None,    # Fully manual migrations
)

affinity = ONDEMAND_NODE_AFFINITY

with dag:
    START = DummyOperator(task_id="odc-db-update-schema")

    # Run update summary
    UPDATE_SCHEMA = KubernetesPodOperator(
        namespace="processing",
        image=EXPLORER_IMAGE,
        cmds=["cubedash-gen"],
        arguments=["--init", "-v"],
        labels={"step": "update-schema"},
        name="update-schema",
        task_id="update-schema",
        get_logs=True,
        is_delete_operator_pod=True,
        affinity=affinity,
        # execution_timeout=timedelta(days=1),
    )

    # Task complete
    COMPLETE = DummyOperator(task_id="done")


    START >> UPDATE_SCHEMA
    UPDATE_SCHEMA >> COMPLETE
