# -*- coding: utf-8 -*-

"""
### DEA ODC prod database - migrate schema

DAG to manually migrate schema for ODC DB when new version of explorer is
deployed.

"""

import pendulum
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from infra.images import EXPLORER_UNSTABLE_IMAGE

local_tz = pendulum.timezone("Africa/Johannesburg")

# Templated DAG arguments
DB_HOSTNAME = "db-writer"

DEFAULT_ARGS = {
    "owner": "Tisham Dhar",
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 11, tzinfo=local_tz),
    "email": ["tisham.dhar@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "AWS_DEFAULT_REGION": "af-south-1",
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_PORT": "5432",
    },
    # Use K8S secrets to send DB Creds
    # Lift secrets into environment variables for datacube database connectivity
    # Use this db-users to run cubedash update-summary
    # NOTE: explorer-admin user permission boundaries are not defined correctly
    #   when schema upgrade require to create agdc additional index. Those steps handled outside manually.
    "secrets": [
        Secret("env", "DB_DATABASE", "explorer-admin", "database-name"),
        Secret("env", "DB_USERNAME", "explorer-admin", "postgres-username"),
        Secret("env", "DB_PASSWORD", "explorer-admin", "postgres-password"),
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

affinity = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{
                "matchExpressions": [{
                    "key": "nodetype",
                    "operator": "In",
                    "values": [
                        "ondemand",
                    ]
                }]
            }]
        }
    }
}

with dag:
    START = DummyOperator(task_id="odc-db-update-schema")

    # Run update summary
    UPDATE_SCHEMA = KubernetesPodOperator(
        namespace="processing",
        image=EXPLORER_UNSTABLE_IMAGE,
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
