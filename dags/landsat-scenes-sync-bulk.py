#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
# [START import_module]
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 27),
    # "schedule_interval": "@once",
    "us_conn_id": "prod-eks-s2-data-transfer",
    "africa_conn_id": "deafrica-prod-migration",
}

default_args = {
    "owner": "rodrigo.carvalho",
    "depends_on_past": False,
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 27)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat-scenes-sync",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
    tags=["example"],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    ALL_COPIES = []
    for i in range(10):
        copy = DummyOperator(task_id=f"copy-tasks-{i}")
        ALL_COPIES.append(copy)

    END = DummyOperator(task_id="end-tasks")

    START >> ALL_COPIES >> END
