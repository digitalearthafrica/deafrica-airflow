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
from airflow.operators.python_operator import PythonOperator

from scripts.rodrigo import retrieve_bulk_data

DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 27),
    # "schedule_interval": "@once",
    "us_conn_id": "prod-eks-s2-data-transfer",
    "africa_conn_id": "deafrica-prod-migration",
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat-scenes-sync-bulk",
    default_args=DEFAULT_ARGS,
    description="Sync bulk files",
    schedule_interval=timedelta(days=1),
    tags=["Scene", "bulk"],
)
# [END instantiate_dag]

with dag:
    START = DummyOperator(task_id="start-tasks")

    processes = []
    files = {
        'landsat_8': 'LANDSAT_OT_C2_L2.csv.gz',
        'landsat_7': 'LANDSAT_ETM_C2_L2.csv.gz',
        'Landsat_4_5': 'LANDSAT_TM_C2_L2.csv.gz'
    }

    for sat, file in files.items():

        processes.append(
            PythonOperator(
                task_id=sat,
                python_callable=retrieve_bulk_data,
                op_kwargs=dict(file_name=file),
                dag=dag,
            )
        )

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
