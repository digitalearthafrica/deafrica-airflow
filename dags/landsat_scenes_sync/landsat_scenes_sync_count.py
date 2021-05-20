"""
# TEST DAG
"""

# [START import_module]
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# [END import_module]
# [START default_args]
from infra.connections import CONN_LANDSAT_WRITE
from landsat_scenes_sync.variables import (
    USGS_S3_BUCKET_NAME,
    USGS_AWS_REGION,
    AFRICA_GZ_PATHROWS_URL,
    BASE_BULK_CSV_URL,
)
from utils.aws_utils import S3
from utils.sync_utils import (
    convert_str_to_date,
    read_big_csv_files_from_gzip,
    read_csv_from_gzip,
    download_file_to_tmp,
)

DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 29),
    "catchup": False,
    "version": "0.2",
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "Landsat_ST_ST_MLS_TEST",
    default_args=DEFAULT_ARGS,
    description="Sync Tests",
    schedule_interval=None,
    tags=["TESTs"],
)

# [END instantiate_dag]


def count_2018_sr_st(file_name):
    """

    :param file_name:
    :return:
    """

    def filter_data(file_path):
        """

        :param file_path:
        :return:
        """
        print("Start Filtering")
        # Download updated Pathrows
        africa_pathrows = read_csv_from_gzip(file_path=AFRICA_GZ_PATHROWS_URL)

        # Variable that ensure the log is going to show at the right time
        for row in read_big_csv_files_from_gzip(file_path):
            date_acquired = convert_str_to_date(row["Date Acquired"])
            if date_acquired.year != 2018:
                continue
            if (
                # Filter to skip all LANDSAT_4
                row.get("Satellite")
                and row["Satellite"] != "LANDSAT_4"
                and row["Satellite"] != "4"
                # Filter to get just day
                and (
                    row.get("Day/Night Indicator")
                    and row["Day/Night Indicator"].upper() == "DAY"
                )
                # Filter to get just from Africa
                and (
                    row.get("WRS Path")
                    and row.get("WRS Row")
                    and int(f"{row['WRS Path'].zfill(3)}{row['WRS Row'].zfill(3)}")
                    in africa_pathrows
                )
            ):
                yield row

    def build_asset_list(scene):
        s3 = S3(conn_id=CONN_LANDSAT_WRITE)

        year_acquired = convert_str_to_date(scene["Date Acquired"]).year
        # USGS changes - for _ when generates the CSV bulk file
        identifier = scene["Sensor Identifier"].lower().replace("_", "-")

        folder_link = (
            "collection02/level-2/standard/{identifier}/{year_acquired}/"
            "{target_path}/{target_row}/{display_id}/".format(
                identifier=identifier,
                year_acquired=year_acquired,
                target_path=scene["WRS Path"].zfill(3),
                target_row=scene["WRS Row"].zfill(3),
                display_id=scene["Display ID"],
            )
        )

        response = s3.list_objects(
            bucket_name=USGS_S3_BUCKET_NAME,
            region=USGS_AWS_REGION,
            prefix=folder_link,
            request_payer="requester",
        )

        if not response.get("Contents"):
            return {scene["Display ID"]: "Not Found"}

        mtl_sr_st_files = {}
        for obj in response["Contents"]:
            if "_ST_stac.json" in obj["Key"]:
                mtl_sr_st_files.update({"ST": obj["Key"]})

            elif "_SR_stac.json" in obj["Key"]:
                mtl_sr_st_files.update({"SR": obj["Key"]})

            elif "_MTL.json" in obj["Key"]:
                mtl_sr_st_files.update({"MTL": obj["Key"]})

        missing = {}
        if not mtl_sr_st_files.get("ST"):
            missing.update({"Missing ST": True})
        if not mtl_sr_st_files.get("SR"):
            missing.update({"Missing SR": True})
        if not mtl_sr_st_files.get("MTL"):
            missing.update({"Missing MTL": True})

        if missing:
            date_acquired = convert_str_to_date(scene["Date Acquired"])
            missing.update({"Date": date_acquired})
            missing.update({"S3 path": f"s3://usgs-landsat/{folder_link}"})
            return {scene["Display ID"]: missing}

    file_path = download_file_to_tmp(url=BASE_BULK_CSV_URL, file_name=file_name)

    num_of_threads = 50
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:

        tasks = [
            executor.submit(
                build_asset_list,
                scene,
            )
            for scene in filter_data(file_path)
        ]

        mtl_count = 0
        st_count = 0
        sr_count = 0
        missing_scenes = []
        for future in as_completed(tasks):
            if future:
                missing_scenes.append(future.result())
                for k, v in future.result().items():
                    mtl_count += 1 if v.get("Missing MSL") else 0
                    st_count += 1 if v.get("Missing ST") else 0
                    sr_count += 1 if v.get("Missing SR") else 0

        print("********************************************************************")
        print(f"MTL Missing {mtl_count}")
        print(f"ST Missing {st_count}")
        print(f"SR Missing {sr_count}")
        print("********************************************************************")
        print("====================================================================")
        print(json.dumps(missing_scenes))
        print("====================================================================")
        print(f"Number Scenes {len(list(filter_data(file_path)))}")
        print("====================================================================")
        print(f"Number Missing some data Scenes {len(missing_scenes)}")


with dag:
    START = DummyOperator(task_id="start-tasks")
    files = {
        "landsat_8": "LANDSAT_OT_C2_L2.csv.gz",
        "landsat_7": "LANDSAT_ETM_C2_L2.csv.gz",
        "Landsat_4_5": "LANDSAT_TM_C2_L2.csv.gz",
    }

    processes = [
        PythonOperator(
            task_id="Landsat_ST_ST_MLS_TEST",
            python_callable=count_2018_sr_st,
            op_kwargs=dict(file_name="LANDSAT_OT_C2_L2.csv.gz"),
        )
    ]

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
