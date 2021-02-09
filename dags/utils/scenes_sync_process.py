"""
    Script to read queue, process messages and save on the S3 bucket
"""
import concurrent.futures
import json
import logging
from collections import Generator

import botocore
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.hooks.S3_hook import S3Hook
from pystac import Item

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE

# ######### AWS CONFIG ############

AWS_CONFIG = {
    "africa_dev_conn_id": SYNC_LANDSAT_CONNECTION_ID,
    "sqs_queue": SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
    "arn": "arn:aws:sqs:ap-southeast-2:717690029437:Rodrigo_Test",
}

# ######### S3 CONFIG ############
SRC_BUCKET_NAME = "sentinel-cogs"
QUEUE_NAME = "deafrica-prod-eks-sentinel-2-data-transfer"
AFRICA_CONN_ID = "deafrica-prod-migration"


# https://github.com/digitalearthafrica/deafrica-extent/blob/master/deafrica-usgs-pathrows.csv.gz


def get_contents_and_attributes():
    hook = S3Hook(aws_conn_id=AFRICA_CONN_ID)
    bucket_name, key = hook.parse_s3_url('')
    contents = hook.read_key(key=key, bucket_name=SRC_BUCKET_NAME)
    contents_dict = json.loads(contents)
    # attributes = get_common_message_attributes(contents_dict)
    return contents
    # return contents, attributes


def get_s3_object(client, bucket: str, key: str):
    try:
        return client.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as error:
        logging.error(error)


def send_sns_message(client, topic: str, content: str):
    try:
        return client.publish(TopicArn=topic, Message=content)
    except botocore.exceptions.ClientError as error:
        logging.error(error)


def get_queue():
    """
    Connect to the right queue
    :return: QUEUE
    """
    try:
        sqs_hook = SQSHook(aws_conn_id=AWS_CONFIG["africa_dev_conn_id"])
        sqs = sqs_hook.get_resource_type("sqs")
        queue = sqs.get_queue_by_name(QueueName=AWS_CONFIG["sqs_queue"])

        return queue

    except Exception as error:
        raise error


def get_messages(limit: int = 16):
    """
    Get messages from a queue resource.
    :return: message
    """
    try:
        queue = get_queue()

        messages = queue.receive_messages(
            VisibilityTimeout=60,
            MaxNumberOfMessages=limit,
            WaitTimeSeconds=10,
        )

        for message in messages:
            yield json.loads(message.body)

    except Exception as error:
        raise error


def convert_dict_to_pystac_item(message: dict):
    try:
        item = Item.from_dict(message)
        return item
    except Exception as error:
        raise error


def delete_messages(messages: list = None):
    """
    Delete messages from the queue
    :param messages:
    :return:
    """
    try:
        [message.delete() for message in messages]
    except Exception as error:
        raise error


def replace_links(item: Item):
    try:
        for link in item.get_links():
            print(link)

        # link["href"] = link["href"].replace(
        #     "https://landsatlook.usgs.gov/sat-api/",
        #     "s3://usgs-landsat/",
        # )

    except Exception as error:
        raise error
#
#
# def replace_asset_links(dataset):
#     try:
#         if dataset.get("assets"):
#             for asset in dataset["assets"]:
#                 if asset.get("href"):
#                     asset["href"] = asset["href"].replace(
#                         "https://landsatlook.usgs.gov",
#                         "s3://usgs-landsat/"
#                     )
#     except Exception as error:
#         raise error


def check_parameters(message):
    try:
        return bool(
            message.get("geometry")
            and message.get("properties")
            and message["geometry"].get("coordinates")
        )

    except Exception as error:
        raise error


def bulk_convert_dict_to_pystac_item(messages: Generator):
    try:
        # Limit number of threads
        num_of_threads = 16
        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            logging.info(
                f"Simultaneously {num_of_threads} conversions (Python threads)"
            )

            futures = []
            try:
                for message in messages:
                    futures.append(executor.submit(convert_dict_to_pystac_item, message))

                    if len(futures) == num_of_threads:
                        results.extend([f.result() for f in futures])
                        futures = []

            except StopIteration:
                results.extend([f.result() for f in futures])

        return results

    except Exception as error:
        raise error


def bulk_items_replace_links(items):
    try:
        for item in items:
            replace_links(item=item)
    except Exception as error:
        raise error


def process():
    """
        Main function to process information from the queue
    :return:
    """

    count_messages = 0
    try:
        limit = 16
        while True:
            # Retrieve messages from the queue
            messages = get_messages(limit=limit)

            if not messages:
                break

            items = bulk_convert_dict_to_pystac_item(messages=messages)

            count_messages += len(items)

            bulk_items_replace_links(items=items)

    except StopIteration:
        logging.info(f'All {count_messages} messages read')
    except Exception as error:
        logging.error(error)
        raise error


# ################## Create ShapeFile process #############################################

# def build_properties_schema(properties: dict):
#     try:
#
#         schema = {}
#         for key, value in properties.items():
#             if type(value) is int:
#                 type_property = 'int'
#             elif type(value) is float:
#                 type_property = 'float'
#             elif type(value) is str:
#                 type_property = 'str'
#             elif type(value) is fiona.rfc3339.FionaDateType:
#                 type_property = 'date'
#             elif type(value) is fiona.rfc3339.FionaTimeType:
#                 type_property = 'time'
#             elif type(value) is fiona.rfc3339.FionaDateTimeType:
#                 type_property = 'datetime'
#             else:
#                 continue
#             schema.update({key: type_property})
#
#         return schema
#
#     except Exception as error:
#         raise error
#
#
# def create_shp_file(datasets: list):
#     try:
#         shapely.speedups.enable()
#
#         # schema = {
#         #     "geometry": "Polygon",
#         #     "properties": {
#         #         'datetime': 'str',
#         #         "landsat:wrs_path": "str",
#         #         "landsat:wrs_row": "str",
#         #         "landsat:scene_id": "str",
#         #     },
#         # }
#
#         schema = {
#             "geometry": "Polygon",
#             "properties": build_properties_schema(properties=datasets[0]['properties'])
#         }
#
#         count = 0
#         logging.info(f"Started")
#         # Write a new Shapefile
#         with fiona.open("/tmp/Shapefile/test.shp", "w", "ESRI Shapefile", schema) as c:
#             # for message in JSON_TEST:
#             for dataset in datasets:
#                 if check_parameters(message=dataset):
#                     poly = shape(dataset["geometry"])
#
#                     c.write(
#                         {
#                             "geometry": mapping(poly),
#                             "properties": {
#                                 key: value
#                                 for key, value in dataset["properties"].items()
#                                 if key in schema["properties"].keys()
#                             },
#                         }
#                     )
#
#                 if count > 20:
#                     break
#                 count += 1
#             return True
#
#     except Exception as error:
#         raise error
