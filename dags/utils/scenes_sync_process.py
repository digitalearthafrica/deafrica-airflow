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
from pystac import Item, Link

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE

# ######### AWS CONFIG ############

AWS_DEV_CONFIG = {
    "africa_dev_conn_id": SYNC_LANDSAT_CONNECTION_ID,
    "sqs_queue": SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
    "sns_topic": "",
    "s3_destination_bucket_name": "deafrica-landsat-dev",
    "s3_source_bucket_name": "usgs-landsat",
}


# https://github.com/digitalearthafrica/deafrica-extent/blob/master/deafrica-usgs-pathrows.csv.gz


def get_contents_and_attributes(
        s3_conn_id: str = AWS_DEV_CONFIG['africa_dev_conn_id'],
        bucket_name: str = AWS_DEV_CONFIG['s3_source_bucket_name']
):
    hook = S3Hook(aws_conn_id=s3_conn_id)
    returned_bucket_name, key = hook.parse_s3_url(bucket_name)
    contents = hook.read_key(key=key, bucket_name=returned_bucket_name)
    contents_dict = json.loads(contents)

    return contents_dict
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
        sqs_hook = SQSHook(aws_conn_id=AWS_DEV_CONFIG["africa_dev_conn_id"])
        sqs = sqs_hook.get_resource_type("sqs")
        queue = sqs.get_queue_by_name(QueueName=AWS_DEV_CONFIG["sqs_queue"])

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

        self_item = item.get_links(rel='self')
        usgs_self_target = self_item[0].target if len(self_item) == 1 and hasattr(self_item[0], 'target') else ''

        # Remove all Links
        [item.remove_links(rel=link.rel) for link in item.get_links()]

        # Add New Links
        self_link = Link(
            rel='self',
            target=f's3://{AWS_DEV_CONFIG["s3_destination_bucket_name"]}/'
        )
        item.add_link(self_link)

        derived_from = Link(
            rel='derived_from',
            target=usgs_self_target if usgs_self_target else 'https://landsatlook.usgs.gov/sat-api/'
        )
        item.add_link(derived_from)

        product_overview = Link(rel='product_overview', target=f's3://{AWS_DEV_CONFIG["s3_destination_bucket_name"]}/')
        item.add_link(product_overview)

        return item

    except Exception as error:
        raise error


def replace_asset_links(item: Item):
    try:

        assets = item.get_assets()
        for key, asset in assets.items():
            asset_href = (
                asset.href if hasattr(asset, 'href') else ''
            ).replace('https://landsatlook.usgs.gov/data/', f's3://{AWS_DEV_CONFIG["s3_destination_bucket_name"]}/')
            if asset_href:
                asset.href = asset_href
        return item
    except Exception as error:
        raise error


def add_odc_product_property(item: Item):
    try:

        properties = item.properties
        sat = properties.get('eo:platform', '')

        if sat == 'LANDSAT_8':
            value = 'ls8_l2sr'
        elif sat == 'LANDSAT_7':
            value = 'ls7_l2sr'
        elif sat == 'LANDSAT_5':
            value = 'ls5_l2sr'
        else:
            logging.error(f'Property odc:product not added due the sat is {sat if sat else "not informed"}')
            raise Exception(f'Property odc:product not added due the sat is {sat if sat else "not informed"}')

        properties.update({'odc:product': value})
        return Item
    except Exception as error:
        raise error


def merge_assets(item: Item):
    # s3://usgs-landsat.s3-us-west-2.amazonaws.com/collection02/level-2/standard/
    try:
        content = get_contents_and_attributes(
            s3_conn_id=AWS_DEV_CONFIG['africa_dev_conn_id'],
            bucket_name=AWS_DEV_CONFIG['s3_source_bucket_name']
        )
        print(content)
        logging.info(content)
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
            for message in messages:

                futures.append(executor.submit(convert_dict_to_pystac_item, message))

                if len(futures) == num_of_threads:
                    # every num_of_threads execute them
                    results.extend([f.result() for f in futures])
                    futures = []

            if futures:
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


def bulk_items_replace_assets(items):
    try:
        for item in items:
            replace_asset_links(item=item)
    except Exception as error:
        raise error


def bulk_items_add_odc_product_property(items):
    try:
        for item in items:
            add_odc_product_property(item=item)
    except Exception as error:
        raise error


def bulk_items_merge_assets(items):
    try:
        for item in items:
            merge_assets(item=item)
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

        # Retrieve messages from the queue
        messages = get_messages(limit=limit)

        if not messages:
            logging.info('No messages were found!')
            return

        items = bulk_convert_dict_to_pystac_item(messages=messages)

        count_messages += len(items)

        bulk_items_replace_links(items=items)

        bulk_items_replace_assets(items=items)

        bulk_items_add_odc_product_property(items=items)

        bulk_items_merge_assets(items=items)

        print(items)

    except StopIteration:
        logging.info(f'All {count_messages} messages read')
    except Exception as error:
        logging.error(error)
        raise error

# ################## Create ShapeFile process #############################################

# def check_parameters(message):
#     try:
#         return bool(
#             message.get("geometry")
#             and message.get("properties")
#             and message["geometry"].get("coordinates")
#         )
#
#     except Exception as error:
#         raise error

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
