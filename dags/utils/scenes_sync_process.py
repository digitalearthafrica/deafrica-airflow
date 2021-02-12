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

from infra.connections import SYNC_LANDSAT_CONNECTION_ID, INDEX_LANDSAT_CONNECTION_ID, SYNC_LANDSAT_USGS_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE, INDEX_LANDSAT_CONNECTION_SQS_QUEUE

# ######### AWS CONFIG ############
from utils.url_request_utils import request_url

AWS_DEV_CONFIG = {
    # Hack to run locally
    # "africa_dev_conn_id": '',
    # "sqs_queue": '',
    # "africa_dev_index_conn_id": '',
    # "sqs_index_queue": '',
    "usgs_api_main_url": "https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-st/items/",
    "africa_dev_conn_id": SYNC_LANDSAT_CONNECTION_ID,
    "africa_dev_usgs_conn_id": SYNC_LANDSAT_USGS_CONNECTION_ID,
    "sqs_queue": SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
    "africa_dev_index_conn_id": INDEX_LANDSAT_CONNECTION_ID,
    "sqs_index_queue": INDEX_LANDSAT_CONNECTION_SQS_QUEUE,
    "sns_topic": "",
    "s3_destination_bucket_name": "deafrica-landsat-dev",
    "s3_source_bucket_name": "usgs-landsat",
}


# https://github.com/digitalearthafrica/deafrica-extent/blob/master/deafrica-usgs-pathrows.csv.gz


def get_contents_and_attributes(
        s3_conn_id: str = AWS_DEV_CONFIG['africa_dev_conn_id'],
        bucket_name: str = AWS_DEV_CONFIG['s3_source_bucket_name'],
        key: str = None
):
    """

    :param s3_conn_id: (str) s3_conn_id: Airflow AWS credentials
    :param bucket_name: (str) bucket_name: AWS S3 bucket which the function will connect to
    :param key: (str) Path to the content which the function will access
    :return: (dict) content
    """
    try:
        if not key:
            raise Exception('Key must be informed to be able connecting to AWS S3')

        hook = S3Hook(aws_conn_id=s3_conn_id)
        contents = hook.read_key(key=key, bucket_name=bucket_name)
        contents_dict = json.loads(contents)

        return contents_dict
    except Exception as error:
        raise error


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
        logging.info(f'Connecting to AWS SQS {AWS_DEV_CONFIG["sqs_queue"]}')
        logging.info(f'Conn_id Name {AWS_DEV_CONFIG["africa_dev_conn_id"]}')
        sqs_hook = SQSHook(aws_conn_id=AWS_DEV_CONFIG["africa_dev_conn_id"])
        sqs = sqs_hook.get_resource_type("sqs")
        queue = sqs.get_queue_by_name(QueueName=AWS_DEV_CONFIG["sqs_queue"])

        return queue

    except Exception as error:
        raise error


def get_messages(limit: int = 10):
    """
     Get messages from a queue resource.
    :param limit:Must be between 1 and 10, if provided.
    :return:
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


def find_s3_path_from_item(item: Item):
    try:
        assets = item.get_assets()
        asset = assets.get('index')
        if asset and hasattr(asset, 'href'):
            file_name = f'{asset.href.split("/")[-1]}_ST_stac.json'
            asset_s3_path = asset.href.replace(
                'https://landsatlook.usgs.gov/stac-browser/', ''
            )
            full_path = f'{asset_s3_path}/{file_name}'

            return full_path

    except Exception as error:
        raise error


def find_url_path_from_item(item: Item):
    try:
        # eg.:  https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-st/items/LE07_L2SP_118044_20210115_20210209_02_T1
        assets = item.get_assets()
        asset = assets.get('index')
        if asset and hasattr(asset, 'href'):
            file_name = f'{asset.href.split("/")[-1]}'
            full_path = f'{AWS_DEV_CONFIG["usgs_api_main_url"]}/{file_name}'
            logging.info(f'path {full_path}')
            return full_path

    except Exception as error:
        raise error


def merge_assets_api(item: Item):
    try:
        # Request API
        response = request_url(url=find_url_path_from_item(item=item))
        new_item = convert_dict_to_pystac_item(message=response)
        logging.info(f'new_item {new_item}')

        item_assets = item.get_assets()
        new_item_assets = new_item.get_assets()

        logging.info(f'item_assets.keys() {item_assets.keys()}')

        for asset in new_item_assets:
            logging.info(f'asset {asset}')

        missing_keys = [key for key, asset in new_item_assets.items() if key not in item_assets.keys()]
        logging.info(f'missing_keys {missing_keys}')

    except Exception as error:
        raise error


def merge_assets(item: Item):
    # s3://usgs-landsat.s3-us-west-2.amazonaws.com/collection02/level-2/standard/
    # s3://usgs-landsat/collection02/level-1/standard/oli-tirs/2020/157/019/LC08_L1GT_157019_20201207_20201217_02_T2/LC08_L1GT_157019_20201207_20201217_02_T2_stac.json"
    try:
        # TODO REMOVE it's here jus for test
        # merge_assets_api(item)

        full_path = find_s3_path_from_item(item=item)

        if full_path:
            logging.info(f'Accessing file {full_path}')
            s3_hook = S3Hook(aws_conn_id=AWS_DEV_CONFIG['africa_dev_usgs_conn_id'])
            s3_obj = s3_hook.get_resource_type('s3').Object(AWS_DEV_CONFIG['s3_source_bucket_name'], full_path)
            response = s3_obj.get(
                **{
                    'RequestPayer': 'requester'
                }
            )
            logging.info(f"Done, response body: {response} Type {type(response)}")
            logging.info(f' JSON {json.loads(response)}')
            new_item = json.loads(response)
            new_item_assets = new_item.get_assets()
            assets = item.get_assets()
            missing_keys = [key for key, asset in new_item_assets.items() if key not in assets.keys()]

            logging.info(f'missing_keys {missing_keys}')

            logging.info(f' Content From USGS now is just merge {response}')
        #  TODO remove this, it's a stopper just for tests
        raise Exception('everything working!!')

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
        # Retrieve messages from the queue
        messages = get_messages()

        if not messages:
            logging.info('No messages were found!')
            return

        logging.info('Start conversion from message to pystac item process')
        items = bulk_convert_dict_to_pystac_item(messages=messages)

        count_messages += len(items)
        logging.info(f'{count_messages} converted')

        logging.info('Start process to replace links')
        bulk_items_replace_links(items=items)
        logging.info('Links Replaced')

        logging.info('Start process to merge assets')
        bulk_items_merge_assets(items=items)
        logging.info('Assets Merged')

        # logging.info('Start process to replace assets links')
        # bulk_items_replace_assets(items=items)
        # logging.info('Assets links replaced')
        #
        # logging.info('Start process to add custom property odc:product')
        # bulk_items_add_odc_product_property(items=items)
        # logging.info('Custom property odc:product added')

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
