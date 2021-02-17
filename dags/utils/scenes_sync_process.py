"""
    Script to read queue, process messages and save on the S3 bucket
"""
import json
import logging
from collections.abc import Generator
from typing import Iterable


from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.hooks.S3_hook import S3Hook

from pystac import Item, Link

from infra.connections import (
    SYNC_LANDSAT_CONNECTION_ID,
    INDEX_LANDSAT_CONNECTION_ID,
    SYNC_LANDSAT_USGS_CONNECTION_ID,
)
from infra.variables import (
    SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
    INDEX_LANDSAT_CONNECTION_SQS_QUEUE,
)

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
    "sns_topic_arn": "arn:aws:sns:af-south-1:717690029437:deafrica-dev-eks-landsat-topic",
    "s3_destination_bucket_name": "deafrica-landsat-dev",
    "s3_source_bucket_name": "usgs-landsat",
}


def publish_to_sns_topic(message: dict, attributes: dict = {}):
    sns_hook = AwsSnsHook(aws_conn_id=AWS_DEV_CONFIG["africa_dev_conn_id"])

    response = sns_hook.publish_to_target(
        target_arn=AWS_DEV_CONFIG["sns_topic_arn"],
        message=json.dumps(message),
        message_attributes=attributes,
    )

    logging.info(f"response {response}")


def get_contents_and_attributes(
    s3_conn_id: str = AWS_DEV_CONFIG["africa_dev_conn_id"],
    bucket_name: str = AWS_DEV_CONFIG["s3_source_bucket_name"],
    key: str = None,
    params: dict = {},
):
    """

    :param params:
    :param s3_conn_id: (str) s3_conn_id: Airflow AWS credentials
    :param bucket_name: (str) bucket_name: AWS S3 bucket which the function will connect to
    :param key: (str) Path to the content which the function will access
    :return: (dict) content
    """
    try:
        if not key:
            raise Exception("Key must be informed to be able connecting to AWS S3")

        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        # TODO Test here
        s3_obj = s3_hook.get_resource_type("s3", region_name="us-west-2").Object(
            bucket_name, key
        )
        response = s3_obj.get(**params)
        response_body = response.get("Body")
        json_body = response_body.read()

        return json.loads(json_body)

        # hook = S3Hook(aws_conn_id=s3_conn_id)
        # contents = hook.read_key(key=key, bucket_name=bucket_name)
        # contents_dict = json.loads(contents)
        #
        # return contents_dict
    except Exception as error:
        logging.info(error)
        raise


def copy_s3_to_s3(
    source_key: str,
    destination_key: str = None,
    s3_conn_id: str = AWS_DEV_CONFIG["africa_dev_conn_id"],
    source_bucket: str = AWS_DEV_CONFIG["s3_source_bucket_name"],
    destination_bucket: str = AWS_DEV_CONFIG["s3_destination_bucket_name"],
):
    """
    Function to copy files from one S3 bucket to another.

    :param source_key: (str) Source file path
    :param destination_key: (str) Destination file path
    :param s3_conn_id:(str) Airflow connection id
    :param source_bucket:(str) Source S3 bucket name
    :param destination_bucket:(str) Destination S3 bucket name
    :return: None
    """
    try:

        if not source_key:
            raise Exception(
                "Source key must be informed to be able connecting to AWS S3"
            )
        elif source_key and not destination_key:
            # If destination_key is not informed, build the same structure as the source_key
            destination_key = source_key.replace(source_bucket, destination_bucket)

        logging.info(
            f"Copy functions Parameters "
            f"{source_key} - "
            f"{destination_key} - "
            f"{s3_conn_id} - "
            f"{source_bucket} - "
            f"{destination_bucket}"
        )

        s3_hook = S3Hook(aws_conn_id=s3_conn_id)

        # This uses a boto3 S3 Client directly, so that we can pass the RequestPayer option.
        response = s3_hook.get_conn().copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key, "VersionId": None},
            ACL="public-read",
            RequesterPayer="requester",
        )

        logging.info(f"returned {response}")

    except Exception as error:
        logging.error(error)
        raise error


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


def get_messages(
    limit: int = None,
    visibility_timeout: int = 60,
    message_attributes: Iterable[str] = ["All"],
):
    """
     Get messages from a queue resource.

    :param message_attributes:
    :param visibility_timeout:
    :param limit:Must be between 1 and 10, if provided.
    :return: Generator
    """
    try:
        queue = get_queue()

        count = 0
        while True:
            messages = queue.receive_messages(
                VisibilityTimeout=visibility_timeout,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
                MessageAttributeNames=message_attributes,
            )
            if len(messages) == 0 or (limit and count >= limit):
                break
            else:
                for message in messages:
                    count += 1
                    yield json.loads(message.body)

    except Exception as error:
        raise error


def convert_dict_to_pystac_item(message: dict):
    """
    Function to convert a dict to a Pystac Item
    :param message: (dict) message from the SQS already converted from a JSON to DICT
    :return: (Pystac Item) Item
    """
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
    """
    Function to replace href URL for Africa's S3 links
    :param item: (Pystac Item) Pystac Item
    :return: None
    """
    try:

        self_item = item.get_links(rel="self")
        usgs_self_target = (
            self_item[0].target
            if len(self_item) == 1 and hasattr(self_item[0], "target")
            else ""
        )

        # Remove all Links
        [item.remove_links(rel=link.rel) for link in item.get_links()]

        # Add New Links
        self_link = Link(
            rel="self", target=f's3://{AWS_DEV_CONFIG["s3_destination_bucket_name"]}/'
        )
        item.add_link(self_link)

        derived_from = Link(
            rel="derived_from",
            target=usgs_self_target
            if usgs_self_target
            else "https://landsatlook.usgs.gov/sat-api/",
        )
        item.add_link(derived_from)

        product_overview = Link(
            rel="product_overview",
            target=f's3://{AWS_DEV_CONFIG["s3_destination_bucket_name"]}/',
        )
        item.add_link(product_overview)

    except Exception as error:
        raise error


def replace_asset_links(item: Item):
    """
    Function to replace asset's href URL for Africa's S3 links
    :param item: (Pystac Item) Pystac Item
    :return: None
    """
    try:

        assets = item.get_assets()
        for key, asset in assets.items():
            asset_href = (asset.href if hasattr(asset, "href") else "").replace(
                "https://landsatlook.usgs.gov/data/",
                f's3://{AWS_DEV_CONFIG["s3_destination_bucket_name"]}/',
            )
            if asset_href:
                asset.href = asset_href

    except Exception as error:
        raise error


def add_odc_product_property(item: Item):
    """
    Function to add Africa's custom property
    :param item: (Pystac Item) Pystac Item
    :return: None
    """
    try:

        properties = item.properties
        sat = properties.get("eo:platform", "")

        if sat == "LANDSAT_8":
            value = "ls8_l2sr"
        elif sat == "LANDSAT_7":
            value = "ls7_l2sr"
        elif sat == "LANDSAT_5":
            value = "ls5_l2sr"
        else:
            logging.error(
                f'Property odc:product not added due the sat is {sat if sat else "not informed"}'
            )
            raise Exception(
                f'Property odc:product not added due the sat is {sat if sat else "not informed"}'
            )
        properties.update({"odc:product": value})

    except Exception as error:
        raise error


def find_s3_path_from_item(item: Item):
    """
    Function to from the href URL within the index in the list of links,
    replace protocol and domain returning just the path, in addition this function completes the file's name
    and adds the extantion json

    :param item:(Pystac Item) Pystac Item
    :return: (String) full path to the json item
    """
    try:
        assets = item.get_assets()
        asset = assets.get("index")
        if asset and hasattr(asset, "href"):
            file_name = f'{asset.href.split("/")[-1]}_ST_stac.json'
            asset_s3_path = asset.href.replace(
                "https://landsatlook.usgs.gov/stac-browser/", ""
            )
            full_path = f"{asset_s3_path}/{file_name}"

            return full_path

    except Exception as error:
        raise error


def find_url_path_from_item(item: Item):
    """
    Function to from the href URL within the index in the list of links,
    replace URL and add file name.

    :param item:(Pystac Item) Pystac Item
    :return: (String) full URL to the API
    """
    try:
        # eg.:  https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-st/items/LE07_L2SP_118044_20210115_20210209_02_T1
        assets = item.get_assets()
        asset = assets.get("index")
        if asset and hasattr(asset, "href"):
            file_name = f'{asset.href.split("/")[-1]}'
            full_path = f'{AWS_DEV_CONFIG["usgs_api_main_url"]}/{file_name}'
            logging.info(f"path {full_path}")
            return full_path

    except Exception as error:
        raise error


def merge_assets_api(sr_item: Item):
    """
    Function to instead get from the S3 bucket, get the ST file from the API
    :param sr_item:(Pystac Item) SR Pystac Item
    :return: St Pystac Item
    """
    try:
        # Request API
        response = request_url(url=find_url_path_from_item(item=sr_item))
        new_item = convert_dict_to_pystac_item(message=response)

        return new_item

        # logging.info(f'new_item {new_item}')
        #
        # item_assets = item.get_assets()
        # new_item_assets = new_item.get_assets()
        #
        # logging.info(f'item_assets.keys() {item_assets.keys()}')
        #
        # for asset in new_item_assets:
        #     logging.info(f'asset {asset}')
        #
        # missing_keys = [key for key, asset in new_item_assets.items() if key not in item_assets.keys()]
        # logging.info(f'missing_keys {missing_keys}')

    except Exception as error:
        raise error


def retrieve_sat_json_file_from_s3_and_convert_to_item(sr_item: Item):
    """
    Function to access AWS USGS S3 and retrieve their ST json file
    :param sr_item: SR Pystac Item
    :return: ST Pystac Item
    """
    try:

        full_path = find_s3_path_from_item(item=sr_item)

        if full_path:
            logging.info(f"Accessing file {full_path}")

            params = {"RequestPayer": "requester"}
            response = get_contents_and_attributes(
                # s3_conn_id=AWS_DEV_CONFIG['africa_dev_usgs_conn_id'],
                bucket_name=AWS_DEV_CONFIG["s3_source_bucket_name"],
                key=full_path,
                params=params,
            )

            # conn = BaseHook.get_connection(AWS_DEV_CONFIG['africa_dev_conn_id'])
            #
            # if conn:
            #     extras = conn.get_extra()
            #     logging.info(f'extras {extras}')
            #     conn.set_extra("{'region_name': 'us-west-2'}")
            #     conn.rotate_fernet_key()
            #     logging.info(f'extras After {conn.get_extra()}')

            # s3_hook = S3Hook(aws_conn_id=AWS_DEV_CONFIG['africa_dev_conn_id'])
            # s3_obj = s3_hook.get_resource_type('s3').Object(AWS_DEV_CONFIG['s3_source_bucket_name'], full_path)
            #
            # response = s3_obj.get(
            #     **{
            #         'RequestPayer': 'requester'
            #     }
            # )

            return convert_dict_to_pystac_item(response)

    except Exception as error:
        raise error


def merge_assets(item: Item):
    """
    Function to merge missing assets (from the ST) into the main Pystac file (SR)
    :param item: Pystac Item
    :return: None
    """
    # s3://usgs-landsat.s3-us-west-2.amazonaws.com/collection02/level-2/standard/
    # s3://usgs-landsat/collection02/level-1/standard/oli-tirs/2020/157/019/LC08_L1GT_157019_20201207_20201217_02_T2/LC08_L1GT_157019_20201207_20201217_02_T2_stac.json"
    try:
        # TODO REMOVE it's here jus for test
        # merge_assets_api(item)

        new_item = retrieve_sat_json_file_from_s3_and_convert_to_item(sr_item=item)

        if new_item:
            new_item_assets = new_item.get_assets()

            assets = item.get_assets()

            # Add missing assets to the original item
            [
                item.add_asset(key=key, asset=asset)
                for key, asset in new_item_assets.items()
                if key not in assets.keys()
            ]

            #  TODO remove this, it's a stopper just for tests
            # raise Exception('everything working!!')

    except Exception as error:
        raise error


def bulk_convert_dict_to_pystac_item(messages: Generator):
    """
    Function to convert message from the SQS to a Pystac Item. Function loop over passed Generator and converts 16
    messages simultaneously.

    :param messages: (list) List of dicts from SQS
    :return: (list) List of Pystac Items
    """
    try:
        return [convert_dict_to_pystac_item(message) for message in messages]
    except Exception as error:
        raise error


def bulk_items_replace_links(items):
    """
    Function to handle multiple items, go through all links and replace URL href link for Africa S3 link.

    :param items:(list) List of Pystac Items
    :return:None
    """

    try:
        [replace_links(item=item) for item in items]

    except Exception as error:
        raise error


def bulk_items_replace_assets_link_to_s3_link(items):
    """
    Function to handle multiple items, go through all assets and replace URL link for Africa S3 link.

    :param items:(list) List of Pystac Items
    :return:None
    """
    try:
        [replace_asset_links(item=item) for item in items]
    except Exception as error:
        raise error


def bulk_items_add_odc_product_property(items: list):
    """
    Function to handle multiple items and add our custom property.
    :param items: (list) List of Pystac Items
    :return: None
    """
    try:
        [add_odc_product_property(item=item) for item in items]
    except Exception as error:
        raise error


def bulk_items_merge_assets(items: list):
    """
    Function to handle multiple items and merge the missing assets.

    :param items: (list) List of Pystac Items
    :return: None
    """
    try:
        [merge_assets(item=item) for item in items]
    except Exception as error:
        raise error


def retrieve_asset_s3_path_from_item(item: Item):
    """
    Function to change the asset URL into an S3 to be copied straight from the bucket
    :param item: (Pystac Item) Item which will be retrieved the asset path
    :return: (dict) Returns a dict which the key is the Item id and the value is a list with the assets' S3 links
    """
    try:
        assets = item.get_assets()
        url_to_replace = "https://landsatlook.usgs.gov/data/"
        if assets:
            asset_items = assets.items()
            logging.info(f"asset_items {asset_items}")
            new_asset_hrefs = {
                item.id: [
                    asset.href.replace(
                        url_to_replace, f'/{AWS_DEV_CONFIG["s3_source_bucket_name"]}/'
                    )
                    for key, asset in assets.items()
                    if hasattr(asset, "href")
                    # Ignores the index key
                    and url_to_replace in asset.href
                ]
            }

            return new_asset_hrefs
        logging.error(f"WARNING No assets to be copied in the Item ({item.id})")
    except Exception as error:
        raise error


def store_original_asset_s3_address(items: list):
    """
    Function to from a list of Items convert their assets' href URL into S3 paths and return a dict which the key is
    the item id and the value is a list of the item's S3 paths
    :param items: (list) List of Pystac Items
    :return: (dict) which the key is the item id and the value is a list of the item's S3 paths
    """
    try:
        result = {}
        [result.update(retrieve_asset_s3_path_from_item(item)) for item in items]
        return result

    except Exception as error:
        raise error


def transfer_data_from_usgs_to_africa(asset_address_list: dict):
    """
    Function to transfer data from USGS' S3 to Africa's S3

    :param asset_address_list:(list) List of dicts with the asset id and links
    :return: None
    """

    try:
        # Limit number of threads
        num_of_threads = 20
        results = []
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            # TODO change message
            logging.info(
                f"Transferring {num_of_threads} assets simultaneously (Python threads)"
            )

            arguments = {
                "s3_conn_id": "",
                "source_bucket": "",
                "destination_bucket": "",
                "source_key": "",
                "destination_key": "",
            }
            # task = {
            #     asset_id: executor.submit(copy_s3_tos_3, arguments)
            #     for asset_id, asset_links in asset_address_list.items()
            # }

            for asset_id, asset_links in asset_address_list.items():

                task = [executor.submit(copy_s3_to_s3, link) for link in asset_links]

                for future in as_completed(task):
                    result = future.result()
                    logging.info(f"transfer_data_from_usgs_to_africa result {result}")
                    results.append(result)
                    logging.info(f"Assets transferred")

        return results
    except Exception as error:
        raise error


def process():
    """
    Main function to process information from the queue
    :return: None
    """

    count_messages = 0
    try:
        logging.info("Starting process")
        # Retrieve messages from the queue
        messages = get_messages(limit=20)

        if not messages:
            logging.info("No messages were found!")
            return

        logging.info("Start conversion from message to pystac item process")
        items = bulk_convert_dict_to_pystac_item(messages=messages)

        count_messages += len(items)
        logging.info(f"{count_messages} converted")

        logging.info("Start process to replace links")
        bulk_items_replace_links(items=items)
        logging.info("Links Replaced")

        logging.info("Start process to merge assets")
        bulk_items_merge_assets(items=items)
        logging.info("Assets Merged")

        logging.info(
            "Start process to store all S3 asset href witch will be retrieved from USGS"
        )
        asset_addresses_dict = store_original_asset_s3_address(items=items)
        logging.info("S3 asset hrefs stored")

        logging.info("Start process to replace assets links")
        bulk_items_replace_assets_link_to_s3_link(items=items)
        logging.info("Assets links replaced")

        logging.info("Start process to add custom property odc:product")
        bulk_items_add_odc_product_property(items=items)
        logging.info("Custom property odc:product added")

        # TODO access S3, copy files and save final SR JSON
        # [logging.info(json.dumps(item.to_dict())) for item in items]

        # Copy files from USGS' S3 and store into Africa's S3
        logging.info("Start process to transfer data from USGS S3 to Africa S3")
        transferred_items = transfer_data_from_usgs_to_africa(asset_addresses_dict)
        logging.info(f"{len(transferred_items)} Transferred from USGS to AFRICA")

        # Send to the SNS
        # logging.info(f'sending {len(items)} Items to the SNS')
        # [publish_to_sns_topic(item.to_dict()) for item in items]
        logging.info("The END")

    except StopIteration:
        logging.info(f"All {count_messages} messages read")
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
