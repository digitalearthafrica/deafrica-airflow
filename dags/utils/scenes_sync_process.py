"""
    Script to read queue, process messages and save on the S3 bucket
"""
import json
import logging
import os

import rasterio

from collections.abc import Generator
from typing import Iterable

from concurrent.futures import ThreadPoolExecutor, as_completed


from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from pystac import (
    Item,
    Link
)

from stactools.landsat.utils import transform_stac_to_stac

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE

from utils.url_request_utils import (
    get_s3_contents_and_attributes,
    copy_s3_to_s3,
    key_not_existent, save_obj_to_s3, publish_to_sns_topic
)

os.environ['CURL_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'

# ######### AWS CONFIG ############
# ######### USGS ############
USGS_S3_BUCKET_NAME = "usgs-landsat"
USGS_AWS_REGION = "us-west-2"
USGS_INDEX_URL = "https://landsatlook.usgs.gov/stac-browser/"
USGS_API_MAIN_URL = "https://landsatlook.usgs.gov/sat-api/"
USGS_DATA_URL = "https://landsatlook.usgs.gov/data/"

# ######### AFRICA ############
AFRICA_SNS_TOPIC_ARN = "arn:aws:sns:af-south-1:717690029437:deafrica-dev-eks-landsat-topic"
AFRICA_AWS_REGION = "af-south-1"
AFRICA_S3_BUCKET_NAME = "deafrica-landsat-dev"
AFRICA_S3_BUCKET_PATH = f's3://{AFRICA_S3_BUCKET_NAME}/'
AFRICA_S3_BUCKET_URL = f"https://{AFRICA_S3_BUCKET_NAME}.s3.{AFRICA_AWS_REGION}.amazonaws.com/"


def get_queue():
    """
    Connect to the right queue
    :return: QUEUE
    """

    logging.info(f'Connecting to AWS SQS {SYNC_LANDSAT_CONNECTION_SQS_QUEUE}')
    logging.info(f'Conn_id Name {SYNC_LANDSAT_CONNECTION_ID}')

    sqs_hook = SQSHook(aws_conn_id=SYNC_LANDSAT_CONNECTION_ID)
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=SYNC_LANDSAT_CONNECTION_SQS_QUEUE)

    return queue


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
                yield message


def convert_dict_to_pystac_item(message: dict):
    """
    Function to convert a dict to a Pystac Item
    :param message: (dict) message from the SQS already converted from a JSON to DICT
    :return: (Pystac Item) Item
    """

    return Item.from_dict(message)


def delete_messages(messages):
    """
    Delete messages from the queue
    :param messages:
    :return:
    """
    [message.delete() for message in messages]


def replace_links(item: Item):
    """
    Function to replace href URL for Africa's S3 links
    :param item: (Pystac Item) Pystac Item
    :return: None
    """

    self_item = item.get_links(rel="self")
    usgs_self_target = (
        self_item[0].target
        if len(self_item) == 1 and hasattr(self_item[0], "target")
        else None
    )

    # Remove all Links
    item.clear_links()

    self_link = Link(
        rel="self",
        target=usgs_self_target.replace(USGS_API_MAIN_URL, AFRICA_S3_BUCKET_PATH)
        if usgs_self_target
        else AFRICA_S3_BUCKET_PATH,
    )
    item.add_link(self_link)

    derived_from = Link(
        rel="derived_from",
        target=usgs_self_target
        if usgs_self_target
        else USGS_API_MAIN_URL,
    )
    item.add_link(derived_from)

    product_overview = Link(
        rel="product_overview",
        target=generate_odc_product_href(item=item),
    )
    item.add_link(product_overview)


def replace_asset_links(item: Item):
    """
    Function to replace asset's href URL for Africa's S3 links
    :param item: (Pystac Item) Pystac Item
    :return: None
    """

    assets = item.get_assets()
    for key, asset in assets.items():
        asset_href = (asset.href if hasattr(asset, "href") else "").replace(
            USGS_DATA_URL,
            AFRICA_S3_BUCKET_PATH,
        ).replace(
            USGS_INDEX_URL,
            AFRICA_S3_BUCKET_PATH
        )
        if asset_href:
            asset.href = asset_href


def identify_landsat(item: Item):
    """
    Function to identify the satellite that the Item was extract from
    :param item:(Pystac Item)
    :return:(str) Datacube sat name format
    """
    properties = item.properties

    sat = properties.get("eo:platform", "")

    if sat == "LANDSAT_8":
        return 'landsat-8'
    elif sat == "LANDSAT_7":
        return 'landsat-7'
    elif sat == "LANDSAT_5":
        return 'landsat-5'
    else:
        raise Exception(
            f'The sat is {"{0} that is not supported for this process".format(sat) if sat else "not informed"}'
        )


def generate_odc_product_href(item: Item):
    """
    Function to generate custom href to odc:product
    :param item: (Pystac Item)
    :return: (str) href
    """

    digital_earth_africa_url = 'https://explorer-af.digitalearth.africa/product/'

    sat = identify_landsat(item=item)
    if sat == "landsat-8":
        value = "ls8_c2l2"
    elif sat == "landsat-7":
        value = "ls7_c2l2"
    elif sat == "landsat-5":
        value = "ls5_c2l2"

    else:
        raise Exception(
            f'Property odc:product not added due the sat is {sat if sat else "not informed"}'
        )

    return f'{digital_earth_africa_url}{value}'


def add_odc_product_and_odc_region_code_properties(item: Item):
    """
    Function to add Africa's custom property
    :param item: (Pystac Item) Pystac Item
    :return: None
    """

    item.properties.update(
        {
            "odc:product": generate_odc_product_href(item=item),
            "odc:region_code": "{path:03d}{row:03d}".format(
                    path=int(item.properties["landsat:wrs_path"]),
                    row=int(item.properties["landsat:wrs_row"]),
                )
        }
    )


def find_s3_path_and_file_name_from_item(item: Item, start_url: str):
    """
    Function to from the href URL within the index in the list of links,
    replace protocol and domain returning just the path, in addition this function completes the file's name
    and adds the extantion json

    :param start_url: (str) URL that will be removed from the href
    :param item:(Pystac Item) Pystac Item
    :return: (String) full path to the json item
    """

    assets = item.get_assets()
    asset = assets.get("index")
    if asset and hasattr(asset, "href"):
        file_name = f'{asset.href.split("/")[-1]}'
        asset_s3_path = asset.href.replace(start_url, "")

        return {'path': asset_s3_path, 'file_name': file_name}


def retrieve_sat_json_file_from_s3_and_convert_to_item(sr_item: Item):
    """
    Function to access AWS USGS S3 and retrieve their ST json file
    :param sr_item: SR Pystac Item
    :return: ST Pystac Item
    """

    path_and_mame = find_s3_path_and_file_name_from_item(
        item=sr_item,
        start_url=USGS_INDEX_URL
    )

    if path_and_mame and path_and_mame.get('path') and path_and_mame.get('file_name'):
        full_path = f"{path_and_mame['path']}/{path_and_mame['file_name']}_ST_stac.json"

        logging.info(f"Accessing file {full_path}")

        params = {"RequestPayer": "requester"}
        response = get_s3_contents_and_attributes(
            s3_conn_id=SYNC_LANDSAT_CONNECTION_ID,
            bucket_name=USGS_S3_BUCKET_NAME,
            key=full_path,
            params=params,
        )

        return convert_dict_to_pystac_item(json.loads(response))


def merge_assets(item: Item):
    """
    Function to merge missing assets (from the ST) into the main Pystac file (SR)
    :param item: Pystac Item
    :return: None
    """
    # s3://usgs-landsat.s3-us-west-2.amazonaws.com/collection02/level-2/standard/
    # s3://usgs-landsat/collection02/level-1/standard/oli-tirs/2020/157/019/LC08_L1GT_157019_20201207_20201217_02_T2/LC08_L1GT_157019_20201207_20201217_02_T2_stac.json"

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


def bulk_convert_dict_to_pystac_item(messages: Generator):
    """
    Function to convert message from the SQS to a Pystac Item. Function loop over passed Generator and converts 16
    messages simultaneously.

    :param messages: (list) List of dicts from SQS
    :return: (list) List of Pystac Items
    """
    return [
        convert_dict_to_pystac_item(
            json.loads(message.body)
        ) for message in messages
    ]


def bulk_items_replace_links(items):
    """
    Function to handle multiple items, go through all links and replace URL href link for Africa S3 link.

    :param items:(list) List of Pystac Items
    :return:None
    """

    [replace_links(item=item) for item in items]


def bulk_items_replace_assets_link_to_s3_link(items):
    """
    Function to handle multiple items, go through all assets and replace URL link for Africa S3 link.

    :param items:(list) List of Pystac Items
    :return:None
    """

    [replace_asset_links(item=item) for item in items]


def bulk_items_add_odc_product_and_odc_region_code_properties(items: list):
    """
    Function to handle multiple items and add our custom property.
    :param items: (list) List of Pystac Items
    :return: None
    """

    [add_odc_product_and_odc_region_code_properties(item=item) for item in items]


def bulk_items_merge_assets(items: list):
    """
    Function to handle multiple items and merge the missing assets.

    :param items: (list) List of Pystac Items
    :return: None
    """

    [merge_assets(item=item) for item in items]


def retrieve_asset_s3_path_from_item(item: Item):
    """
    Function to change the asset URL into an S3 to be copied straight from the bucket
    :param item: (Pystac Item) Item which will be retrieved the asset path
    :return: (list) Returns a list with assets' S3 links
    """
    assets = item.get_assets()

    if assets:
        return [
            asset.href.replace(
                USGS_DATA_URL, ''
            )
            for key, asset in assets.items()
            if hasattr(asset, "href")
            # Ignores the index key
            and USGS_DATA_URL in asset.href
        ]

    logging.error(f"WARNING No assets to be copied in the Item ({item.id})")


def store_original_asset_s3_path(items: list):
    """
    Function to create a list with all original assets path
    :param items: (list) List of Pystac Items
    :return: (list) Returns a list with all paths
    """
    result = []
    [result.extend(retrieve_asset_s3_path_from_item(item)) for item in items]
    return result


def filter_just_missing_assets(asset_paths: list):
    """

    :param asset_paths:(list)
    :return:
    """
    # Limit number of threads
    num_of_threads = 20
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        logging.info('FILTERING MISSING ASSETS')

        tasks = [
                executor.submit(
                    key_not_existent,
                    SYNC_LANDSAT_CONNECTION_ID,
                    AFRICA_AWS_REGION,
                    AFRICA_S3_BUCKET_NAME,
                    link
                ) for link in asset_paths
            ]

        return [future.result() for future in as_completed(tasks) if future.result()]


def transfer_data_from_usgs_to_africa(asset_address_paths: list):
    """
    Function to transfer data from USGS' S3 to Africa's S3

    :param asset_address_paths:(list) Big dicts with the asset id and links
    :return: None
    """

    # Limit number of threads
    num_of_threads = 20
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        logging.info(
            f"Transferring {num_of_threads} assets simultaneously (Python threads) "
            f"from {USGS_S3_BUCKET_NAME} to {AFRICA_S3_BUCKET_NAME}"
        )

        # Check if the key was already copied
        missing_assets = filter_just_missing_assets(asset_address_paths)

        logging.info(f'missing_assets {missing_assets}')

        task = [
            executor.submit(
                copy_s3_to_s3,
                SYNC_LANDSAT_CONNECTION_ID,
                USGS_S3_BUCKET_NAME,
                AFRICA_S3_BUCKET_NAME,
                link,
                link,
                "requester"
            ) for link in missing_assets
        ]

        return [future.result() for future in as_completed(task) if future.result()]


def make_stac_transformation(item: Item):
    """

    :param item:
    :return:
    """
    with rasterio.Env(
            aws_unsigned=True,
            AWS_S3_ENDPOINT='s3.af-south-1.amazonaws.com'
    ):
        self_link = item.get_single_link('self')
        self_target = self_link.target
        source_link = item.get_single_link('derived_from')
        source_target = source_link.target

        # properti = item.properties.pop('odc:product')

        returned = transform_stac_to_stac(
            item=item,
            self_link=self_target,
            source_link=source_target
        )

        if not returned.properties.get('proj:epsg'):
            raise Exception('<proj:epsg> property is required')

        return returned


def transform_old_stac_to_newer_stac(items: list):
    """
    Function to get a list of Pystac Items and transform into stac file version 1.0.0-beta2
    :param items:(list) Pystac Item List
    :return:(list) stac1 Items
    """

    return [make_stac_transformation(item) for item in items]


def save_stac1_to_s3(item_obj: Item):
    """
    Function to save json stac 1 file into Africa S3
    :param item_obj:(str) json to be saved in S3
    :return:
    """
    path_and_file_name = find_s3_path_and_file_name_from_item(
        item=item_obj,
        start_url=AFRICA_S3_BUCKET_URL
    )

    file_name = f"{path_and_file_name['file_name']}_stac.json"

    logging.info(f"destination {path_and_file_name['path']}/{path_and_file_name['file_name']}_stac_1.json")

    save_obj_to_s3(
        s3_conn_id=SYNC_LANDSAT_CONNECTION_ID,
        file=bytes(json.dumps(item_obj.to_dict()).encode('UTF-8')),
        destination_key=f"{path_and_file_name['path']}/{file_name}",
        destination_bucket=AFRICA_S3_BUCKET_NAME,
    )


def save_stac1_items_to_s3(stac_1_items: list):
    """
    Function to handle a list of Pystac Items and send to S3
    :param stac_1_items:(list) List of Pystac Items to be send to S3 as JSON
    :return: None
    """
    [save_stac1_to_s3(item_obj=item) for item in stac_1_items]


def push_stac_items_to_sns(stac_1_items: list):
    """
    Function to push conveterd items to the SNS.
    :param stac_1_items:(list) List of Pystac Items already in the version 1.0.0-beta2
    :return: None
    """
    [logging.info(f"Pushing {item.to_dict()}") for item in stac_1_items]

    [
        publish_to_sns_topic(
            aws_conn_id=SYNC_LANDSAT_CONNECTION_ID,
            target_arn=AFRICA_SNS_TOPIC_ARN,
            message=json.dumps(item.to_dict())
        ) for item in stac_1_items
    ]


def process():
    """
    Main function to process information from the queue
    :return: None
    """

    count_messages = 0
    try:
        logging.info("Starting process")
        # Retrieve messages from the queue
        messages = get_messages(limit=1)

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

        logging.info("Start process to store all S3 asset href witch will be retrieved from USGS")
        asset_addresses_paths = store_original_asset_s3_path(items=items)
        logging.info("S3 asset hrefs stored")

        logging.info("Start process to replace assets links")
        bulk_items_replace_assets_link_to_s3_link(items=items)
        logging.info("Assets links replaced")

        logging.info("Start process to add custom property odc:product")
        bulk_items_add_odc_product_and_odc_region_code_properties(items=items)
        logging.info("Custom property odc:product added")

        # Copy files from USGS' S3 and store into Africa's S3
        logging.info("Start process to transfer data from USGS S3 to Africa S3")
        transferred_items = transfer_data_from_usgs_to_africa(asset_addresses_paths)
        logging.info(f"{len(transferred_items)} new files were transferred from USGS to AFRICA")

        # Transform stac to 1.0.0-beta.2
        logging.info(f"Starting process to transform stac 0.7.0 to 1.0.0-beta.2")
        stac_1_items = transform_old_stac_to_newer_stac(items)
        logging.info(f"{len(stac_1_items)} transformed from stac 0.7.0 to 1.0.0-beta.2")
        # Save new stac 1 Items into Africa's S3

        logging.info(f"Saving new Items to S3 bucket as JSON")
        save_stac1_items_to_s3(stac_1_items)
        logging.info(f"Items saved")

        # Send to the SNS
        logging.info(f'Starting process to send {len(items)} Items to the SNS')
        push_stac_items_to_sns(stac_1_items=stac_1_items)
        logging.info(f'Items pushed to the SNS {AFRICA_SNS_TOPIC_ARN}')

        logging.info(f'Deleting messages')
        delete_messages(messages)
        logging.info('Messages deleted')
        logging.info("Whole Process finished successfully")

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
