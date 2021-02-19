"""
    Script to read queue, process messages and save on the S3 bucket
"""
import json
import logging
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable

from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from pystac import Item, Link

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE

# from utils.stac import transform_stac_to_stac
from utils.url_request_utils import (
    request_url,
    get_s3_contents_and_attributes,
    copy_s3_to_s3,
    key_not_existent
)

# ######### AWS CONFIG ############
# ######### USGS ############
USGS_API_MAIN_URL = "https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-st/items/"
USGS_DATA_URL = "https://landsatlook.usgs.gov/data/"
USGS_S3_BUCKET_NAME = "usgs-landsat"
USGS_AWS_REGION = "us-west-2"
# ######### AFRICA ############
AFRICA_SNS_TOPIC_ARN = "arn:aws:sns:af-south-1:717690029437:deafrica-dev-eks-landsat-topic"
AFRICA_S3_BUCKET_NAME = "deafrica-landsat-dev"
AFRICA_AWS_REGION = "af-south-1"


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
                yield json.loads(message.body)


def convert_dict_to_pystac_item(message: dict):
    """
    Function to convert a dict to a Pystac Item
    :param message: (dict) message from the SQS already converted from a JSON to DICT
    :return: (Pystac Item) Item
    """

    return Item.from_dict(message)


def delete_messages(messages: list = None):
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
        else ""
    )

    # Remove all Links
    [item.remove_links(rel=link.rel) for link in item.get_links()]

    # Add New Links
    self_link = Link(
        rel="self", target=f's3://{AFRICA_S3_BUCKET_NAME}/'
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
        target=f's3://{AFRICA_S3_BUCKET_NAME}/',
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
            "https://landsatlook.usgs.gov/data/",
            f's3://{AFRICA_S3_BUCKET_NAME}/',
        )
        if asset_href:
            asset.href = asset_href


def add_odc_product_property(item: Item):
    """
    Function to add Africa's custom property
    :param item: (Pystac Item) Pystac Item
    :return: None
    """

    properties = item.properties
    sat = properties.get("eo:platform", "")

    digital_earth_africa_url = 'https://explorer-af.digitalearth.africa/product/'

    if sat == "LANDSAT_8":
        value = "ls8_l2sr"
    elif sat == "LANDSAT_7":
        value = "ls7_l2sr"
    elif sat == "LANDSAT_5":
        value = "ls5_l2sr"
    else:
        raise Exception(
            f'Property odc:product not added due the sat is {sat if sat else "not informed"}'
        )
    properties.update({"odc:product": f'{digital_earth_africa_url}{value}'})


def find_s3_path_from_item(item: Item):
    """
    Function to from the href URL within the index in the list of links,
    replace protocol and domain returning just the path, in addition this function completes the file's name
    and adds the extantion json

    :param item:(Pystac Item) Pystac Item
    :return: (String) full path to the json item
    """

    assets = item.get_assets()
    asset = assets.get("index")
    if asset and hasattr(asset, "href"):
        file_name = f'{asset.href.split("/")[-1]}_ST_stac.json'
        asset_s3_path = asset.href.replace(
            "https://landsatlook.usgs.gov/stac-browser/", ""
        )
        full_path = f"{asset_s3_path}/{file_name}"

        return full_path


def find_url_path_from_item(item: Item):
    """
    Function to from the href URL within the index in the list of links,
    replace URL and add file name.

    :param item:(Pystac Item) Pystac Item
    :return: (String) full URL to the API
    """

    # eg.:  https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-st/items/LE07_L2SP_118044_20210115_20210209_02_T1
    assets = item.get_assets()
    asset = assets.get("index")
    if asset and hasattr(asset, "href"):
        file_name = f'{asset.href.split("/")[-1]}'
        full_path = f'{USGS_API_MAIN_URL}/{file_name}'
        logging.info(f"path {full_path}")
        return full_path


def merge_assets_api(sr_item: Item):
    """
    Function to instead get from the S3 bucket, get the ST file from the API
    :param sr_item:(Pystac Item) SR Pystac Item
    :return: St Pystac Item
    """

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


def retrieve_sat_json_file_from_s3_and_convert_to_item(sr_item: Item):
    """
    Function to access AWS USGS S3 and retrieve their ST json file
    :param sr_item: SR Pystac Item
    :return: ST Pystac Item
    """

    full_path = find_s3_path_from_item(item=sr_item)

    if full_path:
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


def bulk_convert_dict_to_pystac_item(messages: Generator):
    """
    Function to convert message from the SQS to a Pystac Item. Function loop over passed Generator and converts 16
    messages simultaneously.

    :param messages: (list) List of dicts from SQS
    :return: (list) List of Pystac Items
    """
    return [convert_dict_to_pystac_item(message) for message in messages]


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


def bulk_items_add_odc_product_property(items: list):
    """
    Function to handle multiple items and add our custom property.
    :param items: (list) List of Pystac Items
    :return: None
    """

    [add_odc_product_property(item=item) for item in items]


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

        return [future.result() for future in as_completed(tasks)]


def transfer_data_from_usgs_to_africa(asset_address_paths: list):
    """
    Function to transfer data from USGS' S3 to Africa's S3

    :param asset_address_paths:(list) Big dicts with the asset id and links
    :return: None
    """

    # Limit number of threads
    num_of_threads = 20
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        # TODO change message
        logging.info(
            f"Transferring {num_of_threads} assets simultaneously (Python threads)"
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
                link.replace(USGS_S3_BUCKET_NAME, AFRICA_S3_BUCKET_NAME),
                "requester"
            ) for link in missing_assets
        ]

        return [future.result() for future in as_completed(task) if future.result()]


def make_stac_transformation(item: Item):
    """

    :param item:
    :return:
    """
    # self_link = ''
    # source_link = ''
    # LINKS [
    # <Link rel=self target=s3://deafrica-landsat-dev/>,
    # <Link rel=derived_from target=https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items/LC08_L2SP_198048_20130330_20200913_02_T1>,
    # <Link rel=product_overview target=s3://deafrica-landsat-dev/>
    # ]
    # logging.info(f'LINKS {[link for link in item.get_links()]}')

    file = None
    for key, asset in item.get_assets().items():
        if key == "SR_B2.TIF":
            # blue_asset = getattr(asset, "SR_B2.TIF")
            blue_asset = item.assets["SR_B2.TIF"]
            logging.info(f"Here is the blue Asset {blue_asset}")
            if hasattr(blue_asset, "href"):
                logging.info(f"Asset HREF {blue_asset.href}")

                file = get_s3_contents_and_attributes(
                    s3_conn_id=SYNC_LANDSAT_CONNECTION_ID,
                    bucket_name=AFRICA_S3_BUCKET_NAME,
                    region_name=AFRICA_AWS_REGION,
                    key=blue_asset.href.replace('s3://deafrica-landsat-dev/', '')
                )

            else:
                logging.info(f"NO HREF")
            break

    logging.info(f'just before test the type is {type(file)}')
    # test = transform_stac_to_stac(
    #     item=item,
    #     blue_asset=file
    #     # self_link=self_link,
    #     # source_link=source_link
    # )
    # logging.info(f'TEST RESULT Seems work')
    # return json.loads(test.to_dict())


def transform_old_stac_to_newer_stac(items: list):
    """
    Function to get a list of Pystac Items and transform into stac file version 1.0.0-beta2
    :param items:(list) Pystac Item List
    :return:(list) stac1 Items
    """
    return [make_stac_transformation(item) for item in items]


def save_stac1_to_s3(item):
    """
    Function to save json stac 1 file into Africa S3
    :param item:
    :return:
    """
    pass


def save_stac1_items_to_s3(stac_1_items: list):
    [save_stac1_to_s3(item=item) for item in stac_1_items]


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
        bulk_items_add_odc_product_property(items=items)
        logging.info("Custom property odc:product added")

        # TODO access S3, copy files and save final SR JSON
        # [logging.info(json.dumps(item.to_dict())) for item in items]

        # Copy files from USGS' S3 and store into Africa's S3
        logging.info("Start process to transfer data from USGS S3 to Africa S3")
        transferred_items = transfer_data_from_usgs_to_africa(asset_addresses_paths)
        logging.info(f"{len(transferred_items)} new files were transferred from USGS to AFRICA")

        # Transform stac to 1.0.0-beta.2
        # TODO complete function
        stac_1_items = transform_old_stac_to_newer_stac(items)
        logging.info(f"I have {len(stac_1_items)} stac {len(items)} items")
        # Save new stac 1 Items into Africa's S3
        # TODO complete function
        # save_stac1_items_to_s3(stac_1_items)

        # Send to the SNS
        # logging.info(f'Starting process to send {len(items)} Items to the SNS')
        # [
        #     publish_to_sns_topic(
        #         aws_conn_id=SYNC_LANDSAT_CONNECTION_ID,
        #         target_arn=AFRICA_SNS_TOPIC_ARN,
        #         message=json.dumps(item.to_dict())
        #     ) for item in items
        # ]
        logging.info("The END")

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
