"""
    Script to read from SQS landsat Africa queue, process messages, save a stac 1.0 and
    all content from USGS into Africa's S3 bucket and send the stac 1.0 as message to SNS to be indexed by Datacube
"""
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable

import rasterio
from pystac import Item, Link
from stactools.landsat.utils import transform_stac_to_stac

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import (
    SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
    LANDSAT_SYNC_SNS_TOPIC_ARN,
)
from landsat_scenes_sync.variables import (
    USGS_API_MAIN_URL,
    USGS_INDEX_URL,
    AFRICA_S3_BUCKET_PATH,
    USGS_DATA_URL,
    AWS_DEFAULT_REGION,
    LANDSAT_SYNC_S3_BUCKET_NAME,
    AFRICA_S3_ENDPOINT,
    USGS_S3_BUCKET_NAME,
    USGS_AWS_REGION,
)
from utils.url_request_utils import time_process
from utils.aws_utils import S3, SQS, SNS

os.environ["CURL_CA_BUNDLE"] = "/etc/ssl/certs/ca-certificates.crt"


class ScenesSyncProcess:
    def __init__(self, conn_id):
        self.s3 = S3(conn_id=conn_id)
        self.sns = SNS(conn_id=conn_id)

    def replace_links(self, item: Item):
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

        # Add derived_from pointing to USGS
        derived_from = Link(
            rel="derived_from",
            target=usgs_self_target if usgs_self_target else USGS_API_MAIN_URL,
        )
        item.add_link(derived_from)

        # Add Self Link point to stac 1.0
        path_and_mame = self.find_s3_path_and_file_name_from_item(
            item=item, start_url=USGS_INDEX_URL
        )

        if (
            path_and_mame
            and path_and_mame.get("path")
            and path_and_mame.get("file_name")
        ):
            file_name = f"{path_and_mame['file_name']}_stac.json"
            self_path = f"{path_and_mame['path']}/{file_name}"

            self_link = Link(rel="self", target=f"{AFRICA_S3_BUCKET_PATH}{self_path}")
            item.add_link(self_link)
            logging.info(f"Self link created {self_link}")
        else:
            raise Exception("There was a issue creating SELF link")

        # Add product_overview
        product_overview_link = Link(
            rel="product_overview",
            target=f'{AFRICA_S3_BUCKET_PATH}{path_and_mame["path"]}',
        )
        logging.info(f"Product Overview link created {product_overview_link}")
        item.add_link(product_overview_link)

    def replace_asset_links(self, item: Item):
        """
        Function to replace asset's href URL for Africa's S3 links
        :param item: (Pystac Item) Pystac Item
        :return: None
        """
        # TODO Move to a utils file
        assets = item.get_assets()
        for key, asset in assets.items():
            asset_href = (
                (asset.href if hasattr(asset, "href") else "")
                .replace(
                    USGS_DATA_URL,
                    AFRICA_S3_BUCKET_PATH,
                )
                .replace(USGS_INDEX_URL, AFRICA_S3_BUCKET_PATH)
            )
            if asset_href:
                asset.href = asset_href

    def add_odc_product_and_odc_region_code_properties(self, item: Item):
        """
        Function to add Africa's custom properties odc:product and odc:region_code
        :param item: (Pystac Item) Pystac Item
        :return: None
        """

        properties = item.properties

        sat = properties.get("eo:platform", "")

        if sat == "LANDSAT_8":
            value = "ls8_c2l2"
        elif sat == "LANDSAT_7":
            value = "ls7_c2l2"
        elif sat == "LANDSAT_5":
            value = "ls5_c2l2"
        else:
            raise Exception(
                f'The sat is {"{0} that is not supported for this process".format(sat) if sat else "not informed"}'
            )

        item.properties.update(
            {
                "odc:product": value,
                "odc:region_code": "{path:03d}{row:03d}".format(
                    path=int(item.properties["landsat:wrs_path"]),
                    row=int(item.properties["landsat:wrs_row"]),
                ),
            }
        )

    def find_s3_path_and_file_name_from_item(self, item: Item, start_url: str):
        """
        Function to from the href URL within the index in the list of links,
        replace protocol and domain returning just the path, in addition this function completes the file's name
        and adds the extension json

        :param start_url: (str) URL that will be removed from the href
        :param item:(Pystac Item) Pystac Item
        :return: (String) full path to the json item
        """

        assets = item.get_assets()
        asset = assets.get("index")
        if asset and hasattr(asset, "href"):

            logging.debug(f"asset.href {asset.href}")

            file_name = f'{asset.href.split("/")[-1]}'
            asset_s3_path = asset.href.replace(start_url, "")

            return {"path": asset_s3_path, "file_name": file_name}

    def retrieve_sat_json_file_from_s3_and_convert_to_item(self, sr_item: Item):
        """
        Function to access AWS USGS S3 and retrieve their ST json file
        :param sr_item: SR Pystac Item
        :return: ST Pystac Item
        """

        path_and_mame = self.find_s3_path_and_file_name_from_item(
            item=sr_item, start_url=USGS_INDEX_URL
        )

        if (
            path_and_mame
            and path_and_mame.get("path")
            and path_and_mame.get("file_name")
        ):
            full_path = (
                f"{path_and_mame['path']}/{path_and_mame['file_name']}_ST_stac.json"
            )

            logging.debug(f"Accessing file {full_path}")

            params = {"RequestPayer": "requester"}
            response = self.s3.get_s3_contents_and_attributes(
                bucket_name=USGS_S3_BUCKET_NAME,
                key=full_path,
                params=params,
            )

            return Item.from_dict(json.loads(response))

    def merge_assets(self, item: Item):
        """
        Function to merge missing assets (from the ST) into the main Pystac file (SR)
        :param item: Pystac Item
        :return: None
        """

        new_item = self.retrieve_sat_json_file_from_s3_and_convert_to_item(sr_item=item)

        if new_item:
            new_item_assets = new_item.get_assets()

            assets = item.get_assets()

            # Add missing assets to the original item
            [
                item.add_asset(key=key, asset=asset)
                for key, asset in new_item_assets.items()
                if key not in assets.keys()
            ]

    def retrieve_asset_s3_path_from_item(self, item: Item):
        """
        Function to change the asset URL into an S3 to be copied straight from the bucket
        :param item: (Pystac Item) Item which will be retrieved the asset path
        :return: (list) Returns a list with assets' S3 links
        """
        assets = item.get_assets()

        if assets:
            return [
                asset.href.replace(USGS_DATA_URL, "")
                for key, asset in assets.items()
                if hasattr(asset, "href")
                # Ignores the index key
                and USGS_DATA_URL in asset.href
            ]

        logging.error(f"WARNING No assets to be copied in the Item ({item.id})")

    def filter_just_missing_assets(self, asset_paths: list):
        """

        :param asset_paths:(list)
        :return:
        """
        # Limit number of threads
        num_of_threads = 20
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            logging.info("FILTERING MISSING ASSETS")

            tasks = [
                executor.submit(
                    self.s3.key_not_existent,
                    LANDSAT_SYNC_S3_BUCKET_NAME,
                    link,
                )
                for link in asset_paths
            ]

            return [
                future.result() for future in as_completed(tasks) if future.result()
            ]

    def transfer_data_from_usgs_to_africa(self, asset_address_paths: list):
        """
        Function to transfer data from USGS' S3 to Africa's S3

        :param asset_address_paths:(list) Big dicts with the asset id and links
        :return: None
        """

        # Limit number of threads
        num_of_threads = 25
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            logging.info(
                f"Transferring {num_of_threads} assets simultaneously (Python threads) "
                f"from {USGS_S3_BUCKET_NAME} to {LANDSAT_SYNC_S3_BUCKET_NAME}"
            )

            # Check if the key was already copied
            missing_assets = self.filter_just_missing_assets(asset_address_paths)

            logging.info(f"Copying missing assets {missing_assets}")
            logging.info(f"USING {SYNC_LANDSAT_CONNECTION_ID}")

            task = [
                executor.submit(
                    self.s3.copy_s3_to_s3_cross_region,
                    USGS_S3_BUCKET_NAME,
                    LANDSAT_SYNC_S3_BUCKET_NAME,
                    USGS_AWS_REGION,
                    AWS_DEFAULT_REGION,
                    link,
                    link,
                    "requester",
                )
                for link in missing_assets
            ]

            [
                future.result()
                for future in as_completed(task)
                if future.result() and future.result() != ""
            ]
            return len(missing_assets)

    def make_stac_transformation(self, item: Item):
        """
        Function to transform a stac 07 to 10
        :param item:
        :return:
        """

        with rasterio.Env(
            aws_unsigned=True,
            AWS_S3_ENDPOINT=AFRICA_S3_ENDPOINT,
            CURL_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt",
        ):
            # TODO Remove the Links below once Stactools library is updated
            self_link = item.get_single_link("self")
            self_target = self_link.target
            source_link = item.get_single_link("derived_from")
            source_target = source_link.target

            logging.info(
                f'item.assets.get("SR_B2.TIF") {item.assets["SR_B2.TIF"].href}'
            )

            returned = transform_stac_to_stac(
                item=item, self_link=self_target, source_link=source_target
            )

            if not returned.properties.get("proj:epsg"):
                raise Exception("<proj:epsg> property is required")

            return returned

    def save_stac1_to_s3(self, item_obj: Item):
        """
        Function to save json stac 1 file into Africa S3
        :param item_obj:(str) json to be saved in S3
        :return:
        """

        destination_key = item_obj.get_single_link("self").target.replace(
            AFRICA_S3_BUCKET_PATH, ""
        )

        logging.info(f"destination {destination_key}")

        self.s3.save_obj_to_s3(
            file=bytes(json.dumps(item_obj.to_dict()).encode("UTF-8")),
            destination_key=destination_key,
            destination_bucket=LANDSAT_SYNC_S3_BUCKET_NAME,
        )

    def check_already_copied(self, item):

        path_and_file_name = self.find_s3_path_and_file_name_from_item(
            item=item, start_url=USGS_INDEX_URL
        )

        file_name = f"{path_and_file_name['file_name']}_stac.json"
        key = f"{path_and_file_name['path']}/{file_name}"

        logging.info(f"Checking for {key}")

        # If not exist return the path, if exist return None
        exist = self.s3.key_not_existent(LANDSAT_SYNC_S3_BUCKET_NAME, key)

        return not bool(exist)


def get_messages(
    limit: int = None,
    visibility_timeout: int = 600,  # 10 minutes, time that the message won't be available in the queue
    message_attributes: Iterable[str] = ["All"],
):
    """
     Function to read messages from a queue resource and return a generator.

    :param message_attributes:
    :param visibility_timeout: (int) Defaulted to 60
    :param limit:(int) Must be between 1 and 10, if provided. If not informed will read till there are nothing left
    :return: Generator
    """

    logging.info(f"Connecting to AWS SQS {SYNC_LANDSAT_CONNECTION_SQS_QUEUE}")
    logging.info(f"Conn_id Name {SYNC_LANDSAT_CONNECTION_ID}")

    sqs_queue = SQS(conn_id=SYNC_LANDSAT_CONNECTION_ID)
    queue = sqs_queue.get_queue(
        queue_name=SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
    )

    count = 0
    while True:
        messages = queue.receive_messages(
            VisibilityTimeout=visibility_timeout,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=message_attributes,
        )

        if len(messages) == 0:
            logging.info("No messages were found!")
            break
        if limit and count >= limit:
            logging.info(f"limit of {limit} reached!")
            break
        else:
            for message in messages:
                count += 1
                yield message


def process():
    """
    Main function to process information from the queue
    :return: None
    """

    try:
        start = time.time()

        logging.info("Starting process")

        scenes_sync = ScenesSyncProcess(conn_id=SYNC_LANDSAT_CONNECTION_ID)
        sns_topic = SNS(conn_id=SYNC_LANDSAT_CONNECTION_ID)

        # Retrieve messages from the queue
        messages = get_messages(visibility_timeout=600)

        for message in messages:
            try:
                start_per_msg = time.time()

                logging.info(f"Message received {message.body}")

                logging.info("Start conversion from message to pystac item process")
                item = Item.from_dict(json.loads(message.body))
                logging.info(f"Message converted {item.to_dict()}")

                logging.info("Checking if Stac was already processed")
                already_processed = scenes_sync.check_already_copied(item=item)
                logging.info(
                    f"Stac {'processed' if already_processed else 'NOT processed'}!"
                )

                if not already_processed:

                    logging.info("Start process to replace links")
                    scenes_sync.replace_links(item=item)
                    logging.info("Links Replaced")

                    logging.info("Start process to merge assets")
                    scenes_sync.merge_assets(item=item)
                    logging.info("Assets Merged")

                    logging.info(
                        "Start process to store all S3 asset href witch will be retrieved from USGS"
                    )
                    asset_addresses_paths = (
                        scenes_sync.retrieve_asset_s3_path_from_item(item)
                    )
                    logging.info("S3 asset hrefs stored")

                    logging.info("Start process to replace assets links")
                    scenes_sync.replace_asset_links(item=item)
                    logging.info("Assets links replaced")

                    logging.info("Start process to add custom property odc:product")
                    scenes_sync.add_odc_product_and_odc_region_code_properties(
                        item=item
                    )
                    logging.info("Custom property odc:product added")

                    # Copy files from USGS' S3 and store into Africa's S3
                    logging.info(
                        "Start process to transfer data from USGS S3 to Africa S3"
                    )
                    transferred_items = scenes_sync.transfer_data_from_usgs_to_africa(
                        asset_addresses_paths
                    )
                    logging.info(
                        f"{transferred_items} new files were transferred from USGS to AFRICA"
                    )

                    # Transform stac to 1.0.0-beta.2
                    logging.info(
                        f"Starting process to transform stac 0.7.0 to 1.0.0-beta.2"
                    )
                    stac_1_item = scenes_sync.make_stac_transformation(item)
                    logging.info(f"Stac transformed from version 0.7.0 to 1.0.0-beta.2")
                    # Save new stac 1 Items into Africa's S3

                    logging.info(f"Saving new Items to S3 bucket as JSON")
                    scenes_sync.save_stac1_to_s3(item_obj=stac_1_item)
                    logging.info(f"Items saved")

                    # Send to the SNS
                    logging.info(f"Pushing Item to the SNS {stac_1_item.to_dict()}")
                    sns_topic.publish_to_sns_topic(
                        target_arn=LANDSAT_SYNC_SNS_TOPIC_ARN,
                        message=json.dumps(stac_1_item.to_dict()),
                    )
                    logging.info(
                        f"Items pushed to the SNS {LANDSAT_SYNC_SNS_TOPIC_ARN}"
                    )

                logging.info(f"Deleting messages")
                message.delete()
                logging.info("Messages deleted")

                logging.info(
                    f"Message processed and sent in {time_process(start=start_per_msg)}"
                )

                logging.info(f"Total execution {time_process(start=start)}")

            except Exception as error:
                logging.error(f"ERROR returned {error}")
                logging.error(f"Message {message.body}")

            logging.info(
                "*-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"
            )

        logging.info("Whole Process finished successfully :)")
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
