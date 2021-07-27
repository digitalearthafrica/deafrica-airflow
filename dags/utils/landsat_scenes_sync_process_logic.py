"""
    Script to read from SQS landsat Africa queue, process messages, save a stac 1.0 and
    all content from USGS into Africa's S3 bucket and send the stac 1.0 as message to SNS
    to be indexed by Datacube
"""
import json
import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from pystac import Item, Link

from infra.connections import CONN_LANDSAT_SYNC, CONN_LANDSAT_WRITE
from infra.sns_topics import LANDSAT_SYNC_SNS_ARN
from infra.sqs_queues import LANDSAT_SYNC_SQS_NAME
from infra.s3_buckets import LANDSAT_SYNC_BUCKET_NAME
from landsat_scenes_sync.variables import (
    USGS_API_MAIN_URL,
    USGS_INDEX_URL,
    AFRICA_S3_BUCKET_PATH,
    USGS_DATA_URL,
    REGION,
    AFRICA_S3_ENDPOINT,
    USGS_S3_BUCKET_NAME,
    USGS_AWS_REGION,
)
from utils.aws_utils import S3, SQS, SNS

from utils.sync_utils import time_process, find_s3_path_and_file_name_from_item

from stactools.landsat.utils import transform_stac_to_stac

os.environ["CURL_CA_BUNDLE"] = "/etc/ssl/certs/ca-certificates.crt"


class ScenesSyncProcess:
    """
    Sync scenes from USGS to Africa
    """

    def __init__(self, logger_name: str = ""):
        self.logger_name = logger_name

    def remove_usgs_extension(self, item: Item):
        """
        Function to remove USGS schema.json among the Item extensions

        :param item: (Pystac Item) Pystac Item
        :return: (Pystac Item) Pystac Item
        """
        extensions = list(
            filter(
                lambda x: ("https://landsat.usgs.gov" not in x), item.stac_extensions
            )
        )
        item.stac_extensions.clear()
        item.stac_extensions.extend(extensions)

        return item

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

        # Add Self Link point to stac 1.0
        path_and_name = find_s3_path_and_file_name_from_item(
            item=item, start_url=USGS_DATA_URL
        )

        # Remove all Links
        item.clear_links()

        # Add derived_from pointing to USGS
        derived_from = Link(
            rel="derived_from",
            target=usgs_self_target if usgs_self_target else USGS_API_MAIN_URL,
        )
        item.add_link(derived_from)

        if path_and_name and path_and_name.get("path"):
            self_link = Link(
                rel="self", target=f"{AFRICA_S3_BUCKET_PATH}{path_and_name['path']}"
            )
            item.add_link(self_link)
            logging.info(f"{self.logger_name} - Self link created {self_link}")
        else:
            raise Exception("There was a issue creating SELF link")

        # Add product_overview
        product_overview_link = Link(
            rel="product_overview",
            target=f'{AFRICA_S3_BUCKET_PATH}{path_and_name["path"]}',
        )
        logging.info(
            f"{self.logger_name} - Product Overview link created {product_overview_link}"
        )
        item.add_link(product_overview_link)

    def replace_asset_links(self, item: Item):
        """
        Function to replace asset's href URL for Africa's S3 links
        :param item: (Pystac Item) Pystac Item
        :return: None
        """

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

    def add_odc_product_and_odc_region_code_properties(
        self, item: Item, stac_type: str
    ):
        """
        Function to add Africa's custom properties odc:product and odc:region_code
        :param stac_type:
        :param item: (Pystac Item) Pystac Item
        :return: None
        """

        properties = item.properties

        sat = properties.get("platform") or properties.get("eo:platform")

        if sat == "LANDSAT_8":
            value = f"ls8_{stac_type}"
        elif sat == "LANDSAT_7":
            value = f"ls7_{stac_type}"
        elif sat == "LANDSAT_5":
            value = f"ls5_{stac_type}"
        else:
            raise Exception(
                f'{f"{sat} does not supported for this process" if sat else "was not informed"}'
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

        logging.error(
            f"{self.logger_name} - WARNING No assets to be copied in the Item ({item.id})"
        )

    def filter_just_missing_assets(self, asset_paths: list):
        """

        :param asset_paths:(list)
        :return:
        """
        # Limit number of threads
        num_of_threads = 25
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            logging.info(f"{self.logger_name} - FILTERING MISSING ASSETS")

            s3 = S3(conn_id=CONN_LANDSAT_SYNC)

            tasks = [
                executor.submit(
                    s3.key_not_existent,
                    LANDSAT_SYNC_BUCKET_NAME,
                    link,
                )
                for link in asset_paths
            ]

            return [
                future.result() for future in as_completed(tasks) if future.result()
            ]

    def transfer_data_from_usgs_to_africa(self, asset_address_paths: list) -> int:
        """
        Function to transfer data from USGS' S3 to Africa's S3

        :param asset_address_paths:(list) Big dicts with the asset id and links
        :return: (int) number of transferred assets
        """

        # Limit number of threads
        num_of_threads = 25
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            logging.info(
                f"{self.logger_name} - Transferring {num_of_threads} assets simultaneously "
                f"(Python threads) from {USGS_S3_BUCKET_NAME} to {LANDSAT_SYNC_BUCKET_NAME}"
            )

            # Check if the key was already copied
            missing_assets = self.filter_just_missing_assets(asset_address_paths)

            logging.info(
                f"{self.logger_name} - Copying missing assets {missing_assets}"
            )

            # It has to be the PDS User
            s3 = S3(conn_id=CONN_LANDSAT_WRITE)

            task = [
                executor.submit(
                    s3.copy_s3_to_s3_cross_region,
                    USGS_S3_BUCKET_NAME,
                    LANDSAT_SYNC_BUCKET_NAME,
                    USGS_AWS_REGION,
                    REGION,
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

    def make_stac_transformation(self, item: Item) -> Item:
        """
        Function to transform a stac 07 to 10
        :param item: item 0.7
        :return:(Item) stac 1.0
        """

        import rasterio

        with rasterio.Env(
            aws_unsigned=True,
            AWS_S3_ENDPOINT=AFRICA_S3_ENDPOINT,
            CURL_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt",
            GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
            GDAL_HTTP_MAX_RETRY="10",
            GDAL_HTTP_RETRY_DELAY="0.5",
        ):
            # TODO Remove the Links below once Stactools library is updated.
            #  The new Stactools does not need self link or source link if they are already
            #  present within the item
            # self_link = item.get_single_link("self")
            # self_target = self_link.target
            # source_link = item.get_single_link("derived_from")
            # source_target = source_link.target

            returned = transform_stac_to_stac(
                item=item,
                # self_link=self_target,
                # source_link=source_target
            )

            if not returned.properties.get("proj:epsg"):
                logging.error(
                    f"{self.logger_name} - There was an issue converting stac. <proj:epsg> "
                    f"property is required"
                )
                raise Exception(
                    "There was an issue converting stac. <proj:epsg> property is required"
                )

            return returned

    def save_stac1_to_s3(self, item_obj: Item):
        """
        Function to save json stac 1 file into Africa S3
        :param item_obj:(str) json to be saved in S3
        :return: None
        """

        destination_key = item_obj.get_single_link("self").target.replace(
            AFRICA_S3_BUCKET_PATH, ""
        )

        logging.info(f"{self.logger_name} - destination {destination_key}")

        s3 = S3(conn_id=CONN_LANDSAT_WRITE)

        s3.save_obj_to_s3(
            file=bytes(json.dumps(item_obj.to_dict()).encode("UTF-8")),
            destination_key=destination_key,
            destination_bucket=LANDSAT_SYNC_BUCKET_NAME,
        )


def check_assets_item(item: Item):
    """
    Function to check assets from a given Item in USGS S3 bucket
    :param item:
    :return:
    """

    # Get all Item assets despite index
    assets = [
        asset.href.replace(USGS_DATA_URL, "")
        for key, asset in item.get_assets().items()
        if hasattr(asset, "href")
        # Ignores the index key
        and key != "index"
    ]

    folder_path = "/".join(assets[0].split("/")[:-1])

    logging.info(f"Scene S3 folder path {folder_path}")

    s3 = S3(conn_id=CONN_LANDSAT_WRITE)

    resp = s3.list_objects(
        bucket_name=USGS_S3_BUCKET_NAME,
        region=USGS_AWS_REGION,
        prefix=folder_path,
        request_payer="requester",
    )

    bucket_content = set(content["Key"] for content in resp["Contents"])

    difference = set(assets).difference(bucket_content)

    if difference:
        raise Exception(f"There are missing assets {difference}")

    logging.info(f"All assets present")

    return not difference


def check_already_copied(item: Item) -> bool:
    """
    Check if the item was already copied based on the <scene>

    :param item:
    :return: (bool)
    """
    path_and_file_name = find_s3_path_and_file_name_from_item(
        item=item, start_url=USGS_DATA_URL
    )

    logging.info(f"Checking for {path_and_file_name['path']}")

    # If not exist return the path, if exist return None
    s3 = S3(conn_id=CONN_LANDSAT_SYNC)
    exist = s3.key_not_existent(LANDSAT_SYNC_BUCKET_NAME, path_and_file_name["path"])

    return not bool(exist)


def retrieve_sr_and_st_update_and_convert_to_item(stac_paths_obj: dict):
    """
    Function to access AWS USGS S3 and retrieve their SR and ST json files, then merge the
    ST and SR assets and
    return SR_ITEM and ST_ITEM respectively

    :param stac_paths_obj:(dict) dict with the path for metadata (SR, ST, ML)
    :return:(dict) Return SR and ST stactools Items in a dict
    """

    def retrieve_file(path: str):
        if path:
            s3 = S3(conn_id=CONN_LANDSAT_WRITE)
            return s3.get_s3_contents_and_attributes(
                bucket_name=USGS_S3_BUCKET_NAME,
                key=path,
                params={"RequestPayer": "requester"},
                region=USGS_AWS_REGION,
            )

    sr_path = stac_paths_obj.get("SR")
    st_path = stac_paths_obj.get("ST")
    mtl_path = stac_paths_obj.get("MTL")

    # Retrieve SR
    logging.debug(f"Accessing SR file {sr_path}")
    response = retrieve_file(sr_path)
    sr_dict = json.loads(response) if response else None

    # Retrieve ST
    logging.debug(f"Accessing ST file {st_path}")
    response = retrieve_file(st_path)
    st_dict = json.loads(response) if response else None

    logging.info(f"SR Stac version {sr_dict['stac_version']}")

    logging.info(
        f"ST Stac version {st_dict['stac_version']}"
        if st_dict
        else f"ST Stac not found {st_path}"
    )

    # Update datetime if stac 0.7
    if "1.0.0" not in sr_dict["stac_version"]:

        logging.debug(f"Accessing MTL file {mtl_path}")

        response = retrieve_file(mtl_path)
        mlt_dict = json.loads(response) if response else None

        if not mlt_dict:

            logging.error(f"MLT.json not found on {mtl_path}")
            raise Exception(f"MLT.json issues {mtl_path}")

        elif (
            not mlt_dict.get("LANDSAT_METADATA_FILE")
            or not mlt_dict["LANDSAT_METADATA_FILE"].get("IMAGE_ATTRIBUTES")
            or not mlt_dict["LANDSAT_METADATA_FILE"]["IMAGE_ATTRIBUTES"].get(
                "SCENE_CENTER_TIME"
            )
        ):
            logging.error(f"SCENE_CENTER_TIME not found on MLT file")
            raise Exception("SCENE_CENTER_TIME not found on MLT file")

        scene_center_time = mlt_dict["LANDSAT_METADATA_FILE"]["IMAGE_ATTRIBUTES"][
            "SCENE_CENTER_TIME"
        ]

        if not sr_dict.get("properties") or not sr_dict["properties"].get("datetime"):
            logging.error(f"SR property datetime is missing {sr_dict}")
            raise Exception(f"SR property datetime is missing {sr_dict}")

        scene_date = sr_dict["properties"]["datetime"]
        sr_dict["properties"]["datetime"] = f"{scene_date}T{scene_center_time}"

        if st_dict and "1.0.0" not in st_dict.get("stac_version"):
            if not st_dict.get("properties") or not st_dict["properties"].get(
                "datetime"
            ):
                st_dict["properties"]["datetime"] = sr_dict["properties"]["datetime"]
            else:
                scene_date = st_dict["properties"]["datetime"]
                st_dict["properties"]["datetime"] = f"{scene_date}T{scene_center_time}"

    sr_item = Item.from_dict(sr_dict)

    st_item = None
    if st_dict:
        st_item = Item.from_dict(st_dict)
        if not st_item.assets.get("ST_B10.TIF") and not st_item.assets.get("ST_B6.TIF"):
            logging.error(f"Either ST_B6.TIF and ST_B10.TIF are missing in {st_dict}")
            # st_item = None
            raise Exception(f"Either ST_B6.TIF and ST_B10.TIF are missing in {st_dict}")
        # Check assets in S3
        check_assets_item(item=st_item)
        # remove unnecessary root Link
        st_item.remove_links("root")

    # If we can load the blue band, use it to add proj information
    if not sr_item.assets.get("SR_B2.TIF"):
        logging.error(f"Asset SR_B2.TIF is missing in {sr_item}")
        raise Exception(f"Asset SR_B2.TIF is missing in {sr_item}")

    # Check assets in S3
    check_assets_item(item=sr_item)

    # remove unnecessary root Link
    sr_item.remove_links("root")

    return {"SR": sr_item, "ST": st_item}


def get_messages(
    limit: int = None,
    visibility_timeout: int = 600,  # 10 minutes
):
    """
     Function to read messages from a queue resource and return a generator.

    :param message_attributes:
    :param visibility_timeout: (int) Defaulted to 60
    :param limit:(int) Must be between 1 and 10, if provided.
    :return: Generator
    """

    logging.info(f"Reading messages with {CONN_LANDSAT_SYNC}")
    logging.info(f"Connecting to AWS SQS {LANDSAT_SYNC_SQS_NAME}")

    sqs_queue = SQS(conn_id=CONN_LANDSAT_SYNC, region=REGION)

    count = 0
    while True:
        messages = sqs_queue.receive_messages(
            queue_name=LANDSAT_SYNC_SQS_NAME,
            visibility_timeout=visibility_timeout,
            max_number_messages=1,
            wait_time_seconds=10,
        )

        logging.info(f"Messages {messages}")

        if len(messages) == 0:
            if count == 0:
                logging.info("No messages were found!")
            else:
                logging.info("All messages read")
            break
        if limit and count >= limit:
            logging.info(f"limit of {limit} reached!")
            break
        else:
            for message in messages:
                count += 1
                yield message


def process_item(stac_type: str, item: Item):
    """

    :param stac_type: Item type SR or ST
    :param item: pystac Item to be processed
    :return: None
    """

    logger_name = f"{item.id}_log"

    scenes_sync = ScenesSyncProcess(logger_name=logger_name)

    sns_topic = SNS(conn_id=CONN_LANDSAT_WRITE)

    logger = logging.getLogger(logger_name)

    logger.info(f"{logger_name} - Processing Item {item.to_dict()}")

    logger.info(f"{logger_name} - Start process to replace links")
    scenes_sync.replace_links(item=item)
    logger.info(f"{logger_name} - Links Replaced")

    logger.info(f"{logger_name} - Start process of removing USGS extension")
    scenes_sync.remove_usgs_extension(item=item)
    logger.info(f"{logger_name} - USGS extension removed")

    logger.info(
        f"{logger_name} - Start process to store all S3 asset href "
        f"witch will be retrieved from USGS"
    )
    asset_addresses_paths = scenes_sync.retrieve_asset_s3_path_from_item(item)
    logger.info(f"{logger_name} - S3 asset hrefs stored")

    logger.info(f"{logger_name} - Start process to replace assets links")
    scenes_sync.replace_asset_links(item=item)
    logger.info(f"{logger_name} - Assets links replaced")

    logger.info(f"{logger_name} - Start process to add custom property odc:product")
    scenes_sync.add_odc_product_and_odc_region_code_properties(
        item=item, stac_type=stac_type.lower()
    )
    logger.info(f"{logger_name} - Custom property odc:product added")

    # Copy files from USGS' S3 and store into Africa's S3
    logger.info(
        f"{logger_name} - Start process to transfer data from USGS S3 to Africa S3"
    )
    transferred_items = scenes_sync.transfer_data_from_usgs_to_africa(
        asset_addresses_paths
    )
    logger.info(
        f"{logger_name} - {transferred_items} new files were transferred from USGS to AFRICA"
    )

    # Transform stac to 1.0.0-beta.2
    # Even with USGS sending stac 1.0, we must pass through the
    # transformation process which add information that's missing from USGS
    logger.info(
        f"{logger_name} - Starting process to transform stac 0.7.0 to 1.0.0-beta.2"
    )
    stac_1_item = scenes_sync.make_stac_transformation(item)
    logger.info(f"{logger_name} - Stac transformed from version 0.7.0 to 1.0.0-beta.2")
    # Save new stac 1 Items into Africa's S3

    logger.info(f"{logger_name} - Saving new Items to S3 bucket as JSON")
    scenes_sync.save_stac1_to_s3(item_obj=stac_1_item)
    logger.info(f"{logger_name} - Items saved")

    # Send to the SNS
    logging.info(f"{logger_name} - Pushing Item to the SNS {stac_1_item.to_dict()}")
    sns_topic.publish_to_sns_topic(
        target_arn=LANDSAT_SYNC_SNS_ARN,
        message=json.dumps(stac_1_item.to_dict()),
    )
    logger.info(f"{logger_name} - Items pushed to the SNS {LANDSAT_SYNC_SNS_ARN}")


def process():
    """
    Main function to process information from the queue
    :return: None
    """

    try:
        start = time.time()
        logging.info("Starting process")
        # Retrieve messages from the queue
        messages = get_messages(visibility_timeout=600)

        for message in messages:
            try:
                start_per_msg = time.time()

                logging.info("Processing Message")
                logging.info(f"Message received {message.body}")
                paths = [
                    stac_paths_obj
                    for scene_id, stac_paths_obj in json.loads(message.body).items()
                ]
                logging.info(f"Message Paths {paths}")

                sr_st_item_dict = None
                for stac_paths_obj in json.loads(message.body).values():
                    logging.info(
                        "Retrieving SR and ST metadata merging and converting to pystac item"
                    )

                    sr_st_item_dict = retrieve_sr_and_st_update_and_convert_to_item(
                        stac_paths_obj=stac_paths_obj,
                    )

                logging.info("Checking if stac was already processed")
                already_processed = check_already_copied(item=sr_st_item_dict["SR"])

                if not already_processed:
                    logging.info(f"Stac NOT processed!")

                    # processing SR and ST in parallel
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        logging.info("Start Processing SR and ST")

                        tasks = [
                            executor.submit(process_item, stac_type, item)
                            for stac_type, item in sr_st_item_dict.items()
                            if item
                        ]

                        [
                            future.result()
                            for future in as_completed(tasks)
                            if future.result()
                        ]
                        logging.info("Finished processing SR and ST")

                logging.info(f"Stac processed!")
                logging.info(f"Deleting messages")
                message.delete()
                logging.info("Messages deleted")

                logging.info(
                    f"Message processed and sent in {time_process(start=start_per_msg)}"
                )

            except Exception as error:
                logging.error(f"ERROR returned {error}")
                logging.error(f"Message {message.body}")
                # print traceback but does not stop execution
                traceback.print_exc()

            logging.info(
                "*-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-"
                "*-*-*-*-*-*-*-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"
            )

        logging.info(f"Total execution {time_process(start=start)}")

    except Exception as error:
        logging.error(error)
        raise error

    logging.info("Whole Process finished successfully :)")
