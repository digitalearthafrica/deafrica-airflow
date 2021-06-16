"""
Class to support gap report process
"""
import csv
import gzip
import json
import logging
import os.path as op
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from utils.aws_utils import S3


class InventoryUtils:
    """
    InventoryUtils
    """

    def __init__(self, conn, bucket_name, region):
        self.s3_utils = S3(conn_id=conn)
        self.region = region
        self.bucket_name = bucket_name
        logging.info(
            f"Inventory utils set to bucket {self.bucket_name} - and - region {self.region}"
        )

    # Modified derived from https://alexwlchan.net/2018/01/listing-s3-keys-redux/
    def find(
        self,
        suffix: str = "",
        sub_key: str = "",
    ):
        """
        Generate objects in an S3 bucket.
        :param suffix:
        :param sub_key: string to be present in the object name
        """
        continuation_token = None
        while True:
            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.s3_utils.list_objects(
                bucket_name=self.bucket_name,
                region=self.region,
                continuation_token=continuation_token,
            )

            if not resp.get("Contents"):
                return

            for obj in resp["Contents"]:
                if sub_key in obj["Key"] and obj["Key"].endswith(suffix):
                    yield obj["Key"]

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            if resp.get("NextContinuationToken"):
                continuation_token = resp["NextContinuationToken"]
            else:
                break

    def latest_manifest(self, key: str = "", suffix: str = ""):
        """
        Return a dictionary of a manifest file"
        """
        logging.info(
            f"Start Looking for latest manifest file on {self.bucket_name} - {self.region}"
        )

        today = datetime.now()
        # get latest manifest file
        for dt in [today, today - timedelta(1)]:

            _key = op.join(key, dt.strftime("%Y-%m-%d"))

            manifest_paths = [k for k in self.find(suffix=suffix, sub_key=_key)]

            if len(manifest_paths) == 1:

                logging.info(f"Latest Manifest {manifest_paths}")

                manifest_key = manifest_paths[0]

                s3_clientobj = self.s3_utils.get_object(
                    bucket_name=self.bucket_name, key=manifest_key, region=self.region
                )

                if not s3_clientobj.get("Body"):
                    raise Exception("Body not found when tried to retrieve manifest")

                return json.loads(s3_clientobj["Body"].read().decode("utf-8"))

        return None

    def retrieve_manifest_files(self, key: str = "", suffix: str = ""):
        """
        retrieve files from a manifest
        :param key:
        :param suffix:
        :return:
        """
        logging.info(f"Retrieve Manifest Files starting")
        manifest = self.latest_manifest(key=key, suffix=suffix)

        logging.info(
            f"Retrieved Manifest {manifest} with {len(manifest['files'])} Files"
        )

        if not manifest.get("files"):
            raise Exception("Files not found in manifest")

        return manifest["files"]

    def list_keys_in_file(self, key: str):
        """
        Open a GZIP file and read the keys in it
        :param key: (str) path to the GZIP file
        :return:
        """
        try:
            if not key:
                raise Exception(
                    "Argument key is required for List Keys in File function"
                )

            # logging.info(f"Downloading {key}")
            gzip_obj = self.s3_utils.get_object(
                bucket_name=self.bucket_name, key=key, region=self.region
            )

            buffer = gzip.open(gzip_obj["Body"], mode="rt")
            reader = csv.reader(buffer)

            logging.info(f"Downloaded and read {key}")

            for bucket, key, *rest in reader:
                yield key

        except Exception as error:
            logging.error(f"ERROR list_keys_in_file key: {key} error: {error}")

    def retrieve_keys_from_inventory(self, manifest_sufix):
        """
        Function to download 50 inventory GZIP files at the same time an retrieve the keys inside of them.

        :return:
        """
        # Limit number of threads
        num_of_threads = 50
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            logging.info("RETRIEVING KEYS FROM INVENTORY FILE")

            tasks = [
                executor.submit(
                    self.list_keys_in_file,
                    file["key"],
                )
                for file in self.retrieve_manifest_files(suffix=manifest_sufix)
                if file.get("key")
            ]

            for future in as_completed(tasks):
                for key in future.result():
                    yield key
