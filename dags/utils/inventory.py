import logging
import json
import gzip
import csv
import os.path as op
from copy import deepcopy
from datetime import datetime, timedelta
from utils.aws_utils import S3
from urllib.parse import urlparse


class InventoryUtils:
    def __init__(self, conn, bucket_name, region):
        self.s3_utils = S3(conn_id=conn)
        self.region = region
        self.bucket_name = bucket_name

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
        logging.info(" Starting Find ")

        continuation_token = None
        while True:
            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.s3_utils.list_objects(
                bucket_name=self.bucket_name,
                region=self.region,
                continuation_token=continuation_token,
            )

            # logging.info(f"Find resp {resp}")
            # logging.info(f"Find Contents {resp.get('Contents')}")
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
        logging.info("Starting latest_manifest")
        # parts = self.urlparse(self.url)
        # get latest manifest file
        today = datetime.now()
        # manifest_url = None

        for dt in [today, today - timedelta(1)]:

            _key = op.join(key, dt.strftime("%Y-%m-%d"))

            logging.info(f"latest_manifest _key {_key}")

            # _url = f"s3://{self.bucket_name}"
            # logging.info(f"latest_manifest _url {_url}")

            manifest_paths = [k for k in self.find(suffix, _key)]

            logging.info(f"latest_manifest manifests {manifest_paths}")

            if len(manifest_paths) == 1:
                manifest_key = manifest_paths[0]
                logging.info(f"latest_manifest Manifest file:{manifest_key}")

                s3_clientobj = self.s3_utils.get_object(
                    bucket_name=self.bucket_name, key=manifest_key, region=self.region
                )

                if not s3_clientobj.get("Body"):
                    raise Exception("Body not found when tried to retrieve manifest")

                return json.loads(s3_clientobj["Body"].read().decode("utf-8"))

        return None

    def https_to_s3(self, url):
        """ Convert https s3 URL to an s3 URL """
        parts = urlparse(url)
        bucket = parts.netloc.split(".")[0]
        s3url = f"s3://{bucket}{parts.path}"
        return s3url

    def urlparse(self, url):
        """ Split S3 URL into bucket, key, filename """
        _url = deepcopy(url)
        if url[0:5] == "https":
            _url = self.https_to_s3(url)
        if _url[0:5] != "s3://":
            raise Exception("Invalid S3 url %s" % _url)

        url_obj = _url.replace("s3://", "").split("/")

        # remove empty items
        url_obj = list(filter(lambda x: x, url_obj))
        return {"bucket": url_obj[0], "key": "/".join(url_obj[1:])}

    def retrieve_manifest_files(self, key: str = "", suffix: str = ""):
        logging.info(f"list_keys_2 starting")
        manifest = self.latest_manifest(key=key, suffix=suffix)
        logging.info(f"retrieve_manifest_files manifest {manifest}")

        if not manifest.get("files"):
            raise Exception("Files not found in manifest")

        return manifest["files"]

    def list_keys_2(self, key: str = "", suffix: str = ""):
        for obj in self.retrieve_manifest_files(key=key, suffix=suffix):

            logging.info(f"list_keys_2 key {obj['key']}")

            gzip_obj = self.s3_utils.get_object(
                bucket_name=self.bucket_name, key=obj["key"], region=self.region
            )

            buffer = gzip.open(gzip_obj["Body"], mode="rt")
            reader = csv.reader(buffer)

            for row in reader:
                yield row

    def list_keys_in_file(self, key: str):
        try:
            if not key:
                raise Exception("Key not provided for list_keys_in_file")

            # logging.info(f"Downloading {key}")
            gzip_obj = self.s3_utils.get_object(
                bucket_name=self.bucket_name, key=key, region=self.region
            )

            buffer = gzip.open(gzip_obj["Body"], mode="rt")
            reader = csv.reader(buffer)

            # logging.info(f"Downloaded and read {key}")

            for row in reader:
                yield row
        except Exception as error:
            logging.error(f"ERROR list_keys_in_file key: {key} error: {error}")
