import json
import unittest

import mock

from dags.utils.scenes_sync_process import process

# from moto import mock_s3, mock_sns

TEST_BUCKET = "s1-magic-bucket"
TEST_TOPIC = "s1-magic-topic"
SAMPLE_FILE = "data/landsat_usgs_example.json"
REGION_NAME = "af-south-1"


class LandsatUSGSTest(unittest.TestCase):
    pass
    # def test_create_shp_from_json(self):
    #     with open("data/landsat_usgs_example.json") as features:
    #         result = create_shp_file([json.load(features)])
    #
    #         self.assertTrue(bool(result))

    # @mock_s3
    # def test_get_s3(self):
    #     client = boto3.client("s3", region_name=REGION_NAME)
    #     client.create_bucket(Bucket=TEST_BUCKET)
    #     client.upload_file(SAMPLE_FILE, TEST_BUCKET, "some_metadata.json")
    #
    #     self.assertTrue(
    #         get_s3_object(client, TEST_BUCKET, "some_metadata.json")
    #     )
    #
    # @mock_sns
    # def test_send_sns(self):
    #     client = boto3.client("sns", region_name=REGION_NAME)
    #     topic = client.create_topic(Name=TEST_TOPIC)
    #     json_file = open(SAMPLE_FILE)
    #     metadata = json_file.read()
    #
    #     self.assertTrue(
    #         send_sns_message(client, topic["TopicArn"], metadata)
    #     )


class LandsatUSGSProcessTest(unittest.TestCase):

    def setUp(self):
        pass

    @mock.patch('dags.utils.scenes_sync_process.get_messages')
    def test_1(self, mocked):
        with open("data/landsat_usgs_example.json") as features:
            mocked.return_value = iter([json.load(features)])
        process()
        self.assertTrue(True)
