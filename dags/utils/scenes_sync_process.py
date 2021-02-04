"""
    Script to read queue, process messages and save on the S3 bucket
"""
import gzip
import json
import logging
import threading

import pandas as pd
import requests
from airflow.contrib.hooks.aws_sqs_hook import SQSHook

# ######### AWS CONFIG ############
AWS_CONFIG = {
    'africa_dev_conn_id': 'conn_sync_landsat_scene',
    'sqs_queue': 'deafrica-dev-eks-sync-landsat-scene',
    'arn': 'arn:aws:sqs:ap-southeast-2:717690029437:Rodrigo_Test'
}

# ######### S3 CONFIG ############
SRC_BUCKET_NAME = "sentinel-cogs"
QUEUE_NAME = "deafrica-prod-eks-sentinel-2-data-transfer"


# https://github.com/digitalearthafrica/deafrica-extent/blob/master/deafrica-usgs-pathrows.csv.gz

# def get_contents_and_attributes(hook, s3_filepath):
#     bucket_name, key = hook.parse_s3_url(s3_filepath)
#     contents = hook.read_key(key=key, bucket_name=SRC_BUCKET_NAME)
#     contents_dict = json.loads(contents)
#     attributes = get_common_message_attributes(contents_dict)
#     return contents, attributes

def publish_messages(datasets):
    """
    Publish messages
    param message: list of messages
    """
    try:

        def post_messages(messages_to_send):
            queue.send_messages(Entries=messages_to_send)

        sqs_hook = SQSHook(aws_conn_id=AWS_CONFIG["africa_dev_conn_id"])
        sqs = sqs_hook.get_resource_type("sqs")
        queue = sqs.get_queue_by_name(QueueName=AWS_CONFIG["sqs_queue"])

        count = 0
        messages = []
        logging.info("Adding messages...")
        for dataset in datasets:
            message = {
                "Id": str(count),
                "MessageBody": json.dumps(dataset),
            }
            messages.append(message)

            count += 1
            if count % 10 == 0:
                post_messages(messages)
                messages = []

        # Post the last messages if there are any
        if len(messages) > 0:
            post_messages(messages)

        return count

    except Exception as error:
        logging.error(error)
        raise error


if __name__ == "__main__":
    pass
