"""
    Script to read queue, process messages and save on the S3 bucket
"""
import json
import logging

from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE

# ######### AWS CONFIG ############
AWS_CONFIG = {
    'africa_dev_conn_id': SYNC_LANDSAT_CONNECTION_ID,
    'sqs_queue': SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
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

def get_queue():
    """
        Connect to the right queue
        :return: QUEUE
    """
    try:
        sqs_hook = SQSHook(aws_conn_id=AWS_CONFIG["africa_dev_conn_id"])
        sqs = sqs_hook.get_resource_type("sqs")
        queue = sqs.get_queue_by_name(QueueName=AWS_CONFIG["sqs_queue"])

        return queue

    except Exception as error:
        raise error


def get_messages():
    """
        Get messages from a queue resource.
        :return: message
    """
    try:
        queue = get_queue()

        while True:
            messages = queue.receive_messages(
                VisibilityTimeout=60,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
                # MessageAttributeNames=["All"],
            )
            if len(messages) == 0:
                break
            else:
                for message in messages:
                    yield message

    except Exception as error:
        raise error


def delete_messages(messages: list = None):
    """
    Delete messages from the queue
    :param messages:
    :return:
    """
    try:
        pass
    except Exception as error:
        raise error


def read_messages():
    try:
        test = get_messages()
        count = 0
        for t in test:
            body = json.loads(t.body)
            metadata = json.loads(body["Message"])
            logging.info(f'body {body}')
            logging.info(f'Message {metadata}')
            # logging.info(t)
            if count > 10:
                break
            count += 1
    except Exception as error:
        logging.error(error)
