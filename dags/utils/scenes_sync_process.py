"""
    Script to read queue, process messages and save on the S3 bucket
"""
import json
import logging

import fiona
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE

# ######### AWS CONFIG ############
from shapely.geometry import mapping

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
        [message.delete() for message in messages]
    except Exception as error:
        raise error


def change_metadata(dataset):
    try:
        # Assets Link
        # s3://usgs-landsat/
        pass
    except Exception as error:
        raise error


def check_parameters(message):
    try:
        return bool(
            message.get('geometry')
            and message.get('properties')
            and message['geometry'].get('coordinates')
        )

    except Exception as error:
        raise error


def create_shp_file():
    try:
        # pprint.pprint(fiona.FIELD_TYPES_MAP)
        # {'date': <class 'fiona.rfc3339.FionaDateType'>,
        #  'datetime': <class 'fiona.rfc3339.FionaDateTimeType'>,
        #  'float': <class 'float'>,
        #  'int': <class 'int'>,
        #  'str': <class 'str'>,
        #  'time': <class 'fiona.rfc3339.FionaTimeType'>}

        # Define a polygon feature geometry with one attribute
        schema = {
            'geometry': 'Polygon',
            'properties': {
                'collection': 'str',
                'datetime': 'datetime',
                'eo:cloud_cover': 'float',
                'eo:sun_azimuth': 'float',
                'eo:sun_elevation': 'float',
                'eo:platform': 'str',
                'eo:instrument': 'str',
                'eo:off_nadir': 'int',
                # 'eo:gsd': 'datetime',
                'landsat:cloud_cover_land': 'float',
                'landsat:wrs_type': 'str',
                'landsat:wrs_path': 'str',
                'landsat:wrs_row': 'str',
                'landsat:scene_id': 'str',
                'landsat:collection_category': 'str',
                'landsat:collection_number': 'str',
                # 'eo:bands': 'list',  ???
            },
        }

        # Write a new Shapefile
        with fiona.open('test.shp', 'w', 'ESRI Shapefile', schema) as c:
            for message in get_messages():
                if check_parameters(message=message):
                    poly = message['geometry']['coordinates']
                    c.write(
                        {
                            'geometry': mapping(poly),
                            'properties': message['properties'],
                        }
                    )


    except Exception as error:
        raise error


def read_messages():
    try:
        test = get_messages()
        count = 0
        logging.info(f'Started')
        for t in test:
            # logging.info(f'message  {t}')
            body = json.loads(t.body)
            # logging.info(f'body {body}')
            # logging.info(f'Delete')
            t.delete()
            if count > 20000:
                break
            count += 1
        logging.info(f'Completed')
    except Exception as error:
        logging.error(error)
