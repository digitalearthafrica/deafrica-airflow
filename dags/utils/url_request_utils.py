import json
import logging

import botocore
import requests

from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.hooks.S3_hook import S3Hook


def test_http_return(returned):
    """
    Test API response
    :param returned:
    :return:
    """
    if hasattr(returned, "status_code") and returned.status_code != 200:
        # TODO Implement dead queue here
        url = returned.url if hasattr(returned, "url") else "Not informed"
        content = returned.content if hasattr(returned, "content") else "Not informed"
        text = returned.text if hasattr(returned, "text") else "Not informed"
        status_code = (
            returned.status_code if hasattr(returned, "status_code") else "Not informed"
        )
        reason = returned.reason if hasattr(returned, "reason") else "Not informed"
        raise Exception(
            f"API return is not 200: \n"
            f"-url: {url} \n"
            f"-content: {content} \n"
            f"-text: {text} \n"
            f"-status_code: {status_code} \n"
            f"-reason: {reason} \n"
        )


def request_url(url: str, params: dict = {}):
    try:

        resp = requests.get(url=url, params=params)
        # Check return 200
        test_http_return(resp)
        return json.loads(resp.content)

    except Exception as error:
        raise error


def check_s3_copy_return(returned: dict):
    if returned.get('ResponseMetadata') and returned['ResponseMetadata'].get('HTTPStatusCode') == 200:
        return returned['ResponseMetadata'].get('RequestId')
    else:
        raise Exception(f'AWS S3 Copy object fail : {returned}')


def get_s3_contents_and_attributes(
        s3_conn_id: str,
        bucket_name: str,
        key: str,
        params: dict = {},
        region_name: str = "us-west-2"
):
    """

    :param region_name:
    :param params:
    :param s3_conn_id: (str) s3_conn_id: Airflow AWS credentials
    :param bucket_name: (str) bucket_name: AWS S3 bucket which the function will connect to
    :param key: (str) Path to the content which the function will access
    :return: (dict) content
    """

    if not key:
        raise Exception("Key must be informed to be able connecting to AWS S3")

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_obj = s3_hook.get_resource_type(
        "s3",
        region_name=region_name
    ).Object(
        bucket_name,
        key
    )
    response = s3_obj.get(**params)
    response_body = response.get("Body")
    return response_body.read()


def save_obj_to_s3(
        s3_conn_id: str,
        file: bytes,
        destination_key: str,
        destination_bucket: str,
        acl: str = "public-read"
):
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    s3_hook.load_bytes(
        bytes_data=file,
        key=destination_key,
        bucket_name=destination_bucket,
        replace=True,
        encrypt=False,
        acl_policy=acl
    )


def copy_s3_to_s3(
        s3_conn_id: str,
        source_bucket: str,
        destination_bucket: str,
        source_key: str,
        destination_key: str = None,
        request_payer: str = None,
        acl: str = "public-read"
):
    """
    Function to copy files from one S3 bucket to another.

    :param source_key:(str) Source file path
    :param destination_key:(str) Destination file path
    :param s3_conn_id:(str) Airflow connection id
    :param source_bucket:(str) Source S3 bucket name
    :param destination_bucket:(str) Destination S3 bucket name
    :param request_payer:(str) When None the S3 owner will pay, when <requester> the solicitor will pay
    :param acl:

    :return: None
    """

    if source_key and not destination_key:
        # If destination_key is not informed, build the same structure as the source_key
        destination_key = source_key.replace(source_bucket, destination_bucket)

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    return check_s3_copy_return(
        # This uses a boto3 S3 Client directly, so that we can pass the RequestPayer option.
        s3_hook.get_conn().copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={
                "Bucket": source_bucket,
                "Key": source_key,
                "VersionId": None
            },
            ACL=acl,
            RequestPayer=request_payer,
        )
    )


def key_not_existent(
        s3_conn_id: str,
        region_name: str,
        bucket_name: str,
        key: str,
):
    """
    Check on a S3 bucket if a object exist or not, if not returns the path, otherwise returns blank

    :param s3_conn_id:(str) Airflow connection id
    :param region_name:
    :param bucket_name:(str) S3 bucket name
    :param key:(str) File path
    :return:(bool) True if exist, False if not
    """
    try:
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_resource = s3_hook.get_resource_type("s3", region_name=region_name)
        s3_obj = s3_resource.Object(bucket_name, key)
        s3_obj.load()
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == "404":
            return key

    logging.info(f"Object {key} already exist at {bucket_name}")
    return ''


def publish_to_sns_topic(
        aws_conn_id: str,
        target_arn: str,
        message: str,
        attributes: dict = {}
):
    """
    Function to publish a message to a SNS
    :param target_arn:(str)
    :param aws_conn_id:(str)
    :param message:(str)
    :param attributes:(dict)
    :return:None
    """
    sns_hook = AwsSnsHook(aws_conn_id=aws_conn_id)

    check_s3_copy_return(
        sns_hook.publish_to_target(
            target_arn=target_arn,
            message=message,
            message_attributes=attributes,
        )
    )


def publish_to_sqs_queue(
        aws_conn_id: str,
        queue_name: str,
        messages: list,
        attributes: dict = {}
):
    """
    Function to publish a message to a SQS
    :param queue_name:(str) AWS SQS queue name
    :param aws_conn_id:(str) Airflow Connection ID
    :param messages:(list) Messages list to be sent
    :param attributes:(dict) not in use yet
    :return:None
    """

    sqs_hook = SQSHook(aws_conn_id=aws_conn_id)
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    returned = queue.send_messages(Entries=messages)


def get_queue(
        aws_conn_id: str,
        queue_name: str,
):
    # Todo move to utils
    """
    Function to connect to the right queue and return an instance of that
    :return: instance of a QUEUE
    """

    sqs_hook = SQSHook(aws_conn_id=aws_conn_id)
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    return queue
