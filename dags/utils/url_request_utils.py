import json

import requests
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
    json_body = response_body.read()
    return json.loads(json_body)


def copy_s3_to_s3(
    s3_conn_id: str,
    source_bucket: str,
    destination_bucket: str,
    source_key: str,
    request_payer: str = None,
    destination_key: str = None,
):
    """
    Function to copy files from one S3 bucket to another.

    :param request_payer: (str) When None the S3 owner will pay, when <requester> the solicitor will pay
    :param source_key: (str) Source file path
    :param destination_key: (str) Destination file path
    :param s3_conn_id:(str) Airflow connection id
    :param source_bucket:(str) Source S3 bucket name
    :param destination_bucket:(str) Destination S3 bucket name
    :return: None
    """

    if not source_key:
        raise Exception(
            "Source key must be informed to be able connecting to AWS S3"
        )
    elif source_key and not destination_key:
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
            ACL="public-read",
            RequestPayer=request_payer,
        )
    )


def publish_to_sns_topic(
        aws_conn_id: str,
        target_arn: str,
        message: str,
        attributes: dict = {}
):
    """

    :param target_arn:
    :param aws_conn_id:
    :param message:
    :param attributes:
    :return:
    """
    sns_hook = AwsSnsHook(aws_conn_id=aws_conn_id)

    response = sns_hook.publish_to_target(
        target_arn=target_arn,
        message=message,
        message_attributes=attributes,
    )
