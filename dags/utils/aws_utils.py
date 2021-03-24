import logging
from copy import deepcopy
from urllib.parse import urlparse

import boto3
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.hooks.S3_hook import S3Hook


class S3:
    def __init__(self, conn_id):
        self.s3_hook = S3Hook(aws_conn_id=conn_id)
        self.aws_hook = AwsHook(aws_conn_id=conn_id)

    def get_bucket(self, bucket_name: str):
        return self.s3_hook.get_bucket(bucket_name=bucket_name)

    def check_s3_copy_return(self, returned: dict):

        if (
            returned.get("ResponseMetadata")
            and returned["ResponseMetadata"].get("HTTPStatusCode") == 200
        ):
            return returned["ResponseMetadata"].get("RequestId")
        else:
            raise Exception(f"AWS S3 Copy object fail : {returned}")

    def s3_urlparse(self, url):
        """
        Split S3 URL into bucket, key, filename
        """
        _url = deepcopy(url)
        if url[0:5] == "https":

            parts = urlparse(url)
            bucket = parts.netloc.split(".")[0]
            _url = f"s3://{bucket}{parts.path}"

        if _url[0:5] != "s3://":
            raise Exception(f"Invalid S3 url {_url}")

        url_obj = _url.replace("s3://", "").split("/")

        # remove empty items
        url_obj = list(filter(lambda x: x, url_obj))
        return {"bucket": url_obj[0], "key": "/".join(url_obj[1:])}

    def get_s3_contents_and_attributes(
        self,
        bucket_name: str,
        key: str,
        params: dict = {},
        region_name: str = "us-west-2",
    ):
        """
        Retrieve S3 object and its attributes

        :param s3_conn_id: (str) s3_conn_id: Airflow AWS credentials
        :param bucket_name: (str) bucket_name: AWS S3 bucket which the function will connect to
        :param key: (str) Path to the content which the function will access
        :param params:
        :param region_name: Defaulted to us-west-2
        :return: (dict) content
        """

        if not key:
            raise Exception("Key must be informed to be able connecting to AWS S3")

        s3_obj = self.s3_hook.get_resource_type("s3", region_name=region_name).Object(
            bucket_name, key
        )

        response = s3_obj.get(**params)
        response_body = response.get("Body")
        return response_body.read()

    def save_obj_to_s3(
        self,
        file: bytes,
        destination_key: str,
        destination_bucket: str,
        acl: str = "public-read",
    ):
        self.s3_hook.load_bytes(
            bytes_data=file,
            key=destination_key,
            bucket_name=destination_bucket,
            replace=True,
            encrypt=False,
            acl_policy=acl,
        )

    def copy_s3_to_s3_same_region(
        self,
        s3_conn_id: str,
        source_bucket: str,
        destination_bucket: str,
        source_key: str,
        destination_key: str = None,
        request_payer: str = None,
        acl: str = "public-read",
    ):
        """
        Function to copy files from one S3 bucket to another. This function is limited to buckets in the same region.

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
            destination_key = source_key

        logging.info(
            f"copy_s3_to_s3 source: {source_key} destination: {destination_key}"
        )

        s3_hook = S3Hook(aws_conn_id=s3_conn_id)

        # This uses a boto3 S3 Client directly, so that we can pass the RequestPayer option.
        returned = s3_hook.get_conn().copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key, "VersionId": None},
            ACL=acl,
            RequestPayer=request_payer,
        )

        logging.info(f"RETURNED - {returned}")

        return self.check_s3_copy_return(returned)

    def copy_s3_to_s3_cross_region(
        self,
        conn_id: str,
        source_bucket: str,
        destination_bucket: str,
        source_bucket_region: str,
        destination_bucket_region: str,
        source_key: str,
        destination_key: str = None,
        request_payer: str = "requester",
        acl: str = "public-read",
    ):
        """
        Function to copy files from a S3 to another. Slower process however allows cross-region copies.
        In case of the copy is in the same region use copy_s3_to_s3_same_region

        :param conn_id:(str) Airflow connection id
        :param source_key:(str) Source file path
        :param destination_key:(str) Destination file path
        :param destination_bucket_region: Region which the file goes to
        :param source_bucket_region: Region which the file comes from
        :param source_bucket:(str) Source S3 source_bucket_client name
        :param destination_bucket:(str) Destination S3 source_bucket_client name
        :param request_payer:(str) When None the S3 owner will pay, when <requester> the solicitor will pay
        :param acl:

        :return: None
        """

        if source_key and not destination_key:
            # If destination_key is not informed, build the same structure as the source_key
            destination_key = source_key

        logging.info(
            f"copy_s3_to_s3_cross_region source: {source_key} destination: {destination_key}"
        )

        cred = self.aws_hook.get_session().get_credentials()

        source_bucket_client = boto3.client(
            "s3",
            aws_access_key_id=cred.access_key,
            aws_secret_access_key=cred.secret_key,
            region_name=source_bucket_region,
        )

        s3_obj = source_bucket_client.get_object(
            Bucket=source_bucket, Key=source_key, RequestPayer=request_payer
        )

        streaming_body = s3_obj["Body"]
        destination_bucket_client = boto3.client(
            "s3",
            aws_access_key_id=cred.access_key,
            aws_secret_access_key=cred.secret_key,
            region_name=destination_bucket_region,
        )

        destination_bucket_client.upload_fileobj(
            streaming_body, destination_bucket, destination_key, ExtraArgs=dict(ACL=acl)
        )

    def key_not_existent(
        self,
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

        exist = self.s3_hook.check_for_key(key, bucket_name=bucket_name)
        return key if not exist else ""

    # def list_keys(self):
    #     manifest = self.latest_manifest()
    #     for obj in manifest["files"]:
    #         print(obj["key"])
    #         gzip_obj = self.s3.get_object(Bucket=self.bucket, Key=obj["key"])
    #         buffer = gzip.open(gzip_obj["Body"], mode="rt")
    #         reader = csv.reader(buffer)
    #         for row in reader:
    #             yield row


class SQS:
    def __init__(self, conn_id):
        sqs_hook_conn = SQSHook(aws_conn_id=conn_id)
        self.sqs_hook = sqs_hook_conn.get_resource_type("sqs")

    def publish_to_sqs_queue(
        self, queue_name: str, messages: list, attributes: dict = {}
    ):
        """
        Function to publish a message to a SQS
        :param queue_name:(str) AWS SQS queue name
        :param aws_conn_id:(str) Airflow Connection ID
        :param messages:(list) Messages list to be sent
        :param attributes:(dict) not in use yet
        :return:None
        """
        queue = self.sqs_hook.get_queue_by_name(QueueName=queue_name)
        returned = queue.send_messages(Entries=messages)

    def get_queue(self, queue_name: str):
        """
        Function to connect to the right queue and return an instance of that
        :return: instance of a QUEUE
        """

        return self.sqs_hook.get_queue_by_name(QueueName=queue_name)


class SNS:
    def __init__(self, conn_id):
        self.sns_hook = AwsSnsHook(aws_conn_id=conn_id)

    def check_publish_return_return(self, returned: dict):
        """

        :param returned:
        :return:
        """
        if (
            returned.get("ResponseMetadata")
            and returned["ResponseMetadata"].get("HTTPStatusCode") == 200
        ):
            return returned["ResponseMetadata"].get("RequestId")
        else:
            raise Exception(f"AWS SNS publishing process fail : {returned}")

    def publish_to_sns_topic(
        self, target_arn: str, message: str, attributes: dict = {}
    ):
        """
        Function to publish a message to a SNS
        :param target_arn:(str)
        :param aws_conn_id:(str)
        :param message:(str)
        :param attributes:(dict)
        :return:None
        """

        self.check_publish_return_return(
            self.sns_hook.publish_to_target(
                target_arn=target_arn,
                message=message,
                message_attributes=attributes,
            )
        )
