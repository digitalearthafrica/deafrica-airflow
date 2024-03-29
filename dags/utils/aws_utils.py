"""
AWS wrap using Airflow hooks
"""
import logging

import boto3
from airflow import AirflowException
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.hooks.S3_hook import S3Hook
from botocore.exceptions import ClientError


class S3:
    """
    Class for AWS S3 bucket
    """

    def __init__(self, conn_id):
        self.s3_hook = S3Hook(aws_conn_id=conn_id)

    def get_bucket_client(
        self,
        endpoint_url: str = None,
        region: str = "af-south-1",
    ):
        """
        function to return a bucket client
        :param endpoint_url:(str) Not in use for now
        :param region: AWS Region
        :return:
        """
        cred = self.s3_hook.get_session().get_credentials()

        if not cred:
            raise AirflowException(
                "Credential not found, please ensure you have the right one configured "
                "on Airflow connections"
            )

        session = boto3.session.Session()

        return session.client(
            "s3",
            aws_access_key_id=cred.access_key,
            aws_secret_access_key=cred.secret_key,
            region_name=region,
            # endpoint_url=endpoint_url
        )

    def check_s3_copy_return(self, returned: dict):
        """
        Function to check if the copy is valid
        :param returned:
        :return:
        """
        if (
            returned.get("ResponseMetadata")
            and returned["ResponseMetadata"].get("HTTPStatusCode") == 200
        ):
            return returned["ResponseMetadata"].get("RequestId")
        else:
            raise Exception(f"AWS S3 Copy object fail : {returned}")

    def get_s3_contents_and_attributes(
        self,
        bucket_name: str,
        key: str,
        params: dict = {},
        region: str = "af-south-1",
    ):
        """
        Retrieve S3 object and its attributes

        :param s3_conn_id: (str) s3_conn_id: Airflow AWS credentials
        :param bucket_name: (str) bucket_name: AWS S3 bucket which the function will connect to
        :param key: (str) Path to the content which the function will access
        :param params:
        :param region: Defaulted to af-south-1
        :return: (dict) content
        """

        if not key:
            raise Exception("Key must be informed to be able connecting to AWS S3")

        s3_obj = self.s3_hook.get_resource_type("s3", region_name=region).Object(
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
        acl: str = "bucket-owner-full-control",
    ):
        """
        Function to save obj strem into a S3 bucket
        :param file: (bytes) Stream file
        :param destination_key: (str) destination key
        :param destination_bucket: (str) destination bucket name
        :param acl: (str) permissions
        :return: None
        """
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
        source_bucket: str,
        destination_bucket: str,
        source_key: str,
        destination_key: str = None,
        request_payer: str = None,
        acl: str = "bucket-owner-full-control",
    ):
        """
        Function to copy files from one S3 bucket to another. This function is limited to buckets in the same region.

        :param source_key:(str) Source file path
        :param destination_key:(str) Destination file path
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

        # This uses a boto3 S3 Client directly, so that we can pass the RequestPayer option.
        returned = self.s3_hook.get_conn().copy_object(
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
        source_bucket: str,
        destination_bucket: str,
        source_bucket_region: str,
        destination_bucket_region: str,
        source_key: str,
        endpoint_url: str = None,
        destination_key: str = None,
        request_payer: str = "requester",
        acl: str = "bucket-owner-full-control",
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
        :param endpoint_url:(str)
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

        s3_obj = self.get_object(
            bucket_name=source_bucket,
            key=source_key,
            region=source_bucket_region,
            request_payer=request_payer,
        )

        streaming_body = s3_obj["Body"]

        destination_bucket_client = self.get_bucket_client(
            region=destination_bucket_region, endpoint_url=endpoint_url
        )

        destination_bucket_client.upload_fileobj(
            streaming_body, destination_bucket, destination_key, ExtraArgs=dict(ACL=acl)
        )

    def check_for_key(
            self,
            bucket_name: str,
            region: str,
            key: str,
    ):
        """
        Check on a S3 bucket if a object exist or not,
        if not returns False, otherwise returns the path

        :param bucket_name:(str) S3 bucket name
        :param region: (str) destination region
        :param key:(str) File path
        :return:(bool) True if exist, False if not
        """

        if not bucket_name:
            ValueError('Bucket name is required')

        client = self.get_bucket_client(region=region)
        try:
            client.head_object(
                Bucket=bucket_name,
                Key=key,
            )
            return True
        except ClientError as error:
            logging.info(error.response["Error"]["Message"])
            return False
        except Exception as e:
            logging.error(e)
            raise e

    def get_object(
        self,
        bucket_name: str,
        key: str,
        region: str,
        endpoint_url: str = None,
        request_payer: str = "",
    ):
        """
        Function to retrieve an object from an AWS S# bucket according to the key sent
        :param bucket_name:(str) bucket name
        :param key: (str) path to the file
        :param region: (str) AWS region
        :param endpoint_url:
        :param request_payer:(str)
        :return:
        """
        # logging.info(
        #     f"get_object {bucket_name} {key} {region} {endpoint_url} {request_payer}"
        # )

        bucket_client = self.get_bucket_client(region=region, endpoint_url=endpoint_url)
        return bucket_client.get_object(
            Bucket=bucket_name, Key=key, RequestPayer=request_payer
        )

    def put_object(
        self,
        bucket_name: str,
        key: str,
        region: str,
        endpoint_url: str = None,
        body: str = "",
        content_type: str = "binary/octet-stream",
        acl: str = "bucket-owner-full-control",
    ):
        """
        Function to upload an object to an AWS S3 bucket
        :param bucket_name: (str) bucket name
        :param key: (str) path to the file
        :param region: (str) AWS region
        :param endpoint_url:(str)
        :param body: (str) body
        :param content_type: (str) Set the content type of the file being uploaded
        :param acl: (str) permissions
        :return:
        """
        bucket_client = self.get_bucket_client(region=region, endpoint_url=endpoint_url)

        return bucket_client.put_object(Bucket=bucket_name, Key=key, Body=body, ACL=acl, ContentType=content_type)

    def list_objects(
        self,
        bucket_name: str,
        region: str,
        endpoint_url: str = None,
        prefix: str = None,
        request_payer: str = None,
        continuation_token: str = None,
    ):
        """
        List objects into an AWS S3 Bucket
        :param bucket_name: (str)
        :param region: (str)
        :param endpoint_url:(str)
        :param prefix: (str)
        :param request_payer: (str)
        :param continuation_token: (str)
        :return: (dict)
        """
        bucket_client = self.get_bucket_client(region=region, endpoint_url=endpoint_url)

        kwargs = {"Bucket": bucket_name}

        if request_payer:
            kwargs.update({"RequestPayer": request_payer})

        if prefix:
            kwargs.update({"Prefix": prefix})

        if continuation_token:
            kwargs.update({"ContinuationToken": continuation_token})

        return bucket_client.list_objects_v2(**kwargs)


class SNS:
    """
    Class to wrap AWS SNS topics' calls
    """

    def __init__(self, conn_id):
        self.sns_hook = AwsSnsHook(aws_conn_id=conn_id)

    def check_publish_return_return(self, returned: dict):
        """
        Check if the return is valid
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
