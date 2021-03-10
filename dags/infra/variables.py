"""
# Environment shared variables read from airflow variable config, provided by infrastracture
# https://airflow.apache.org/docs/stable/concepts.html?highlight=variable#storing-variables-in-environment-variables
# Variables set using Environment Variables would not appear in the Airflow UI but you will be able to use it in your DAG file
"""
from airflow.models import Variable

# DB config
DB_DATABASE = Variable.get("db_database", "odc")
DB_HOSTNAME = Variable.get("db_hostname", "db-writer")

AWS_DEFAULT_REGION = Variable.get("aws_default_region", "af-south-1")
SECRET_AWS_NAME = Variable.get("secret_aws_name", "processing-aws-creds-prod")
DB_DUMP_S3_ROLE = Variable.get("db_dump_s3_role", "deafrica-prod-af-eks-db-dump-to-s3")
SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")
DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-dev-odc-db-dump")

SECRET_EXPLORER_WRITER_NAME = Variable.get("explorer_secret", "explorer-writer")
SECRET_OWS_WRITER_NAME = Variable.get("ows_secret", "ows-writer")
