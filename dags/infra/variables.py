"""
# Environment shared variables read from airflow variable config, provided by infrastracture
# https://airflow.apache.org/docs/stable/concepts.html?highlight=variable#storing-variables-in-environment-variables
# Variables set using Environment Variables would not appear in the Airflow UI but you will be able to use it in your DAG file
"""
from airflow.models import Variable

# DB config
DB_DATABASE = Variable.get("db_database", "africa")
DB_HOSTNAME = Variable.get("db_hostname", "db-writer")

AWS_DEFAULT_REGION = Variable.get("aws_default_region", "us-west-2")
SECRET_AWS_NAME = Variable.get("secret_aws_name", "indexing-aws-creds-prod")
DB_DUMP_S3_ROLE = Variable.get(
    "db_dump_s3_role", "deafrica-prod-af-eks-db-dump-to-s3"
)
SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")
DB_DUMP_S3_BUCKET = Variable.get(
    "db_dump_s3_bucketname", "deafrica-dev-odc-db-dump"
)

SECRET_EXPLORER_WRITER_NAME = Variable.get("explorer_secret", "explorer-db")
SECRET_OWS_WRITER_NAME = Variable.get("ows_secret", "ows-db")

# Sentinel-2 US connection, this is based on svc-deafrica-prod-eks-s2-data-transfer
# user which is in deafrica account
US_CONN_ID = "deafrica-prod-eks-s2-data-transfer"
# Sentinel-2 PDS connection, this is based on svc-deafrica-sentinel-2-bucket-write
AFRICA_CONN_ID = "deafrica-sentinel-2-bucket-write"
