"""
# Variables provided from infra to dags
# audit: 11/05/2021
"""
from airflow.models import Variable

LANDSAT_SYNC_S3_C2_FOLDER_NAME = Variable.get("landsat_c2_folder_name", "collection02")

LANDSAT_SYNC_S3_STATUS_REPORT_FOLDER_NAME = Variable.get(
    "LANDSAT_SYNC_S3_STATUS_REPORT_FOLDER_NAME", "status-report"
)

SENTINEL_2_S3_COGS_FOLDER_NAME = Variable.get(
    "deafrica_sentinel_2_cogs_folder", "sentinel-s2-l2a-cogs"
)

AWS_DEFAULT_REGION = Variable.get("aws_default_region", "af-south-1")
SECRET_AWS_NAME = Variable.get("secret_aws_name", "processing-aws-creds-dev")

###### DB config
DB_DATABASE = Variable.get("db_database", "odc")
DB_HOSTNAME = Variable.get("db_hostname", "db-writer")
DB_PORT = Variable.get("db_port", "5432")

SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")  # qa

SECRET_ODC_ADMIN_NAME = Variable.get("db_odc_admin_secret", "odc-admin")
SECRET_OWS_ADMIN_NAME = Variable.get("db_ows_admin_secret", "ows-admin")
SECRET_EXPLORER_ADMIN_NAME = Variable.get("db_explorer_admin_secret", "explorer-admin")

SECRET_ODC_WRITER_NAME = Variable.get("db_odc_writer_secret", "odc-writer")
SECRET_OWS_WRITER_NAME = Variable.get("db_ows_writer_secret", "ows-writer")
SECRET_EXPLORER_WRITER_NAME = Variable.get("db_explorer_writer_secret", "explorer-writer")

SECRET_ODC_READER_NAME = Variable.get("db_odc_reader_secret", "odc-reader")
SECRET_OWS_READER_NAME = Variable.get("db_ows_reader_secret", "ows-reader")
#####