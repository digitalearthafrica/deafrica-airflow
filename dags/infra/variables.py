"""
# Variables provided from infra to dags
# audit: 02/07/2021
"""
from airflow.models import Variable

REGION = Variable.get("region", "af-south-1")
INDEXING_FROM_SQS_USER_SECRET = Variable.get("indexing_from_sqs_user_secret", "indexing-user-creds-dev")
ODC_ADMIN_USER_SECRET = Variable.get("odc_admin_user_secret", "odc-admin-user-creds")

###### DB config
DB_DATABASE = Variable.get("db_database", "odc")
DB_WRITER = Variable.get("db_writer", "db-writer")
DB_READER = Variable.get("db_reader", "db-reader")
DB_PORT = Variable.get("db_port", "5432")

SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")

SECRET_ODC_ADMIN_NAME = Variable.get("db_odc_admin_secret", "odc-admin")
SECRET_OWS_ADMIN_NAME = Variable.get("db_ows_admin_secret", "ows-admin")
SECRET_EXPLORER_ADMIN_NAME = Variable.get("db_explorer_admin_secret", "explorer-admin")

SECRET_ODC_WRITER_NAME = Variable.get("db_odc_writer_secret", "odc-writer")
SECRET_OWS_WRITER_NAME = Variable.get("db_ows_writer_secret", "ows-writer")
SECRET_EXPLORER_WRITER_NAME = Variable.get("db_explorer_writer_secret", "explorer-writer")

SECRET_ODC_READER_NAME = Variable.get("db_odc_reader_secret", "odc-reader")
SECRET_OWS_READER_NAME = Variable.get("db_ows_reader_secret", "ows-reader")
