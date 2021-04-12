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
SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")

SECRET_EXPLORER_WRITER_NAME = Variable.get(
    "db_explorer_writer_secret", "explorer-writer"
)
SECRET_OWS_WRITER_NAME = Variable.get("db_ows_writer_secret", "ows-writer")
SECRET_ODC_WRITER_NAME = Variable.get("db_odc_writer_secret", "odc-writer")
