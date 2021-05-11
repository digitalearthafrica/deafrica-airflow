"""
# Variables provided from infra to dags
# audit: 11/05/2021
"""
from airflow.models import Variable

DB_DUMP_S3_ROLE = Variable.get(
    "db_dump_s3_role", "deafrica-prod-af-eks-db-dump-to-s3"
)  # qa
