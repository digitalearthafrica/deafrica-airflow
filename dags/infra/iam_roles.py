"""
# iam roles supplied by terraform
"""
from airflow.models import Variable

DB_DUMP_S3_ROLE = Variable.get("db_dump_s3_role", "deafrica-prod-af-eks-db-dump-to-s3")
