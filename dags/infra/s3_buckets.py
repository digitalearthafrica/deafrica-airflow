"""
# s3 buckets
"""
from airflow.models import Variable


DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-dev-odc-db-dump")
