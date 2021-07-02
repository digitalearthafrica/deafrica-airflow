"""
# Task Pools
"""
from airflow.models import Variable

# Task Pools
WAGL_TASK_POOL = Variable.get("wagl_task_pool", "wagl_processing_pool")
DEA_NEWDATA_PROCESSING_POOL = Variable.get(
    "newdata_indexing_pool", "NewDeaData_indexing_pool"
)
C3_INDEXING_POOL = Variable.get("c3_indexing_pool", "c3_indexing_pool")
NBART_INDEXING_POOL = Variable.get("nbart_indexing_pool", "nbart_indexing_pool")
