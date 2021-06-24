# Sentinel 2

## DAGs
- s2_geomedian_indexing
  *

- sentinel-2-backlog-indexing
  *

- sentinel-2_indexing
  * This Dag will start a Python process which will run ```Datacube``` command ```sqs-to-dc```
    to index all messages in the queue ```deafrica-prod-af-eks-sentinel-2-indexing``` to the database.

- sentinel_2_gap_report
  * This Dag will start a Python process which will compare files between the Africa S3 bucket and Element-84 s3 bucket.
    This process will be done comparing inventory buckets and generating a report as a result.

### utils

- aws_utils
    * Additional Python logic for AWS connections.
