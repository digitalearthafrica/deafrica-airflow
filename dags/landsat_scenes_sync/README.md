# DEAfrica Landsat Scenes Sync
Dags to identify USGS Collection 2 Landsat scenes either in bulk or per-day, push them onto a queue, and then
to copy the data to Cape Town and upgrade the STAC metadata and create a notification.

## Development

### DAGs
- landsat-scenes-indexing
  * This Dag will be started by the scheduler to pull the messages from a second SQS queue, which is
    filled automatically by the SNS, and will index into the Datacube.

- landsat_scenes_gap_report
  * This Dag will start a Python process which will download tar.gz files for each satellite from USGS
    file server, and filter by execution date, bbox, and pathrow resulting in a list of valid ids. After that,
    the process will compare with Landsat bucket in PDS and save a file with the differences.



## Airflow Requirements

Install pip modules

```bash
    pip install --user --upgrade pip && pip install --user stactools[all] pystac rasterio
```

### Logic and utils
Under the folder utils, there are the python logic and additional files.

- scenes_sync
    * Logic to download bulk files from USGS file server or access the USGS API and send the valid stacs to Africa's queue

- scenes_sync_process
    * Logic to pull messages from the SQS queue, validate the message, process one by one adding missing items,
      changing links to point to our S3 bucket and transforming from stac 0.7 to 1.0.
      After that sending to an SNS topic.

- aws_utils
    * Additional Python logic for AWS connections.

- sync_utils
    * Additional Python logic to treat requests, read files and etc.
