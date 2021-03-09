# DEAfrica Landsat Scenes Sync
Dags to read, either in bulk or daily, stacs released by USGS and migrate to Africa's servers

## Development

### DAGs
- landsat-scenes-bulk
  * This Dag will start a Python process which will download GZ files, which are split per landsat and collection, from USGS 
    file server, send the stac, in bulk, to our SQS queue in Africa. 
    
- landsat-scenes-daily
  * This Dag will start a Python process which will access the USGS API, accordingly to informed dates, and 
    send the stac to our SQS queue in Africa.
        
- landsat-scenes-process
  * This Dag will be started by the scheduler to pull messages from the SQS queue, validate the message, process 
    the message adding missing items, changing links to point to our S3 bucket and transforming from stac 0.7 to 1.0. 
    After that sending to an SNS topic.
  
- landsat-scenes-indexing
  * This Dag will be started by the scheduler to pull the messages from a second SQS queue, which is 
    filled automatically by the SNS, and will process by Datacube.  

## Airflow Requirements

Install pip modules

```bash
    pip install --user stactools[all] && pip install --user pystac && pip install --user    
```

### Logic and utils
Under the folder utils, there are the python logic and additional files.

- scenes_sync
    * Logic to download bulk files from USGS file server or access the USGS API and send the valid stacs to Africa's queue

- scenes_sync_process
    * Logic to pull messages from the SQS queue, validate the message, process one by one adding missing items, 
      changing links to point to our S3 bucket and transforming from stac 0.7 to 1.0.
      After that sending to an SNS topic.

- url_request_utils
    * Additional Python logic to treat requests and AWS connections.