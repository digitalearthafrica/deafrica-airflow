"""
# Migrate(copy) data between S3 buckets

DAG to periodically check a SQS and copy new data to Cape Town
The SQS is subscribed to the following SNS topic:
arn:aws:sns:us-west-2:482759440949:cirrus-dev-publish
"""
import json
import boto3
import re

from shapely.geometry import shape
from shapely.ops import transform
from pyproj  import CRS, Transformer, Proj

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.operators import TriggerDagRunOperator
from airflow.hooks.S3_hook import S3Hook

default_args = {
    'owner': 'Airflow',
    "start_date": datetime(2020, 6, 12),
    'email': ['toktam.ebadi@ga.gov.au'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    "aws_conn_id": "deafrica_data_dev_migration",
    "dest_bucket_name": "africa-migration-test",
    "src_bucket_name": "sentinel-cogs",
    "schedule_interval": "@daily",
    "crs_black_list": "[32601, 32701, 32660, 32760]",
    "sqs_queue": ("https://sqs.us-west-2.amazonaws.com/565417506782/"
                  "deafrica-prod-eks-sentinel-2-data-transfer")
}

def extract_src_key(src_url):
    """
    Extract object key from specified url path e.g.
    https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/
    2020/S2A_38PKS_20200609_0_L2A/B11.tif
    :param src_url: Full http path of the object
    :return: Object path relative to the base bucket
    """

    matches = (re.finditer("/", src_url))
    matches_positions = [match.start() for match in matches]
    start = matches_positions[2] + 1
    return src_url[start:]

def load_stac(s3_hook, stac_json_path):
    """
    load the STAC file associated with each scene
    :param s3_hook: Hook to interact with S3
    :param stac_json_path: Path to the STAC file on S3
    :return: Dictionary of the STAC file
    """

    content_object = s3_hook.get_key(key=extract_src_key(stac_json_path),
                     bucket_name=default_args['src_bucket_name'])
    content = content_object.get()['Body'].read().decode('utf-8')
    data = json.loads(content)
    return data

def africa_extent():
    """
    Load Africa boundaries
    :return: Shape object of the boundaries
    """

    boundaries = json.load(open("data/africa-extent.json"))
    return shape(boundaries["features"][0]["geometry"])

def scene_stac_utm(stac):
    """
    Extract the UTM zone from the STAC file
    :return: UTM zone
    """

    return stac['properties']['proj:epsg']

def reproject_scene_geom_to_epsg_4326(stac, scene_geometry):
    """
    Reproject CRS of the scene to WGS84 (same as the Africa extent)
    :param stac: STAC file
    :param scene_geometry: Scene geometry
    :return: Scene geometry reprojected to epsg:4326
    """

    src_epsg = scene_stac_utm(stac)
    src_gcs = "epsg:" + str(src_epsg)

    e_target = CRS('epsg:4326')
    e_source = CRS(src_gcs)
    project = Transformer.from_proj(Proj(e_source), Proj(e_target))
    return transform(project.transform, scene_geometry) 

def is_in_africa(s3_hook, scene_geometry, africa_footprint, stac_file):
    """
    Check whether a scene falls within Africa boundary
    :param s3_hook: hook to interact with S3
    :param scene_geometry: Shape object of the scene Geometry
    :param africa_footprint: A dictionary containing Africa boundary
    :param stac_file: path to the STAC file on S3
    :return: a boolean indicating whether a scene falls within Africa boundary
    """

    stac = load_stac(s3_hook, stac_file)
    src_epsg = scene_stac_utm(stac)
    scene_epsg_4326 = reproject_scene_geom_to_epsg_4326(stac, scene_geometry)
    # Exclude scenes on the anti-meridian
    merdian_180 = src_epsg not in list(default_args["crs_black_list"])
    return  scene_geometry.intersects(africa_footprint) & merdian_180

def copy_s3_objects(ti, **kwargs):
    """
    Copy objects from a s3 bucket to another s3 bucket.
    :param ti: Task instance
    """

    s3_hook = S3Hook(aws_conn_id=dag.default_args['aws_conn_id'])
    messages = ti.xcom_pull(key='Messages', task_ids='test_trigger_dagrun')
    # Load Africa footprint
    africa_footprint = africa_extent()

    # if messages:
    for rec in messages:
        body = json.loads(rec)
        message = json.loads(body['Message'])
        scene_geometry = shape(message["geometry"])

        # Extract URL of the json file
        urls = [message["links"][0]["href"]]
        is_in_africa_flag = is_in_africa(s3_hook, scene_geometry,
                                                    africa_footprint, urls[0])
        if is_in_africa_flag:
            
            # Add URL of .tif files
            urls.extend([v["href"] for k, v in message["assets"].items() if "geotiff" in v['type']])
            for src_url in urls:
                src_key = extract_src_key(src_url)
                s3_hook.copy_object(source_bucket_key=src_key,
                                    dest_bucket_key=src_key,
                                    source_bucket_name=default_args['src_bucket_name'],
                                    dest_bucket_name=default_args['dest_bucket_name'])
                print("Copied scene:", src_key)

def get_queue():
    """
    Return the SQS queue object
    """
    sqs_hook = SQSHook(aws_conn_id=dag.default_args['aws_conn_id'])
    queue_url = default_args['sqs_queue']
    queue_name = queue_url[queue_url.rindex("/") + 1:]
    sqs = sqs_hook.get_resource_type('sqs')   
    return sqs.get_queue_by_name(QueueName=queue_name)   
    
def trigger_sensor(ti, **kwargs):
    """
    Function to fork tasks
    If there are messages in the queue, it pushes them to task index object, and calls the copy function
    otherwise it calls a task that terminates the DAG run
    :param ti: Task instance
    :return: String id of the downstream task
    """

    queue = get_queue()   
    print("Queue size:", int(queue.attributes.get("ApproximateNumberOfMessages")))
    if int(queue.attributes.get("ApproximateNumberOfMessages")) > 0 :    
        max_num_polls = 100                      
        msg_list = [queue.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=10) for i in range(max_num_polls)]
        import itertools
        msg_list  = list(itertools.chain(*msg_list))
        messages = [] 
        for msg in msg_list:
            messages.append(msg.body) 
            msg.delete()            
        ti.xcom_push(key="Messages", value=messages)     
        print("Count of read messages:", len(messages))    
        return "copy_scenes"
    else:
         return "end"
         
def end_dag():
    print("Message queue is empty, terminating DAG")

with DAG('sentinel-2_data_transfer', default_args=default_args,
         tags=["Sentinel-2", "transfer"], catchup=False) as dag:

    BRANCH_OPT = BranchPythonOperator(
        task_id='test_trigger_dagrun',        
        python_callable=trigger_sensor,
        provide_context=True)

    COPY_OBJECTS = PythonOperator(
        task_id='copy_scenes',
        provide_context=True,
        python_callable=copy_s3_objects
    )

    END_DAG = PythonOperator(
        task_id='end',
        python_callable=end_dag
    )
    
    BRANCH_OPT >> [COPY_OBJECTS, END_DAG]
    