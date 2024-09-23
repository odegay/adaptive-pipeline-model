import os
import base64
import json
import google.auth
from google.cloud import batch_v1
from google.api_core.exceptions import AlreadyExists, NotFound
from google.protobuf import duration_pb2
import time
import datetime
import logging
from adpipwfwconst import MSG_TYPE

BATCH_JOB_STATUS_SCHEDULED = 2
BATCH_JOB_STATUS_COMPLETED = 4

#Function triggers with the message type MSG_TYPE.GENERATE_NEW_MODEL = 5, recevied from the adaptive-pipeline-workflow-topic
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # Capture DEBUG, INFO, WARNING, ERROR, CRITICAL
if not root_logger.handlers:
    # Create console handler and set its log level to DEBUG
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # Create formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    # Add the handler to the root logger
    root_logger.addHandler(ch)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Capture DEBUG, INFO, WARNING, ERROR, CRITICAL

def validate_pubsub_message(event):
    """Validate the Pub/Sub message."""
    if 'data' in event:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        pubsub_message = json.loads(pubsub_message)
        logger.debug(f"Decoded Pub/Sub message: {pubsub_message}")
        logger.debug(f"Type of MSG_TYPE in message: {type(pubsub_message['status'])}")
        if 'status' in pubsub_message:
            if pubsub_message['status'] == MSG_TYPE.GENERATE_NEW_MODEL.value:
                return pubsub_message
            else:
                logger.debug(f"Skipping message with status: {pubsub_message['status']}")
                return None
        else:
            logger.error("Message does not contain status field. Message: {pubsub_message}")
            return None           
    else:
        logger.error("Message does not contain data field. Event: {event}")
        return None

def adaptive_pipeline_model_function(event, context):
    """Triggered by a message on a Pub/Sub topic and triggers a GCP Batch job."""
    if (validate_pubsub_message(event) is None):
        logger.debug("The Pub/Sub message didn't pass the validation. See the reason in the logs. Exiting.")
        return
        
    # Authenticate and initialize the Batch client
    credentials, project_id = google.auth.default()
    client = batch_v1.BatchServiceClient(credentials=credentials)

    # Use a consistent job name
    #job_name = f"projects/{project_id}/locations/us-central1/jobs/model-training-job"
    job_id = "model-training-job"
    parent = f"projects/{project_id}/locations/us-central1"

    # Create the max_run_duration using protobuf Duration
    max_run_duration = duration_pb2.Duration()
    max_run_duration.seconds = 3600  # Set the duration to 3600 seconds (1 hour)

    # Generate the batch job configuration as a Python dictionary
    batch_job_config_dict = {
        'task_groups': [{
            'task_spec': {
                'runnables': [{
                    'container': {
                        'image_uri': f"gcr.io/{project_id}/model-train-batch-image:latest",
                        'commands': []  # Add any necessary commands here
                    }
                }],
                'max_run_duration': max_run_duration  # Set the maximum run duration
            },
            'task_count': 1,
            'parallelism': 1
        }],
        'allocation_policy': {  # Updated allocation policy
            'instances': [{
                'policy': {}
            }]
        },
        'logs_policy': {
            'destination': 'CLOUD_LOGGING'  # Ensure logs are sent to Cloud Logging
        }
    }

    

    # Try deleting the previous job if it exists
    # try:
    #     client.delete_job(name=f"{parent}/jobs/{job_id}")
    #     logger.debug(f"Deleted previous job with job_id: {job_id}")
    # except NotFound:
    #     logger.debug(f"No existing job to delete: {job_id} not found.")
    # except Exception as e:
    #     logger.debug(f"Error deleting previous job: {str(e)}")

    # Convert the batch_job_config_dict to a JSON object
    #batch_job_config_json = json.dumps(batch_job_config_dict)
    job_config = batch_v1.Job(**batch_job_config_dict)    

    # Submit the Batch job
    try:
        job = client.create_job(
            parent=parent,
            job=job_config
        )
        job_name = job.name  # Get the full job resource name, which includes the job ID
        job_id = job_name.split('/')[-1]  # Extract just the job ID
        logger.debug(f"Batch job triggered successfully with job_id: {job_id}")
        
        # Poll the job status with dynamic retries and a time limit of 8 minutes
        max_duration = 8 * 60  # 8 minutes in seconds
        start_time = datetime.datetime.now()
        retry_count = 0
        polling_interval = 15  # initial polling interval in seconds
        max_retries = 33  # Arbitrary high number to prevent infinite retries, based on 8 minutes and 15s intervals

        while retry_count < max_retries:
            try:
                 # Check if we're nearing the time limit
                elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
                if elapsed_time >= max_duration:
                    logger.debug("Time limit reached, stopping the job status polling.")
                    break

                job_status = client.get_job(name=job_name)
                logger.debug(f"Job status: {job_status.status.state}")
            
                # Exit the loop if the job is found or completed
                if job_status.status.state == BATCH_JOB_STATUS_SCHEDULED or job_status.status.state == BATCH_JOB_STATUS_COMPLETED:
                    logger.debug(f"Job is schedulled or completed, exiting the loop. The status is: {job_status.status.state}")
                    break                
            except NotFound:
                logger.debug(f"Job not found yet, retrying ({retry_count + 1})...")
            except Exception as e:
                logger.debug(f"Error getting job status: {str(e)}")
            time.sleep(polling_interval)
            retry_count += 1        
        logger.debug(f"Exiting the job status polling loop after {retry_count} retries. The job status is: {job_status.status.state}")
    except AlreadyExists:
        logger.debug(f"Job {job_name} already exists and is still running.")
    except Exception as e:
        logger.debug(f"Failed to submit Batch job: {str(e)}")
        logger.debug(f"Job configuration: {batch_job_config_dict}")
