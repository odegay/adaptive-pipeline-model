import os
import base64
import json
import google.auth
from google.cloud import batch_v1
from google.api_core.exceptions import AlreadyExists, NotFound
import time
import logging
from adpipwfwconst import MSG_TYPE

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
    job_name = f"projects/{project_id}/locations/us-central1/jobs/model-training-job"

    # Generate the batch job configuration as a Python dictionary
    batch_job_config_dict = {
        'name': job_name,
        'taskGroups': [{
            'taskSpec': {
                'runnables': [{
                    'container': {
                        'imageUri': f"gcr.io/{project_id}/model-train-batch-image:latest",
                        'commands': []  # Add any necessary commands here
                    }
                }],
                'maxRunDuration': "3600s"
            },
            'taskCount': 1,
            'parallelism': 1
        }],
        'allocationPolicy': {
            'instances': [{
                'policy': {}
            }]
        }
    }

    # Try deleting the previous job if it exists
    try:
        client.delete_job(name=job_name)
        logger.debug(f"Deleted previous job with name: {job_name}")
    except NotFound:
        logger.debug(f"No existing job to delete: {job_name} not found.")
    except Exception as e:
        logger.debug(f"Error deleting previous job: {str(e)}")

    # Convert the batch_job_config_dict to a Job object
    job_config = batch_v1.Job(**batch_job_config_dict)

    # Submit the Batch job
    try:
        job = client.create_job(
            parent=f"projects/{project_id}/locations/us-central1",
            job=job_config  # Pass the Job object, not a dictionary
        )
        logger.debug("Batch job triggered successfully.")
        
        # Poll the job status
        job_status = client.get_job(name=job_name)
        logger.debug(f"Job status: {job_status.status.state}")
        
        # Optionally, wait for the job to start running (optional step)
        while job_status.status.state == batch_v1.JobStatus.State.PENDING:
            logger.debug("Waiting for job to start running...")
            time.sleep(5)
            job_status = client.get_job(name=job_name)
        
        logger.debug(f"Job is now in state: {job_status.status.state}")

    except AlreadyExists:
        logger.debug(f"Job {job_name} already exists and is still running.")
    except Exception as e:
        logger.debug(f"Failed to submit Batch job: {str(e)}")
        logger.debug(f"Job configuration: {batch_job_config_dict}")
