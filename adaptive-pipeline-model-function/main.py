import os
import google.auth
from google.cloud import batch_v1
from google.api_core.exceptions import AlreadyExists, NotFound
import time

def adaptive_pipeline_model_function(event, context):
    """Triggered by a message on a Pub/Sub topic and triggers a GCP Batch job."""
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
        print(f"Deleted previous job with name: {job_name}")
    except NotFound:
        print(f"No existing job to delete: {job_name} not found.")
    except Exception as e:
        print(f"Error deleting previous job: {str(e)}")

    # Submit the Batch job
    try:
        job = client.create_job(
            parent=f"projects/{project_id}/locations/us-central1",
            job=batch_job_config_dict
        )
        print("Batch job triggered successfully.")
        
        # Poll the job status
        job_status = client.get_job(name=job_name)
        print(f"Job status: {job_status.status.state}")
        
        # Optionally, wait for the job to start running (optional step)
        while job_status.status.state == batch_v1.JobStatus.State.PENDING:
            print("Waiting for job to start running...")
            time.sleep(5)
            job_status = client.get_job(name=job_name)
        
        print(f"Job is now in state: {job_status.status.state}")
        
    except AlreadyExists:
        print(f"Job {job_name} already exists and is still running.")
    except Exception as e:
        print(f"Failed to submit Batch job: {str(e)}")
