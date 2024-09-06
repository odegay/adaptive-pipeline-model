import os
import google.auth
from google.cloud import batch_v1

def adaptive_pipeline_model_function(event, context):
    """Triggered by a message on a Pub/Sub topic and triggers a GCP Batch job."""
    # Authenticate and initialize the Batch client
    credentials, project_id = google.auth.default()
    client = batch_v1.BatchServiceClient(credentials=credentials)
    # Generate the batch job configuration as a Python dictionary
    batch_job_config_dict = {
        'name': "projects/{}/locations/us-central1/jobs/model-training-job".format(project_id),
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
    # Submit the Batch job
    client.create_job(
        parent=f"projects/{project_id}/locations/us-central1",
        job=batch_job_config_dict
    )
    print("Batch job triggered successfully.")
