import os
import google.auth
from google.cloud import batch_v1

def adaptive_pipeline_model_function(event, context):
    """Triggered by a message on a Pub/Sub topic and triggers a GCP Batch job."""
    
    # Authenticate and initialize the Batch client
    credentials, project_id = google.auth.default()
    client = batch_v1.BatchServiceClient(credentials=credentials)

    # Path to the batch job config file
    batch_config_path = os.path.join(os.path.dirname(__file__), 'batch-config', 'batch-job-config.yaml')
    
    # Read the batch job configuration
    with open(batch_config_path, 'r') as f:
        batch_job_config = f.read()
    
    # Parse and submit the Batch job
    client.submit_job(
        parent=f"projects/{project_id}/locations/us-central1",  # Update with your location
        job=batch_job_config,
    )
    
    print("Batch job triggered successfully.")