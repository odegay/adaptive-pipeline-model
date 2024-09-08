import base64
import json
from adpipsvcfuncs import publish_to_pubsub, load_current_pipeline_data, save_current_pipeline_data
from adpipsvcfuncs import fetch_gcp_secret, load_valid_json
from adpipwfwconst import MSG_TYPE
from adpipwfwconst import PIPELINE_TOPICS as TOPICS
import logging
import requests
from build_ffn_configured import build_flexible_model

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

# Function to load data from the csv file located in the GCS bucket to the DataFrame
def load_data_from_gcs_bucket(bucket_name: str, file_name: str) -> dict:
    try:
        url = f"https://storage.googleapis.com/{bucket_name}/{file_name}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"Failed to load data from the GCS bucket: {bucket_name}, file: {file_name}")
            return None
    except Exception as e:
        logger.error(f"Failed to load data from the GCS bucket: {bucket_name}, file: {file_name}, error: {e}")
        return None


# Function to get the FFN model configuration for a given pipeline_id
def adaptive_pipeline_get_model(pipeline_id: str) -> dict:
    
    pipeline_data = load_current_pipeline_data(pipeline_id)
    if pipeline_data is None:
        logger.error(f"Failed to load pipeline data for pipeline_id: {pipeline_id}")
        return None
    
    if 'current_configuration' not in pipeline_data:
        logger.error(f"current_configuration not found in pipeline data for pipeline_id: {pipeline_id}")
        return None
    
    model_config = pipeline_data.get('current_configuration')

    hidden_layers_model = build_flexible_model(model_config)
    logger.error(f"DEBUG MODE break for pipeline_id: {pipeline_id}")

# Function that is triggered by a cloud function to process the batch data    
def train_model():
    # Example of a very basic model training logic
    logger.debug("Starting model training...")

    # Dummy model training process
    model_result = 2 + 2

    logger.debug(f"Model training completed. Result: {model_result}")

if __name__ == "__main__":
    train_model()