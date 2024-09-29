import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from google.cloud import storage
from adpipsvcfuncs import fetch_gcp_secret, load_valid_json
import logging

logger = logging.getLogger('batch_logger')
if not logger.handlers:
    # Create console handler and set its log level to DEBUG
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # Create formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    # Add the handler to the root logger
    logger.addHandler(ch)

def load_data_from_gcs(bucket_uri: str) -> pd.DataFrame:
    """
    Load CSV data from a Google Cloud Storage bucket URI and return as a Pandas DataFrame.

    :param bucket_uri: The URI of the CSV file in the GCP bucket.
    :return: DataFrame containing the CSV data.
    """
    storage_client = storage.Client()
    bucket_name, file_path = bucket_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    data = blob.download_as_text()
    df = pd.read_csv(pd.compat.StringIO(data))
    return df

def split_features_and_output(df: pd.DataFrame, output_column: str = 'Num1'):
    """
    Split the DataFrame into features and output based on the specified column name.

    :param df: Input DataFrame containing the data.
    :param output_column: The column to be used as output. Default is 'Num1'.
    :return: Tuple of (features DataFrame, output Series)
    """
    output = df[output_column]
    features = df.drop(columns=[output_column])
    return features, output

def normalize_features(features: pd.DataFrame) -> pd.DataFrame:
    """
    Perform min-max normalization on the feature set.

    :param features: DataFrame containing the features.
    :return: Normalized DataFrame with values scaled between 0 and 1.
    """
    scaler = MinMaxScaler()
    normalized_features = pd.DataFrame(scaler.fit_transform(features), columns=features.columns)
    return normalized_features

def prepare_input_tensor(features: pd.DataFrame, output: pd.Series, test_size: float = 0.2):
    """
    Convert features and output into TensorFlow tensors and split into training and testing sets.

    :param features: DataFrame containing the features.
    :param output: Series containing the output labels.
    :param test_size: Fraction of the data to be used as test set.
    :return: Train and test TensorFlow tensors for both features and output.
    """
    X_train, X_test, y_train, y_test = train_test_split(features, output, test_size=test_size, random_state=42)

    # Convert DataFrames and Series to TensorFlow tensors
    train_features_tensor = tf.convert_to_tensor(X_train, dtype=tf.float32)
    test_features_tensor = tf.convert_to_tensor(X_test, dtype=tf.float32)
    train_output_tensor = tf.convert_to_tensor(y_train, dtype=tf.float32)
    test_output_tensor = tf.convert_to_tensor(y_test, dtype=tf.float32)

    return (train_features_tensor, train_output_tensor), (test_features_tensor, test_output_tensor)

# Main script flow for FFN training
def main_training_process():
    # Example usage
    secret_name = "adaptive-pipeline-dataset-n1-lnk"  # The name of the GCP secret containing the bucket URI    
    bucket_uri = fetch_gcp_secret(secret_name)

    # Load data
    df = load_data_from_gcs(bucket_uri)

    # Split features and output
    features, output = split_features_and_output(df)

    # Normalize features
    normalized_features = normalize_features(features)

    # Prepare input tensors
    (train_features_tensor, train_output_tensor), (test_features_tensor, test_output_tensor) = prepare_input_tensor(normalized_features, output)

    # Return the prepared tensors for further processing (e.g., model training)
    return train_features_tensor, train_output_tensor, test_features_tensor, test_output_tensor