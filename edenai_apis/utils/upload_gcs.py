import datetime
import json
import os
from io import BytesIO
from typing import Callable, Tuple
from uuid import uuid4

from google.cloud import storage

from edenai_apis.loaders.data_loader import ProviderDataEnum
from edenai_apis.loaders.loaders import load_provider
from edenai_apis.settings import keys_path

BUCKET = "mow-ai-engine-dev"
BUCKET_RESOURCE = ""

PROVIDER_PROCESS = "provider_process"
USER_PROCESS = "users_process"

URL_SHORT_PERIOD = 3600
URL_LONG_PERIOD = 3600 * 24 * 7

def set_time_and_presigned_url_process(process_type: str) -> Tuple[Callable, int, str]:
    """Returns a tuple with the appropriate function to call, the URL expiration time, and the bucket to which
    the file will be uploaded, depending on the process type

    Args:
        process_type (str): Specifies the upload type, whether it's to generate a URL for a provider, or for a user

    Returns:
        Tuple[Callable, int, str]: A tuple with the appropriate function to call, the URL expiration time, and the bucket
    """
    if process_type == PROVIDER_PROCESS:
        return get_gcs_file_url, URL_SHORT_PERIOD, BUCKET
    if process_type == USER_PROCESS:
        return get_gcs_file_url, URL_LONG_PERIOD, BUCKET_RESOURCE


def gcs_client_load() -> storage.Client:
    """Initializes and returns the Google Cloud Storage client."""
    return storage.Client()


def upload_file_to_gcs(file_path: str, file_name: str, process_type=PROVIDER_PROCESS):
    """Upload file to GCS"""
    filename = f"{uuid4()}_{file_name}"
    gcs_client = gcs_client_load()
    bucket_name = set_time_and_presigned_url_process(process_type)[2]
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    blob.upload_from_filename(file_path)
    func_call, process_time, _ = set_time_and_presigned_url_process(process_type)
    return func_call(blob.name, process_time)


def upload_file_bytes_to_gcs(
    file: BytesIO, file_name: str, process_type: str = PROVIDER_PROCESS
) -> str:
    """Upload file bytes to GCS"""
    filename = f"{uuid4()}_{file_name}"
    gcs_client = gcs_client_load()
    bucket_name = set_time_and_presigned_url_process(process_type)[2]
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    blob.upload_from_file(file, rewind=True)
    func_call, process_time, _ = set_time_and_presigned_url_process(process_type)
    return func_call(blob.name, process_time)


def get_gcs_file_url(filename: str, process_time: int) -> str:
    """Generate a signed URL for a GCS file"""
    gcs_client = gcs_client_load()
    bucket_name = BUCKET
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    url = blob.generate_signed_url(
        expiration=datetime.timedelta(seconds=process_time),
        version="v4",
    )
    return url


def get_providers_json_from_gcs():
    """Retrieve providers JSON from GCS"""
    gcs_client = gcs_client_load()
    bucket = gcs_client.bucket("providers-cost")
    blob = bucket.blob("providers_cost_master.json")
    json_data = blob.download_as_text()
    json_dict = json.loads(json_data)
    return json_dict["cost_data"]
