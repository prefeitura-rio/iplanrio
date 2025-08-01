# -*- coding: utf-8 -*-
from typing import List

from google.cloud import storage
from google.cloud.storage.blob import Blob

from iplanrio.pipelines_utils.env import get_bd_credentials_from_env


def delete_blobs_list(bucket_name: str, blobs: List[Blob], mode: str = "prod") -> None:
    """
    Deletes all blobs in the bucket that are in the blobs list.
    Mode needs to be "prod" or "staging"
    """
    storage_client = get_gcs_client(mode=mode)

    bucket = storage_client.bucket(bucket_name)
    bucket.delete_blobs(blobs)


def get_gcs_client(mode: str = "staging") -> storage.Client:
    """
    Get a GCS client with the credentials from the environment.
    Mode needs to be "prod" or "staging"

    Args:
        mode (str): The mode to filter by (prod or staging).

    Returns:
        storage.Client: The GCS client.
    """

    credentials = get_bd_credentials_from_env(mode=mode)
    return storage.Client(credentials=credentials)


def list_blobs_with_prefix(
    bucket_name: str, prefix: str, mode: str = "staging"
) -> List[Blob]:
    """
    Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    Mode needs to be "prod" or "staging"

    Args:
        bucket_name (str): The name of the bucket.
        prefix (str): The prefix to filter by.
        mode (str): The mode to filter by (prod or staging).

    Returns:
        List[Blob]: The list of blobs.
    """
    storage_client = get_gcs_client(mode=mode)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return list(blobs)


def parse_blobs_to_partition_dict(blobs: list) -> dict:
    """
    Extracts the partition information from the blobs.
    """

    partitions_dict = {}
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                try:
                    partitions_dict[key].append(value)
                except KeyError:
                    partitions_dict[key] = [value]
    return partitions_dict


def parse_blobs_to_partition_list(blobs: List[Blob]) -> List[str]:
    """
    Extracts the partition information from the blobs.
    """
    partitions = []
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                if key == "data_particao":
                    partitions.append(value)
    return partitions


def upload_file_to_bucket(
    bucket_name: str, file_path: str, destination_blob_name: str, mode: str = None
) -> "Blob":
    """
    Uploads a file to the bucket.
    Mode needs to be "prod" or "staging"

    Args:
        bucket_name (str): The name of the bucket.
        file_path (str): The path of the file to upload.
        destination_blob_name (str): The name of the blob.
        mode (str): The mode to filter by (prod or staging).
    """

    storage_client = get_gcs_client(mode=mode)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    return blob
