import os
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
import logging

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

# Setup Logging
LOGFILE = "s3_migration.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(LOGFILE)
file_handler.setLevel(logging.DEBUG)  # logging level for the file handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)  # logging level for the console handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def upload_file_to_s3(
    s3_client,
    local_file_path,
    bucket_name,
    local_folder,
    prepend_path,
    progress_bar,
    max_retries=3,
):
    """
    Uploads a single file to S3 using the provided S3 client.

    Args:
        s3_client (boto3.client): The S3 client object.
        local_file_path (str | Path): The path to the local file.
        bucket_name (str): The name of the S3 bucket.
        local_folder (str | Path): The root local folder for relative path calculation.
        prepend_path (str): The path to prepend to the S3 key.
        progress_bar (tqdm.tqdm): The progress bar object.
    """
    relative_path = os.path.relpath(local_file_path, local_folder)
    s3_key = os.path.join(prepend_path, relative_path).replace(os.sep, "/")

    for attempt in range(max_retries + 1):
        try:
            # Check if the file already exists in S3
            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                logger.debug(f"Skipping {local_file_path} - already exists in S3")
                break  # Skip if file exists
            except ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    raise  # Re-raise other errors

            # Upload the file
            s3_client.upload_file(
                local_file_path,
                bucket_name,
                s3_key,
                Config=boto3.s3.transfer.TransferConfig(use_threads=True),
            )
            logger.debug(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
            break  # Exit the loop if upload is successful
        except Exception as e:  # Catch any exception during upload
            if attempt >= max_retries:
                logger.error(f"Failed to upload {local_file_path}")
                raise  # Re-raise the error after max retries
            logger.warning(
                f"Error uploading {local_file_path}, retrying in 2 seconds...\n{e}"
            )
            time.sleep(2)
    progress_bar.update(1)


def migrate_folder_to_s3(
    local_folder,
    s3_url,
    bucket_name,
    access_key,
    secret_key,
    prepend_path,
    token=None,
    num_workers=4,
):
    """
    Migrates a complete folder structure from a local drive to an S3 bucket, preserving file paths.

    Args:
        local_folder (str): The path to the local folder to be migrated.
        s3_url (str): The URL of the S3 endpoint.
        bucket_name (str): The name of the S3 bucket.
        access_key (str): The AWS access key.
        secret_key (str): The AWS secret key.
        prepend_path (str): The path to prepend to the S3 key.
        token (str, optional): The AWS session token (if required by the S3 provider).
        num_workers (int, optional): The number of worker threads to use for concurrent uploads.
    """
    logger.info(f"Beginning upload of {local_folder} to s3 bucket: '{bucket_name}'")
    logger.info(f"Prepending path: '{prepend_path}' to S3 keys")
    logger.info(f"See {LOGFILE} for more detailed logs")

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=token,
    )

    all_files = []
    for root, dirs, files in os.walk(local_folder):
        all_files.extend(os.path.join(root, file) for file in files)

    # Use tqdm progress bar and let threads update it concurrently
    with tqdm(
        total=len(all_files), desc="Processing files", unit="file"
    ) as progress_bar, ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(
                upload_file_to_s3,
                s3,
                file_path,
                bucket_name,
                local_folder,
                prepend_path,
                progress_bar,
            )
            for file_path in all_files
        ]
        # Wait for all tasks to complete
        for future in futures:
            future.result()  # This will raise any exceptions that occurred during upload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate a folder structure to S3")
    parser.add_argument(
        "--local_folder", required=True, help="Path to the local folder"
    )
    parser.add_argument("--s3_url", required=True, help="S3 endpoint URL")
    parser.add_argument("--bucket_name", required=True, help="S3 bucket name")
    parser.add_argument("--access_key", required=True, help="AWS access key")
    parser.add_argument("--secret_key", required=True, help="AWS secret key")
    parser.add_argument(
        "--prepend_path", required=True, help="Path to prepend to the S3 key"
    )
    parser.add_argument("--token", help="AWS session token (if required)", default=None)
    parser.add_argument(
        "--num_workers",
        type=int,
        default=4,
        help="Number of worker threads to use for concurrent uploads",
    )

    args = parser.parse_args()

    migrate_folder_to_s3(
        args.local_folder,
        args.s3_url,
        args.bucket_name,
        args.access_key,
        args.secret_key,
        args.prepend_path,
        args.token,
        args.num_workers,
    )
