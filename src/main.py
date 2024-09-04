import os
import argparse
from concurrent.futures import ThreadPoolExecutor

import boto3
from tqdm import tqdm
from P4 import P4, P4Exception


def upload_file_to_s3(
    s3_client, local_file_path, bucket_name, local_folder, progress_bar
):
    """
    Uploads a single file to S3 using the provided S3 client.

    Args:
        s3_client (boto3.client): The S3 client object.
        local_file_path (str): The path to the local file.
        bucket_name (str): The name of the S3 bucket.
        local_folder (str): The root local folder for relative path calculation.
        pbar (tqdm.tqdm): The progress bar object.
    """
    s3_key = os.path.relpath(local_file_path, local_folder)
    s3_client.upload_file(
        local_file_path,
        bucket_name,
        s3_key,
        Config=boto3.s3.transfer.TransferConfig(use_threads=True),
    )
    progress_bar.update(1)
    print(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")


def migrate_folder_to_s3(
    local_folder, s3_url, bucket_name, access_key, secret_key, token=None, num_workers=4
):
    """
    Migrates a complete folder structure from a local drive to an S3 bucket, preserving file paths.

    Args:
        local_folder (str): The path to the local folder to be migrated.
        s3_url (str): The URL of the S3 endpoint.
        bucket_name (str): The name of the S3 bucket.
        access_key (str): The AWS access key.
        secret_key (str): The AWS secret key.
        token (str, optional): The AWS session token (if required by the S3 provider).
        num_workers (int, optional): The number of worker threads to use for concurrent uploads.
    """

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=token,
    )

    # Get a list of all files to upload for the progress bar
    all_files = []
    for root, dirs, files in os.walk(local_folder):
        all_files.extend(os.path.join(root, file) for file in files)

    with tqdm(
        total=len(all_files), desc="Uploading files", unit="file"
    ) as progress_bar, ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit upload tasks to the thread pool
        futures = [
            executor.submit(
                upload_file_to_s3,
                s3,
                file_path,
                bucket_name,
                local_folder,
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
        args.token,
        args.num_workers,
    )
