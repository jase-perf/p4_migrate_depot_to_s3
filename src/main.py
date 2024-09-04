import os
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from tqdm import tqdm
from P4 import P4, P4Exception

p4 = P4()
p4.connect()


def get_p4_depot_root() -> Path:
    """
    Returns the local directory where depots are stored.
    This is either the P4ROOT environment variable or the server.depot.root configuration value if it is set.
    """
    p4root = p4.run("configure", "show", "P4ROOT")[0]["Value"]
    depot_root = p4.run("configure", "show", "server.depot.root")
    depot_root = Path(depot_root[0]["Value"] if depot_root else p4root)
    return depot_root


def get_p4_depots() -> list[str]:
    """
    Returns a list of all depots in the Perforce server.
    """
    depots = p4.run("depots")
    return [depot["name"] for depot in depots]


def upload_file_to_s3(
    s3_client, local_file_path, bucket_name, local_folder, progress_bar
):
    """
    Uploads a single file to S3 using the provided S3 client.

    Args:
        s3_client (boto3.client): The S3 client object.
        local_file_path (str | Path): The path to the local file.
        bucket_name (str): The name of the S3 bucket.
        local_folder (str | Path): The root local folder for relative path calculation.
        progress_bar (tqdm.tqdm): The progress bar object.
    """
    # Check if the file already exists in S3
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        print(f"Skipping {local_file_path} - already exists in S3")
    except boto3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":  # File not found
            s3_client.upload_file(
                local_file_path,
                bucket_name,
                s3_key,
                Config=boto3.s3.transfer.TransferConfig(use_threads=True),
            )
            print(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
        else:
            raise  # Re-raise other errors

    progress_bar.update(1)


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
        total=len(all_files), desc="Processing files", unit="file"
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
