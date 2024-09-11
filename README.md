# S3 Folder Migration Tool

This project contains a Python script (`upload_to_s3.py`) that migrates a complete folder structure from a local drive to an S3 bucket, preserving file paths.

The purpose of this is to move a Helix Core depot from local block storage to cheaper S3 storage. After migrating the depot files, the depot specification can be updated to point to the S3 bucket.

See the [Helix Core Admin Guide](https://www.perforce.com/manuals/p4sag/Content/P4SAG/depots.moving.prod.html?Highlight=s3#S3_storage) for more information about S3-backed depots.

## Features

- Uploads an entire folder structure to an S3 bucket
- Preserves relative file paths
- Supports concurrent uploads for improved performance
- Skips files that already exist in the S3 bucket
- Provides detailed logging and a progress bar
- Generates the necessary S3 address for Perforce depot specification

## Prerequisites

- Python 3.6+
- Required Python packages (install using `pip install -r requirements.txt`):
  - boto3
  - tqdm

## Usage

Run the script from the command line with the following arguments:

```
python src/upload_to_s3.py --local_folder <path_to_local_folder> --bucket_name <s3_bucket_name> --access_key <aws_access_key> --secret_key <aws_secret_key> --prepend_path <path_to_prepend> [additional_options]
```

### Required Arguments

- `--local_folder`: Path to the local folder to be migrated
- `--bucket_name`: Name of the S3 bucket
- `--access_key`: AWS access key
- `--secret_key`: AWS secret key
- `--prepend_path`: Path to prepend to the S3 key
- `--s3_url`: S3 endpoint URL (not required for AWS S3)

### Optional Arguments

- `--aws_region`: AWS region (only required if using AWS S3)
- `--token`: AWS session token (if required)
- `--num_workers`: Number of worker threads for concurrent uploads (default: 4)

## Example

### AWS
```
python src/upload_to_s3.py --aws_region us-west-2 --bucket_name my-s3-bucket --access_key AKIAIOSFODNN7EXAMPLE --secret_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY --prepend_path /p4/1/depots --local_folder /p4/1/depots/my_depot
```

### Digital Ocean
```
python src/upload_to_s3.py --s3_url https://my-s3-bucket.sfo3.digitaloceanspaces.com --bucket_name my-s3-bucket --access_key AKIAIOSFODNN7EXAMPLE --secret_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY --prepend_path /p4/1/depots --local_folder /p4/1/depots/my_depot
```

## Output

The script provides:

1. A progress bar showing the upload status
2. Detailed logs in the console and in a file named `s3_migration.log`
3. The S3 address to be used in the Perforce depot specification

### Update Depot Specification

After the script finishes copying the files to s3, it will provide you with an address field to copy and paste into your depot spec. You can use the `p4 depot` command to update your depot specification. For example:

    p4 depot my_depot

This will open the depot spec in your text editor. If it does not already exist, add a new line for the `Address:` field where you will paste the line you copied from the script.

```
Depot:	my_depot

Owner:	perforce

Date:	2024/09/10 21:11:02

Description:
	Example of a depot spec using s3 storage

Type:	stream

Address:	s3,region:us-west-2,bucket:my-s3-bucket,accessKey:******,secretKey:******

StreamDepth:	//my_depot/1

Map:	my_depot/...
```

Note that once the `Address:` field is added to the depot spec, it will no longer reference the `Map:` path for local storage and that local folder can be moved, renamed, or deleted once you've confirmed all the files have been copied to S3.


## Logging

Detailed logs are written to `s3_migration.log` in the same directory as the script. The log file contains DEBUG level messages, while the console output shows INFO level messages and above.

## Error Handling

The script includes retry logic for failed uploads and will attempt to upload a file up to 3 times before moving on to the next file.

## Contributing

Feel free to submit issues or pull requests if you have suggestions for improvements or encounter any problems.

## License

This project is licensed under the MIT license.
