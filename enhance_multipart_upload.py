import boto3
from boto3.s3.transfer import TransferConfig
import os
from datetime import datetime
import uuid

def upload_file_with_transfer_manager(file_path, bucket_name, client_id=None):
    """
    Upload a file using TransferManager with a unique key for each client.
    """
    # Initialize the S3 client for Ozone
    client = boto3.client(
        's3',
        endpoint_url='http://ozone-server:9878',  # Replace with your Ozone endpoint
        aws_access_key_id='your_access_key',
        aws_secret_access_key='your_secret_key'
    )

    # Generate a unique key name for the file
    unique_id = uuid.uuid4()  # Generate a unique identifier
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Add a timestamp for further uniqueness
    key_name = f"{client_id}_{timestamp}_{unique_id}_{os.path.basename(file_path)}" if client_id else f"{timestamp}_{unique_id}_{os.path.basename(file_path)}"

    # Set transfer configuration
    config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,  # 5 MB threshold for multipart upload
        multipart_chunksize=5 * 1024 * 1024,  # 5 MB per chunk
        use_threads=True  # Enable multithreaded uploads
    )

    try:
        # Upload file with TransferManager
        client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=key_name,
            Config=config
        )
        print(f"File {file_path} uploaded successfully as {key_name}.")
    except Exception as e:
        print(f"Upload failed: {e}")

if __name__ == "__main__":
    # Details
    bucket_name = "your-bucket-name"
    file_path = "path/to/your/large/file"
    client_id = "client_123"  # Replace with a unique identifier for each client (optional)

    upload_file_with_transfer_manager(file_path, bucket_name, client_id)
