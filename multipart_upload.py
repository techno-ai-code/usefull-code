import boto3
import os

def initiate_multipart_upload(client, bucket_name, key_name):
    """
    Initiate multipart upload and return the upload ID.
    """
    response = client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
    return response['UploadId']

def upload_part(client, bucket_name, key_name, upload_id, file_path, part_number, start, end):
    """
    Upload a specific part of the file.
    """
    with open(file_path, 'rb') as f:
        f.seek(start)
        data = f.read(end - start + 1)
        response = client.upload_part(
            Bucket=bucket_name,
            Key=key_name,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=data
        )
    return {"PartNumber": part_number, "ETag": response['ETag']}

def complete_multipart_upload(client, bucket_name, key_name, upload_id, parts):
    """
    Complete the multipart upload.
    """
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key_name,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts}
    )

def upload_file(client, bucket_name, file_path, key_name):
    """
    Upload the whole file using multipart upload.
    """
    file_size = os.path.getsize(file_path)
    part_size = 5 * 1024 * 1024  # 5 MB

    # Calculate number of parts
    num_parts = (file_size + part_size - 1) // part_size
    parts = []

    # Initiate multipart upload
    upload_id = initiate_multipart_upload(client, bucket_name, key_name)

    try:
        # Upload parts
        for part_number in range(1, num_parts + 1):
            start = (part_number - 1) * part_size
            end = min(start + part_size - 1, file_size - 1)
            part_info = upload_part(client, bucket_name, key_name, upload_id, file_path, part_number, start, end)
            parts.append(part_info)

        # Sort parts by PartNumber
        parts.sort(key=lambda x: x['PartNumber'])

        # Complete multipart upload
        complete_multipart_upload(client, bucket_name, key_name, upload_id, parts)
        print(f"File {file_path} uploaded successfully as {key_name}.")
    except Exception as e:
        # Abort upload if there is an error
        client.abort_multipart_upload(Bucket=bucket_name, Key=key_name, UploadId=upload_id)
        print(f"Upload failed: {e}")

if __name__ == "__main__":
    # Ozone S3 client
    client = boto3.client(
        's3',
        endpoint_url='http://ozone-server:9878',  # Replace with your Ozone endpoint
        aws_access_key_id='your_access_key',
        aws_secret_access_key='your_secret_key'
    )

    # Details
    bucket_name = "your-bucket-name"
    file_path = "path/to/your/large/file"

    # Generate a unique key name for each client
    key_name = f"client_upload_{os.path.basename(file_path)}"

    upload_file(client, bucket_name, file_path, key_name)




====================================================================================================================
@app.get("/download")
def download_file(object_name: str):
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_name)

        def file_iterator():
            for chunk in response['Body'].iter_chunks(chunk_size=8192):
                yield chunk

        return StreamingResponse(
            file_iterator(),
            media_type='application/octet-stream',
            headers={"Content-Disposition": f"attachment; filename={object_name}"}
        )
    except ClientError as e:
        raise HTTPException(status_code=404, detail=f"File not found: {str(e)}")

