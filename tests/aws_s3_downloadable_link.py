import boto3

s3 = boto3.client('s3')

url = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'your-bucket-name', 'Key': 'your/file/path'},
    ExpiresIn=3600
)

print(url)



import requests
import boto3

presigned_url = "https://your-presigned-url-here"
target_bucket = "your-target-bucket"
target_key = "your/prefix/filename.pdf"  # Example: "uploads/2025/report.pdf"

# Step 1: Download from the presigned URL
response = requests.get(presigned_url)

# Step 2: Upload to your S3 bucket
s3 = boto3.client("s3")
s3.put_object(
    Bucket=target_bucket,
    Key=target_key,
    Body=response.content
)

print("File uploaded to s3://%s/%s" % (target_bucket, target_key))
