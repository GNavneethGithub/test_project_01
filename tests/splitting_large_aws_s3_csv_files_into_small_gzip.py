import boto3
import gzip
import io
import time
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError, NoCredentialsError

# --- Core Processing Logic ---

def parse_s3_uri(uri):
    """Helper to extract bucket and key from an s3:// URI."""
    if not uri.startswith('s3://'):
        raise ValueError(f"URI must start with 's3://': {uri}")
    
    # Split the URI: s3://bucket/key/path/file
    parts = uri[5:].split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    return bucket, key

def create_s3_client(aws_creds, region_name='us-east-1'):
    """Initializes and returns a Boto3 S3 client using explicit credentials."""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            region_name=region_name # Provide a default region if one is not in the dict
        )
        return s3_client
    except KeyError as e:
        raise ValueError(f"Missing required key in aws_creds dictionary: {e}")
    except Exception as e:
        print(f"Error creating S3 client: {e}")
        raise

def process_and_upload_chunk(s3_client, source_bucket, source_key, target_bucket, target_prefix, 
                             start_byte, end_byte, chunk_index, max_retries=3):
    """
    Reads a byte range from S3, compresses it, and uploads the GZIP file.
    Implements a robust exponential backoff retry mechanism.
    """
    range_header = f'bytes={start_byte}-{end_byte}'
    # Ensure chunk files are named sequentially and are placed in the target folder
    target_key = f"{target_prefix}part_{chunk_index:05d}.csv.gz"
    
    print(f"|--- Chunk {chunk_index:05d}: Range {range_header} --> {target_key}")

    for attempt in range(max_retries):
        try:
            # 1. Read the chunk from S3 using Range GET
            response = s3_client.get_object(
                Bucket=source_bucket,
                Key=source_key,
                Range=range_header
            )
            
            # The .read() call only pulls the chunk's data into memory
            chunk_data = response['Body'].read()
            
            # 2. Compress the data in memory using GZIP
            compressed_data = io.BytesIO()
            # Use 'wb' for writing binary data to the in-memory buffer
            with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gzf:
                gzf.write(chunk_data)
            
            compressed_data.seek(0) # Reset buffer position to the start for reading/uploading
            
            # 3. Upload the compressed data to the target S3 location
            s3_client.upload_fileobj(
                compressed_data,
                Bucket=target_bucket,
                Key=target_key,
                ExtraArgs={
                    'ContentType': 'application/gzip',
                    'ContentEncoding': 'gzip'
                }
            )
            
            print(f"|--- Chunk {chunk_index:05d}: Successfully uploaded.")
            return True  # Success!

        except (ClientError, NoCredentialsError) as e:
            # ClientError captures typical S3 API errors (e.g., 404, permissions, connection timeouts)
            # NoCredentialsError ensures we catch explicit credential issues early
            error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', 'N/A')
            print(f"|--- Chunk {chunk_index:05d} (Attempt {attempt + 1}/{max_retries}): Failed with error code {error_code}: {e}")
            if attempt < max_retries - 1:
                # Exponential backoff: 2, 4, 8 seconds, etc.
                sleep_time = 2 ** attempt
                print(f"|--- Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time) 
            else:
                print(f"|--- Chunk {chunk_index:05d}: Failed after {max_retries} attempts. Stopping.")
                return False  # All retries failed
        except Exception as e:
            # Catch all other unexpected errors
            print(f"|--- Chunk {chunk_index:05d} (Attempt {attempt + 1}/{max_retries}): Unexpected error: {e}")
            return False


# --- Main Orchestration Function ---

def process_s3_file_in_parallel(aws_creds, aws_s3_source_file_uri, aws_s3_target_folder_uri, number_of_parallel_threads):
    """
    Orchestrates the chunking, parallel processing, and upload of a large S3 file.
    
    Inputs:
    - aws_creds (dict): Contains 'aws_access_key_id' and 'aws_secret_access_key'.
    - aws_s3_source_file_uri (str): s3://bucket/key/file.csv
    - aws_s3_target_folder_uri (str): s3://bucket/folder/
    - number_of_parallel_threads (int): Max worker threads.
    """
    
    s3_client = create_s3_client(aws_creds)

    source_bucket, source_key = parse_s3_uri(aws_s3_source_file_uri)
    target_bucket, target_prefix = parse_s3_uri(aws_s3_target_folder_uri)
    
    # Ensure target_prefix ends with a slash for folder-like behavior
    if target_prefix and not target_prefix.endswith('/'):
        target_prefix += '/'

    print(f"Source: s3://{source_bucket}/{source_key}")
    print(f"Target: s3://{target_bucket}/{target_prefix}")
    print(f"Threads: {number_of_parallel_threads}")
    
    # 1. Get the file size
    try:
        print("\nChecking file size...")
        response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
        file_size = response['ContentLength']
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ ERROR: Source file not found: s3://{source_bucket}/{source_key}")
        else:
            print(f"❌ ERROR: Head object failed: {e}")
        return False
    except Exception as e:
        print(f"❌ ERROR: An unexpected error occurred: {e}")
        return False

    # Target chunk size is 250MB (250 * 1024 * 1024 bytes)
    chunk_size = 250 * 1024 * 1024 
    
    # 2. Calculate the byte ranges
    chunks_to_process = []
    start = 0
    chunk_index = 0
    
    while start < file_size:
        # The 'Range' header is inclusive, so we subtract 1 from the end byte
        end = min(start + chunk_size - 1, file_size - 1)
        chunks_to_process.append({
            'start_byte': start, 
            'end_byte': end, 
            'index': chunk_index
        })
        start = end + 1
        chunk_index += 1

    print(f"Total size: {file_size / (1024*1024*1024):.2f} GB | Chunks to process: {len(chunks_to_process)}\n")
    
    # 3. Use ThreadPoolExecutor for parallel processing
    results = []
    with ThreadPoolExecutor(max_workers=number_of_parallel_threads) as executor:
        # Submit all chunk processing tasks
        future_to_chunk = {
            executor.submit(
                process_and_upload_chunk, 
                s3_client, source_bucket, source_key, target_bucket, target_prefix,
                chunk['start_byte'], chunk['end_byte'], chunk['index']
            ): chunk 
            for chunk in chunks_to_process
        }
        
        # Collect results as they complete
        for future in future_to_chunk:
            try:
                success = future.result()
                results.append(success)
            except Exception as e:
                # This catches exceptions from the executor itself, not the ones handled inside the worker function
                print(f"Chunk processing resulted in a critical error: {e}")
                results.append(False)

    # 4. Report final status
    successful_chunks = sum(results)
    total_chunks = len(chunks_to_process)
    
    print("\n" + "-"*50)
    if successful_chunks == total_chunks:
        print(f"✅ SUCCESS: All {total_chunks} chunks processed and uploaded.")
        print(f"Source file s3://{source_bucket}/{source_key} was NOT deleted.")
        print(f"Output files are in s3://{target_bucket}/{target_prefix}")
        return True
    else:
        failed_chunks = total_chunks - successful_chunks
        print(f"⚠️ FAILURE: {failed_chunks}/{total_chunks} chunks failed after max retries.")
        print("Please check the logs for the specific chunk indexes that failed.")
        return False

# --- Example Usage ---

if __name__ == "__main__":
    # 🔑 1. AWS Credentials (REPLACE WITH YOUR ACTUAL DICT)
    # The dictionary name requested by the user is 'aws_creds'
    AWS_CREDS_DICT = {
        'aws_access_key_id': 'YOUR_AWS_ACCESS_KEY_ID', # <-- REPLACE THIS
        'aws_secret_access_key': 'YOUR_AWS_SECRET_ACCESS_KEY', # <-- REPLACE THIS
        # Optional: Specify the region of your bucket if it's not the default
        # 'region_name': 'us-west-2' 
    }
    
    # 📂 2. S3 URIs (REPLACE WITH YOUR ACTUAL URIs)
    SOURCE_FILE_URI = "s3://your-source-bucket/path/to/your/50gb_file.csv"
    TARGET_FOLDER_URI = "s3://your-target-bucket/output/processed_csv/" 
    
    # ⚙️ 3. Configuration
    NUM_THREADS = 10 # Adjust based on your internet speed and CPU capacity

    # Run the main process
    try:
        overall_success = process_s3_file_in_parallel(
            aws_creds=AWS_CREDS_DICT,
            aws_s3_source_file_uri=SOURCE_FILE_URI,
            aws_s3_target_folder_uri=TARGET_FOLDER_URI,
            number_of_parallel_threads=NUM_THREADS
        )
        print(f"\nOverall process completed with status: {overall_success}")
    except ValueError as e:
        print(f"Execution aborted due to configuration error: {e}")
    except Exception as e:
        print(f"A critical script error occurred: {e}")