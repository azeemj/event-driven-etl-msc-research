To integrate the split_file function into your AWS Step Functions state machine and pass the dynamically generated chunked file names and correlation ID to subsequent Lambda functions (DataExtraction-phase02 in this case), you'll need to make a few modifications. Let's go timport boto3
import json
from datetime import datetime
import logging
import uuid

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

def create_dynamodb_table(table_name):
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'tenant_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'chunk_key',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'tenant_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'chunk_key',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Wait until the table exists.
        dynamodb.get_waiter('table_exists').wait(TableName=table_name)
        return True
    except dynamodb.exceptions.ResourceInUseException:
        # Table already exists
        return True
    except Exception as e:
        logger.error(f"Error creating DynamoDB table: {str(e)}")
        return f"Error creating DynamoDB table: {str(e)}"

def insert_into_dynamodb(tenant_id, chunk_key, number_of_records, correlation_id, 
                            filename, stage= "initial"):
    table_name = f'SplitDataTracker{tenant_id}'
    
    # Ensure the table exists
    create_table_response = create_dynamodb_table(table_name)
    if create_table_response is not True:
        return create_table_response

    try:
        response = dynamodb.put_item(
            TableName=table_name,
            Item={
                'tenant_id': {'S': tenant_id},
                'chunk_key': {'S': chunk_key},
                'upload_timestamp': {'S': datetime.utcnow().isoformat()},
                'number_of_records': {'N': str(number_of_records)},
                'correlation_id': {'S': correlation_id},
                'filename': {'S': filename},
                'stage': {'S': stage}
            }
        )
        return True
    except Exception as e:
        logger.error(f"Error inserting into DynamoDB: {str(e)}")
        return f"Error inserting into DynamoDB: {str(e)}"

def split_file(bucket_name, file_key, tenant_id, chunk_size=1000, correlation_id=None):
    # Use Lambda's /tmp/ directory for temporary files
    download_path = '/tmp/2mb_data.json'

    try:
        # Download the file from S3
        s3_client.download_file(bucket_name, file_key, download_path)
        logger.info(f"Successfully downloaded {file_key} from bucket {bucket_name} to {download_path}")
    except Exception as e:
        logger.error(f"Error downloading file from S3: {str(e)}")
        return f"Error downloading file from S3: {str(e)}"

    # Load the JSON data
    try:
        with open(download_path, 'r') as file:
            data = json.load(file)
        logger.info("Successfully loaded JSON data from downloaded file")
    except Exception as e:
        logger.error(f"Error reading JSON data: {str(e)}")
        return f"Error reading JSON data: {str(e)}"

    # Check that the data is a list
    if not isinstance(data, list):
        error_msg = "The input data must be a list of records"
        logger.error(error_msg)
        return error_msg

    # Extract the file name without extension
    file_name = file_key.split('/')[-1].split('.')[0]

    # Create the base folder path for the split files
    base_folder = f'data/spilited_file/{tenant_id}/{correlation_id}'

    # Split the data into chunks
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Upload each chunk as a separate file
    chunk_keys = []
    for i, chunk in enumerate(chunks):
        chunk_key = f'{base_folder}/{file_name}_chunk_{i}.json'
        upload_path = f'/tmp/{file_name}_chunk_{i}.json'  # Using /tmp/ for Lambda writable directory
        with open(upload_path, 'w') as chunk_file:
            json.dump(chunk, chunk_file)
        try:
            s3_client.upload_file(upload_path, bucket_name, chunk_key)
            chunk_keys.append(chunk_key)
            
            # Insert file information into DynamoDB
            insert_response = insert_into_dynamodb(tenant_id, chunk_key, len(chunk), correlation_id, file_name)
            if insert_response is not True:
                return insert_response
        except Exception as e:
            logger.error(f"Error uploading chunk {chunk_key} to S3: {str(e)}")
            return f"Error uploading chunk {chunk_key} to S3: {str(e)}"

    return chunk_keys

def lambda_handler(event, context):
    bucket_name = event.get('bucket_name', 'event-driven-msc')
    tenant_id = event.get('tenant_id', 'tenant_one')
    file_key = event.get('file_key', f'large-data-set/{tenant_id}/2mb_data.json')
    chunk_size = event.get('chunk_size', 1000)
    
    # Generate correlation ID
    correlation_id = str(uuid.uuid4())

    logger.info(f"Starting split file process for tenant: {tenant_id}, file: {file_key}, chunk_size: {chunk_size}, correlation_id: {correlation_id}")

    chunk_keys = split_file(bucket_name, file_key, tenant_id, chunk_size, correlation_id)

    if isinstance(chunk_keys, list):
        return {
            'statusCode': 200,
            'chunk_keys': chunk_keys
        }
    else:
        return {
            'statusCode': 500,
            'errorMessage': chunk_keys  # Return the error message
        }
hrough the steps to achieve this: