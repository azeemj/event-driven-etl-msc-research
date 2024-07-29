import json
import boto3
import logging
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define bucket name
BUCKET_NAME = 'event-driven-msc'
STAGE = "RevertDataFromTransformationLambda"

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Extract key details from event
        execution_arn = event.get('executionArn')
        cause = json.loads(event["input_to_lambda"]["Cause"])
        error_message = json.loads(cause["errorMessage"])
        correlation_id = error_message["correlation_id"]
        tenant_id = error_message["tenant_id"]
        tenant_id_job = error_message['tenant_id_job']
        tenant_id, job_date = tenant_id_job.split('/')
        
        #get transformation file path
        transformed_folder_path = None
        
        
        if not all([correlation_id, tenant_id, execution_arn]):
            raise ValueError('Missing required parameters')

        #get raw data file path
        if 'raw_data_file_path' in error_message['raw_data_file_path']:
            logger.info(f"Attempting to delete objects in: {error_message['raw_data_file_path']}")
            delete_object_if_exists(BUCKET_NAME, error_message['raw_data_file_path'])
        
        #deleting transfrmed data
        if 'processed_data' in error_message['processed_data']:
            logger.info(f"Attempting to delete objects in: {error_message['processed_data']}")
            transformed_folder_path = error_message['processed_data']
            delete_object_if_exists(BUCKET_NAME, transformed_folder_path)
        
        

        # Update tracking information
        tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
        insert_into_dynamodb(tracking_table_name, correlation_id, STAGE, f"Cleaned {transformed_folder_path}", "Success", execution_arn)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully cleaned {transformed_folder_path}')
        }
        
    except Exception as e:
        logger.error(f'Error during processing: {e}')
        record_failure(event, str(e), execution_arn)
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def delete_object_if_exists(bucket_name, object_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"Deleted {object_key} from {bucket_name}")
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info(f"Object {object_key} does not exist in {bucket_name}")
        else:
            raise
        
def insert_into_dynamodb(table_name, correlation_id, stage, message, status, execution_arn):
    # Insert or update tracking information in DynamoDB
    dynamodb_client.put_item(
        TableName=table_name,
        Item={
            "CorrelationId": {"S": correlation_id},
            "Stage": {"S": stage},
            "TrackingInfo": {"S": message},
            "Status": {"S": status},
            "ExecutionArn": {"S": execution_arn},
            "Timestamp": {"S": datetime.utcnow().isoformat()}
        }
    )
    logger.info(f"Recorded in DynamoDB: {message}")

def record_failure(event, error_message, execution_arn):
    logger.error(f"Recording failure: {error_message}")
    # Extract failure details and record them in DynamoDB
    insert_into_dynamodb(f'ETLDemoTrackingTable{tenant_id}', correlation_id, STAGE, f"Failure: {error_message}", "Failed", execution_arn)

