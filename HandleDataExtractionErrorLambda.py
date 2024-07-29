import json
import logging
import boto3
from datetime import datetime

# Configure logging and others
logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
stage = "HandleDataExtractionErrorLambda"
BUCKET_NAME = 'event-driven-msc'

def lambda_handler(event, context):
    
    extracted_file_path = None
    tenant_id = None
    
    try:
        # Log the incoming event
        logger.info(f"Received event: {json.dumps(event)}")
        execution_arn = event.get('executionArn')
        # Extract the Cause field, which is a JSON string
        cause_str = event["input_to_lambda"]["Cause"]

        # Parse the JSON string in Cause
        cause = json.loads(cause_str)

        # Extract the errorMessage field, which is a nested JSON string
        error_message_str = cause["errorMessage"]

        # Parse the errorMessage JSON string
        error_message = json.loads(error_message_str)

        # Access the correlation_id
        correlation_id = error_message["correlation_id"]
        logger.info(f"Correlation ID: {correlation_id}")
        
        if "raw_data_file_path" in error_message:
            extracted_file_path = error_message["raw_data_file_path"]
        
        tenant_id, job_date = error_message['tenant_id_job'].split('/')
        
        # Log the error details
        error_details = event.get('Error', 'Unknown error')
        logger.error(f"Error occurred in DataExtraction-Lambda: {error_details}")
        logger.error(f"Cause: {cause_str}")
        
        try:
            extracted_file_deletion = False
            # List and delete one object in the folder
            logger.info(f"extracted_file_path: {extracted_file_path}")
            if extracted_file_path is not None:
                delete_object_if_exists(BUCKET_NAME, extracted_file_path)
                extracted_file_deletion = True
            
            #tracking information
            msg = f"HandleDataExtractionErrorLambda-File deletion{extracted_file_deletion}"
            logger.info(f"HandleDataExtractionErrorLambda-File deletion : {msg}")
            
            tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
            insert_into_dynamDB(tracking_table_name, correlation_id, stage, extracted_file_path, msg, execution_arn)
    
            # Return output
            output = {
                'statusCode': 200,'tenant_id':tenant_id,
                'body': json.dumps({'message': 'Error handled successfully'})
            }
    
            return output
            
        except Exception as e:
            return {
                    'statusCode': 500,'tenant_id':tenant_id,
                    'body': json.dumps(f'Error: {str(e)}')
                }
    except Exception as e:
        return {
                'statusCode': 500,'tenant_id':tenant_id,
                'body': json.dumps(f'Error: {str(e)}')
            }

def insert_into_dynamDB(tracking_table_name, correlation_id, stage, tracking_file, status, execution_arn):
    logger.error(f"insert_into_dynamDB in HandleDataExtractionErrorLambda: correlation_id {correlation_id} -{tracking_table_name}")
    logger.error(f"insert_into_dynamDB in HandleDataExtractionErrorLambda: execution_arn {execution_arn} -{tracking_file}")
    response  = dynamodb_client.put_item(
        TableName=tracking_table_name,
        Item={
            "CorrelationId": {"S": correlation_id},
            "Stage": {"S": stage},
            "TrackingInfo": {"S": tracking_file},
            "Timestamp": {"S": datetime.utcnow().isoformat()},
            "Status": {"S": status},
            'arn': {'S': execution_arn},
            'timestamp': {'S': datetime.utcnow().isoformat()}
        }
    )
    
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error(f"Failed to insert record into DynamoDB: {response}")
        raise Exception("Failed to insert record into DynamoDB")
        
    return True
    
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

