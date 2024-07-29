import json
import boto3
import logging

# Initialize AWS clients
dynamodb_client = boto3.client('dynamodb')

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define stage
stage = "RevertDataFromLoadErrorLambda"

BUCKET_NAME = 'event-driven-msc'

def lambda_handler(event, context):
    
    tenant_id = None
    
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        execution_arn = event.get('executionArn')
        # Extract the Cause field, which is a JSON string
        cause_str = event["input_to_lambda"]["Cause"]
        
        # Extract the errorMessage field, which is a nested JSON string
        error_message_str = cause_str["errorMessage"]
        # Access the correlation_id
        # Parse the errorMessage JSON string
        error_message = json.loads(error_message_str)
        
        tenant_id_job = error_message['tenant_id_job']
        tenant_id, job_date = tenant_id_job.split('/')
        
        tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
    
        # Access the correlation_id
        correlation_id = error_message["correlation_id"]
        logger.info(f"Correlation ID: {correlation_id}")
        transformed_folder_path = None
        raw_data_file_path = None
        
        if not all([correlation_id, cause_str, execution_arn]):
            raise ValueError('Required missing parameters')
        
    
            try:
                #get raw data file path
                if 'raw_data_file_path' in error_message['raw_data_file_path']:
                    logger.info(f"Attempting to delete objects in: {error_message['raw_data_file_path']}")
                    delete_object_if_exists(BUCKET_NAME, error_message['raw_data_file_path'])
                
                #deleting transfrmed data
                if 'processed_data' in error_message['processed_data']:
                    logger.info(f"Attempting to delete objects in: {error_message['processed_data']}")
                    transformed_folder_path = error_message['processed_data']
                    delete_object_if_exists(BUCKET_NAME, transformed_folder_path)
        
                
                insert_into_dynamDB(tracking_table_name, correlation_id, stage,
                f'Successfully deleted from s3 transformed folder', "Failed", execution_arn)
        
                return {
                    'statusCode': 200,'tenant_id':tenant_id,
                    'body': json.dumps(f'Successfully deleted items with correlation_id: {correlation_id}')
                }
        
            except Exception as e:
                logger.error(f'Error: {e}')
                return {
                    'statusCode': 500,'tenant_id':tenant_id,
                    'body': json.dumps(f'Error: {str(e)}')
                }
            
    except Exception as e:
        logger.error(f'Error: {e}')
        return {
            'statusCode': 500,'tenant_id':tenant_id,
            'body': json.dumps(f'Error: {str(e)}')
        }


def insert_into_dynamodb(tracking_table_name, correlation_id, stage, tracking_file, status, execution_arn):
    try:
        response = dynamodb_client.put_item(
            TableName=tracking_table_name,
            Item={
                "CorrelationId": {"S": correlation_id},
                "Stage": {"S": stage},
                "TrackingInfo": {"S": tracking_file},
                "Status": {"S": status},
                'arn': {'S': execution_arn},
                'timestamp': {'S': datetime.utcnow().isoformat()}
            }
        )
    except Exception as e:
        logger.info(f'insert_into_dynamodb exception{e}')


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