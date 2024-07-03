import json
import boto3
import logging
import uuid
from datetime import datetime
import csv
from datetime import datetime
from io import StringIO

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
Bucket = 'event-driven-msc'
stage = "Loading"

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    # Declare global variables
    global tenant_id_job
    global correlation_id
    global tracking_table_name
    
    try:
        # Extract input parameters
        logger.info(f'Event: {event}')
        
        filename_data = json.loads(event['input_to_lambda']['body'])
        filename = filename_data['filepath'] #"transformed-data/processed-data/tenant_one/2024-06-18.csv" 
        tenant_id_job = filename_data['tenant_id_job'] #"tenant_one/2024-06-18" 
        correlation_id = filename_data['correlation_id'] #"9edb03f8-3082-491a-895e-8aa35022b973" 
        bucket_name = Bucket
        key = tenant_id_job
        tracking_info = filename
        logger.info(f'correlation_id: {correlation_id}')

        # Load data from S3
        final_data_key = filename
        logger.info(f'Source filename: {filename}')
        logger.info(f'final_data_key: {final_data_key}')
        
        # Get tenant ID and create table in runtime
        tenant_id = tenant_id_job.split('/')
        tracking_table_name = f'ETLDemoTrackingTable{tenant_id[0]}'
        dynamodb_table_name = f'ETLData{tenant_id[0]}'
        
        # Create tracking table if it does not exist
        create_dynamodb_table_if_not_exists(tracking_table_name)
        
        # Create data table if it does not exist
        create_dynamodb_table_if_not_exists(dynamodb_table_name, data_table=True)
        # Process CSV file from S3 and insert data into DynamoDB
        process_csv_and_insert_to_dynamodb(Bucket, final_data_key, dynamodb_table_name, correlation_id)

        
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        # Insert tracking information into DynamoDB
        insert_into_dynamDB(tracking_table_name, correlation_id, stage, filename, "Sucess")
        
        logger.info(f"Data loaded into S3 and tracking information inserted into DynamoDB for key: {key}")

        # Return output
        output = {
            'statusCode': 200,
            'body': json.dumps({'filepath': final_data_key})
        }
        
        return output

    except Exception as e:
        # Log and return error response
        logger.info(f'Error: {e}')
        error_output = {
            'error_message': str(e),
            'tenant_id_job': f'{tenant_id_job}',
            'correlation_id': correlation_id
        }
        
        # Create tracking table if it does not exist
        create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        # Insert tracking information into DynamoDB
        insert_into_dynamDB(tracking_table_name, correlation_id, stage, "Unknown", "Failed")
        
        raise Exception(json.dumps(error_output))

def create_dynamodb_table_if_not_exists(table_name, data_table=False):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        key_schema = [
            {'AttributeName': 'Key', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'Stage', 'KeyType': 'RANGE'}  # Sort key
        ]
        attribute_definitions = [
            {'AttributeName': 'Key', 'AttributeType': 'S'},
            {'AttributeName': 'Stage', 'AttributeType': 'S'}
        ]
        if data_table:
            key_schema = [{'AttributeName': 'Key', 'KeyType': 'HASH'}]  # Only partition key for data table
            attribute_definitions = [{'AttributeName': 'Key', 'AttributeType': 'S'}]
        
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)

def insert_into_dynamDB(tracking_table_name, correlation_id, stage, tracking_file, status):
    response  = dynamodb_client.put_item(
        TableName=tracking_table_name,
        Item={
            "Key": {"S": correlation_id},
            "Stage": {"S": stage},
            "TrackingInfo": {"S": tracking_file},
            "Timestamp": {"S": datetime.utcnow().isoformat()},
            "Status": {"S": status}
        }
    )
    
    
def process_csv_and_insert_to_dynamodb(bucket_name, key, table_name, correlation_id):
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    csv_content = response['Body'].read().decode('utf-8')
    csv_reader = csv.DictReader(StringIO(csv_content))

    items = []
    for row in csv_reader:
        item = {k: {'S': v} for k, v in row.items() if v}
        item['Key'] = {'S': str(uuid.uuid4())}
        item['CorrelationId'] = {'S': correlation_id}
        items.append({'PutRequest': {'Item': item}})
        
        if len(items) == 25:
            dynamodb_client.batch_write_item(RequestItems={table_name: items})
            logger.info(f'Inserted batch of 25 items into {table_name}')
            items = []
    
    if items:
        dynamodb_client.batch_write_item(RequestItems={table_name: items})
        logger.info(f'Inserted final batch of {len(items)} items into {table_name}')
    
    # Add a status entry indicating completion
    insert_status_entry(table_name, correlation_id)

def insert_status_entry(table_name, correlation_id):
    status_item = {
        "Key": {"S": str(uuid.uuid4())},
        "CorrelationId": {"S": correlation_id},
        "Status": {"S": "Completed"},
        "Timestamp": {"S": datetime.utcnow().isoformat()}
    }
    dynamodb_client.put_item(
        TableName=table_name,
        Item=status_item
    )
    logger.info(f'Status entry inserted into {table_name}')