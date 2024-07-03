import json
import pandas as pd
import boto3
import logging
import time
from datetime import datetime

# Initialize S3 client and bucket name
s3_client = boto3.client('s3')
Bucket = 'event-driven-msc'
dynamodb_client = boto3.client('dynamodb')

stage = "Transformation" 

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Declare global variables
    global tenant_id_job
    global correlation_id
    global tracking_table_name
    
    try:
        # Record start time
        start_time = time.time()
        logger.info(f'Event: {event}')
        
        # Extract file path from event
        filename_data = json.loads(event['input_to_lambda']['Payload']['body'])
        filename = filename_data['filepath']
        tenant_id_job = filename_data['tenant_id_job']
        correlation_id = filename_data['correlation_id']
        
        logger.info(f'filename: {filename}')
        logger.info(f'correlation_id: {correlation_id}')
        
        # Copy the S3 data to a backup location
        copy_source = {'Bucket': Bucket, 'Key': filename}
        replaced_key = filename.replace('raw-data', 'backup-data')
        backup_key = f'transformed-data/{replaced_key}'
        
        tenant_id = tenant_id_job.split('/')
        tracking_table_name = f'ETLDemoTrackingTable{tenant_id[0]}'
        
        s3_client.copy_object(CopySource=copy_source, Bucket=Bucket, Key= backup_key)
        logger.info(f'Backup of {filename} created at {backup_key}')
        
        # Perform transformation
        processed_data = transformation(event, filename)
        
        # Record end time
        end_time = time.time()
        
        # Calculate execution time
        execution_time = end_time - start_time
        
       
       
        # Create tracking table if it does not exist
        create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        # Insert tracking information into DynamoDB
        insert_into_dynamDB(tracking_table_name, correlation_id, stage, filename, "Sucess")
        
        # Log execution time
        logger.info(f"Execution time: {execution_time} seconds")
        
        # Return output
        output = {
            'statusCode': 200,
            'body': json.dumps({'filepath': processed_data, 'tenant_id_job': tenant_id_job, 'correlation_id': correlation_id})
        }
        
        return output
    except Exception as e:
        # Log and return error response
        logger.info(f'Error:{e}')
        print('test')
        error_output = {
            'error_message': str(e),
            'tenant_id_job': tenant_id_job,
            'correlation_id': correlation_id
        }
        # Create tracking table if it does not exist
        create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        # Insert tracking information into DynamoDB
        insert_into_dynamDB(tracking_table_name, correlation_id, stage, "Unknown", "Failed")
        
        raise Exception(json.dumps(error_output))
        
def transformation(event, filename):
    try:
        # Get JSON content from S3
        response = s3_client.get_object(Bucket=Bucket, Key=filename)
        json_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON content
        data = json.loads(json_content)
        
        # Extract relevant data and transform
        news_data = []
        for headline in data["articles"]:
            if len(headline) > 5:
                news_data.append({
                    'Title': headline[2] if len(headline[2]) > 0 else 'Missing',
                    'Source': headline[0] if len(headline[0]) > 0 else 'Missing',
                    'Time': headline[3] if len(headline[3]) > 0 else 'Missing',
                    'Author': headline[4].split('By ')[-1] if len(headline[4]) > 0 else 'Missing',
                    'Link': headline[5] if len(headline[5]) > 0 else 'Missing'
                })
        
        # Convert to DataFrame
        news_df = pd.DataFrame(news_data)
        
        # Convert DataFrame to CSV format
        csv_content = news_df.to_csv(index=False)
        
        # Write CSV content to S3
        csv_key = filename.replace('.json', '.csv')
        replaced_key = csv_key.replace('raw-data', 'processed-data')
       
        s3_client.put_object(Bucket=Bucket, Key=f"transformed-data/{replaced_key}", Body=csv_content.encode('utf-8'))
        
        logger.info(f'Transformation completed.')
        return f"transformed-data/{replaced_key}"
    except Exception as e:
        # Log and raise exception
        logger.info(f'Error during transformation:{e}')
        raise e

def create_dynamodb_table_if_not_exists(table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'Key', 'KeyType': 'HASH'},  # Partition key
                {'AttributeName': 'Stage', 'KeyType': 'RANGE'}  # Sort key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'Key', 'AttributeType': 'S'},
                {'AttributeName': 'Stage', 'AttributeType': 'S'}
            ],
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