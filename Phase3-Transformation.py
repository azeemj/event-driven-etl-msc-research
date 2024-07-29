import boto3
import json
from datetime import datetime
import logging
import time
import pandas as pd
import uuid

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3', region_name='us-east-1')
dynamodb_client = boto3.client('dynamodb')
stage = "Transformation"
#tenant_id = 'tenant_one'
today_date = datetime.today().date()
Bucket = 'event-driven-msc'


def lambda_handler(event, context):
    logger.info(f'Event: {event}')
    logger.info(f"correlation_id: {event.get('correlation_id')}")
    chunk_key = event.get('chunk_key')
    correlation_id = event.get('correlation_id')
    tenant_id = event.get('tenant_id')
    execution_arn = event.get('executionArn')
    
    #raw file data 
    raw_data_file_path = event.get('raw_data_file_path', None)
    filename = raw_data_file_path
    
    processed_data = None
    tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
    
    if not all([chunk_key, correlation_id, filename, execution_arn]):
        raise ValueError('Required missing parameters')
    
    try:
        # Record start time
        start_time = time.time()
        
        # Copy the S3 data to a backup location
        copy_source = {'Bucket': Bucket, 'Key': filename}
        replaced_key = filename.replace('raw-data', 'backup-data')
        backup_key = f'transformed-data/{replaced_key}'
        
        # Process the chunk
        s3_client.copy_object(CopySource=copy_source, Bucket=Bucket, Key= backup_key)
        logger.info(f'Backup of {filename} created at {backup_key}')
        
        # Perform transformation
        processed_data = transformation(event, filename, correlation_id)
        
        
        # Create tracking table if it does not exist
        create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        # Insert tracking information into DynamoDB
        insert_into_dynamDB(tracking_table_name, correlation_id, stage, filename, "Sucess", execution_arn)
      
        # Return output
        output = {
            'statusCode': 200,
            'tenant_id_job': f'{tenant_id}/{today_date}',
            'correlation_id': correlation_id,
            'chunk_key': chunk_key,
            'processed_data': processed_data,
            'tenant_id': tenant_id,
            'raw_data_file_path': raw_data_file_path
            
        }
        
        # Record end time
        end_time = time.time()
        # Calculate execution time
        execution_time = end_time - start_time
         # Log execution time
        logger.info(f"Execution time: {execution_time} seconds")
       
        return output
    
    except Exception as e:
         # Log and return error response
        logger.info(f'Error:{e}')
        print('test')
        error_output = {
            'error_message': str(e),
            'tenant_id_job': f'{tenant_id}/{today_date}',
            'correlation_id': correlation_id,
            'tenant_id': tenant_id,
            'raw_data_file_path': raw_data_file_path,
            'processed_data': processed_data
        }
        # Create tracking table if it does not exist
        create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        # Insert tracking information into DynamoDB
        insert_into_dynamDB(tracking_table_name, correlation_id, stage, "Unknown", "Failed", execution_arn)
        
        raise Exception(json.dumps(error_output))

def transformation(event, filename, correlation_id):
    try:
        # Get JSON content from S3
        response = s3_client.get_object(Bucket=Bucket, Key=filename)
        json_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON content
        data = json.loads(json_content)
        #raise Exception("Intentional exception")
        # Extract relevant data and transform
        news_data = []
        for headline in data:
            if len(headline) > 5:
                news_data.append({
                    'Title': headline[2] if len(headline[2]) > 0 else 'Missing',
                    'Source': headline[0] if len(headline[0]) > 0 else 'Missing',
                    'Time': headline[3] if len(headline[3]) > 0 else 'Missing',
                    'Author': headline[4].split('By ')[-1] if len(headline[4]) > 0 else 'Missing',
                    'Link': headline[5] if len(headline[5]) > 0 else 'Missing',
                    'CorrelationId': correlation_id,
                    'Key' : str(uuid.uuid4())
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
        logger.info(f"Table {table_name} does not exist. Creating...")
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'CorrelationId', 'KeyType': 'HASH'},
                {'AttributeName': 'Stage', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'CorrelationId', 'AttributeType': 'S'},
                {'AttributeName': 'Stage', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Wait until the table exists
        dynamodb_client.get_waiter('table_exists').wait(TableName=table_name)
        logger.info(f"Table {table_name} created.")

def insert_into_dynamDB(table_name, correlation_id, stage, tracking_file, status, execution_arn):
    if not correlation_id or not stage:
        raise ValueError(f'Missing required keys: correlation_id={correlation_id}, stage={stage}', execution_arn)
        
    item = {
        "CorrelationId": {"S": correlation_id},
        "Stage": {"S": stage},
        "TrackingInfo": {"S": tracking_file},
        "Status": {"S": status},
        'arn': {'S': execution_arn},
        'timestamp': {'S': datetime.utcnow().isoformat()}
    }
    logger.info(f"Inserting item into DynamoDB: {item}")
    dynamodb_client.put_item(TableName=table_name, Item=item)
