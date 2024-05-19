import json
import boto3
import logging
import uuid

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
Bucket = 'event-driven-msc'

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Extract input parameters
        logger.info(f'Event: {event}')
        
        filename_data = json.loads(event['input_to_lambda']['body'])
        filename = filename_data['filepath'] #"transformed-data/processed-data/tenant_one/2024-05-17.csv" #
        tenant_id_job = filename_data['tenant_id_job'] #"tenant_one/2024-05-17" #
        bucket_name = Bucket
        key = tenant_id_job #event['Key']
        tracking_info = filename #event['TrackingInfo']

        # Load data from S3
        #response = s3_client.get_object(Bucket=bucket_name, Key=key)
        #data = response['Body'].read().decode('utf-8')
        final_data_key = filename.replace('transformed-data/processed-data', 'final-data')
        #final_data_key = f'final-data/{replaced_key}'
        logger.info(f'Source filname: {filename}')
        logger.info(f'final_data_key: {final_data_key}')
        #print(f'Source filname: {filename}')
        #print(f'final_data_key: {final_data_key}')
        
        #s3_client.copy_object(CopySource=filename, Bucket=Bucket, Key= final_data_key)
        #logger.info(f'Copy of {filename} created at {final_data_key}')
        
        #get tennat id and create table in runtime
        tenant_id = tenant_id_job.split('/')
        table_name = f'TrackingTable{tenant_id[0]}'
        
         # Create table if it does not exist
        create_dynamodb_table_if_not_exists(table_name)

        # Insert tracking information into DynamoDB
        dynamodb_client.put_item(
            TableName= table_name,
            Item={
                'Key': {'S': str(uuid.uuid4())},
                'Stage': {'S':"Loading"},
                'TrackingInfo': {'S': filename}
            }
        )

        #logger.info(f"Data loaded from S3 and tracking information inserted into DynamoDB for key: {key}")

        # Return output
        output = {
            'statusCode': 200,
            'body': json.dumps({'filepath': 'filepath'})
        }
        
        return output

    except Exception as e:
        logger.error(f'Error: {e}')
        '''
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred during data loading and tracking.')
        }
        '''
        raise
    
def create_dynamodb_table_if_not_exists(table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'Key', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'Key', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
