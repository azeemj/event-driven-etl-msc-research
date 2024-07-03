import json
import boto3
import logging
import uuid
from datetime import datetime
import time

# Initialize AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
dynamodb_client = boto3.client('dynamodb')
Bucket = 'event-driven-msc'
stage = "Loading"

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f'Event: {event}')

    filename_data = event['input_to_lambda']
    filename = filename_data['filepath']
    tenant_id_job = filename_data['tenant_id_job']
    correlation_id = filename_data['correlation_id']
    
    tenant_id = tenant_id_job.split('/')[0]
    tracking_table_name = f'ETLDemoTrackingTable{tenant_id}'
    athena_table_name = f'ETLDataDemo{tenant_id}'
    athena_database_name = 'ETLMonitoring'
    
    try:
        bucket_name = Bucket
        key = tenant_id_job
        tracking_info = filename
        logger.info(f'correlation_id: {correlation_id}')

        final_data_key = filename
        logger.info(f'Source filename: {filename}')
        logger.info(f'final_data_key: {final_data_key}')
        
        create_dynamodb_table_if_not_exists(tracking_table_name)
        
        create_athena_database(athena_database_name)
        process_csv_and_insert_to_athena(Bucket, final_data_key, athena_database_name, athena_table_name, correlation_id)

        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        insert_into_dynamodb(tracking_table_name, correlation_id, stage, filename, "Success")
        
        logger.info(f"Data loaded into S3 and tracking information inserted into DynamoDB for key: {key}")

        output = {
            'statusCode': 200,
            'filepath': final_data_key
        }
        
        return output

    except Exception as e:
        logger.error(f'Error: {e}')
        error_output = {
            'error_message': str(e),
            'tenant_id_job': tenant_id_job,
            'correlation_id': correlation_id
        }
        
        create_dynamodb_table_if_not_exists(tracking_table_name)
        logger.info(f'Inserting record with Key: {correlation_id} and Stage: {stage} into {tracking_table_name}')
        insert_into_dynamodb(tracking_table_name, correlation_id, stage, "Unknown", "Failed")
        
        raise Exception(json.dumps(error_output))

def create_athena_database(database_name):
    s3_output_location = f's3://{Bucket}/athena-output/'
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    start_query_execution_response = athena_client.start_query_execution(
        QueryString=create_database_query,
        ResultConfiguration={'OutputLocation': s3_output_location}
    )

    query_execution_id = start_query_execution_response['QueryExecutionId']
    logger.info(f'Started Athena query execution for database creation with ID: {query_execution_id}')

    # Polling for query execution completion
    status = 'RUNNING'
    while status in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['FAILED', 'CANCELLED']:
            raise Exception(f'Athena query failed or was cancelled: {response["QueryExecution"]["Status"]["StateChangeReason"]}')
        logger.info(f'Athena query status: {status}')
        time.sleep(5)

    logger.info(f'Athena database {database_name} created or already exists.')

def process_csv_and_insert_to_athena(bucket_name, key, database_name, table_name, correlation_id):
    processed_key_prefix = key.rsplit('/', 1)[0] + "/"
    logger.info(f'process_csv_and_insert_to_athena database {processed_key_prefix} -{database_name} created or already exists.')
    create_athena_table(bucket_name, processed_key_prefix, database_name, table_name)
    return processed_key_prefix

def create_athena_table(bucket_name, key_prefix, database_name, table_name):
    s3_output_location = f's3://{bucket_name}/athena-output/'
    data_files_location = f's3://{bucket_name}/{key_prefix}'

    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
        `Key` string,
        `Title` string,
        `Source` string,
        `Time` string,
        `Author` string,
        `Link` string,
        `CorrelationId` string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
        'serialization.format' = ',',
        'field.delim' = ','
    ) LOCATION '{data_files_location}'
    TBLPROPERTIES ('has_encrypted_data'='false');
    """

    start_query_execution_response = athena_client.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': s3_output_location}
    )

    query_execution_id = start_query_execution_response['QueryExecutionId']
    logger.info(f'Started Athena query execution for table creation with ID: {query_execution_id}')

    # Polling for query execution completion
    status = 'RUNNING'
    while status in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['FAILED', 'CANCELLED']:
            raise Exception(f'Athena query failed or was cancelled: {response["QueryExecution"]["Status"]["StateChangeReason"]}')
        logger.info(f'Athena query status: {status}')
        time.sleep(5)

    logger.info(f'Athena table {database_name}.{table_name} created or updated successfully.')

def create_dynamodb_table_if_not_exists(table_name, data_table=False):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        key_schema = [
            {'AttributeName': 'Key', 'KeyType': 'HASH'},
            {'AttributeName': 'Stage', 'KeyType': 'RANGE'}
        ]
        attribute_definitions = [
            {'AttributeName': 'Key', 'AttributeType': 'S'},
            {'AttributeName': 'Stage', 'AttributeType': 'S'}
        ]
        if data_table:
            key_schema = [{'AttributeName': 'Key', 'KeyType': 'HASH'}]
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

def insert_into_dynamodb(tracking_table_name, correlation_id, stage, tracking_file, status):
    try:
        response = dynamodb_client.put_item(
            TableName=tracking_table_name,
            Item={
                "Key": {"S": correlation_id},
                "Stage": {"S": stage},
                "TrackingInfo": {"S": tracking_file},
                "Timestamp": {"S": datetime.utcnow().isoformat()},
                "Status": {"S": status}
            }
        )
    except Exception as e:
        logger.info(f'insert_into_dynamodb exception{e}')