import json
import boto3
import logging

# Initialize AWS clients
s3_client = boto3.client('s3')
Bucket = 'event-driven-msc'

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Extract input parameters
        logger.info(f'Event: {event}')
        
        # Extract filepath from the event payload
        filename_data = event['input_to_lambda']['Payload']['body']
        filename_data = json.loads(filename_data)
        source_key = filename_data['filepath']  # e.g., "transformed-data/processed-data/tenant_one/2024-06-18.csv"
        
        # Define the destination key
        destination_key = source_key.replace("transformed-data/processed-data", "load-data")
        
        # Copy the file from the source key to the destination key
        copy_source = {'Bucket': Bucket, 'Key': source_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=Bucket, Key=destination_key)
        
        logger.info(f"Copied file from {source_key} to {destination_key}")

        # Return output
        output = {
            'statusCode': 200,
            'body': json.dumps({'source': source_key, 'destination': destination_key})
        }
        
        return output

    except Exception as e:
        logger.error(f'Error: {e}')
        error_output = {
            'statusCode': 500,
            'error_message': str(e),
        }
        raise Exception(json.dumps(error_output))
