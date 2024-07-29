import json
import aioboto3
import logging
import asyncio

# Constants
Bucket = 'event-driven-msc'

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Asynchronous Lambda Handler
def lambda_handler(event, context):
    # Create a new event loop for this handler invocation
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(handle_event(event, context))

async def handle_event(event, context):
    try:
        # Extract input parameters
        logger.info(f'Event: {event}')
        
        # Extract filepath from the event payload
        filename_data = event['input_to_lambda']
        source_key = filename_data['transformed_file_path']  # e.g., "transformed-data/processed-data/tenant_one/2024-06-18.csv"
        tenant_id = filename_data['tenant_id']
        # Define the destination key
        destination_key = source_key.replace("transformed-data/processed-data", "final-backup-data")
        
        # Copy the file from the source key to the destination key
        copy_source = {'Bucket': Bucket, 'Key': source_key}
        
        # Create an aioboto3 session
        session = aioboto3.Session()
        async with session.client('s3') as s3_client:
            await s3_client.copy_object(CopySource=copy_source, Bucket=Bucket, Key=destination_key)
        
        logger.info(f"Copied file from {source_key} to {destination_key}")

        # Return output
        output = {
            'statusCode': 200,
            'source': source_key,
            'destination': destination_key,
            'tenant_id': tenant_id
        }
        
        return output

    except Exception as e:
        logger.error(f'Error: {e}')
        error_output = {
            'statusCode': 500,
            'error_message': str(e),
            'tenant_id': tenant_id
        }
        raise Exception(json.dumps(error_output))
