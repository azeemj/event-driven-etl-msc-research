import json
import pandas as pd
import boto3
import logging
import time
# Initialize S3 client and bucket name
s3_client = boto3.client('s3')
Bucket = 'event-driven-msc'

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Record start time
        start_time = time.time()
        # Extract file path from event
        event = {'sampleKey1': 'sampleValue1', 'key3': 100, 
        'input_to_lambda': {'statusCode': 200, 'body': '{"filepath": "raw-data/tenant_one/2024-05-11.json"}'}}
        filename_data = json.loads(event['input_to_lambda']['body'])
        filename = filename_data['filepath']
        print('event', event)
        # Perform transformation
        transformation(event, filename)
        
        # Record end time
        end_time = time.time()
        
        # Calculate execution time
        execution_time = end_time - start_time
        
        # Log execution time
        logger.info(f"Execution time: {execution_time} seconds")
        
        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps('Transformation completed successfully!')
        }
    except Exception as e:
        # Log and return error response
        print('Error:', e)
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred during transformation.')
        }

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
                    'Title': headline[2] if len(headline[1]) > 0 else 'Missing',
                    'Source': headline[0] if len(headline[0]) > 0 else 'Missing',
                    'Time': headline[3] if len(headline[3]) > 0 else 'Missing',
                    'Author': headline[4].split('By ')[-1] if len(headline [4]) > 0 else 'Missing',
                    'Link': headline[5] if len(headline [5]) > 0 else 'Missing'
                })
        
        # Convert to DataFrame
        news_df = pd.DataFrame(news_data)
        
        # Convert DataFrame to CSV format
        csv_content = news_df.to_csv(index=False)
        
        # Write CSV content to S3
        csv_key = filename.replace('.json', '.csv')
        s3_client.put_object(Bucket=Bucket, Key=f"transformed-data/{csv_key}", Body=csv_content.encode('utf-8'))
        
        print('Transformation completed.')
    except Exception as e:
        # Log and raise exception
        print('Error during transformation:', e)
        raise e
