import json
import requests
import boto3
from bs4 import BeautifulSoup

s3_client = boto3.client('s3',  region_name='us-east-1')
dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')

def lambda_handler(event, context):
    print("print in ")
    # URL of the website to scrape
    url = "https://news.google.com/search?q=uk&hl=en-CA&gl=CA&ceid=CA%3Aen"
    
    try:
        # Send HTTP GET request to the URL
        response = requests.get(url)
        print('test', response)
        # Check if request was successful
        if response.status_code == 200:
            # Parse HTML content using BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            # Extract desired data from the HTML
            headlines = []
            for headline in soup.find_all('h3'):
                print('test--two', headline.text)
                headlines.append(headline.text)
            # Store scraped data in S3
            s3_client.put_object(Bucket='s3://event-driven-msc/data-extraction/', Key='scraped_data.json', Body=json.dumps({'headlines': headlines}))
            # Store tracking information in DynamoDB
            dynamodb_client.put_item(
                TableName='data-extraction-us',
                Item={
                    'job_id': {'S': event['job_id']},
                    'status': {'S': 'completed'},
                    'timestamp': {'S': str(datetime.now())}
                }
            )
            # Return the scraped data
            return {
                'statusCode': 200,
                'body': json.dumps({'headlines': headlines})
            }
        else:
            return {
                'statusCode': response.status_code,
                'body': "Failed to fetch data from the website"
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
