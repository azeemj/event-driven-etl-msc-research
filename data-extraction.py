import json
import requests
import boto3
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Search Query
query = 'UK Economy'
tenant_id = 'tenant_two'
today_date = datetime.today().date()

s3_client = boto3.client('s3', region_name='us-east-1')

def lambda_handler(event, context):
    try:
        # Record start time
        start_time = time.time()
        
        # Intentionally cause an error to simulate an exception
        #raise Exception("Intentional error occurred")
        
        # Extract data from web scraping
        filepath = extract_data() #f'raw-data/{tenant_id}/{today_date}.json' 
        
        # Record end time
        end_time = time.time()
        
        # Calculate execution time
        execution_time = end_time - start_time
        
        # Log execution time
        logger.info(f"Execution time: {execution_time} seconds")
        
        # Return output
        output = {
            'statusCode': 200,
            'body': json.dumps({'filepath': filepath, 'tenant_id_job':f'{tenant_id}/{today_date}'})
        }
        
        return output
    
    except Exception as e:
        logger.error(f'Error: {e}')
        raise

def extract_data(): 
    # Encode special characters in the query
    query_encoded = encode_special_characters(query)
    
    # Construct the URL for web scraping
    url = f"https://news.google.com/search?q={query_encoded}&hl=en-US&gl=US&ceid=US%3Aen"
    
    # Send HTTP request to the URL
    response = requests.get(url)
    
    # Parse HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract articles and links
    articles = soup.find_all('article')
    links = [article.find('a')['href'].replace("./articles/", "https://news.google.com/articles/") for article in articles[:10]]
    
    # Extract news text and split
    news_text = [article.get_text(separator='\n') for article in articles]
    news_text_split = [text.split('\n') for text in news_text]
    
    # Write data to S3
    key = f'raw-data/{tenant_id}/{today_date}.json'
    s3_client.put_object(Bucket='event-driven-msc', Key=key, Body=json.dumps({'articles': news_text_split}))
    
    return key

def encode_special_characters(text):
    # Define special characters mapping
    special_characters = {'&': '%26', '=': '%3D', '+': '%2B', ' ': '%20'}
    
    # Encode special characters in the text
    encoded_text = ''.join(special_characters.get(char, char) for char in text.lower())
    
    return encoded_text
