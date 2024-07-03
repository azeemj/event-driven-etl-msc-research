import boto3
import csv
import os
import uuid
from faker import Faker
from io import StringIO

# Initialize the S3 client
s3_client = boto3.client('s3')

# Initialize the Faker library
fake = Faker()

# Define the S3 bucket name and file size
BUCKET_NAME = 'your-s3-bucket-name'
CSV_COLUMNS = ['id', 'name', 'email', 'date_of_birth', 'address']
TARGET_FILE_SIZE_MB = 100  # Change to 1 for 1 MB file
FILE_PREFIX = 'generated-data'

def generate_csv_data(num_records):
    """
    Generate CSV data with the specified number of records.
    """
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=CSV_COLUMNS)
    writer.writeheader()

    for _ in range(num_records):
        writer.writerow({
            'id': str(uuid.uuid4()),
            'name': fake.name(),
            'email': fake.email(),
            'date_of_birth': fake.date_of_birth().isoformat(),
            'address': fake.address()
        })

    return csv_buffer.getvalue()

def lambda_handler(event, context):
    # Calculate the number of records needed to reach the target file size
    record_size = 200  # Approximate size of one record in bytes
    num_records = (TARGET_FILE_SIZE_MB * 1024 * 1024) // record_size

    # Generate CSV data
    csv_data = generate_csv_data(num_records)

    # Define the file name and path
    file_name = f'{FILE_PREFIX}-{TARGET_FILE_SIZE_MB}MB.csv'
    file_path = f'/tmp/{file_name}'

    # Write CSV data to a temporary file
    with open(file_path, 'w') as file:
        file.write(csv_data)

    # Upload the file to S3
    s3_client.upload_file(file_path, BUCKET_NAME, file_name)

    return {
        'statusCode': 200,
        'body': f'Successfully generated and uploaded {TARGET_FILE_SIZE_MB}MB CSV file to S3'
    }
