import boto3
import time
from datetime import datetime, timedelta
import json
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('logs')

def lambda_handler(event, context):
    tenant_id = event.get('tenant_id')
    execution_id = context.aws_request_id
    log_group_name = '/aws/vendedlogs/states/MyStateMachine-phase3-ETL-datapipeline'

    # Define the log group name and Insights query
    query = """
    fields @timestamp, @message, details.name, type
    | filter type like /TaskStateEntered|TaskStateExited/
    | sort @timestamp asc
    """

    # Define the start and end times for the query (e.g., last 5 minutes)
    end_time = int(time.time() * 1000)
    start_time = end_time - 35 * 60 * 1000

    # Start the query
    response = client.start_query(
        logGroupName=log_group_name,
        startTime=start_time,
        endTime=end_time,
        queryString=query,
    )
    query_id = response['queryId']

    # Wait for the query to complete
    while True:
        response = client.get_query_results(queryId=query_id)
        if response['status'] == 'Complete':
            break
        elif response['status'] == 'Failed' or response['status'] == 'Cancelled':
            raise Exception(f"Query failed or was cancelled: {response}")

        time.sleep(1)

    # Process the query results
    results = response['results']
    log_entries = []

    for result in results:
        entry = {field['field']: field['value'] for field in result}
        log_entries.append(entry)

    # Helper function to convert timestamp to datetime
    def to_datetime(timestamp):
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

    # Initialize a dictionary to store execution times for each stage
    execution_times = {}

    # Loop through the logs and calculate the execution time for each stage
    for log in log_entries:
        event_type = log.get('type', '')
        timestamp = log.get('@timestamp', '')
        name = log.get('details.name', '')

        if event_type == "TaskStateEntered":
            if name not in execution_times:
                execution_times[name] = {"start": None, "end": None, "total_time": 0}
            execution_times[name]["start"] = to_datetime(timestamp)
        elif event_type == "TaskStateExited":
            if name in execution_times and execution_times[name]["start"]:
                execution_times[name]["end"] = to_datetime(timestamp)
                execution_times[name]["total_time"] += (execution_times[name]["end"] - execution_times[name]["start"]).total_seconds()

    # Convert datetime objects to strings
    for name, times in execution_times.items():
        if times["start"]:
            times["start"] = times["start"].isoformat()
        if times["end"]:
            times["end"] = times["end"].isoformat()

    # Log the results
    for name, times in execution_times.items():
        custom_message = {
            'event': 'Step Function Execution End - Metrics', 
            'tenant_id': tenant_id,
            'execution_id': execution_id,
            'stage': name,
            'total_execution_time_seconds': times['total_time']
        }
        
        log_group_name_to_summarise = '/aws/states/Phase-03-un3o6ponq'
        log_to_cloudwatch(execution_id, custom_message, log_group_name_to_summarise)

    return {
        'statusCode': 200,
        'body': execution_times
    }

def log_to_cloudwatch(execution_id, message, log_group_name):
    log_stream_name = '{}'.format(execution_id)
    
    log_stream_name = '{}'.format(execution_id)

    try:
        # Check if the log group exists
        response = client.describe_log_groups(
            logGroupNamePrefix=log_group_name
        )
        if len(response['logGroups']) == 0:
            # Log group doesn't exist, create it
            client.create_log_group(logGroupName=log_group_name)
            logger.info(f"Created log group: {log_group_name}")
        
        # Check if the log stream exists
        response = client.describe_log_streams(
            logGroupName=log_group_name,
            logStreamNamePrefix=log_stream_name
        )
        if len(response['logStreams']) == 0:
            # Log stream doesn't exist, create it
            client.create_log_stream(
                logGroupName=log_group_name,
                logStreamName=log_stream_name
            )
            logger.info(f"Created log stream: {log_stream_name}")

        # Put log events to client Logs
        response = client.put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=[
                {
                    'timestamp': int(round(time.time() * 1000)),
                    'message': json.dumps(message)
                }
            ]
        )
        logger.info(f"Logged message to client Logs. Response: {response}")

    except client.exceptions.ResourceNotFoundException as e:
        logger.error(f"Log group does not exist: {log_group_name}. Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error putting log events to CloudWatch Logs: {str(e)}")