import boto3
import time
from datetime import datetime, timedelta
import json
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
sf_client = boto3.client('stepfunctions')
logs_client = boto3.client('logs')

def get_step_function_execution_times(execution_arn):
    events = []
    next_token = None

    # Retrieve all events handling pagination
    while True:
        if next_token:
            response = sf_client.get_execution_history(
                executionArn=execution_arn,
                maxResults=50,  # Reduced maxResults to 50
                reverseOrder=False,
                nextToken=next_token
            )
        else:
            response = sf_client.get_execution_history(
                executionArn=execution_arn,
                maxResults=50,  # Reduced maxResults to 50
                reverseOrder=False
            )
        events.extend(response['events'])
        next_token = response.get('nextToken', None)
        if not next_token:
            break

    start_time = None
    end_time = None
    task_start_times = {}
    task_end_times = {}
    produce_metrics_start_time_stamp = None

    for event in events:
        event_timestamp = event['timestamp']
        if event['type'] == 'ExecutionStarted':
            start_time = event_timestamp
        elif event['type'] in ['ExecutionSucceeded', 'ExecutionFailed']:
            end_time = event_timestamp
        elif event['type'] == 'TaskStateEntered':
            state_name = event['stateEnteredEventDetails']['name']
            if state_name not in task_start_times:
                task_start_times[state_name] = []
            task_start_times[state_name].append(event_timestamp)
            # ProduceMetrics entered time is assigned to calculated Step machine exit time
            if state_name == 'ProduceMetrics':
                produce_metrics_start_time_stamp = event_timestamp
        elif event['type'] == 'TaskStateExited':
            state_name = event['stateExitedEventDetails']['name']
            if state_name not in task_end_times:
                task_end_times[state_name] = []
            task_end_times[state_name].append(event_timestamp)
   
    # Calculate individual task execution times
    task_execution_times = {}
    for task in task_start_times:
        if task in task_end_times:
            total_task_time = sum(
                (end - start).total_seconds()
                for start, end in zip(task_start_times[task], task_end_times[task])
            )
            task_execution_times[task] = total_task_time

    

    # Calculate total execution time up to ProduceMetrics task start time
    total_execution_time = (produce_metrics_start_time_stamp - start_time).total_seconds() if start_time and produce_metrics_start_time_stamp else None
    logger.info(f"total_execution_time ???????????????- {total_execution_time}")

    return total_execution_time, task_execution_times

def lambda_handler(event, context):
    logger.info(f"produce metrics event- {event}")
    execution_arn = event.get('executionArn')
    tenant_id = event.get('tenant_id')
    execution_id = context.aws_request_id
    log_group_name = '/aws/vendedlogs/states/MyStateMachine-phase3-ETL-datapipeline'

    if not execution_arn or not log_group_name:
        return {
            'statusCode': 400,
            'body': 'Missing executionArn or logGroupName in the event.'
        }

    try:
        retry_attempts = 2
        while retry_attempts > 0:
            # Get Step Function execution times
            total_execution_time, task_execution_times = get_step_function_execution_times(execution_arn)
            print('task_execution_times', task_execution_times)
            print('total_execution_time', total_execution_time)

            if len(task_execution_times) < 5:
                retry_attempts -= 1
                time.sleep(2)  # Adding a small delay before retrying
                continue
            break

        if any(task in task_execution_times for task in [
            'HandleDataExtractionErrorLambda',
            'RevertDataFromTransformationLambda',
            'RevertDataFromLoadErrorLambda'
             ]):
            logger.info("Exception handled:")
        elif retry_attempts == 0:
            return {
                'statusCode': 500,
                'body': 'Failed to retrieve valid execution times after retries.'
            }

        logger.info(f"Total execution time: {total_execution_time} seconds")
        logger.info("Individual task execution times:")
        for task, task_time in task_execution_times.items():
            logger.info(f"{task}: {task_time} seconds")

            if task != 'ProduceMetrics':
                custom_message = {
                    'event': 'Step Function Execution stage wise - Metrics',
                    'tenant_id': tenant_id,
                    'stage': task,
                    'execution_arn': execution_arn,
                    'execution_id': context.aws_request_id,
                    'total_execution_time_seconds': task_time,
                    'timestamp': datetime.utcnow().isoformat()
                }
                log_group_name_to_summarise = '/aws/states/Phase-03-un3o6ponq'
                log_to_cloudwatch(context.aws_request_id, custom_message, log_group_name_to_summarise)

        # Log the total execution time of the entire Step Function
        if total_execution_time is not None:
            total_execution_message = {
                'event': 'Step Function Total Execution Time',
                'tenant_id': tenant_id,
                'execution_id': context.aws_request_id,
                'execution_arn': execution_arn,
                'total_execution_time_seconds': total_execution_time,
                'timestamp': datetime.utcnow().isoformat()
            }

            log_group_name_to_summarise = '/aws/states/Phase-03-un3o6ponq'
            log_to_cloudwatch(context.aws_request_id, total_execution_message, log_group_name_to_summarise)

        return {
            'statusCode': 200,
            'body': {
                'total_execution_time_seconds': total_execution_time,
                'stage_execution_times': task_execution_times
            }
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Internal server error: {str(e)}"
        }

def log_to_cloudwatch(execution_id, message, log_group_name):
    log_stream_name = '{}'.format(execution_id)

    try:
        # Check if the log group exists
        response = logs_client.describe_log_groups(
            logGroupNamePrefix=log_group_name
        )
        if len(response['logGroups']) == 0:
            # Log group doesn't exist, create it
            logs_client.create_log_group(logGroupName=log_group_name)
            logger.info(f"Created log group: {log_group_name}")

        # Check if the log stream exists
        response = logs_client.describe_log_streams(
            logGroupName=log_group_name,
            logStreamNamePrefix=log_stream_name
        )
        if len(response['logStreams']) == 0:
            # Log stream doesn't exist, create it
            logs_client.create_log_stream(
                logGroupName=log_group_name,
                logStreamName=log_stream_name
            )
            logger.info(f"Created log stream: {log_stream_name}")

        # Put log events to CloudWatch Logs
        response = logs_client.put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=[
                {
                    'timestamp': int(round(time.time() * 1000)),
                    'message': json.dumps(message)
                }
            ]
        )
        logger.info(f"Logged message to CloudWatch Logs. Response: {response}")

    except logs_client.exceptions.ResourceNotFoundException as e:
        logger.error(f"Log group does not exist: {log_group_name}. Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error putting log events to CloudWatch Logs: {str(e)}")
