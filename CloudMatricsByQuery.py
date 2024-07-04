import boto3
import time
from datetime import datetime, timedelta

def lambda_handler(event, context):
    # Initialize the CloudWatch Logs client
    client = boto3.client('logs')
    
    # Define the log group name and Insights query
    log_group_name = '/aws/vendedlogs/states/MyStateMachine-phase3-ETL-datapipeline'
    query = """
    fields @timestamp, @message, details.name, type
    | filter type like /TaskStateEntered|TaskStateExited/
    | sort @timestamp asc
    """
    
    # Define the start and end times for the query (e.g., last 1 hour)
    end_time = int(time.time() * 1000)
    start_time = end_time - 1 * 60 * 60 * 1000
    
    # Start the query
    response = client.start_query(
        logGroupName=log_group_name,
        startTime=start_time,
        endTime=end_time,
        queryString=query,
    )
    print("Start query response:", response)
    query_id = response['queryId']
    
    # Wait for the query to complete
    while True:
        response = client.get_query_results(queryId=query_id)
        print("Query status response:", response)
        if response['status'] == 'Complete':
            print("Query completed successfully.")
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
    print("Execution times for each state:")
    for name, times in execution_times.items():
        print(f"Stage: {name}, Total Execution Time: {times['total_time']} seconds")
    
    return {
        'statusCode': 200,
        'body': execution_times
    }
