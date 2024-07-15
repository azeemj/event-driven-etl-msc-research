import json

json_data_list = [
    r'''
    {
        "@timestamp": "2024-07-03 15:04:27.995",
        "@message": {
            "id": "33",
            "type": "ExecutionSucceeded",
            "details": {
                "output": "[{\"statusCode\": 200, \"body\": \"{\\\"source\\\": \\\"transformed-data/processed-data/tenant_one/a035e3b8-440b-4f3d-90c5-c405e398e932/2mb_data_one_part_chunk_0.csv\\\", \\\"destination\\\": \\\"load-data/tenant_one/a035e3b8-440b-4f3d-90c5-c405e398e932/2mb_data_one_part_chunk_0.csv\\\"}\"}]",
                "outputDetails": {
                    "truncated": false
                }
            },
            "previous_event_id": "32",
            "event_timestamp": "1720019067995",
            "execution_arn": "arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:e70cbe0c-f220-4172-a186-8d3830916fb0",
            "redrive_count": "0"
        },
        "execution_arn": "arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:e70cbe0c-f220-4172-a186-8d3830916fb0",
        "id": "33",
        "previous_event_id": "32"
    }
    ''',
    r'''
    {
        "@timestamp": "2024-07-03 15:10:01.123",
        "@message": {
            "id": "34",
            "type": "ExecutionFailed",
            "details": {
                "error_message": "Task failed due to timeout",
                "error_code": 500
            },
            "previous_event_id": "33",
            "event_timestamp": "1720019067996",
            "execution_arn": "arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:e70cbe0c-f220-4172-a186-8d3830916fb1",
            "redrive_count": "1"
        },
        "execution_arn": "arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:e70cbe0c-f220-4172-a186-8d3830916fb1",
        "id": "34",
        "previous_event_id": "33"
    }
    '''
]


# data = json.load(json_data_list)
# print("test", data)
# Parse all JSON strings into Python dictionaries
data = [json.loads(json_str) for json_str in json_data_list]
print("test", data)
# Iterate over each parsed dictionary
# for item in data:
#     print(f"@timestamp: {item['@timestamp']}")
#     print(f"id: {item['id']}")
#     print(f"execution_arn: {item['execution_arn']}")
    
#     print(f"Message ID: {item['@message']['id']}")
#     print(f"Execution type: {item['@message']['type']}")
#     print(f"Event timestamp: {item['@message']['event_timestamp']}")
    
#     if 'output' in item['@message']['details']:
#         print(f"Output: {item['@message']['details']['output']}")
#     if 'outputDetails' in item['@message']['details'] and item['@message']['details']['outputDetails'] and 'truncated' in item['@message']['details']['outputDetails']:
#         print(f"Output Details truncated: {item['@message']['details']['outputDetails']['truncated']}")
    
print("---")  # Separator for clarity
