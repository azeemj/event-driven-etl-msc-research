import json

js = [{
    "id": "17",
    "type": "TaskStateEntered",
    "details": {
        "input": "{\"tenant_id\":\"tenant_one\","
        "\"chunk_key\":\"data/spilited_file/tenant_one/30aecaea-aa53-47de-92d3-de6c1d607e41/1mb_test_data_chunk_2.json\","
        "\"executionArn\":\"arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:92b93d68-3a67-4542-8203-4af7b35ab047\","
        "\"correlation_id\":\"30aecaea-aa53-47de-92d3-de6c1d607e41\"}",
        "inputDetails": {
            "truncated": "false"
        },
        "name": "DataExtraction-Lambda"
    },
    "previous_event_id": "11",
    "event_timestamp": "1721188274143",
    "execution_arn": "arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:92b93d68-3a67-4542-8203-4af7b35ab047",
    "redrive_count": "0"
},
{
    "id": "17",
    "type": "TaskStateEntered",
    "details": {
        "input": "{\"tenant_id\":\"tenant_one\","
        "\"chunk_key\":\"data/spilited_file/tenant_one/30aecaea-aa53-47de-92d3-de6c1d607e41/1mb_test_data_chunk_2.json\","
        "\"executionArn\":\"arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:92b93d68-3a67-4542-8203-4af7b35ab047\","
        "\"correlation_id\":\"30aecaea-aa53-47de-92d3-de6c1d607e41\"}",
        "inputDetails": {
            "truncated": "false"
        },
        "name": "DataExtraction-Lambda"
    },
    "previous_event_id": "11",
    "event_timestamp": "1721188274143",
    "execution_arn": "arn:aws:states:us-east-1:905418415996:execution:MyStateMachine-un3o6ponq:92b93d68-3a67-4542-8203-4af7b35ab047",
    "redrive_count": "0"
}
]

# Access the nested JSON string within the dictionary and decode it
for data in js:
    details_input = json.loads(data["details"]["input"])
    #details_input["details"]["input"] = details_input

    # Print the modified dictionary
    print(details_input['tenant_id'])
