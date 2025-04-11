import logging
from azure.synapse.artifacts import ArtifactsClient
from azure.monitor.query import LogsQueryClient
from datetime import datetime, timedelta
import pytz
import pandas as pd
import json
import os

WORKSPACE_ID = os.getenv("SYNAPSE_WORKSPACE_ID")
SYNAPSE_WORKSPACE_NAME = os.getenv("SYNAPSE_WORKSPACE_NAME")
TIMEZONE = os.getenv("TIMEZONE")

query_pipeline_triggers = """
SynapseIntegrationTriggerRuns
| where TriggerType == 'CustomEventsTrigger'
"""
query_trace_events = """
AppTraces
| where Message startswith "Triggering new event"
"""

def get_active_trigger_subjects(credential):
    logging.info(f"Getting active trigger subjects from Synapse Workspace: {SYNAPSE_WORKSPACE_NAME}")
    synapse_endpoint = f"https://{SYNAPSE_WORKSPACE_NAME}.dev.azuresynapse.net"
    trigger_client = ArtifactsClient(endpoint=synapse_endpoint, credential=credential)
    triggers = trigger_client.trigger.get_triggers_by_workspace()

    subjects = []
    for trigger in triggers:
        properties = trigger.properties
        if properties.type == 'CustomEventsTrigger':    
            subject_begins_with = properties.subject_begins_with
            subject_ends_with = properties.subject_ends_with
            subjects.append(subject_begins_with+subject_ends_with)
    
    subjects = list(set(subjects))
    return subjects


def query_log_analytics(credential, query, timespan):
    logging.info(f"Querying Log Analytics with query: {query}")
    logs_client = LogsQueryClient(credential)
    response_pipeline_triggers = logs_client.query_workspace(
        workspace_id=WORKSPACE_ID,
        query=query,
        timespan=timespan
    )

    return response_pipeline_triggers

def get_triggered_pipelines(credential, timespan=None):
    if not timespan:
        tz = pytz.timezone(TIMEZONE)
        start_time = datetime.now(tz) - timedelta(days=1)
        end_time = datetime.now(tz)
        timespan = (start_time, end_time)

    df = {"triggerTime": [],"eventTime": [],"eventType": [],"subject": []}

    logging.info("Running query_log_analytics()")
    response = query_log_analytics(credential, query_pipeline_triggers, timespan)

    for row in response.tables[0].rows:
        parameters = row[16]
        outer_dict = json.loads(parameters)
        event_payload = outer_dict['EventPayload']
        inner_dict = json.loads(event_payload)
        outer_dict['EventPayload'] = inner_dict

        df['triggerTime'].append(outer_dict['TriggerTime'])
        df['eventTime'].append(outer_dict['EventPayload']['eventTime'])
        df['eventType'].append(outer_dict['EventPayload']['eventType'])
        df['subject'].append(outer_dict['EventPayload']['subject'])

    df = pd.DataFrame(df)
    return df

def get_triggered_events(credential, timespan=None):
    if not timespan:
        tz = pytz.timezone(TIMEZONE)
        start_time = datetime.now(tz) - timedelta(days=1)
        end_time = datetime.now(tz)
        timespan = (start_time, end_time)

    df = {"eventTime": [],"eventType": [],"subject": []}

    logging.info("Running query_log_analytics()")
    response = query_log_analytics(credential, query_trace_events, timespan)

    for row in response.tables[0].rows:
        message = row[2]
        dict_index = message.index('{')
        message = message[dict_index:]
        message = message.replace("'", "\"")
        message = json.loads(message)
        
        df['eventTime'].append(message['eventTime'])
        df['eventType'].append(message['eventType'])
        df['subject'].append(message['subject'])

    df = pd.DataFrame(df)
    return df