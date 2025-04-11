import os
import requests
import json
import azure.functions as func
from azure.eventgrid import EventGridPublisherClient, EventGridEvent
from azure.identity import DefaultAzureCredential
from azure.mgmt.servicebus import ServiceBusManagementClient
from azure.servicebus import ServiceBusClient
from functions.utils import load_env
import logging

load_env()
logging.basicConfig(level=logging.INFO)

EVENT_GRID_TOPIC = os.getenv('EVENT_GRID_TOPIC')
EVENT_GRID_TOPIC_ENDPOINT = os.getenv("EVENT_GRID_TOPIC_ENDPOINT")
AEG_SAS_KEY = os.getenv("AEG_SAS_KEY")


def get_edw_pipeline_mapping():
    mapping = os.getenv("EDW_PIPELINE_MAPPING")
    return json.loads(mapping)
    

def get_edw_blob_event_subject(event):
    current_subject = event.get('subject')
    start_index = len("/blobServices/default/containers/")
    current_subject = current_subject[start_index:]
    pipeline_key = '/'.join(current_subject.split('/')[:-1]) + '/'
    
    mapping = get_edw_pipeline_mapping()
    pipeline_value = mapping.get(pipeline_key)
    if not pipeline_value:
        logging.error(f"Pipeline mapping not found for {pipeline_key}")
        raise Exception(f"Pipeline mapping not found for {pipeline_key}")
    
    return pipeline_value


def update_datamart_event(msg_body:str, new_event_type:str) -> dict:
    message_body = json.loads(msg_body)
    message_body["eventType"] = new_event_type
    return message_body


def update_blob_event(msg: func.ServiceBusMessage, msg_body:str, new_event_type:str, topic:str) -> dict:
    message = json.loads(str(msg_body.replace("'", "\"")))
    subject = get_edw_blob_event_subject(message)

    eventType = new_event_type
    blobUrl = message.get('data').get('blobUrl')
    blobName = blobUrl.split('/')[-1]
    dataVersion = message.get('dataVersion')
    metadataVersion = message.get('metadataVersion')
    eventTime = message.get('eventTime')

    message_body = {
        'id': 'I01',
        'eventType': eventType,
        'subject': subject,
        'data': {
            'source_file_name': blobName
        },
        'dataVersion': dataVersion,
        'metadataVersion': metadataVersion,
        'eventTime': eventTime,
        'topic': topic
    }

    return message_body


def update_event(msg: func.ServiceBusMessage, msg_body:str, event_type:str) -> dict:
    if event_type == 'pipeline_queue_trigger':
        return update_datamart_event(msg_body, 'pipeline_event_trigger')
    elif event_type == 'Microsoft.Storage.BlobCreated':
        return update_blob_event(msg, msg_body, 'blob_event_trigger', EVENT_GRID_TOPIC)
    return None
    

def trigger_new_event(event_data:dict):
    headers = {'aeg-sas-key': AEG_SAS_KEY}
    response = requests.post(EVENT_GRID_TOPIC_ENDPOINT, headers=headers, data=json.dumps([event_data]))

    if response.status_code != 200:
        logging.info(f"Failed to send event to Event Grid. Status code: {response.status_code}, Response: {response.text}")
    else:
        logging.info("Event sent to Event Grid successfully.")


def get_dead_letter_count(subscription_id:str, resource_group:str, service_bus_namespace:str, queue_name:str) -> int:
    credential = DefaultAzureCredential()
    servicebus_mgmt_client = ServiceBusManagementClient(credential, subscription_id)

    queue = servicebus_mgmt_client.queues.get(resource_group, service_bus_namespace, queue_name)
    dead_letter_message_count = queue.count_details.dead_letter_message_count
    return dead_letter_message_count


def get_active_message_count(subscription_id:str, resource_group:str, service_bus_namespace:str, queue_name:str) -> int:
    credential = DefaultAzureCredential()
    servicebus_mgmt_client = ServiceBusManagementClient(credential, subscription_id)

    queue = servicebus_mgmt_client.queues.get(resource_group, service_bus_namespace, queue_name)
    active_message_count = queue.count_details.active_message_count
    return active_message_count


def get_dead_letter_info(service_bus_connection_str:str, dead_letter_queue_name:str) -> dict:
    info = []
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=service_bus_connection_str)

    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=dead_letter_queue_name, max_wait_time=30)

        with receiver:
            for msg in receiver:
                try:
                    enqueued_time = msg.enqueued_time_utc
                    dead_letter_reason = msg.dead_letter_reason
                    message = json.loads(str(msg))
                    subject = message['subject']

                    info.append({
                        "enqueued_time": enqueued_time,
                        "dead_letter_reason": dead_letter_reason,
                        "subject": subject
                    })

                except Exception as e:
                    logging.info(f"Failed to get dead letter info: {e}")
                    raise e

            return info