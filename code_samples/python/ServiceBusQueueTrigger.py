import sys
sys.path.append('..')

import os
import azure.functions as func
from functions.event_manager import *
from functions.pipeline_manager import *
from functions.analytics import log_service_bus_activities
from functions.utils import load_env
import logging

load_env()
logging.basicConfig(level=logging.DEBUG)

def main(msg: func.ServiceBusMessage):
    logging.info('Python ServiceBus queue trigger processed message: %s',
                 msg.get_body().decode('utf-8'))
    limit = int(os.getenv("RUNNING_PIPELINE_LIMIT"))
    sleep = int(os.getenv("QUERY_ACTIVE_PIPELINES_SLEEP_TIME"))
    subscription_id = os.getenv('SUBSCRIPTION_ID')
    resource_group = os.getenv('RESOURCE_GROUP')
    service_bus_namespace = os.getenv('SERVICE_BUS_NAMESPACE')
    queue_name = os.getenv('QUEUE_NAME')
    
    message_body = msg.get_body().decode('utf-8')
    message_body = message_body.replace("'", "\"")
    message_body = message_body.replace('""""', '""')
    eventType = json.loads(message_body)['eventType']
    if sleep:
        logging.info(f"Sleeping for {sleep} seconds before checking active pipelines.")
    
    running = get_running_pipeline_count(sleep=sleep)
    active_msgs = get_active_message_count(subscription_id, resource_group, service_bus_namespace, queue_name)
    log_service_bus_activities(running, limit, active_msgs)
    
    logging.info(f"Running pipelines: {running}, Limit: {limit}")
    if running <= limit:
        processed_data = update_event(msg, message_body, eventType)
        logging.info(f"Triggering new event: {processed_data}")
        trigger_new_event(processed_data)
    
    else:
        info = f"Running pipelines greater than limit (Limit:{limit}, Running:{running}). Requeuing event: {message_body}"
        logging.info(info)
        raise Exception(info)
