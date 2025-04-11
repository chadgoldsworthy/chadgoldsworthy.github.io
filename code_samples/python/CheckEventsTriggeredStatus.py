#0
import sys
sys.path.append('..')

import logging
from azure.functions import TimerRequest
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from functions.trigger_manager import *
from datetime import datetime
from io import StringIO

def main(mytimer: TimerRequest) -> None:
    logging.info('CheckEventsTriggeredStatus function started.')
    try:
        BLOB_CONNECTION_STRING = os.getenv("ANALYTICS_STORE_CONNECTION_STRING")
        BLOB_CONTAINER_NAME = "analytics"
        TIMEZONE = os.getenv("TIMEZONE")
        tz = pytz.timezone(TIMEZONE)

        credential = DefaultAzureCredential()

        active_subjects = get_active_trigger_subjects(credential)
        logging.info(f'Retrieved {len(active_subjects)} active trigger subjects.')

        df_triggered_pipelines = get_triggered_pipelines(credential)
        df_triggered_events = get_triggered_events(credential)
        logging.info(f'Retrieved {len(df_triggered_pipelines)} triggered pipelines and {len(df_triggered_events)} triggered events.')

        df = df_triggered_events.merge(df_triggered_pipelines, on=['eventTime', 'subject'], how='left', indicator=True)
        df = df[df['subject'].isin(active_subjects)] # filter out inactive triggers

        df['Passed'] = df['_merge'] == 'both'
        df = df[['eventTime', 'triggerTime', 'eventType_x', 'subject', 'Passed']]
        df = df.rename(columns={'eventType_x': 'eventType'})
        df = df.sort_values(by='eventTime')

        logging.info(f'Uploading results to blob storage.')
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

        csv_data = StringIO()
        df.to_csv(csv_data, index=False)

        blob_file = f'pipeline_events_vs_triggered/pipeline_events_vs_triggered_{datetime.now(tz).strftime("%Y%m%d")}.csv'
        blob_client = blob_container_client.get_blob_client(blob_file)
        blob_client.upload_blob(csv_data.getvalue(), overwrite=True)

        logging.info('CheckEventsTriggeredStatus function completed successfully.')
    except Exception as e:
        logging.error(f"Failed to process 'CheckEventsTriggeredStatus': {e}")
        raise e
