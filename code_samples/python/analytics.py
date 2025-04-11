import os
import io
import csv
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import pytz

BLOB_CONNECTION_STRING = os.getenv("ANALYTICS_STORE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = "analytics"
TIMEZONE = os.getenv("TIMEZONE")

def log_service_bus_activities(running_pipelines:int, limit:int, active_messages:int):
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    file_dir = f"service_bus_queues"
    file_name = f"service_bus_queue_runs_{datetime.now().strftime('%Y%m%d')}.csv"
    file_path = os.path.join(file_dir, file_name)

    row = [now, running_pipelines, limit, active_messages]
    
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=file_path)
    
    try:
        download_stream = blob_client.download_blob()
        blob_data = download_stream.readall().decode("utf-8")
        existing_data = list(csv.reader(io.StringIO(blob_data)))
    except Exception as e:
        logging.info(f"Blob does not exist or failed to read: {e}")
        existing_data = [["TimeGenerated", "RunningPipelines", "PipelineLimit", "ActiveMessages"]]

    existing_data.append(row)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(existing_data)
    blob_client.upload_blob(output.getvalue(), overwrite=True)
    