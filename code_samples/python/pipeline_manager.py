from azure.identity import DefaultAzureCredential
from azure.synapse.artifacts import ArtifactsClient
from azure.synapse.artifacts.models import RunFilterParameters, RunQueryFilter, RunQueryFilterOperand
import os
from datetime import datetime, timedelta
from functions.utils import load_env
from datetime import datetime
import pytz
import time

load_env()
TIMEZONE = os.getenv("TIMEZONE")
tz = pytz.timezone(TIMEZONE)


def get_running_pipeline_count(sleep:int=0):
    time.sleep(sleep)

    SYNAPSE_WORKSPACE_NAME = os.getenv("SYNAPSE_WORKSPACE_NAME")
    credential = DefaultAzureCredential()
    synapse_endpoint = f"https://{SYNAPSE_WORKSPACE_NAME}.dev.azuresynapse.net"
    client = ArtifactsClient(endpoint=synapse_endpoint, credential=credential)

    last_updated_after = datetime.now(tz) - timedelta(days=1)
    last_updated_before = datetime.now(tz)

    filter_parameters = RunFilterParameters(
        filters=[
            RunQueryFilter(
                operand=RunQueryFilterOperand.STATUS,
                operator="Equals",
                values=["InProgress", "Queued"]
            )
        ],
        last_updated_after=last_updated_after,
        last_updated_before=last_updated_before
    )

    response = client.pipeline_run.query_pipeline_runs_by_workspace(
        filter_parameters=filter_parameters
    )

    return len(response.value)



def trigger_pipeline(pipeline_name:str, parameters:dict=None):
    credential = DefaultAzureCredential()
    SYNAPSE_WORKSPACE_NAME = os.getenv("SYNAPSE_WORKSPACE_NAME")
    synapse_endpoint = f"https://{SYNAPSE_WORKSPACE_NAME}.dev.azuresynapse.net"
    client = ArtifactsClient(endpoint=synapse_endpoint, credential=credential)

    response = client.pipeline.create_pipeline_run(
        pipeline_name=pipeline_name,
        parameters=parameters
    )

    response = client.pipeline.create_pipeline_run(
        pipeline_name=pipeline_name
    )

    return response.run_id