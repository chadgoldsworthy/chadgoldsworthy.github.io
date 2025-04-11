import papermill as pm
from azure.storage.blob import BlobServiceClient
from src.fileFunctions import file_download, file_upload
from src.fileMapping import track_file_mapping
from src.logFunctions import log_activity, log_active_run
from src.configs import config_get
from src.dbFunctions import track_file_load_initiate, track_file_load_complete
from src.aiFunctions import ai_clean_notebook_error_message
from datetime import datetime
import os
import uuid

def main(parameters) -> str:
    try:
        status = 0
        output = None
        error_message = None
        
        run_id = str(uuid.uuid4())
        log_active_run(run_id, 'start')

        configs = config_get()
        batch_id = parameters["batch_id"] 
        input = {
            "batch_id" : batch_id,
            "configs": configs,
            "parameters" : parameters["parameters"]
        }
        
        notebook = parameters["notebook_path"]                      
        notebook_container = configs["storage_accounts"]["source"]["notebook_container"]
        connection_string = configs["storage_accounts"]["source"]["connection_string"]
        kernel = configs["python"]["kernel"]

        track_file_load_initiate(configs, batch_id, notebook, parameters["parameters"], track_file_mapping)

        guid = str(uuid.uuid4())
        input_path = f"/tmp/input_{guid}.ipynb"
        output_path = f"/tmp/output_{guid}.ipynb"

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        file_download(blob_service_client, notebook_container, notebook, input_path)
        
    except Exception as e: 
        track_file_load_complete(configs, batch_id, parameters["parameters"], 'Failed', str(e))
        log_activity(batch_id, status, notebook, output, f'{e}\n\n')
        log_active_run(run_id, 'end')
        return f"'execute_notebook' activity failed with error : '{e}'"

    try:
        pm.execute_notebook(input_path, output_path, kernel_name=kernel, parameters=input)
        status = 1

    except Exception as error:
        status = 0
        error_message = error
    
    finally:
        today = datetime.today().strftime("%Y%m%d%H%M%S%f")
        output = f"notebooks_output/{today}_{batch_id}.ipynb"
        file_upload(blob_service_client, notebook_container, output_path, output)

        os.remove(input_path)
        os.remove(output_path)
        log_active_run(run_id, 'end')
        if status == 1: 
            log_activity(batch_id, 1, notebook, output)
            return f"Success! The output notebook path is '{output}'."
        else: 
            track_file_load_complete(configs, batch_id, parameters["parameters"], 'Failed', str(error_message))
            log_activity(batch_id, 2, notebook, output, f'{error_message}\n\n')
        
            full_error_message = f"Notebook run failed with error : {error_message}\nThe output notebook path is '{output}'."
            return ai_clean_notebook_error_message(full_error_message)


    