import logging
import azure.functions as func
import azure.durable_functions as df

async def main(req: func.HttpRequest, starter: str):    

    try:    
        req_get = req.get_json()
        batch_id = req_get.get("batch_id")
        notebook_path = req_get.get("notebook_path")
        parameters = req_get.get("parameters")
        
        if (not batch_id) or (not notebook_path) or (not parameters):
            logging.info("Invalid or missing parameters.")
            return "Invalid or missing parameters."

        input = {
            "batch_id": batch_id,
            "notebook_path": notebook_path,
            "parameters": parameters
        }       

        client = df.DurableOrchestrationClient(starter)
        logging.info(f"Triggering 'orchestrator'.")
        instance_id = await client.start_new(
            orchestration_function_name = "orchestrator",
            instance_id = None,
            client_input = input
        )
    
        logging.info(f"Orchestrator triggered with instance id : '{instance_id}'.")
        return client.create_check_status_response(req, instance_id)

    except Exception as error:
        raise ValueError(f"Orchestrator failed with error : {error}")