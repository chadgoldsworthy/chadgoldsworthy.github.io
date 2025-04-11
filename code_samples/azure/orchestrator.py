import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    try:

        context.set_custom_status("'Orchestrator started.")
        batch_id = context.get_input()["batch_id"]
        notebook_path = context.get_input()["notebook_path"]
        parameters = context.get_input()["parameters"]
        
        input = {
            "batch_id": batch_id,
            "notebook_path": notebook_path,
            "parameters": parameters
        }
        
        context.set_custom_status(f"Orchestrator calling notebook activity [{notebook_path}]")
        result = yield context.call_activity('execute_notebook', input)
        context.set_custom_status("Orchestrator notebook activity completed")

        return result
        
    except Exception as error:
        context.set_custom_status(f"Failed.")
        raise ValueError(f"Run ended in 'orchestrator_function' with error : {error}")

try:
    main = df.Orchestrator.create(orchestrator_function)
except Exception as error:
    raise ValueError(f"Run ended in 'orchestrator_function' with error : {error}")