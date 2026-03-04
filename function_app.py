import azure.functions as func
import logging

# Why: Import the blueprint from task_chain.py so the runtime discovers
# the taskChain HTTP trigger alongside the ping function below.
from task_chain import bp as bp_task_chain

# Why: FunctionApp is the root object for the Python v2 programming model.
# ANONYMOUS auth level is required because Graph webhook notifications
# cannot pass function keys.
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Why: Registering the blueprint tells the runtime to load all functions
# defined in task_chain.py as if they were defined in this file.
app.register_blueprint(bp_task_chain)


@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    # Why: Minimal endpoint to confirm the deployment pipeline is working.
    logging.info("Ping received")
    return func.HttpResponse("Monica is alive.", status_code=200)
