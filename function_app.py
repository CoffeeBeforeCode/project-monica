import azure.functions as func
import logging

from task_chain import bp as bp_task_chain
from webhook_renewal import bp as bp_renewal

# Why: task_creator temporarily removed to isolate which blueprint
# is causing Timer Triggers not to register.
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_blueprint(bp_task_chain)
app.register_blueprint(bp_renewal)


@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Ping received")
    return func.HttpResponse("Monica is alive.", status_code=200)
