import azure.functions as func
import logging

from task_chain import bp as bp_task_chain
from webhook_renewal import bp as bp_renewal
from task_creator import bp_creator

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_blueprint(bp_task_chain)
app.register_blueprint(bp_renewal)
app.register_blueprint(bp_creator)


@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Ping received")
    return func.HttpResponse("Monica is alive.", status_code=200)
