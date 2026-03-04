import azure.functions as func
import logging

# Why: FunctionApp is the root object for the Python v2 programming model.
# ANONYMOUS auth level means the endpoint is publicly reachable without a key —
# required because Graph webhook notifications cannot pass function keys.
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    # Why: A minimal endpoint with no imports or dependencies.
    # If this registers and responds, the deployment pipeline is confirmed working.
    # If it does not, the problem is infrastructure — not the function code.
    logging.info("Ping received")
    return func.HttpResponse("Monica is alive.", status_code=200)
