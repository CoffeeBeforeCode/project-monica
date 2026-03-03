# Project Monica - Function App
# Main entry point for all Azure Functions
# Python 3.12 / Azure Functions v2 programming model

import azure.functions as func
import logging
import os
import re
import requests
from datetime import datetime, timezone

app = func.FunctionApp()


# --- Authentication ---
def get_access_token():
    identity_endpoint = os.environ["IDENTITY_ENDPOINT"]
    identity_header = os.environ["IDENTITY_HEADER"]
    token_url = f"{identity_endpoint}?resource=https://graph.microsoft.com&api-version=2019-08-01"
    headers = {"X-IDENTITY-HEADER": identity_header}
    response = requests.get(token_url, headers=headers)
    return response.json()["access_token"]


@app.route(route="taskchain", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def taskChain(req: func.HttpRequest) -> func.HttpResponse:
    validation_token = req.params.get("validationToken")
    if validation_token:
        return func.HttpResponse(validation_token, status_code=200, mimetype="text/plain")
    return func.HttpResponse("OK", status_code=200)
