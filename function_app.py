# Project Monica - Function App
# Main entry point for all Azure Functions
# Python 3.12 / Azure Functions v2 programming model

import azure.functions as func
import logging
import json
import os
import requests

app = func.FunctionApp()


# --- Authentication ---
def get_access_token():
    """
    Why: The Function App uses Managed Identity to authenticate with Microsoft Graph.
    This means no credentials in code - Azure handles the token automatically.
    IDENTITY_ENDPOINT and IDENTITY_HEADER are injected by Azure at runtime.
    """
    identity_endpoint = os.environ["IDENTITY_ENDPOINT"]
    identity_header = os.environ["IDENTITY_HEADER"]
    token_url = f"{identity_endpoint}?resource=https://graph.microsoft.com&api-version=2019-08-01"
    headers = {"X-IDENTITY-HEADER": identity_header}
    response = requests.get(token_url, headers=headers)
    return response.json()["access_token"]


# --- OneDrive: Read task-chains.json ---
def get_task_chains(token):
    """
    Why: Task chain rules live in a human-readable JSON file in OneDrive,
    not hardcoded in the function. This means Phillip can add new chains
    by editing a file - no code deployment required.
    """
    drive_id = "b!P6rMZy1cnUiuZLBDURE_GkKIGD_9euVDsIfqU_9bzzdFt7Iel1D4SY7FwvJum6B5"
    file_path = "[00] Systems/Infrastructure/Monica/config/task-chains.json"
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()


# --- To Do: Get list ID by name ---
def get_list_id(token, user_id, list_name):
    """
    Why: Graph API requires a list ID, not a list name. This function
    translates the human-readable name in task-chains.json into the
    ID Microsoft To Do needs to create the task in the right place.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/todo/lists"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    lists = response.json().get("value", [])
    for lst in lists:
        if lst["displayName"] == list_name:
            return lst["id"]
    return None


# --- To Do: Create successor task ---
def create_task(token, user_id, list_id, task_name, category):
    """
    Why: This is Monica's core action - creating the successor task
    in the correct list with the correct domain category applied.
    Task naming follows the Founding Specification convention exactly:
    Verb: Activity beginning with a capital letter.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/todo/lists/{list_id}/tasks"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "title": task_name,
        "categories": [category]
    }
    response = requests.post(url, headers=headers, json=body)
    return response.status_code, response.json()


# --- Task Chain: Combined GET (validation) and POST (notification) handler ---
@app.route(route="taskchain", methods=["GET", "POST"])
def taskChain(req: func.HttpRequest) -> func.HttpResponse:
    """
    Why: Azure Functions does not allow two separate functions to share
    the same route. The GET and POST handlers must be combined into one
    function that checks the HTTP method and responds appropriately.
    GET handles Graph's validation handshake - required before a webhook
    subscription can be registered.
    POST handles the actual task completion notifications from Graph.
    """

    # --- GET: Webhook validation handshake ---
    if req.method == "GET":
        validation_token = req.params.get("validationToken")
        if validation_token:
            logging.info("Webhook validation request received - responding with token")
            return func.HttpResponse(
                validation_token,
                status_code=200,
                mimetype="text/plain"
            )
        return func.HttpResponse("No validation token", status_code=400)

    # --- POST: Task completion notification ---
    logging.info("taskChain function triggered")
    user_id = "cda66539-6f2a-4a27-a5a3-a493061f8711"

    try:
        body = req.get_json()
        notifications = body.get("value", [])

        for notification in notifications:
            resource = notification.get("resource", "")
            logging.info(f"Notification received for resource: {resource}")

            token = get_access_token()
            chains = get_task_chains(token)

            task_url = f"https://graph.microsoft.com/v1.0/{resource}"
            headers = {"Authorization": f"Bearer {token}"}
            task_response = requests.get(task_url, headers=headers)
            task = task_response.json()
            completed_title = task.get("title", "")
            logging.info(f"Completed task title: {completed_title}")

            for chain in chains:
                if chain["trigger_task"] == completed_title:
                    logging.info(f"Chain match found: {chain['creates_task']}")
                    list_id = get_list_id(token, user_id, chain["list"])
                    if not list_id:
                        logging.error(f"List not found: {chain['list']}")
                        continue
                    status, result = create_task(
                        token,
                        user_id,
                        list_id,
                        chain["creates_task"],
                        chain["category"]
                    )
                    logging.info(f"Successor task created: {chain['creates_task']} - Status: {status}")

        return func.HttpResponse("OK", status_code=200)

    except Exception as e:
        logging.error(f"taskChain error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
