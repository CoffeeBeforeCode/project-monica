# Project Monica - Function App
# Main entry point for all Azure Functions
# Python 3.12 / Azure Functions v2 programming model

import azure.functions as func
import logging
import json
import os
import requests
from webhook_renewal import bp as renewal_bp

app = func.FunctionApp()
app.register_functions(renewal_bp)


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


# --- To Do: Get recently completed tasks from a list ---
def get_completed_tasks(token, user_id, list_id):
    """
    Why: Graph's webhook notification tells us which list changed but sends
    the resource path in an internal format that cannot be used directly.
    Instead of trying to fetch a specific task from that path, we query
    the list for all completed tasks and check each one against the chain
    rules. This is reliable regardless of how Graph formats the notification.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/todo/lists/{list_id}/tasks?$filter=status eq 'completed'"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json().get("value", [])


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


# --- Task Chain: Combined validation and notification handler ---
@app.route(route="taskchain", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def taskChain(req: func.HttpRequest) -> func.HttpResponse:
    """
    Why: The validationToken check must happen before anything else,
    regardless of HTTP method. Graph sends its validation handshake as
    a POST, not a GET as might be expected. Checking for the token first
    means both validation and task notifications are handled correctly.
    """

    # --- Validation handshake (GET or POST) ---
    # Why: Graph may send the validation request as either method.
    # Checking for the token parameter first handles both cases safely.
    validation_token = req.params.get("validationToken")
    if validation_token:
        logging.info("Webhook validation request received - responding with token")
        return func.HttpResponse(
            validation_token,
            status_code=200,
            mimetype="text/plain"
        )

    # --- POST: Task completion notification ---
    logging.info("taskChain function triggered")
    user_id = "cda66539-6f2a-4a27-a5a3-a493061f8711"

    try:
        body = req.get_json()
        notifications = body.get("value", [])

        token = get_access_token()
        chains = get_task_chains(token)

        for notification in notifications:
            resource = notification.get("resource", "")
            logging.info(f"Notification received for resource: {resource}")

            # Extract the list ID from the resource path
            # Why: The list ID appears between single quotes after 'lists('
            # We extract it here to query the list directly rather than
            # using the internal resource path which cannot be fetched directly.
            import re
            list_id_match = re.search(r"lists\('([^']+)'\)", resource)
            if not list_id_match:
                logging.error("Could not extract list ID from resource path")
                continue

            list_id = list_id_match.group(1)
            logging.info(f"Querying list ID: {list_id}")

            # Query the list for completed tasks
            completed_tasks = get_completed_tasks(token, user_id, list_id)
            logging.info(f"Found {len(completed_tasks)} completed tasks in list")

            for task in completed_tasks:
                completed_title = task.get("title", "")
                logging.info(f"Checking completed task: {completed_title}")

                for chain in chains:
                    if chain["trigger_task"] == completed_title:
                        logging.info(f"Chain match found: {chain['creates_task']}")

                        target_list_id = get_list_id(token, user_id, chain["list"])
                        if not target_list_id:
                            logging.error(f"Target list not found: {chain['list']}")
                            continue

                        status, result = create_task(
                            token,
                            user_id,
                            target_list_id,
                            chain["creates_task"],
                            chain["category"]
                        )
                        logging.info(f"Successor task created: {chain['creates_task']} - Status: {status}")

        return func.HttpResponse("OK", status_code=200)

    except Exception as e:
        logging.error(f"taskChain error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
