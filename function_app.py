import azure.functions as func
import logging
import json
import requests
import os
from datetime import datetime, timezone

# Why: Blueprint imports pull in the Timer Trigger functions defined in
# separate files. The Python v2 programming model requires all functions
# to be registered under a single FunctionApp instance — Blueprints allow
# the code to be split across files while still satisfying that requirement.
from webhook_renewal import bp as bp_renewal
from task_creator import bp as bp_creator

# Why: FunctionApp is the root object for the Python v2 programming model.
# ANONYMOUS auth level is required because Graph webhook notifications
# cannot pass function keys in their requests — the endpoints must be
# publicly reachable without a key.
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Why: Register the Blueprint modules so the runtime discovers their
# Timer Trigger functions alongside the HTTP Triggers defined below.
app.register_blueprint(bp_renewal)
app.register_blueprint(bp_creator)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

USER_ID = "cda66539-6f2a-4a27-a5a3-a493061f8711"

LIST_IDS = {
    "Admin":          "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBKAAA=",
    "Deep":           "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBJAAA=",
    "Home":           "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBLAAA=",
    "Inbox":          "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG3VT6_AAA=",
    "Out":            "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBMAAA=",
    "Some Day/Maybe": "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG3VT7GAAA=",
    "Waiting For":    "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG3VT7FAAA=",
}

TASK_CHAINS_PATH = "/[00] Systems/Infrastructure/Monica/config/task-chains.json"


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def get_access_token() -> str:
    """
    Why: Obtains a Graph API bearer token via Managed Identity. Azure injects
    IDENTITY_ENDPOINT and IDENTITY_HEADER at runtime — no credentials are stored
    in code or environment variables. The token is scoped to the permissions
    granted to the Managed Identity: Tasks.ReadWrite.All and Files.Read.All.
    """
    endpoint = os.environ["IDENTITY_ENDPOINT"]
    header   = os.environ["IDENTITY_HEADER"]
    url = f"{endpoint}?api-version=2019-08-01&resource=https://graph.microsoft.com"
    response = requests.get(url, headers={"X-IDENTITY-HEADER": header})
    response.raise_for_status()
    return response.json()["access_token"]


def get_task_chains(token: str) -> list:
    """
    Why: Reads task-chains.json from OneDrive via Graph API. Storing chains
    in OneDrive means they can be updated without a code deployment — Monica
    picks up changes on the next webhook notification automatically.
    """
    url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_ID}"
        f"/drive/root:{TASK_CHAINS_PATH}:/content"
    )
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json()


def get_list_id(list_name: str) -> str | None:
    """
    Why: Translates a human-readable list name from task-chains.json into
    the stable Graph API list ID. Returns None for unrecognised names so
    the caller can skip gracefully rather than crash.
    """
    return LIST_IDS.get(list_name)


def task_exists(token: str, list_id: str, title: str) -> bool:
    """
    Why: Checks whether a task with the given title already exists in the
    target list, scoped to tasks created today. Graph often sends multiple
    notifications for a single completion event — without this guard Monica
    would create duplicate successor tasks. Scoping to today prevents false
    positives from recurring tasks that share a title across days.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_ID}"
        f"/todo/lists/{list_id}/tasks"
        f"?$filter=title eq '{title}'"
        f" and createdDateTime ge {today}T00:00:00Z"
    )
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if response.status_code != 200:
        logging.warning(
            f"task_exists check failed with {response.status_code} — "
            "assuming task does not exist to avoid blocking chain"
        )
        return False
    return len(response.json().get("value", [])) > 0


def create_task(
    token: str,
    list_id: str,
    title: str,
    category: str = None,
    due_time: str = None
) -> dict:
    """
    Why: Creates a successor task in the specified To Do list via Graph API.
    - category sets the named colour label in To Do (e.g. "Home", "Admin"),
      which matches the GTD context system.
    - due_time is an optional HH:MM string (UTC). When present it sets the
      task's due date to today at that time — used for Dry tasks so they
      surface at 19:00 on the day the Wash task is completed, not at midnight.
    """
    body: dict = {"title": title}

    if category:
        body["categories"] = [category]

    if due_time:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        body["dueDateTime"] = {
            "dateTime": f"{today}T{due_time}:00",
            "timeZone": "UTC"
        }

    url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_ID}"
        f"/todo/lists/{list_id}/tasks"
    )
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json=body
    )
    response.raise_for_status()
    return response.json()


# ─────────────────────────────────────────────────────────────────────────────
# HTTP TRIGGER — taskChain
# ─────────────────────────────────────────────────────────────────────────────

@app.route(route="taskChain", methods=["GET", "POST"])
def taskChain(req: func.HttpRequest) -> func.HttpResponse:
    """
    Why: A single route handles both sides of the Graph webhook lifecycle:

    GET  — Subscription validation. Graph sends a validationToken query
           parameter when a subscription is first registered. Monica must
           echo it back as plain text within 10 seconds or Graph rejects
           the subscription.

    POST — Task completion notification. Graph posts a JSON payload when
           a monitored task changes. Monica checks whether the task was
           completed, looks up the matching chain in task-chains.json,
           deduplicates, and creates the successor task.
    """

    # ── GET: Webhook validation ───────────────────────────────────────────────
    validation_token = req.params.get("validationToken")
    if validation_token:
        logging.info("Webhook validation request received — echoing token")
        return func.HttpResponse(
            body=validation_token,
            status_code=200,
            mimetype="text/plain"
        )

    # ── POST: Notification processing ────────────────────────────────────────
    try:
        body = req.get_json()
    except ValueError:
        logging.error("Request body could not be parsed as JSON")
        return func.HttpResponse("Bad request", status_code=400)

    notifications = body.get("value", [])
    if not notifications:
        logging.info("Notification payload contained no items")
        return func.HttpResponse("OK", status_code=200)

    token  = get_access_token()
    chains = get_task_chains(token)

    seen_list_ids: set = set()

    for notification in notifications:
        resource = notification.get("resource", "")
        parts = resource.split("/")

        list_id_from_resource = None
        if "lists" in parts:
            idx = parts.index("lists")
            if idx + 1 < len(parts):
                list_id_from_resource = parts[idx + 1]

        if list_id_from_resource in seen_list_ids:
            logging.info(f"Duplicate notification for list {list_id_from_resource} — skipping")
            continue
        if list_id_from_resource:
            seen_list_ids.add(list_id_from_resource)

        task_id = parts[-1] if parts else None
        if not task_id:
            logging.warning("Could not extract task ID from resource — skipping notification")
            continue

        task_url = f"https://graph.microsoft.com/v1.0/{resource}"
        task_response = requests.get(
            task_url,
            headers={"Authorization": f"Bearer {token}"}
        )
        if task_response.status_code != 200:
            logging.warning(f"Could not fetch task {task_id} ({task_response.status_code}) — skipping")
            continue

        task = task_response.json()

        if task.get("status") != "completed":
            logging.info(f"Task '{task.get('title')}' updated but not completed — ignoring")
            continue

        completed_title = task.get("title", "")
        logging.info(f"Completed task detected: '{completed_title}'")

        for chain in chains:
            if chain.get("trigger") != completed_title:
                continue

            successor_title  = chain.get("successor")
            target_list_name = chain.get("list")
            category         = chain.get("category")
            due_time         = chain.get("due_time")

            target_list_id = get_list_id(target_list_name)
            if not target_list_id:
                logging.warning(f"List name '{target_list_name}' not found in LIST_IDS — skipping chain")
                break

            if task_exists(token, target_list_id, successor_title):
                logging.info(f"Successor '{successor_title}' already exists today — skipping")
                break

            created = create_task(token, target_list_id, successor_title, category, due_time)
            logging.info(
                f"Successor task created: '{created.get('title')}' "
                f"in list '{target_list_name}'"
            )
            break

    return func.HttpResponse("OK", status_code=200)
