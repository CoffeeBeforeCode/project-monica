import azure.functions as func
import logging
import requests
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# Why: Blueprint allows this function to live in its own file while still
# being registered under the main FunctionApp instance in function_app.py.
bp = func.Blueprint()

USER_ID   = "cda66539-6f2a-4a27-a5a3-a493061f8711"
LONDON_TZ = ZoneInfo("Europe/London")

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


def get_access_token() -> str:
    # Why: Managed Identity authentication — Azure injects these environment
    # variables at runtime. No credentials are stored in code.
    endpoint = os.environ["IDENTITY_ENDPOINT"]
    header   = os.environ["IDENTITY_HEADER"]
    url      = f"{endpoint}?api-version=2019-08-01&resource=https://graph.microsoft.com"
    response = requests.get(url, headers={"X-IDENTITY-HEADER": header})
    response.raise_for_status()
    return response.json()["access_token"]


def get_task_chains(token: str) -> list:
    # Why: Reads task-chains.json from OneDrive so chains can be updated
    # without a code deployment.
    url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_ID}"
        f"/drive/root:{TASK_CHAINS_PATH}:/content"
    )
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json()


def get_list_id(list_name: str) -> str | None:
    # Why: Translates list name to stable Graph API list ID.
    # Returns None for unrecognised names so the caller can skip gracefully.
    return LIST_IDS.get(list_name)


def get_recently_completed_tasks(token: str, list_id: str) -> list:
    # Why: Graph To Do webhooks notify at the list collection level — the
    # resource path ends in /tasks with no task ID appended. We therefore
    # cannot fetch a specific task by ID from the notification. Instead we
    # query the list for tasks completed in the last five minutes, which
    # is a wide enough window to catch any task that triggered this
    # notification without risking false positives from older completions.
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_ID}"
        f"/todo/lists/{list_id}/tasks"
        f"?$filter=status eq 'completed' and lastModifiedDateTime ge {cutoff}"
    )
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if response.status_code != 200:
        logging.warning(
            f"Could not fetch completed tasks from list {list_id} "
            f"({response.status_code}) — skipping"
        )
        return []
    return response.json().get("value", [])


def task_exists(token: str, list_id: str, title: str) -> bool:
    # Why: Prevents duplicate successor tasks. Graph often sends multiple
    # notifications for a single completion event. Scoped to today (London
    # date) to avoid false positives from recurring tasks with the same title.
    today_london = datetime.now(LONDON_TZ).strftime("%Y-%m-%d")
    url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_ID}"
        f"/todo/lists/{list_id}/tasks"
        f"?$filter=title eq '{title}'"
        f" and createdDateTime ge {today_london}T00:00:00Z"
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
    # Why: Creates the successor task in the correct To Do list.
    # category sets the GTD context label. due_time is a London local time
    # string (HH:MM) from task-chains.json — converted to UTC before sending
    # to Graph so Dry tasks surface at the correct local time year-round
    # regardless of GMT/BST.
    body: dict = {"title": title}
    if category:
        body["categories"] = [category]
    if due_time:
        # Why: Parse due_time as London local time and convert to UTC.
        # "19:00" in task-chains.json means 19:00 on your clock — 18:00 UTC
        # in BST, 19:00 UTC in GMT. Storing as London time in the JSON means
        # the value never needs updating at clock changes.
        now_london = datetime.now(LONDON_TZ)
        h, m       = int(due_time.split(":")[0]), int(due_time.split(":")[1])
        local_dt   = now_london.replace(hour=h, minute=m, second=0, microsecond=0)
        due_dt_utc = local_dt.astimezone(timezone.utc)
        body["dueDateTime"] = {
            "dateTime": due_dt_utc.strftime("%Y-%m-%dT%H:%M:%S.0000000"),
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


@bp.route(route="taskChain", methods=["GET", "POST"])
def taskChain(req: func.HttpRequest) -> func.HttpResponse:
    # Why: GET handles webhook validation — Graph sends a validationToken
    # when a subscription is first registered and expects it echoed back
    # as plain text within 10 seconds.
    validation_token = req.params.get("validationToken")
    if validation_token:
        logging.info("Webhook validation request received — echoing token")
        return func.HttpResponse(
            body=validation_token,
            status_code=200,
            mimetype="text/plain"
        )

    # Why: POST handles task completion notifications from Graph.
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

    # Why: Deduplicate by list ID within a single notification batch.
    # Graph sometimes sends multiple notifications for the same list in one POST.
    seen_list_ids: set = set()

    for notification in notifications:
        resource = notification.get("resource", "")

        # Why: Strip a leading slash before splitting. Graph registers
        # subscriptions with a leading slash in the resource path and echoes
        # that same path back in notifications. Without stripping it, split("/")
        # produces an empty string as the first element, which corrupts all
        # subsequent index lookups.
        parts = resource.lstrip("/").split("/")

        list_id_from_resource = None
        if "lists" in parts:
            idx = parts.index("lists")
            if idx + 1 < len(parts):
                list_id_from_resource = parts[idx + 1]

        if not list_id_from_resource:
            logging.warning(f"Could not extract list ID from resource '{resource}' — skipping")
            continue

        if list_id_from_resource in seen_list_ids:
            logging.info(f"Duplicate notification for list {list_id_from_resource} — skipping")
            continue

        seen_list_ids.add(list_id_from_resource)

        # Why: Query the list for recently completed tasks rather than
        # attempting to fetch a task by ID. Graph To Do webhooks notify at
        # the collection level (/tasks), not the individual task level
        # (/tasks/{taskId}), so there is no task ID to extract from the
        # resource path.
        completed_tasks = get_recently_completed_tasks(token, list_id_from_resource)

        if not completed_tasks:
            logging.info(f"No recently completed tasks found in list {list_id_from_resource}")
            continue

        for task in completed_tasks:
            completed_title = task.get("title", "")
            logging.info(f"Completed task detected: '{completed_title}'")

            for chain in chains:
                if chain.get("trigger_task") != completed_title:
                    continue

                successor_title  = chain.get("creates_task")
                target_list_name = chain.get("list")
                category         = chain.get("category")
                due_time         = chain.get("due_time")

                target_list_id = get_list_id(target_list_name)
                if not target_list_id:
                    logging.warning(
                        f"List name '{target_list_name}' not found in LIST_IDS — skipping chain"
                    )
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
