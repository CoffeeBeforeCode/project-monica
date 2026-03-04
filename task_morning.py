# task_morning.py
# Why: Creates daily morning tasks at 05:00 UTC. Self-contained — all helpers
# are defined in this file so a failure in any other task file cannot affect
# this one. Uses two lists: Home for personal routine tasks, Admin for the
# Chase account audit which Phillip reviews over morning coffee before 07:00.

import azure.functions as func
import logging
import os
import requests
from datetime import datetime, timezone

bp = func.Blueprint()

USER_ID = "cda66539-6f2a-4a27-a5a3-a493061f8711"
HOME_LIST_ID  = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBLAAA="
ADMIN_LIST_ID = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBKAAA="


def get_access_token() -> str | None:
    """
    Why: Acquires a token from the Managed Identity endpoint. This allows the
    Function to call Graph API without any stored credentials — Azure handles
    authentication automatically via the assigned identity.
    """
    identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
    identity_header   = os.environ.get("IDENTITY_HEADER")
    if not identity_endpoint or not identity_header:
        logging.error("Managed Identity environment variables not set.")
        return None
    try:
        response = requests.get(
            f"{identity_endpoint}?api-version=2019-08-01&resource=https://graph.microsoft.com",
            headers={"X-IDENTITY-HEADER": identity_header},
            timeout=10
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logging.error(f"Token acquisition failed: {e}")
        return None


def create_todo_task(token: str, list_id: str, title: str, category: str,
                     due_utc: datetime = None, reminder_utc: datetime = None) -> None:
    """
    Why: Single reusable function for task creation. Accepts list_id as a
    parameter so tasks can be routed to any To Do list — Home or Admin.
    Due and reminder times are optional UTC datetimes serialised to the
    ISO format the Graph API expects.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "title": title,
        "categories": [category]
    }
    if due_utc:
        body["dueDateTime"] = {
            "dateTime": due_utc.strftime("%Y-%m-%dT%H:%M:%S.0000000"),
            "timeZone": "UTC"
        }
    if reminder_utc:
        body["reminderDateTime"] = {
            "dateTime": reminder_utc.strftime("%Y-%m-%dT%H:%M:%S.0000000"),
            "timeZone": "UTC"
        }
        body["isReminderOn"] = True

    url = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/todo/lists/{list_id}/tasks"
    response = requests.post(url, headers=headers, json=body, timeout=10)
    if response.status_code == 201:
        logging.info(f"Created task: {title}")
    else:
        logging.error(f"Failed to create task: {title} - {response.status_code} {response.text}")


def today_utc_at(hour: int, minute: int = 0) -> datetime:
    """
    Why: Returns today's date at the given UTC hour and minute as an
    aware datetime. Keeps due time calculations readable at the call site.
    """
    now = datetime.now(timezone.utc)
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


@bp.timer_trigger(
    schedule="0 0 5 * * *",
    arg_name="timer",
    run_on_startup=False
)
def createMorningTasks(timer: func.TimerRequest) -> None:
    """
    Why: Fires every day at 05:00 UTC. Creates the core morning routine tasks
    plus the Chase account audit. First: Make the Bed is given a due time of
    04:00 so it sorts to the top of the list above all other 05:00 tasks.
    Audit: Chase accounts goes to the Admin list so it appears in the right
    GTD context for the morning review at 05:15.
    """
    logging.info("createMorningTasks fired")
    token = get_access_token()
    if not token:
        return

    top_of_list = today_utc_at(4, 0)
    morning     = today_utc_at(5, 0)

    # Home list — daily routine
    create_todo_task(token, HOME_LIST_ID,  "First: Make the Bed",  "[00] System", due_utc=top_of_list)
    create_todo_task(token, HOME_LIST_ID,  "Take: Morning pill",   "[01] Self",   due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Shower",               "[01] Self",   due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Train: Place",         "[05] Family", due_utc=morning)

    # Admin list — morning audit
    create_todo_task(token, ADMIN_LIST_ID, "Audit: Chase accounts", "[00] System", due_utc=morning)
