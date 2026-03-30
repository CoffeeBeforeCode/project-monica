# task_morning.py
# Why: Creates daily morning tasks at 05:00 London local time. Self-contained —
# all helpers are defined here so a failure in this file cannot affect other
# functions.
import azure.functions as func
import logging
import os
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

bp = func.Blueprint()

USER_ID       = "cda66539-6f2a-4a27-a5a3-a493061f8711"
HOME_LIST_ID  = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBLAAA="
ADMIN_LIST_ID = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBKAAA="
LONDON_TZ     = ZoneInfo("Europe/London")


def get_access_token() -> str | None:
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
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body    = {"title": title, "categories": [category]}
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
    url      = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/todo/lists/{list_id}/tasks"
    response = requests.post(url, headers=headers, json=body, timeout=10)
    if response.status_code == 201:
        logging.info(f"Created task: {title}")
    else:
        logging.error(f"Failed to create task: {title} - {response.status_code} {response.text}")


def today_london_at(hour: int, minute: int = 0) -> datetime:
    """
    Why: Takes a London local hour and returns it as a UTC-aware datetime.
    05:00 London always means 05:00 on your clock — 04:00 UTC in BST,
    05:00 UTC in GMT. WEBSITE_TIME_ZONE=Europe/London controls when this
    function fires; this helper ensures due times shown in To Do are also
    correct local time year-round.
    """
    now_london = datetime.now(LONDON_TZ)
    local_dt   = now_london.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return local_dt.astimezone(timezone.utc)


@bp.timer_trigger(
    schedule="0 0 5 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def createMorningTasks(timer: func.TimerRequest) -> None:
    """
    Why: Fires every day at 05:00 London local time. Creates the core morning
    routine tasks plus the Chase account audit. First: Make the Bed is given
    a due time of 04:00 London so it sorts above all other 05:00 tasks and
    appears at the top of the list.
    """
    logging.info("createMorningTasks fired")
    token = get_access_token()
    if not token:
        return

    top_of_list = today_london_at(4, 0)
    morning     = today_london_at(5, 0)

    create_todo_task(token, HOME_LIST_ID,  "First: Make the Bed",   "[00] System", due_utc=top_of_list)
    create_todo_task(token, HOME_LIST_ID,  "Take: Morning pill",    "[01] Self",   due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Shower",                "[01] Self",   due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Train: Place",          "[05] Family", due_utc=morning)
    create_todo_task(token, ADMIN_LIST_ID, "Audit: Chase accounts", "[00] System", due_utc=morning)
