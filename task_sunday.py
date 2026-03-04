# task_sunday.py
import azure.functions as func
import logging
import os
import requests
from datetime import datetime, timezone

bp = func.Blueprint()

USER_ID      = "cda66539-6f2a-4a27-a5a3-a493061f8711"
HOME_LIST_ID = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBLAAA="


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
    body = {"title": title, "categories": [category]}
    if due_utc:
        body["dueDateTime"] = {"dateTime": due_utc.strftime("%Y-%m-%dT%H:%M:%S.0000000"), "timeZone": "UTC"}
    if reminder_utc:
        body["reminderDateTime"] = {"dateTime": reminder_utc.strftime("%Y-%m-%dT%H:%M:%S.0000000"), "timeZone": "UTC"}
        body["isReminderOn"] = True
    url = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/todo/lists/{list_id}/tasks"
    response = requests.post(url, headers=headers, json=body, timeout=10)
    if response.status_code == 201:
        logging.info(f"Created task: {title}")
    else:
        logging.error(f"Failed to create task: {title} - {response.status_code} {response.text}")


def today_utc_at(hour: int, minute: int = 0) -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


@bp.timer_trigger(
    schedule="0 0 17 * * 0",
    arg_name="timer",
    run_on_startup=False
)
def createSundayTasks(timer: func.TimerRequest) -> None:
    """
    Why: Sunday napkins wash fires at 17:00 UTC — same logic as the evening
    pill. A Sunday morning trigger would fire before the napkins from the
    previous week's dinner are ready to wash.
    """
    logging.info("createSundayTasks fired")
    token = get_access_token()
    if not token:
        return
    create_todo_task(token, HOME_LIST_ID, "Wash: Napkins", "[00] System", due_utc=today_utc_at(17, 0))
