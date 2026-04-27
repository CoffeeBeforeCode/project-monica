import azure.functions as func
import logging
import os
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

bp = func.Blueprint()

USER_ID      = "cda66539-6f2a-4a27-a5a3-a493061f8711"
HOME_LIST_ID = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBLAAA="
ADMIN_LIST_ID = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBKAAA="
LONDON_TZ    = ZoneInfo("Europe/London")


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
    05:00 London always means 05:00 on your clock regardless of GMT/BST.
    """
    now_london = datetime.now(LONDON_TZ)
    local_dt   = now_london.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return local_dt.astimezone(timezone.utc)


@bp.timer_trigger(
    schedule="0 0 5 * * 1",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def createMondayTasks(timer: func.TimerRequest) -> None:
    """
    Why: Fires every Monday at 05:00 London local time. Blue Monday laundry
    and the weekly vacuum. The vacuum gets a 09:00 London reminder so it
    surfaces again mid-morning if not yet done.
    LinkedIn and Upwork are due at 09:00 so they surface at the start of
    the working day rather than alongside the household tasks at 05:00.
    """
    logging.info("createMondayTasks fired")
    token = get_access_token()
    if not token:
        return

    morning   = today_london_at(5, 0)
    workhour  = today_london_at(9, 0)
    reminder  = today_london_at(9, 0)

    create_todo_task(token, HOME_LIST_ID,  "Wash: Blue Monday",        "[00] System", due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Vacuum: through and dust", "[00] System", due_utc=morning, reminder_utc=reminder)
    create_todo_task(token, ADMIN_LIST_ID, "Check: LinkedIn",          "[02] Work",   due_utc=workhour)
    create_todo_task(token, ADMIN_LIST_ID, "Prospect: Upwork",         "[02] Work",   due_utc=workhour)
