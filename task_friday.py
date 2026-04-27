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
    05:00 London always means 05:00 on your clock regardless of GMT/BST.
    """
    now_london = datetime.now(LONDON_TZ)
    local_dt   = now_london.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return local_dt.astimezone(timezone.utc)


@bp.timer_trigger(
    schedule="0 0 5 * * 5",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def createFridayTasks(timer: func.TimerRequest) -> None:
    """
    Why: Fires every Friday at 05:00 London local time.

    Weekly tasks (every Friday):
      - Vacuum: through and dust (09:00 reminder)
      - Place: Black Bin outside
      - Place: Food Bin outside
      - Sweep: Curb
      - Empty: Bins
      - Check: LinkedIn / Prospect: Upwork

    Fortnightly Cycle A (from 1 May 2026):
      - Place: Brown Bin outside
      - Wash: Bedding

    Fortnightly Cycle B (from 8 May 2026):
      - Place: Green Bin outside
      - Place: Bottle Bin outside
      - Change: Bedding
      - Wash: AJC's Bath Towels
      - Wash: PJC's Bath Towel

    Why two separate fortnightly cycles rather than one:
      The bin collection and laundry schedules alternate on opposite weeks.
      NCRONTAB cannot express fortnightly patterns so the week is
      calculated in code from fixed start dates. weeks_since % 2 == 0
      identifies Cycle A weeks; % 2 == 1 identifies Cycle B weeks.
      Both cycles begin from their respective first occurrence dates —
      if now is before the start date (weeks_since < 0) neither fires,
      which prevents spurious tasks before the schedule begins.
    """
    logging.info("createFridayTasks fired")
    token = get_access_token()
    if not token:
        return

    now      = datetime.now(timezone.utc)
    morning  = today_london_at(5, 0)
    workhour = today_london_at(9, 0)
    reminder = today_london_at(9, 0)

    # ── Weekly tasks — every Friday ───────────────────────────────────────────
    create_todo_task(token, HOME_LIST_ID,  "Vacuum: through and dust", "[00] System", due_utc=morning, reminder_utc=reminder)
    create_todo_task(token, HOME_LIST_ID,  "Place: Black Bin outside", "[00] System", due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Place: Food Bin outside",  "[00] System", due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Sweep: Curb",              "[00] System", due_utc=morning)
    create_todo_task(token, HOME_LIST_ID,  "Empty: Bins",              "[00] System", due_utc=morning)

    # ── Fortnightly cycle calculation ─────────────────────────────────────────
    # Why datetime(2026, 5, 1): Cycle A begins on the first occurrence date.
    # Cycle B begins exactly one week later. weeks_since counts complete
    # 7-day periods elapsed since the cycle start — integer division
    # ensures a partial week does not advance the counter prematurely.
    cycle_a_start = datetime(2026, 5, 1, tzinfo=timezone.utc)
    weeks_since   = (now - cycle_a_start).days // 7

    is_cycle_a = weeks_since >= 0 and weeks_since % 2 == 0
    is_cycle_b = weeks_since >= 0 and weeks_since % 2 == 1

    # ── Cycle A tasks ─────────────────────────────────────────────────────────
    if is_cycle_a:
        create_todo_task(token, HOME_LIST_ID, "Place: Brown Bin outside", "[00] System", due_utc=morning)
        create_todo_task(token, HOME_LIST_ID, "Wash: Bedding",            "[00] System", due_utc=morning)

    # ── Cycle B tasks ─────────────────────────────────────────────────────────
    if is_cycle_b:
        create_todo_task(token, HOME_LIST_ID, "Place: Green Bin outside",  "[00] System", due_utc=morning)
        create_todo_task(token, HOME_LIST_ID, "Place: Bottle Bin outside", "[00] System", due_utc=morning)
        create_todo_task(token, HOME_LIST_ID, "Change: Bedding",           "[00] System", due_utc=morning)
        create_todo_task(token, HOME_LIST_ID, "Wash: AJC's Bath Towels",   "[00] System", due_utc=morning)
        create_todo_task(token, HOME_LIST_ID, "Wash: PJC's Bath Towel",    "[00] System", due_utc=morning)

    # ── Work tasks ────────────────────────────────────────────────────────────
    create_todo_task(token, ADMIN_LIST_ID, "Check: LinkedIn",  "[02] Work", due_utc=workhour)
    create_todo_task(token, ADMIN_LIST_ID, "Prospect: Upwork", "[02] Work", due_utc=workhour)
