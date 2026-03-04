# task_creator.py
# Why: All scheduled task creation lives here, separate from the webhook handler
# in function_app.py. Using a Blueprint keeps each concern in its own file
# while still sharing the single FunctionApp instance defined in function_app.py.

import azure.functions as func
import logging
import os
import requests
from datetime import datetime, timezone, timedelta

bp_creator = func.Blueprint()

USER_ID = "cda66539-6f2a-4a27-a5a3-a493061f8711"
HOME_LIST_ID = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBLAAA="


def get_access_token() -> str | None:
    """
    Why: Managed Identity token acquisition. Duplicated from webhook_renewal.py
    for the same reason — self-contained files avoid cross-module import issues
    in the Azure Functions runtime.
    """
    identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
    identity_header = os.environ.get("IDENTITY_HEADER")
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


def create_todo_task(token: str, title: str, category: str, due_utc: datetime = None, reminder_utc: datetime = None) -> None:
    """
    Why: Single reusable function for task creation. Accepts optional due and
    reminder datetimes so each timer can specify exactly when the task should
    appear and remind. All times are passed as UTC datetime objects and
    serialised to the ISO format Graph expects.
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

    url = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/todo/lists/{HOME_LIST_ID}/tasks"
    response = requests.post(url, headers=headers, json=body, timeout=10)
    if response.status_code == 201:
        logging.info(f"Created task: {title}")
    else:
        logging.error(f"Failed to create task: {title} - {response.status_code} {response.text}")


def today_utc_at(hour: int, minute: int = 0) -> datetime:
    """
    Why: Helper that returns a UTC datetime for today at a given hour and minute.
    Used to set due and reminder times relative to when the timer fires.
    """
    now = datetime.now(timezone.utc)
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


# --- Daily 05:00 UTC Timer ---
# Why: Creates the four morning tasks every day at 05:00 UTC.
# First: Make the Bed is given a due time of 04:00 UTC so it sorts
# to the top of the list above all other 05:00 tasks.
@bp_creator.timer_trigger(
    schedule="0 0 5 * * *",
    arg_name="timer",
    run_on_startup=False
)
def createMorningTasks(timer: func.TimerRequest) -> None:
    logging.info("createMorningTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(5, 0)
    top_of_list = today_utc_at(4, 0)

    create_todo_task(token, "First: Make the Bed",    "[00] System", due_utc=top_of_list)
    create_todo_task(token, "Take: Morning pill",     "[01] Self",   due_utc=today)
    create_todo_task(token, "Shower",                 "[01] Self",   due_utc=today)
    create_todo_task(token, "Train: Place",           "[05] Family", due_utc=today)


# --- Daily 17:00 UTC Timer ---
# Why: Evening pill is only relevant from 17:00 onwards.
# Creating it at 17:00 means it never clutters the morning list.
@bp_creator.timer_trigger(
    schedule="0 0 17 * * *",
    arg_name="timer",
    run_on_startup=False
)
def createEveningTasks(timer: func.TimerRequest) -> None:
    logging.info("createEveningTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(17, 0)
    create_todo_task(token, "Take: Evening pill", "[01] Self", due_utc=today)


# --- Monday 05:00 UTC Timer ---
@bp_creator.timer_trigger(
    schedule="0 0 5 * * 1",
    arg_name="timer",
    run_on_startup=False
)
def createMondayTasks(timer: func.TimerRequest) -> None:
    logging.info("createMondayTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(5, 0)
    reminder = today_utc_at(9, 0)
    create_todo_task(token, "Wash: Blue Monday",        "[00] System", due_utc=today)
    create_todo_task(token, "Vacuum: through and dust", "[00] System", due_utc=today, reminder_utc=reminder)


# --- Tuesday 05:00 UTC Timer ---
@bp_creator.timer_trigger(
    schedule="0 0 5 * * 2",
    arg_name="timer",
    run_on_startup=False
)
def createTuesdayTasks(timer: func.TimerRequest) -> None:
    logging.info("createTuesdayTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(5, 0)
    create_todo_task(token, "Wash: Beige Tuesday", "[00] System", due_utc=today)


# --- Wednesday 05:00 UTC Timer ---
@bp_creator.timer_trigger(
    schedule="0 0 5 * * 3",
    arg_name="timer",
    run_on_startup=False
)
def createWednesdayTasks(timer: func.TimerRequest) -> None:
    logging.info("createWednesdayTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(5, 0)
    reminder = today_utc_at(9, 0)
    create_todo_task(token, "Wash: Black Wednesday",    "[00] System", due_utc=today)
    create_todo_task(token, "Vacuum: through and dust", "[00] System", due_utc=today, reminder_utc=reminder)


# --- Thursday 05:00 UTC Timer ---
@bp_creator.timer_trigger(
    schedule="0 0 5 * * 4",
    arg_name="timer",
    run_on_startup=False
)
def createThursdayTasks(timer: func.TimerRequest) -> None:
    logging.info("createThursdayTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(5, 0)
    create_todo_task(token, "Wash: Ad-hoc Laundry", "[00] System", due_utc=today)


# --- Friday 05:00 UTC Timer ---
# Why: Two alternating fortnightly washes fall on Fridays — Bath Towels and Bedding.
# Rather than trying to calculate alternating weeks in a cron expression (which
# is not supported), we use a single Friday trigger and calculate which wash
# is due based on the number of weeks elapsed since each series started.
# Bath Towels: commencing 27 Feb 2026 (week 0, 2, 4...)
# Bedding:     commencing 06 Mar 2026 (week 0, 2, 4...)
@bp_creator.timer_trigger(
    schedule="0 0 5 * * 5",
    arg_name="timer",
    run_on_startup=False
)
def createFridayTasks(timer: func.TimerRequest) -> None:
    logging.info("createFridayTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(5, 0)
    reminder = today_utc_at(9, 0)
    create_todo_task(token, "Vacuum: through and dust", "[00] System", due_utc=today, reminder_utc=reminder)

    # Bath Towels: every 2nd Friday from 27 Feb 2026
    bath_towels_start = datetime(2026, 2, 27, tzinfo=timezone.utc)
    weeks_since_bath = (today - bath_towels_start).days // 7
    if weeks_since_bath >= 0 and weeks_since_bath % 2 == 0:
        create_todo_task(token, "Wash: Bath Towels", "[00] System", due_utc=today)

    # Bedding: every 2nd Friday from 06 Mar 2026
    bedding_start = datetime(2026, 3, 6, tzinfo=timezone.utc)
    weeks_since_bedding = (today - bedding_start).days // 7
    if weeks_since_bedding >= 0 and weeks_since_bedding % 2 == 0:
        create_todo_task(token, "Wash: Bedding", "[00] System", due_utc=today)


# --- Sunday 17:00 UTC Timer ---
@bp_creator.timer_trigger(
    schedule="0 0 17 * * 0",
    arg_name="timer",
    run_on_startup=False
)
def createSundayTasks(timer: func.TimerRequest) -> None:
    logging.info("createSundayTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(17, 0)
    create_todo_task(token, "Wash: Napkins", "[00] System", due_utc=today)


# --- Monthly 05:00 UTC on 1st Timer ---
# Why: NCRONTAB syntax does not support "first day of month" directly in the
# day-of-week field, but it does support day-of-month. The expression
# "0 0 5 1 * *" fires at 05:00 UTC on the 1st of every month.
@bp_creator.timer_trigger(
    schedule="0 0 5 1 * *",
    arg_name="timer",
    run_on_startup=False
)
def createMonthlyTasks(timer: func.TimerRequest) -> None:
    logging.info("createMonthlyTasks fired")
    token = get_access_token()
    if not token:
        return

    today = today_utc_at(5, 0)
    create_todo_task(token, "Audit: Credit Score (Chase/Discover)", "[00] System", due_utc=today)
    create_todo_task(token, "Update: Financial Position",           "[00] System", due_utc=today)
