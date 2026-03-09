# task_guardian.py
#
# Why this file exists:
#   Azure Functions on a Consumption plan can be recycled by the platform for
#   maintenance at any time. When this happens during a 05:00 UTC trigger window,
#   the morning and day-of-week tasks are silently lost — the app is not running
#   to receive the trigger, and Azure does not replay missed timer events.
#
#   This guardian fires at 06:00 UTC every day — one hour after the task
#   triggers. It checks whether every expected task for today exists in To Do.
#   If any are missing, it creates them and sends a Leo alert to the Daily
#   Operations channel so the failure is never silent.
#
#   If everything is present, it logs a single clean OK line and exits.
#   The cost of running this check daily is negligible.

import azure.functions as func
import logging
import os
import requests
from datetime import datetime, timezone

bp = func.Blueprint()

USER_ID       = "cda66539-6f2a-4a27-a5a3-a493061f8711"
HOME_LIST_ID  = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBLAAA="
ADMIN_LIST_ID = "AAMkADk2MmYyN2U1LWRjZWQtNDJjOC1hMjFiLThlNzVjYzRmMDJmOQAuAAAAAAAfD4se_DbiSLJ1kLVyFgjcAQDiRt3FrJvhSa6XMQrXYM-wAAG5bJBKAAA="


def get_access_token() -> str | None:
    """
    Why: Acquires a token from the Managed Identity endpoint. No stored
    credentials — Azure handles authentication automatically. Identical
    pattern to all other task files so behaviour is consistent.
    """
    identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
    identity_header   = os.environ.get("IDENTITY_HEADER")
    if not identity_endpoint or not identity_header:
        logging.error("Guardian: Managed Identity environment variables not set.")
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
        logging.error(f"Guardian: Token acquisition failed: {e}")
        return None


def today_utc_at(hour: int, minute: int = 0) -> datetime:
    """
    Why: Returns today's date at the given UTC hour as an aware datetime.
    Consistent with all other task files — keeps due time calculations readable.
    """
    now = datetime.now(timezone.utc)
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


def task_exists_today(token: str, list_id: str, title: str) -> bool:
    """
    Why: Checks whether a task with the given title was created today (UTC)
    in the specified list. Fetches all tasks and filters in Python because the
    Graph API's OData filter support for todo task dates is limited and
    has proven unreliable in this project before.

    Returns True if found (original trigger fired correctly — no action needed).
    Returns False if not found OR if the query itself fails — it is safer to
    recreate a task than to skip recovery on a query error.
    """
    headers = {"Authorization": f"Bearer {token}"}
    url     = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/todo/lists/{list_id}/tasks"
    today   = datetime.now(timezone.utc).date()

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        tasks = response.json().get("value", [])
        for task in tasks:
            if task.get("title", "").strip() == title.strip():
                created_raw = task.get("createdDateTime", "")
                if created_raw:
                    created_date = datetime.fromisoformat(
                        created_raw.replace("Z", "+00:00")
                    ).date()
                    if created_date == today:
                        return True
        return False
    except Exception as e:
        logging.error(f"Guardian: task_exists_today failed for '{title}': {e}")
        return False


def create_todo_task(token: str, list_id: str, title: str, category: str,
                     due_utc: datetime = None, reminder_utc: datetime = None) -> bool:
    """
    Why: Same task creation pattern as all other task files. Returns True on
    success so the caller can build the recovery list for the Teams alert.
    """
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
        logging.info(f"Guardian: Recovered missing task: {title}")
        return True
    else:
        logging.error(f"Guardian: Failed to recover '{title}' — {response.status_code} {response.text}")
        return False


def send_teams_alert(token: str, recovered: list[str]) -> None:
    """
    Why: Posts a plain-text alert directly to the Daily Operations channel via
    the Graph API chatMessage endpoint. This is intentionally simpler than the
    Bot Framework connector — it requires only ChannelMessage.Send on the
    Managed Identity (see deployment note below).

    The recovery logic does not depend on this alert succeeding. If the
    permission is not yet granted, the alert fails and logs an error, but
    the tasks are already created. Recovery always happens; visibility is
    best-effort until the permission is confirmed.

    DEPLOYMENT NOTE: Before this alert will work, add ChannelMessage.Send
    to the Managed Identity in Azure AD. This is a separate step from the
    existing Tasks.ReadWrite.All and Files.Read.All permissions already granted.

    App settings required (already present from Session 21/22):
      TEAMS_TEAM_ID               — the Monica Team ID
      TEAMS_DAILY_OPERATIONS_ID   — the Daily Operations channel thread ID
    """
    team_id    = os.environ.get("TEAMS_TEAM_ID")
    channel_id = os.environ.get("TEAMS_DAILY_OPERATIONS_ID")

    if not team_id or not channel_id:
        logging.warning(
            "Guardian: Teams alert skipped — "
            "TEAMS_TEAM_ID or TEAMS_DAILY_OPERATIONS_ID not configured."
        )
        return

    task_lines = "\n".join(f"  • {t}" for t in recovered)
    message    = (
        f"⚠️ Leo — Guardian Alert\n\n"
        f"The following tasks were not created at 05:00 UTC and have been recovered "
        f"at 06:00 UTC:\n\n{task_lines}\n\n"
        f"Cause: Azure recycled the Consumption plan instance during the 05:00 trigger "
        f"window. Tasks are now in To Do. No action required from you."
    )

    url      = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages"
    headers  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body     = {"body": {"content": message}}

    try:
        response = requests.post(url, headers=headers, json=body, timeout=10)
        if response.status_code in (200, 201):
            logging.info("Guardian: Teams alert sent to Daily Operations.")
        else:
            logging.error(
                f"Guardian: Teams alert failed — {response.status_code} {response.text}"
            )
    except Exception as e:
        logging.error(f"Guardian: Teams alert exception: {e}")


def get_expected_tasks_today() -> list[dict]:
    """
    Why: The single source of truth for what tasks should exist on any given day.
    Mirrors the logic in the individual task files exactly — same titles, same
    list IDs, same categories, same alternating-week calculation for Friday.

    Sunday tasks (Wash: Napkins) fire at 17:00 UTC, after this guardian's
    06:00 window. They are intentionally excluded — checking for a task that
    legitimately hasn't been created yet would cause false recoveries every
    Sunday morning.

    Evening tasks (Take: Evening pill) fire at 17:00 UTC for the same reason
    and are excluded on the same logic.
    """
    now     = datetime.now(timezone.utc)
    weekday = now.weekday()  # Monday=0 … Sunday=6
    morning = today_utc_at(5, 0)
    tasks   = []

    # ── Daily morning tasks (every day, every week) ───────────────────────────
    tasks += [
        {
            "list_id":  HOME_LIST_ID,
            "title":    "First: Make the Bed",
            "category": "[00] System",
            "due_utc":  today_utc_at(4, 0),   # sorts above all 05:00 tasks
        },
        {
            "list_id":  HOME_LIST_ID,
            "title":    "Take: Morning pill",
            "category": "[01] Self",
            "due_utc":  morning,
        },
        {
            "list_id":  HOME_LIST_ID,
            "title":    "Shower",
            "category": "[01] Self",
            "due_utc":  morning,
        },
        {
            "list_id":  HOME_LIST_ID,
            "title":    "Train: Place",
            "category": "[05] Family",
            "due_utc":  morning,
        },
        {
            "list_id":  ADMIN_LIST_ID,
            "title":    "Audit: Chase accounts",
            "category": "[00] System",
            "due_utc":  morning,
        },
    ]

    # ── Day-of-week tasks ─────────────────────────────────────────────────────
    if weekday == 0:  # Monday
        tasks += [
            {
                "list_id":  HOME_LIST_ID,
                "title":    "Wash: Blue Monday",
                "category": "[00] System",
                "due_utc":  morning,
            },
            {
                "list_id":     HOME_LIST_ID,
                "title":       "Vacuum: through and dust",
                "category":    "[00] System",
                "due_utc":     morning,
                "reminder_utc": today_utc_at(9, 0),
            },
        ]

    elif weekday == 1:  # Tuesday
        tasks += [
            {
                "list_id":  HOME_LIST_ID,
                "title":    "Wash: Beige Tuesday",
                "category": "[00] System",
                "due_utc":  morning,
            },
        ]

    elif weekday == 2:  # Wednesday
        tasks += [
            {
                "list_id":  HOME_LIST_ID,
                "title":    "Wash: Black Wednesday",
                "category": "[00] System",
                "due_utc":  morning,
            },
            {
                "list_id":     HOME_LIST_ID,
                "title":       "Vacuum: through and dust",
                "category":    "[00] System",
                "due_utc":     morning,
                "reminder_utc": today_utc_at(9, 0),
            },
        ]

    elif weekday == 3:  # Thursday
        tasks += [
            {
                "list_id":  HOME_LIST_ID,
                "title":    "Wash: Ad-hoc Laundry",
                "category": "[00] System",
                "due_utc":  morning,
            },
        ]

    elif weekday == 4:  # Friday
        tasks += [
            {
                "list_id":     HOME_LIST_ID,
                "title":       "Vacuum: through and dust",
                "category":    "[00] System",
                "due_utc":     morning,
                "reminder_utc": today_utc_at(9, 0),
            },
        ]
        # Bath Towels: every 2nd Friday from 27 Feb 2026
        # Why: NCRONTAB cannot express alternating weeks, so we calculate
        # elapsed weeks from the series start date — matching task_friday.py exactly.
        bath_towels_start = datetime(2026, 2, 27, tzinfo=timezone.utc)
        weeks_since_bath  = (now - bath_towels_start).days // 7
        if weeks_since_bath >= 0 and weeks_since_bath % 2 == 0:
            tasks.append({
                "list_id":  HOME_LIST_ID,
                "title":    "Wash: Bath Towels",
                "category": "[00] System",
                "due_utc":  morning,
            })

        # Bedding: every 2nd Friday from 06 Mar 2026
        bedding_start       = datetime(2026, 3, 6, tzinfo=timezone.utc)
        weeks_since_bedding = (now - bedding_start).days // 7
        if weeks_since_bedding >= 0 and weeks_since_bedding % 2 == 0:
            tasks.append({
                "list_id":  HOME_LIST_ID,
                "title":    "Wash: Bedding",
                "category": "[00] System",
                "due_utc":  morning,
            })

    return tasks


@bp.timer_trigger(
    schedule="0 0 6 * * *",
    arg_name="timer",
    run_on_startup=False
)
def taskGuardian(timer: func.TimerRequest) -> None:
    """
    Why: Fires every day at 06:00 UTC — one hour after the morning and
    day-of-week triggers at 05:00. Checks every expected task for today.
    Recovers any that are missing. Alerts Leo's Daily Operations channel
    if recovery was needed. Logs a clean OK if everything was present.

    This function is the answer to silent failure. The Consumption plan
    can be recycled by Azure at any time. When that happens at 05:00, tasks
    are lost without trace. This guardian ensures that loss is always detected,
    always recovered, and always visible.
    """
    logging.info("taskGuardian fired")
    token = get_access_token()
    if not token:
        return

    expected  = get_expected_tasks_today()
    recovered = []

    for task in expected:
        if not task_exists_today(token, task["list_id"], task["title"]):
            success = create_todo_task(
                token,
                task["list_id"],
                task["title"],
                task["category"],
                due_utc      = task.get("due_utc"),
                reminder_utc = task.get("reminder_utc"),
            )
            if success:
                recovered.append(task["title"])

    if recovered:
        logging.warning(
            f"taskGuardian: Recovered {len(recovered)} missing task(s): {recovered}"
        )
        send_teams_alert(token, recovered)
    else:
        logging.info("taskGuardian: All expected tasks present. No recovery needed. ✓")
