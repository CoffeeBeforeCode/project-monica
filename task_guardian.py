# task_guardian.py
#
# Why this file exists:
#   The morning task triggers fire at 05:00 UTC. If the host is not fully
#   stable at that moment — for example, immediately after a restart or an
#   infrastructure event — the triggers can be silently lost. Azure does not
#   replay missed timer events.
#
#   This guardian fires at 05:15 UTC every day — fifteen minutes after the
#   task triggers. It checks whether every expected task for today exists in
#   To Do. If any are missing, it creates them and sends a Leo alert to the
#   Daily Operations channel so the failure is never silent.
#
#   If everything is present, it logs a single clean OK line and exits.
#   The cost of running this check daily is negligible.
#
# Session 25 fix:
#   send_teams_alert rewritten to use the Bot Framework Connector API
#   (same pattern as email_digest.py) rather than the Graph chatMessage
#   endpoint. The Graph endpoint requires ChannelMessage.Send on the
#   Managed Identity, which is a delegated-only permission and cannot be
#   granted as an application permission. The Bot Framework pattern uses
#   client credentials (BOT_APP_ID + BOT_CLIENT_SECRET) and posts directly
#   to the channel ID — confirmed working in Session 24.
#
# Session 30 fix:
#   Alert message rewritten in Leo's voice — first contact of the day,
#   fact then action, no machinery explanation.
#   File header updated to reflect B1 plan (no longer Consumption plan).

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


def _get_bot_token() -> str:
    """
    Why: Acquires a Bot Framework token via client credentials flow.
    Separate from the Graph token because the Bot Framework and Graph
    use different OAuth audiences and cannot share a token.
    The same pattern is used in email_digest.py — kept identical so
    the two files behave consistently.
    App settings required (already present):
      BOT_APP_ID, BOT_CLIENT_SECRET, TENANT_ID
    """
    bot_app_id = os.environ["BOT_APP_ID"]
    bot_secret = os.environ["BOT_CLIENT_SECRET"]
    tenant_id  = os.environ["TENANT_ID"]

    resp = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     bot_app_id,
            "client_secret": bot_secret,
            "scope":         "https://api.botframework.com/.default",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


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


def send_teams_alert(recovered: list[str]) -> None:
    """
    Why: Posts a plain-text alert to the Daily Operations channel via the
    Bot Framework Connector API — the same direct-post pattern confirmed
    working in Session 24.

    Why Bot Framework rather than Graph chatMessage:
      Graph's POST /teams/{id}/channels/{id}/messages requires
      ChannelMessage.Send, which is a delegated-only permission. It cannot
      be granted to a Managed Identity as an application permission, making
      it unavailable to a timer-triggered Function with no signed-in user.
      The Bot Framework Connector uses client credentials (BOT_APP_ID and
      BOT_CLIENT_SECRET) and posts directly to the channel thread ID —
      no additional permissions required beyond what is already in place.

    Why Leo's voice:
      This is the first message of the day when the 05:00 run has failed.
      Leo states the fact, lists what he's recovered, and closes. No
      explanation of infrastructure. No hedging. Fact, action, done.

    App settings required (already present):
      BOT_APP_ID, BOT_CLIENT_SECRET, TENANT_ID,
      TEAMS_SERVICE_URL, TEAMS_DAILY_OPERATIONS_ID
    """
    service_url = os.environ.get("TEAMS_SERVICE_URL", "").rstrip("/")
    channel_id  = os.environ.get("TEAMS_DAILY_OPERATIONS_ID")
    bot_app_id  = os.environ.get("BOT_APP_ID")

    if not service_url or not channel_id or not bot_app_id:
        logging.warning(
            "Guardian: Teams alert skipped — "
            "TEAMS_SERVICE_URL, TEAMS_DAILY_OPERATIONS_ID or BOT_APP_ID not configured."
        )
        return

    task_lines = "\n".join(f"  • {t}" for t in recovered)
    message    = (
        f"Good morning, Phillip.\n\n"
        f"The 05:00 run didn't fire. I've recovered the missing tasks:\n\n"
        f"{task_lines}\n\n"
        f"Everything's in To Do."
    )

    try:
        bot_token = _get_bot_token()
    except Exception as e:
        logging.error(f"Guardian: failed to acquire bot token for Teams alert — {e}")
        return

    url     = f"{service_url}/v3/conversations/{channel_id}/activities"
    payload = {
        "type": "message",
        "from": {"id": f"28:{bot_app_id}", "name": "Leo"},
        "text": message,
    }

    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {bot_token}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=15,
        )
        if resp.status_code in (200, 201):
            logging.info("Guardian: Teams alert delivered to Daily Operations.")
        else:
            logging.error(
                f"Guardian: Teams alert failed — {resp.status_code} {resp.text}"
            )
    except Exception as e:
        logging.error(f"Guardian: Teams alert exception: {e}")


def get_expected_tasks_today() -> list[dict]:
    """
    Why: The single source of truth for what tasks should exist on any given day.
    Mirrors the logic in the individual task files exactly — same titles, same
    list IDs, same categories, same alternating-week calculation for Friday.
    Sunday tasks (Wash: Napkins) fire at 17:00 UTC, after this guardian's
    05:15 window. They are intentionally excluded — checking for a task that
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
                "list_id":      HOME_LIST_ID,
                "title":        "Vacuum: through and dust",
                "category":     "[00] System",
                "due_utc":      morning,
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
                "list_id":      HOME_LIST_ID,
                "title":        "Vacuum: through and dust",
                "category":     "[00] System",
                "due_utc":      morning,
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
                "list_id":      HOME_LIST_ID,
                "title":        "Vacuum: through and dust",
                "category":     "[00] System",
                "due_utc":      morning,
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
    schedule="0 15 5 * * *",   # 05:15 UTC every day
    arg_name="timer",
    run_on_startup=False
)
def taskGuardian(timer: func.TimerRequest) -> None:
    """
    Why: Fires every day at 05:15 UTC — fifteen minutes after the morning and
    day-of-week triggers at 05:00. Checks every expected task for today.
    Recovers any that are missing. Alerts Leo's Daily Operations channel
    if recovery was needed. Logs a clean OK if everything was present.

    This function is the answer to silent failure. If the host is not fully
    stable at 05:00 for any reason, tasks are lost without trace. This guardian
    ensures that loss is always detected, always recovered, and always visible.

    The 15-minute gap is deliberate: it gives the 05:00 triggers enough time
    to complete before the guardian checks whether they succeeded. Firing too
    soon would cause false recoveries on days when the triggers fired correctly
    but had not yet finished writing to To Do.
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
        send_teams_alert(recovered)
    else:
        logging.info("taskGuardian: All expected tasks present. No recovery needed. ✓")
