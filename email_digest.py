"""
email_digest.py — Monica Email Digest Timer Trigger

Fires every 2 hours (05:00–19:00 UTC daily).
On Sundays, the 05:00 slot is suppressed in code.

Fetches emails received since the last digest run, groups them by
Outlook category, and delivers a plain-text summary to the Teams
Daily Operations channel via the Bot Framework Connector API.

WHY this file is self-contained:
  Each Blueprint file owns its own get_access_token() so that one
  broken file cannot take down the rest of the Function App. If the
  Graph token fails here, only this function errors — everything else
  keeps running.
"""

import os
import json
import logging
import requests
import azure.functions as func

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo          # Python 3.9+ — handles GMT/BST automatically
from azure.storage.blob import BlobServiceClient

# ── Blueprint registration ───────────────────────────────────────────────────
# WHY: The v2 Python model uses Blueprints so each file registers its own
# functions. function_app.py imports and registers this bp object.
bp = func.Blueprint()

# ── Constants ────────────────────────────────────────────────────────────────
LONDON_TZ   = ZoneInfo("Europe/London")   # handles GMT/BST switch on 29 Mar 2026
BLOB_CONTAINER = "monica-digest"          # container name in the storage account
BLOB_NAME      = "last_run.txt"           # stores the ISO timestamp of the last run

# Outlook categories used in the Monica categorisation system
CATEGORY_ORDER = [
    "[00] Action Required",
    "[00] Read Later",
    "[00] System",
    "[01] Self",
    "[02] Work",
    "[03] Friendship Circles",
    "[04] Community",
    "[05] Family",
    "[99] Archive",
]


# ── Timer Trigger ─────────────────────────────────────────────────────────────
@bp.timer_trigger(
    schedule="0 0 5,7,9,11,13,15,17,19 * * *",   # every 2 hours, 05:00–19:00 UTC
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def emailDigest(timer: func.TimerRequest) -> None:
    """
    WHY this schedule:
      The cron fires at 05, 07, 09, 11, 13, 15, 17, 19 UTC every day.
      On Sundays the 05:00 slot is suppressed below so the digest starts
      at 07:00 — Phillip has a slower Sunday morning routine.
    """
    now_utc    = datetime.now(timezone.utc)
    now_london = now_utc.astimezone(LONDON_TZ)
    tz_label   = "BST" if now_london.utcoffset() == timedelta(hours=1) else "GMT"

    # Suppress Sunday 05:00 UTC slot
    # WHY: A single cron expression is simpler than two separate ones.
    # The code guard handles the one slot we want to skip.
    if now_utc.weekday() == 6 and now_utc.hour == 5:
        logging.info("emailDigest: Sunday 05:00 UTC suppressed.")
        return

    logging.info(f"emailDigest: starting at {now_utc.isoformat()} UTC")

    # ── Step 1: Read last-run timestamp from Blob Storage ────────────────────
    # WHY: We only want emails that arrived since the previous digest.
    # Storing the timestamp externally means it survives Function App restarts.
    last_run_utc = _read_last_run()
    logging.info(f"emailDigest: last run was {last_run_utc.isoformat() if last_run_utc else 'never'}")

    # ── Step 2: Fetch emails from Microsoft Graph ─────────────────────────────
    token = get_access_token()
    if not token:
        logging.error("emailDigest: no access token — aborting")
        return
    emails = _fetch_emails(token, last_run_utc)
    logging.info(f"emailDigest: fetched {len(emails)} emails")

    # ── Step 3: Write the new last-run timestamp ──────────────────────────────
    # WHY: We write BEFORE formatting so that even if delivery fails the
    # window advances. We do not want to re-deliver the same batch.
    _write_last_run(now_utc)

    # ── Step 4: If no emails, send a brief confirmation and exit ─────────────
    if not emails:
        _send_to_teams(
            token,
            f"📭 No new emails since last digest ({_fmt_time(last_run_utc or now_utc, tz_label)})."
        )
        return

    # ── Step 5: Group emails by Outlook category ──────────────────────────────
    grouped = _group_by_category(emails)

    # ── Step 6: Format the digest message ────────────────────────────────────
    message = _format_digest(grouped, last_run_utc, now_london, tz_label)

    # ── Step 7: Deliver to Teams ──────────────────────────────────────────────
    _send_to_teams(token, message)
    logging.info("emailDigest: digest delivered successfully")


# ── Authentication ─────────────────────────────────────────────────────────────
def get_access_token() -> str | None:
    """
    Obtain a Microsoft Graph access token using the Function App's
    system-assigned Managed Identity.

    WHY IDENTITY_ENDPOINT and IDENTITY_HEADER:
      Azure Functions provides these two environment variables automatically
      at runtime. They point to a local token broker that the Functions host
      manages. This is the correct pattern for Azure Functions — the
      169.254.169.254 metadata address used by VMs does not work here and
      will time out. The task files (task_morning.py etc.) use this same
      pattern and are confirmed working.
    """
    identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
    identity_header   = os.environ.get("IDENTITY_HEADER")

    if not identity_endpoint or not identity_header:
        logging.error("emailDigest: Managed Identity environment variables not set.")
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
        logging.error(f"emailDigest: token acquisition failed: {e}")
        return None


def _get_bot_token() -> str:
    """
    Obtain a Bot Framework access token using the Bot App ID and client
    secret stored in Key Vault (surfaced as environment variables).

    WHY a separate token:
      The Graph token above is for reading email. The Bot Framework uses
      a different OAuth endpoint and audience to authorise message delivery
      to Teams. They are completely separate credential flows.
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


# ── Email fetching ─────────────────────────────────────────────────────────────
def _fetch_emails(token: str, since: datetime | None) -> list[dict]:
    """
    Fetch emails from the Inbox received after `since`.

    WHY $filter on receivedDateTime:
      Rather than pulling all unread mail and filtering in Python, we ask
      Graph to filter server-side. This keeps the payload small and avoids
      the 50-item default page limit becoming a problem over time.

    WHY top=100:
      A 2-hour window on a busy inbox might exceed 50 items. 100 is a
      reasonable ceiling; if more arrive we log a warning.
    """
    headers = {"Authorization": f"Bearer {token}"}

    if since:
        # Format to ISO 8601 without microseconds — Graph requires this form
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        filter_clause = f"receivedDateTime ge {since_str}"
    else:
        # First ever run: fall back to the last 2 hours
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        since_str = two_hours_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
        filter_clause = f"receivedDateTime ge {since_str}"

    url = (
        "https://graph.microsoft.com/v1.0/me/mailFolders/Inbox/messages"
        f"?$filter={filter_clause}"
        "&$top=100"
        "&$select=subject,from,receivedDateTime,categories,isRead"
        "&$orderby=receivedDateTime desc"
    )

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    emails = data.get("value", [])
    if data.get("@odata.nextLink"):
        logging.warning("emailDigest: more than 100 emails in window — some omitted")

    return emails


# ── Blob Storage helpers ───────────────────────────────────────────────────────
def _get_blob_client():
    """
    WHY AzureWebJobsStorage:
      This connection string is already required by the Azure Functions
      runtime for its own state management. We reuse it to store the
      last-run timestamp rather than provisioning a separate storage
      account. Azure resolves the Key Vault reference at runtime so
      os.environ['AzureWebJobsStorage'] returns the actual connection string.
    """
    conn_str = os.environ["AzureWebJobsStorage"]
    service  = BlobServiceClient.from_connection_string(conn_str)
    # Ensure the container exists (idempotent)
    try:
        service.create_container(BLOB_CONTAINER)
    except Exception:
        pass  # container already exists — not an error
    return service.get_blob_client(container=BLOB_CONTAINER, blob=BLOB_NAME)


def _read_last_run() -> datetime | None:
    """
    Read the ISO timestamp written by the previous digest run.
    Returns None if this is the first ever run.

    WHY store as plain ISO text:
      Simple, human-readable, and trivially editable from the Azure Portal
      Storage Explorer if we ever need to reset the window manually.
    """
    try:
        client = _get_blob_client()
        data   = client.download_blob().readall().decode("utf-8").strip()
        return datetime.fromisoformat(data).replace(tzinfo=timezone.utc)
    except Exception:
        return None  # blob not found — first run


def _write_last_run(timestamp: datetime) -> None:
    """Store the current run time so the next invocation can filter from here."""
    try:
        client = _get_blob_client()
        client.upload_blob(
            timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            overwrite=True,
        )
    except Exception as e:
        logging.error(f"emailDigest: failed to write last_run blob — {e}")


# ── Formatting helpers ─────────────────────────────────────────────────────────
def _group_by_category(emails: list[dict]) -> dict[str, list[dict]]:
    """
    Group emails by their first Outlook category.

    WHY first category only:
      An email can technically have multiple categories, but Monica's system
      assigns exactly one. Taking the first element keeps the logic simple.
    """
    grouped: dict[str, list[dict]] = {cat: [] for cat in CATEGORY_ORDER}
    grouped["Uncategorised"] = []

    for email in emails:
        cats = email.get("categories", [])
        key  = cats[0] if cats else "Uncategorised"
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(email)

    return grouped


def _fmt_time(dt: datetime, tz_label: str) -> str:
    """
    Format a UTC datetime for display in the London timezone.

    WHY convert to London time:
      The digest is for Phillip's benefit. Showing UTC timestamps requires
      mental arithmetic; showing the London local time is immediately useful.
    """
    local = dt.astimezone(LONDON_TZ)
    return local.strftime(f"%H:%M {tz_label} on %a %d %b")


def _format_digest(
    grouped:    dict[str, list[dict]],
    since:      datetime | None,
    now_london: datetime,
    tz_label:   str,
) -> str:
    """
    Build the plain-text digest message.

    WHY plain text for Session 18:
      Adaptive Cards require a separate well-tested payload structure.
      Plain text is delivered and readable immediately, with no render risk.
      Session 19 will replace this function body with Adaptive Card JSON.
    """
    tz_label_now = "BST" if now_london.utcoffset() == timedelta(hours=1) else "GMT"
    header_time  = now_london.strftime(f"%H:%M {tz_label_now}")

    if since:
        window_str = f"since {_fmt_time(since, tz_label)}"
    else:
        window_str = "last 2 hours (first run)"

    lines = [
        f"📬 Email Digest — {header_time}",
        f"Window: {window_str}",
        "",
    ]

    total = sum(len(v) for v in grouped.values())
    lines.append(f"{total} email(s) received")
    lines.append("")

    # Categorised emails — iterate in defined order then uncategorised
    ordered_keys = CATEGORY_ORDER + ["Uncategorised"]
    for cat in ordered_keys:
        items = grouped.get(cat, [])
        if not items:
            continue
        lines.append(f"── {cat} ({len(items)}) ──")
        for email in items[:10]:   # cap per-category to keep message readable
            sender  = email.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
            subject = email.get("subject", "(no subject)").strip()
            # Truncate long subjects so the message stays scannable
            if len(subject) > 60:
                subject = subject[:57] + "…"
            lines.append(f"  • {sender}: {subject}")
        if len(items) > 10:
            lines.append(f"  … and {len(items) - 10} more")
        lines.append("")

    return "\n".join(lines)


# ── Teams delivery ─────────────────────────────────────────────────────────────
def _send_to_teams(graph_token: str, text: str) -> None:
    """
    Post a plain-text message to the Teams Daily Operations channel
    via the Bot Framework Connector API.

    WHY Bot Framework Connector (not Graph API):
      The digest is delivered as a bot message — it appears as Monica
      speaking in the channel. Graph's /chats endpoint works differently
      and requires additional permissions. The Bot Framework Connector
      is the correct path for bot-originated messages.

    WHY TEAMS_DAILY_OPERATIONS_ID and TEAMS_SERVICE_URL from env:
      Both values were captured from the live bot interaction and stored
      in Key Vault during Session 12. Reading them from environment
      variables (Key Vault references) means no values are hardcoded and
      the delivery target can be changed in Key Vault without a redeploy.
    """
    bot_token    = _get_bot_token()
    service_url  = os.environ["TEAMS_SERVICE_URL"].rstrip("/")
    conversation = os.environ["TEAMS_DAILY_OPERATIONS_ID"]
    bot_app_id   = os.environ["BOT_APP_ID"]

    url = f"{service_url}/v3/conversations/{conversation}/activities"

    payload = {
        "type": "message",
        "from": {"id": bot_app_id},
        "text": text,
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type":  "application/json",
        },
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    logging.info(f"emailDigest: Teams delivery status {resp.status_code}")
