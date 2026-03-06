"""
email_digest.py — Monica Email Digest Timer Trigger

Fires every 2 hours (05:00–19:00 UTC daily).
On Sundays, the 05:00 slot is suppressed in code.

Fetches emails received since the last digest run and delivers one
Adaptive Card per email to the Teams Daily Operations channel via the
Bot Framework Connector API.

WHY this file is self-contained:
  Each Blueprint file owns its own get_access_token() so that one
  broken file cannot take down the rest of the Function App. If the
  Graph token fails here, only this function errors — everything else
  keeps running.

Session 19 change:
  Plain text formatting replaced with Adaptive Cards. One card per
  email, ordered newest first. Each card contains sender details,
  subject, body preview, and four triage action buttons.

Session 20 change:
  Sender profile photo added to each card. Resolution order:
    1. Internal M365 user photo (Graph /users/{email}/photo/$value)
    2. Saved contact photo (Graph /me/contacts filtered by email)
    3. Envelope icon fallback (embedded SVG, no external dependency)
"""

import os
import json
import logging
import time
import base64
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
LONDON_TZ      = ZoneInfo("Europe/London")   # handles GMT/BST switch on 29 Mar 2026
BLOB_CONTAINER = "monica-digest"             # container name in the storage account
BLOB_NAME      = "last_run.txt"              # stores the ISO timestamp of the last run

# WHY 0.3 seconds between cards:
#   Sending many cards in rapid succession can hit Bot Framework rate limits.
#   A small delay keeps us well within the per-second limit without
#   meaningfully slowing delivery — 20 cards still arrive in under 10 seconds.
CARD_SEND_DELAY = 0.3

# WHY an embedded SVG rather than a hosted URL:
#   An external URL would create a runtime dependency on a third-party host.
#   If that host is unavailable, every card in the digest would show a broken
#   image. Embedding the icon as a base64 data URI means it always renders,
#   with zero external dependencies. Teams Adaptive Cards support data URIs
#   in Image elements.
ENVELOPE_ICON = (
    "data:image/svg+xml;base64,"
    "PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAy"
    "NCAyNCIgZmlsbD0ibm9uZSIgc3Ryb2tlPSIjODg4ODg4IiBzdHJva2Utd2lkdGg9IjEuNSIg"
    "c3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIj48cmVjdCB4"
    "PSIyIiB5PSI0IiB3aWR0aD0iMjAiIGhlaWdodD0iMTYiIHJ4PSIyIiByeT0iMiIvPjxwb2x5"
    "bGluZSBwb2ludHM9IjIsNCAxMiwxMyAyMiw0Ii8+PC9zdmc+"
)


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
    # WHY: We write BEFORE delivery so that even if delivery fails the window
    # advances. We do not want to re-deliver the same batch next run.
    _write_last_run(now_utc)

    # ── Step 4: If no emails, send a brief plain-text confirmation and exit ───
    if not emails:
        since_label = _fmt_time(last_run_utc or now_utc, tz_label)
        _send_text_to_teams(f"📭 No new emails since last digest ({since_label}).")
        return

    # ── Step 5: Send one Adaptive Card per email, newest first ───────────────
    # WHY newest first: emails already arrive ordered by receivedDateTime desc
    # from the Graph query. No re-sorting needed.
    for email in emails:
        card = _build_card(email, tz_label, token)
        _send_card_to_teams(card)
        time.sleep(CARD_SEND_DELAY)   # avoid Bot Framework rate limits

    logging.info(f"emailDigest: {len(emails)} card(s) delivered successfully")


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

    WHY bodyPreview and id in $select:
      bodyPreview gives us the first ~255 characters of the email body,
      which we display in the card. id is the Graph message identifier
      we embed in each triage button's data payload so the messages
      function knows which email to act on when a button is pressed.
    """
    headers = {"Authorization": f"Bearer {token}"}

    if since:
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        filter_clause = f"receivedDateTime ge {since_str}"
    else:
        # First ever run: fall back to the last 2 hours
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        since_str = two_hours_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
        filter_clause = f"receivedDateTime ge {since_str}"

    url = (
        "https://graph.microsoft.com/v1.0/users/cda66539-6f2a-4a27-a5a3-a493061f8711/mailFolders/Inbox/messages"
        f"?$filter={filter_clause}"
        "&$top=100"
        "&$select=id,subject,from,receivedDateTime,categories,isRead,bodyPreview"
        "&$orderby=receivedDateTime desc"
    )

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    emails = data.get("value", [])
    if data.get("@odata.nextLink"):
        logging.warning("emailDigest: more than 100 emails in window — some omitted")

    return emails


# ── Sender photo resolution ────────────────────────────────────────────────────
def _get_sender_photo(token: str, sender_email: str) -> str:
    """
    Resolve a sender's profile photo to a base64 data URI.

    WHY base64 data URI rather than a URL:
      Graph photo endpoints return raw binary image data, not a URL. They
      also require an authorisation header — they cannot be used as a src
      attribute in an Adaptive Card Image element directly. Converting to
      a base64 data URI packages the binary as a self-contained string that
      Adaptive Cards can render without any further HTTP calls or auth.

    WHY image/jpeg as the MIME type regardless of file extension:
      JPEG images use the MIME type image/jpeg whether the file on disk is
      named .jpg or .jpeg. The extension is irrelevant to the binary format.
      Graph always returns JPEG data from its photo endpoints, so
      image/jpeg is always correct here.

    Resolution order:
      1. Internal M365 user — Graph /users/{email}/photo/$value
         Works for anyone in the same tenant (colleagues, licensed users).
      2. Saved contact — Graph /me/contacts filtered by email address,
         then fetch that contact's stored photo.
         Works for external senders Phillip has saved with a photo.
      3. Envelope icon fallback — the ENVELOPE_ICON constant defined above.
         Used when neither lookup finds a photo. Always succeeds.
    """
    headers = {"Authorization": f"Bearer {token}"}

    # ── Attempt 1: internal M365 user photo ──────────────────────────────────
    try:
        resp = requests.get(
            f"https://graph.microsoft.com/v1.0/users/{sender_email}/photo/$value",
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            encoded = base64.b64encode(resp.content).decode("utf-8")
            logging.info(f"emailDigest: internal photo found for {sender_email}")
            return f"data:image/jpeg;base64,{encoded}"
    except Exception as e:
        logging.debug(f"emailDigest: internal photo lookup failed for {sender_email} — {e}")

    # ── Attempt 2: saved contact photo ───────────────────────────────────────
    # WHY filter by emailAddresses/any():
    #   A contact can have multiple email addresses. The any() OData
    #   operator checks all of them, so we catch contacts regardless of
    #   which address was used as the primary.
    try:
        search_url = (
            "https://graph.microsoft.com/v1.0/me/contacts"
            f"?$filter=emailAddresses/any(e:e/address eq '{sender_email}')"
            "&$select=id,displayName"
            "&$top=1"
        )
        search_resp = requests.get(search_url, headers=headers, timeout=10)
        if search_resp.status_code == 200:
            contacts = search_resp.json().get("value", [])
            if contacts:
                contact_id = contacts[0]["id"]
                photo_resp = requests.get(
                    f"https://graph.microsoft.com/v1.0/me/contacts/{contact_id}/photo/$value",
                    headers=headers,
                    timeout=10,
                )
                if photo_resp.status_code == 200:
                    encoded = base64.b64encode(photo_resp.content).decode("utf-8")
                    logging.info(f"emailDigest: contact photo found for {sender_email}")
                    return f"data:image/jpeg;base64,{encoded}"
    except Exception as e:
        logging.debug(f"emailDigest: contact photo lookup failed for {sender_email} — {e}")

    # ── Attempt 3: envelope icon fallback ────────────────────────────────────
    logging.debug(f"emailDigest: no photo found for {sender_email} — using envelope icon")
    return ENVELOPE_ICON


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


# ── Time formatting helper ─────────────────────────────────────────────────────
def _fmt_time(dt: datetime, tz_label: str) -> str:
    """
    Format a UTC datetime for display in the London timezone.

    WHY convert to London time:
      The digest is for Phillip's benefit. Showing UTC timestamps requires
      mental arithmetic; showing the London local time is immediately useful.
    """
    local = dt.astimezone(LONDON_TZ)
    return local.strftime(f"%H:%M {tz_label} on %a %d %b")


# ── Adaptive Card builder ──────────────────────────────────────────────────────
def _build_card(email: dict, tz_label: str, token: str) -> dict:
    """
    Build one Adaptive Card dict for a single email.

    WHY one card per email:
      The design principle is A&E triage — each email is a decision, and
      the card is the triage interface. A summary card before individual
      cards would create another inbox and add a step. The individual
      card IS the triage.

    WHY Container with style=emphasis as the outer wrapper:
      Teams overrides backgroundColor on cards, so we cannot force a
      dark background. Instead we use the native emphasis style, which
      renders as a grey card in both light and dark Teams themes.
      bleed=True extends the background to the full card edge.

    WHY default-style containers for triage buttons:
      White/light tiles against the grey card background give natural
      contrast without any colour overrides. selectAction makes the
      entire container clickable — the label is centred within it —
      giving us full-width, equal-sized tappable areas.

    WHY emailId in button data:
      When a button is pressed, Teams sends an Action.Submit payload to
      the /api/messages endpoint. The messages function needs the Graph
      message ID to know which email to move, flag, or delete. We embed
      it here so no state lookup is required at action time.

    WHY we pass token into _build_card:
      Photo resolution requires a Graph API call. Rather than obtaining
      a second token inside the photo helper, we pass the token already
      acquired by the main digest loop. One token, one acquisition.

    NOTE — View button deep link:
      A proper per-message Outlook deep link requires URL-encoding the
      Graph message ID. For now the View button opens the Outlook inbox.
      Per-message deep links are a future refinement.
    """
    # ── Extract email fields ──────────────────────────────────────────────────
    sender_name  = email.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
    sender_email = email.get("from", {}).get("emailAddress", {}).get("address", "")
    subject      = (email.get("subject", "") or "(no subject)").strip()
    body_preview = (email.get("bodyPreview", "") or "").strip()
    email_id     = email.get("id", "")

    # Truncate preview to ~150 characters — keeps cards a consistent height
    if len(body_preview) > 150:
        body_preview = body_preview[:147] + "…"

    # Convert received time to London local time for display
    received_str = email.get("receivedDateTime", "")
    try:
        received_utc    = datetime.fromisoformat(received_str.replace("Z", "+00:00"))
        received_london = received_utc.astimezone(LONDON_TZ)
        time_label      = received_london.strftime("%H:%M")
    except Exception:
        time_label = ""

    # ── Resolve sender photo ──────────────────────────────────────────────────
    # WHY we resolve photo per card rather than batching:
    #   Each email may have a different sender. Batching would require
    #   collecting all senders first, then resolving, then building cards —
    #   more complex with no meaningful performance gain at typical digest
    #   volumes (2–20 emails). Per-card resolution keeps the logic simple.
    photo_uri = _get_sender_photo(token, sender_email) if sender_email else ENVELOPE_ICON

    # ── Build and return the card dict ────────────────────────────────────────
    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [
            {
                "type": "Container",
                "style": "emphasis",
                "bleed": True,
                "items": [
                    # ── Sender row: photo + name, email address, timestamp ────
                    {
                        "type": "ColumnSet",
                        "columns": [
                            # Photo column — fixed width, vertically centred
                            {
                                "type": "Column",
                                "width": "auto",
                                "verticalContentAlignment": "Center",
                                "spacing": "Small",
                                "items": [
                                    {
                                        "type": "Image",
                                        "url": photo_uri,
                                        "size": "Small",
                                        "style": "Person",
                                        "altText": f"Photo of {sender_name}"
                                    }
                                ]
                            },
                            # Name + email + timestamp column
                            {
                                "type": "Column",
                                "width": "stretch",
                                "spacing": "Small",
                                "items": [
                                    {
                                        "type": "TextBlock",
                                        "text": sender_name,
                                        "weight": "Bolder",
                                        "wrap": True,
                                        "spacing": "None"
                                    },
                                    {
                                        "type": "ColumnSet",
                                        "spacing": "None",
                                        "columns": [
                                            {
                                                "type": "Column",
                                                "width": "stretch",
                                                "items": [
                                                    {
                                                        "type": "TextBlock",
                                                        "text": sender_email,
                                                        "isSubtle": True,
                                                        "size": "Small",
                                                        "wrap": True,
                                                        "spacing": "None"
                                                    }
                                                ]
                                            },
                                            {
                                                "type": "Column",
                                                "width": "auto",
                                                "items": [
                                                    {
                                                        "type": "TextBlock",
                                                        "text": time_label,
                                                        "isSubtle": True,
                                                        "size": "Small",
                                                        "horizontalAlignment": "Right",
                                                        "spacing": "None"
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    # ── Subject line ──────────────────────────────────────────
                    {
                        "type": "TextBlock",
                        "text": subject,
                        "weight": "Bolder",
                        "size": "Medium",
                        "wrap": True,
                        "spacing": "Small"
                    },
                    # ── Body preview ──────────────────────────────────────────
                    {
                        "type": "TextBlock",
                        "text": body_preview,
                        "isSubtle": True,
                        "wrap": True,
                        "maxLines": 3,
                        "spacing": "Small"
                    },
                    # ── Triage buttons — 2×2 grid ─────────────────────────────
                    {
                        "type": "ColumnSet",
                        "spacing": "Medium",
                        "columns": [
                            {
                                "type": "Column",
                                "width": "stretch",
                                "spacing": "Small",
                                "items": [
                                    {
                                        "type": "Container",
                                        "style": "default",
                                        "spacing": "Small",
                                        "selectAction": {
                                            "type": "Action.Submit",
                                            "data": {
                                                "triageAction": "action",
                                                "emailId": email_id
                                            }
                                        },
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "Action",
                                                "horizontalAlignment": "Center",
                                                "weight": "Bolder",
                                                "spacing": "Small"
                                            }
                                        ]
                                    },
                                    {
                                        "type": "Container",
                                        "style": "default",
                                        "spacing": "Small",
                                        "selectAction": {
                                            "type": "Action.Submit",
                                            "data": {
                                                "triageAction": "waiting",
                                                "emailId": email_id
                                            }
                                        },
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "Waiting For",
                                                "horizontalAlignment": "Center",
                                                "weight": "Bolder",
                                                "spacing": "Small"
                                            }
                                        ]
                                    }
                                ]
                            },
                            {
                                "type": "Column",
                                "width": "stretch",
                                "spacing": "Small",
                                "items": [
                                    {
                                        "type": "Container",
                                        "style": "default",
                                        "spacing": "Small",
                                        "selectAction": {
                                            "type": "Action.OpenUrl",
                                            "url": "https://outlook.office365.com/mail/inbox"
                                        },
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "View",
                                                "horizontalAlignment": "Center",
                                                "weight": "Bolder",
                                                "spacing": "Small"
                                            }
                                        ]
                                    },
                                    {
                                        "type": "Container",
                                        "style": "default",
                                        "spacing": "Small",
                                        "selectAction": {
                                            "type": "Action.Submit",
                                            "data": {
                                                "triageAction": "delete",
                                                "emailId": email_id
                                            }
                                        },
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "Delete",
                                                "horizontalAlignment": "Center",
                                                "weight": "Bolder",
                                                "spacing": "Small"
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }


# ── Teams delivery ─────────────────────────────────────────────────────────────
def _get_delivery_config() -> tuple[str, str, str, str]:
    """
    Retrieve the shared Bot Framework delivery config.

    WHY extracted as a helper:
      Both _send_text_to_teams and _send_card_to_teams need the same four
      values. Extracting them avoids repetition and keeps each send
      function focused on its payload format only.

    Returns: (bot_token, service_url, conversation_id, bot_app_id)
    """
    bot_token    = _get_bot_token()
    service_url  = os.environ["TEAMS_SERVICE_URL"].rstrip("/")
    conversation = os.environ["TEAMS_DAILY_OPERATIONS_ID"]
    bot_app_id   = os.environ["BOT_APP_ID"]
    return bot_token, service_url, conversation, bot_app_id


def _send_text_to_teams(text: str) -> None:
    """
    Post a plain-text message to the Teams Daily Operations channel.

    WHY plain text for the no-email case:
      A single short status message does not need a card. Plain text is
      lighter and renders immediately in all Teams clients.
    """
    bot_token, service_url, conversation, bot_app_id = _get_delivery_config()
    url = f"{service_url}/v3/conversations/{conversation}/activities"

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type":  "application/json",
        },
        json={
            "type": "message",
            "from": {"id": bot_app_id},
            "text": text,
        },
        timeout=15,
    )
    resp.raise_for_status()
    logging.info(f"emailDigest: plain text delivered — status {resp.status_code}")


def _send_card_to_teams(card: dict) -> None:
    """
    Post a single Adaptive Card to the Teams Daily Operations channel
    via the Bot Framework Connector API.

    WHY attachments with contentType adaptive card:
      The Bot Framework Connector expects Adaptive Cards wrapped in an
      attachments array with the contentType set to the adaptive card
      MIME type. Without this wrapper Teams renders the card JSON as
      raw text rather than a rendered card.

    WHY Bot Framework Connector (not Graph API):
      The digest is delivered as a bot message — it appears as Monica
      speaking in the channel. Graph's /chats endpoint works differently
      and requires additional permissions. The Bot Framework Connector
      is the correct path for bot-originated messages.
    """
    bot_token, service_url, conversation, bot_app_id = _get_delivery_config()
    url = f"{service_url}/v3/conversations/{conversation}/activities"

    payload = {
        "type": "message",
        "from": {"id": bot_app_id},
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card,
            }
        ],
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
    logging.info(f"emailDigest: card delivered — status {resp.status_code}")
