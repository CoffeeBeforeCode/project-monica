"""
messages.py — Monica Bot Framework HTTP Trigger

Receives all incoming Teams activity at:
  POST /api/messages

This endpoint is the Bot Framework channel endpoint. Teams sends every
message, reaction, and event addressed to Monica here as a JSON Activity
object.

Current state (Session 20):
  - Validates the incoming request has a JSON body
  - Logs the conversation ID and service URL
  - Routes triage button presses (Action.Submit payloads) to the
    appropriate Graph API action: move, flag, delete, or create task
  - Falls back to plain-text acknowledgement for typed messages

Triage actions (wired to email digest Adaptive Card buttons):
  action    — move to Action folder, flag, create Admin task for tomorrow
  waiting   — move to Waiting For folder, create Waiting For task in 5 days
  delete    — delete the email permanently

WHY this file is self-contained:
  Same Blueprint pattern as all other Monica functions. A crash here
  affects only the bot endpoint — timers and task chains keep running.
"""

import os
import json
import logging
import requests
import azure.functions as func

from datetime import datetime, timezone, timedelta

# ── Blueprint registration ────────────────────────────────────────────────────
bp = func.Blueprint()

# ── Constants ─────────────────────────────────────────────────────────────────
# WHY hardcoded user ID:
#   All other Monica files use this same value. The Managed Identity token
#   grants access to this specific mailbox. It is not a secret — it is the
#   Entra Object ID of Phillip's M365 account and appears in Graph API paths.
USER_ID = "cda66539-6f2a-4a27-a5a3-a493061f8711"
GRAPH_BASE = f"https://graph.microsoft.com/v1.0/users/{USER_ID}"


# ── HTTP Trigger ──────────────────────────────────────────────────────────────
@bp.route(route="messages", methods=["POST"])
def messages(req: func.HttpRequest) -> func.HttpResponse:
    """
    Receive an incoming Teams bot activity.

    WHY POST only:
      Bot Framework always delivers activities via POST. GET requests to
      this endpoint are not part of the protocol and are rejected cleanly.

    WHY we log CONVERSATION_ID_CAPTURE and SERVICE_URL_CAPTURE:
      These values are needed to deliver proactive messages back to Teams.
      They were captured in Session 12 and stored in Key Vault, but we
      keep logging them so they can be re-captured if the secrets ever
      need refreshing.
    """
    logging.info("messages: incoming request received")

    # ── Parse the Activity body ───────────────────────────────────────────────
    try:
        body = req.get_json()
    except ValueError:
        logging.warning("messages: request body is not valid JSON")
        return func.HttpResponse("Bad Request", status_code=400)

    # ── Log capture lines ─────────────────────────────────────────────────────
    conversation    = body.get("conversation", {})
    conversation_id = conversation.get("id", "")
    service_url     = body.get("serviceUrl", "")

    logging.info(f"CONVERSATION_ID_CAPTURE: {conversation_id}")
    logging.info(f"SERVICE_URL_CAPTURE: {service_url}")

    # ── Activity type routing ─────────────────────────────────────────────────
    activity_type = body.get("type", "")
    logging.info(f"messages: activity type = {activity_type}")

    if activity_type == "message":
        _handle_message(body, service_url, conversation_id)
    elif activity_type == "conversationUpdate":
        # WHY no reply: replying to conversationUpdate can cause unwanted
        # messages when Monica is first installed in a conversation.
        logging.info("messages: conversationUpdate received — no reply sent")
    else:
        logging.info(f"messages: unhandled activity type '{activity_type}'")

    # WHY 200 with empty body:
    #   Bot Framework expects a 200 response within 15 seconds. An empty
    #   200 is the correct acknowledgement — it prevents Teams from retrying.
    return func.HttpResponse(status_code=200)


# ── Message handler ────────────────────────────────────────────────────────────
def _handle_message(body: dict, service_url: str, conversation_id: str) -> None:
    """
    Handle an inbound Teams message activity.

    WHY we check `value` before `text`:
      When a user presses an Adaptive Card Action.Submit button, Teams
      delivers a message activity where the `value` field contains the
      button's data payload and `text` is empty or absent. If we only
      check `text` we will silently ignore every button press. Checking
      `value` first means triage actions are caught and routed correctly
      before we fall through to the text handler.
    """
    # ── Route triage button presses ───────────────────────────────────────────
    value = body.get("value")
    if value and "triageAction" in value:
        logging.info(f"messages: triage action received — {value.get('triageAction')}")
        _handle_triage(value)
        return

    # ── Fall through to plain-text handler ────────────────────────────────────
    text_in = body.get("text", "").strip()
    logging.info(f"messages: received text: {text_in!r}")

    reply_text = (
        "👋 Monica here. Command handling is coming in a future session. "
        "I received your message."
    )

    try:
        _send_reply(service_url, conversation_id, body, reply_text)
    except Exception as e:
        logging.error(f"messages: failed to send reply — {e}")


# ── Triage dispatcher ──────────────────────────────────────────────────────────
def _handle_triage(value: dict) -> None:
    """
    Dispatch a triage button press to the correct Graph API action.

    WHY we get a fresh Graph token here:
      Triage actions require Microsoft Graph permissions (mail read/write,
      tasks write). The Managed Identity token is obtained fresh on each
      invocation — tokens have a 1-hour lifetime and caching across
      invocations is not safe in a stateless Function.

    WHY we log success/failure but do not surface errors to the user:
      The card has already been delivered. There is no clean way to update
      a delivered Adaptive Card to show an error state in the current
      architecture. Logging is the right failure mode for now — a future
      session can add acknowledgement replies.
    """
    action   = value.get("triageAction", "").lower()
    email_id = value.get("emailId", "")

    if not email_id:
        logging.error("messages: triage payload missing emailId — cannot act")
        return

    token = get_access_token()
    if not token:
        logging.error("messages: could not obtain Graph token for triage action")
        return

    try:
        if action == "action":
            _action_email(token, email_id)
        elif action == "waiting":
            _waiting_email(token, email_id)
        elif action == "delete":
            _delete_email(token, email_id)
        else:
            logging.warning(f"messages: unknown triageAction '{action}' — ignored")
    except Exception as e:
        logging.error(f"messages: triage action '{action}' failed — {e}")


# ── Triage action: Action ──────────────────────────────────────────────────────
def _action_email(token: str, email_id: str) -> None:
    """
    Handle the Action triage button.

    Steps:
      1. Fetch the email subject and sender (needed for the task title).
      2. Move the email to the Action mail folder.
      3. Flag the email.
      4. Create a Microsoft To Do task in the Admin list, due tomorrow.

    WHY fetch the email first:
      The card button payload only contains the Graph message ID. We need
      the subject and sender name to create a meaningful task title
      (e.g. "Email: Project Update from ACME Ltd") rather than an opaque
      reference the user cannot scan at a glance.

    WHY due tomorrow (not today):
      Action items need processing time. Setting due tomorrow gives a
      natural buffer — if it truly needs doing today, Phillip will see
      it immediately and act; if not, it sits correctly in tomorrow's list.
    """
    logging.info(f"messages: _action_email — {email_id[:20]}…")

    # Step 1: fetch email details for task title
    email       = _fetch_email(token, email_id)
    subject     = (email.get("subject") or "(no subject)").strip()
    sender_name = email.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
    task_title  = f"Email: {subject} — {sender_name}"

    # Step 2: move to Action folder
    folder_id = _get_folder_id(token, "Action")
    _move_email(token, email_id, folder_id)
    logging.info("messages: email moved to Action folder")

    # Step 3: flag the email
    _flag_email(token, email_id)
    logging.info("messages: email flagged")

    # Step 4: create To Do task in Admin list, due tomorrow
    tomorrow   = datetime.now(timezone.utc) + timedelta(days=1)
    list_id    = _get_todo_list_id(token, "Admin")
    _create_task(token, list_id, task_title, tomorrow)
    logging.info(f"messages: task created in Admin — '{task_title}'")


# ── Triage action: Waiting For ─────────────────────────────────────────────────
def _waiting_email(token: str, email_id: str) -> None:
    """
    Handle the Waiting For triage button.

    Steps:
      1. Fetch the email subject and sender.
      2. Move the email to the Waiting For mail folder.
      3. Create a Microsoft To Do task in the Waiting For list, due in 5 days.

    WHY 5 days:
      Waiting For items are things where action depends on someone else
      responding. Five days is a reasonable chase interval — long enough
      not to be pestering, short enough to catch stalls before they become
      problems.

    WHY no flag:
      Flagging is reserved for Action items — things that require Phillip
      to act. Waiting For items are pending someone else, so flagging would
      create a false signal in the flagged view.
    """
    logging.info(f"messages: _waiting_email — {email_id[:20]}…")

    # Step 1: fetch email details for task title
    email       = _fetch_email(token, email_id)
    subject     = (email.get("subject") or "(no subject)").strip()
    sender_name = email.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
    task_title  = f"Chase: {subject} — {sender_name}"

    # Step 2: move to Waiting For folder
    folder_id = _get_folder_id(token, "Waiting For")
    _move_email(token, email_id, folder_id)
    logging.info("messages: email moved to Waiting For folder")

    # Step 3: create To Do task in Waiting For list, due in 5 days
    due_date = datetime.now(timezone.utc) + timedelta(days=5)
    list_id  = _get_todo_list_id(token, "Waiting For")
    _create_task(token, list_id, task_title, due_date)
    logging.info(f"messages: task created in Waiting For — '{task_title}'")


# ── Triage action: Delete ──────────────────────────────────────────────────────
def _delete_email(token: str, email_id: str) -> None:
    """
    Handle the Delete triage button.

    WHY permanent delete (not move to Deleted Items):
      The Delete button is for definite junk — emails the user is certain
      do not need archiving. Moving to Deleted Items would leave them in
      the 2-year auto-archive window unnecessarily. A hard delete is the
      correct action when the decision is certain.

    WHY no task created:
      Delete means done. No follow-up is needed.
    """
    logging.info(f"messages: _delete_email — {email_id[:20]}…")

    url  = f"{GRAPH_BASE}/messages/{email_id}"
    resp = requests.delete(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    logging.info("messages: email permanently deleted")


# ── Graph helpers ──────────────────────────────────────────────────────────────
def _fetch_email(token: str, email_id: str) -> dict:
    """
    Fetch a single email from Graph to retrieve its subject and sender.

    WHY $select:
      We only need subject and from — requesting the full message body
      would return a much larger payload for no benefit here.
    """
    url  = (
        f"{GRAPH_BASE}/messages/{email_id}"
        "?$select=subject,from"
    )
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _get_folder_id(token: str, folder_name: str) -> str:
    """
    Look up a mail folder's Graph ID by its display name.

    WHY look up rather than hardcode:
      Folder IDs are opaque strings that can change if a folder is deleted
      and recreated. Looking them up by display name is resilient to that.
      The cost is one extra API call per triage action — acceptable given
      the low frequency of button presses.

    WHY mailFolders/search rather than childFolders:
      The Action and Waiting For folders sit at the top level of the
      mailbox (not nested inside Inbox). mailFolders returns all
      top-level folders; Inbox/childFolders would miss them.
    """
    url  = f"{GRAPH_BASE}/mailFolders?$filter=displayName eq '{folder_name}'"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    folders = resp.json().get("value", [])

    if not folders:
        raise ValueError(f"Mail folder '{folder_name}' not found")

    return folders[0]["id"]


def _move_email(token: str, email_id: str, destination_folder_id: str) -> None:
    """
    Move an email to a destination folder using the Graph move action.

    WHY /move rather than PATCH parentFolderId:
      The Graph API provides a dedicated /move action for this purpose.
      It is atomic — the message either moves or the call fails cleanly.
      PATCHing parentFolderId achieves the same result but /move is the
      documented idiomatic approach.
    """
    url  = f"{GRAPH_BASE}/messages/{email_id}/move"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
        json={"destinationId": destination_folder_id},
        timeout=15,
    )
    resp.raise_for_status()


def _flag_email(token: str, email_id: str) -> None:
    """
    Set the follow-up flag on an email to 'flagged'.

    WHY flagStatus flagged (not complete):
      'flagged' marks the email as requiring follow-up — the correct
      signal for an Action item. 'complete' would mark it as already
      done, which is the wrong state at triage time.
    """
    url  = f"{GRAPH_BASE}/messages/{email_id}"
    resp = requests.patch(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
        json={"flag": {"flagStatus": "flagged"}},
        timeout=15,
    )
    resp.raise_for_status()


def _get_todo_list_id(token: str, list_name: str) -> str:
    """
    Look up a Microsoft To Do list's ID by its display name.

    WHY fetch all lists and filter in Python:
      The To Do Graph endpoint does not support $filter on displayName
      directly — it returns a 400 if you try. We fetch all lists
      (a small payload — most users have fewer than 20) and match in
      Python instead.
    """
    url  = f"{GRAPH_BASE}/todo/lists"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    lists = resp.json().get("value", [])

    for todo_list in lists:
        if todo_list.get("displayName", "").lower() == list_name.lower():
            return todo_list["id"]

    raise ValueError(f"To Do list '{list_name}' not found")


def _create_task(
    token:    str,
    list_id:  str,
    title:    str,
    due_date: datetime,
) -> None:
    """
    Create a task in a Microsoft To Do list with a due date.

    WHY dueDateTime uses UTC with timeZone 'UTC':
      To Do stores all due dates as UTC midnight of the chosen day.
      Passing the date in UTC with the timeZone field set to 'UTC' is
      the cleanest approach — no timezone conversion ambiguity.

    WHY importance normal (default):
      Importance is not set explicitly here. The Action button creates
      a flagged email which signals urgency in Outlook; the To Do task
      captures the item for planning. Importance can be elevated manually
      if needed — we do not want Monica to override Phillip's priority
      judgements automatically.
    """
    # Format as ISO date string at midnight UTC
    due_str = due_date.strftime("%Y-%m-%dT00:00:00.0000000")

    url  = f"{GRAPH_BASE}/todo/lists/{list_id}/tasks"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
        json={
            "title": title,
            "dueDateTime": {
                "dateTime": due_str,
                "timeZone": "UTC",
            },
        },
        timeout=15,
    )
    resp.raise_for_status()


# ── Reply sender ───────────────────────────────────────────────────────────────
def _send_reply(
    service_url:     str,
    conversation_id: str,
    incoming_body:   dict,
    text:            str,
) -> None:
    """
    Post a reply to the originating Teams conversation.

    WHY we include replyToId:
      Setting replyToId threads the reply under the original message in
      Teams, keeping the conversation tidy rather than posting a new
      top-level message.
    """
    bot_token  = _get_bot_token()
    bot_app_id = os.environ["BOT_APP_ID"]

    url = (
        f"{service_url.rstrip('/')}/v3/conversations/"
        f"{conversation_id}/activities/{incoming_body.get('id', '')}"
    )

    payload = {
        "type":      "message",
        "from":      {"id": bot_app_id},
        "recipient": incoming_body.get("from", {}),
        "replyToId": incoming_body.get("id", ""),
        "text":      text,
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
    logging.info(f"messages: reply delivered, status {resp.status_code}")


# ── Authentication ─────────────────────────────────────────────────────────────
def get_access_token() -> str | None:
    """
    Obtain a Microsoft Graph access token via the Function App's
    system-assigned Managed Identity.

    WHY IDENTITY_ENDPOINT and IDENTITY_HEADER:
      Azure Functions provides these two environment variables automatically
      at runtime. They point to a local token broker that the Functions host
      manages. This is the correct pattern for Azure Functions — the
      169.254.169.254 metadata address used by VMs does not work here.
    """
    identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
    identity_header   = os.environ.get("IDENTITY_HEADER")

    if not identity_endpoint or not identity_header:
        logging.error("messages: Managed Identity environment variables not set")
        return None

    try:
        resp = requests.get(
            f"{identity_endpoint}?api-version=2019-08-01&resource=https://graph.microsoft.com",
            headers={"X-IDENTITY-HEADER": identity_header},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("access_token")
    except Exception as e:
        logging.error(f"messages: token acquisition failed — {e}")
        return None


def _get_bot_token() -> str:
    """
    Obtain a Bot Framework access token using the Bot App ID and client
    secret stored in Key Vault (surfaced as app settings).

    WHY client credentials flow (not Managed Identity):
      The Bot Framework token endpoint accepts only the Bot App ID and
      client secret registered in Azure Bot Service. Managed Identity
      is not an option here — Bot Service does not support it for this
      flow.
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
