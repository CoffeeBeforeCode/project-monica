"""
messages.py — Monica Bot Framework HTTP Trigger
Receives all incoming Teams activity at:
  POST /api/messages
This endpoint is the Bot Framework channel endpoint. Teams sends every
message, reaction, and event addressed to Monica here as a JSON Activity
object.
Current state (Session 42):
  - Validates the incoming request has a JSON body
  - Returns 200 immediately — before any Graph API work begins
  - Dispatches message handling to a background thread so the Bot
    Framework acknowledgement is never delayed by Graph API calls
  - Routes triage button presses (Action.Submit payloads) to the
    appropriate Graph API action: move, flag, delete, or create task
  - Routes messages from the Conversation channel to Claude via the
    Anthropic API, using Leo's voice and Phillip's context as the
    system prompt, with a 7-day rolling conversation history
  - Falls back to plain-text acknowledgement for all other typed messages
Triage actions (wired to email digest Adaptive Card buttons):
  action    — move to Action folder, flag, create Admin task for tomorrow
  waiting   — move to Waiting For folder, create Waiting For task in 5 days
  delete    — delete the email permanently
Session 25 fix:
  - reply_text updated from "Monica here" to Leo's voice.
  - from.name set to "Leo" in _send_reply. Previously absent, causing
    Teams to fall back to the Azure Bot registration name "monica-bot".
Session 33 fix:
  - Bot Framework requires a 200 response within ~5 seconds.
    Action and Waiting For involve 6-7 sequential Graph API calls —
    more than enough to exceed that window and trigger "Unable to reach
    app" in Teams. Delete succeeded because it makes only one call.
  - Fix: return 200 immediately, then process the activity in a
    background thread via threading.Thread. The Bot Framework gets its
    acknowledgement within milliseconds. Graph work continues
    uninterrupted in the background.
  - No change to any triage logic, Graph helpers, or auth functions.
Session 42 addition:
  - Conversation channel routing. Messages from TEAMS_CONVERSATION_ID
    are passed to Claude (Anthropic API) with Leo's voice and Phillip's
    context loaded fresh from OneDrive on every call. A 7-day rolling
    conversation log is loaded as history and appended after each reply.
WHY this file is self-contained:
  Same Blueprint pattern as all other Monica functions. A crash here
  affects only the bot endpoint — timers and task chains keep running.
"""
import os
import json
import logging
import requests
import threading
import anthropic
import azure.functions as func
from datetime import datetime, timezone, timedelta

# ── Blueprint registration ────────────────────────────────────────────────────
bp = func.Blueprint()

# ── Constants ─────────────────────────────────────────────────────────────────
# WHY hardcoded user ID:
#   All other Monica files use this same value. The Managed Identity token
#   grants access to this specific mailbox. It is not a secret — it is the
#   Entra Object ID of Phillip's M365 account and appears in Graph API paths.
USER_ID    = "cda66539-6f2a-4a27-a5a3-a493061f8711"
GRAPH_BASE = f"https://graph.microsoft.com/v1.0/users/{USER_ID}"

# WHY hardcoded Drive ID:
#   The Drive ID identifies Phillip's OneDrive. It is stable — it does not
#   change when files are moved or renamed. Using it directly avoids an
#   extra API call to resolve /me/drive on every invocation. It is not a
#   secret — it appears in every Graph API path that addresses this drive.
DRIVE_ID = "b!P6rMZy1cnUiuZLBDURE_GkKIGD_9euVDsIfqU_9bzzdFt7Iel1D4SY7FwvJum6B5"

# ── HTTP Trigger ──────────────────────────────────────────────────────────────
@bp.route(route="messages", methods=["POST"])
def messages(req: func.HttpRequest) -> func.HttpResponse:
    """
    Receive an incoming Teams bot activity.
    WHY POST only:
      Bot Framework always delivers activities via POST. GET requests to
      this endpoint are not part of the protocol and are rejected cleanly.
    WHY we return 200 before processing:
      The Bot Framework requires a 200 acknowledgement within ~5 seconds.
      Action and Waiting For triage actions involve 6–7 sequential Graph
      API calls. Running those calls synchronously on the request thread
      exceeds the timeout and Teams shows "Unable to reach app".
      Returning 200 immediately and dispatching to a background thread
      means Teams gets its acknowledgement within milliseconds. The Graph
      work continues in the background regardless.
    WHY we still parse the body before spawning the thread:
      If the body is not valid JSON, we return 400 before spawning
      anything — no point starting a thread for a malformed request.
      The 400 path is fast and does not risk the timeout.
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

    # ── Acknowledge immediately ───────────────────────────────────────────────
    # WHY daemon=True:
    #   A daemon thread is automatically killed when the main process
    #   exits. This is the correct choice for a fire-and-forget background
    #   task — we do not want the Azure Functions host to stay alive
    #   waiting for the thread to finish before it can recycle.
    thread = threading.Thread(
        target=_process_activity,
        args=(body, service_url, conversation_id),
        daemon=True,
    )
    thread.start()
    return func.HttpResponse(status_code=200)


# ── Activity processor (runs in background thread) ────────────────────────────
def _process_activity(body: dict, service_url: str, conversation_id: str) -> None:
    """
    Process the Teams activity in a background thread.
    WHY separated from the HTTP trigger:
      The HTTP trigger must return 200 immediately. This function contains
      all the work that would previously have blocked that return. Separating
      them makes the timing responsibility explicit — the trigger owns the
      acknowledgement; this function owns the processing.
    WHY we catch all exceptions at the top level:
      An unhandled exception in a background thread is silently swallowed
      by Python. Catching at the top level ensures any failure is logged
      and visible in Application Insights rather than disappearing.
    """
    try:
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
    except Exception as e:
        logging.error(f"messages: unhandled exception in background thread — {e}")


# ── Message handler ────────────────────────────────────────────────────────────
def _handle_message(body: dict, service_url: str, conversation_id: str) -> None:
    """
    Handle an inbound Teams message activity.
    WHY we check `value` before anything else:
      When a user presses an Adaptive Card Action.Submit button, Teams
      delivers a message activity where the `value` field contains the
      button's data payload and `text` is empty or absent. If we only
      check `text` we will silently ignore every button press. Checking
      `value` first means triage actions are caught and routed correctly
      before we evaluate the conversation ID or fall through to Claude.
    WHY we compare conversation_id against TEAMS_CONVERSATION_ID:
      TEAMS_CONVERSATION_ID identifies the Conversation channel — the
      dedicated 1:1 channel between Phillip and Leo. Messages from that
      channel are routed to Claude. All other channels (Daily Operations,
      triage card interactions, any future channels) keep the existing
      placeholder response. The comparison is exact string equality —
      the Bot Framework conversation ID is stable for a given channel.
    WHY we load OneDrive files on every call:
      monica-voice.md and phillip-context.md are living documents.
      Loading them fresh on every message means Leo always reflects the
      current voice spec and Phillip's current situation without requiring
      a redeployment. The latency cost (two extra Graph calls) is
      acceptable in a conversational context.
    """
    # ── Route triage button presses ───────────────────────────────────────────
    value = body.get("value")
    if value and "triageAction" in value:
        logging.info(f"messages: triage action received — {value.get('triageAction')}")
        _handle_triage(value)
        return

    text_in = body.get("text", "").strip()
    logging.info(f"messages: received text: {text_in!r}")

    # ── Route Conversation channel messages to Claude ─────────────────────────
    teams_conversation_id = os.environ.get("TEAMS_CONVERSATION_ID", "")

    if conversation_id == teams_conversation_id:
        logging.info("messages: Conversation channel — routing to Claude")
        token = get_access_token()
        if not token:
            logging.error("messages: could not obtain Graph token for Conversation channel")
            return
        try:
            voice_md   = _load_onedrive_file(token, "monica/voice/monica-voice.md")
            context_md = _load_onedrive_file(token, "monica/memory/phillip-context.md")
            history    = _load_conversation_history(token)
            leo_reply  = _call_claude(text_in, voice_md, context_md, history)
            _send_reply(service_url, conversation_id, body, leo_reply)
            _append_conversation_log(token, text_in, leo_reply)
        except Exception as e:
            logging.error(f"messages: Conversation channel handling failed — {e}")
        return

    # ── Fall through: all other channels use placeholder ──────────────────────
    # WHY Leo's voice here:
    #   The bot character is Leo McGarry. All user-facing text should
    #   reflect that — "Monica here" was the previous placeholder from
    #   before the character rename in Session 23.
    reply_text = (
        "👋 Leo here. Command handling is coming in a future session. "
        "I received your message."
    )
    try:
        _send_reply(service_url, conversation_id, body, reply_text)
    except Exception as e:
        logging.error(f"messages: failed to send reply — {e}")


# ── OneDrive file loader ───────────────────────────────────────────────────────
def _load_onedrive_file(token: str, path: str) -> str:
    """
    Read a file from OneDrive by its path and return its text content.
    WHY /drives/{DRIVE_ID}/root:/{path}:/content:
      This is the Graph API pattern for addressing a OneDrive item by
      its path relative to the drive root and retrieving its raw content
      directly. It avoids a two-step lookup (resolve item ID, then fetch
      content) by combining both into a single request.
    WHY we use the module-level DRIVE_ID constant:
      The Drive ID is stable and shared across all OneDrive operations in
      this file. Defining it once at module level avoids repetition and
      makes it easy to update if the drive ever changes.
    """
    url  = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{path}:/content"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.text


# ── Conversation history loader ────────────────────────────────────────────────
def _load_conversation_history(token: str) -> str:
    """
    Load the last 7 days of conversation log files from OneDrive and
    return their combined text, oldest day first.
    WHY 7 days:
      Seven days gives Claude enough recent context to maintain continuity
      across conversations without overloading the context window with
      stale history. The window is a rolling 7 days — it shifts forward
      each day automatically.
    WHY we iterate by date rather than listing the folder:
      Iterating by date means we always know exactly which files we are
      requesting and in what order. Listing the folder and sorting would
      work but introduces an extra API call and more parsing logic.
      Missing files (days with no conversation) are silently skipped —
      a 404 from OneDrive simply means no log existed for that date.
    WHY oldest first:
      Presenting history in chronological order (oldest first) matches
      natural reading order and helps Claude understand how the
      conversation has evolved over time.
    """
    today = datetime.now(timezone.utc).date()
    parts = []

    for days_ago in range(6, -1, -1):
        # WHY range(6, -1, -1): iterates 6, 5, 4, 3, 2, 1, 0 — giving us
        # oldest day first (6 days ago) through to today (0 days ago).
        date = today - timedelta(days=days_ago)
        path = f"monica/logs/conversations/conversation-{date.isoformat()}.md"
        try:
            content = _load_onedrive_file(token, path)
            if content.strip():
                parts.append(content.strip())
        except Exception:
            # WHY silently skip: a missing file means no conversation
            # happened that day. This is expected and not an error.
            pass

    return "\n\n".join(parts)


# ── Claude API caller ──────────────────────────────────────────────────────────
def _call_claude(
    user_text:  str,
    voice_md:   str,
    context_md: str,
    history:    str,
) -> str:
    """
    Call the Anthropic API and return Leo's reply as a string.
    WHY voice_md + newline + context_md as the system prompt:
      monica-voice.md defines Leo's character, tone, and behaviour rules.
      phillip-context.md defines who Phillip is, his current projects,
      clients, and priorities. Together they give Claude everything it
      needs to respond as Leo with full awareness of Phillip's situation.
      Separating them with a newline keeps each document's structure
      intact — no mixing of headings or content.
    WHY history prepended to user_text in a single user message:
      The Anthropic messages API expects a strict alternating user/assistant
      turn structure. Rebuilding the full multi-turn history as separate
      messages would require parsing the log files back into attributed
      turns — fragile and over-engineered for this use case. Prepending
      the log as plain text in the user message gives Claude the context
      it needs with a simple, robust implementation.
    WHY model claude-sonnet-4-20250514:
      This is the model specified in the Conversation channel design
      specification. It is the same model that powers Phillip's direct
      Claude Pro work — not a reduced or proxied version.
    WHY max_tokens 1000:
      Sufficient for conversational replies. Leo's voice is direct and
      efficient — verbose responses are not characteristic of the
      character. 1000 tokens is approximately 750 words, which is more
      than enough for any exchange in this channel.
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    system_prompt = voice_md + "\n" + context_md

    # WHY we only prepend history if it exists:
    #   On the first ever message there will be no log files — history
    #   will be an empty string. Prepending an empty string would add a
    #   leading newline to the user message, which is unnecessary noise.
    if history:
        user_content = f"Conversation history (last 7 days):\n\n{history}\n\n---\n\n{user_text}"
    else:
        user_content = user_text

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=system_prompt,
        messages=[
            {"role": "user", "content": user_content},
        ],
    )
    return message.content[0].text


# ── Conversation log appender ──────────────────────────────────────────────────
def _append_conversation_log(
    token:     str,
    user_text: str,
    leo_reply: str,
) -> None:
    """
    Append today's exchange to the conversation log in OneDrive.
    Creates the file if it does not exist.
    WHY PUT rather than PATCH:
      The Graph API does not support appending to a file directly. The
      correct approach is to GET the current content, append the new
      entry in memory, and PUT the full updated content back. PUT with
      /content replaces the file entirely — which is exactly what we
      want when we have assembled the new version ourselves.
    WHY we treat a non-200 GET as "file does not exist":
      On the first exchange of any given day the log file will not exist
      yet. Graph returns 404 in that case. Rather than special-casing 404
      versus other errors, we treat any failed GET as "start fresh" and
      create the file with a date header. A genuine Graph error on the
      subsequent PUT will raise_for_status and be caught by the caller.
    WHY the log format uses markdown:
      The files live in OneDrive and are human-readable. Markdown gives
      them structure (date headers, horizontal rules between exchanges)
      without requiring any special tooling to read.
    """
    today     = datetime.now(timezone.utc).date()
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    path      = f"monica/logs/conversations/conversation-{today.isoformat()}.md"
    url       = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{path}:/content"

    new_entry = (
        f"\n\n---\n\n"
        f"**{timestamp}**\n\n"
        f"**Phillip:** {user_text}\n\n"
        f"**Leo:** {leo_reply}\n"
    )

    # Step 1: attempt to fetch today's existing log
    get_resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )

    if get_resp.status_code == 200:
        updated_content = get_resp.text + new_entry
    else:
        # File does not exist yet — create with a date header
        updated_content = f"# Conversation Log — {today.isoformat()}\n" + new_entry

    # Step 2: PUT the full updated content back
    put_resp = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "text/plain; charset=utf-8",
        },
        data=updated_content.encode("utf-8"),
        timeout=15,
    )
    put_resp.raise_for_status()
    logging.info(f"messages: conversation log updated — {path}")


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
    tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
    list_id  = _get_todo_list_id(token, "Admin")
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
    WHY top-level mailFolders:
      Action and Waiting For are top-level folders in the mailbox (same
      level as Inbox). Querying mailFolders returns all top-level folders
      and the $filter narrows it to the one we need.
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
    WHY from.name is "Leo":
      Without an explicit name field, Teams falls back to the Azure Bot
      registration name ("monica-bot"). Setting it here overrides that
      at the activity level so replies appear as sent by Leo.
    """
    bot_token  = _get_bot_token()
    bot_app_id = os.environ["BOT_APP_ID"]

    url = (
        f"{service_url.rstrip('/')}/v3/conversations/"
        f"{conversation_id}/activities/{incoming_body.get('id', '')}"
    )
    payload = {
        "type":      "message",
        "from":      {"id": bot_app_id, "name": "Leo"},
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
