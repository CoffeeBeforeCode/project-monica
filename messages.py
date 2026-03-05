"""
messages.py — Monica Bot Framework HTTP Trigger

Receives all incoming Teams activity at:
  POST /api/messages

This endpoint is the Bot Framework channel endpoint. Teams sends every
message, reaction, and event addressed to Monica here as a JSON Activity
object.

Current state (Session 18):
  - Validates the incoming request has a JSON body
  - Logs the conversation ID and service URL (used in Session 12 to
    populate Key Vault secrets)
  - Responds with a plain-text acknowledgement so Phillip can see Monica
    is alive

Phase 2 (future session):
  - Full natural-language command handling
  - Routing commands to the appropriate Monica function

WHY this file is self-contained:
  Same Blueprint pattern as all other Monica functions. A crash here
  affects only the bot endpoint — timers and task chains keep running.
"""

import os
import json
import logging
import requests
import azure.functions as func

# ── Blueprint registration ────────────────────────────────────────────────────
bp = func.Blueprint()


# ── HTTP Trigger ──────────────────────────────────────────────────────────────
@bp.route(route="messages", methods=["POST"])
def messages(req: func.HttpRequest) -> func.HttpResponse:
    """
    Receive an incoming Teams bot activity.

    WHY POST only:
      Bot Framework always delivers activities via POST. GET requests to
      this endpoint are not part of the protocol and are rejected cleanly.

    WHY we log CONVERSATION_ID_CAPTURE and SERVICE_URL_CAPTURE:
      These values are needed to deliver proactive messages back to Teams
      (e.g. the email digest). They were captured in Session 12 by sending
      Monica a message, then reading these log lines from Application
      Insights. The values are now stored in Key Vault, but we keep logging
      them so they can be re-captured if the secrets ever need refreshing.
    """
    logging.info("messages: incoming request received")

    # ── Parse the Activity body ───────────────────────────────────────────────
    try:
        body = req.get_json()
    except ValueError:
        logging.warning("messages: request body is not valid JSON")
        return func.HttpResponse("Bad Request", status_code=400)

    # ── Log capture lines ─────────────────────────────────────────────────────
    # WHY: These specific prefixes were used in Session 12 to grep
    # Application Insights logs for the values we needed.
    conversation = body.get("conversation", {})
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
        # Teams fires this when the bot is added to a conversation.
        # We log it but do not reply — replying to conversationUpdate
        # can cause unwanted messages when Monica is first installed.
        logging.info("messages: conversationUpdate received — no reply sent")
    else:
        logging.info(f"messages: unhandled activity type '{activity_type}'")

    # WHY 200 with empty body:
    #   Bot Framework expects a 200 response within 15 seconds. If we do
    #   not respond, Teams marks the delivery as failed and retries. An
    #   empty 200 is the correct acknowledgement.
    return func.HttpResponse(status_code=200)


# ── Message handler ────────────────────────────────────────────────────────────
def _handle_message(body: dict, service_url: str, conversation_id: str) -> None:
    """
    Handle an inbound text message from Phillip.

    Current behaviour: reply with a simple acknowledgement.
    Phase 2 will replace this with intent parsing and command dispatch.

    WHY reply via the Connector API rather than returning text in the HTTP response:
      Returning text in the 200 response body does work for simple bots,
      but the Bot Framework Connector API is the correct pattern for
      proactive and structured replies. It also keeps the reply logic
      consistent with the digest delivery path.
    """
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
def get_access_token() -> str:
    """
    Managed Identity token for Microsoft Graph (included for consistency
    with the Blueprint pattern — not currently used in this file but
    will be needed when Phase 2 command handling reads To Do or calendar data).

    WHY present even if unused now:
      Keeping the pattern consistent means Phase 2 has one less thing to
      add, and code review is simpler when every Blueprint file looks the same.
    """
    identity_url = (
        "http://169.254.169.254/metadata/identity/oauth2/token"
        "?api-version=2019-08-01"
        "&resource=https://graph.microsoft.com"
    )
    resp = requests.get(identity_url, headers={"Metadata": "true"}, timeout=10)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _get_bot_token() -> str:
    """
    Obtain a Bot Framework access token.

    WHY client credentials flow (not Managed Identity):
      The Bot Framework token endpoint accepts only the Bot App ID and
      client secret registered in Azure Bot Service. Managed Identity
      is not an option here — Bot Service does not support it for this
      flow. The secret is stored in Key Vault and surfaced via the
      BOT_CLIENT_SECRET app setting.
    """
    bot_app_id  = os.environ["BOT_APP_ID"]
    bot_secret  = os.environ["BOT_CLIENT_SECRET"]
    tenant_id   = os.environ["TENANT_ID"]

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
