# webhook_renewal.py
# Why: Azure Functions Python v2 supports Blueprints — a way to define functions
# in separate files without creating a second FunctionApp instance. This file
# registers its Timer Trigger on a Blueprint object. The Blueprint is then
# registered with the main app in function_app.py. Each cron stays in its own
# file; there is still only one FunctionApp.

import azure.functions as func
import logging
import requests
import os
from datetime import datetime, timezone, timedelta

bp = func.Blueprint()


@bp.timer_trigger(
    schedule="0 0 7 * * *",
    arg_name="timer",
    run_on_startup=False
)
def renewWebhookSubscriptions(timer: func.TimerRequest) -> None:
    """
    Why: Graph webhook subscriptions for To Do resources expire after a maximum of
    4,230 minutes (~2.9 days). Without renewal, Monica stops receiving task completion
    notifications and the task chain silently breaks. This function runs daily at
    07:00 UTC, retrieves all active subscriptions, and renews any that expire within
    48 hours. Running daily with a 48-hour lookahead means every subscription is
    renewed at least once before it lapses — regardless of the 2.9-day cap.
    """
    logging.info("Webhook renewal function started.")

    token = get_access_token()
    if not token:
        logging.error("Failed to obtain access token. Renewal aborted.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # --- Retrieve all active subscriptions ---
    # Why: Fetching dynamically rather than hardcoding the subscription ID means
    # this function covers all subscriptions — including the additional list
    # subscriptions registered later in this session — without any code changes.
    response = requests.get(
        "https://graph.microsoft.com/v1.0/subscriptions",
        headers=headers,
        timeout=30
    )

    if response.status_code != 200:
        logging.error(f"Failed to retrieve subscriptions: {response.status_code} {response.text}")
        return

    subscriptions = response.json().get("value", [])
    logging.info(f"Found {len(subscriptions)} active subscription(s).")

    # --- Renew any subscription expiring within 48 hours ---
    # Why: 4,200 minutes is slightly inside the 4,230-minute cap. The small buffer
    # prevents boundary errors if there is any clock skew between Azure and Graph.
    now = datetime.now(timezone.utc)
    renewal_threshold = now + timedelta(hours=48)
    new_expiry = (now + timedelta(minutes=4200)).strftime("%Y-%m-%dT%H:%M:%SZ")

    for sub in subscriptions:
        sub_id = sub.get("id")
        expiry_str = sub.get("expirationDateTime", "")
        resource = sub.get("resource", "unknown")

        try:
            expiry = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
        except ValueError:
            logging.warning(f"Could not parse expiry for subscription {sub_id}: {expiry_str}")
            continue

        if expiry <= renewal_threshold:
            logging.info(f"Renewing subscription {sub_id} (resource: {resource}, expires: {expiry_str})")

            patch_response = requests.patch(
                f"https://graph.microsoft.com/v1.0/subscriptions/{sub_id}",
                headers=headers,
                json={"expirationDateTime": new_expiry},
                timeout=30
            )

            if patch_response.status_code == 200:
                logging.info(f"Renewed subscription {sub_id} — new expiry: {new_expiry}")
            else:
                logging.error(
                    f"Failed to renew subscription {sub_id}: "
                    f"{patch_response.status_code} {patch_response.text}"
                )
        else:
            logging.info(f"Subscription {sub_id} is current (expires: {expiry_str})")


def get_access_token() -> str | None:
    """
    Why: Duplicated from function_app.py rather than imported because Azure Functions
    does not guarantee module resolution across files in the same deployment package.
    Keeping it self-contained avoids import errors.
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
