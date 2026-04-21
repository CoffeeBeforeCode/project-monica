"""
email_digest_1300.py — Monica Email Digest  |  13:00 slot

Fires daily at 13:00 London local time, seven days a week.

WHY this is a standalone file:
  See email_digest_0500.py. Each slot is an independent timer trigger.
  A failure here cannot affect any other slot.

Slot behaviour:
  - Concertina email triage card with Leo's 13:00 greeting in the header.
  - Clear inbox card if inbox is empty.
"""

import logging
import azure.functions as func
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from digest_shared import (
    LONDON_TZ,
    get_access_token,
    _read_last_run,
    _write_last_run,
    _fetch_emails,
    _build_concertina_card,
    _build_clear_inbox_card,
    _greeting,
    _email_count_line,
    _send_card_to_teams,
)

bp = func.Blueprint()

SLOT_HOUR = 13


@bp.timer_trigger(
    schedule="0 0 13 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def emailDigest1300(timer: func.TimerRequest) -> None:
    now_utc    = datetime.now(timezone.utc)
    now_london = now_utc.astimezone(LONDON_TZ)
    tz_label   = "BST" if now_london.utcoffset() == timedelta(hours=1) else "GMT"

    logging.info(
        f"emailDigest1300: starting at {now_utc.isoformat()} UTC "
        f"({now_london.strftime('%H:%M')} {tz_label})"
    )

    token = get_access_token()
    if not token:
        logging.error("emailDigest1300: no access token — aborting")
        return

    last_run_utc = _read_last_run()
    emails       = _fetch_emails(token, last_run_utc)
    logging.info(f"emailDigest1300: fetched {len(emails)} emails")
    _write_last_run(now_utc)

    emails        = list(reversed(emails))
    slot_greeting = _greeting(SLOT_HOUR)
    count_line    = _email_count_line(len(emails))

    if emails:
        _send_card_to_teams(
            _build_concertina_card(
                emails, tz_label, token, now_london,
                greeting=slot_greeting,
                count_line=count_line,
            )
        )
        logging.info(f"emailDigest1300: concertina card delivered — {len(emails)} email(s)")
    else:
        _send_card_to_teams(
            _build_clear_inbox_card(greeting=slot_greeting, count_line=count_line)
        )
        logging.info("emailDigest1300: clear inbox card delivered")
