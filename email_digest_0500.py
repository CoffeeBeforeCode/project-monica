"""
email_digest_0500.py — Monica Email Digest  |  05:00 slot
 # test edit
Fires daily at 05:00 London local time, seven days a week.

WHY this is a standalone file:
  Previously the 05:00 slot was one branch inside a single email_digest.py
  file covering all eight daily slots. Refactored in Session 33 into eight
  independent files — one per slot — each with its own cron expression and
  timer trigger. A failure here cannot affect any other slot. This file can
  be tested independently via the portal's Run Now button.

WHY Sunday 05:00 is no longer suppressed:
  The original single file suppressed Sunday 05:00 in code, meaning Sunday
  started at 07:00. That suppression required a runtime weekday check inside
  every slot's logic. Session 33 decision: Sunday starts at 05:00 like every
  other day. If Phillip is asleep, the card is there when he wakes up.
  The principal's briefing is ready when the principal is.

Slot behaviour:
  - Unified morning briefing card: greeting + markets + weather + agenda.
  - Followed by concertina email triage card (or clear inbox card).
  - is_monday check used for market voice line time reference ('on Friday'
    vs 'overnight').
"""

import logging
import time
import azure.functions as func
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from digest_shared import (
    LONDON_TZ,
    CARD_SEND_DELAY,
    get_access_token,
    _read_last_run,
    _write_last_run,
    _fetch_emails,
    _fetch_weather,
    _fetch_calendar_events,
    _fetch_market_data,
    _build_morning_briefing_card,
    _build_concertina_card,
    _build_clear_inbox_card,
    _email_count_line,
    _send_card_to_teams,
)

bp = func.Blueprint()


@bp.timer_trigger(
    schedule="0 0 5 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def emailDigest0500(timer: func.TimerRequest) -> None:
    now_utc    = datetime.now(timezone.utc)
    now_london = now_utc.astimezone(LONDON_TZ)
    tz_label   = "BST" if now_london.utcoffset() == timedelta(hours=1) else "GMT"
    is_monday  = (now_london.weekday() == 0)

    logging.info(
        f"emailDigest0500: starting at {now_utc.isoformat()} UTC "
        f"({now_london.strftime('%H:%M')} {tz_label}) — is_monday={is_monday}"
    )

    # ── Fetch Graph token ─────────────────────────────────────────────────────
    token = get_access_token()
    if not token:
        logging.error("emailDigest0500: no access token — aborting")
        return

    # ── Fetch emails ──────────────────────────────────────────────────────────
    # WHY fetch before sending:
    #   The email count appears in the concertina header. Fetching first
    #   means the count is accurate when the card is built. last_run is
    #   written immediately after fetch so a mid-send crash does not cause
    #   duplicates on the next run.
    last_run_utc = _read_last_run()
    logging.info(
        f"emailDigest0500: last run was "
        f"{last_run_utc.isoformat() if last_run_utc else 'never'}"
    )
    emails = _fetch_emails(token, last_run_utc)
    logging.info(f"emailDigest0500: fetched {len(emails)} emails")
    _write_last_run(now_utc)

    # Oldest first — natural chronological triage order
    emails     = list(reversed(emails))
    count_line = _email_count_line(len(emails))

    # ── Fetch supporting data ─────────────────────────────────────────────────
    weather = None
    try:
        weather = _fetch_weather()
    except Exception as e:
        logging.error(f"emailDigest0500: weather fetch failed — {e}")

    events = []
    try:
        events = _fetch_calendar_events(token, now_utc)
    except Exception as e:
        logging.error(f"emailDigest0500: calendar fetch failed — {e}")

    market_data = None
    try:
        market_data = _fetch_market_data(is_evening=False)
    except Exception as e:
        logging.error(f"emailDigest0500: market fetch failed — {e}")

    # ── Morning briefing card ─────────────────────────────────────────────────
    # WHY unified card:
    #   One card — greeting, markets, weather, agenda — delivered as a single
    #   message. Leo briefs the day in one clean delivery. The email count
    #   sits at the top of the concertina card immediately above the emails
    #   it describes, not buried after weather and agenda.
    morning_card = _build_morning_briefing_card(
        weather, events, now_london, tz_label,
        market_data=market_data,
        is_monday=is_monday,
    )
    _send_card_to_teams(morning_card)
    time.sleep(CARD_SEND_DELAY)
    logging.info("emailDigest0500: morning briefing card delivered")

    # ── Concertina or clear inbox card ────────────────────────────────────────
    # WHY greeting=None:
    #   The greeting is already in the morning briefing card. Passing None
    #   suppresses it from the concertina header to avoid repetition.
    if emails:
        concertina_card = _build_concertina_card(
            emails, tz_label, token, now_london,
            greeting=None,
            count_line=count_line,
        )
        _send_card_to_teams(concertina_card)
        logging.info(
            f"emailDigest0500: concertina card delivered — {len(emails)} email(s)"
        )
    else:
        _send_card_to_teams(
            _build_clear_inbox_card(greeting=None, count_line=count_line)
        )
        logging.info("emailDigest0500: clear inbox card delivered")
