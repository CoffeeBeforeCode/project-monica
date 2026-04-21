"""
email_digest_1900.py — Monica Email Digest  |  19:00 slot

Fires daily at 19:00 London local time, seven days a week.

WHY this is a standalone file:
  See email_digest_0500.py. Each slot is an independent timer trigger.
  A failure here cannot affect any other slot.

Slot behaviour:
  - Concertina email triage card with evening greeting + FTSE close line.
  - Clear inbox card if inbox is empty (also carries FTSE close line).
  - Tomorrow's agenda card.
  - Goodnight card. Leo closes the day.

WHY FTSE only at 19:00 (no US markets):
  FTSE closes at 16:30 BST — the final number is available by 19:00.
  US markets close at 21:00 BST — they are still live at 19:00.
  Showing partial US intraday figures as a 'close' would be misleading.
  FTSE only is the accurate choice.

WHY three cards in sequence at 19:00:
  Leo closes the working day in order: inbox (last work action), tomorrow's
  agenda (the transition), goodnight (the close). The sequence matters.
  Saying goodnight before the look-ahead would feel like leaving the room
  before the briefing is finished.
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
    _fetch_calendar_events,
    _fetch_market_data,
    _build_concertina_card,
    _build_clear_inbox_card,
    _build_agenda_card,
    _build_goodnight_card,
    _evening_ftse_line,
    _greeting,
    _email_count_line,
    _send_card_to_teams,
)

bp = func.Blueprint()

SLOT_HOUR = 19


@bp.timer_trigger(
    schedule="0 0 19 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def emailDigest1900(timer: func.TimerRequest) -> None:
    now_utc    = datetime.now(timezone.utc)
    now_london = now_utc.astimezone(LONDON_TZ)
    tz_label   = "BST" if now_london.utcoffset() == timedelta(hours=1) else "GMT"

    logging.info(
        f"emailDigest1900: starting at {now_utc.isoformat()} UTC "
        f"({now_london.strftime('%H:%M')} {tz_label})"
    )

    # ── Fetch Graph token ─────────────────────────────────────────────────────
    token = get_access_token()
    if not token:
        logging.error("emailDigest1900: no access token — aborting")
        return

    # ── Fetch emails ──────────────────────────────────────────────────────────
    last_run_utc = _read_last_run()
    emails       = _fetch_emails(token, last_run_utc)
    logging.info(f"emailDigest1900: fetched {len(emails)} emails")
    _write_last_run(now_utc)

    emails        = list(reversed(emails))
    slot_greeting = _greeting(SLOT_HOUR)
    count_line    = _email_count_line(len(emails))

    # ── FTSE close line ───────────────────────────────────────────────────────
    # WHY fetched before the concertina card is built:
    #   The market line appears in the concertina header immediately below
    #   the greeting. It must be available when the card is constructed.
    market_line = None
    try:
        market_data_eve = _fetch_market_data(is_evening=True)
        if market_data_eve and "ftse" in market_data_eve:
            market_line = _evening_ftse_line(market_data_eve["ftse"])
    except Exception as e:
        logging.error(f"emailDigest1900: evening market fetch failed — {e}")

    # ── Concertina or clear inbox card ────────────────────────────────────────
    if emails:
        _send_card_to_teams(
            _build_concertina_card(
                emails, tz_label, token, now_london,
                greeting=slot_greeting,
                count_line=count_line,
                market_line=market_line,
            )
        )
        logging.info(
            f"emailDigest1900: concertina card delivered — {len(emails)} email(s)"
        )
    else:
        _send_card_to_teams(
            _build_clear_inbox_card(
                greeting=slot_greeting,
                count_line=count_line,
                market_line=market_line,
            )
        )
        logging.info("emailDigest1900: clear inbox card delivered")

    time.sleep(CARD_SEND_DELAY)

    # ── Tomorrow's agenda card ────────────────────────────────────────────────
    # WHY now_london + 1 day passed as the datetime:
    #   _build_agenda_card uses the passed datetime for the date label.
    #   Passing today's datetime would show today's date on a card titled
    #   TOMORROW'S AGENDA.
    try:
        tomorrow_events = _fetch_calendar_events(token, now_utc, day_offset=1)
        tomorrow_london = now_london + timedelta(days=1)
        tomorrow_card   = _build_agenda_card(
            tomorrow_events,
            tomorrow_london,
            tz_label,
            label="TOMORROW'S AGENDA",
            intro="Here's what tomorrow looks like, Phillip.",
        )
        _send_card_to_teams(tomorrow_card)
        time.sleep(CARD_SEND_DELAY)
        logging.info(
            f"emailDigest1900: tomorrow agenda card delivered — "
            f"{len(tomorrow_events)} event(s)"
        )
    except Exception as e:
        logging.error(f"emailDigest1900: tomorrow agenda card failed — {e}")

    # ── Goodnight card ────────────────────────────────────────────────────────
    _send_card_to_teams(_build_goodnight_card())
    logging.info("emailDigest1900: goodnight card delivered")
