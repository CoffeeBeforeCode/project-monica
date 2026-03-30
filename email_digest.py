"""
email_digest.py — Monica Email Digest Timer Trigger
Fires every 2 hours (05:00–19:00 London local time daily).
On Sundays, the 05:00 slot is suppressed in code.
Fetches emails received since the last digest run and delivers them as a
single concertina Adaptive Card to the Teams Daily Operations channel via
the Bot Framework Connector API.

WHY this file is self-contained:
  Each Blueprint file owns its own get_access_token() so that one
  broken file cannot take down the rest of the Function App. If the
  Graph token fails here, only this function errors — everything else
  keeps running.

Session 20 additions:
  - Sender profile photo (internal M365, saved contacts, envelope fallback)
  - Digest header card with time-aware greeting
  - Weather card (Open-Meteo, Basingstoke RG21 5NP, Celsius) on first
    daily slot
  - Agenda card (today's calendar events from Graph) on first and second
    daily slot

Session 21 fix:
  - _create_channel_conversation added to resolve 403 Forbidden error.

Session 22 fix:
  - _create_channel_conversation now logs the full Bot Framework response
    body before raising on error.

Session 24 fix:
  - _create_channel_conversation removed entirely. The 19:...@thread.tacv2
    channel ID is itself a valid Bot Framework conversation ID.

Session 25 fix:
  - from.name set to "Leo" in both _send_text_to_teams and
    _send_card_to_teams.

Session 26 change:
  - Email cards sent as a single Bot Framework message activity.
  - Email order reversed for chronological triage.
  - CARD_SEND_DELAY removed from email send path.

Session 29 changes:
  - Greeting card is now the very first message in every digest run,
    before weather and agenda. It includes the email count so Phillip
    knows the workload before anything else is delivered.
  - Emails fetched before greeting is sent so the count is available.
  - All-day events from the previous day are filtered out.
  - Concertina email card replaces the previous one-card-per-email pattern.
  - _build_header_card, _build_card, and _send_cards_to_teams removed.
  - _build_greeting_card and _build_concertina_card added.

Session 30 changes:
  - Standalone greeting card removed entirely.
  - First slot: single unified morning briefing card (greeting + weather
    + agenda), followed by concertina card with email count in header.
    Weather and agenda are only ever sent at first slot.
  - All other slots (07:00–17:00): concertina card with slot-specific
    Leo greeting and email count integrated into the card header.
  - 19:00: concertina card (greeting + count) followed by tomorrow's
    agenda card ("Here's what tomorrow looks like, Phillip.") and a
    goodnight card ("Good night, Sir."). Leo closes the day.
  - Leo voice greetings — fixed per slot, short declaratives, alternating
    between Phillip and Sir. Defined in _greeting().
  - Email count lines rewritten in Leo's voice — _email_count_line().
  - Triage button containers styled with Adaptive Card container styles:
    Action=accent, Waiting For=warning, View=emphasis, Delete=attention.
  - _build_greeting_card removed. Replaced by _build_morning_briefing_card,
    _build_clear_inbox_card, and _build_goodnight_card.
  - _fetch_calendar_events gains a day_offset parameter (used for tomorrow).
  - _build_agenda_card gains label and intro parameters (used for tomorrow).
  - _build_event_items extracted as a shared helper.

Session 31 changes:
  - slot logic now uses London local time (now_london.hour / now_london.weekday)
    rather than UTC. WEBSITE_TIME_ZONE=Europe/London is set on the Function App,
    so the cron fires at London local time. Internal checks must match.
  - Morning briefing card greeting split into two TextBlocks: "Good morning, Sir."
    at ExtraLarge and "Here's the day." at Large, separated by a line break.

Slot logic:
  First slot  (weather + agenda + email digest):
    Mon–Sat: 05:00 London local time
    Sun:     07:00 London local time  (05:00 is suppressed)
  All other slots (email digest only):
    07:00, 09:00, 11:00, 13:00, 15:00, 17:00, 19:00
  19:00 additionally delivers tomorrow's agenda and goodnight card.
"""
import os
import logging
import random
import time
import base64
import requests
import azure.functions as func
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from azure.storage.blob import BlobServiceClient

# ── Blueprint registration ───────────────────────────────────────────────────
bp = func.Blueprint()

# ── Constants ────────────────────────────────────────────────────────────────
LONDON_TZ       = ZoneInfo("Europe/London")
BLOB_CONTAINER  = "monica-digest"
BLOB_NAME       = "last_run.txt"
CARD_SEND_DELAY = 0.3

# Basingstoke RG21 5NP coordinates
WEATHER_LAT = 51.2654
WEATHER_LON = -1.0872

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
    schedule="0 0 5,7,9,11,13,15,17,19 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def emailDigest(timer: func.TimerRequest) -> None:
    now_utc    = datetime.now(timezone.utc)
    now_london = now_utc.astimezone(LONDON_TZ)
    tz_label   = "BST" if now_london.utcoffset() == timedelta(hours=1) else "GMT"

    # WHY slot logic uses London local time:
    #   WEBSITE_TIME_ZONE=Europe/London is set on the Function App, so the
    #   cron fires at London local time year-round. weekday and hour must
    #   be derived from now_london so that Sunday suppression, first-slot
    #   detection, greeting selection, and the 19:00 close all stay correct
    #   regardless of whether GMT or BST is in effect.
    weekday = now_london.weekday()   # 0=Mon … 6=Sun
    hour    = now_london.hour

    # Suppress Sunday 05:00 London time
    if weekday == 6 and hour == 5:
        logging.info("emailDigest: Sunday 05:00 suppressed.")
        return

    # ── Determine slot type ───────────────────────────────────────────────────
    is_first_slot = (weekday != 6 and hour == 5) or (weekday == 6 and hour == 7)

    logging.info(
        f"emailDigest: starting at {now_utc.isoformat()} UTC "
        f"({now_london.strftime('%H:%M')} {tz_label}) — "
        f"first_slot={is_first_slot}, hour={hour}"
    )

    # ── Fetch Graph token ─────────────────────────────────────────────────────
    token = get_access_token()
    if not token:
        logging.error("emailDigest: no access token — aborting")
        return

    # ── Fetch emails ──────────────────────────────────────────────────────────
    # WHY fetch emails before sending anything:
    #   The email count line appears in the concertina card header at every
    #   slot. Fetching first means the count is always accurate when the
    #   card is built. last_run is written immediately after fetch so a
    #   crash mid-send does not cause duplicates on the next run.
    last_run_utc = _read_last_run()
    logging.info(
        f"emailDigest: last run was "
        f"{last_run_utc.isoformat() if last_run_utc else 'never'}"
    )
    emails = _fetch_emails(token, last_run_utc)
    logging.info(f"emailDigest: fetched {len(emails)} emails")
    _write_last_run(now_utc)

    # Oldest first — natural chronological triage order
    emails = list(reversed(emails))

    greeting   = _greeting(hour, is_first_slot)
    count_line = _email_count_line(len(emails))

    # ── First slot: unified morning briefing card + concertina ────────────────
    if is_first_slot:
        # WHY unified morning briefing card:
        #   Previously three separate cards (greeting, weather, agenda).
        #   A single card is cleaner — one message, one scroll position,
        #   Leo greets and briefs the day in a single delivery.
        # WHY email count in concertina header, not morning card:
        #   The natural flow is: brief the day, then here are the emails.
        #   Stating the email count before weather and agenda would mean
        #   13 lines of weather and agenda between the count and the emails.
        #   Putting the count at the top of the concertina card means it
        #   sits immediately above the emails it describes.
        weather = None
        try:
            weather = _fetch_weather()
        except Exception as e:
            logging.error(f"emailDigest: weather fetch failed — {e}")

        events = []
        try:
            events = _fetch_calendar_events(token, now_utc)
        except Exception as e:
            logging.error(f"emailDigest: calendar fetch failed — {e}")

        morning_card = _build_morning_briefing_card(weather, events, now_london, tz_label)
        _send_card_to_teams(morning_card)
        time.sleep(CARD_SEND_DELAY)
        logging.info("emailDigest: morning briefing card delivered")

        # Concertina — no greeting (already in morning card), count only
        if emails:
            concertina_card = _build_concertina_card(
                emails, tz_label, token, now_london,
                greeting=None, count_line=count_line,
            )
            _send_card_to_teams(concertina_card)
            logging.info(f"emailDigest: concertina card delivered — {len(emails)} email(s)")
        else:
            _send_card_to_teams(_build_clear_inbox_card(greeting=None, count_line=count_line))
            logging.info("emailDigest: inbox clear card delivered")

    # ── 19:00: concertina + tomorrow's agenda + goodnight ─────────────────────
    elif hour == 19:
        # WHY three cards at 19:00:
        #   Leo closes the working day in sequence — here is the inbox,
        #   here is tomorrow, good night. The order matters: inbox is
        #   the last work action; the look-ahead is the transition;
        #   goodnight is the close. Reversing the order would feel wrong.
        if emails:
            concertina_card = _build_concertina_card(
                emails, tz_label, token, now_london,
                greeting=greeting, count_line=count_line,
            )
            _send_card_to_teams(concertina_card)
            logging.info(f"emailDigest: concertina card delivered — {len(emails)} email(s)")
        else:
            _send_card_to_teams(_build_clear_inbox_card(greeting=greeting, count_line=count_line))
            logging.info("emailDigest: inbox clear card delivered")
        time.sleep(CARD_SEND_DELAY)

        try:
            tomorrow_events = _fetch_calendar_events(token, now_utc, day_offset=1)
            # WHY now_london + 1 day:
            #   _build_agenda_card uses the passed datetime for the date
            #   label. Passing today's datetime would show today's date
            #   on tomorrow's agenda card.
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
                f"emailDigest: tomorrow agenda card delivered — "
                f"{len(tomorrow_events)} event(s)"
            )
        except Exception as e:
            logging.error(f"emailDigest: tomorrow agenda card failed — {e}")

        _send_card_to_teams(_build_goodnight_card())
        logging.info("emailDigest: goodnight card delivered")

    # ── All other slots: concertina with greeting in header ───────────────────
    else:
        if emails:
            concertina_card = _build_concertina_card(
                emails, tz_label, token, now_london,
                greeting=greeting, count_line=count_line,
            )
            _send_card_to_teams(concertina_card)
            logging.info(f"emailDigest: concertina card delivered — {len(emails)} email(s)")
        else:
            _send_card_to_teams(_build_clear_inbox_card(greeting=greeting, count_line=count_line))
            logging.info("emailDigest: inbox clear card delivered")


# ── Authentication ─────────────────────────────────────────────────────────────
def get_access_token() -> str | None:
    """
    Obtain a Microsoft Graph access token via Managed Identity.
    WHY IDENTITY_ENDPOINT and IDENTITY_HEADER:
      Azure Functions provides these automatically at runtime. They point
      to a local token broker. The 169.254.169.254 VM metadata address
      does not work in Azure Functions and will time out.
    """
    identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
    identity_header   = os.environ.get("IDENTITY_HEADER")
    if not identity_endpoint or not identity_header:
        logging.error("emailDigest: Managed Identity environment variables not set.")
        return None
    try:
        response = requests.get(
            f"{identity_endpoint}?api-version=2019-08-01"
            f"&resource=https://graph.microsoft.com",
            headers={"X-IDENTITY-HEADER": identity_header},
            timeout=10,
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logging.error(f"emailDigest: token acquisition failed: {e}")
        return None


def _get_bot_token() -> str:
    """
    Bot Framework access token via client credentials.
    WHY separate from the Graph token:
      Graph and Bot Framework use different OAuth audiences and
      credential flows. They cannot share a token.
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


# ── Weather fetching ───────────────────────────────────────────────────────────
def _fetch_weather() -> dict:
    """
    Fetch a 5-day forecast from Open-Meteo for Basingstoke RG21 5NP.
    WHY Open-Meteo:
      Free with no API key required. No Key Vault secret needed, no
      rate limit concerns at Monica's usage volume. Excellent UK coverage.
    WHY daily rather than hourly:
      The morning card gives a day-level overview. Hourly data is more
      detail than is useful at 05:00.
    """
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={WEATHER_LAT}&longitude={WEATHER_LON}"
        "&daily=weathercode,temperature_2m_max,temperature_2m_min,"
        "precipitation_probability_max,windspeed_10m_max,"
        "winddirection_10m_dominant"
        "&timezone=Europe%2FLondon"
        "&forecast_days=5"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()["daily"]

    def parse_day(i: int) -> dict:
        code        = data["weathercode"][i]
        emoji, desc = _wmo_to_label(code)
        return {
            "date":     data["time"][i],
            "emoji":    emoji,
            "desc":     desc,
            "high":     round(data["temperature_2m_max"][i]),
            "low":      round(data["temperature_2m_min"][i]),
            "rain_pct": data["precipitation_probability_max"][i],
            "wind_kmh": round(data["windspeed_10m_max"][i]),
            "wind_dir": _degrees_to_compass(data["winddirection_10m_dominant"][i]),
        }

    return {"today": parse_day(0), "forecast": [parse_day(i) for i in range(1, 5)]}


def _wmo_to_label(code: int) -> tuple[str, str]:
    """
    Map a WMO weather interpretation code to an emoji and short description.
    WHY emoji rather than weather icon images:
      External image URLs have no guaranteed uptime. Emoji render natively
      in Teams TextBlocks with zero external dependency.
    """
    mapping = {
        0:  ("☀️",  "Clear sky"),
        1:  ("🌤️", "Mainly clear"),
        2:  ("⛅",  "Partly cloudy"),
        3:  ("☁️",  "Overcast"),
        45: ("🌫️", "Fog"),
        48: ("🌫️", "Icy fog"),
        51: ("🌦️", "Light drizzle"),
        53: ("🌦️", "Drizzle"),
        55: ("🌧️", "Heavy drizzle"),
        61: ("🌧️", "Light rain"),
        63: ("🌧️", "Rain"),
        65: ("🌧️", "Heavy rain"),
        71: ("🌨️", "Light snow"),
        73: ("🌨️", "Snow"),
        75: ("❄️",  "Heavy snow"),
        77: ("🌨️", "Snow grains"),
        80: ("🌦️", "Light showers"),
        81: ("🌧️", "Showers"),
        82: ("🌧️", "Heavy showers"),
        85: ("🌨️", "Snow showers"),
        86: ("❄️",  "Heavy snow showers"),
        95: ("⛈️",  "Thunderstorm"),
        96: ("⛈️",  "Thunderstorm + hail"),
        99: ("⛈️",  "Heavy thunderstorm"),
    }
    return mapping.get(code, ("🌡️", "Unknown"))


def _degrees_to_compass(degrees: float) -> str:
    """
    Convert a wind direction in degrees to a compass point.
    WHY compass rather than degrees:
      "Wind from the SW" is immediately meaningful. "225°" requires
      mental conversion.
    """
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    return directions[round(degrees / 45) % 8]


# ── Calendar fetching ──────────────────────────────────────────────────────────
def _fetch_calendar_events(
    token: str, now_utc: datetime, day_offset: int = 0
) -> list[dict]:
    """
    Fetch calendar events for a given day from Microsoft Graph.
    WHY day_offset:
      day_offset=0 (default) returns today's events.
      day_offset=1 returns tomorrow's — used by the 19:00 closing card.
      A single function handles both cases to avoid duplication.
    WHY calendarView rather than /events with a filter:
      calendarView automatically expands recurring events into individual
      instances. A filter on /events would only return the series master.
    WHY filter yesterday's all-day events after fetching:
      Graph's calendarView startDateTime is inclusive. An all-day event
      from the previous day ends exactly at the query start — so Graph
      returns it. We filter in Python: if the event is all-day and its
      end date matches our day_start date, it belongs to the day before.
      Multi-day events that span into the target day have an end date
      after day_start and are correctly retained.
    """
    london_now = now_utc.astimezone(LONDON_TZ)
    day_start  = (
        london_now.replace(hour=0, minute=0, second=0, microsecond=0)
        + timedelta(days=day_offset)
    )
    day_end    = day_start + timedelta(days=1)
    start_str  = day_start.strftime("%Y-%m-%dT%H:%M:%S")
    end_str    = day_end.strftime("%Y-%m-%dT%H:%M:%S")

    url = (
        f"https://graph.microsoft.com/v1.0/users/"
        f"cda66539-6f2a-4a27-a5a3-a493061f8711"
        f"/calendarView"
        f"?startDateTime={start_str}&endDateTime={end_str}"
        "&$select=subject,start,end,location,isAllDay,organizer"
        "&$orderby=start/dateTime"
        "&$top=20"
    )
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    data   = resp.json()
    events = data.get("value", [])

    if data.get("@odata.nextLink"):
        logging.warning("emailDigest: more than 20 events — some omitted")

    today_date_str = day_start.strftime("%Y-%m-%d")
    filtered = []
    for event in events:
        if event.get("isAllDay"):
            end_dt_str = event.get("end", {}).get("dateTime", "")
            if end_dt_str.startswith(today_date_str):
                logging.info(
                    f"emailDigest: skipping previous day's all-day event "
                    f"'{event.get('subject', '')}'"
                )
                continue
        filtered.append(event)

    return filtered


# ── Event items helper ─────────────────────────────────────────────────────────
def _build_event_items(events: list[dict]) -> list[dict]:
    """
    Build the Adaptive Card body items for a list of calendar events.
    WHY extracted as a helper:
      Both _build_morning_briefing_card and _build_agenda_card need to
      render calendar events. Extracting the logic here means one source
      of truth — a change to event formatting applies everywhere.
    """
    if not events:
        return [
            {
                "type": "TextBlock",
                "text": "No meetings today — enjoy the space.",
                "isSubtle": True,
                "wrap": True,
                "spacing": "Small",
            }
        ]

    items = []
    for i, event in enumerate(events):
        if event.get("isAllDay"):
            time_str = "All day"
        else:
            try:
                start_dt = datetime.fromisoformat(
                    event["start"]["dateTime"]
                ).replace(tzinfo=timezone.utc).astimezone(LONDON_TZ)
                end_dt = datetime.fromisoformat(
                    event["end"]["dateTime"]
                ).replace(tzinfo=timezone.utc).astimezone(LONDON_TZ)
                time_str = f"{start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}"
            except Exception:
                time_str = ""

        subject  = (event.get("subject") or "No title").strip()
        location = (event.get("location", {}).get("displayName", "") or "").strip()

        organiser_email = (
            event.get("organizer", {})
                 .get("emailAddress", {})
                 .get("address", "")
        ).lower()
        show_organiser = (
            organiser_email
            and "cda66539" not in organiser_email
            and "phillip" not in organiser_email
        )
        organiser_name = (
            event.get("organizer", {})
                 .get("emailAddress", {})
                 .get("name", "")
        )

        items.append({
            "type": "ColumnSet",
            "spacing": "Small" if i == 0 else "Medium",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": time_str,
                            "weight": "Bolder",
                            "size": "Small",
                            "color": "Warning",
                            "horizontalAlignment": "Right",
                            "spacing": "None",
                        }
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "spacing": "Small",
                    "items": [
                        item for item in [
                            {
                                "type": "TextBlock",
                                "text": subject,
                                "weight": "Bolder",
                                "wrap": True,
                                "spacing": "None",
                            },
                            {
                                "type": "TextBlock",
                                "text": f"📍 {location}",
                                "isSubtle": True,
                                "size": "Small",
                                "wrap": True,
                                "spacing": "None",
                            } if location else None,
                            {
                                "type": "TextBlock",
                                "text": f"👤 {organiser_name}",
                                "isSubtle": True,
                                "size": "Small",
                                "spacing": "None",
                            } if show_organiser else None,
                        ]
                        if item is not None
                    ],
                },
            ],
        })

    return items


# ── Agenda card builder ────────────────────────────────────────────────────────
def _build_agenda_card(
    events: list[dict],
    now_london: datetime,
    tz_label: str,
    label: str = "TODAY'S AGENDA",
    intro: str | None = None,
) -> dict:
    """
    Build an agenda Adaptive Card for any given day.
    WHY label and intro parameters:
      label changes the card header — "TODAY'S AGENDA" vs "TOMORROW'S AGENDA".
      intro adds an opening line before the event list — used for the 19:00
      tomorrow card: "Here's what tomorrow looks like, Phillip."
      Both default to today's values so existing call sites are unchanged.
    """
    date_str = now_london.strftime(f"%A %d %B — {tz_label}")

    container_items: list[dict] = []

    if intro:
        container_items.append({
            "type": "TextBlock",
            "text": intro,
            "size": "Medium",
            "wrap": True,
            "spacing": "None",
        })

    container_items += [
        {
            "type": "TextBlock",
            "text": f"📅 {label}",
            "weight": "Bolder",
            "color": "Warning",
            "spacing": "None" if not intro else "Small",
        },
        {
            "type": "TextBlock",
            "text": date_str,
            "isSubtle": True,
            "size": "Small",
            "spacing": "None",
        },
        {
            "type": "TextBlock",
            "text": "─────────────────────",
            "color": "Warning",
            "spacing": "Small",
        },
    ]

    container_items.extend(_build_event_items(events))

    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [
            {
                "type": "Container",
                "style": "emphasis",
                "bleed": True,
                "items": container_items,
            }
        ],
    }


# ── Email fetching ─────────────────────────────────────────────────────────────
def _fetch_emails(token: str, since: datetime | None) -> list[dict]:
    """
    Fetch emails from the Inbox received after `since`.
    WHY $filter on receivedDateTime: Graph filters server-side.
    WHY top=100: a 2-hour window on a busy inbox can exceed 50 items.
    WHY fallback to 2 hours when since is None:
      If last_run has never been written, defaulting to 2 hours prevents
      a flood of historical mail on first use.
    """
    headers = {"Authorization": f"Bearer {token}"}
    if since:
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        since_str     = two_hours_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = (
        "https://graph.microsoft.com/v1.0/users/"
        "cda66539-6f2a-4a27-a5a3-a493061f8711"
        "/mailFolders/Inbox/messages"
        f"?$filter=receivedDateTime ge {since_str}"
        "&$top=100"
        "&$select=id,subject,from,receivedDateTime,categories,isRead,bodyPreview"
        "&$orderby=receivedDateTime desc"
    )
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data   = resp.json()
    emails = data.get("value", [])
    if data.get("@odata.nextLink"):
        logging.warning("emailDigest: more than 100 emails in window — some omitted")
    return emails


# ── Sender photo resolution ────────────────────────────────────────────────────
def _get_sender_photo(token: str, sender_email: str) -> str:
    """
    Resolve a sender's profile photo to a base64 data URI.
    Resolution order: internal M365 user photo → saved contact photo → envelope fallback.
    WHY base64 data URI:
      Adaptive Cards in Teams do not reliably render Graph-authenticated
      image URLs. Embedding as base64 removes the auth dependency entirely.
    """
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(
            f"https://graph.microsoft.com/v1.0/users/{sender_email}/photo/$value",
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            encoded = base64.b64encode(resp.content).decode("utf-8")
            return f"data:image/jpeg;base64,{encoded}"
    except Exception as e:
        logging.debug(f"emailDigest: internal photo lookup failed — {e}")

    try:
        search_resp = requests.get(
            "https://graph.microsoft.com/v1.0/me/contacts"
            f"?$filter=emailAddresses/any(e:e/address eq '{sender_email}')"
            "&$select=id&$top=1",
            headers=headers,
            timeout=10,
        )
        if search_resp.status_code == 200:
            contacts = search_resp.json().get("value", [])
            if contacts:
                contact_id = contacts[0]["id"]
                photo_resp = requests.get(
                    f"https://graph.microsoft.com/v1.0/me/contacts/"
                    f"{contact_id}/photo/$value",
                    headers=headers,
                    timeout=10,
                )
                if photo_resp.status_code == 200:
                    encoded = base64.b64encode(photo_resp.content).decode("utf-8")
                    return f"data:image/jpeg;base64,{encoded}"
    except Exception as e:
        logging.debug(f"emailDigest: contact photo lookup failed — {e}")

    return ENVELOPE_ICON


# ── Blob Storage helpers ───────────────────────────────────────────────────────
def _get_blob_client():
    conn_str = os.environ["AzureWebJobsStorage"]
    service  = BlobServiceClient.from_connection_string(conn_str)
    try:
        service.create_container(BLOB_CONTAINER)
    except Exception:
        pass
    return service.get_blob_client(container=BLOB_CONTAINER, blob=BLOB_NAME)


def _read_last_run() -> datetime | None:
    try:
        client = _get_blob_client()
        data   = client.download_blob().readall().decode("utf-8").strip()
        return datetime.fromisoformat(data).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _write_last_run(timestamp: datetime) -> None:
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
    local = dt.astimezone(LONDON_TZ)
    return local.strftime(f"%H:%M {tz_label} on %a %d %b")


# ── Leo voice helpers ──────────────────────────────────────────────────────────
def _greeting(hour: int, is_first_slot: bool) -> str:
    """
    Return the correct greeting for the slot.
    WHY fixed per slot rather than random:
      Leo's greetings are specific to time of day and defined by Phillip.
      The alternation between 'Phillip' and 'Sir' is deliberate — it
      reflects the register variation in the voice profile and ensures
      neither form of address becomes monotonous across the day.
    WHY is_first_slot takes precedence over hour:
      On Sundays, the first slot is 07:00 — not 05:00. is_first_slot
      is already calculated correctly for Sunday suppression upstream,
      so using it here means the morning greeting is always correct
      regardless of the day.
    """
    if is_first_slot:
        return "Good morning, Sir. Here's the day."
    greetings = {
        7:  "Morning, Phillip.",
        9:  "Morning, Sir.",
        11: "Morning, Phillip.",
        13: "Good afternoon, Phillip.",
        15: "Afternoon, Sir.",
        17: "Evening, Sir.",
        19: "Good evening, Phillip.",
    }
    return greetings.get(hour, "Good morning, Phillip.")


def _email_count_line(count: int) -> str:
    """
    Return a Leo-voice email count line.
    WHY short declaratives:
      The previous text ('We have N emails for you to triage in this moment')
      is too long and too soft for Leo's voice. Short declaratives —
      'Four emails to work through.' — match the character profile exactly.
    WHY number words up to twelve, then digits:
      Written numbers read more naturally in a conversational context.
      Above twelve, digits are clearer.
    WHY random choice for zero:
      Two equivalent phrasings for an empty inbox. Alternating at random
      avoids the same line appearing every quiet morning.
    """
    if count == 0:
        return random.choice(["Nothing in the inbox.", "Inbox is clear."])
    word_map = {
        1: "One", 2: "Two", 3: "Three", 4: "Four", 5: "Five",
        6: "Six", 7: "Seven", 8: "Eight", 9: "Nine", 10: "Ten",
        11: "Eleven", 12: "Twelve",
    }
    word = word_map.get(count, str(count))
    if count == 1:
        return "One email."
    return f"{word} emails to work through."


# ── Morning briefing card builder ──────────────────────────────────────────────
def _build_morning_briefing_card(
    weather: dict | None,
    events: list[dict],
    now_london: datetime,
    tz_label: str,
) -> dict:
    """
    Build the unified first-slot card: greeting + weather + agenda.
    WHY a single card rather than three:
      Previously the first slot sent three separate cards — greeting,
      weather, agenda. A single card is one scroll position, one message
      in the channel, and a more coherent briefing. Leo greets and
      briefs the day in one delivery. The email count and inbox follow
      immediately in the concertina card.
    WHY weather before agenda:
      The day starts with conditions (weather), then commitments (agenda).
      That is the natural planning order.
    WHY weather section is conditional:
      If the Open-Meteo fetch fails, the card renders without the weather
      section rather than failing entirely. A morning without weather is
      better than no morning card.
    WHY greeting is two TextBlocks:
      "Good morning, Sir." is the address — large and immediate.
      "Here's the day." is the handover — one size smaller, visually
      subordinate. The line break gives each phrase its own weight
      without requiring a separate card.
    """
    date_str = now_london.strftime(f"%A %d %B — {tz_label}")

    items: list[dict] = [
        {
            "type": "TextBlock",
            "text": "Good morning, Sir.",
            "weight": "Bolder",
            "size": "ExtraLarge",
            "color": "Warning",
            "spacing": "None",
            "wrap": True,
        },
        {
            "type": "TextBlock",
            "text": "Here's the day.",
            "weight": "Bolder",
            "size": "Large",
            "color": "Warning",
            "spacing": "None",
            "wrap": True,
        },
        {
            "type": "TextBlock",
            "text": date_str,
            "isSubtle": True,
            "size": "Small",
            "spacing": "None",
        },
    ]

    # ── Weather section ───────────────────────────────────────────────────────
    if weather:
        today    = weather["today"]
        forecast = weather["forecast"]

        items.append({
            "type": "TextBlock",
            "text": "─────────────────────",
            "color": "Warning",
            "spacing": "Medium",
        })
        items.append({
            "type": "TextBlock",
            "text": "🌦️ WEATHER — BASINGSTOKE",
            "weight": "Bolder",
            "color": "Warning",
            "spacing": "None",
        })
        items.append({
            "type": "ColumnSet",
            "spacing": "Small",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "verticalContentAlignment": "Center",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": today["emoji"],
                            "size": "ExtraLarge",
                            "spacing": "None",
                        }
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "spacing": "Small",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": today["desc"],
                            "weight": "Bolder",
                            "size": "Large",
                            "spacing": "None",
                        },
                        {
                            "type": "TextBlock",
                            "text": f"{today['high']}°C / {today['low']}°C",
                            "size": "Medium",
                            "spacing": "None",
                        },
                        {
                            "type": "TextBlock",
                            "text": (
                                f"🌂 {today['rain_pct']}% chance of rain"
                                f"   💨 {today['wind_kmh']} km/h {today['wind_dir']}"
                            ),
                            "isSubtle": True,
                            "size": "Small",
                            "wrap": True,
                            "spacing": "None",
                        },
                    ],
                },
            ],
        })

        forecast_columns = []
        for day in forecast:
            dt       = datetime.strptime(day["date"], "%Y-%m-%d")
            day_name = dt.strftime("%a")
            forecast_columns.append({
                "type": "Column",
                "width": "stretch",
                "items": [
                    {
                        "type": "TextBlock",
                        "text": day_name,
                        "horizontalAlignment": "Center",
                        "weight": "Bolder",
                        "size": "Small",
                        "spacing": "None",
                    },
                    {
                        "type": "TextBlock",
                        "text": day["emoji"],
                        "horizontalAlignment": "Center",
                        "size": "Large",
                        "spacing": "None",
                    },
                    {
                        "type": "TextBlock",
                        "text": f"{day['high']}°",
                        "horizontalAlignment": "Center",
                        "weight": "Bolder",
                        "size": "Small",
                        "spacing": "None",
                    },
                    {
                        "type": "TextBlock",
                        "text": f"{day['low']}°",
                        "horizontalAlignment": "Center",
                        "isSubtle": True,
                        "size": "Small",
                        "spacing": "None",
                    },
                ],
            })

        items.append({
            "type": "TextBlock",
            "text": "─────────────────────",
            "color": "Warning",
            "spacing": "Small",
        })
        items.append({"type": "ColumnSet", "spacing": "Small", "columns": forecast_columns})

    # ── Agenda section ────────────────────────────────────────────────────────
    items.append({
        "type": "TextBlock",
        "text": "─────────────────────",
        "color": "Warning",
        "spacing": "Medium",
    })
    items.append({
        "type": "TextBlock",
        "text": "📅 TODAY'S AGENDA",
        "weight": "Bolder",
        "color": "Warning",
        "spacing": "None",
    })
    items.extend(_build_event_items(events))

    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [
            {
                "type": "Container",
                "style": "emphasis",
                "bleed": True,
                "items": items,
            }
        ],
    }


# ── Clear inbox card builder ───────────────────────────────────────────────────
def _build_clear_inbox_card(
    greeting: str | None,
    count_line: str,
) -> dict:
    """
    Build a minimal card for slots where the inbox is empty.
    WHY send a card rather than nothing:
      Leo checks in at every slot. If he sends nothing when the inbox is
      clear, Phillip has no confirmation the digest ran. The card is brief
      — greeting (if applicable) and a single count line — but it confirms
      the run happened and the inbox is genuinely empty.
    WHY greeting is optional:
      At the first slot, the greeting is already in the morning briefing
      card. Passing greeting=None omits it from this card to avoid
      a second "Good morning, Sir." in the channel.
    """
    items: list[dict] = []

    if greeting:
        items.append({
            "type": "TextBlock",
            "text": greeting,
            "weight": "Bolder",
            "size": "Large",
            "color": "Warning",
            "spacing": "None",
        })

    items.append({
        "type": "TextBlock",
        "text": count_line,
        "size": "Medium",
        "wrap": True,
        "spacing": "Small" if greeting else "None",
    })

    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [
            {
                "type": "Container",
                "style": "emphasis",
                "bleed": True,
                "items": items,
            }
        ],
    }


# ── Goodnight card builder ─────────────────────────────────────────────────────
def _build_goodnight_card() -> dict:
    """
    Build the end-of-day closing card sent at 19:00 after the inbox and
    tomorrow's agenda have been delivered.
    WHY a dedicated card:
      Leo closes the day the same way he would leave the Oval Office —
      "Good night, Sir." It is the last thing in the channel each day.
      A dedicated card gives it visual weight and a clear close.
    WHY after tomorrow's agenda, not before:
      Leo hands over the inbox, shows tomorrow, then closes. Saying
      goodnight before the look-ahead would feel like leaving the room
      before the briefing is finished.
    """
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
                    {
                        "type": "TextBlock",
                        "text": "Good night, Sir.",
                        "weight": "Bolder",
                        "size": "ExtraLarge",
                        "color": "Warning",
                        "spacing": "None",
                    }
                ],
            }
        ],
    }


# ── Concertina email card builder ──────────────────────────────────────────────
def _build_concertina_card(
    emails: list[dict],
    tz_label: str,
    token: str,
    now_london: datetime,
    greeting: str | None,
    count_line: str,
) -> dict:
    """
    Build a single Adaptive Card containing all emails as collapsible rows.
    WHY concertina rather than one card per email:
      A single card with all emails collapsed means the full inbox is
      visible at a glance. Phillip expands only the email he is triaging.
      The previous one-card-per-email pattern anchored Teams to the last
      card and required scrolling up through every email before triaging.
    WHY greeting and count_line parameters:
      At the first slot, greeting=None — the greeting was already delivered
      in the morning briefing card. At all other slots, the greeting appears
      at the top of the concertina header so Leo's voice opens every card.
      count_line always appears below the greeting (or at the top if no
      greeting) — it sits immediately above the emails it describes.
    WHY Action.ToggleVisibility:
      Native Adaptive Card mechanism for show/hide without a round-trip
      to the backend. The toggle fires client-side in Teams — instant.
    WHY triage button container styles:
      Action=accent (blue), Waiting For=warning (amber),
      View=emphasis (grey), Delete=attention (red). These are the four
      native Adaptive Card container style values that produce distinct
      background colours in Teams. Previously the buttons were unstyled
      and indistinguishable from plain containers.
    """
    date_str = now_london.strftime(f"%A %d %B %Y — %H:%M {tz_label}")

    # ── Card header ───────────────────────────────────────────────────────────
    header_items: list[dict] = []

    if greeting:
        header_items.append({
            "type": "TextBlock",
            "text": greeting,
            "weight": "Bolder",
            "size": "Large",
            "color": "Warning",
            "spacing": "None",
        })

    header_items.append({
        "type": "TextBlock",
        "text": count_line,
        "size": "Medium",
        "wrap": True,
        "spacing": "Small" if greeting else "None",
    })

    if not greeting:
        # WHY EMAIL TRIAGE label only when there is no greeting:
        #   When Leo greets, the context is clear — his name and the emails
        #   follow immediately. When there is no greeting (first slot,
        #   greeting already delivered), a label anchors the card.
        header_items.insert(0, {
            "type": "TextBlock",
            "text": "📧 EMAIL TRIAGE",
            "weight": "Bolder",
            "color": "Warning",
            "spacing": "None",
        })

    header_items.append({
        "type": "TextBlock",
        "text": date_str,
        "isSubtle": True,
        "size": "Small",
        "spacing": "None",
    })

    body: list[dict] = [
        {
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "items": header_items,
        }
    ]

    # ── Email rows ────────────────────────────────────────────────────────────
    for i, email in enumerate(emails):
        sender_name  = (
            email.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
        )
        sender_addr  = (
            email.get("from", {}).get("emailAddress", {}).get("address", "")
        )
        subject      = (email.get("subject", "") or "(no subject)").strip()
        body_preview = (email.get("bodyPreview", "") or "").strip()
        email_id     = email.get("id", "")

        if len(body_preview) > 200:
            body_preview = body_preview[:197] + "…"

        received_str = email.get("receivedDateTime", "")
        try:
            received_utc    = datetime.fromisoformat(received_str.replace("Z", "+00:00"))
            received_london = received_utc.astimezone(LONDON_TZ)
            time_label      = received_london.strftime("%H:%M")
        except Exception:
            time_label = ""

        detail_id = f"email_detail_{i}"
        photo_uri = (
            _get_sender_photo(token, sender_addr) if sender_addr else ENVELOPE_ICON
        )

        if i > 0:
            body.append({
                "type": "TextBlock",
                "text": "─────────────────────",
                "color": "Warning",
                "spacing": "None",
                "size": "Small",
            })

        # ── Collapsed summary row ─────────────────────────────────────────────
        # WHY selectAction on the Container:
        #   Tapping anywhere on the row toggles the detail section.
        #   Large tap target — important on mobile.
        body.append({
            "type": "Container",
            "spacing": "Small",
            "selectAction": {
                "type": "Action.ToggleVisibility",
                "targetElements": [detail_id],
            },
            "items": [
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
                                    "text": sender_name,
                                    "weight": "Bolder",
                                    "size": "Small",
                                    "spacing": "None",
                                    "wrap": False,
                                },
                                {
                                    "type": "TextBlock",
                                    "text": subject,
                                    "isSubtle": True,
                                    "size": "Small",
                                    "spacing": "None",
                                    "wrap": False,
                                },
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "auto",
                            "verticalContentAlignment": "Center",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": time_label,
                                    "isSubtle": True,
                                    "size": "Small",
                                    "horizontalAlignment": "Right",
                                    "spacing": "None",
                                },
                                {
                                    "type": "TextBlock",
                                    "text": "▼",
                                    "isSubtle": True,
                                    "size": "Small",
                                    "horizontalAlignment": "Right",
                                    "spacing": "None",
                                },
                            ],
                        },
                    ],
                }
            ],
        })

        # ── Expanded detail section ───────────────────────────────────────────
        body.append({
            "type": "Container",
            "id": detail_id,
            "isVisible": False,
            "spacing": "Small",
            "items": [
                {
                    "type": "ColumnSet",
                    "spacing": "Small",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "auto",
                            "verticalContentAlignment": "Center",
                            "items": [
                                {
                                    "type": "Image",
                                    "url": photo_uri,
                                    "size": "Small",
                                    "style": "Person",
                                    "altText": f"Photo of {sender_name}",
                                }
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "verticalContentAlignment": "Center",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": sender_addr,
                                    "isSubtle": True,
                                    "size": "Small",
                                    "wrap": True,
                                    "spacing": "None",
                                }
                            ],
                        },
                    ],
                },
                {
                    "type": "TextBlock",
                    "text": body_preview,
                    "isSubtle": True,
                    "wrap": True,
                    "maxLines": 4,
                    "spacing": "Small",
                },
                # ── Triage buttons ────────────────────────────────────────────
                # WHY ActionSet rather than container-based buttons:
                #   Container-based buttons with style colours rendered as flat
                #   pastel boxes — visually weak and not obviously interactive.
                #   ActionSet renders native Teams buttons with proper visual
                #   weight, raised appearance, and clear affordance.
                # WHY these three styles:
                #   positive  — blue, conventionally means "do something" (Action)
                #   default   — neutral, for secondary actions (Waiting For, View)
                #   destructive — red, conventionally means "irreversible" (Delete)
                # NOTE: Action.Submit payloads for Action and Waiting For are
                #   placeholders. A dedicated session will wire these to Graph
                #   API calls via the messages function and taskChain.
                {
                    "type": "ActionSet",
                    "spacing": "Small",
                    "actions": [
                        {
                            "type": "Action.Submit",
                            "title": "Action",
                            "style": "positive",
                            "data": {
                                "triageAction": "action",
                                "emailId": email_id,
                            },
                        },
                        {
                            "type": "Action.Submit",
                            "title": "Waiting For",
                            "style": "default",
                            "data": {
                                "triageAction": "waiting",
                                "emailId": email_id,
                            },
                        },
                        {
                            "type": "Action.OpenUrl",
                            "title": "View",
                            "style": "default",
                            "url": "https://outlook.office365.com/mail/",
                        },
                        {
                            "type": "Action.Submit",
                            "title": "Delete",
                            "style": "destructive",
                            "data": {
                                "triageAction": "delete",
                                "emailId": email_id,
                            },
                        },
                    ],
                },
            ],
        })

    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": body,
    }


# ── Teams delivery ─────────────────────────────────────────────────────────────
def _get_delivery_config() -> tuple[str, str, str, str]:
    bot_token   = _get_bot_token()
    service_url = os.environ["TEAMS_SERVICE_URL"].rstrip("/")
    channel_id  = os.environ["TEAMS_DAILY_OPERATIONS_ID"]
    bot_app_id  = os.environ["BOT_APP_ID"]
    return bot_token, service_url, channel_id, bot_app_id


def _send_text_to_teams(text: str) -> None:
    bot_token, service_url, channel_id, bot_app_id = _get_delivery_config()
    url  = f"{service_url}/v3/conversations/{channel_id}/activities"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type": "application/json",
        },
        json={
            "type": "message",
            "from": {"id": f"28:{bot_app_id}", "name": "Leo"},
            "text": text,
        },
        timeout=15,
    )
    resp.raise_for_status()
    logging.info(f"emailDigest: plain text delivered — status {resp.status_code}")


def _send_card_to_teams(card: dict) -> None:
    bot_token, service_url, channel_id, bot_app_id = _get_delivery_config()
    url     = f"{service_url}/v3/conversations/{channel_id}/activities"
    payload = {
        "type": "message",
        "from": {"id": f"28:{bot_app_id}", "name": "Leo"},
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
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    logging.info(f"emailDigest: card delivered — status {resp.status_code}")
