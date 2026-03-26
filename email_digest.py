"""
email_digest.py — Monica Email Digest Timer Trigger
Fires every 2 hours (05:00–19:00 UTC daily).
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
  - All-day events from the previous day are filtered out. Graph's
    calendarView boundary is inclusive on startDateTime, which causes
    yesterday's all-day event (ending exactly at today's midnight) to
    appear in the agenda. A post-fetch filter removes it.
  - Concertina email card replaces the previous one-card-per-email and
    multi-attachment patterns. All emails are delivered as a single
    Adaptive Card with collapsed rows. Each row shows sender, subject,
    and received time. Tapping a row expands the body preview and triage
    buttons via Action.ToggleVisibility.
  - _build_header_card, _build_card, and _send_cards_to_teams removed.
  - _build_greeting_card and _build_concertina_card added.

Slot logic:
  First slot  (weather + agenda + digest):
    Mon–Sat: 05:00 UTC
    Sun:     07:00 UTC  (05:00 is suppressed)
  Second slot (agenda + digest only):
    Mon–Sat: 07:00 UTC
    Sun:     09:00 UTC
  All other slots: email digest only
"""
import os
import logging
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
# WHY hardcoded: weather is always for Phillip's home base. If location
# ever changes it is a one-line edit. No need for dynamic lookup.
WEATHER_LAT = 51.2654
WEATHER_LON = -1.0872

# WHY an embedded SVG rather than a hosted URL:
#   Embedding the icon as a base64 data URI means it always renders,
#   with zero external dependencies.
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
    weekday    = now_utc.weekday()   # 0=Mon … 6=Sun
    hour       = now_utc.hour

    # Suppress Sunday 05:00 UTC
    if weekday == 6 and hour == 5:
        logging.info("emailDigest: Sunday 05:00 UTC suppressed.")
        return

    # ── Determine slot type ───────────────────────────────────────────────────
    # WHY slot logic in UTC:
    #   The cron schedule is defined in UTC. Comparing against UTC hour
    #   is therefore always correct, even when London is in BST.
    is_first_slot  = (weekday != 6 and hour == 5) or (weekday == 6 and hour == 7)
    is_second_slot = (weekday != 6 and hour == 7) or (weekday == 6 and hour == 9)

    logging.info(
        f"emailDigest: starting at {now_utc.isoformat()} UTC — "
        f"first_slot={is_first_slot}, second_slot={is_second_slot}"
    )

    # ── Fetch Graph token (used for email, calendar, contacts, photos) ────────
    token = get_access_token()
    if not token:
        logging.error("emailDigest: no access token — aborting")
        return

    # ── Fetch emails before sending anything ─────────────────────────────────
    # WHY fetch emails first:
    #   The greeting card states how many emails there are. We need the
    #   count before sending the first message. Fetching early also means
    #   last_run is written at the start of delivery, not after — so a
    #   crash mid-send does not cause duplicate emails on the next run.
    last_run_utc = _read_last_run()
    logging.info(
        f"emailDigest: last run was "
        f"{last_run_utc.isoformat() if last_run_utc else 'never'}"
    )
    emails = _fetch_emails(token, last_run_utc)
    logging.info(f"emailDigest: fetched {len(emails)} emails")
    _write_last_run(now_utc)

    # ── Step 1: Greeting card (always first) ──────────────────────────────────
    # WHY greeting first:
    #   Leo greets Phillip before delivering any briefing. The greeting
    #   includes the email count so the workload is known immediately,
    #   before weather and agenda are read.
    greeting_card = _build_greeting_card(now_london, tz_label, len(emails))
    _send_card_to_teams(greeting_card)
    time.sleep(CARD_SEND_DELAY)
    logging.info("emailDigest: greeting card delivered")

    # ── Step 2: Weather card (first slot only) ────────────────────────────────
    # WHY weather only on first slot:
    #   The weather card sets up the day. Repeating it every 2 hours
    #   would be noise. Once in the morning is the right cadence.
    if is_first_slot:
        try:
            weather      = _fetch_weather()
            weather_card = _build_weather_card(weather, now_london, tz_label)
            _send_card_to_teams(weather_card)
            time.sleep(CARD_SEND_DELAY)
            logging.info("emailDigest: weather card delivered")
        except Exception as e:
            logging.error(f"emailDigest: weather card failed — {e}")

    # ── Step 3: Agenda card (first and second slot) ───────────────────────────
    # WHY agenda on both first and second slot:
    #   First slot gives the full day view at wake-up. Second slot is a
    #   useful reminder before the day starts properly. After that,
    #   repeating the agenda would be noise.
    if is_first_slot or is_second_slot:
        try:
            events      = _fetch_calendar_events(token, now_utc)
            agenda_card = _build_agenda_card(events, now_london, tz_label)
            _send_card_to_teams(agenda_card)
            time.sleep(CARD_SEND_DELAY)
            logging.info(
                f"emailDigest: agenda card delivered — {len(events)} event(s)"
            )
        except Exception as e:
            logging.error(f"emailDigest: agenda card failed — {e}")

    # ── Step 4: Email concertina card (all slots) ─────────────────────────────
    if not emails:
        # Greeting already told Phillip the inbox is clear. Nothing more to send.
        return

    # WHY reversed: Graph returns emails newest-first (receivedDateTime desc).
    #   Reversing puts the oldest email at the top so Phillip reads
    #   chronologically top-to-bottom — the natural triage order.
    emails = list(reversed(emails))

    concertina_card = _build_concertina_card(emails, tz_label, token, now_london)
    _send_card_to_teams(concertina_card)
    logging.info(
        f"emailDigest: concertina card delivered — {len(emails)} email(s)"
    )


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
      Free with no API key required — no Key Vault secret needed, no
      rate limit concerns at Monica's usage volume. Excellent UK coverage.
      Temperatures are returned in Celsius natively.
    WHY daily rather than hourly:
      The morning card gives a day-level overview. Hourly data is more
      detail than is useful at 05:00.
    Returns a dict with keys:
      today: {description, emoji, high, low, rain_pct, wind_kmh, wind_dir}
      forecast: list of 4 dicts [{day_name, emoji, high, low}]
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
        code       = data["weathercode"][i]
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

    today    = parse_day(0)
    forecast = [parse_day(i) for i in range(1, 5)]
    return {"today": today, "forecast": forecast}


def _wmo_to_label(code: int) -> tuple[str, str]:
    """
    Map a WMO weather interpretation code to an emoji and short description.
    WHY emoji in TextBlocks rather than weather icon images:
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
      "Wind from the SW" is immediately meaningful. "Wind from 225°"
      requires mental conversion.
    """
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    index = round(degrees / 45) % 8
    return directions[index]


# ── Weather card builder ───────────────────────────────────────────────────────
def _build_weather_card(
    weather: dict, now_london: datetime, tz_label: str
) -> dict:
    today    = weather["today"]
    forecast = weather["forecast"]
    date_str = now_london.strftime(f"%A %d %B — {tz_label}")

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
                        "text": "🌦️ WEATHER — BASINGSTOKE",
                        "weight": "Bolder",
                        "color": "Warning",
                        "spacing": "None",
                    },
                    {
                        "type": "TextBlock",
                        "text": date_str,
                        "isSubtle": True,
                        "size": "Small",
                        "spacing": "None",
                    },
                    {
                        "type": "ColumnSet",
                        "spacing": "Medium",
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
                                        "text": (
                                            f"{today['high']}°C / {today['low']}°C"
                                        ),
                                        "size": "Medium",
                                        "spacing": "None",
                                    },
                                    {
                                        "type": "TextBlock",
                                        "text": (
                                            f"🌂 {today['rain_pct']}% chance of rain"
                                            f"   💨 {today['wind_kmh']} km/h"
                                            f" {today['wind_dir']}"
                                        ),
                                        "isSubtle": True,
                                        "size": "Small",
                                        "wrap": True,
                                        "spacing": "None",
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "type": "TextBlock",
                        "text": "─────────────────────",
                        "color": "Warning",
                        "spacing": "Small",
                    },
                    {
                        "type": "ColumnSet",
                        "spacing": "Small",
                        "columns": forecast_columns,
                    },
                ],
            }
        ],
    }


# ── Calendar fetching ──────────────────────────────────────────────────────────
def _fetch_calendar_events(token: str, now_utc: datetime) -> list[dict]:
    """
    Fetch today's calendar events from Microsoft Graph.
    WHY calendarView rather than /events with a filter:
      calendarView automatically expands recurring events into individual
      instances. A filter on /events would only return the series master.
    WHY today midnight to midnight in London time:
      We want events for the calendar day as Phillip experiences it.
    WHY strftime rather than isoformat:
      isoformat() on a timezone-aware datetime produces +00:00 or +01:00
      suffixes which Graph's calendarView endpoint rejects with a 400.
      strftime produces a clean naive datetime string that Graph accepts.
    WHY filter yesterday's all-day events after fetching:
      Graph's calendarView startDateTime is inclusive. An all-day event
      from yesterday has end = today at 00:00:00, which is exactly our
      startDateTime — so Graph returns it. We filter it out in Python by
      checking whether the all-day event's end date string starts with
      today's date. Yesterday's event ends on today's date; today's
      all-day event ends on tomorrow's date. This is safe: multi-day
      events that span into today have an end date after today and are
      correctly retained.
    """
    london_now = now_utc.astimezone(LONDON_TZ)
    day_start  = london_now.replace(hour=0, minute=0, second=0, microsecond=0)
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
        logging.warning("emailDigest: more than 20 events today — some omitted")

    # Filter out all-day events that belong to yesterday.
    # Yesterday's all-day event: end.dateTime starts with today's date string.
    # Today's all-day event:     end.dateTime starts with tomorrow's date string.
    today_date_str = day_start.strftime("%Y-%m-%d")
    filtered = []
    for event in events:
        if event.get("isAllDay"):
            end_dt_str = event.get("end", {}).get("dateTime", "")
            if end_dt_str.startswith(today_date_str):
                logging.info(
                    f"emailDigest: skipping yesterday's all-day event "
                    f"'{event.get('subject', '')}'"
                )
                continue
        filtered.append(event)

    return filtered


# ── Agenda card builder ────────────────────────────────────────────────────────
def _build_agenda_card(
    events: list[dict], now_london: datetime, tz_label: str
) -> dict:
    date_str = now_london.strftime(f"%A %d %B — {tz_label}")

    if not events:
        event_items = [
            {
                "type": "TextBlock",
                "text": "No meetings today — enjoy the space.",
                "isSubtle": True,
                "wrap": True,
                "spacing": "Small",
            }
        ]
    else:
        event_items = []
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
                    time_str = (
                        f"{start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}"
                    )
                except Exception:
                    time_str = ""

            subject  = (event.get("subject") or "No title").strip()
            location = (
                event.get("location", {}).get("displayName", "") or ""
            ).strip()

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

            spacing = "Small" if i == 0 else "Medium"

            event_items.append({
                "type": "ColumnSet",
                "spacing": spacing,
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
                            item
                            for item in [
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
                                }
                                if location
                                else None,
                                {
                                    "type": "TextBlock",
                                    "text": f"👤 {organiser_name}",
                                    "isSubtle": True,
                                    "size": "Small",
                                    "spacing": "None",
                                }
                                if show_organiser
                                else None,
                            ]
                            if item is not None
                        ],
                    },
                ],
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
                "items": [
                    {
                        "type": "TextBlock",
                        "text": "📅 TODAY'S AGENDA",
                        "weight": "Bolder",
                        "color": "Warning",
                        "spacing": "None",
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
                    *event_items,
                ],
            }
        ],
    }


# ── Email fetching ─────────────────────────────────────────────────────────────
def _fetch_emails(token: str, since: datetime | None) -> list[dict]:
    """
    Fetch emails from the Inbox received after `since`.
    WHY $filter on receivedDateTime: Graph filters server-side, keeping payload small.
    WHY top=100: a 2-hour window on a busy inbox might exceed the default 50-item limit.
    WHY $orderby=receivedDateTime desc: list is reversed after fetching so the
      concertina card stacks oldest-at-top — the natural chronological triage order.
    WHY fallback to 2 hours when since is None:
      If last_run has never been written (e.g. first ever run, or blob was lost),
      defaulting to 2 hours prevents a flood of historical mail on first use.
    """
    headers = {"Authorization": f"Bearer {token}"}
    if since:
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        since_str     = two_hours_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

    filter_clause = f"receivedDateTime ge {since_str}"
    url = (
        "https://graph.microsoft.com/v1.0/users/"
        "cda66539-6f2a-4a27-a5a3-a493061f8711"
        "/mailFolders/Inbox/messages"
        f"?$filter={filter_clause}"
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
        search_url = (
            "https://graph.microsoft.com/v1.0/me/contacts"
            f"?$filter=emailAddresses/any(e:e/address eq '{sender_email}')"
            "&$select=id&$top=1"
        )
        search_resp = requests.get(search_url, headers=headers, timeout=10)
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


# ── Time formatting helpers ────────────────────────────────────────────────────
def _fmt_time(dt: datetime, tz_label: str) -> str:
    local = dt.astimezone(LONDON_TZ)
    return local.strftime(f"%H:%M {tz_label} on %a %d %b")


def _greeting(now_london: datetime) -> str:
    hour = now_london.hour
    if hour < 12:
        return "Good morning, Phillip"
    elif hour < 17:
        return "Good afternoon, Phillip"
    else:
        return "Good evening, Phillip"


# ── Greeting card builder ──────────────────────────────────────────────────────
def _build_greeting_card(
    now_london: datetime, tz_label: str, email_count: int
) -> dict:
    """
    Build the opening greeting card — always the first message Leo delivers.
    WHY greeting first:
      A chief of staff greets the principal before delivering any briefing.
      Including the email count here means Phillip knows his workload
      before reading weather or agenda.
    WHY Adaptive Card rather than plain text:
      The card format gives us colour and size control — the large amber
      greeting is visually distinct and immediately readable in the channel.
    """
    greeting_text = _greeting(now_london)

    if email_count == 0:
        triage_text = "Your inbox is clear — nothing to triage in this moment."
    elif email_count == 1:
        triage_text = "We have 1 email for you to triage in this moment."
    else:
        triage_text = (
            f"We have {email_count} emails for you to triage in this moment."
        )

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
                        "text": greeting_text,
                        "weight": "Bolder",
                        "size": "ExtraLarge",
                        "color": "Warning",
                        "spacing": "None",
                    },
                    {
                        "type": "TextBlock",
                        "text": triage_text,
                        "size": "Medium",
                        "wrap": True,
                        "spacing": "Small",
                    },
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
) -> dict:
    """
    Build a single Adaptive Card containing all emails as collapsible rows.
    WHY concertina rather than one card per email:
      The previous pattern sent one card per email, meaning Teams anchored
      to the last card and Phillip had to scroll up through every email
      before triaging — processing each one twice. A single card with all
      emails collapsed means the full inbox is visible at a glance. Phillip
      expands only the email he is triaging at that moment.
    WHY Action.ToggleVisibility:
      This is the native Adaptive Card mechanism for show/hide without
      requiring a round-trip to a backend. The toggle fires client-side
      in Teams — instant response, no Bot Framework call needed.
    WHY unique IDs per email (email_detail_0, email_detail_1 …):
      Action.ToggleVisibility targets elements by their id. Each detail
      section needs a unique id so toggling one row does not affect others.
    WHY photos in the expanded detail only:
      Showing photos in the collapsed summary row would make it much taller
      and defeat the purpose of the compact view. Photos appear only when
      an email is expanded, where there is space for them.
    """
    date_str = now_london.strftime(f"%A %d %B %Y — %H:%M {tz_label}")

    # Card header
    body: list[dict] = [
        {
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "items": [
                {
                    "type": "TextBlock",
                    "text": "📧 EMAIL TRIAGE",
                    "weight": "Bolder",
                    "color": "Warning",
                    "spacing": "None",
                },
                {
                    "type": "TextBlock",
                    "text": date_str,
                    "isSubtle": True,
                    "size": "Small",
                    "spacing": "None",
                },
            ],
        }
    ]

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
            received_utc    = datetime.fromisoformat(
                received_str.replace("Z", "+00:00")
            )
            received_london = received_utc.astimezone(LONDON_TZ)
            time_label      = received_london.strftime("%H:%M")
        except Exception:
            time_label = ""

        detail_id = f"email_detail_{i}"

        # Resolve sender photo (shown in expanded detail only)
        photo_uri = (
            _get_sender_photo(token, sender_addr) if sender_addr else ENVELOPE_ICON
        )

        # Separator between emails (not before the first one)
        if i > 0:
            body.append({
                "type": "TextBlock",
                "text": "─────────────────────",
                "color": "Warning",
                "spacing": "None",
                "size": "Small",
            })

        # ── Collapsed summary row (always visible) ────────────────────────────
        # WHY selectAction on the Container:
        #   Tapping anywhere on the summary row toggles the detail section.
        #   This gives a large tap target, which is important on mobile.
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

        # ── Expanded detail section (hidden until tapped) ─────────────────────
        # WHY isVisible: False:
        #   The detail starts collapsed. Action.ToggleVisibility flips this
        #   client-side in Teams when the summary row is tapped.
        body.append({
            "type": "Container",
            "id": detail_id,
            "isVisible": False,
            "spacing": "Small",
            "items": [
                # Sender photo + email address
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
                # Body preview
                {
                    "type": "TextBlock",
                    "text": body_preview,
                    "isSubtle": True,
                    "wrap": True,
                    "maxLines": 4,
                    "spacing": "Small",
                },
                # Triage buttons
                # NOTE: Button actions (move to Done, etc.) are placeholder
                # Submit actions for now. A dedicated session will wire these
                # to actual Graph API calls via the taskChain function.
                {
                    "type": "ColumnSet",
                    "spacing": "Small",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "Container",
                                    "style": "default",
                                    "selectAction": {
                                        "type": "Action.Submit",
                                        "data": {
                                            "triageAction": "action",
                                            "emailId": email_id,
                                        },
                                    },
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "Action",
                                            "horizontalAlignment": "Center",
                                            "weight": "Bolder",
                                            "size": "Small",
                                            "spacing": "Small",
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "Container",
                                    "style": "default",
                                    "selectAction": {
                                        "type": "Action.Submit",
                                        "data": {
                                            "triageAction": "waiting",
                                            "emailId": email_id,
                                        },
                                    },
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "Waiting For",
                                            "horizontalAlignment": "Center",
                                            "weight": "Bolder",
                                            "size": "Small",
                                            "spacing": "Small",
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "Container",
                                    "style": "default",
                                    "selectAction": {
                                        "type": "Action.OpenUrl",
                                        "url": "https://outlook.office365.com/mail/",
                                    },
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "View",
                                            "horizontalAlignment": "Center",
                                            "weight": "Bolder",
                                            "size": "Small",
                                            "spacing": "Small",
                                        }
                                    ],
                                }
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "Container",
                                    "style": "default",
                                    "selectAction": {
                                        "type": "Action.Submit",
                                        "data": {
                                            "triageAction": "delete",
                                            "emailId": email_id,
                                        },
                                    },
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "Delete",
                                            "horizontalAlignment": "Center",
                                            "weight": "Bolder",
                                            "size": "Small",
                                            "spacing": "Small",
                                        }
                                    ],
                                }
                            ],
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
    url = f"{service_url}/v3/conversations/{channel_id}/activities"
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
    url = f"{service_url}/v3/conversations/{channel_id}/activities"
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
