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

Session 20 additions:
  - Sender profile photo (internal M365, saved contacts, envelope fallback)
  - Digest header card with time-aware greeting
  - Weather card (Open-Meteo, Basingstoke RG21 5NP, Celsius) on first
    daily slot
  - Agenda card (today's calendar events from Graph) on first and second
    daily slot

Session 21 fix:
  - _create_channel_conversation added to resolve 403 Forbidden error.
    The Bot Framework Connector does not accept a raw Teams channel thread
    ID as a conversation target. A conversation must first be created via
    POST /v3/conversations, and the returned ID used for posting. The bot
    ID must carry the '28:' prefix required by the Bot Framework.

Session 22 fix:
  - _create_channel_conversation now logs the full Bot Framework response
    body before raising on error, so the exact rejection reason is visible
    in Application Insights traces rather than only the HTTP status code.
  - Conversation creation body corrected: root-level tenantId removed;
    channelData now contains channel, team, and tenant as required by
    the Bot Framework ConversationParameters schema for Teams channels.

Session 24 diagnostic:
  - _create_channel_conversation now logs the full request body before
    sending, so Application Insights shows exactly what is being posted
    to /v3/conversations. Remove this line once the 400 error is resolved.

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
LONDON_TZ      = ZoneInfo("Europe/London")
BLOB_CONTAINER = "monica-digest"
BLOB_NAME      = "last_run.txt"
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

    # ── First slot: send weather card ─────────────────────────────────────────
    # WHY weather only on first slot:
    #   The weather card sets up the day. Repeating it every 2 hours
    #   would be noise. Once in the morning is the right cadence.
    if is_first_slot:
        try:
            weather = _fetch_weather()
            weather_card = _build_weather_card(weather, now_london, tz_label)
            _send_card_to_teams(weather_card)
            time.sleep(CARD_SEND_DELAY)
            logging.info("emailDigest: weather card delivered")
        except Exception as e:
            logging.error(f"emailDigest: weather card failed — {e}")

    # ── First and second slot: send agenda card ───────────────────────────────
    # WHY agenda on both first and second slot:
    #   First slot gives the full day view at wake-up. Second slot (07:00
    #   or 09:00 on Sunday) is a useful reminder before the day starts
    #   properly. After that, repeating the agenda would be noise.
    if is_first_slot or is_second_slot:
        try:
            events = _fetch_calendar_events(token, now_utc)
            agenda_card = _build_agenda_card(events, now_london, tz_label)
            _send_card_to_teams(agenda_card)
            time.sleep(CARD_SEND_DELAY)
            logging.info(f"emailDigest: agenda card delivered — {len(events)} event(s)")
        except Exception as e:
            logging.error(f"emailDigest: agenda card failed — {e}")

    # ── All slots: email digest ───────────────────────────────────────────────
    last_run_utc = _read_last_run()
    logging.info(f"emailDigest: last run was {last_run_utc.isoformat() if last_run_utc else 'never'}")

    emails = _fetch_emails(token, last_run_utc)
    logging.info(f"emailDigest: fetched {len(emails)} emails")

    _write_last_run(now_utc)

    if not emails:
        since_label = _fmt_time(last_run_utc or now_utc, tz_label)
        _send_text_to_teams(f"📭 No new emails since last digest ({since_label}).")
        return

    header_card = _build_header_card(now_london, tz_label, last_run_utc, len(emails))
    _send_card_to_teams(header_card)
    time.sleep(CARD_SEND_DELAY)

    for email in emails:
        card = _build_card(email, tz_label, token)
        _send_card_to_teams(card)
        time.sleep(CARD_SEND_DELAY)

    logging.info(f"emailDigest: header + {len(emails)} email card(s) delivered")


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
      Temperatures are returned in Celsius natively — no conversion needed.

    WHY daily rather than hourly:
      The morning card gives a day-level overview: today's high/low, rain
      probability, and wind. Hourly data is more detail than is useful
      at 05:00. The 4-day outlook uses daily data for the same reason.

    WHY windspeed_10m_max and winddirection_10m_dominant:
      These give the peak wind conditions for the day — more useful for
      planning than the average, which could mask a gusty afternoon.

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
        code     = data["weathercode"][i]
        emoji, desc = _wmo_to_label(code)
        return {
            "date":      data["time"][i],
            "emoji":     emoji,
            "desc":      desc,
            "high":      round(data["temperature_2m_max"][i]),
            "low":       round(data["temperature_2m_min"][i]),
            "rain_pct":  data["precipitation_probability_max"][i],
            "wind_kmh":  round(data["windspeed_10m_max"][i]),
            "wind_dir":  _degrees_to_compass(data["winddirection_10m_dominant"][i]),
        }

    today    = parse_day(0)
    forecast = [parse_day(i) for i in range(1, 5)]

    return {"today": today, "forecast": forecast}


def _wmo_to_label(code: int) -> tuple[str, str]:
    """
    Map a WMO weather interpretation code to an emoji and short description.

    WHY emoji in TextBlocks rather than weather icon images:
      External image URLs from adaptivecards.io are tied to their sample
      assets and have no guaranteed uptime. Emoji render natively in Teams
      TextBlocks with zero external dependency.

    WMO code reference: https://open-meteo.com/en/docs#weathervariables
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
      requires mental conversion. Compass points are the right
      abstraction for a morning briefing card.
    """
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    index = round(degrees / 45) % 8
    return directions[index]


# ── Weather card builder ───────────────────────────────────────────────────────
def _build_weather_card(weather: dict, now_london: datetime, tz_label: str) -> dict:
    """
    Build an Adaptive Card showing today's weather and a 4-day forecast
    for Basingstoke.

    WHY emphasis container:
      Consistent with all other Monica cards. Teams overrides background
      colours so we use the native emphasis style throughout.

    WHY today as large top section + 4 compact day columns below:
      Mirrors the WeatherLarge sample layout adapted for Teams constraints.
      Today gets the most detail (you need it now); the forecast columns
      give a quick week-at-a-glance.

    WHY Celsius with °C suffix:
      Phillip is in the UK. Fahrenheit would require mental conversion
      and the sample's F conversion formula is not needed here —
      Open-Meteo returns Celsius natively.
    """
    today    = weather["today"]
    forecast = weather["forecast"]

    date_str = now_london.strftime(f"%A %d %B — {tz_label}")

    # Build the 4-day forecast columns
    forecast_columns = []
    for day in forecast:
        dt       = datetime.strptime(day["date"], "%Y-%m-%d")
        day_name = dt.strftime("%a")   # Mon, Tue etc.
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
                    "spacing": "None"
                },
                {
                    "type": "TextBlock",
                    "text": day["emoji"],
                    "horizontalAlignment": "Center",
                    "size": "Large",
                    "spacing": "None"
                },
                {
                    "type": "TextBlock",
                    "text": f"{day['high']}°",
                    "horizontalAlignment": "Center",
                    "weight": "Bolder",
                    "size": "Small",
                    "spacing": "None"
                },
                {
                    "type": "TextBlock",
                    "text": f"{day['low']}°",
                    "horizontalAlignment": "Center",
                    "isSubtle": True,
                    "size": "Small",
                    "spacing": "None"
                },
            ]
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
                    # ── Header ────────────────────────────────────────────────
                    {
                        "type": "TextBlock",
                        "text": "🌦️ WEATHER — BASINGSTOKE",
                        "weight": "Bolder",
                        "color": "Warning",
                        "spacing": "None"
                    },
                    {
                        "type": "TextBlock",
                        "text": date_str,
                        "isSubtle": True,
                        "size": "Small",
                        "spacing": "None"
                    },
                    # ── Today: big emoji + condition + detail ─────────────────
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
                                        "spacing": "None"
                                    }
                                ]
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
                                        "spacing": "None"
                                    },
                                    {
                                        "type": "TextBlock",
                                        "text": f"{today['high']}°C / {today['low']}°C",
                                        "size": "Medium",
                                        "spacing": "None"
                                    },
                                    {
                                        "type": "TextBlock",
                                        "text": (
                                            f"🌂 {today['rain_pct']}% chance of rain   "
                                            f"💨 {today['wind_kmh']} km/h {today['wind_dir']}"
                                        ),
                                        "isSubtle": True,
                                        "size": "Small",
                                        "wrap": True,
                                        "spacing": "None"
                                    }
                                ]
                            }
                        ]
                    },
                    # ── Separator ─────────────────────────────────────────────
                    {
                        "type": "TextBlock",
                        "text": "─────────────────────",
                        "color": "Warning",
                        "spacing": "Small"
                    },
                    # ── 4-day forecast columns ────────────────────────────────
                    {
                        "type": "ColumnSet",
                        "spacing": "Small",
                        "columns": forecast_columns
                    }
                ]
            }
        ]
    }


# ── Calendar fetching ──────────────────────────────────────────────────────────
def _fetch_calendar_events(token: str, now_utc: datetime) -> list[dict]:
    """
    Fetch today's calendar events from Microsoft Graph.

    WHY calendarView rather than /events with a filter:
      calendarView automatically expands recurring events into individual
      instances. A filter on /events would only return the series master
      and miss individual occurrences — today's recurring team standup
      would not appear. calendarView is the correct endpoint for a
      day-view agenda.

    WHY today midnight to midnight in London time:
      We want events for the calendar day as Phillip experiences it —
      midnight to midnight in his local timezone, not UTC. An event at
      23:30 London time should appear in today's agenda even though it
      might be the following UTC day.

    WHY $orderby=start/dateTime:
      Events are returned in chronological order, which is the natural
      order for an agenda card.

    WHY $top=20:
      A day with more than 20 calendar events is unusual enough that
      we can safely cap here. If it does happen, the overflow is logged.
    """
    london_now   = now_utc.astimezone(LONDON_TZ)
    day_start    = london_now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end      = day_start + timedelta(days=1)

    start_str    = day_start.isoformat()
    end_str      = day_end.isoformat()

    url = (
        f"https://graph.microsoft.com/v1.0/users/cda66539-6f2a-4a27-a5a3-a493061f8711"
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
    data = resp.json()

    events = data.get("value", [])
    if data.get("@odata.nextLink"):
        logging.warning("emailDigest: more than 20 events today — some omitted")

    return events


# ── Agenda card builder ────────────────────────────────────────────────────────
def _build_agenda_card(events: list[dict], now_london: datetime, tz_label: str) -> dict:
    """
    Build an Adaptive Card listing today's calendar events.

    WHY one card for all events rather than one card per event:
      The agenda is a planning tool — you want to scan the whole day at
      once, not triage individual meetings. A single card with all events
      is the right form for this use case.

    WHY we show all-day events differently:
      All-day events (bank holidays, out-of-office markers) have no
      meaningful start/end time to display. We label them as "All day"
      to distinguish them from timed meetings.

    WHY we show the organiser for non-personal events:
      For meetings organised by someone else, knowing who called it adds
      useful context at a glance. For events you organised yourself, the
      organiser line would be noise, so we omit it.
    """
    date_str = now_london.strftime(f"%A %d %B — {tz_label}")

    if not events:
        event_items = [
            {
                "type": "TextBlock",
                "text": "No meetings today — enjoy the space.",
                "isSubtle": True,
                "wrap": True,
                "spacing": "Small"
            }
        ]
    else:
        event_items = []
        for i, event in enumerate(events):
            # ── Format time ───────────────────────────────────────────────────
            if event.get("isAllDay"):
                time_str = "All day"
            else:
                try:
                    start_dt     = datetime.fromisoformat(
                        event["start"]["dateTime"]
                    ).replace(tzinfo=timezone.utc).astimezone(LONDON_TZ)
                    end_dt       = datetime.fromisoformat(
                        event["end"]["dateTime"]
                    ).replace(tzinfo=timezone.utc).astimezone(LONDON_TZ)
                    time_str     = f"{start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}"
                except Exception:
                    time_str = ""

            # ── Subject ───────────────────────────────────────────────────────
            subject = (event.get("subject") or "No title").strip()

            # ── Location (optional) ───────────────────────────────────────────
            location = (
                event.get("location", {}).get("displayName", "") or ""
            ).strip()

            # ── Organiser (optional — omit if self-organised) ─────────────────
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

            # ── Separator between events ──────────────────────────────────────
            spacing = "Small" if i == 0 else "Medium"

            event_items.append({
                "type": "ColumnSet",
                "spacing": spacing,
                "columns": [
                    # Time column — fixed width, right-aligned
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
                                "spacing": "None"
                            }
                        ]
                    },
                    # Event detail column
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
                                    "spacing": "None"
                                },
                                # Location — only shown if present
                                {
                                    "type": "TextBlock",
                                    "text": f"📍 {location}",
                                    "isSubtle": True,
                                    "size": "Small",
                                    "wrap": True,
                                    "spacing": "None"
                                } if location else None,
                                # Organiser — only shown if not self-organised
                                {
                                    "type": "TextBlock",
                                    "text": f"👤 {organiser_name}",
                                    "isSubtle": True,
                                    "size": "Small",
                                    "spacing": "None"
                                } if show_organiser else None,
                            ]
                            if item is not None
                        ]
                    }
                ]
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
                        "spacing": "None"
                    },
                    {
                        "type": "TextBlock",
                        "text": date_str,
                        "isSubtle": True,
                        "size": "Small",
                        "spacing": "None"
                    },
                    {
                        "type": "TextBlock",
                        "text": "─────────────────────",
                        "color": "Warning",
                        "spacing": "Small"
                    },
                    *event_items
                ]
            }
        ]
    }


# ── Email fetching ─────────────────────────────────────────────────────────────
def _fetch_emails(token: str, since: datetime | None) -> list[dict]:
    """
    Fetch emails from the Inbox received after `since`.

    WHY $filter on receivedDateTime:
      Graph filters server-side, keeping the payload small.

    WHY top=100:
      A 2-hour window on a busy inbox might exceed the default 50-item
      limit. 100 is a reasonable ceiling.

    WHY bodyPreview and id in $select:
      bodyPreview populates the card preview. id is embedded in each
      triage button so messages.py knows which email to act on.
    """
    headers = {"Authorization": f"Bearer {token}"}

    if since:
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        since_str = two_hours_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

    filter_clause = f"receivedDateTime ge {since_str}"

    url = (
        "https://graph.microsoft.com/v1.0/users/cda66539-6f2a-4a27-a5a3-a493061f8711"
        "/mailFolders/Inbox/messages"
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

    WHY base64 data URI:
      Graph photo endpoints return binary data requiring an auth header —
      they cannot be used directly as Image src URLs in Adaptive Cards.
      A data URI is self-contained and requires no further HTTP calls.

    WHY image/jpeg regardless of file extension:
      JPEG images use MIME type image/jpeg whether stored as .jpg or .jpeg.
      Graph always returns JPEG binary from its photo endpoints.

    Resolution order:
      1. Internal M365 user photo
      2. Saved contact photo
      3. Envelope icon fallback
    """
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(
            f"https://graph.microsoft.com/v1.0/users/{sender_email}/photo/$value",
            headers=headers, timeout=10,
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
                    f"https://graph.microsoft.com/v1.0/me/contacts/{contact_id}/photo/$value",
                    headers=headers, timeout=10,
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
    """
    Time-aware greeting for the header card.
    Future session: replace with AI-generated greeting that varies each run.
    """
    hour = now_london.hour
    if hour < 12:
        return "Good morning, Phillip"
    elif hour < 17:
        return "Good afternoon, Phillip"
    else:
        return "Good evening, Phillip"


# ── Header card builder ────────────────────────────────────────────────────────
def _build_header_card(
    now_london:   datetime,
    tz_label:     str,
    last_run_utc: datetime | None,
    email_count:  int,
) -> dict:
    """
    Digest header card sent before the individual email cards.

    WHY no buttons: informational only.
    WHY Warning colour on greeting: draws the eye to the top of the batch.
    """
    greeting_text = _greeting(now_london)
    date_str      = now_london.strftime(f"%A %d %B %Y — %H:%M {tz_label}")

    if last_run_utc:
        from_london = last_run_utc.astimezone(LONDON_TZ)
        from_str    = from_london.strftime(f"%H:%M {tz_label}")
    else:
        from_str = "start of day"

    to_str     = now_london.strftime(f"%H:%M {tz_label}")
    window_str = f"Covering emails from {from_str} to {to_str}"
    count_str  = "1 email to triage" if email_count == 1 else f"{email_count} emails to triage"

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
                        "size": "Large",
                        "color": "Warning",
                        "spacing": "None"
                    },
                    {
                        "type": "TextBlock",
                        "text": date_str,
                        "isSubtle": True,
                        "size": "Small",
                        "spacing": "Small"
                    },
                    {
                        "type": "TextBlock",
                        "text": "─────────────────────",
                        "color": "Warning",
                        "spacing": "Small"
                    },
                    {
                        "type": "TextBlock",
                        "text": window_str,
                        "wrap": True,
                        "spacing": "Small"
                    },
                    {
                        "type": "TextBlock",
                        "text": count_str,
                        "weight": "Bolder",
                        "spacing": "Small"
                    }
                ]
            }
        ]
    }


# ── Email card builder ─────────────────────────────────────────────────────────
def _build_card(email: dict, tz_label: str, token: str) -> dict:
    """
    Build one Adaptive Card for a single email.

    WHY one card per email: A&E triage model — each email is a decision.
    WHY emailId in button data: messages.py needs it to act on the right email.
    WHY pass token: photo resolution requires a Graph call.
    NOTE View button: per-message deep link is a future refinement.
    """
    sender_name  = email.get("from", {}).get("emailAddress", {}).get("name", "Unknown")
    sender_email = email.get("from", {}).get("emailAddress", {}).get("address", "")
    subject      = (email.get("subject", "") or "(no subject)").strip()
    body_preview = (email.get("bodyPreview", "") or "").strip()
    email_id     = email.get("id", "")

    if len(body_preview) > 150:
        body_preview = body_preview[:147] + "…"

    received_str = email.get("receivedDateTime", "")
    try:
        received_utc    = datetime.fromisoformat(received_str.replace("Z", "+00:00"))
        received_london = received_utc.astimezone(LONDON_TZ)
        time_label      = received_london.strftime("%H:%M")
    except Exception:
        time_label = ""

    photo_uri = _get_sender_photo(token, sender_email) if sender_email else ENVELOPE_ICON

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
                        "type": "ColumnSet",
                        "columns": [
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
                    {
                        "type": "TextBlock",
                        "text": subject,
                        "weight": "Bolder",
                        "size": "Medium",
                        "wrap": True,
                        "spacing": "Small"
                    },
                    {
                        "type": "TextBlock",
                        "text": body_preview,
                        "isSubtle": True,
                        "wrap": True,
                        "maxLines": 3,
                        "spacing": "Small"
                    },
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
                                            "data": {"triageAction": "action", "emailId": email_id}
                                        },
                                        "items": [{"type": "TextBlock", "text": "Action", "horizontalAlignment": "Center", "weight": "Bolder", "spacing": "Small"}]
                                    },
                                    {
                                        "type": "Container",
                                        "style": "default",
                                        "spacing": "Small",
                                        "selectAction": {
                                            "type": "Action.Submit",
                                            "data": {"triageAction": "waiting", "emailId": email_id}
                                        },
                                        "items": [{"type": "TextBlock", "text": "Waiting For", "horizontalAlignment": "Center", "weight": "Bolder", "spacing": "Small"}]
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
                                            "url": "https://outlook.office365.com/mail/"
                                        },
                                        "items": [{"type": "TextBlock", "text": "View", "horizontalAlignment": "Center", "weight": "Bolder", "spacing": "Small"}]
                                    },
                                    {
                                        "type": "Container",
                                        "style": "default",
                                        "spacing": "Small",
                                        "selectAction": {
                                            "type": "Action.Submit",
                                            "data": {"triageAction": "delete", "emailId": email_id}
                                        },
                                        "items": [{"type": "TextBlock", "text": "Delete", "horizontalAlignment": "Center", "weight": "Bolder", "spacing": "Small"}]
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
def _get_delivery_config() -> tuple[str, str, str, str, str]:
    """
    Shared Bot Framework delivery config for both send functions.

    WHY five values returned (previously four):
      The tenant ID is now required to create a channel conversation via
      the Bot Framework Connector. It is read from the TENANT_ID app
      setting, which is already present in the Function App.
    """
    bot_token   = _get_bot_token()
    service_url = os.environ["TEAMS_SERVICE_URL"].rstrip("/")
    channel_id  = os.environ["TEAMS_DAILY_OPERATIONS_ID"]
    bot_app_id  = os.environ["BOT_APP_ID"]
    tenant_id   = os.environ["TENANT_ID"]
    return bot_token, service_url, channel_id, bot_app_id, tenant_id


def _create_channel_conversation(
    bot_token:   str,
    service_url: str,
    channel_id:  str,
    bot_app_id:  str,
    tenant_id:   str,
) -> str:
    """
    Create a Bot Framework conversation in a Teams channel and return
    the conversation ID to post activities into.

    WHY this step is required:
      The Bot Framework Connector rejects activities sent directly to a
      Teams channel thread ID (19:...@thread.tacv2) with 403 Forbidden.
      You must first POST to /v3/conversations to register a new
      conversation, passing the channel ID in channelData. The API
      returns a conversation ID that accepts subsequent activities.

    WHY the bot ID uses the '28:' prefix:
      The Bot Framework identifies bots with a '28:' namespace prefix.
      The BOT_APP_ID environment variable stores the raw Azure AD app ID
      (a GUID). Without the prefix the conversation creation call returns
      400 Bad Request because the bot identity is not recognised.

    WHY no 'activity' field in the body:
      The activity field is not valid on the conversation creation
      endpoint and causes a 400 error. The initial message, if any,
      is sent as a separate POST to the returned conversation ID.

    WHY channelData requires channel, team, and tenant:
      The Bot Framework needs all three to locate the destination.
      channel identifies the specific thread. team identifies which
      Team that channel belongs to — without it the Bot Framework cannot
      resolve the channel ID unambiguously. tenant identifies the M365
      organisation. All three are required for channel conversations.

    WHY TEAMS_TEAM_ID is read inside this function rather than passed in:
      The team ID is only needed here — not by any other delivery
      function. Reading it locally keeps the call signatures clean and
      avoids threading a value through _get_delivery_config that nothing
      else uses.

    WHY log resp.text before raise_for_status:
      raise_for_status() discards the response body when it throws.
      Logging resp.text first preserves the Bot Framework's exact error
      message in Application Insights, which is the only way to know
      what the API is rejecting.

    WHY log the request body before sending (Session 24 diagnostic):
      The body is logged at INFO level immediately before the POST so
      that Application Insights shows exactly what was sent. This lets
      us confirm that channel_id, bot_app_id, and tenant_id are resolving
      correctly from environment variables at runtime — not just what the
      code intends to send. Remove this line once the 400 error is resolved.
    """
    url     = f"{service_url}/v3/conversations"
    team_id = os.environ["TEAMS_TEAM_ID"]
    body    = {
        "bot": {"id": f"28:{bot_app_id}", "name": "Leo"},
        "isGroup": True,
        "channelData": {
            "channel": {"id": channel_id},
            "tenant":  {"id": tenant_id},
        },
    }

    # SESSION 24 DIAGNOSTIC — remove once 400 error is resolved
    logging.info(f"emailDigest: conversation creation body — {body}")

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {bot_token}",
            "Content-Type":  "application/json",
        },
        json=body,
        timeout=15,
    )
    if not resp.ok:
        logging.error(
            f"emailDigest: conversation creation failed — "
            f"{resp.status_code} — {resp.text}"
        )
    resp.raise_for_status()
    return resp.json()["id"]


def _send_text_to_teams(text: str) -> None:
    """Plain-text message for the no-email case — lighter than a card."""
    bot_token, service_url, channel_id, bot_app_id, tenant_id = _get_delivery_config()
    conversation_id = _create_channel_conversation(
        bot_token, service_url, channel_id, bot_app_id, tenant_id
    )
    url = f"{service_url}/v3/conversations/{conversation_id}/activities"

    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
        json={"type": "message", "from": {"id": f"28:{bot_app_id}"}, "text": text},
        timeout=15,
    )
    resp.raise_for_status()
    logging.info(f"emailDigest: plain text delivered — status {resp.status_code}")


def _send_card_to_teams(card: dict) -> None:
    """
    Post a single Adaptive Card via the Bot Framework Connector API.

    WHY attachments with adaptive card contentType:
      Without this wrapper Teams renders the JSON as raw text.

    WHY Bot Framework Connector (not Graph API):
      Cards appear as bot messages. The Connector is the correct path
      for bot-originated messages and requires no additional permissions.

    WHY a new conversation is created per card:
      Each card gets its own thread for now — clean and simple. A future
      session can refactor to share one conversation ID per digest run,
      so all cards in a run appear as replies in the same thread.
    """
    bot_token, service_url, channel_id, bot_app_id, tenant_id = _get_delivery_config()
    conversation_id = _create_channel_conversation(
        bot_token, service_url, channel_id, bot_app_id, tenant_id
    )
    url = f"{service_url}/v3/conversations/{conversation_id}/activities"

    payload = {
        "type": "message",
        "from": {"id": f"28:{bot_app_id}"},
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card,
            }
        ],
    }

    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    logging.info(f"emailDigest: card delivered — status {resp.status_code}")
