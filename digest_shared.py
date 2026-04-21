"""
digest_shared.py — Shared helpers for Monica Email Digest

WHY this module exists:
  The email digest was previously a single file with one timer trigger
  covering all eight daily slots (05:00–19:00) via runtime conditional
  logic. Refactored in Session 33 into eight independent slot files —
  one per time slot, each with its own cron expression and timer trigger.
  All shared constants, helper functions, and card builders live here.
  A failure in one slot file cannot affect another. Each slot is
  independently deployable and testable via the portal's Run Now button.

WHY self-contained get_access_token:
  Each Blueprint file that needs a Graph token imports get_access_token()
  from here. If the token call fails, only the calling slot errors —
  everything else keeps running.

Session 33 changes from the original email_digest.py:
  - Sunday 05:00 suppression removed. Sunday now starts at 05:00 like
    every other day. The is_first_slot / weekday conditional is gone.
  - FTSE 250 (^FTMC) added as a fifth market index. The FTSE 100 tracks
    large-cap international exposure; the FTSE 250 tracks mid-cap
    domestic UK health. Divergence between them is analytically
    significant and worth Leo noting.
  - Comparative market context added: 4-week and 52-week percentage
    changes shown beneath each index row. yfinance history(period='1y')
    provides the full year of data needed. iloc[-21] approximates four
    trading weeks; iloc[0] approximates one year.
  - _greeting() simplified: no longer takes is_first_slot parameter.
    The 05:00 slot file never calls _greeting() — the morning card has
    its own hardcoded greeting. The 07:00–19:00 files call _greeting(hour)
    directly.
  - _fetch_market_data() period changed from '2d' to '1y' to support
    comparative context. The additional data volume is handled efficiently
    by yfinance and does not affect runtime meaningfully.
"""

import os
import logging
import random
import base64
import requests
import yfinance as yf
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from azure.storage.blob import BlobServiceClient

# ── Constants ────────────────────────────────────────────────────────────────

LONDON_TZ      = ZoneInfo("Europe/London")
BLOB_CONTAINER = "monica-digest"
BLOB_NAME      = "last_run.txt"
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
        logging.error("digest_shared: Managed Identity environment variables not set.")
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
        logging.error(f"digest_shared: token acquisition failed: {e}")
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
      Free with no API key required. No Key Vault secret needed. Excellent
      UK coverage at Monica's usage volume.
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

# ── Market data fetching ───────────────────────────────────────────────────────

def _fetch_market_data(is_evening: bool = False) -> dict | None:
    """
    Fetch market index closing data using yfinance, including comparative context.
    WHY yfinance:
      No API key required. Covers all five required indices with a
      consistent call pattern.
    WHY period='1y':
      Returns a full year of trading sessions. iloc[-1] is the most recent
      close; iloc[-2] is the prior session for the day-over-day change;
      iloc[-21] approximates four trading weeks ago; iloc[0] approximates
      52 weeks ago. Period '2d' was sufficient when only the day change was
      shown — '1y' is required now that comparative context is included.
    WHY FTSE 250 alongside FTSE 100:
      The FTSE 100 tracks large-cap internationally exposed companies —
      it tells you what global markets are doing as much as the UK.
      The FTSE 250 is predominantly domestic mid-cap — a better proxy for
      the health of the UK economy itself. When the two diverge, the
      divergence is the story. Leo can name it when it is worth naming.
    WHY is_evening:
      At 19:00 BST, FTSE has closed (16:30 BST) but US markets are still
      live. Showing partial US figures would be misleading. When
      is_evening=True, only FTSE 100 is fetched for the close line.
    WHY float() conversion:
      yfinance returns numpy float64 values. Converting to Python float
      avoids serialisation issues if the dict is ever logged as JSON.
    """
    symbols = (
        {"ftse": "^FTSE"}
        if is_evening
        else {
            "ftse":    "^FTSE",
            "ftse250": "^FTMC",
            "sp500":   "^GSPC",
            "dow":     "^DJI",
            "nasdaq":  "^IXIC",
        }
    )
    results = {}
    for key, symbol in symbols.items():
        try:
            hist = yf.Ticker(symbol).history(period="1y")
            if hist.empty:
                logging.warning(f"digest_shared: no history returned for {symbol}")
                continue

            close = float(hist["Close"].iloc[-1])

            # Day-over-day change
            if len(hist) >= 2:
                prev_close = float(hist["Close"].iloc[-2])
                change     = close - prev_close
                change_pct = (change / prev_close) * 100
            else:
                change     = 0.0
                change_pct = 0.0

            # Four-week comparative (~20 trading days)
            four_week_pct = None
            if len(hist) >= 21:
                four_week_close = float(hist["Close"].iloc[-21])
                if four_week_close:
                    four_week_pct = ((close - four_week_close) / four_week_close) * 100

            # 52-week comparative (oldest available data in 1y window)
            year_pct = None
            if len(hist) >= 50:  # Guard against thin data sets
                year_close = float(hist["Close"].iloc[0])
                if year_close:
                    year_pct = ((close - year_close) / year_close) * 100

            results[key] = {
                "close":         close,
                "change":        change,
                "change_pct":    change_pct,
                "four_week_pct": four_week_pct,
                "year_pct":      year_pct,
            }
        except Exception as e:
            logging.warning(f"digest_shared: market fetch failed for {symbol} — {e}")
            continue

    return results if results else None


def _market_voice_line(market_data: dict, is_monday: bool) -> str:
    """
    Return a single Leo-voice sentence interpreting market direction.
    WHY a sentence rather than numbers:
      Leo briefs direction first. The numbers sit below as supporting
      detail. Reading out a ticker is not Leo's register.
    WHY is_monday:
      On Monday morning the most recent session was Friday. 'Overnight'
      would be inaccurate. 'On Friday' is precise.
    WHY S&P 500 as the US proxy:
      The S&P is the broadest US index and the one most commonly
      referenced in a leadership context.
    WHY FTSE 100 as the UK proxy, not FTSE 250:
      The voice line is directional shorthand. The FTSE 100 is the
      recognised headline index. The 250's comparative story is told
      in the data rows below.
    """
    time_ref = "on Friday" if is_monday else "overnight"
    ftse  = market_data.get("ftse")
    sp500 = market_data.get("sp500")

    if not ftse and not sp500:
        return ""

    if ftse and sp500:
        ftse_up = ftse["change_pct"] >= 0
        sp_up   = sp500["change_pct"] >= 0
        if ftse_up and sp_up:
            return f"Both markets closed higher {time_ref}, Sir."
        elif not ftse_up and not sp_up:
            return f"Both markets closed lower {time_ref}."
        elif sp_up and not ftse_up:
            return f"Wall Street closed higher {time_ref}. London finished down."
        else:
            return f"Wall Street finished lower {time_ref}. London closed in the green."

    if ftse:
        direction = "higher" if ftse["change_pct"] >= 0 else "lower"
        return f"London closed {direction} {time_ref}."

    return ""


def _build_market_items(market_data: dict, voice_line: str) -> list[dict]:
    """
    Build Adaptive Card body items for the markets section.
    WHY voice_line first, then numbers:
      Leo interprets direction in one sentence; the index rows are the
      evidence beneath it. Conclusion first, supporting detail below.
    WHY ▲/▼ with Good/Attention colour:
      Direction is immediately legible without arithmetic. Green/red is
      the universal market convention.
    WHY comparative context shown as subtitle beneath each index:
      4W and 1Y percentages give the number a frame of reference without
      adding a separate card or section. A number in isolation tells you
      where something is; the comparatives tell you whether that matters.
    WHY thousands separator, no decimals for large indices:
      FTSE and Dow trade above 1,000 — decimal places add noise without
      value at a briefing level.
    """
    items: list[dict] = []

    if voice_line:
        items.append({
            "type": "TextBlock",
            "text": voice_line,
            "weight": "Bolder",
            "size": "Medium",
            "wrap": True,
            "spacing": "None",
        })

    index_labels = {
        "ftse":    "FTSE 100",
        "ftse250": "FTSE 250",
        "sp500":   "S&P 500",
        "dow":     "Dow Jones",
        "nasdaq":  "NASDAQ",
    }

    for key, label in index_labels.items():
        entry = market_data.get(key)
        if not entry:
            continue

        close      = entry["close"]
        change_pct = entry["change_pct"]
        up         = change_pct >= 0
        arrow      = "▲" if up else "▼"
        colour     = "Good" if up else "Attention"
        close_str  = f"{close:,.0f}" if close >= 1000 else f"{close:,.2f}"
        change_str = f"{arrow} {abs(change_pct):.2f}%"

        # Comparative context subtitle
        comp_parts = []
        if entry.get("four_week_pct") is not None:
            fw = entry["four_week_pct"]
            fw_arrow = "▲" if fw >= 0 else "▼"
            comp_parts.append(f"4W {fw_arrow}{abs(fw):.1f}%")
        if entry.get("year_pct") is not None:
            yr = entry["year_pct"]
            yr_arrow = "▲" if yr >= 0 else "▼"
            comp_parts.append(f"1Y {yr_arrow}{abs(yr):.1f}%")
        comp_line = "  |  ".join(comp_parts) if comp_parts else None

        # Label column items: index name + optional comparative line
        label_items: list[dict] = [
            {
                "type": "TextBlock",
                "text": label,
                "size": "Small",
                "spacing": "None",
            }
        ]
        if comp_line:
            label_items.append({
                "type": "TextBlock",
                "text": comp_line,
                "size": "Small",
                "isSubtle": True,
                "spacing": "None",
            })

        items.append({
            "type": "ColumnSet",
            "spacing": "Small",
            "columns": [
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": label_items,
                },
                {
                    "type": "Column",
                    "width": "auto",
                    "verticalContentAlignment": "Center",
                    "items": [{
                        "type": "TextBlock",
                        "text": close_str,
                        "size": "Small",
                        "weight": "Bolder",
                        "horizontalAlignment": "Right",
                        "spacing": "None",
                    }],
                },
                {
                    "type": "Column",
                    "width": "auto",
                    "verticalContentAlignment": "Center",
                    "items": [{
                        "type": "TextBlock",
                        "text": change_str,
                        "size": "Small",
                        "color": colour,
                        "horizontalAlignment": "Right",
                        "spacing": "None",
                    }],
                },
            ],
        })

    return items


def _evening_ftse_line(ftse: dict) -> str:
    """
    Build the single FTSE 100 close line for the 19:00 card header.
    WHY direction word rather than just a number:
      Leo's voice leads with interpretation. 'London closed higher today'
      is the briefing; the number is the supporting detail. Both appear
      in a single line so the header stays compact.
    """
    direction  = "higher" if ftse["change_pct"] >= 0 else "lower"
    arrow      = "▲" if ftse["change_pct"] >= 0 else "▼"
    close_str  = f"{ftse['close']:,.0f}"
    change_str = f"{arrow} {abs(ftse['change_pct']):.2f}%"
    return f"London closed {direction} today.  FTSE {close_str}  {change_str}"

# ── Calendar fetching ──────────────────────────────────────────────────────────

def _fetch_calendar_events(
    token: str, now_utc: datetime, day_offset: int = 0
) -> list[dict]:
    """
    Fetch calendar events for a given day from Microsoft Graph.
    WHY day_offset:
      day_offset=0 returns today's events (default).
      day_offset=1 returns tomorrow's — used by the 19:00 closing card.
    WHY calendarView rather than /events with a filter:
      calendarView automatically expands recurring events into individual
      instances. A filter on /events returns only the series master.
    WHY filter yesterday's all-day events after fetching:
      Graph's calendarView startDateTime is inclusive. An all-day event
      from the previous day ends exactly at the query start — so Graph
      returns it. We filter in Python: if the event is all-day and its
      end date matches day_start, it belongs to the day before.
    """
    london_now = now_utc.astimezone(LONDON_TZ)
    day_start  = (
        london_now.replace(hour=0, minute=0, second=0, microsecond=0)
        + timedelta(days=day_offset)
    )
    day_end   = day_start + timedelta(days=1)
    start_str = day_start.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = day_end.strftime("%Y-%m-%dT%H:%M:%S")

    url = (
        "https://graph.microsoft.com/v1.0/users/"
        "cda66539-6f2a-4a27-a5a3-a493061f8711"
        "/calendarView"
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
        logging.warning("digest_shared: more than 20 events — some omitted")

    today_date_str = day_start.strftime("%Y-%m-%d")
    filtered = []
    for event in events:
        if event.get("isAllDay"):
            end_dt_str = event.get("end", {}).get("dateTime", "")
            if end_dt_str.startswith(today_date_str):
                logging.info(
                    f"digest_shared: skipping previous day all-day event "
                    f"'{event.get('subject', '')}'"
                )
                continue
        filtered.append(event)

    return filtered

# ── Event items helper ─────────────────────────────────────────────────────────

def _build_event_items(events: list[dict]) -> list[dict]:
    """
    Build Adaptive Card body items for a list of calendar events.
    WHY extracted as a helper:
      Both _build_morning_briefing_card and _build_agenda_card render
      calendar events. One source of truth — a change applies everywhere.
    """
    if not events:
        return [{
            "type": "TextBlock",
            "text": "No meetings today — enjoy the space.",
            "isSubtle": True,
            "wrap": True,
            "spacing": "Small",
        }]

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
                    "items": [{
                        "type": "TextBlock",
                        "text": time_str,
                        "weight": "Bolder",
                        "size": "Small",
                        "color": "Warning",
                        "horizontalAlignment": "Right",
                        "spacing": "None",
                    }],
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
      label changes the card header (TODAY vs TOMORROW).
      intro adds an opening line before the event list — used for the
      19:00 tomorrow card.
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
        "body": [{
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "items": container_items,
        }],
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
        logging.warning("digest_shared: more than 100 emails in window — some omitted")

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
        logging.debug(f"digest_shared: internal photo lookup failed — {e}")

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
        logging.debug(f"digest_shared: contact photo lookup failed — {e}")

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
        logging.error(f"digest_shared: failed to write last_run blob — {e}")

# ── Time formatting helper ─────────────────────────────────────────────────────

def _fmt_time(dt: datetime, tz_label: str) -> str:
    local = dt.astimezone(LONDON_TZ)
    return local.strftime(f"%H:%M {tz_label} on %a %d %b")

# ── Leo voice helpers ──────────────────────────────────────────────────────────

def _greeting(hour: int) -> str:
    """
    Return the correct Leo-voice greeting for the given hour.
    WHY simplified from original:
      Previously took an is_first_slot parameter to handle Sunday 07:00
      being treated as the first slot. Sunday now starts at 05:00 like
      all other days (Session 33 decision). The 05:00 slot file never
      calls _greeting() — the morning briefing card has its own hardcoded
      greeting. This function is called only by the 07:00–19:00 slot files.
    WHY alternating Phillip and Sir:
      Reflects the register variation in Leo's voice profile. Neither form
      of address becomes monotonous across the day.
    """
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
      Short declaratives — 'Four emails to work through.' — match the
      character profile. Leo does not pad.
    WHY number words up to twelve, then digits:
      Written numbers read more naturally in a conversational context.
      Above twelve, digits are clearer.
    WHY random choice for zero:
      Two equivalent phrasings for an empty inbox avoid the same line
      appearing every quiet morning.
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
    market_data: dict | None = None,
    is_monday: bool = False,
) -> dict:
    """
    Build the unified first-slot card: greeting + markets + weather + agenda.
    WHY markets before weather:
      Natural CoS briefing order: overnight context (markets), today's
      conditions (weather), today's commitments (agenda). Leo gives Phillip
      what happened while he slept before telling him what the day will feel like.
    WHY market_data is conditional:
      If yfinance fails, the card renders without the markets section.
      A morning without market data is better than no morning card.
    WHY greeting is two TextBlocks:
      'Good morning, Sir.' is the address — large and immediate.
      'Here's the day.' is the handover — one size smaller. Each phrase
      has its own visual weight without requiring a separate card.
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

    # ── Markets section ───────────────────────────────────────────────────────
    if market_data:
        voice_line   = _market_voice_line(market_data, is_monday)
        market_items = _build_market_items(market_data, voice_line)
        if market_items:
            items.append({
                "type": "TextBlock",
                "text": "─────────────────────",
                "color": "Warning",
                "spacing": "Medium",
            })
            items.append({
                "type": "TextBlock",
                "text": "📈 MARKETS",
                "weight": "Bolder",
                "color": "Warning",
                "spacing": "None",
            })
            items.extend(market_items)

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
                    "items": [{
                        "type": "TextBlock",
                        "text": today["emoji"],
                        "size": "ExtraLarge",
                        "spacing": "None",
                    }],
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
        "body": [{
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "items": items,
        }],
    }

# ── Clear inbox card builder ───────────────────────────────────────────────────

def _build_clear_inbox_card(
    greeting: str | None,
    count_line: str,
    market_line: str | None = None,
) -> dict:
    """
    Build a minimal card for slots where the inbox is empty.
    WHY send a card rather than nothing:
      Leo checks in at every slot. A card confirms the digest ran and
      the inbox is genuinely empty. Silence is ambiguous.
    WHY greeting is optional:
      At 05:00, the greeting is already in the morning briefing card.
      Passing greeting=None omits it here to avoid a second
      'Good morning, Sir.' in the channel.
    WHY market_line is optional:
      Only the 19:00 slot passes the FTSE close line.
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

    if market_line:
        items.append({
            "type": "TextBlock",
            "text": market_line,
            "size": "Small",
            "isSubtle": True,
            "wrap": True,
            "spacing": "Small" if greeting else "None",
        })

    items.append({
        "type": "TextBlock",
        "text": count_line,
        "size": "Medium",
        "wrap": True,
        "spacing": "Small" if (greeting or market_line) else "None",
    })

    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [{
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "items": items,
        }],
    }

# ── Goodnight card builder ─────────────────────────────────────────────────────

def _build_goodnight_card() -> dict:
    """
    Build the end-of-day closing card sent at 19:00.
    WHY a dedicated card:
      Leo closes the day as he would leave the Oval Office — 'Good night,
      Sir.' It is the last thing in the channel each day.
    WHY Large rather than ExtraLarge:
      The goodnight card closes the day — settled and calm rather than loud.
    """
    return {
        "type": "AdaptiveCard",
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [{
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "items": [{
                "type": "TextBlock",
                "text": "Good night, Sir.",
                "weight": "Bolder",
                "size": "Large",
                "color": "Warning",
                "spacing": "None",
            }],
        }],
    }

# ── Concertina email card builder ──────────────────────────────────────────────

def _build_concertina_card(
    emails: list[dict],
    tz_label: str,
    token: str,
    now_london: datetime,
    greeting: str | None,
    count_line: str,
    market_line: str | None = None,
) -> dict:
    """
    Build a single Adaptive Card containing all emails as collapsible rows.
    WHY concertina rather than one card per email:
      A single card with all emails collapsed means the full inbox is
      visible at a glance. Phillip expands only the email he is triaging.
    WHY greeting and count_line parameters:
      At 05:00, greeting=None — the greeting is already in the morning card.
      At all other slots, the greeting appears at the top of the header.
      count_line always sits immediately above the emails it describes.
    WHY market_line is optional:
      Only the 19:00 slot passes the FTSE close.
    WHY two ActionSet blocks rather than one:
      A single ActionSet with four buttons wraps as 3+1 on mobile.
      Splitting into two blocks of two forces a consistent 2+2 layout.
    WHY Action.ToggleVisibility:
      Native Adaptive Card mechanism for show/hide without a backend
      round-trip. The toggle fires client-side in Teams — instant.
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

    if market_line:
        header_items.append({
            "type": "TextBlock",
            "text": market_line,
            "size": "Small",
            "isSubtle": True,
            "wrap": True,
            "spacing": "Small" if greeting else "None",
        })

    header_items.append({
        "type": "TextBlock",
        "text": count_line,
        "size": "Medium",
        "wrap": True,
        "spacing": "Small" if (greeting or market_line) else "None",
    })

    if not greeting:
        # WHY EMAIL TRIAGE label only when there is no greeting:
        #   When there is no greeting (05:00, greeting already delivered),
        #   a label anchors the card.
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

    body: list[dict] = [{
        "type": "Container",
        "style": "emphasis",
        "bleed": True,
        "items": header_items,
    }]

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
        body.append({
            "type": "Container",
            "spacing": "Small",
            "selectAction": {
                "type": "Action.ToggleVisibility",
                "targetElements": [detail_id],
            },
            "items": [{
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
            }],
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
                            "items": [{
                                "type": "Image",
                                "url": photo_uri,
                                "size": "Small",
                                "style": "Person",
                                "altText": f"Photo of {sender_name}",
                            }],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "verticalContentAlignment": "Center",
                            "items": [{
                                "type": "TextBlock",
                                "text": sender_addr,
                                "isSubtle": True,
                                "size": "Small",
                                "wrap": True,
                                "spacing": "None",
                            }],
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
                # ── Triage buttons — row 1: Action + Waiting For ──────────────
                {
                    "type": "ActionSet",
                    "spacing": "Small",
                    "actions": [
                        {
                            "type": "Action.Submit",
                            "title": "Action",
                            "style": "positive",
                            "data": {"triageAction": "action", "emailId": email_id},
                        },
                        {
                            "type": "Action.Submit",
                            "title": "Waiting For",
                            "style": "default",
                            "data": {"triageAction": "waiting", "emailId": email_id},
                        },
                    ],
                },
                # ── Triage buttons — row 2: View + Delete ─────────────────────
                {
                    "type": "ActionSet",
                    "spacing": "Small",
                    "actions": [
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
                            "data": {"triageAction": "delete", "emailId": email_id},
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


def _send_card_to_teams(card: dict) -> None:
    bot_token, service_url, channel_id, bot_app_id = _get_delivery_config()
    url     = f"{service_url}/v3/conversations/{channel_id}/activities"
    payload = {
        "type": "message",
        "from": {"id": f"28:{bot_app_id}", "name": "Leo"},
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": card,
        }],
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
    logging.info(f"digest_shared: card delivered — status {resp.status_code}")


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
    logging.info(f"digest_shared: text delivered — status {resp.status_code}")
