import os
import io
import time
import html
import smtplib
import logging
import threading
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone, date
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path

import requests
import telebot
from flask import Flask, request
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from PIL import Image, ImageDraw, ImageFont

from database import (
    test_connection,
    get_table_counts,
    bulk_save_orders,
    bulk_save_morning_snapshot,
    bulk_finalize_snapshot,
    get_snapshot_count,
    get_snapshot_orders,
    get_orders_by_codes,
    get_open_delays,
    get_open_delay_codes,
    get_operational_orders,
    get_operational_order_codes,
    get_delayed_snapshot_orders,
    replace_daily_otd,
    get_daily_otd,
    get_otd_history,
    get_period_store_otd,
    get_store_period_otd,
    get_period_snapshots,
    get_database_cleanup_preview,
    delete_data_for_period,
    get_database_delete_all_preview,
    delete_all_bot_data,
)


# ============================================================
# CONFIG
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

TELEGRAM_API_KEY = os.getenv("TELEGRAM_API_KEY")
KASPI_AUTH_TOKEN = os.getenv("KASPI_AUTH_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://nbot-n94j.onrender.com").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "oms-kz-telegram-webhook")
PORT = int(os.getenv("PORT", "5000"))

MORNING_REPORT_TIME = os.getenv("MORNING_REPORT_TIME", "15:50")
EVENING_REPORT_TIME = os.getenv("EVENING_REPORT_TIME", "15:57")
SYNC_LOOKBACK_DAYS = int(os.getenv("SYNC_LOOKBACK_DAYS", "30"))
KASPI_PARALLEL_WORKERS = max(1, min(int(os.getenv("KASPI_PARALLEL_WORKERS", "12")), 20))
BACKGROUND_SYNC_MINUTES = max(5, int(os.getenv("BACKGROUND_SYNC_MINUTES", "10")))
AUTO_REPORT_WINDOW_MINUTES = max(1, int(os.getenv("AUTO_REPORT_WINDOW_MINUTES", "15")))

EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_TO = os.getenv("EMAIL_TO")
EMAIL_CC = os.getenv("EMAIL_CC")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.yandex.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

KASPI_URL = "https://kaspi.kz/shop/api/v2/orders"

if not TELEGRAM_API_KEY:
    raise RuntimeError("TELEGRAM_API_KEY is not set")
if not KASPI_AUTH_TOKEN:
    raise RuntimeError("KASPI_AUTH_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

bot = telebot.TeleBot(TELEGRAM_API_KEY, threaded=True, num_threads=8)
app = Flask(__name__)

# Kazakhstan has UTC+5 nationwide since 2024.
KZ_TZ = timezone(timedelta(hours=5))

STORE_MAPPING = {
    "14576033_9005": "Karaganda Tair",
    "14576033_9020": "Almaty Mart",
    "14576033_9003": "Almaty Aport",
    "14576033_9080": "Astana InStreet",
    "14576033_9078": "Aktobe InStreet",
    "14576033_9077": "Almaty InStreet",
    "14576033_9004": "Shym Bayan Sulu",
    "14576033_9104": "Astana Reebok",
    "14576033_9006": "Astana Asia Park",
    "14576033_9101": "Aktobe Reebok",
    "14576033_9041": "Almaty Warehouse",
}


def store_code_from_pickup_id(pickup_point_id):
    if not pickup_point_id:
        return "—"
    value = str(pickup_point_id)
    if "_" in value:
        value = value.rsplit("_", 1)[-1]
    return value[-4:] if len(value) >= 4 else value


STORE_CODE_BY_NAME = {
    name: store_code_from_pickup_id(pickup_id)
    for pickup_id, name in STORE_MAPPING.items()
}


def store_label(store_name, pickup_point_id=None):
    code = (
        store_code_from_pickup_id(pickup_point_id)
        if pickup_point_id
        else STORE_CODE_BY_NAME.get(store_name, "—")
    )
    return f"{code} {store_name}" if code != "—" else store_name
STORE_IDS_BY_NAME = {v: k for k, v in STORE_MAPPING.items()}

_user_state = {}
_report_lock = threading.Lock()
_scheduler_state = {"morning": None, "evening": None}


# ============================================================
# TIME / DISPLAY
# ============================================================

def now_kz():
    return datetime.now(KZ_TZ)


def today_kz():
    return now_kz().date()


def fmt_date(value):
    if not value:
        return "-"
    if isinstance(value, datetime):
        value = value.astimezone(KZ_TZ).date()
    return value.strftime("%d.%m.%Y")


def fmt_dt(value):
    if not value:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=KZ_TZ)
    return value.astimezone(KZ_TZ).strftime("%d.%m.%Y %H:%M")


def parse_date_text(text):
    text = text.strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise ValueError("Use YYYY-MM-DD or DD.MM.YYYY")


def parse_hhmm(value):
    h, m = value.split(":")
    return int(h), int(m)


def scheduled_time_window_open(now_value, hhmm, window_minutes=None):
    """
    True only inside the automatic report window.

    Example with 09:00 and 15 minutes:
        09:00:00 <= now < 09:15:00

    This prevents a Render deploy/restart at 13:00 from sending a missed
    morning email several hours late.
    """
    window_minutes = window_minutes or AUTO_REPORT_WINDOW_MINUTES
    h, m = parse_hhmm(hhmm)

    scheduled = datetime(
        now_value.year,
        now_value.month,
        now_value.day,
        h,
        m,
        tzinfo=KZ_TZ,
    )
    end = scheduled + timedelta(minutes=window_minutes)
    return scheduled <= now_value < end


def otd_icon(value):
    if value is None:
        return "⚪"
    value = float(value)
    if value >= 95:
        return "🟢"
    if value >= 90:
        return "🟡"
    if value >= 85:
        return "🟠"
    return "🔴"


def otd_fill(value):
    if value is None:
        return "D9E1F2"
    value = float(value)
    if value >= 95:
        return "C6EFCE"
    if value >= 90:
        return "FFEB9C"
    if value >= 85:
        return "F4B183"
    return "FFC7CE"


def delay_age_days(planned):
    if not planned:
        return 0
    if planned.tzinfo is None:
        planned = planned.replace(tzinfo=KZ_TZ)
    return max(0, (today_kz() - planned.astimezone(KZ_TZ).date()).days)


def delay_age_label(planned):
    days = delay_age_days(planned)
    if days <= 0:
        return "Today"
    if days == 1:
        return "1 Day"
    if days == 2:
        return "2 Days"
    return "3+ Days"


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ============================================================
# TELEGRAM HELPERS
# ============================================================

def send_long_message(chat_id, text, reply_markup=None):
    if len(text) <= 3900:
        return bot.send_message(chat_id, text, reply_markup=reply_markup)

    parts = []
    remaining = text
    while remaining:
        cut = min(3900, len(remaining))
        if cut < len(remaining):
            pos = remaining.rfind("\n", 0, cut)
            if pos > 500:
                cut = pos
        parts.append(remaining[:cut])
        remaining = remaining[cut:].lstrip()

    message = None
    for i, part in enumerate(parts):
        message = bot.send_message(
            chat_id,
            part,
            reply_markup=reply_markup if i == len(parts) - 1 else None,
        )
    return message


def safe_edit(call, text, reply_markup=None):
    try:
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=reply_markup,
        )
    except Exception:
        bot.send_message(call.message.chat.id, text, reply_markup=reply_markup)


def progress_message(chat_id, text):
    return bot.send_message(chat_id, text)


def update_progress(message, text):
    try:
        bot.edit_message_text(text, message.chat.id, message.message_id)
    except Exception:
        pass


# ============================================================
# KASPI API
# ============================================================

def kaspi_headers():
    return {
        "X-Auth-Token": KASPI_AUTH_TOKEN,
        "Accept": "application/vnd.api+json;charset=UTF-8",
        "Content-Type": "application/vnd.api+json",
        "User-Agent": "OMS-KZ-Reports/3.0",
    }


def kaspi_get(params, attempts=3):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                KASPI_URL,
                headers=kaspi_headers(),
                params=params,
                timeout=(10, 35),
            )
            logging.info(
                "Kaspi API | status=%s | attempt=%s | page=%s",
                response.status_code,
                attempt,
                params.get("page[number]", "-"),
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            logging.warning("Kaspi request failed attempt=%s: %s", attempt, exc)
            if attempt < attempts:
                time.sleep(attempt * 1.5)
    raise last_error


def ts_to_dt(value):
    if value in (None, "", 0):
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=KZ_TZ)
    except Exception:
        return None


def parse_kaspi_order(raw_order, current_time=None):
    """
    Parse a Kaspi order.

    Primary deadline:
        courierTransmissionPlanningDate

    Fallback ONLY for discovering old/current open delays when Kaspi does not
    return courierTransmissionPlanningDate:
        plannedDeliveryDate

    The Morning OTD snapshot still uses the real courier planning date only.
    This prevents delivery date from silently replacing the OTD deadline.
    """
    current_time = current_time or now_kz()
    a = raw_order.get("attributes") or {}
    delivery = a.get("kaspiDelivery") or {}

    code = str(a.get("code") or "").strip()
    pickup_id = str(a.get("pickupPointId") or "").strip()
    store_name = STORE_MAPPING.get(pickup_id, pickup_id or "Unknown Store")
    status = str(a.get("status") or "UNKNOWN")

    courier_planned = ts_to_dt(
        a.get("courierTransmissionPlanningDate")
        or delivery.get("courierTransmissionPlanningDate")
    )
    planned_delivery = ts_to_dt(a.get("plannedDeliveryDate"))
    actual = ts_to_dt(
        a.get("courierTransmissionDate")
        or delivery.get("courierTransmissionDate")
    )
    creation = ts_to_dt(a.get("creationDate"))

    # Effective deadline is used only for persistent delay discovery/storage.
    # Real OTD morning eligibility is controlled by courier_planning_date below.
    effective_planned = courier_planned or planned_delivery
    planning_source = (
        "courierTransmissionPlanningDate"
        if courier_planned
        else ("plannedDeliveryDate_fallback" if planned_delivery else None)
    )

    is_cancelled = status.upper() == "CANCELLED"
    was_delayed = False
    current_delay = False
    delay_started = None
    delay_resolved = None
    delay_minutes = 0

    if effective_planned:
        if actual:
            if actual > effective_planned:
                was_delayed = True
                delay_started = effective_planned
                delay_resolved = actual
                delay_minutes = max(
                    0,
                    int((actual - effective_planned).total_seconds() // 60),
                )
        elif not is_cancelled and current_time > effective_planned:
            was_delayed = True
            current_delay = True
            delay_started = effective_planned
            delay_minutes = max(
                0,
                int((current_time - effective_planned).total_seconds() // 60),
            )

    if is_cancelled:
        current_delay = False

    return {
        "order_code": code,
        "pickup_point_id": pickup_id,
        "store_name": store_name,
        "creation_date": creation,
        "planned_transmission_date": effective_planned,
        "courier_planning_date": courier_planned,
        "planned_delivery_date": planned_delivery,
        "planning_source": planning_source,
        "actual_transmission_date": actual,
        "current_status": status,
        "is_cancelled": is_cancelled,
        "was_delayed": was_delayed,
        "is_currently_delayed": current_delay,
        "delay_started_at": delay_started,
        "delay_resolved_at": delay_resolved,
        "delay_minutes": delay_minutes,
    }


def fetch_accepted_orders(lookback_days=None):
    """
    Fetch ACCEPTED_BY_MERCHANT / KASPI_DELIVERY orders.

    IMPORTANT:
    Kaspi API allows a maximum creationDate search interval of 14 days.
    Therefore a longer sync period (for example 30 days) is automatically
    split into safe date windows and merged in memory.

    Orders are de-duplicated by order code.
    """
    lookback_days = lookback_days or SYNC_LOOKBACK_DAYS

    end_dt = now_kz()
    start_dt = end_dt - timedelta(days=lookback_days)

    # Keep each request window comfortably below the documented 14-day maximum.
    # 13 days avoids boundary/rounding issues.
    max_window = timedelta(days=13)

    result_by_code = {}
    window_start = start_dt
    started = time.perf_counter()
    window_no = 0

    while window_start < end_dt:
        window_no += 1
        window_end = min(window_start + max_window, end_dt)

        page = 0
        page_size = 100

        logging.info(
            "Kaspi sync window %s | %s -> %s",
            window_no,
            window_start.strftime("%Y-%m-%d %H:%M"),
            window_end.strftime("%Y-%m-%d %H:%M"),
        )

        while True:
            params = {
                "page[number]": page,
                "page[size]": page_size,
                "filter[orders][creationDate][$ge]": int(
                    window_start.timestamp() * 1000
                ),
                "filter[orders][creationDate][$le]": int(
                    window_end.timestamp() * 1000
                ),
                "filter[orders][status]": "ACCEPTED_BY_MERCHANT",
                "filter[orders][state]": "KASPI_DELIVERY",
            }

            data = kaspi_get(params)
            rows = data.get("data") or []

            for row in rows:
                attrs = row.get("attributes") or {}
                code = str(attrs.get("code") or "").strip()

                # Code is the stable business identifier.
                # Fallback to API entity id only if code is unexpectedly absent.
                key = code or str(row.get("id") or "")
                if key:
                    result_by_code[key] = row

            logging.info(
                "Kaspi sync window %s | page=%s | received=%s | unique_total=%s",
                window_no,
                page,
                len(rows),
                len(result_by_code),
            )

            if len(rows) < page_size:
                break

            page += 1

        # The next window starts 1 millisecond after the previous one,
        # so the same boundary timestamp is not requested twice.
        window_start = window_end + timedelta(milliseconds=1)

    result = list(result_by_code.values())

    logging.info(
        "Accepted Kaspi orders fetched | %s unique orders | windows=%s | %.2fs",
        len(result),
        window_no,
        time.perf_counter() - started,
    )

    return result


def fetch_order_by_code(order_code):
    data = kaspi_get(
        {
            "filter[orders][code]": str(order_code),
            "page[number]": 0,
            "page[size]": 100,
        }
    )
    rows = data.get("data") or []
    for row in rows:
        code = str((row.get("attributes") or {}).get("code") or "")
        if code == str(order_code):
            return row
    return rows[0] if rows else None


def fetch_codes_parallel(codes, progress_callback=None):
    """
    Parallel API refresh. Returns {code: parsed_order_or_None}.
    A small worker pool makes OTD fast without flooding Kaspi.
    """
    codes = list(dict.fromkeys(str(c) for c in codes if c))
    if not codes:
        return {}

    result = {}
    done = 0
    total = len(codes)

    def worker(code):
        try:
            raw = fetch_order_by_code(code)
            return code, parse_kaspi_order(raw) if raw else None
        except Exception:
            logging.exception("Failed Kaspi refresh for %s", code)
            return code, None

    with ThreadPoolExecutor(max_workers=KASPI_PARALLEL_WORKERS) as executor:
        futures = [executor.submit(worker, code) for code in codes]
        for future in as_completed(futures):
            code, parsed = future.result()
            result[code] = parsed
            done += 1
            if progress_callback and (done == total or done % 20 == 0):
                progress_callback(done, total)

    return result


# ============================================================
# SYNC / MORNING
# ============================================================

def sync_current_orders(progress_callback=None):
    started = time.perf_counter()
    raw_orders = fetch_accepted_orders()
    current_time = now_kz()

    parsed = []
    for raw in raw_orders:
        order = parse_kaspi_order(raw, current_time)
        if order["order_code"]:
            parsed.append(order)

    # One bulk upsert + one compact history insert.
    bulk_save_orders(parsed, write_history=True)

    if progress_callback:
        progress_callback(len(parsed), len(parsed))

    logging.info(
        "Current order sync complete | %s orders | %.2fs",
        len(parsed),
        time.perf_counter() - started,
    )
    return parsed


def create_morning_snapshot(report_date=None, force=False, progress_callback=None):
    report_date = report_date or today_kz()

    # IMPORTANT:
    # Always sync first. This discovers old overdue accepted orders even when
    # today's immutable Morning Snapshot was already created by an earlier deploy.
    parsed = sync_current_orders(progress_callback=progress_callback)

    existing = get_snapshot_count(report_date)
    if existing and not force:
        logging.info(
            "Morning snapshot already exists | %s | %s orders | current orders still synced",
            report_date,
            existing,
        )
        return get_snapshot_orders(report_date), False

    started = time.perf_counter()

    # OTD snapshot uses ONLY courierTransmissionPlanningDate.
    # plannedDeliveryDate fallback is intentionally not used for OTD denominator.
    today_orders = [
        o for o in parsed
        if o.get("courier_planning_date")
        and o["courier_planning_date"].astimezone(KZ_TZ).date() == report_date
        and not o.get("is_cancelled")
    ]

    # Save the real courier plan into snapshot/DB for Morning OTD orders.
    for o in today_orders:
        o["planned_transmission_date"] = o["courier_planning_date"]

    id_map = bulk_save_orders(today_orders, write_history=False)
    bulk_save_morning_snapshot(report_date, today_orders, id_map)

    logging.info(
        "Morning snapshot complete | date=%s | orders=%s | %.2fs",
        report_date,
        len(today_orders),
        time.perf_counter() - started,
    )
    return get_snapshot_orders(report_date), True


# ============================================================
# OPEN DELAYS
# ============================================================

def refresh_open_delays(progress_callback=None):
    codes = get_open_delay_codes()
    if not codes:
        return []

    existing = get_orders_by_codes(codes)
    refreshed = fetch_codes_parallel(codes, progress_callback=progress_callback)

    to_save = []
    for code in codes:
        parsed = refreshed.get(code)
        if parsed:
            to_save.append(parsed)
        else:
            # Never infer cancellation from a missing API response.
            row = existing.get(code)
            if row:
                to_save.append(dict(row))

    # dict(row) contains all DB columns; bulk_save_orders reads only the needed keys.
    bulk_save_orders(to_save, write_history=True)
    return get_open_delays()


def operational_cutoff_end(report_date=None):
    report_date = report_date or today_kz()
    next_day = report_date + timedelta(days=1)
    return datetime(
        next_day.year,
        next_day.month,
        next_day.day,
        0,
        0,
        tzinfo=KZ_TZ,
    )


def refresh_operational_orders(progress_callback=None):
    """
    Refresh Telegram Pending/Orders data from Kaspi before showing it.
    """
    sync_current_orders()

    cutoff_end = operational_cutoff_end(today_kz())
    codes = get_operational_order_codes(cutoff_end)

    if not codes:
        return []

    existing = get_orders_by_codes(codes)
    refreshed = fetch_codes_parallel(codes, progress_callback=progress_callback)

    to_save = []
    for code in codes:
        parsed = refreshed.get(code)
        if parsed:
            to_save.append(parsed)
        else:
            row = existing.get(code)
            if row:
                # Missing API response is not treated as cancellation.
                to_save.append(dict(row))

    if to_save:
        bulk_save_orders(to_save, write_history=True)

    return get_operational_orders(cutoff_end)


def split_operational_orders(orders, report_date=None):
    report_date = report_date or today_kz()
    pending = []
    delayed = []

    for order in orders:
        planned = order.get("planned_transmission_date")
        if not planned:
            continue

        local_date = planned.astimezone(KZ_TZ).date()

        if order.get("is_currently_delayed") or local_date < report_date:
            delayed.append(order)
        elif local_date == report_date:
            pending.append(order)

    return pending, delayed


# ============================================================
# DAILY OTD
# ============================================================

def finalize_daily_otd(report_date=None, progress_callback=None):
    report_date = report_date or today_kz()
    snapshots = get_snapshot_orders(report_date)
    if not snapshots:
        return []

    started = time.perf_counter()
    codes = [str(s["order_code"]) for s in snapshots]
    existing = get_orders_by_codes(codes)

    refreshed = fetch_codes_parallel(codes, progress_callback=progress_callback)

    final_orders = []
    current_time = now_kz()

    for snap in snapshots:
        code = str(snap["order_code"])
        parsed = refreshed.get(code)

        if parsed is None:
            row = existing.get(code)
            if not row:
                continue
            parsed = {
                "order_code": code,
                "pickup_point_id": row.get("pickup_point_id"),
                "store_name": row.get("store_name"),
                "creation_date": row.get("creation_date"),
                "planned_transmission_date": row.get("planned_transmission_date"),
                "actual_transmission_date": row.get("actual_transmission_date"),
                "current_status": row.get("current_status"),
                "is_cancelled": bool(row.get("is_cancelled")),
                "was_delayed": bool(row.get("was_delayed")),
                "is_currently_delayed": bool(row.get("is_currently_delayed")),
                "delay_started_at": row.get("delay_started_at"),
                "delay_resolved_at": row.get("delay_resolved_at"),
                "delay_minutes": int(row.get("delay_minutes") or 0),
            }

        planned = parsed.get("planned_transmission_date") or snap.get("planned_transmission_date")
        actual = parsed.get("actual_transmission_date")
        cancelled = bool(parsed.get("is_cancelled"))

        was_on_time = False
        delayed_for_otd = False
        otd_delay_minutes = 0

        if not cancelled and planned:
            if actual:
                if actual <= planned:
                    was_on_time = True
                else:
                    delayed_for_otd = True
                    otd_delay_minutes = max(0, int((actual - planned).total_seconds() // 60))
            elif current_time > planned:
                delayed_for_otd = True
                otd_delay_minutes = max(0, int((current_time - planned).total_seconds() // 60))

        parsed["was_on_time"] = was_on_time
        parsed["snapshot_delayed"] = delayed_for_otd
        parsed["otd_delay_minutes"] = otd_delay_minutes
        final_orders.append(parsed)

    id_map = bulk_save_orders(final_orders, write_history=True)
    bulk_finalize_snapshot(report_date, final_orders, id_map)

    # Refresh previously open delays not already in today's snapshot.
    open_codes = [c for c in get_open_delay_codes() if c not in set(codes)]
    if open_codes:
        old_refreshed = fetch_codes_parallel(open_codes)
        old_existing = get_orders_by_codes(open_codes)
        old_save = [
            old_refreshed.get(c) or dict(old_existing[c])
            for c in open_codes
            if old_refreshed.get(c) or c in old_existing
        ]
        bulk_save_orders(old_save, write_history=True)

    open_orders = get_open_delays()
    open_by_store = defaultdict(int)
    for order in open_orders:
        open_by_store[order["store_name"]] += 1

    by_store = {}
    for order in final_orders:
        store = order["store_name"]
        row = by_store.setdefault(
            store,
            {
                "store_name": store,
                "pickup_point_id": order["pickup_point_id"],
                "morning_orders": 0,
                "cancelled_orders": 0,
                "actual_orders": 0,
                "on_time_orders": 0,
                "delayed_orders": 0,
                "otd_percent": None,
                "open_delays": 0,
            },
        )
        row["morning_orders"] += 1
        if order.get("is_cancelled"):
            row["cancelled_orders"] += 1
        elif order.get("was_on_time"):
            row["on_time_orders"] += 1
        elif order.get("snapshot_delayed"):
            row["delayed_orders"] += 1

    rows = []
    for store, row in by_store.items():
        row["actual_orders"] = max(0, row["morning_orders"] - row["cancelled_orders"])
        if row["actual_orders"]:
            row["otd_percent"] = round(
                row["on_time_orders"] / row["actual_orders"] * 100,
                2,
            )
        row["open_delays"] = open_by_store.get(store, 0)
        rows.append(row)

    replace_daily_otd(report_date, rows)

    logging.info(
        "Daily OTD complete | date=%s | orders=%s | stores=%s | %.2fs",
        report_date,
        len(final_orders),
        len(rows),
        time.perf_counter() - started,
    )
    return rows


# ============================================================
# REPORT TEXT
# ============================================================

def summarize_rows(rows):
    total = {
        "morning_orders": 0,
        "cancelled_orders": 0,
        "actual_orders": 0,
        "on_time_orders": 0,
        "delayed_orders": 0,
        "open_delays": 0,
        "otd_percent": None,
    }
    for r in rows:
        for key in (
            "morning_orders",
            "cancelled_orders",
            "actual_orders",
            "on_time_orders",
            "delayed_orders",
        ):
            total[key] += int(r.get(key) or 0)
    total["open_delays"] = len(get_open_delays())
    if total["actual_orders"]:
        total["otd_percent"] = round(
            total["on_time_orders"] / total["actual_orders"] * 100,
            2,
        )
    return total


def _ru_age_label(planned):
    return {
        "Today": "сегодня",
        "1 Day": "1 день",
        "2 Days": "2 дня",
        "3+ Days": "3+ дней",
    }.get(delay_age_label(planned), delay_age_label(planned))


def _store_groups(orders):
    grouped = defaultdict(list)
    for order in orders:
        grouped[order["store_name"]].append(order)
    return grouped


def morning_summary_text(report_date):
    snaps = get_snapshot_orders(report_date)
    old_open = [
        o for o in get_open_delays()
        if o.get("planned_transmission_date")
        and o["planned_transmission_date"].astimezone(KZ_TZ).date() < report_date
    ]

    grouped = _store_groups(snaps)

    text = (
        f"Утренний отчет\n"
        f"Дата: {fmt_date(report_date)}\n\n"
        f"На сегодня: {len(snaps)} заказов\n"
        f"Старых просрочек: {len(old_open)}"
    )

    if grouped:
        text += "\n\nПо магазинам\n"
        for store in sorted(grouped):
            text += f"\n{store}\n"
            text += f"Заказов: {len(grouped[store])}\n"

    return text.rstrip()


def daily_otd_text(report_date):
    rows = get_daily_otd(report_date)
    if not rows:
        return f"Итог дня\nДата: {fmt_date(report_date)}\n\nДанных пока нет."

    total = summarize_rows(rows)
    pct = total["otd_percent"]
    pct_text = "—" if pct is None else f"{float(pct):.2f}%"

    text = (
        f"Итог дня\n"
        f"Дата: {fmt_date(report_date)}\n\n"
        f"Заказов утром: {total['morning_orders']}\n"
        f"Учтено в OTD: {total['actual_orders']}\n"
        f"Передано вовремя: {total['on_time_orders']}\n"
        f"Просрочено: {total['delayed_orders']}\n"
        f"OTD: {pct_text}\n\n"
        f"По магазинам"
    )

    for r in rows:
        spct = r["otd_percent"]
        spct_text = "—" if spct is None else f"{float(spct):.2f}%"
        text += (
            f"\n\n{r['store_name']}\n"
            f"Вовремя: {r['on_time_orders']} из {r['actual_orders']}\n"
            f"Просрочено: {r['delayed_orders']}\n"
            f"OTD: {spct_text}"
        )

    text += f"\n\nОткрытых просрочек сейчас: {len(get_open_delays())}"
    return text.rstrip()


def open_delays_text(filter_age=None):
    orders = get_open_delays()

    if filter_age:
        orders = [
            o for o in orders
            if delay_age_label(o.get("planned_transmission_date")) == filter_age
        ]

    if not orders:
        return "📊 Статистика по задержанным заказам:\n\nЗадержанных заказов нет."

    grouped = defaultdict(list)
    for o in orders:
        grouped[o["store_name"]].append(o)

    lines = ["📊 Статистика по задержанным заказам:", ""]

    for store in sorted(grouped):
        lines.append(
            f"{STORE_CODE_BY_NAME.get(store, '—')} {store}: "
            f"{len(grouped[store])} заказов"
        )

    lines.extend([
        "",
        f"✅ Итого: {len(orders)} заказов",
        "",
        "📦 Задержанные заказы по магазинам:",
    ])

    for store in sorted(grouped):
        lines.append(
            f"Магазин {STORE_CODE_BY_NAME.get(store, '—')} {store}:"
        )
        for order in grouped[store]:
            lines.append(f"🔸 Номер заказа: {order['order_code']}")

    return "\n".join(lines)

def pending_orders_text(report_date, orders=None):
    if orders is None:
        orders = get_operational_orders(operational_cutoff_end(report_date))

    pending, _ = split_operational_orders(orders, report_date)

    if not pending:
        return "📊 Статистика по заказам, ожидающим передачи:\n\nЗаказов нет."

    grouped = defaultdict(list)
    for o in pending:
        grouped[o["store_name"]].append(o)

    lines = ["📊 Статистика по заказам, ожидающим передачи:", ""]

    for store in sorted(grouped):
        lines.append(
            f"{STORE_CODE_BY_NAME.get(store, '—')} {store}: "
            f"{len(grouped[store])} заказов"
        )

    lines.extend([
        "",
        f"✅ Итого: {len(pending)} заказов",
        "",
        "📦 Заказы по магазинам:",
    ])

    for store in sorted(grouped):
        lines.append(
            f"Магазин {STORE_CODE_BY_NAME.get(store, '—')} {store}:"
        )
        for order in grouped[store]:
            lines.append(f"🔸 Номер заказа: {order['order_code']}")

    return "\n".join(lines)

def all_orders_text(report_date, orders=None):
    if orders is None:
        orders = get_operational_orders(operational_cutoff_end(report_date))

    pending, delayed = split_operational_orders(orders, report_date)
    active = pending + delayed

    if not active:
        return "📊 Статистика по активным заказам:\n\nЗаказов нет."

    grouped = defaultdict(list)
    for o in active:
        grouped[o["store_name"]].append(o)

    lines = [
        "📊 Статистика по активным заказам:",
        "",
        f"Ожидают передачи: {len(pending)} заказов",
        f"Задержанные: {len(delayed)} заказов",
        "",
    ]

    for store in sorted(grouped):
        lines.append(
            f"{STORE_CODE_BY_NAME.get(store, '—')} {store}: "
            f"{len(grouped[store])} заказов"
        )

    lines.extend([
        "",
        f"✅ Итого: {len(active)} заказов",
        "",
        "📦 Активные заказы по магазинам:",
    ])

    for store in sorted(grouped):
        lines.append(
            f"Магазин {STORE_CODE_BY_NAME.get(store, '—')} {store}:"
        )
        for order in grouped[store]:
            lines.append(f"🔸 Номер заказа: {order['order_code']}")

    return "\n".join(lines)

def today_delays_text(report_date):
    orders = get_delayed_snapshot_orders(report_date)

    if not orders:
        return "📊 Статистика по просроченным заказам за день:\n\nПросрочек нет."

    grouped = defaultdict(list)
    for o in orders:
        grouped[o["store_name"]].append(o)

    lines = ["📊 Статистика по просроченным заказам за день:", ""]

    for store in sorted(grouped):
        lines.append(
            f"{STORE_CODE_BY_NAME.get(store, '—')} {store}: "
            f"{len(grouped[store])} заказов"
        )

    lines.extend([
        "",
        f"✅ Итого: {len(orders)} заказов",
        "",
        "📦 Просроченные заказы по магазинам:",
    ])

    for store in sorted(grouped):
        lines.append(
            f"Магазин {STORE_CODE_BY_NAME.get(store, '—')} {store}:"
        )
        for order in grouped[store]:
            lines.append(f"🔸 Номер заказа: {order['order_code']}")

    return "\n".join(lines)

def history_text(start_date, end_date):
    rows = get_otd_history(start_date, end_date)

    if not rows:
        return (
            f"История OTD\n"
            f"Период: {fmt_date(start_date)} — {fmt_date(end_date)}\n\n"
            f"Данных нет."
        )

    text = (
        f"История OTD\n"
        f"Период: {fmt_date(start_date)} — {fmt_date(end_date)}"
    )

    for r in rows:
        pct = r["otd_percent"]
        pct_text = "—" if pct is None else f"{float(pct):.2f}%"
        text += (
            f"\n\n{fmt_date(r['report_date'])}\n"
            f"Вовремя: {r['on_time_orders']} из {r['actual_orders']}\n"
            f"Просрочено: {r['delayed_orders']}\n"
            f"OTD: {pct_text}"
        )

    return text.rstrip()


def period_summary_text(start_date, end_date):
    rows = get_period_store_otd(start_date, end_date)

    if not rows:
        return (
            f"Отчет за период\n"
            f"{fmt_date(start_date)} — {fmt_date(end_date)}\n\n"
            f"Данных нет."
        )

    total = summarize_rows(rows)
    pct = total["otd_percent"]
    pct_text = "—" if pct is None else f"{float(pct):.2f}%"

    text = (
        f"Отчет за период\n"
        f"{fmt_date(start_date)} — {fmt_date(end_date)}\n\n"
        f"Заказов: {total['actual_orders']}\n"
        f"Вовремя: {total['on_time_orders']}\n"
        f"Просрочено: {total['delayed_orders']}\n"
        f"OTD: {pct_text}\n\n"
        f"По магазинам"
    )

    for r in rows:
        spct = r["otd_percent"]
        spct_text = "—" if spct is None else f"{float(spct):.2f}%"
        text += (
            f"\n\n{r['store_name']}\n"
            f"Вовремя: {r['on_time_orders']} из {r['actual_orders']}\n"
            f"OTD: {spct_text}"
        )

    return text.rstrip()


# ============================================================
# EXCEL
# ============================================================

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
THIN = Side(style="thin", color="D9E1F2")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)


def style_sheet(ws, freeze="A2"):
    ws.freeze_panes = freeze
    ws.auto_filter.ref = ws.dimensions
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = BORDER
    for row in ws.iter_rows():
        for cell in row:
            cell.border = BORDER
            cell.alignment = Alignment(vertical="top")
    for col_idx, column in enumerate(ws.columns, 1):
        width = min(
            45,
            max(12, max(len(str(cell.value or "")) for cell in column) + 2),
        )
        ws.column_dimensions[get_column_letter(col_idx)].width = width


def build_morning_excel(report_date):
    snaps = get_snapshot_orders(report_date)
    open_orders = get_open_delays()

    wb = Workbook()
    ws = wb.active
    ws.title = "Morning Orders"
    ws.append([
        "Order Code", "Store Code", "Store", "Pickup Point", "Planned Transmission",
        "Actual Transmission", "Status"
    ])
    for s in snaps:
        ws.append([
            s["order_code"],
            STORE_CODE_BY_NAME.get(s["store_name"], store_code_from_pickup_id(s.get("pickup_point_id"))),
            s["store_name"],
            s["pickup_point_id"],
            fmt_dt(s.get("planned_transmission_date")),
            fmt_dt(s.get("actual_transmission_date")),
            s.get("final_status") or s.get("current_status"),
        ])
    style_sheet(ws)

    old = wb.create_sheet("Previous Open Delays")
    old.append([
        "Order Code", "Store Code", "Store", "Pickup Point", "Planned Transmission",
        "Delay Age", "Current Status"
    ])
    for o in open_orders:
        if o.get("planned_transmission_date") and o["planned_transmission_date"].astimezone(KZ_TZ).date() < report_date:
            old.append([
                o["order_code"],
                STORE_CODE_BY_NAME.get(o["store_name"], store_code_from_pickup_id(o.get("pickup_point_id"))),
                o["store_name"],
                o["pickup_point_id"],
                fmt_dt(o.get("planned_transmission_date")),
                delay_age_label(o.get("planned_transmission_date")),
                o.get("current_status"),
            ])
    style_sheet(old)

    summary = wb.create_sheet("Summary", 0)
    summary.append(["Store Code", "Store", "Planned Today", "Previous Open Delays"])
    today_by_store = defaultdict(int)
    old_by_store = defaultdict(int)
    for s in snaps:
        today_by_store[s["store_name"]] += 1
    for o in open_orders:
        if o.get("planned_transmission_date") and o["planned_transmission_date"].astimezone(KZ_TZ).date() < report_date:
            old_by_store[o["store_name"]] += 1
    stores = sorted(set(today_by_store) | set(old_by_store))
    for store in stores:
        summary.append([STORE_CODE_BY_NAME.get(store, "—"), store, today_by_store[store], old_by_store[store]])
    summary.append(["", "TOTAL", sum(today_by_store.values()), sum(old_by_store.values())])
    style_sheet(summary)

    path = Path(tempfile.gettempdir()) / f"Morning_Orders_{report_date.isoformat()}.xlsx"
    wb.save(path)
    return path


def build_daily_excel(report_date):
    rows = get_daily_otd(report_date)
    snaps = get_snapshot_orders(report_date)
    delayed = get_delayed_snapshot_orders(report_date)
    open_orders = get_open_delays()

    wb = Workbook()
    summary = wb.active
    summary.title = "Summary"
    summary.append([
        "Store Code", "Store", "Morning Orders", "Cancelled", "Actual", "On Time",
        "Delayed", "OTD %", "Open Delays"
    ])
    for r in rows:
        summary.append([
            STORE_CODE_BY_NAME.get(r["store_name"], "—"),
            r["store_name"],
            r["morning_orders"],
            r["cancelled_orders"],
            r["actual_orders"],
            r["on_time_orders"],
            r["delayed_orders"],
            float(r["otd_percent"]) if r["otd_percent"] is not None else None,
            r["open_delays"],
        ])
    total = summarize_rows(rows)
    summary.append([
        "",
        "TOTAL",
        total["morning_orders"],
        total["cancelled_orders"],
        total["actual_orders"],
        total["on_time_orders"],
        total["delayed_orders"],
        total["otd_percent"],
        len(open_orders),
    ])
    style_sheet(summary)
    for row in range(2, summary.max_row + 1):
        cell = summary.cell(row=row, column=8)
        cell.fill = PatternFill("solid", fgColor=otd_fill(cell.value))
        if cell.value is not None:
            cell.number_format = '0.00"%"'

    todays = wb.create_sheet("Today's Orders")
    todays.append([
        "Order Code", "Store Code", "Store", "Pickup Point", "Planned", "Actual",
        "Final Status", "Cancelled", "On Time", "Delayed", "Delay Minutes"
    ])
    for s in snaps:
        todays.append([
            s["order_code"],
            STORE_CODE_BY_NAME.get(s["store_name"], store_code_from_pickup_id(s.get("pickup_point_id"))),
            s["store_name"], s["pickup_point_id"],
            fmt_dt(s.get("planned_transmission_date")),
            fmt_dt(s.get("actual_transmission_date")),
            s.get("final_status"),
            "Yes" if s.get("was_cancelled") else "No",
            "Yes" if s.get("was_on_time") else "No",
            "Yes" if s.get("was_delayed") else "No",
            int(s.get("delay_minutes") or 0),
        ])
    style_sheet(todays)

    dws = wb.create_sheet("Delayed Orders")
    dws.append([
        "Order Code", "Store Code", "Store", "Planned", "Actual", "Delay Minutes", "Status"
    ])
    for s in delayed:
        dws.append([
            s["order_code"], STORE_CODE_BY_NAME.get(s["store_name"], "—"), s["store_name"],
            fmt_dt(s.get("planned_transmission_date")),
            fmt_dt(s.get("actual_transmission_date")),
            int(s.get("delay_minutes") or 0),
            s.get("final_status"),
        ])
    style_sheet(dws)

    ows = wb.create_sheet("Open Delays")
    ows.append([
        "Order Code", "Store Code", "Store", "Planned", "Delay Age", "Delay Minutes", "Current Status"
    ])
    for o in open_orders:
        ows.append([
            o["order_code"], STORE_CODE_BY_NAME.get(o["store_name"], "—"), o["store_name"],
            fmt_dt(o.get("planned_transmission_date")),
            delay_age_label(o.get("planned_transmission_date")),
            int(o.get("delay_minutes") or 0),
            o.get("current_status"),
        ])
    style_sheet(ows)

    path = Path(tempfile.gettempdir()) / f"Daily_OTD_{report_date.isoformat()}.xlsx"
    wb.save(path)
    return path


def build_period_excel(start_date, end_date):
    store_rows = get_period_store_otd(start_date, end_date)
    daily_rows = get_otd_history(start_date, end_date)
    snaps = get_period_snapshots(start_date, end_date)

    wb = Workbook()
    ws = wb.active
    ws.title = "By Store"
    ws.append([
        "Store Code", "Store", "Morning Orders", "Cancelled", "Actual",
        "On Time", "Delayed", "OTD %"
    ])
    for r in store_rows:
        ws.append([
            STORE_CODE_BY_NAME.get(r["store_name"], "—"), r["store_name"],
            r["morning_orders"], r["cancelled_orders"],
            r["actual_orders"], r["on_time_orders"], r["delayed_orders"],
            float(r["otd_percent"]) if r["otd_percent"] is not None else None,
        ])
    style_sheet(ws)
    for row in range(2, ws.max_row + 1):
        cell = ws.cell(row=row, column=8)
        cell.fill = PatternFill("solid", fgColor=otd_fill(cell.value))
        if cell.value is not None:
            cell.number_format = '0.00"%"'

    dws = wb.create_sheet("By Day")
    dws.append([
        "Date", "Morning Orders", "Cancelled", "Actual",
        "On Time", "Delayed", "OTD %"
    ])
    for r in reversed(daily_rows):
        dws.append([
            fmt_date(r["report_date"]), r["morning_orders"], r["cancelled_orders"],
            r["actual_orders"], r["on_time_orders"], r["delayed_orders"],
            float(r["otd_percent"]) if r["otd_percent"] is not None else None,
        ])
    style_sheet(dws)
    for row in range(2, dws.max_row + 1):
        cell = dws.cell(row=row, column=7)
        cell.fill = PatternFill("solid", fgColor=otd_fill(cell.value))

    ows = wb.create_sheet("Orders")
    ows.append([
        "Date", "Order Code", "Store Code", "Store", "Planned", "Actual",
        "Cancelled", "On Time", "Delayed", "Delay Minutes"
    ])
    for s in snaps:
        ows.append([
            fmt_date(s["report_date"]), s["order_code"],
            STORE_CODE_BY_NAME.get(s["store_name"], "—"), s["store_name"],
            fmt_dt(s.get("planned_transmission_date")),
            fmt_dt(s.get("actual_transmission_date")),
            "Yes" if s.get("was_cancelled") else "No",
            "Yes" if s.get("was_on_time") else "No",
            "Yes" if s.get("was_delayed") else "No",
            int(s.get("delay_minutes") or 0),
        ])
    style_sheet(ows)

    path = Path(tempfile.gettempdir()) / f"OTD_{start_date.isoformat()}_{end_date.isoformat()}.xlsx"
    wb.save(path)
    return path


# ============================================================
# PNG TABLE SCREENSHOTS
# ============================================================

def _load_report_font(size=24, bold=False):
    candidates = []
    if bold:
        candidates.extend([
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        ])
    else:
        candidates.extend([
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ])
    for candidate in candidates:
        if os.path.exists(candidate):
            return ImageFont.truetype(candidate, size=size)
    return ImageFont.load_default()


def _draw_table_image(title, subtitle, headers, rows, filename, otd_column=None):
    """
    Creates a clean, saveable PNG report table for Telegram/email.
    rows: list[list[str]]
    otd_column: zero-based column index to apply OTD background colors.
    """
    title_font = _load_report_font(30, True)
    subtitle_font = _load_report_font(20, False)
    header_font = _load_report_font(19, True)
    cell_font = _load_report_font(18, False)

    padding_x = 18
    padding_y = 13
    row_height = 50
    title_h = 100

    # Estimate column widths from visible text.
    col_widths = []
    for col_idx, header in enumerate(headers):
        candidates = [str(header)]
        candidates.extend(str(r[col_idx]) if col_idx < len(r) else "" for r in rows)
        max_chars = min(max(len(x) for x in candidates), 34)
        width = max(115, min(390, max_chars * 11 + padding_x * 2))
        col_widths.append(width)

    table_width = sum(col_widths)
    image_width = max(1100, table_width + 60)
    image_height = title_h + row_height * (len(rows) + 1) + 50

    img = Image.new("RGB", (image_width, image_height), "white")
    draw = ImageDraw.Draw(img)

    draw.text((30, 20), title, font=title_font, fill="#111827")
    draw.text((30, 60), subtitle, font=subtitle_font, fill="#4B5563")

    x0 = 30
    y0 = title_h

    # Header
    x = x0
    for i, header in enumerate(headers):
        w = col_widths[i]
        draw.rectangle((x, y0, x + w, y0 + row_height), fill="#1F4E78", outline="#D1D5DB")
        draw.text((x + padding_x, y0 + padding_y), str(header), font=header_font, fill="white")
        x += w

    # Rows
    y = y0 + row_height
    for row_idx, row in enumerate(rows):
        x = x0
        base_fill = "#F8FAFC" if row_idx % 2 == 0 else "white"
        for col_idx, value in enumerate(row):
            w = col_widths[col_idx]
            fill = base_fill
            if otd_column is not None and col_idx == otd_column:
                raw = str(value).replace("%", "").strip()
                try:
                    fill = f"#{otd_fill(float(raw))}"
                except Exception:
                    fill = base_fill
            draw.rectangle((x, y, x + w, y + row_height), fill=fill, outline="#D1D5DB")
            draw.text((x + padding_x, y + padding_y), str(value), font=cell_font, fill="#111827")
            x += w
        y += row_height

    path = Path(tempfile.gettempdir()) / filename
    img.save(path, format="PNG", optimize=True)
    return path


def build_morning_table_image(report_date):
    snaps = get_snapshot_orders(report_date)
    open_orders = get_open_delays()

    planned = defaultdict(int)
    previous = defaultdict(int)
    for s in snaps:
        planned[s["store_name"]] += 1
    for o in open_orders:
        p = o.get("planned_transmission_date")
        if p and p.astimezone(KZ_TZ).date() < report_date:
            previous[o["store_name"]] += 1

    stores = sorted(set(planned) | set(previous))
    rows = [
        [STORE_CODE_BY_NAME.get(store, "—"), store, planned[store], previous[store]]
        for store in stores
    ]
    rows.append([
        "",
        "TOTAL",
        sum(planned.values()),
        sum(previous.values()),
    ])

    return _draw_table_image(
        "OMS KZ — Morning Report",
        fmt_date(report_date),
        ["Store Code", "Store", "Planned Today", "Previous Open Delays"],
        rows,
        f"Morning_Report_{report_date.isoformat()}.png",
    )


def build_daily_table_image(report_date):
    report_rows = get_daily_otd(report_date)
    total = summarize_rows(report_rows)

    rows = []
    for r in report_rows:
        pct = r["otd_percent"]
        pct_text = "N/A" if pct is None else f"{float(pct):.2f}%"
        rows.append([
            STORE_CODE_BY_NAME.get(r["store_name"], "—"),
            r["store_name"],
            f"{r['morning_orders']} ({r['actual_orders']})",
            r["on_time_orders"],
            r["delayed_orders"],
            pct_text,
        ])

    total_pct = total["otd_percent"]
    rows.append([
        "",
        "TOTAL",
        f"{total['morning_orders']} ({total['actual_orders']})",
        total["on_time_orders"],
        total["delayed_orders"],
        "N/A" if total_pct is None else f"{float(total_pct):.2f}%",
    ])

    return _draw_table_image(
        "OMS KZ — Daily OTD",
        fmt_date(report_date),
        ["Store Code", "Store", "Orders", "On Time", "Delayed", "OTD"],
        rows,
        f"Daily_OTD_{report_date.isoformat()}.png",
        otd_column=5,
    )


def send_report_photo(chat_id, path, caption=None):
    with open(path, "rb") as photo:
        bot.send_photo(
            chat_id,
            photo,
            caption=caption,
        )

# ============================================================
# EMAIL
# ============================================================

def parse_recipients(value):
    if not value:
        return []
    return [x.strip() for x in value.replace(";", ",").split(",") if x.strip()]


def email_is_configured():
    return bool(EMAIL_FROM and EMAIL_PASSWORD and EMAIL_TO)


def send_email(subject, html_body, attachment_paths=None):
    if not email_is_configured():
        raise RuntimeError("EMAIL_FROM / EMAIL_PASSWORD / EMAIL_TO are not fully configured")

    to_list = parse_recipients(EMAIL_TO)
    cc_list = parse_recipients(EMAIL_CC)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr(("OMS KZ Reports", EMAIL_FROM))
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg.set_content("This email contains an HTML report.")
    msg.add_alternative(html_body, subtype="html")

    for attachment_path in (attachment_paths or []):
        p = Path(attachment_path)
        suffix = p.suffix.lower()
        if suffix == ".xlsx":
            maintype = "application"
            subtype = "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif suffix == ".png":
            maintype = "image"
            subtype = "png"
        else:
            maintype = "application"
            subtype = "octet-stream"

        with open(p, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype=maintype,
                subtype=subtype,
                filename=p.name,
            )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_FROM, EMAIL_PASSWORD)
        smtp.send_message(msg, to_addrs=to_list + cc_list)


def html_table(headers, rows):
    th = "".join(
        f"<th style='padding:8px;border:1px solid #d9e2f3;background:#1f4e78;color:white'>{html.escape(str(h))}</th>"
        for h in headers
    )
    body = ""
    for row in rows:
        tds = ""
        for value, bg in row:
            style = f"background:{bg};" if bg else ""
            tds += f"<td style='padding:8px;border:1px solid #d9e2f3;{style}'>{html.escape(str(value))}</td>"
        body += f"<tr>{tds}</tr>"
    return f"<table style='border-collapse:collapse;font-family:Arial;font-size:13px'><tr>{th}</tr>{body}</table>"


def morning_email_html(report_date):
    snaps = get_snapshot_orders(report_date)
    open_orders = get_open_delays()
    planned = defaultdict(int)
    previous = defaultdict(int)
    for s in snaps:
        planned[s["store_name"]] += 1
    for o in open_orders:
        p = o.get("planned_transmission_date")
        if p and p.astimezone(KZ_TZ).date() < report_date:
            previous[o["store_name"]] += 1

    stores = sorted(set(planned) | set(previous))
    rows = [[
        (STORE_CODE_BY_NAME.get(store, "—"), None),
        (store, None),
        (planned[store], None),
        (previous[store], None),
    ] for store in stores]
    rows.append([
        ("", "#D9EAF7"),
        ("TOTAL", "#D9EAF7"),
        (sum(planned.values()), "#D9EAF7"),
        (sum(previous.values()), "#D9EAF7"),
    ])

    table = html_table(["Store Code", "Store", "Planned Today", "Previous Open Delays"], rows)
    return f"""
    <div style="font-family:Arial;color:#1f1f1f">
      <p>Hello colleagues,<br>Добрый день, коллеги!</p>
      <p>Please find the OMS KZ morning pending report for <b>{fmt_date(report_date)}</b>.<br>
      Ниже утренняя сводка OMS KZ на <b>{fmt_date(report_date)}</b>.</p>
      {table}
      <p>Detailed order-level information is attached in Excel.<br>
      Детальная информация по заказам находится во вложенном Excel.</p>
      <p>Best regards,<br>OMS KZ</p>
    </div>
    """


def daily_email_html(report_date):
    rows = get_daily_otd(report_date)
    total = summarize_rows(rows)

    table_rows = []
    for r in rows:
        pct = r["otd_percent"]
        pct_text = "N/A" if pct is None else f"{float(pct):.2f}%"
        bg = f"#{otd_fill(pct)}"
        table_rows.append([
            (STORE_CODE_BY_NAME.get(r["store_name"], "—"), None),
            (r["store_name"], None),
            (f"{r['morning_orders']} ({r['actual_orders']})", None),
            (r["on_time_orders"], None),
            (r["delayed_orders"], None),
            (pct_text, bg),
        ])

    total_pct = total["otd_percent"]
    total_pct_text = "N/A" if total_pct is None else f"{float(total_pct):.2f}%"
    table_rows.append([
        ("", "#D9EAF7"),
        ("TOTAL", "#D9EAF7"),
        (f"{total['morning_orders']} ({total['actual_orders']})", "#D9EAF7"),
        (total["on_time_orders"], "#D9EAF7"),
        (total["delayed_orders"], "#D9EAF7"),
        (total_pct_text, f"#{otd_fill(total_pct)}"),
    ])

    main_table = html_table(["Store Code", "Store", "Orders", "On Time", "Delayed", "OTD"], table_rows)

    ages = defaultdict(int)
    for o in get_open_delays():
        ages[delay_age_label(o.get("planned_transmission_date"))] += 1
    delay_rows = [
        [("Delayed Today", None), (ages["Today"], None)],
        [("Open 1 Day", None), (ages["1 Day"], None)],
        [("Open 2 Days", None), (ages["2 Days"], None)],
        [("Open 3+ Days", None), (ages["3+ Days"], None)],
        [("Total Open Delays", "#D9EAF7"), (sum(ages.values()), "#D9EAF7")],
    ]
    delay_table = html_table(["Delay Status", "Orders"], delay_rows)

    return f"""
    <div style="font-family:Arial;color:#1f1f1f">
      <p>Hello colleagues,<br>Добрый день, коллеги!</p>
      <p>Please find the OMS KZ Daily OTD report for <b>{fmt_date(report_date)}</b>.<br>
      Ниже итоговый OTD отчёт OMS KZ за <b>{fmt_date(report_date)}</b>.</p>
      {main_table}
      <br>
      {delay_table}
      <p><b>Orders</b> format: Morning Orders (Actual after cancellations).<br>
      Формат <b>Orders</b>: Утренние заказы (фактические после отмен).</p>
      <p>Individual order numbers are not shown in the email body. Detailed data is in Excel.<br>
      Номера заказов в теле письма не показываются. Детальная информация находится в Excel.</p>
      <p>Best regards,<br>OMS KZ</p>
    </div>
    """


def send_morning_email(report_date):
    excel_path = build_morning_excel(report_date)
    image_path = build_morning_table_image(report_date)
    try:
        send_email(
            f"OMS KZ | Morning Pending Report | {fmt_date(report_date)}",
            morning_email_html(report_date),
            [excel_path, image_path],
        )
    finally:
        for path in (excel_path, image_path):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


def send_daily_email(report_date):
    excel_path = build_daily_excel(report_date)
    image_path = build_daily_table_image(report_date)
    try:
        send_email(
            f"OMS KZ | Daily OTD Report | {fmt_date(report_date)}",
            daily_email_html(report_date),
            [excel_path, image_path],
        )
    finally:
        for path in (excel_path, image_path):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


# ============================================================
# MENU
# ============================================================

def main_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Утренний отчет", callback_data="morning"),
        InlineKeyboardButton("Итог дня", callback_data="daily"),
        InlineKeyboardButton("Ожидают передачи", callback_data="pending"),
        InlineKeyboardButton("Просрочки за день", callback_data="delayed_today"),
        InlineKeyboardButton("Открытые просрочки", callback_data="open"),
        InlineKeyboardButton("История по дням", callback_data="history"),
        InlineKeyboardButton("OTD за месяц", callback_data="monthly"),
        InlineKeyboardButton("Отчет по магазину", callback_data="stores"),
        InlineKeyboardButton("Период", callback_data="custom"),
        InlineKeyboardButton("Экспорт Excel", callback_data="export"),
    )
    kb.add(InlineKeyboardButton("Удалить старые данные", callback_data="db_cleanup"))
    return kb


def back_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Назад", callback_data="menu"))
    return kb


def morning_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Показать заказы", callback_data="pending"),
        InlineKeyboardButton("Старые просрочки", callback_data="open"),
        InlineKeyboardButton("Excel", callback_data="export_morning"),
        InlineKeyboardButton("Отправить на почту", callback_data="email_morning"),
        InlineKeyboardButton("Назад", callback_data="menu"),
    )
    return kb


def daily_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Просрочки за день", callback_data="delayed_today"),
        InlineKeyboardButton("Открытые просрочки", callback_data="open"),
        InlineKeyboardButton("По магазинам", callback_data="stores"),
        InlineKeyboardButton("Excel", callback_data="export_daily"),
        InlineKeyboardButton("Отправить на почту", callback_data="email_daily"),
        InlineKeyboardButton("Назад", callback_data="menu"),
    )
    return kb


def open_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Today", callback_data="open_age:Today"),
        InlineKeyboardButton("1 Day", callback_data="open_age:1 Day"),
        InlineKeyboardButton("2 Days", callback_data="open_age:2 Days"),
        InlineKeyboardButton("3+ Days", callback_data="open_age:3+ Days"),
        InlineKeyboardButton("Show All Orders", callback_data="open_all"),
        InlineKeyboardButton("Назад", callback_data="menu"),
    )
    return kb


def history_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Today", callback_data="hist:today"),
        InlineKeyboardButton("Yesterday", callback_data="hist:yesterday"),
        InlineKeyboardButton("Last 7 Days", callback_data="hist:7"),
        InlineKeyboardButton("Select Date", callback_data="hist:select"),
        InlineKeyboardButton("Назад", callback_data="menu"),
    )
    return kb


def store_menu():
    kb = InlineKeyboardMarkup(row_width=1)
    for store in sorted(STORE_MAPPING.values()):
        kb.add(InlineKeyboardButton(store, callback_data=f"store:{STORE_IDS_BY_NAME[store]}"))
    kb.add(InlineKeyboardButton("Назад", callback_data="menu"))
    return kb


def db_cleanup_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Старше 30 дней", callback_data="dbclean:30"),
        InlineKeyboardButton("Старше 60 дней", callback_data="dbclean:60"),
        InlineKeyboardButton("Старше 90 дней", callback_data="dbclean:90"),
        InlineKeyboardButton("Выбрать период", callback_data="dbclean:custom"),
        InlineKeyboardButton("Удалить все данные", callback_data="dbclean:all"),
        InlineKeyboardButton("Назад", callback_data="menu"),
    )
    return kb


def db_cleanup_confirm_menu(start_date, end_date):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(
            "Подтвердить удаление",
            callback_data=f"dbclean_confirm:{start_date.isoformat()}:{end_date.isoformat()}",
        ),
        InlineKeyboardButton("Отмена", callback_data="db_cleanup"),
    )
    return kb


def db_cleanup_all_confirm_menu():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("Да, удалить ВСЕ данные", callback_data="dbclean_all_confirm"),
        InlineKeyboardButton("Отмена", callback_data="db_cleanup"),
    )
    return kb


# ============================================================
# COMMANDS
# ============================================================

bot.set_my_commands([
    BotCommand("menu", "Главное меню"),
    BotCommand("morning", "Утренний отчет"),
    BotCommand("daily_otd", "Итог дня"),
    BotCommand("pending_orders", "Ожидают передачи сегодня"),
    BotCommand("orders", "Все активные заказы"),
    BotCommand("open_delays", "Открытые просрочки"),
    BotCommand("history", "История OTD"),
    BotCommand("monthly_otd", "OTD за текущий месяц"),
    BotCommand("send_pending_report", "Отправить утренний отчет на почту"),
    BotCommand("send_report", "Отправить итог дня на почту"),
    BotCommand("db_test", "Проверить базу данных"),
])


@bot.message_handler(commands=["start", "menu", "report"])
def cmd_menu(message):
    bot.send_message(message.chat.id, "OMS KZ", reply_markup=main_menu())


@bot.message_handler(commands=["db_test"])
def cmd_db_test(message):
    ok, info = test_connection()
    if not ok:
        bot.send_message(message.chat.id, f"❌ Database error\n{info}")
        return
    counts = get_table_counts()
    bot.send_message(
        message.chat.id,
        "✅ SUPABASE CONNECTED\n\n"
        f"Orders: {counts['orders']}\n"
        f"Order History: {counts['order_status_history']}\n"
        f"Daily Snapshots: {counts['daily_order_snapshot']}\n"
        f"Daily OTD: {counts['daily_otd']}"
    )


def run_morning_for_chat(chat_id):
    if not _report_lock.acquire(blocking=False):
        bot.send_message(chat_id, "⏳ Another report is already running. Please wait.")
        return
    progress = progress_message(chat_id, "Формирую утренний отчет...")
    started = time.perf_counter()
    try:
        def cb(done, total):
            update_progress(progress, f"Утренний отчет: {done}/{total}")

        snaps, created = create_morning_snapshot(progress_callback=cb)
        elapsed = time.perf_counter() - started
        update_progress(
            progress,
            f"✅ Morning Report ready in {elapsed:.1f}s\n"
            f"Orders in snapshot: {len(snaps)}"
        )
        send_long_message(chat_id, morning_summary_text(today_kz()), reply_markup=morning_menu())
        image_path = build_morning_table_image(today_kz())
        try:
            send_report_photo(
                chat_id,
                image_path,
                caption=f"Утренний отчет | {fmt_date(today_kz())}",
            )
        finally:
            image_path.unlink(missing_ok=True)
    except Exception as exc:
        logging.exception("Morning report failed")
        update_progress(progress, f"❌ Morning Report error\n{exc}")
    finally:
        _report_lock.release()


@bot.message_handler(commands=["morning"])
def cmd_morning(message):
    threading.Thread(target=run_morning_for_chat, args=(message.chat.id,), daemon=True).start()


def run_daily_for_chat(chat_id):
    if not _report_lock.acquire(blocking=False):
        bot.send_message(chat_id, "⏳ Another report is already running. Please wait.")
        return
    progress = progress_message(chat_id, "Формирую итог дня...")
    started = time.perf_counter()
    try:
        if get_snapshot_count(today_kz()) == 0:
            update_progress(progress, "Нет утреннего снимка за сегодня. Сначала запустите /morning.")
            return

        def cb(done, total):
            update_progress(progress, f"Проверяю статусы Kaspi: {done}/{total}")

        rows = finalize_daily_otd(progress_callback=cb)
        elapsed = time.perf_counter() - started
        update_progress(progress, f"✅ Daily OTD ready in {elapsed:.1f}s")
        send_long_message(chat_id, daily_otd_text(today_kz()), reply_markup=daily_menu())
        image_path = build_daily_table_image(today_kz())
        try:
            send_report_photo(
                chat_id,
                image_path,
                caption=f"Итог дня | {fmt_date(today_kz())}",
            )
        finally:
            image_path.unlink(missing_ok=True)
    except Exception as exc:
        logging.exception("Daily OTD failed")
        update_progress(progress, f"❌ Daily OTD error\n{exc}")
    finally:
        _report_lock.release()


@bot.message_handler(commands=["daily_otd"])
def cmd_daily(message):
    threading.Thread(target=run_daily_for_chat, args=(message.chat.id,), daemon=True).start()


def run_operational_for_chat(chat_id, mode="all"):
    if not _report_lock.acquire(blocking=False):
        bot.send_message(chat_id, "Сейчас выполняется другой отчет. Попробуйте через минуту.")
        return

    progress = progress_message(chat_id, "Обновляю заказы из Kaspi...")

    try:
        def cb(done, total):
            if total:
                update_progress(progress, f"Проверяю статусы: {done}/{total}")

        orders = refresh_operational_orders(progress_callback=cb)

        if mode == "pending":
            result = pending_orders_text(today_kz(), orders)
        else:
            result = all_orders_text(today_kz(), orders)

        update_progress(progress, "Данные обновлены.")
        send_long_message(chat_id, result, reply_markup=back_menu())

    except Exception as exc:
        logging.exception("Operational orders refresh failed")
        update_progress(progress, f"Ошибка обновления: {exc}")

    finally:
        _report_lock.release()


@bot.message_handler(commands=["pending_orders"])
def cmd_pending(message):
    threading.Thread(
        target=run_operational_for_chat,
        args=(message.chat.id, "pending"),
        daemon=True,
    ).start()


@bot.message_handler(commands=["orders"])
def cmd_orders(message):
    threading.Thread(
        target=run_operational_for_chat,
        args=(message.chat.id, "all"),
        daemon=True,
    ).start()


@bot.message_handler(commands=["open_delays"])
def cmd_open(message):
    send_long_message(message.chat.id, open_delays_text(), reply_markup=open_menu())


@bot.message_handler(commands=["history"])
def cmd_history(message):
    end = today_kz()
    start = end - timedelta(days=6)
    send_long_message(message.chat.id, history_text(start, end), reply_markup=history_menu())


@bot.message_handler(commands=["monthly_otd"])
def cmd_monthly(message):
    end = today_kz()
    start = end.replace(day=1)
    send_long_message(message.chat.id, period_summary_text(start, end), reply_markup=back_menu())


@bot.message_handler(commands=["send_pending_report"])
def cmd_send_pending_email(message):
    try:
        send_morning_email(today_kz())
        bot.send_message(message.chat.id, "Утренний отчет отправлен на почту.")
    except Exception as exc:
        logging.exception("Morning email failed")
        bot.send_message(message.chat.id, f"❌ Email error: {exc}")


@bot.message_handler(commands=["send_report"])
def cmd_send_daily_email(message):
    try:
        if not get_daily_otd(today_kz()):
            finalize_daily_otd(today_kz())
        send_daily_email(today_kz())
        bot.send_message(message.chat.id, "Итог дня отправлен на почту.")
    except Exception as exc:
        logging.exception("Daily email failed")
        bot.send_message(message.chat.id, f"❌ Email error: {exc}")


# ============================================================
# CALLBACKS
# ============================================================

@bot.callback_query_handler(func=lambda call: True)
def callbacks(call):
    data = call.data
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id)

    if data.startswith("period_export:"):
        _, start_text, end_text = data.split(":")
        start = date.fromisoformat(start_text)
        end = date.fromisoformat(end_text)
        try:
            path = build_period_excel(start, end)
            with open(path, "rb") as f:
                bot.send_document(chat_id, f, visible_file_name=path.name)
            path.unlink(missing_ok=True)
        except Exception as exc:
            logging.exception("Period export failed")
            bot.send_message(chat_id, f"❌ Export error: {exc}")
        return

    if data == "db_cleanup":
        safe_edit(
            call,
            "Удаление старых данных из Supabase\n\n"
            "Можно удалить историю за выбранный период. "
            "Текущие открытые просрочки и незавершенные заказы защищены.",
            db_cleanup_menu(),
        )
        return

    if data.startswith("dbclean:"):
        option = data.split(":", 1)[1]

        if option == "custom":
            _user_state[chat_id] = {"mode": "db_cleanup_period"}
            bot.send_message(
                chat_id,
                "Введите период для удаления:\n"
                "DD.MM.YYYY DD.MM.YYYY\n\n"
                "Пример: 01.01.2026 30.06.2026"
            )
            return

        if option == "all":
            preview = get_database_delete_all_preview()
            total_rows = sum(int(v or 0) for v in preview.values())
            safe_edit(
                call,
                "ВНИМАНИЕ: удалить ВСЕ данные из базы бота?\n\n"
                f"Daily OTD: {preview.get('daily_otd', 0)}\n"
                f"Снимки заказов: {preview.get('daily_order_snapshot', 0)}\n"
                f"История статусов: {preview.get('order_status_history', 0)}\n"
                f"Заказы: {preview.get('orders', 0)}\n\n"
                f"Всего строк: {total_rows}\n\n"
                "Будут удалены также текущие заказы и открытые просрочки. "
                "Таблицы Supabase останутся. После следующей синхронизации "
                "актуальные заказы снова загрузятся из Kaspi.",
                db_cleanup_all_confirm_menu(),
            )
            return

        days = int(option)
        end_date = today_kz() - timedelta(days=days)
        start_date = date(2000, 1, 1)

        preview = get_database_cleanup_preview(start_date, end_date)
        total_rows = sum(int(v or 0) for v in preview.values())

        safe_edit(
            call,
            f"Удалить данные до {fmt_date(end_date)} включительно?\n\n"
            f"Daily OTD: {preview.get('daily_otd', 0)}\n"
            f"Снимки заказов: {preview.get('daily_order_snapshot', 0)}\n"
            f"История статусов: {preview.get('order_status_history', 0)}\n"
            f"Старые завершенные заказы: {preview.get('orders', 0)}\n\n"
            f"Всего строк: {total_rows}\n\n"
            "Открытые просрочки и незавершенные заказы не удаляются.",
            db_cleanup_confirm_menu(start_date, end_date),
        )
        return

    if data == "dbclean_all_confirm":
        counts = delete_all_bot_data()
        total_rows = sum(int(v or 0) for v in counts.values())
        safe_edit(
            call,
            "Все данные базы бота удалены.\n\n"
            f"Daily OTD: {counts.get('daily_otd', 0)}\n"
            f"Снимки заказов: {counts.get('daily_order_snapshot', 0)}\n"
            f"История статусов: {counts.get('order_status_history', 0)}\n"
            f"Заказы: {counts.get('orders', 0)}\n\n"
            f"Удалено строк: {total_rows}\n\n"
            "Структура таблиц Supabase сохранена.",
            back_menu(),
        )
        return

    if data.startswith("dbclean_confirm:"):
        _, start_text, end_text = data.split(":", 2)
        start_date = date.fromisoformat(start_text)
        end_date = date.fromisoformat(end_text)

        counts = delete_data_for_period(start_date, end_date)
        total_rows = sum(int(v or 0) for v in counts.values())

        safe_edit(
            call,
            f"Удаление завершено.\n\n"
            f"Период: {fmt_date(start_date)} — {fmt_date(end_date)}\n"
            f"Daily OTD: {counts.get('daily_otd', 0)}\n"
            f"Снимки заказов: {counts.get('daily_order_snapshot', 0)}\n"
            f"История статусов: {counts.get('order_status_history', 0)}\n"
            f"Старые завершенные заказы: {counts.get('orders', 0)}\n\n"
            f"Удалено строк: {total_rows}",
            back_menu(),
        )
        return

    if data == "menu":
        safe_edit(call, "OMS KZ", main_menu())
        return

    if data == "morning":
        threading.Thread(target=run_morning_for_chat, args=(chat_id,), daemon=True).start()
        return

    if data == "daily":
        threading.Thread(target=run_daily_for_chat, args=(chat_id,), daemon=True).start()
        return

    if data == "pending":
        threading.Thread(
            target=run_operational_for_chat,
            args=(chat_id, "pending"),
            daemon=True,
        ).start()
        return

    if data == "orders_all":
        threading.Thread(
            target=run_operational_for_chat,
            args=(chat_id, "all"),
            daemon=True,
        ).start()
        return

    if data == "delayed_today":
        safe_edit(call, today_delays_text(today_kz()), back_menu())
        return

    if data in ("open", "open_all"):
        safe_edit(call, open_delays_text(), open_menu())
        return

    if data.startswith("open_age:"):
        age = data.split(":", 1)[1]
        safe_edit(call, open_delays_text(age), open_menu())
        return

    if data == "history":
        safe_edit(call, "История OTD", history_menu())
        return

    if data == "hist:today":
        d = today_kz()
        safe_edit(call, history_text(d, d), history_menu())
        return

    if data == "hist:yesterday":
        d = today_kz() - timedelta(days=1)
        safe_edit(call, history_text(d, d), history_menu())
        return

    if data == "hist:7":
        end = today_kz()
        start = end - timedelta(days=6)
        safe_edit(call, history_text(start, end), history_menu())
        return

    if data == "hist:select":
        _user_state[chat_id] = {"mode": "history_date"}
        bot.send_message(chat_id, "Введите дату: YYYY-MM-DD или DD.MM.YYYY")
        return

    if data == "monthly":
        end = today_kz()
        start = end.replace(day=1)
        safe_edit(call, period_summary_text(start, end), back_menu())
        return

    if data == "stores":
        safe_edit(call, "🏬 SELECT STORE", store_menu())
        return

    if data.startswith("store:"):
        pickup = data.split(":", 1)[1]
        store = STORE_MAPPING.get(pickup, pickup)
        end = today_kz()
        start = end - timedelta(days=29)
        rows = get_store_period_otd(store, start, end)
        if not rows:
            safe_edit(call, f"🏬 {store}\n\nNo data for last 30 days.", store_menu())
            return
        morning = sum(int(r["morning_orders"] or 0) for r in rows)
        actual = sum(int(r["actual_orders"] or 0) for r in rows)
        on_time = sum(int(r["on_time_orders"] or 0) for r in rows)
        delayed = sum(int(r["delayed_orders"] or 0) for r in rows)
        pct = round(on_time / actual * 100, 2) if actual else None
        pct_text = "N/A" if pct is None else f"{pct:.2f}%"
        text = (
            f"🏬 STORE REPORT\n{store}\n"
            f"{fmt_date(start)} — {fmt_date(end)}\n\n"
            f"Orders: {morning} ({actual})\n"
            f"On Time: {on_time}\n"
            f"Delayed: {delayed}\n"
            f"OTD: {otd_icon(pct)} {pct_text}\n"
            f"Current Open Delays: {sum(1 for o in get_open_delays() if o['store_name'] == store)}"
        )
        safe_edit(call, text, store_menu())
        return

    if data == "custom":
        _user_state[chat_id] = {"mode": "custom_period"}
        bot.send_message(
            chat_id,
            "Send period in one message:\n"
            "YYYY-MM-DD YYYY-MM-DD\n\n"
            "Example:\n2026-09-01 2026-09-11"
        )
        return

    if data in ("export", "export_daily"):
        try:
            path = build_daily_excel(today_kz())
            with open(path, "rb") as f:
                bot.send_document(chat_id, f, visible_file_name=path.name)
            path.unlink(missing_ok=True)
        except Exception as exc:
            logging.exception("Daily export failed")
            bot.send_message(chat_id, f"❌ Export error: {exc}")
        return

    if data == "export_morning":
        try:
            path = build_morning_excel(today_kz())
            with open(path, "rb") as f:
                bot.send_document(chat_id, f, visible_file_name=path.name)
            path.unlink(missing_ok=True)
        except Exception as exc:
            logging.exception("Morning export failed")
            bot.send_message(chat_id, f"❌ Export error: {exc}")
        return

    if data == "email_morning":
        try:
            send_morning_email(today_kz())
            bot.send_message(chat_id, "Утренний отчет отправлен на почту.")
        except Exception as exc:
            bot.send_message(chat_id, f"❌ Email error: {exc}")
        return

    if data == "email_daily":
        try:
            send_daily_email(today_kz())
            bot.send_message(chat_id, "Итог дня отправлен на почту.")
        except Exception as exc:
            bot.send_message(chat_id, f"❌ Email error: {exc}")
        return


# ============================================================
# TEXT INPUT FOR DATE / PERIOD
# ============================================================

@bot.message_handler(func=lambda m: m.chat.id in _user_state and not (m.text or "").startswith("/"))
def state_input(message):
    state = _user_state.get(message.chat.id) or {}
    mode = state.get("mode")

    try:
        if mode == "history_date":
            d = parse_date_text(message.text)
            _user_state.pop(message.chat.id, None)
            send_long_message(message.chat.id, history_text(d, d), reply_markup=history_menu())
            return

        if mode == "db_cleanup_period":
            parts = message.text.replace("—", " ").replace("–", " ").split()
            if len(parts) != 2:
                raise ValueError("Укажите ровно две даты")

            start = parse_date_text(parts[0])
            end = parse_date_text(parts[1])

            if start > end:
                start, end = end, start

            # Never allow deleting today/future data through this menu.
            latest_allowed = today_kz() - timedelta(days=1)
            if end > latest_allowed:
                end = latest_allowed

            if start > end:
                raise ValueError("Нельзя удалять сегодняшний или будущий день")

            preview = get_database_cleanup_preview(start, end)
            total_rows = sum(int(v or 0) for v in preview.values())

            _user_state.pop(message.chat.id, None)

            bot.send_message(
                message.chat.id,
                f"Удалить данные за период {fmt_date(start)} — {fmt_date(end)}?\n\n"
                f"Daily OTD: {preview.get('daily_otd', 0)}\n"
                f"Снимки заказов: {preview.get('daily_order_snapshot', 0)}\n"
                f"История статусов: {preview.get('order_status_history', 0)}\n"
                f"Старые завершенные заказы: {preview.get('orders', 0)}\n\n"
                f"Всего строк: {total_rows}\n\n"
                "Открытые просрочки и незавершенные заказы не удаляются.",
                reply_markup=db_cleanup_confirm_menu(start, end),
            )
            return

        if mode == "custom_period":
            parts = message.text.replace("—", " ").replace("–", " ").split()
            if len(parts) != 2:
                raise ValueError("Send exactly two dates")
            start = parse_date_text(parts[0])
            end = parse_date_text(parts[1])
            if start > end:
                start, end = end, start
            if (end - start).days > 366:
                raise ValueError("Maximum period is 366 days")
            _user_state.pop(message.chat.id, None)
            text = period_summary_text(start, end)

            kb = InlineKeyboardMarkup()
            kb.add(
                InlineKeyboardButton(
                    "📥 Export Excel",
                    callback_data=f"period_export:{start.isoformat()}:{end.isoformat()}"
                ),
                InlineKeyboardButton("🔙 Back", callback_data="menu"),
            )
            send_long_message(message.chat.id, text, reply_markup=kb)
            return

    except Exception as exc:
        bot.send_message(message.chat.id, f"❌ {exc}\nTry again or /menu")



# ============================================================
# BACKGROUND CURRENT-ORDER SYNC
# ============================================================

def background_sync_loop():
    """
    Keeps the DB current without making every Telegram button wait for Kaspi.
    Open Delays / History / Store Report therefore read quickly from Supabase.
    """
    logging.info("Background sync started | every %s minutes", BACKGROUND_SYNC_MINUTES)

    # Small startup delay lets Flask/webhook become ready first.
    time.sleep(8)

    while True:
        if _report_lock.acquire(blocking=False):
            try:
                started = time.perf_counter()
                sync_current_orders()
                logging.info(
                    "Background sync complete | %.2fs",
                    time.perf_counter() - started,
                )
            except Exception:
                logging.exception("Background sync failed")
            finally:
                _report_lock.release()
        else:
            logging.info("Background sync skipped: report is running")

        time.sleep(BACKGROUND_SYNC_MINUTES * 60)

# ============================================================
# AUTOMATIC JOBS
# ============================================================

def automatic_morning_job():
    if not _report_lock.acquire(blocking=False):
        logging.warning("Morning job skipped because another report is running")
        return
    try:
        create_morning_snapshot(today_kz())
        if email_is_configured():
            send_morning_email(today_kz())
        logging.info("Automatic morning job completed")
    except Exception:
        logging.exception("Automatic morning job failed")
    finally:
        _report_lock.release()


def automatic_evening_job():
    if not _report_lock.acquire(blocking=False):
        logging.warning("Evening job skipped because another report is running")
        return
    try:
        if get_snapshot_count(today_kz()) == 0:
            logging.warning("Evening OTD skipped: no morning snapshot")
            return
        finalize_daily_otd(today_kz())
        if email_is_configured():
            send_daily_email(today_kz())
        logging.info("Automatic evening job completed")
    except Exception:
        logging.exception("Automatic evening job failed")
    finally:
        _report_lock.release()


def scheduler_loop():
    logging.info(
        "Scheduler started | morning=%s | evening=%s | window=%s min | UTC+5",
        MORNING_REPORT_TIME,
        EVENING_REPORT_TIME,
        AUTO_REPORT_WINDOW_MINUTES,
    )

    while True:
        try:
            current = now_kz()
            d = current.date()

            # MORNING:
            # - only inside the configured time window
            # - only once in this process
            # - do not recreate/resend after a restart if today's snapshot exists
            if scheduled_time_window_open(current, MORNING_REPORT_TIME):
                if _scheduler_state["morning"] != d:
                    if get_snapshot_count(d) == 0:
                        _scheduler_state["morning"] = d
                        threading.Thread(
                            target=automatic_morning_job,
                            daemon=True,
                        ).start()
                    else:
                        _scheduler_state["morning"] = d
                        logging.info(
                            "Automatic morning skipped | snapshot already exists for %s",
                            d,
                        )

            # EVENING:
            # - only inside the configured time window
            # - only once in this process
            # - do not resend after a restart if Daily OTD is already stored
            if scheduled_time_window_open(current, EVENING_REPORT_TIME):
                if _scheduler_state["evening"] != d:
                    existing_daily = get_daily_otd(d)
                    if not existing_daily:
                        _scheduler_state["evening"] = d
                        threading.Thread(
                            target=automatic_evening_job,
                            daemon=True,
                        ).start()
                    else:
                        _scheduler_state["evening"] = d
                        logging.info(
                            "Automatic evening skipped | Daily OTD already exists for %s",
                            d,
                        )

            time.sleep(30)

        except Exception:
            logging.exception("Scheduler loop error")
            time.sleep(30)


# ============================================================
# FLASK / WEBHOOK
# ============================================================

@app.route("/", methods=["GET"])
def home():
    return "OMS KZ Bot v3 optimized is running", 200


@app.route(f"/{WEBHOOK_SECRET}", methods=["POST"])
def telegram_webhook():
    try:
        update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
        bot.process_new_updates([update])
        return "OK", 200
    except Exception:
        logging.exception("Telegram webhook error")
        return "ERROR", 500


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    ok, info = test_connection()
    if ok:
        logging.info("Supabase connected")
    else:
        logging.error("Supabase connection failed: %s", info)

    threading.Thread(target=scheduler_loop, daemon=True).start()
    threading.Thread(target=background_sync_loop, daemon=True).start()

    try:
        bot.remove_webhook()
        time.sleep(1)
        webhook = f"{WEBHOOK_URL}/{WEBHOOK_SECRET}"
        bot.set_webhook(url=webhook)
        logging.info("Telegram webhook installed")
    except Exception:
        logging.exception("Failed to install Telegram webhook")

    app.run(host="0.0.0.0", port=PORT, threaded=True)
