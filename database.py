import os
import time
import logging
import threading
from datetime import datetime, timedelta, timezone

import requests
import telebot
from flask import Flask, request
from telebot.types import BotCommand

from database import (
    test_connection,
    get_table_counts,
    upsert_order,
    get_order_by_code,
    save_status_history,
    mark_order_morning_snapshot,
    save_daily_snapshot,
    get_snapshot_orders,
    get_open_delays,
    save_daily_otd,
    get_daily_otd,
    get_otd_history
)


# ============================================================
# CONFIG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )
)

API_KEY = os.getenv("TELEGRAM_API_KEY")
KASPI_AUTH_TOKEN = os.getenv("KASPI_AUTH_TOKEN")

if not API_KEY:
    raise ValueError(
        "TELEGRAM_API_KEY is not set"
    )

if not KASPI_AUTH_TOKEN:
    raise ValueError(
        "KASPI_AUTH_TOKEN is not set"
    )


bot = telebot.TeleBot(API_KEY)

app = Flask(__name__)


KASPI_URL = (
    "https://kaspi.kz/shop/api/v2/orders"
)

KZ_TZ = timezone(
    timedelta(hours=5)
)


# Можно менять потом через Render Environment
MORNING_REPORT_TIME = os.getenv(
    "MORNING_REPORT_TIME",
    "09:00"
)

EVENING_REPORT_TIME = os.getenv(
    "EVENING_REPORT_TIME",
    "20:00"
)


# ============================================================
# STORES
# ============================================================

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
    "14576033_9041": "Almaty Warehouse"
}


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

commands = [
    BotCommand(
        "db_test",
        "Проверить базу данных"
    ),

    BotCommand(
        "morning",
        "Сохранить Morning Snapshot"
    ),

    BotCommand(
        "daily_otd",
        "Рассчитать OTD сегодня"
    ),

    BotCommand(
        "pending_orders",
        "Заказы на передачу сегодня"
    ),

    BotCommand(
        "orders",
        "Открытые задержки"
    ),

    BotCommand(
        "open_delays",
        "Текущие открытые задержки"
    ),

    BotCommand(
        "history",
        "История OTD за 7 дней"
    )
]

bot.set_my_commands(commands)


# ============================================================
# HELPERS
# ============================================================

def now_kz():
    return datetime.now(KZ_TZ)


def today_kz():
    return now_kz().date()


def timestamp_to_datetime(value):
    if not value:
        return None

    try:
        return datetime.fromtimestamp(
            int(value) / 1000,
            tz=KZ_TZ
        )

    except Exception:
        return None


def send_long_message(
    chat_id,
    text
):
    max_length = 4000

    while text:
        part = text[:max_length]

        bot.send_message(
            chat_id,
            part
        )

        text = text[max_length:]


def kaspi_headers():
    return {
        "X-Auth-Token":
            KASPI_AUTH_TOKEN,

        "Accept":
            "application/vnd.api+json;charset=UTF-8",

        "Content-Type":
            "application/vnd.api+json",

        "User-Agent":
            "OMS-KZ-OTD-Bot/2.0"
    }


# ============================================================
# KASPI REQUEST
# ============================================================

def kaspi_get(
    params,
    attempts=3
):
    last_error = None

    for attempt in range(
        1,
        attempts + 1
    ):
        try:
            response = requests.get(
                KASPI_URL,
                headers=kaspi_headers(),
                params=params,
                timeout=30
            )

            logging.info(
                "Kaspi request | "
                f"status={response.status_code}"
            )

            response.raise_for_status()

            return response.json()

        except Exception as e:
            last_error = e

            logging.error(
                "Kaspi request failed | "
                f"attempt={attempt} | "
                f"{e}"
            )

            if attempt < attempts:
                time.sleep(
                    attempt * 2
                )

    raise last_error


# ============================================================
# PARSE KASPI ORDER
# ============================================================

def parse_kaspi_order(
    raw_order,
    current_time=None
):
    if current_time is None:
        current_time = now_kz()

    attributes = raw_order.get(
        "attributes",
        {}
    )

    code = str(
        attributes.get(
            "code",
            ""
        )
    ).strip()

    pickup_point_id = str(
        attributes.get(
            "pickupPointId",
            ""
        )
    ).strip()

    store_name = STORE_MAPPING.get(
        pickup_point_id,
        pickup_point_id
        or "Unknown Store"
    )

    status = (
        attributes.get(
            "status"
        )
        or "UNKNOWN"
    )

    creation_date = (
        timestamp_to_datetime(
            attributes.get(
                "creationDate"
            )
        )
    )

    # В документации эти поля могут идти
    # непосредственно в attributes.
    # Старый ответ Kaspi у некоторых продавцов
    # также содержит kaspiDelivery.
    kaspi_delivery = (
        attributes.get(
            "kaspiDelivery"
        )
        or {}
    )

    planned_raw = (
        attributes.get(
            "courierTransmissionPlanningDate"
        )
        or
        kaspi_delivery.get(
            "courierTransmissionPlanningDate"
        )
    )

    actual_raw = (
        attributes.get(
            "courierTransmissionDate"
        )
        or
        kaspi_delivery.get(
            "courierTransmissionDate"
        )
    )

    planned = timestamp_to_datetime(
        planned_raw
    )

    actual = timestamp_to_datetime(
        actual_raw
    )

    is_cancelled = (
        status == "CANCELLED"
    )

    was_delayed = False
    is_currently_delayed = False

    delay_started_at = None
    delay_resolved_at = None
    delay_minutes = 0

    # CANCELLED не должен портить OTD.
    if not is_cancelled and planned:

        if actual:

            if actual > planned:

                was_delayed = True

                delay_started_at = (
                    planned
                )

                delay_resolved_at = (
                    actual
                )

                delay_minutes = max(
                    0,
                    int(
                        (
                            actual
                            - planned
                        ).total_seconds()
                        / 60
                    )
                )

        else:

            if current_time > planned:

                was_delayed = True

                is_currently_delayed = True

                delay_started_at = (
                    planned
                )

                delay_minutes = max(
                    0,
                    int(
                        (
                            current_time
                            - planned
                        ).total_seconds()
                        / 60
                    )
                )

    return {
        "order_code":
            code,

        "pickup_point_id":
            pickup_point_id,

        "store_name":
            store_name,

        "creation_date":
            creation_date,

        "planned_transmission_date":
            planned,

        "actual_transmission_date":
            actual,

        "current_status":
            status,

        "is_cancelled":
            is_cancelled,

        "was_delayed":
            was_delayed,

        "is_currently_delayed":
            is_currently_delayed,

        "delay_started_at":
            delay_started_at,

        "delay_resolved_at":
            delay_resolved_at,

        "delay_minutes":
            delay_minutes
    }


# ============================================================
# FETCH ACCEPTED ORDERS
# ============================================================

def fetch_accepted_orders(
    lookback_days=14
):
    current = now_kz()

    start = (
        current
        - timedelta(
            days=lookback_days
        )
    )

    page = 0
    result = []

    while True:

        params = {
            "page[number]":
                page,

            "page[size]":
                100,

            "filter[orders][creationDate][$ge]":
                int(
                    start.timestamp()
                    * 1000
                ),

            "filter[orders][creationDate][$le]":
                int(
                    current.timestamp()
                    * 1000
                ),

            "filter[orders][status]":
                "ACCEPTED_BY_MERCHANT",

            "filter[orders][state]":
                "KASPI_DELIVERY"
        }

        data = kaspi_get(
            params
        )

        orders = data.get(
            "data",
            []
        )

        result.extend(
            orders
        )

        if len(orders) < 100:
            break

        page += 1

    return result


# ============================================================
# FETCH ORDER BY CODE
# ============================================================

def fetch_order_by_code(
    order_code
):
    data = kaspi_get(
        {
            "filter[orders][code]":
                str(order_code),

            "page[number]":
                0,

            "page[size]":
                100
        }
    )

    orders = data.get(
        "data",
        []
    )

    if not orders:
        return None

    # На всякий случай ищем точное совпадение.
    for raw_order in orders:

        code = str(
            raw_order
            .get(
                "attributes",
                {}
            )
            .get(
                "code",
                ""
            )
        )

        if code == str(
            order_code
        ):
            return raw_order

    return orders[0]


# ============================================================
# SAVE ORDER
# ============================================================

def save_parsed_order(
    parsed_order
):
    saved = upsert_order(
        parsed_order
    )

    if saved:
        save_status_history(
            saved["id"],
            parsed_order
        )

    return saved


# ============================================================
# MORNING SNAPSHOT
# ============================================================

def create_morning_snapshot(
    report_date=None
):
    if report_date is None:
        report_date = today_kz()

    current = now_kz()

    raw_orders = (
        fetch_accepted_orders()
    )

    today_orders = []

    for raw in raw_orders:

        parsed = parse_kaspi_order(
            raw,
            current
        )

        planned = parsed.get(
            "planned_transmission_date"
        )

        if not planned:
            continue

        # Главное правило:
        # ориентируемся исключительно на
        # courierTransmissionPlanningDate.
        #
        # Для 9041 никаких специальных
        # исключений больше нет.
        if planned.date() != report_date:
            continue

        saved = save_parsed_order(
            parsed
        )

        if not saved:
            continue

        mark_order_morning_snapshot(
            saved["id"],
            report_date
        )

        parsed[
            "was_on_time"
        ] = False

        parsed[
            "snapshot_delayed"
        ] = False

        save_daily_snapshot(
            report_date,
            saved["id"],
            parsed
        )

        today_orders.append(
            parsed
        )

    logging.info(
        "Morning snapshot created | "
        f"date={report_date} | "
        f"orders={len(today_orders)}"
    )

    return today_orders


# ============================================================
# REFRESH ONE ORDER
# ============================================================

def refresh_order(
    order_code
):
    raw = fetch_order_by_code(
        order_code
    )

    if not raw:
        logging.warning(
            "Order not found in Kaspi | "
            f"{order_code}"
        )

        # ВАЖНО:
        # отсутствие заказа НЕ считаем отменой.
        return None

    parsed = parse_kaspi_order(
        raw
    )

    saved = save_parsed_order(
        parsed
    )

    return parsed, saved


# ============================================================
# REFRESH ALL OPEN DELAYS
# ============================================================

def refresh_open_delays():
    open_orders = (
        get_open_delays()
    )

    refreshed = 0

    for order in open_orders:

        try:
            result = refresh_order(
                order["order_code"]
            )

            if result:
                refreshed += 1

            # Не атакуем Kaspi множеством запросов.
            time.sleep(0.15)

        except Exception:
            logging.exception(
                "Open delay refresh failed | "
                f"{order['order_code']}"
            )

    logging.info(
        "Open delays refreshed | "
        f"{refreshed}"
    )

    return refreshed


# ============================================================
# FINALIZE DAILY OTD
# ============================================================

def finalize_daily_otd(
    report_date=None
):
    if report_date is None:
        report_date = today_kz()

    snapshots = (
        get_snapshot_orders(
            report_date
        )
    )

    if not snapshots:
        logging.warning(
            "No morning snapshot exists | "
            f"{report_date}"
        )

        return []

    # Сначала обновляем старые открытые задержки.
    refresh_open_delays()

    current = now_kz()

    final_rows = []

    for snapshot in snapshots:

        order_code = str(
            snapshot[
                "order_code"
            ]
        )

        try:
            raw = fetch_order_by_code(
                order_code
            )

            # Если API временно не вернул заказ —
            # не называем его CANCELLED.
            if raw:

                parsed = (
                    parse_kaspi_order(
                        raw,
                        current
                    )
                )

                saved = (
                    save_parsed_order(
                        parsed
                    )
                )

            else:

                existing = (
                    get_order_by_code(
                        order_code
                    )
                )

                if not existing:
                    continue

                parsed = {
                    "order_code":
                        order_code,

                    "pickup_point_id":
                        existing[
                            "pickup_point_id"
                        ],

                    "store_name":
                        existing[
                            "store_name"
                        ],

                    "planned_transmission_date":
                        existing[
                            "planned_transmission_date"
                        ],

                    "actual_transmission_date":
                        existing[
                            "actual_transmission_date"
                        ],

                    "current_status":
                        existing[
                            "current_status"
                        ],

                    "is_cancelled":
                        existing[
                            "is_cancelled"
                        ],

                    "was_delayed":
                        existing[
                            "was_delayed"
                        ],

                    "is_currently_delayed":
                        existing[
                            "is_currently_delayed"
                        ],

                    "delay_minutes":
                        existing[
                            "delay_minutes"
                        ]
                        or 0
                }

                saved = existing

            planned = (
                parsed.get(
                    "planned_transmission_date"
                )
            )

            actual = (
                parsed.get(
                    "actual_transmission_date"
                )
            )

            cancelled = bool(
                parsed.get(
                    "is_cancelled"
                )
            )

            on_time = False
            delayed_for_otd = False
            delay_minutes = 0

            if not cancelled and planned:

                if actual:

                    if actual <= planned:
                        on_time = True

                    else:
                        delayed_for_otd = True

                        delay_minutes = max(
                            0,
                            int(
                                (
                                    actual
                                    - planned
                                ).total_seconds()
                                / 60
                            )
                        )

                else:

                    # Только если deadline уже наступил.
                    if current > planned:

                        delayed_for_otd = True

                        delay_minutes = max(
                            0,
                            int(
                                (
                                    current
                                    - planned
                                ).total_seconds()
                                / 60
                            )
                        )

            parsed[
                "was_on_time"
            ] = on_time

            parsed[
                "snapshot_delayed"
            ] = delayed_for_otd

            parsed[
                "delay_minutes"
            ] = delay_minutes

            save_daily_snapshot(
                report_date,
                saved["id"],
                parsed
            )

            final_rows.append(
                parsed
            )

            time.sleep(
                0.15
            )

        except Exception:
            logging.exception(
                "Finalization failed | "
                f"{order_code}"
            )

    # --------------------------------------------------------
    # AGGREGATE PER STORE
    # --------------------------------------------------------

    store_data = {}

    for order in final_rows:

        store = order[
            "store_name"
        ]

        if store not in store_data:

            store_data[store] = {
                "pickup_point_id":
                    order[
                        "pickup_point_id"
                    ],

                "morning_orders":
                    0,

                "cancelled_orders":
                    0,

                "on_time_orders":
                    0,

                "delayed_orders":
                    0
            }

        row = store_data[
            store
        ]

        row[
            "morning_orders"
        ] += 1

        if order.get(
            "is_cancelled"
        ):
            row[
                "cancelled_orders"
            ] += 1

        elif order.get(
            "was_on_time"
        ):
            row[
                "on_time_orders"
            ] += 1

        elif order.get(
            "snapshot_delayed"
        ):
            row[
                "delayed_orders"
            ] += 1

    open_delays = (
        get_open_delays()
    )

    open_by_store = {}

    for order in open_delays:

        store = order[
            "store_name"
        ]

        open_by_store[
            store
        ] = (
            open_by_store.get(
                store,
                0
            )
            + 1
        )

    result = []

    for store, row in (
        store_data.items()
    ):

        morning = (
            row[
                "morning_orders"
            ]
        )

        cancelled = (
            row[
                "cancelled_orders"
            ]
        )

        actual_orders = max(
            0,
            morning
            - cancelled
        )

        on_time = (
            row[
                "on_time_orders"
            ]
        )

        delayed = (
            row[
                "delayed_orders"
            ]
        )

        if actual_orders > 0:

            otd = round(
                on_time
                / actual_orders
                * 100,
                2
            )

        else:

            otd = None

        open_count = (
            open_by_store.get(
                store,
                0
            )
        )

        save_daily_otd(
            report_date=
                report_date,

            store_name=
                store,

            pickup_point_id=
                row[
                    "pickup_point_id"
                ],

            morning_orders=
                morning,

            cancelled_orders=
                cancelled,

            actual_orders=
                actual_orders,

            on_time_orders=
                on_time,

            delayed_orders=
                delayed,

            otd_percent=
                otd,

            open_delays=
                open_count
        )

        result.append(
            {
                "store_name":
                    store,

                "morning_orders":
                    morning,

                "cancelled_orders":
                    cancelled,

                "actual_orders":
                    actual_orders,

                "on_time_orders":
                    on_time,

                "delayed_orders":
                    delayed,

                "otd_percent":
                    otd,

                "open_delays":
                    open_count
            }
        )

    return result


# ============================================================
# OTD COLOR
# ============================================================

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


# ============================================================
# DB TEST
# ============================================================

@bot.message_handler(
    commands=[
        "db_test"
    ]
)
def db_test_command(
    message
):
    success, info = (
        test_connection()
    )

    if not success:

        bot.send_message(
            message.chat.id,
            "❌ Database connection failed\n\n"
            f"{info}"
        )

        return

    counts = (
        get_table_counts()
    )

    bot.send_message(
        message.chat.id,

        "✅ SUPABASE CONNECTED\n\n"

        f"Orders: "
        f"{counts['orders']}\n"

        f"History: "
        f"{counts['order_status_history']}\n"

        f"Snapshots: "
        f"{counts['daily_order_snapshot']}\n"

        f"Daily OTD: "
        f"{counts['daily_otd']}"
    )


# ============================================================
# MANUAL MORNING
# ============================================================

@bot.message_handler(
    commands=[
        "morning"
    ]
)
def morning_command(
    message
):
    bot.send_message(
        message.chat.id,
        "☀️ Creating morning snapshot..."
    )

    try:

        orders = (
            create_morning_snapshot()
        )

        by_store = {}

        for order in orders:

            store = (
                order[
                    "store_name"
                ]
            )

            by_store[
                store
            ] = (
                by_store.get(
                    store,
                    0
                )
                + 1
            )

        open_delays = (
            get_open_delays()
        )

        text = (
            "☀️ MORNING REPORT\n"
            f"{today_kz().strftime('%d.%m.%Y')}\n\n"
        )

        total = 0

        for store in sorted(
            by_store
        ):

            count = (
                by_store[
                    store
                ]
            )

            total += count

            text += (
                f"🏬 {store}: "
                f"{count}\n"
            )

        text += (
            "\n"
            f"📦 Planned Today: "
            f"{total}\n"
            f"🚨 Previous Open Delays: "
            f"{len(open_delays)}"
        )

        send_long_message(
            message.chat.id,
            text
        )

    except Exception as e:

        logging.exception(
            "Morning command failed"
        )

        bot.send_message(
            message.chat.id,
            f"❌ Error: {e}"
        )


# ============================================================
# PENDING TODAY WITH ORDER NUMBERS
# ============================================================

@bot.message_handler(
    commands=[
        "pending_orders"
    ]
)
def pending_orders_command(
    message
):
    try:

        raw_orders = (
            fetch_accepted_orders()
        )

        current = now_kz()

        grouped = {}

        for raw in raw_orders:

            order = (
                parse_kaspi_order(
                    raw,
                    current
                )
            )

            planned = (
                order.get(
                    "planned_transmission_date"
                )
            )

            if not planned:
                continue

            if (
                planned.date()
                != today_kz()
            ):
                continue

            if order.get(
                "actual_transmission_date"
            ):
                continue

            store = (
                order[
                    "store_name"
                ]
            )

            grouped.setdefault(
                store,
                []
            ).append(
                order
            )

        if not grouped:

            bot.send_message(
                message.chat.id,
                "✅ Нет заказов, ожидающих "
                "передачи сегодня."
            )

            return

        text = (
            "📦 PENDING ORDERS\n"
            f"{today_kz().strftime('%d.%m.%Y')}\n\n"
        )

        total = 0

        for store in sorted(
            grouped
        ):

            orders = (
                grouped[
                    store
                ]
            )

            total += len(
                orders
            )

            text += (
                f"🏬 {store} "
                f"({len(orders)})\n"
            )

            for order in orders:

                text += (
                    f"• "
                    f"{order['order_code']}\n"
                )

            text += "\n"

        text += (
            f"Total: {total}"
        )

        send_long_message(
            message.chat.id,
            text
        )

    except Exception as e:

        logging.exception(
            "Pending command failed"
        )

        bot.send_message(
            message.chat.id,
            f"❌ Error: {e}"
        )


# ============================================================
# OPEN DELAYS WITH ORDER NUMBERS
# ============================================================

def build_open_delays_text():
    orders = (
        get_open_delays()
    )

    if not orders:
        return (
            "✅ OPEN DELAYS\n\n"
            "Нет открытых задержек."
        )

    current = now_kz()

    grouped = {}

    for order in orders:

        store = (
            order[
                "store_name"
            ]
        )

        grouped.setdefault(
            store,
            []
        ).append(
            order
        )

    text = (
        "🚨 OPEN DELAYS\n"
        f"{today_kz().strftime('%d.%m.%Y')}\n\n"
    )

    for store in sorted(
        grouped
    ):

        store_orders = (
            grouped[
                store
            ]
        )

        text += (
            f"🏬 {store} "
            f"({len(store_orders)})\n"
        )

        for order in (
            store_orders
        ):

            planned = order[
                "planned_transmission_date"
            ]

            if planned:

                delta = (
                    current
                    - planned
                )

                hours = max(
                    0,
                    int(
                        delta.total_seconds()
                        // 3600
                    )
                )

                days = (
                    delta.days
                )

                planned_text = (
                    planned.astimezone(
                        KZ_TZ
                    ).strftime(
                        "%d.%m %H:%M"
                    )
                )

            else:

                hours = 0
                days = 0
                planned_text = "N/A"

            if days >= 3:

                age = (
                    f"🔴 {days} days"
                )

            elif days == 2:

                age = (
                    "🟠 2 days"
                )

            elif days == 1:

                age = (
                    "🟡 1 day"
                )

            else:

                age = (
                    f"⚠️ {hours}h"
                )

            text += (
                f"• "
                f"{order['order_code']}\n"
                f"  Planned: "
                f"{planned_text}\n"
                f"  Delay: "
                f"{age}\n"
            )

        text += "\n"

    text += (
        f"Total Open: {len(orders)}"
    )

    return text


@bot.message_handler(
    commands=[
        "orders",
        "open_delays"
    ]
)
def open_delays_command(
    message
):
    try:

        bot.send_message(
            message.chat.id,
            "🔄 Refreshing open delays..."
        )

        refresh_open_delays()

        text = (
            build_open_delays_text()
        )

        send_long_message(
            message.chat.id,
            text
        )

    except Exception as e:

        logging.exception(
            "Open delays command failed"
        )

        bot.send_message(
            message.chat.id,
            f"❌ Error: {e}"
        )


# ============================================================
# DAILY OTD
# ============================================================

@bot.message_handler(
    commands=[
        "daily_otd"
    ]
)
def daily_otd_command(
    message
):
    try:

        bot.send_message(
            message.chat.id,
            "🌙 Calculating Daily OTD..."
        )

        rows = finalize_daily_otd()

        if not rows:

            bot.send_message(
                message.chat.id,

                "⚠️ Morning snapshot "
                "for today is empty.\n\n"

                "Run /morning first."
            )

            return

        total_morning = sum(
            x[
                "morning_orders"
            ]
            for x in rows
        )

        total_cancelled = sum(
            x[
                "cancelled_orders"
            ]
            for x in rows
        )

        total_actual = sum(
            x[
                "actual_orders"
            ]
            for x in rows
        )

        total_on_time = sum(
            x[
                "on_time_orders"
            ]
            for x in rows
        )

        total_delayed = sum(
            x[
                "delayed_orders"
            ]
            for x in rows
        )

        if total_actual > 0:

            total_otd = round(
                total_on_time
                / total_actual
                * 100,
                2
            )

        else:
            total_otd = None

        icon = otd_icon(
            total_otd
        )

        if total_otd is None:
            otd_text = "N/A"
        else:
            otd_text = (
                f"{total_otd:.2f}%"
            )

        text = (
            "🌙 DAILY OTD\n"
            f"{today_kz().strftime('%d.%m.%Y')}\n\n"

            f"Orders: "
            f"{total_morning} "
            f"({total_actual})\n"

            f"Cancelled: "
            f"{total_cancelled}\n"

            f"On Time: "
            f"{total_on_time}\n"

            f"Delayed: "
            f"{total_delayed}\n"

            f"OTD: "
            f"{icon} "
            f"{otd_text}\n\n"

            "BY STORE\n\n"
        )

        for row in sorted(
            rows,
            key=lambda x:
                x[
                    "store_name"
                ]
        ):

            otd = row[
                "otd_percent"
            ]

            icon = otd_icon(
                otd
            )

            if otd is None:
                percent = "N/A"
            else:
                percent = (
                    f"{otd:.2f}%"
                )

            text += (
                f"🏬 "
                f"{row['store_name']}\n"

                f"Orders: "
                f"{row['morning_orders']} "
                f"({row['actual_orders']}) | "

                f"Delayed: "
                f"{row['delayed_orders']} | "

                f"{icon} "
                f"{percent}\n\n"
            )

        open_orders = (
            get_open_delays()
        )

        today_count = 0
        day1 = 0
        day2 = 0
        day3 = 0

        current = now_kz()

        for order in open_orders:

            planned = order[
                "planned_transmission_date"
            ]

            if not planned:
                continue

            days = (
                current.date()
                - planned.astimezone(
                    KZ_TZ
                ).date()
            ).days

            if days <= 0:
                today_count += 1

            elif days == 1:
                day1 += 1

            elif days == 2:
                day2 += 1

            else:
                day3 += 1

        text += (
            "⏳ OPEN DELAYS\n\n"

            f"Today: "
            f"{today_count}\n"

            f"1 Day: "
            f"{day1}\n"

            f"2 Days: "
            f"{day2}\n"

            f"3+ Days: "
            f"{day3}\n"

            f"Total Open: "
            f"{len(open_orders)}"
        )

        send_long_message(
            message.chat.id,
            text
        )

    except Exception as e:

        logging.exception(
            "Daily OTD command failed"
        )

        bot.send_message(
            message.chat.id,
            f"❌ Error: {e}"
        )


# ============================================================
# HISTORY
# ============================================================

@bot.message_handler(
    commands=[
        "history"
    ]
)
def history_command(
    message
):
    try:

        rows = (
            get_otd_history(
                7
            )
        )

        if not rows:

            bot.send_message(
                message.chat.id,
                "История пока пустая."
            )

            return

        text = (
            "📅 OTD HISTORY\n\n"
        )

        for row in rows:

            value = (
                row[
                    "otd_percent"
                ]
            )

            icon = otd_icon(
                value
            )

            if value is None:
                value_text = "N/A"
            else:
                value_text = (
                    f"{float(value):.2f}%"
                )

            text += (
                f"{row['report_date'].strftime('%d.%m.%Y')}\n"

                f"Orders: "
                f"{row['morning_orders']} "
                f"({row['actual_orders']}) | "

                f"Delayed: "
                f"{row['delayed_orders']} | "

                f"{icon} "
                f"{value_text}\n\n"
            )

        send_long_message(
            message.chat.id,
            text
        )

    except Exception as e:

        logging.exception(
            "History command failed"
        )

        bot.send_message(
            message.chat.id,
            f"❌ Error: {e}"
        )


# ============================================================
# AUTOMATIC SCHEDULER
# ============================================================

last_morning_run = None
last_evening_run = None


def automatic_morning_job():
    try:
        orders = (
            create_morning_snapshot()
        )

        logging.info(
            "Automatic morning job complete | "
            f"orders={len(orders)}"
        )

    except Exception:
        logging.exception(
            "Automatic morning job failed"
        )


def automatic_evening_job():
    try:
        rows = (
            finalize_daily_otd()
        )

        logging.info(
            "Automatic evening OTD complete | "
            f"stores={len(rows)}"
        )

    except Exception:
        logging.exception(
            "Automatic evening job failed"
        )


def scheduler_loop():
    global last_morning_run
    global last_evening_run

    logging.info(
        "Scheduler started | "
        f"morning={MORNING_REPORT_TIME} | "
        f"evening={EVENING_REPORT_TIME} | "
        "timezone=UTC+5"
    )

    while True:

        try:

            current = now_kz()

            current_time = (
                current.strftime(
                    "%H:%M"
                )
            )

            date = (
                current.date()
            )

            if (
                current_time
                == MORNING_REPORT_TIME
                and
                last_morning_run
                != date
            ):

                last_morning_run = (
                    date
                )

                threading.Thread(
                    target=
                        automatic_morning_job,
                    daemon=True
                ).start()

            if (
                current_time
                == EVENING_REPORT_TIME
                and
                last_evening_run
                != date
            ):

                last_evening_run = (
                    date
                )

                threading.Thread(
                    target=
                        automatic_evening_job,
                    daemon=True
                ).start()

            time.sleep(
                20
            )

        except Exception:

            logging.exception(
                "Scheduler loop error"
            )

            time.sleep(
                30
            )


# ============================================================
# FLASK / WEBHOOK
# ============================================================

@app.route(
    "/",
    methods=[
        "GET"
    ]
)
def home():
    return (
        "OMS KZ Bot is running",
        200
    )


@app.route(
    "/" + API_KEY,
    methods=[
        "POST"
    ]
)
def webhook():
    try:

        json_string = (
            request
            .get_data()
            .decode(
                "utf-8"
            )
        )

        update = (
            telebot.types.Update
            .de_json(
                json_string
            )
        )

        bot.process_new_updates(
            [update]
        )

        return (
            "OK",
            200
        )

    except Exception:

        logging.exception(
            "Webhook processing failed"
        )

        return (
            "ERROR",
            500
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    success, message = (
        test_connection()
    )

    if success:
        logging.info(
            "✅ Supabase connected"
        )

    else:
        logging.error(
            "❌ Supabase connection failed | "
            f"{message}"
        )

    scheduler_thread = (
        threading.Thread(
            target=
                scheduler_loop,
            daemon=True
        )
    )

    scheduler_thread.start()

    # Важно:
    # сначала снимаем старый webhook,
    # затем ставим новый.
    try:
        bot.remove_webhook()
        time.sleep(1)

        webhook_url = os.getenv(
            "WEBHOOK_URL",
            "https://nbot-n94j.onrender.com"
        )

        webhook_url = (
            webhook_url.rstrip(
                "/"
            )
        )

        bot.set_webhook(
            url=(
                f"{webhook_url}/"
                f"{API_KEY}"
            )
        )

        logging.info(
            "Telegram webhook installed"
        )

    except Exception:
        logging.exception(
            "Webhook setup failed"
        )

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
