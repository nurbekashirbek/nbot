import os
import requests
import logging
from datetime import datetime, timedelta, timezone
import telebot
import schedule
import time
import openpyxl
import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import base64
import threading
from telebot.types import BotCommand
from flask import Flask, request

from database import test_connection, get_table_counts


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)


# ============================================================
# DATABASE CONNECTION TEST ON STARTUP
# ============================================================

db_ok, db_message = test_connection()

if db_ok:
    logging.info("✅ Supabase database connected successfully")
else:
    logging.error(
        f"❌ Supabase database connection failed: {db_message}"
    )


# ============================================================
# TELEGRAM BOT
# ============================================================

API_KEY = os.getenv('TELEGRAM_API_KEY')

if not API_KEY:
    raise ValueError("TELEGRAM_API_KEY is not set")

bot = telebot.TeleBot(API_KEY)


# ============================================================
# TELEGRAM COMMAND MENU
# ============================================================

commands = [
    BotCommand(
        'orders',
        'Получить список задержанных заказов'
    ),
    BotCommand(
        'pending_orders',
        'Получить список заказов, ожидающих передачи'
    ),
    BotCommand(
        'send_report',
        'Отправить отчет по задержанным заказам'
    ),
    BotCommand(
        'send_pending_report',
        'Отправить отчет по ожидающим заказам'
    ),
    BotCommand(
        'db_test',
        'Проверить подключение к базе данных'
    )
]

bot.set_my_commands(commands)


# ============================================================
# KASPI API
# ============================================================

API_URL = 'https://kaspi.kz/shop/api/v2/orders'

UTC_PLUS_5 = timezone(
    timedelta(hours=5)
)


# ============================================================
# STORE MAPPING
# ============================================================

store_mapping = {
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
    "Итого": "Total"
}


# ============================================================
# SEND LONG TELEGRAM MESSAGE
# ============================================================

def send_long_message(chat_id, message):

    max_message_length = 4096

    while len(message) > max_message_length:

        bot.send_message(
            chat_id,
            message[:max_message_length]
        )

        message = message[
            max_message_length:
        ]

    bot.send_message(
        chat_id,
        message
    )


# ============================================================
# DATE RANGE
# ============================================================

def get_date_range():

    today = datetime.now(
        UTC_PLUS_5
    )

    start_date = today - timedelta(
        days=14
    )

    return start_date, today


# ============================================================
# KASPI HEADERS
# ============================================================

def get_kaspi_headers():

    kaspi_token = os.getenv(
        'KASPI_AUTH_TOKEN'
    )

    if not kaspi_token:
        raise ValueError(
            "KASPI_AUTH_TOKEN is not set"
        )

    return {
        'X-Auth-Token': kaspi_token,
        'User-Agent': 'PostmanRuntime/7.32.0',
        'Accept':
            'application/vnd.api+json;charset=UTF-8',
        'Connection': 'keep-alive'
    }


# ============================================================
# OVERDUE ORDERS
# ============================================================

def get_overdue_orders():

    try:

        start_date, today = (
            get_date_range()
        )

        cutoff_time = today.replace(
            hour=23,
            minute=0,
            second=0,
            microsecond=0
        )

        params = {
            'page[number]': 0,
            'page[size]': 100,

            'filter[orders][creationDate][$ge]':
                int(
                    start_date.timestamp()
                    * 1000
                ),

            'filter[orders][creationDate][$le]':
                int(
                    today.timestamp()
                    * 1000
                ),

            'filter[orders][status]':
                'ACCEPTED_BY_MERCHANT',

            'filter[orders][state]':
                'KASPI_DELIVERY'
        }

        headers = get_kaspi_headers()

        overdue_orders_by_store = {}

        page_number = 0

        while True:

            params[
                'page[number]'
            ] = page_number

            max_attempts = 2
            attempt = 1
            response = None

            while attempt <= max_attempts:

                try:

                    response = requests.get(
                        API_URL,
                        params=params,
                        headers=headers,
                        timeout=30
                    )

                    logging.info(
                        "Kaspi overdue API "
                        f"response: "
                        f"{response.status_code}"
                    )

                    response.raise_for_status()

                    break

                except requests.exceptions.RequestException as e:

                    logging.error(
                        f"Overdue API attempt "
                        f"{attempt}: {e}"
                    )

                    if (
                        attempt
                        == max_attempts
                    ):
                        return None

                    attempt += 1

                    time.sleep(5)

            if response is None:
                return None

            data = response.json()

            orders = data.get(
                'data',
                []
            )

            if not orders:
                break

            for order in orders:

                attributes = order.get(
                    'attributes',
                    {}
                )

                order_code = (
                    attributes.get(
                        'code',
                        'Нет номера заказа'
                    )
                )

                pickup_point_id = (
                    attributes.get(
                        'pickupPointId',
                        'Неизвестный магазин'
                    )
                )

                pickup_point = (
                    store_mapping.get(
                        pickup_point_id,
                        pickup_point_id
                    )
                )

                kaspi_delivery = (
                    attributes.get(
                        'kaspiDelivery',
                        {}
                    )
                    or {}
                )

                planning_timestamp = (
                    kaspi_delivery.get(
                        'courierTransmissionPlanningDate'
                    )
                )

                actual_timestamp = (
                    kaspi_delivery.get(
                        'courierTransmissionDate'
                    )
                )

                if not planning_timestamp:
                    continue

                planned_date = (
                    datetime.fromtimestamp(
                        planning_timestamp / 1000,
                        tz=UTC_PLUS_5
                    )
                )

                if (
                    planned_date < today
                    or (
                        planned_date.date()
                        == today.date()
                        and planned_date
                        < cutoff_time
                    )
                ):

                    if actual_timestamp is None:

                        if (
                            pickup_point
                            not in
                            overdue_orders_by_store
                        ):
                            overdue_orders_by_store[
                                pickup_point
                            ] = []

                        overdue_orders_by_store[
                            pickup_point
                        ].append(
                            order_code
                        )

            if len(orders) < params[
                'page[size]'
            ]:
                break

            page_number += 1

        total = sum(
            len(v)
            for v
            in overdue_orders_by_store.values()
        )

        logging.info(
            f"Overdue orders found: {total}"
        )

        return overdue_orders_by_store

    except Exception as e:

        logging.error(
            f"get_overdue_orders error: {e}"
        )

        return None


# ============================================================
# PENDING ORDERS
# ============================================================

def get_pending_orders():

    try:

        start_date, today = (
            get_date_range()
        )

        start_of_day = today.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        end_of_day = today.replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=999999
        )

        params = {
            'page[number]': 0,
            'page[size]': 100,

            'filter[orders][creationDate][$ge]':
                int(
                    start_date.timestamp()
                    * 1000
                ),

            'filter[orders][creationDate][$le]':
                int(
                    today.timestamp()
                    * 1000
                ),

            'filter[orders][status]':
                'ACCEPTED_BY_MERCHANT',

            'filter[orders][state]':
                'KASPI_DELIVERY'
        }

        headers = get_kaspi_headers()

        pending_orders_by_store = {}

        page_number = 0

        while True:

            params[
                'page[number]'
            ] = page_number

            max_attempts = 2
            attempt = 1
            response = None

            while attempt <= max_attempts:

                try:

                    response = requests.get(
                        API_URL,
                        params=params,
                        headers=headers,
                        timeout=30
                    )

                    logging.info(
                        "Kaspi pending API "
                        f"response: "
                        f"{response.status_code}"
                    )

                    response.raise_for_status()

                    break

                except requests.exceptions.RequestException as e:

                    logging.error(
                        f"Pending API attempt "
                        f"{attempt}: {e}"
                    )

                    if (
                        attempt
                        == max_attempts
                    ):
                        return None

                    attempt += 1

                    time.sleep(5)

            if response is None:
                return None

            data = response.json()

            orders = data.get(
                'data',
                []
            )

            if not orders:
                break

            for order in orders:

                attributes = order.get(
                    'attributes',
                    {}
                )

                order_code = attributes.get(
                    'code',
                    'Нет номера заказа'
                )

                pickup_point_id = (
                    attributes.get(
                        'pickupPointId',
                        'Неизвестный магазин'
                    )
                )

                pickup_point = (
                    store_mapping.get(
                        pickup_point_id,
                        pickup_point_id
                    )
                )

                kaspi_delivery = (
                    attributes.get(
                        'kaspiDelivery',
                        {}
                    )
                    or {}
                )

                planning_timestamp = (
                    kaspi_delivery.get(
                        'courierTransmissionPlanningDate'
                    )
                )

                actual_timestamp = (
                    kaspi_delivery.get(
                        'courierTransmissionDate'
                    )
                )

                if not planning_timestamp:
                    continue

                planned_date = (
                    datetime.fromtimestamp(
                        planning_timestamp / 1000,
                        tz=UTC_PLUS_5
                    )
                )

                if (
                    start_of_day
                    <= planned_date
                    <= end_of_day
                    and
                    actual_timestamp is None
                ):

                    if (
                        pickup_point
                        not in
                        pending_orders_by_store
                    ):

                        pending_orders_by_store[
                            pickup_point
                        ] = []

                    pending_orders_by_store[
                        pickup_point
                    ].append(
                        order_code
                    )

            if len(orders) < params[
                'page[size]'
            ]:
                break

            page_number += 1

        total = sum(
            len(v)
            for v
            in pending_orders_by_store.values()
        )

        logging.info(
            f"Pending orders found: {total}"
        )

        return pending_orders_by_store

    except Exception as e:

        logging.error(
            f"get_pending_orders error: {e}"
        )

        return None


# ============================================================
# CREATE EXCEL
# ============================================================

def create_excel(
    orders_by_store,
    sheet_name="Orders"
):

    wb = openpyxl.Workbook()

    ws1 = wb.active

    ws1.title = sheet_name

    ws1.append([
        "Store",
        "Order Number"
    ])

    for store, orders in (
        orders_by_store.items()
    ):

        for order_code in orders:

            ws1.append([
                store,
                order_code
            ])

    ws2 = wb.create_sheet(
        "Statistics"
    )

    ws2.append([
        "Store",
        "Number of Orders"
    ])

    total_orders = 0

    for store, orders in (
        orders_by_store.items()
    ):

        ws2.append([
            store,
            len(orders)
        ])

        total_orders += len(
            orders
        )

    ws2.append([
        "Итого",
        total_orders
    ])

    safe_sheet_name = (
        sheet_name
        .lower()
        .replace(
            ' ',
            '_'
        )
    )

    file_name = (
        f"{safe_sheet_name}_"
        f"{datetime.now(UTC_PLUS_5).strftime('%Y%m%d_%H%M%S')}.xlsx"
    )

    wb.save(
        file_name
    )

    return file_name


# ============================================================
# SCREENSHOT
# ============================================================

def create_table_screenshot(
    df,
    filename
):

    fig, ax = plt.subplots(
        figsize=(
            7,
            max(
                2,
                len(df) * 0.4
            )
        )
    )

    ax.axis(
        'off'
    )

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='center',
        loc='center'
    )

    table.auto_set_font_size(
        False
    )

    table.set_fontsize(
        12
    )

    table.scale(
        1,
        1.5
    )

    plt.tight_layout()

    plt.savefig(
        filename,
        bbox_inches='tight',
        pad_inches=0.1
    )

    plt.close()


def create_statistics_screenshot(
    file_name
):

    df = pd.read_excel(
        file_name,
        sheet_name="Statistics"
    )

    screenshot_filename = (
        "statistics_screenshot_"
        f"{datetime.now(UTC_PLUS_5).strftime('%Y%m%d_%H%M%S')}.png"
    )

    create_table_screenshot(
        df,
        screenshot_filename
    )

    return screenshot_filename


# ============================================================
# SEND EMAIL
# ============================================================

def send_email(
    file_name,
    subject,
    email_body
):

    max_attempts = 2
    attempt = 1

    screenshot_filename = None

    while attempt <= max_attempts:

        try:

            from_email = os.getenv(
                'EMAIL_FROM'
            )

            email_password = os.getenv(
                'EMAIL_PASSWORD'
            )

            to_email_raw = os.getenv(
                'EMAIL_TO',
                ''
            )

            cc_email_raw = os.getenv(
                'EMAIL_CC',
                ''
            )

            if not from_email:
                raise ValueError(
                    "EMAIL_FROM is not set"
                )

            if not email_password:
                raise ValueError(
                    "EMAIL_PASSWORD is not set"
                )

            to_email = [
                email.strip()
                for email
                in to_email_raw.split(',')
                if email.strip()
            ]

            cc_emails = [
                email.strip()
                for email
                in cc_email_raw.split(',')
                if email.strip()
            ]

            screenshot_filename = (
                create_statistics_screenshot(
                    file_name
                )
            )

            with open(
                screenshot_filename,
                "rb"
            ) as img_file:

                img_base64 = (
                    base64.b64encode(
                        img_file.read()
                    ).decode(
                        'utf-8'
                    )
                )

            msg = MIMEMultipart(
                'alternative'
            )

            msg[
                'From'
            ] = (
                f"Nurbek ASHIRBEK "
                f"<{from_email}>"
            )

            msg[
                'To'
            ] = ', '.join(
                to_email
            )

            msg[
                'Cc'
            ] = ', '.join(
                cc_emails
            )

            msg[
                'Subject'
            ] = subject

            html_body = f"""
            <html>
                <body>

                    <p>
                        {email_body}
                    </p>

                    <img
                        src="data:image/png;base64,{img_base64}"
                        alt="Statistics Table"
                        style="
                            width:100%;
                            max-width:500px;
                        "
                    />

                    <p style="margin-top:20px;">
                        С уважением,
                    </p>

                    <p>

                        <span
                            style="
                                color:#FF5733;
                                font-weight:bold;
                                font-size:22px;
                            "
                        >
                            Nurbek ASHIRBEK
                        </span>

                        <br>

                        <span>
                            E-commerce specialist
                        </span>

                    </p>

                </body>
            </html>
            """

            msg.attach(
                MIMEText(
                    html_body,
                    'html'
                )
            )

            with open(
                file_name,
                'rb'
            ) as f:

                attachment = (
                    MIMEApplication(
                        f.read(),
                        _subtype="xlsx"
                    )
                )

                attachment.add_header(
                    'Content-Disposition',
                    'attachment',
                    filename=os.path.basename(
                        file_name
                    )
                )

                msg.attach(
                    attachment
                )

            server = smtplib.SMTP(
                'smtp.yandex.com',
                587
            )

            server.starttls()

            server.login(
                from_email,
                email_password
            )

            all_recipients = (
                to_email
                + cc_emails
            )

            server.sendmail(
                from_email,
                all_recipients,
                msg.as_string()
            )

            server.quit()

            logging.info(
                "Email sent successfully"
            )

            return True

        except Exception as e:

            logging.error(
                f"Email attempt "
                f"{attempt}: {e}"
            )

            if (
                attempt
                == max_attempts
            ):
                return False

            attempt += 1

            time.sleep(
                10
            )

        finally:

            if (
                screenshot_filename
                and
                os.path.exists(
                    screenshot_filename
                )
            ):

                try:

                    os.remove(
                        screenshot_filename
                    )

                except Exception as e:

                    logging.error(
                        "Screenshot delete "
                        f"error: {e}"
                    )

            if (
                file_name
                and
                os.path.exists(
                    file_name
                )
            ):

                try:

                    os.remove(
                        file_name
                    )

                except Exception as e:

                    logging.error(
                        "Excel delete "
                        f"error: {e}"
                    )


# ============================================================
# /DB_TEST
# ============================================================

@bot.message_handler(
    commands=[
        'db_test'
    ]
)
def db_test(
    message
):

    try:

        bot.send_message(
            message.chat.id,
            "🔄 Проверяю подключение "
            "к Supabase..."
        )

        success, error_message = (
            test_connection()
        )

        if not success:

            bot.send_message(
                message.chat.id,
                "❌ Не удалось подключиться "
                "к Supabase.\n\n"
                f"Причина: "
                f"{error_message}"
            )

            return

        counts = get_table_counts()

        if counts is None:

            bot.send_message(
                message.chat.id,
                "⚠️ Подключение к Supabase "
                "работает, но таблицы "
                "прочитать не удалось."
            )

            return

        response = (
            "✅ Supabase connected "
            "successfully!\n\n"
            "📊 Database status:\n\n"

            f"📦 Orders: "
            f"{counts['orders']}\n"

            f"📝 Order history: "
            f"{counts['order_status_history']}\n"

            f"☀️ Daily snapshots: "
            f"{counts['daily_order_snapshot']}\n"

            f"📈 Daily OTD: "
            f"{counts['daily_otd']}"
        )

        bot.send_message(
            message.chat.id,
            response
        )

    except Exception as e:

        logging.error(
            f"DB test command error: {e}"
        )

        bot.send_message(
            message.chat.id,
            "❌ Database test error.\n\n"
            "Проверь Render Logs."
        )


# ============================================================
# /ORDERS
# ============================================================

@bot.message_handler(
    commands=[
        'orders'
    ]
)
def fetch_orders(
    message
):

    try:

        bot.send_message(
            message.chat.id,
            "🔄 Получение списка "
            "просроченных заказов..."
        )

        overdue_orders_by_store = (
            get_overdue_orders()
        )

        if not overdue_orders_by_store:

            bot.send_message(
                message.chat.id,
                "❌ Нет просроченных заказов "
                "за указанный период."
            )

            return

        response_text_orders = (
            "📦 Задержанные заказы "
            "по магазинам:\n\n"
        )

        for store, orders in (
            overdue_orders_by_store.items()
        ):

            response_text_orders += (
                f"Магазин {store}:\n"
            )

            for order_code in orders:

                response_text_orders += (
                    f"  🔸 Номер заказа: "
                    f"{order_code}\n"
                )

            response_text_orders += "\n"

        send_long_message(
            message.chat.id,
            response_text_orders
        )

        response_text_count = (
            "📊 Статистика по "
            "задержанным заказам:\n\n"
        )

        total_orders = 0

        for store, orders in (
            overdue_orders_by_store.items()
        ):

            response_text_count += (
                f"{store}: "
                f"{len(orders)} заказов\n"
            )

            total_orders += len(
                orders
            )

        response_text_count += (
            f"\n✅ Итого: "
            f"{total_orders} заказов"
        )

        send_long_message(
            message.chat.id,
            response_text_count
        )

        file_name = create_excel(
            overdue_orders_by_store,
            sheet_name="Overdue Orders"
        )

        with open(
            file_name,
            'rb'
        ) as file:

            bot.send_document(
                message.chat.id,
                file
            )

        screenshot_filename = (
            create_statistics_screenshot(
                file_name
            )
        )

        with open(
            screenshot_filename,
            'rb'
        ) as img_file:

            bot.send_photo(
                message.chat.id,
                img_file
            )

        if os.path.exists(
            file_name
        ):
            os.remove(
                file_name
            )

        if os.path.exists(
            screenshot_filename
        ):
            os.remove(
                screenshot_filename
            )

    except Exception as e:

        logging.error(
            f"/orders error: {e}"
        )

        bot.send_message(
            message.chat.id,
            f"Произошла ошибка: {e}"
        )


# ============================================================
# /PENDING_ORDERS
# ============================================================

@bot.message_handler(
    commands=[
        'pending_orders'
    ]
)
def fetch_pending_orders(
    message
):

    try:

        bot.send_message(
            message.chat.id,
            "🔄 Получение списка заказов, "
            "ожидающих передачи курьеру..."
        )

        pending_orders_by_store = (
            get_pending_orders()
        )

        if not pending_orders_by_store:

            bot.send_message(
                message.chat.id,
                "❌ Нет заказов, "
                "ожидающих передачи курьеру."
            )

            return

        response_text_orders = (
            "📦 Заказы, ожидающие "
            "передачи курьеру:\n\n"
        )

        for store, orders in (
            pending_orders_by_store.items()
        ):

            response_text_orders += (
                f"Магазин {store}:\n"
            )

            for order_code in orders:

                response_text_orders += (
                    f"  🔸 Номер заказа: "
                    f"{order_code}\n"
                )

            response_text_orders += "\n"

        send_long_message(
            message.chat.id,
            response_text_orders
        )

        response_text_count = (
            "📊 Статистика по "
            "ожидающим заказам:\n\n"
        )

        total_orders = 0

        for store, orders in (
            pending_orders_by_store.items()
        ):

            response_text_count += (
                f"{store}: "
                f"{len(orders)} заказов\n"
            )

            total_orders += len(
                orders
            )

        response_text_count += (
            f"\n✅ Итого: "
            f"{total_orders} заказов"
        )

        send_long_message(
            message.chat.id,
            response_text_count
        )

        file_name = create_excel(
            pending_orders_by_store,
            sheet_name="Pending Orders"
        )

        with open(
            file_name,
            'rb'
        ) as file:

            bot.send_document(
                message.chat.id,
                file
            )

        screenshot_filename = (
            create_statistics_screenshot(
                file_name
            )
        )

        with open(
            screenshot_filename,
            'rb'
        ) as img_file:

            bot.send_photo(
                message.chat.id,
                img_file
            )

        if os.path.exists(
            file_name
        ):
            os.remove(
                file_name
            )

        if os.path.exists(
            screenshot_filename
        ):
            os.remove(
                screenshot_filename
            )

    except Exception as e:

        logging.error(
            f"/pending_orders error: {e}"
        )

        bot.send_message(
            message.chat.id,
            f"Произошла ошибка: {e}"
        )


# ============================================================
# /SEND_REPORT
# ============================================================

@bot.message_handler(
    commands=[
        'send_report'
    ]
)
def send_report(
    message
):

    try:

        bot.send_message(
            message.chat.id,
            "🔄 Запуск отчета "
            "по просроченным заказам..."
        )

        overdue_orders_by_store = (
            get_overdue_orders()
        )

        if not overdue_orders_by_store:

            bot.send_message(
                message.chat.id,
                "❌ Нет просроченных заказов."
            )

            return

        file_name = create_excel(
            overdue_orders_by_store,
            sheet_name="Overdue Orders"
        )

        email_body = (
            "Good evening, "
            "There are delayed orders "
            "that were supposed to be "
            "handed over to the courier today."
            "<br><br>"
            "Қайырлы кеш, "
            "Төменде кешіккен тапсырыс саны."
        )

        success = send_email(
            file_name,
            subject="Delayed orders OMS",
            email_body=email_body
        )

        if success:

            bot.send_message(
                message.chat.id,
                "✅ Отчет успешно отправлен "
                "по электронной почте."
            )

        else:

            bot.send_message(
                message.chat.id,
                "❌ Не удалось отправить отчет."
            )

    except Exception as e:

        logging.error(
            f"/send_report error: {e}"
        )

        bot.send_message(
            message.chat.id,
            f"Произошла ошибка: {e}"
        )


# ============================================================
# /SEND_PENDING_REPORT
# ============================================================

@bot.message_handler(
    commands=[
        'send_pending_report'
    ]
)
def send_pending_report(
    message
):

    try:

        bot.send_message(
            message.chat.id,
            "🔄 Запуск отчета "
            "по ожидающим заказам..."
        )

        pending_orders_by_store = (
            get_pending_orders()
        )

        if not pending_orders_by_store:

            bot.send_message(
                message.chat.id,
                "❌ Нет заказов, "
                "ожидающих передачи курьеру."
            )

            return

        file_name = create_excel(
            pending_orders_by_store,
            sheet_name="Pending Orders"
        )

        email_body = (
            "Қайырлы таң, "
            "Төменде бүгін курьерге "
            "жіберілуі керек тапсырыс саны."
            "<br><br>"
            "Good morning, "
            "Attached are all pending orders "
            "for courier handover today."
        )

        success = send_email(
            file_name,
            subject="Pending orders OMS",
            email_body=email_body
        )

        if success:

            bot.send_message(
                message.chat.id,
                "✅ Отчет успешно отправлен "
                "по электронной почте."
            )

        else:

            bot.send_message(
                message.chat.id,
                "❌ Не удалось отправить отчет."
            )

    except Exception as e:

        logging.error(
            f"/send_pending_report error: {e}"
        )

        bot.send_message(
            message.chat.id,
            f"Произошла ошибка: {e}"
        )


# ============================================================
# AUTO OVERDUE REPORT
# ============================================================

def job_overdue():

    try:

        logging.info(
            "Запуск автоотправки "
            "отчета overdue..."
        )

        overdue_orders_by_store = (
            get_overdue_orders()
        )

        if not overdue_orders_by_store:

            logging.info(
                "Нет overdue заказов."
            )

            return

        file_name = create_excel(
            overdue_orders_by_store,
            sheet_name="Overdue Orders"
        )

        email_body = (
            "Good evening, "
            "There are delayed orders "
            "that were supposed to be "
            "handed over to the courier today."
            "<br><br>"
            "Қайырлы кеш, "
            "Төменде кешіккен тапсырыс саны."
        )

        send_email(
            file_name,
            subject="Delayed orders OMS",
            email_body=email_body
        )

    except Exception as e:

        logging.error(
            f"job_overdue error: {e}"
        )


# ============================================================
# AUTO PENDING REPORT
# ============================================================

def job_pending():

    try:

        logging.info(
            "Запуск автоотправки "
            "pending отчета..."
        )

        pending_orders_by_store = (
            get_pending_orders()
        )

        if not pending_orders_by_store:

            logging.info(
                "Нет pending заказов."
            )

            return

        file_name = create_excel(
            pending_orders_by_store,
            sheet_name="Pending Orders"
        )

        email_body = (
            "Қайырлы таң, "
            "Төменде бүгін курьерге "
            "жіберілуі керек тапсырыс саны."
            "<br><br>"
            "Good morning, "
            "Attached are all pending orders "
            "for courier handover today."
        )

        send_email(
            file_name,
            subject="Pending orders OMS",
            email_body=email_body
        )

    except Exception as e:

        logging.error(
            f"job_pending error: {e}"
        )


# ============================================================
# SCHEDULE
# ============================================================

schedule.every().day.at(
    "14:59"
).do(
    job_overdue
)

schedule.every().day.at(
    "03:59"
).do(
    job_pending
)


def run_scheduler():

    while True:

        try:

            schedule.run_pending()

            time.sleep(
                1
            )

        except Exception as e:

            logging.error(
                f"Scheduler error: {e}"
            )

            time.sleep(
                15
            )


scheduler_thread = threading.Thread(
    target=run_scheduler,
    daemon=True
)

scheduler_thread.start()


# ============================================================
# FLASK
# ============================================================

app = Flask(
    __name__
)


@app.route(
    '/' + API_KEY,
    methods=[
        'POST'
    ]
)
def webhook():

    update = (
        telebot.types.Update.de_json(
            request.stream
            .read()
            .decode(
                'utf-8'
            )
        )
    )

    bot.process_new_updates(
        [
            update
        ]
    )

    return 'ok', 200


@app.route('/')
def index():

    return (
        'NBOT is running'
    )


# ============================================================
# START
# ============================================================

if __name__ == '__main__':

    try:

        bot.remove_webhook()

        bot.set_webhook(
            url=(
                f"https://nbot-n94j.onrender.com/"
                f"{API_KEY}"
            )
        )

        port = int(
            os.environ.get(
                'PORT',
                5000
            )
        )

        app.run(
            host='0.0.0.0',
            port=port
        )

    except Exception as e:

        logging.error(
            f"Main loop error: {e}"
        )
