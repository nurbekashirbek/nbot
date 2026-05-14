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
from io import BytesIO
import base64
import threading
from telebot.types import BotCommand
from flask import Flask, request

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота
API_KEY = os.getenv('TELEGRAM_API_KEY')
bot = telebot.TeleBot(API_KEY)

# Устанавливаем меню команд
commands = [
    BotCommand('orders', 'Получить список задержанных заказов'),
    BotCommand('pending_orders', 'Получить список заказов, ожидающих передачи'),
    BotCommand('send_report', 'Отправить отчет по задержанным заказам'),
    BotCommand('send_pending_report', 'Отправить отчет по ожидающим заказам')
]

bot.set_my_commands(commands)

# URL для API
API_URL = 'https://kaspi.kz/shop/api/v2/orders'

# Таймзона UTC+5
UTC_PLUS_5 = timezone(timedelta(hours=5))

# Словарь для замены кодов магазинов на читаемые названия
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

def send_long_message(chat_id, message):
    max_message_length = 4096
    while len(message) > max_message_length:
        bot.send_message(chat_id, message[:max_message_length])
        message = message[max_message_length:]
    bot.send_message(chat_id, message)

# Функция для получения диапазона дат
def get_date_range():
    today = datetime.now(UTC_PLUS_5)
    start_date = today - timedelta(days=14)
    return start_date, today

# Функция для получения просроченных заказов с повторными попытками
def get_overdue_orders():
    try:
        start_date, today = get_date_range()
        cutoff_time = today.replace(hour=23, minute=0, second=0, microsecond=0)

        params = {
            'page[number]': 0,
            'page[size]': 100,
            'filter[orders][creationDate][$ge]': int(start_date.timestamp() * 1000),
            'filter[orders][creationDate][$le]': int(today.timestamp() * 1000),
            'filter[orders][status]': 'ACCEPTED_BY_MERCHANT',
            'filter[orders][state]': 'KASPI_DELIVERY'
        }

        headers = {
            'X-Auth-Token': os.getenv('KASPI_AUTH_TOKEN'),
            'User-Agent': 'PostmanRuntime/7.32.0',
            'Accept': 'application/vnd.api+json;charset=UTF-8',
            'Connection': 'keep-alive'
        }

        logging.info("Отправка запроса к API Kaspi...")
        logging.info(f"URL: {API_URL}")
        logging.info(f"Параметры: {params}")

        overdue_orders_by_store = {}
        page_number = 0

        while True:
            params['page[number]'] = page_number
            max_attempts = 2
            attempt = 1

            while attempt <= max_attempts:
                try:
                    response = requests.get(API_URL, params=params, headers=headers)
                    logging.info(f'Ответ API: {response.status_code}')
                    response.raise_for_status()
                    break
                except requests.exceptions.ConnectionError as e:
                    logging.error(f"Попытка {attempt}: Ошибка соединения: {e}")
                    if attempt == max_attempts:
                        logging.error("Достигнуто максимальное количество попыток. Прерываем.")
                        return None
                    attempt += 1
                    time.sleep(5)

            data = response.json()

            if 'data' not in data or not data['data']:
                logging.info("Нет данных на текущей странице")
                break

            logging.info(f"На странице {page_number} заказов: {len(data['data'])}")

            for order in data['data']:
                order_code = order['attributes'].get('code', 'Нет номера заказа')
                pickup_point = order['attributes'].get('pickupPointId', 'Неизвестный магазин')
                pickup_point = store_mapping.get(pickup_point, pickup_point)
                courier_transmission_planning_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionPlanningDate')
                courier_transmission_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionDate')

                if courier_transmission_planning_date:
                    planned_date = datetime.fromtimestamp(courier_transmission_planning_date / 1000, tz=UTC_PLUS_5)
                    if (planned_date < today) or (planned_date.date() == today.date() and planned_date < cutoff_time):
                        if courier_transmission_date is None:
                            if pickup_point not in overdue_orders_by_store:
                                overdue_orders_by_store[pickup_point] = []
                            overdue_orders_by_store[pickup_point].append(order_code)

            if len(data['data']) < params['page[size]']:
                break
            else:
                page_number += 1

        logging.info(f"Найдено просроченных заказов: {sum(len(orders) for orders in overdue_orders_by_store.values())}")
        return overdue_orders_by_store

    except Exception as e:
        logging.error(f"Ошибка при запросе к API: {e}")
        return None

# Функция для получения заказов, ожидающих передачи, с повторными попытками
def get_pending_orders():
    try:
        start_date, today = get_date_range()
        start_of_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = today.replace(hour=23, minute=59, second=59, microsecond=0)

        params = {
            'page[number]': 0,
            'page[size]': 100,
            'filter[orders][creationDate][$ge]': int(start_date.timestamp() * 1000),
            'filter[orders][creationDate][$le]': int(today.timestamp() * 1000),
            'filter[orders][status]': 'ACCEPTED_BY_MERCHANT',
            'filter[orders][state]': 'KASPI_DELIVERY'
        }

        
        headers = {
            'X-Auth-Token': os.getenv('KASPI_AUTH_TOKEN'),
            'User-Agent': 'PostmanRuntime/7.32.0',
            'Accept': 'application/vnd.api+json;charset=UTF-8',
            'Connection': 'keep-alive'
        }

        logging.info("Отправка запроса к API Kaspi для получения ожидающих заказов...")
        logging.info(f"URL: {API_URL}")
        logging.info(f"Параметры: {params}")

        pending_orders_by_store = {}
        page_number = 0

        while True:
            params['page[number]'] = page_number
            max_attempts = 2
            attempt = 1

            while attempt <= max_attempts:
                try:
                    response = requests.get(API_URL, params=params, headers=headers)
                    logging.info(f'Ответ API: {response.status_code}')
                    response.raise_for_status()
                    break
                except requests.exceptions.ConnectionError as e:
                    logging.error(f"Попытка {attempt}: Ошибка соединения: {e}")
                    if attempt == max_attempts:
                        logging.error("Достигнуто максимальное количество попыток. Прерываем.")
                        return None
                    attempt += 1
                    time.sleep(5)

            data = response.json()

            if 'data' not in data or not data['data']:
                logging.info("Нет данных на текущей странице")
                break

            logging.info(f"На странице {page_number} заказов: {len(data['data'])}")

            for order in data['data']:
                order_code = order['attributes'].get('code', 'Нет номера заказа')
                pickup_point = order['attributes'].get('pickupPointId', 'Неизвестный магазин')
                pickup_point = store_mapping.get(pickup_point, pickup_point)
                courier_transmission_planning_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionPlanningDate')
                courier_transmission_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionDate')

                # Для точки 9041 (Almaty Warehouse) берем все заказы без фильтра по дате планируемой передачи
                if pickup_point == store_mapping.get("14576033_9041", "Almaty Warehouse"):
                    if courier_transmission_date is None:
                        if pickup_point not in pending_orders_by_store:
                            pending_orders_by_store[pickup_point] = []
                        pending_orders_by_store[pickup_point].append(order_code)
                else:
                    # Для остальных точек фильтруем по дате планируемой передачи на сегодня
                    if courier_transmission_planning_date:
                        planned_date = datetime.fromtimestamp(courier_transmission_planning_date / 1000, tz=UTC_PLUS_5)
                        if start_of_day <= planned_date <= end_of_day and courier_transmission_date is None:
                            if pickup_point not in pending_orders_by_store:
                                pending_orders_by_store[pickup_point] = []
                            pending_orders_by_store[pickup_point].append(order_code)

            if len(data['data']) < params['page[size]']: 
                break
            else:
                page_number += 1

        logging.info(f"Найдено заказов, ожидающих передачи: {sum(len(orders) for orders in pending_orders_by_store.values())}")
        return pending_orders_by_store

    except Exception as e:
        logging.error(f"Ошибка при запросе к API: {e}")
        return None

# Функция для создания Excel файла
def create_excel(orders_by_store, sheet_name="Orders"):
    wb = openpyxl.Workbook()
    
    ws1 = wb.active
    ws1.title = sheet_name
    ws1.append(["Store", "Order Number"])

    for store, orders in orders_by_store.items():
        for order_code in orders:
            ws1.append([store, order_code])

    ws2 = wb.create_sheet("Statistics")
    ws2.append(["Store", "Number of Orders"])
    
    total_orders = 0
    for store, orders in orders_by_store.items():
        ws2.append([store, len(orders)])
        total_orders += len(orders)

    ws2.append(["Итого", total_orders])

    file_name = f"{sheet_name.lower()}_orders_{datetime.now(UTC_PLUS_5).strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(file_name)

    return file_name

# Функция для создания скриншота таблицы
def create_table_screenshot(df, filename):
    fig, ax = plt.subplots(figsize=(7, max(2, len(df) * 0.4)))
    ax.axis('off')

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='center',
        loc='center'
    )
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 1.5)

    plt.tight_layout()
    plt.savefig(filename, bbox_inches='tight', pad_inches=0.1)
    plt.close()

# Функция для создания скриншота листа "Statistics"
def create_statistics_screenshot(file_name):
    df = pd.read_excel(file_name, sheet_name="Statistics")
    screenshot_filename = f"statistics_screenshot_{datetime.now(UTC_PLUS_5).strftime('%Y%m%d_%H%M%S')}.png"
    create_table_screenshot(df, screenshot_filename)
    return screenshot_filename

# Функция для отправки email с повторными попытками
def send_email(file_name, subject, email_body):
    max_attempts = 2
    attempt = 1
    screenshot_filename = None

    while attempt <= max_attempts:
        try:
            from_email = os.getenv('EMAIL_FROM')
            to_email = os.getenv('EMAIL_TO').split(',')
            cc_emails = os.getenv('EMAIL_CC').split(',')

            screenshot_filename = create_statistics_screenshot(file_name)
            
            with open(screenshot_filename, "rb") as img_file:
                img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
            
            msg = MIMEMultipart('alternative')
            msg['From'] = f'Nurbek ASHIRBEK <{from_email}>'
            msg['To'] = ', '.join(to_email)
            msg['Cc'] = ', '.join(cc_emails)
            msg['Subject'] = subject

            html_body = f'''
            <html>
                <body>
                    <p>{email_body}</p>
                    <img src="data:image/png;base64,{img_base64}" alt="Statistics Table" style="width: 100%; max-width: 500px;" />
                    <p style="margin-top: 20px;">С уважением,</p>
                    <p>
                        <span style="color: #FF5733; font-weight: bold; font-size: 22px;">Nurbek ASHIRBEK</span><br>
                        <span style="color: #000000;">E-commerce specialist</span>
                    </p>
                </body>
            </html>
            '''
            msg.attach(MIMEText(html_body, 'html'))

            with open(file_name, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype="xlsx")
                attachment.add_header('Content-Disposition', 'attachment', filename=file_name)
                msg.attach(attachment)

            with open(screenshot_filename, 'rb') as img_file:
                img_attachment = MIMEApplication(img_file.read(), _subtype="png")
                img_attachment.add_header('Content-Disposition', 'attachment', filename=screenshot_filename)
                msg.attach(img_attachment)

            server = smtplib.SMTP('smtp.yandex.com', 587)
            server.starttls()
            server.login(from_email, os.getenv('EMAIL_PASSWORD'))

            all_recipients = to_email + cc_emails
            server.sendmail(from_email, all_recipients, msg.as_string())
            server.quit()

            logging.info('Email sent successfully with the embedded statistics table screenshot and attachment.')
            break

        except (smtplib.SMTPException, ConnectionResetError) as e:
            logging.error(f"Попытка {attempt}: Ошибка отправки email: {e}")
            if attempt == max_attempts:
                logging.error("Достигнуто максимальное количество попыток отправки email. Прерываем.")
                break
            attempt += 1
            time.sleep(10)

        finally:
            if screenshot_filename and os.path.exists(screenshot_filename):
                try:
                    os.remove(screenshot_filename)
                except Exception as e:
                    logging.error(f"Ошибка при удалении файла {screenshot_filename}: {e}")
            if os.path.exists(file_name):
                try:
                    os.remove(file_name)
                except Exception as e:
                    logging.error(f"Ошибка при удалении файла {file_name}: {e}")

# Обработка команды /orders
@bot.message_handler(commands=['orders'])
def fetch_orders(message):
    try:
        bot.send_message(message.chat.id, '🔄 Получение списка просроченных заказов...')

        overdue_orders_by_store = get_overdue_orders()
        
        if not overdue_orders_by_store:
            bot.send_message(message.chat.id, '❌ Нет просроченных заказов за указанный период.')
            return

        response_text_orders = f'📦 Задержанные заказы по магазинам:\n\n'
        for store, orders in overdue_orders_by_store.items():
            response_text_orders += f'Магазин {store}:\n'
            for order_code in orders:
                response_text_orders += f'  🔸 Номер заказа: {order_code}\n'

        send_long_message(message.chat.id, response_text_orders)

        response_text_count = '📊 Статистика по задержанным заказам:\n\n'
        total_orders = 0
        for store, orders in overdue_orders_by_store.items():
            response_text_count += f'{store}: {len(orders)} заказов\n'
            total_orders += len(orders)

        response_text_count += f'\n✅ Итого: {total_orders} заказов'
        send_long_message(message.chat.id, response_text_count)

        file_name = create_excel(overdue_orders_by_store, sheet_name="Overdue Orders")
        
        with open(file_name, 'rb') as file:
            bot.send_document(message.chat.id, file)

        screenshot_filename = create_statistics_screenshot(file_name)
        
        with open(screenshot_filename, 'rb') as img_file:
            bot.send_photo(message.chat.id, img_file)

        os.remove(file_name)
        os.remove(screenshot_filename)

    except Exception as e:
        logging.error(f"Ошибка при обработке заказов: {e}")
        bot.send_message(message.chat.id, f'Произошла ошибка: {e}')

# Обработка команды /pending_orders
@bot.message_handler(commands=['pending_orders'])
def fetch_pending_orders(message):
    try:
        bot.send_message(message.chat.id, '🔄 Получение списка заказов, ожидающих передачи курьеру...')

        pending_orders_by_store = get_pending_orders()
        
        if not pending_orders_by_store:
            bot.send_message(message.chat.id, '❌ Нет заказов, ожидающих передачи курьеру за указанный период.')
            return

        response_text_orders = f'📦 Заказы, ожидающие передачи курьеру, по магазинам:\n\n'
        for store, orders in pending_orders_by_store.items():
            response_text_orders += f'Магазин {store}:\n'
            for order_code in orders:
                response_text_orders += f'  🔸 Номер заказа: {order_code}\n'

        send_long_message(message.chat.id, response_text_orders)

        response_text_count = '📊 Статистика по заказам, ожидающим передачи:\n\n'
        total_orders = 0
        for store, orders in pending_orders_by_store.items():
            response_text_count += f'{store}: {len(orders)} заказов\n'
            total_orders += len(orders)

        response_text_count += f'\n✅ Итого: {total_orders} заказов'
        send_long_message(message.chat.id, response_text_count)

        file_name = create_excel(pending_orders_by_store, sheet_name="Pending Orders")
        
        with open(file_name, 'rb') as file:
            bot.send_document(message.chat.id, file)

        screenshot_filename = create_statistics_screenshot(file_name)
        
        with open(screenshot_filename, 'rb') as img_file:
            bot.send_photo(message.chat.id, img_file)

        os.remove(file_name)
        os.remove(screenshot_filename)

    except Exception as e:
        logging.error(f"Ошибка при обработке заказов: {e}")
        bot.send_message(message.chat.id, f'Произошла ошибка: {e}')

# Обработка команды /send_report
@bot.message_handler(commands=['send_report'])
def send_report(message):
    try:
        bot.send_message(message.chat.id, '🔄 Запуск отчета по просроченным заказам...')

        overdue_orders_by_store = get_overdue_orders()
        
        if not overdue_orders_by_store:
            bot.send_message(message.chat.id, '❌ Нет просроченных заказов за указанный период.')
            return

        file_name = create_excel(overdue_orders_by_store, sheet_name="Overdue Orders")
        email_body = (
            "Good evening, There are delayed orders that were supposed to be handed over to the courier today. "
            "Please find these orders.\n\n"
            "Қайырлы кеш, Төменде кешіккен тапсырыс саны."
        )
        send_email(file_name, subject="Delayed orders OMS", email_body=email_body)

        bot.send_message(message.chat.id, '✅ Отчет успешно отправлен по электронной почте.')

    except Exception as e:
        logging.error(f"Ошибка при отправке отчета вручную: {e}")
        bot.send_message(message.chat.id, f'Произошла ошибка: {e}')

# Обработка команды /send_pending_report
@bot.message_handler(commands=['send_pending_report'])
def send_pending_report(message):
    try:
        bot.send_message(message.chat.id, '🔄 Запуск отчета по заказам, ожидающим передачи курьеру...')

        pending_orders_by_store = get_pending_orders()
        
        if not pending_orders_by_store:
            bot.send_message(message.chat.id, '❌ Нет заказов, ожидающих передачи курьеру за указанный период.')
            return

        file_name = create_excel(pending_orders_by_store, sheet_name="Pending Orders")
        email_body = (
            "Қайырлы таң, Төменде бүгін курьерге жіберілуі керек тапсырыс саны.\n\n"
            "Good morning, Attached are all the pending orders for courier handover today."
        )
        send_email(file_name, subject="Pending orders OMS", email_body=email_body)

        bot.send_message(message.chat.id, '✅ Отчет успешно отправлен по электронной почте.')

    except Exception as e:
        logging.error(f"Ошибка при отправке отчета вручную: {e}")
        bot.send_message(message.chat.id, f'Произошла ошибка: {e}')

# Авторассылка в 6 вечера для просроченных заказов с обработкой ошибок
def job_overdue():
    try:
        logging.info("Запуск автоотправки отчета по просроченным заказам...")
        overdue_orders_by_store = get_overdue_orders()
        
        if not overdue_orders_by_store:
            logging.info("Нет просроченных заказов для автоотправки.")
            return

        file_name = create_excel(overdue_orders_by_store, sheet_name="Overdue Orders")
        email_body = (
            "Good evening, There are delayed orders that were supposed to be handed over to the courier today. "
            "Please find these orders.\n\n"
            "Қайырлы кеш, Төменде кешіккен тапсырыс саны."
        )
        send_email(file_name, subject="Delayed orders OMS", email_body=email_body)
        logging.info("Автоотправка отчета по просроченным заказам завершена.")

    except Exception as e:
        logging.error(f"Ошибка при автоотправке отчета по просроченным заказам: {e}")

# Авторассылка в 10 утра для заказов, ожидающих передачи с обработкой ошибок
def job_pending():
    try:
        logging.info("Запуск автоотправки отчета по ожидающим заказам...")
        pending_orders_by_store = get_pending_orders()
        
        if not pending_orders_by_store:
            logging.info("Нет заказов, ожидающих передачи, для автоотправки.")
            return

        file_name = create_excel(pending_orders_by_store, sheet_name="Pending Orders")
        email_body = (
            "Қайырлы таң, Төменде бүгін курьерге жіберілуі керек тапсырыс саны.\n\n"
            "Good morning, Attached are all the pending orders for courier handover today."
        )
        send_email(file_name, subject="Pending orders OMS", email_body=email_body)
        logging.info("Автоотправка отчета по ожидающим заказам завершена.")

    except Exception as e:
        logging.error(f"Ошибка при автоотправке отчета по ожидающим заказам: {e}")

# Планирование задач в UTC+5
schedule.every().day.at("14:59").do(job_overdue)
schedule.every().day.at("03:59").do(job_pending)

def run_scheduler():
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка в планировщике: {e}")
            time.sleep(15)

scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.daemon = True
scheduler_thread.start()

# Инициализация Flask приложения
app = Flask(__name__)

@app.route('/' + API_KEY, methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.stream.read().decode('utf-8'))
    bot.process_new_updates([update])
    return 'ok', 200

@app.route('/')
def index():
    return 'Hello, World!'

# Запуск бота
if __name__ == '__main__':
    try:
        bot.remove_webhook()
        bot.set_webhook(url=f'https://nbot-n94j.onrender.com/{API_KEY}')
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        logging.error(f"Ошибка в основном цикле: {e}")





