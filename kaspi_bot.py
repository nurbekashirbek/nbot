import os
import requests
import logging
from datetime import datetime, timedelta, timezone
import telebot
import schedule
import time
import openpyxl
import matplotlib
matplotlib.use('Agg')  # Устанавливаем не-GUI бэкенд перед импортом pyplot
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
API_KEY = os.getenv('TELEGRAM_API_KEY')  # Используем переменную окружения
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

def send_long_message(chat_id, message):
    max_message_length = 4096
    while len(message) > max_message_length:
        bot.send_message(chat_id, message[:max_message_length])
        message = message[max_message_length:]
    bot.send_message(chat_id, message)

# Функция для получения диапазона дат
def get_date_range():
    today = datetime.now(UTC_PLUS_5)  # Текущее время в UTC+5
    start_date = today - timedelta(days=14)  # За последние 14 дней
    return start_date, today

# Функция для получения просроченных заказов
def get_overdue_orders():
    try:
        start_date, today = get_date_range()

        # Устанавливаем время 23:00 для текущей даты
        cutoff_time = today.replace(hour=23, minute=0, second=0, microsecond=0)

        params = {
            'page[number]': 0,  # Начинаем с первой страницы
            'page[size]': 100,  # Устанавливаем максимальное количество элементов на странице
            'filter[orders][creationDate][$ge]': int(start_date.timestamp() * 1000),
            'filter[orders][creationDate][$le]': int(today.timestamp() * 1000),
            'filter[orders][status]': 'ACCEPTED_BY_MERCHANT',
            'filter[orders][state]': 'KASPI_DELIVERY'
        }

        headers = {
            'X-Auth-Token': os.getenv('KASPI_AUTH_TOKEN'),  # Используем переменную окружения
            'User-Agent': 'PostmanRuntime/7.32.0',
            'Accept': 'application/vnd.api+json;charset=UTF-8',
            'Connection': 'keep-alive'
        }

        logging.info("Отправка запроса к API Kaspi...")
        logging.info(f"URL: {API_URL}")
        logging.info(f"Параметры: {params}")

        overdue_orders_by_store = {}  # Сюда будем собирать просроченные заказы по магазинам
        page_number = 0  # Начинаем с первой страницы

        while True:
            params['page[number]'] = page_number  # Устанавливаем номер текущей страницы
            response = requests.get(API_URL, params=params, headers=headers)

            logging.info(f'Ответ API: {response.status_code}')

            if response.status_code != 200:
                logging.error(f"Ошибка API: {response.status_code} - {response.text}")
                return None

            data = response.json()

            if 'data' not in data or not data['data']:
                logging.info("Нет данных на текущей странице")
                break  # Прерываем цикл, если данных на странице нет

            # Логируем количество заказов на текущей странице
            logging.info(f"На странице {page_number} заказов: {len(data['data'])}")

            # Обрабатываем данные с текущей страницы
            for order in data['data']:
                order_code = order['attributes'].get('code', 'Нет номера заказа')  # Используем 'code' для номера заказа
                pickup_point = order['attributes'].get('pickupPointId', 'Неизвестный магазин')  # Получаем магазин
                courier_transmission_planning_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionPlanningDate')
                courier_transmission_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionDate')

                # Фильтрация по плановой дате передачи курьеру и проверка на просрочку
                if courier_transmission_planning_date:
                    planned_date = datetime.fromtimestamp(courier_transmission_planning_date / 1000, tz=UTC_PLUS_5)
                    # Условие: плановая дата прошла ИЛИ плановая дата равна текущей дате и время меньше 23:00
                    if (planned_date < today) or (planned_date.date() == today.date() and planned_date < cutoff_time):
                        # Если фактическая дата передачи курьеру ещё не заполнена
                        if courier_transmission_date is None:
                            if pickup_point not in overdue_orders_by_store:
                                overdue_orders_by_store[pickup_point] = []
                            overdue_orders_by_store[pickup_point].append(order_code)

            # Проверяем, есть ли данные на следующей странице
            if len(data['data']) < params['page[size]']:  # Если заказов на странице меньше, чем размер страницы, то данных больше нет
                break  # Выходим из цикла, так как это последняя страница
            else:
                page_number += 1  # Переходим к следующей странице

        logging.info(f"Найдено просроченных заказов: {sum(len(orders) for orders in overdue_orders_by_store.values())}")
        return overdue_orders_by_store

    except Exception as e:
        logging.error(f"Ошибка при запросе к API: {e}")
        return None

# Функция для получения заказов, ожидающих передачи курьеру
def get_pending_orders():
    try:
        start_date, today = get_date_range()  # Получаем диапазон 14 дней

        # Устанавливаем начало и конец текущего дня
        start_of_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = today.replace(hour=23, minute=59, second=59, microsecond=0)

        params = {
            'page[number]': 0,  # Начинаем с первой страницы
            'page[size]': 100,  # Устанавливаем максимальное количество элементов на странице
            'filter[orders][creationDate][$ge]': int(start_date.timestamp() * 1000),
            'filter[orders][creationDate][$le]': int(today.timestamp() * 1000),
            'filter[orders][status]': 'ACCEPTED_BY_MERCHANT',
            'filter[orders][state]': 'KASPI_DELIVERY'
        }

        headers = {
            'X-Auth-Token': os.getenv('KASPI_AUTH_TOKEN'),  # Используем переменную окружения
            'User-Agent': 'PostmanRuntime/7.32.0',
            'Accept': 'application/vnd.api+json;charset=UTF-8',
            'Connection': 'keep-alive'
        }

        logging.info("Отправка запроса к API Kaspi для получения ожидающих заказов...")
        logging.info(f"URL: {API_URL}")
        logging.info(f"Параметры: {params}")

        pending_orders_by_store = {}  # Сюда будем собирать заказы, ожидающие передачи
        page_number = 0  # Начинаем с первой страницы

        while True:
            params['page[number]'] = page_number  # Устанавливаем номер текущей страницы
            response = requests.get(API_URL, params=params, headers=headers)

            logging.info(f'Ответ API: {response.status_code}')

            if response.status_code != 200:
                logging.error(f"Ошибка API: {response.status_code} - {response.text}")
                return None

            data = response.json()

            if 'data' not in data or not data['data']:
                logging.info("Нет данных на текущей странице")
                break  # Прерываем цикл, если данных на странице нет

            # Логируем количество заказов на текущей странице
            logging.info(f"На странице {page_number} заказов: {len(data['data'])}")

            # Обрабатываем данные с текущей страницы
            for order in data['data']:
                order_code = order['attributes'].get('code', 'Нет номера заказа')  # Используем 'code' для номера заказа
                pickup_point = order['attributes'].get('pickupPointId', 'Неизвестный магазин')  # Получаем магазин
                courier_transmission_planning_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionPlanningDate')
                courier_transmission_date = order['attributes'].get('kaspiDelivery', {}).get('courierTransmissionDate')

                # Фильтрация:
                if courier_transmission_planning_date:
                    planned_date = datetime.fromtimestamp(courier_transmission_planning_date / 1000, tz=UTC_PLUS_5)
                    # Проверяем, что planned_date находится в диапазоне 14 дней и в пределах текущего дня
                    if start_date <= planned_date <= end_of_day and courier_transmission_date is None:
                        if pickup_point not in pending_orders_by_store:
                            pending_orders_by_store[pickup_point] = []
                        pending_orders_by_store[pickup_point].append(order_code)

            # Проверяем, есть ли данные на следующей странице
            if len(data['data']) < params['page[size]']:  # Если заказов на странице меньше, чем размер страницы, то данных больше нет
                break  # Выходим из цикла, так как это последняя страница
            else:
                page_number += 1  # Переходим к следующей странице

        logging.info(f"Найдено заказов, ожидающих передачи: {sum(len(orders) for orders in pending_orders_by_store.values())}")
        return pending_orders_by_store

    except Exception as e:
        logging.error(f"Ошибка при запросе к API: {e}")
        return None

# Функция для создания Excel файла
def create_excel(orders_by_store, sheet_name="Orders"):
    wb = openpyxl.Workbook()
    
    # Создаем лист с деталями заказов
    ws1 = wb.active
    ws1.title = sheet_name
    ws1.append(["Store", "Order Number"])

    for store, orders in orders_by_store.items():
        for order_code in orders:
            ws1.append([store, order_code])

    # Создаем лист с количеством заказов по магазинам
    ws2 = wb.create_sheet("Statistics")
    ws2.append(["Store", "Number of Orders"])
    
    total_orders = 0  # Общее количество заказов
    for store, orders in orders_by_store.items():
        ws2.append([store, len(orders)])
        total_orders += len(orders)  # Считаем общее количество

    # Добавляем итоговую строку
    ws2.append(["Итого", total_orders])

    # Сохраняем файл
    file_name = f"{sheet_name.lower()}_orders_{datetime.now(UTC_PLUS_5).strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb.save(file_name)

    return file_name

# Функция для создания скриншота таблицы
def create_table_screenshot(df, filename):
    # Настройка размера фигуры в зависимости от количества строк
    fig, ax = plt.subplots(figsize=(7, max(2, len(df) * 0.4)))  # Ширина 7, высота зависит от количества строк
    ax.axis('off')  # Отключаем оси

    # Создаем таблицу
    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='center',
        loc='center'
    )
    table.auto_set_font_size(False)  # Отключаем автоматический подбор размера шрифта
    table.set_fontsize(12)  # Устанавливаем размер шрифта
    table.scale(1, 1.5)  # Масштабируем таблицу (ширина, высота)

    # Убираем лишние отступы
    plt.tight_layout()

    # Сохраняем изображение с минимальными отступами
    plt.savefig(filename, bbox_inches='tight', pad_inches=0.1)  # pad_inches=0.1 убирает отступы
    plt.close()

# Функция для создания скриншота листа "Statistics"
def create_statistics_screenshot(file_name):
    # Чтение данных из листа "Statistics"
    df = pd.read_excel(file_name, sheet_name="Statistics")
    
    # Создание скриншота и сохранение в файл
    screenshot_filename = f"statistics_screenshot_{datetime.now(UTC_PLUS_5).strftime('%Y%m%d_%H%M%S')}.png"
    create_table_screenshot(df, screenshot_filename)
    
    return screenshot_filename

# Функция для отправки email
def send_email(file_name, subject, email_body):
    try:
        from_email = os.getenv('EMAIL_FROM')  # Используем переменную окружения
        to_email = os.getenv('EMAIL_TO').split(',')  # Используем переменную окружения
        cc_emails = os.getenv('EMAIL_CC').split(',')  # Используем переменную окружения

        # Создаем скриншот листа "Statistics"
        screenshot_filename = create_statistics_screenshot(file_name)
        
        # Чтение скриншота в base64 для вставки в тело письма
        with open(screenshot_filename, "rb") as img_file:
            img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
        
        # Создаем письмо
        msg = MIMEMultipart('alternative')
        msg['From'] = f'Nurbek ASHIRBEK <{from_email}>'
        msg['To'] = ', '.join(to_email)  # Преобразуем список в строку
        msg['Cc'] = ', '.join(cc_emails)  # Преобразуем список в строку
        msg['Subject'] = subject

        # HTML с изображением
        html_body = f'''
        <html>
            <body>
                <p>{email_body}</p>
                <img src="data:image/png;base64,{img_base64}" alt="Statistics Table" style="width: 100%; max-width: 500px;" />
                <p>С уважением,</p>
            </body>
        </html>
        '''
        msg.attach(MIMEText(html_body, 'html'))

        # Прикрепляем Excel-файл
        with open(file_name, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype="xlsx")
            attachment.add_header('Content-Disposition', 'attachment', filename=file_name)
            msg.attach(attachment)

        # Прикрепляем скриншот как вложение
        with open(screenshot_filename, 'rb') as img_file:
            img_attachment = MIMEApplication(img_file.read(), _subtype="png")
            img_attachment.add_header('Content-Disposition', 'attachment', filename=screenshot_filename)
            msg.attach(img_attachment)

        # Отправка письма
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(from_email, os.getenv('EMAIL_PASSWORD'))  # Используем переменную окружения

        all_recipients = to_email + cc_emails  # Объединяем списки
        server.sendmail(from_email, all_recipients, msg.as_string())
        server.quit()

        # Удаляем временные файлы
        os.remove(screenshot_filename)
        os.remove(file_name)

        logging.info('Email sent successfully with the embedded statistics table screenshot and attachment.')
    except Exception as e:
        logging.error(f'Error sending email: {e}')

# Обработка команды /orders
@bot.message_handler(commands=['orders'])
def fetch_orders(message):
    try:
        bot.send_message(message.chat.id, '🔄 Получение списка просроченных заказов...')

        overdue_orders_by_store = get_overdue_orders()
        
        if not overdue_orders_by_store:
            bot.send_message(message.chat.id, '❌ Нет просроченных заказов за указанный период.')
            return

        # Формируем первое сообщение с номерами заказов
        response_text_orders = f'📦 Задержанные заказы по магазинам:\n\n'
        for store, orders in overdue_orders_by_store.items():
            response_text_orders += f'Магазин {store}:\n'
            for order_code in orders:
                response_text_orders += f'  🔸 Номер заказа: {order_code}\n'

        # Отправляем сообщение с номерами заказов
        send_long_message(message.chat.id, response_text_orders)

        # Формируем второе сообщение с количеством заказов
        response_text_count = '📊 Статистика по задержанным заказам:\n\n'
        total_orders = 0  # Общее количество заказов
        for store, orders in overdue_orders_by_store.items():
            response_text_count += f'{store}: {len(orders)} заказов\n'
            total_orders += len(orders)  # Считаем общее количество

        # Добавляем итоговую строку с общим количеством заказов
        response_text_count += f'\n✅ Итого: {total_orders} заказов'

        # Отправляем сообщение с количеством заказов
        send_long_message(message.chat.id, response_text_count)

        # Создаем Excel-файл
        file_name = create_excel(overdue_orders_by_store, sheet_name="Overdue Orders")
        
        # Отправляем Excel-файл
        with open(file_name, 'rb') as file:
            bot.send_document(message.chat.id, file)

        # Создаем скриншот листа "Statistics"
        screenshot_filename = create_statistics_screenshot(file_name)
        
        # Отправляем скриншот
        with open(screenshot_filename, 'rb') as img_file:
            bot.send_photo(message.chat.id, img_file)

        # Удаляем временные файлы
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

        # Формируем первое сообщение с номерами заказов
        response_text_orders = f'📦 Заказы, ожидающие передачи курьеру, по магазинам:\n\n'
        for store, orders in pending_orders_by_store.items():
            response_text_orders += f'Магазин {store}:\n'
            for order_code in orders:
                response_text_orders += f'  🔸 Номер заказа: {order_code}\n'

        # Отправляем сообщение с номерами заказов
        send_long_message(message.chat.id, response_text_orders)

        # Формируем второе сообщение с количеством заказов
        response_text_count = '📊 Статистика по заказам, ожидающим передачи:\n\n'
        total_orders = 0  # Общее количество заказов
        for store, orders in pending_orders_by_store.items():
            response_text_count += f'{store}: {len(orders)} заказов\n'
            total_orders += len(orders)  # Считаем общее количество

        # Добавляем итоговую строку с общим количеством заказов
        response_text_count += f'\n✅ Итого: {total_orders} заказов'

        # Отправляем сообщение с количеством заказов
        send_long_message(message.chat.id, response_text_count)

        # Создаем Excel-файл
        file_name = create_excel(pending_orders_by_store, sheet_name="Pending Orders")
        
        # Отправляем Excel-файл
        with open(file_name, 'rb') as file:
            bot.send_document(message.chat.id, file)

        # Создаем скриншот листа "Statistics"
        screenshot_filename = create_statistics_screenshot(file_name)
        
        # Отправляем скриншот
        with open(screenshot_filename, 'rb') as img_file:
            bot.send_photo(message.chat.id, img_file)

        # Удаляем временные файлы
        os.remove(file_name)
        os.remove(screenshot_filename)

    except Exception as e:
        logging.error(f"Ошибка при обработке заказов: {e}")
        bot.send_message(message.chat.id, f'Произошла ошибка: {e}')

# Обработка команды /send_report (отправка рассылки вручную)
@bot.message_handler(commands=['send_report'])
def send_report(message):
    try:
        bot.send_message(message.chat.id, '🔄 Запуск отчета по просроченным заказам...')

        overdue_orders_by_store = get_overdue_orders()
        
        if not overdue_orders_by_store:
            bot.send_message(message.chat.id, '❌ Нет просроченных заказов за указанный период.')
            return

        # Создаем Excel и отправляем его по email
        file_name = create_excel(overdue_orders_by_store, sheet_name="Overdue Orders")
        email_body = (
            "Добрый вечер, остались заказы, которые должны были быть переданы сегодня курьеру. "
            "Прошу вывести эти заказы и передать, это портит статистику своевременных передач."
        )
        send_email(file_name, subject="Задержанные заказы OMS", email_body=email_body)

        bot.send_message(message.chat.id, '✅ Отчет успешно отправлен по электронной почте.')

    except Exception as e:
        logging.error(f"Ошибка при отправке отчета вручную: {e}")
        bot.send_message(message.chat.id, f'Произошла ошибка: {e}')

# Обработка команды /send_pending_report (отправка рассылки вручную)
@bot.message_handler(commands=['send_pending_report'])
def send_pending_report(message):
    try:
        bot.send_message(message.chat.id, '🔄 Запуск отчета по заказам, ожидающим передачи курьеру...')

        pending_orders_by_store = get_pending_orders()
        
        if not pending_orders_by_store:
            bot.send_message(message.chat.id, '❌ Нет заказов, ожидающих передачи курьеру за указанный период.')
            return

        # Создаем Excel и отправляем его по email
        file_name = create_excel(pending_orders_by_store, sheet_name="Pending Orders")
        email_body = (
            "Доброе утро, высылаю количество заказов ожидающих передачи курьеру сегодня!"
            "Прошу вывести все заказы до прихода курьера)"
        )
        send_email(file_name, subject="Ожидающие заказы OMS", email_body=email_body)

        bot.send_message(message.chat.id, '✅ Отчет успешно отправлен по электронной почте.')

    except Exception as e:
        logging.error(f"Ошибка при отправке отчета вручную: {e}")
        bot.send_message(message.chat.id, f'Произошла ошибка: {e}')

# Авторассылка в 6 вечера для просроченных заказов
def job_overdue():
    overdue_orders_by_store = get_overdue_orders()
    
    if overdue_orders_by_store:
        file_name = create_excel(overdue_orders_by_store, sheet_name="Overdue Orders")
        email_body = (
            "Добрый вечер, остались заказы, которые должны были быть переданы сегодня курьеру. "
            "Прошу вывести эти заказы и передать, это портит статистику своевременных передач."
        )
        send_email(file_name, subject="Задержанные заказы OMS", email_body=email_body)

# Авторассылка в 10 утра для заказов, ожидающих передачи
def job_pending():
    pending_orders_by_store = get_pending_orders()
    
    if pending_orders_by_store:
        file_name = create_excel(pending_orders_by_store, sheet_name="Pending Orders")
        email_body = (
            "Доброе утро, высылаю количество заказов ожидающих передачи курьеру сегодня!"
            "Прошу вывести все заказы до прихода курьера)"
        )
        send_email(file_name, subject="Ожидающие заказы OMS", email_body=email_body)

# Планирование задач в UTC+5
schedule.every().day.at("12:59").do(job_overdue)  # по UTC+5 
schedule.every().day.at("04:59").do(job_pending)  # по UTC+5

def run_scheduler():
    while True:
        try:
            schedule.run_pending()  # Проверяет, нужно ли выполнить задачи
            time.sleep(1)  # Пауза в 1 секунду, чтобы не нагружать процессор
        except Exception as e:
            logging.error(f"Ошибка в планировщике: {e}")
            time.sleep(15)  # Если ошибка, ждем 15 секунд перед новой попыткой

# Запуск планировщика в отдельном потоке
scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.daemon = True  # Поток завершится, если основной поток завершится
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
        # Устанавливаем вебхук
        bot.remove_webhook()
        bot.set_webhook(url=f'https://nbot-n94j.onrender.com/{API_KEY}')
        
        # Запускаем Flask приложение
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        logging.error(f"Ошибка в основном цикле: {e}")
