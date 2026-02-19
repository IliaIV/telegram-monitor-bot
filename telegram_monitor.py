"""
Версия: 3.2.0-Render (Релиз от 19.02.2026)
Изменения:
- Исправлена ошибка invalid_grant при аутентификации Google
- Улучшена обработка JSON из переменной окружения
- Добавлена функция для очистки JSON от лишних символов
"""

import asyncio
import datetime
import os
import io
import sys
import logging
import traceback
import json
import re
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient, events
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from aiohttp import web

# ============ ФУНКЦИЯ ДЛЯ МОСКОВСКОГО ВРЕМЕНИ ============
def get_moscow_time():
    """Возвращает текущее время в часовом поясе Москвы (UTC+3)"""
    utc_time = datetime.now(timezone.utc)
    moscow_time = utc_time + timedelta(hours=3)
    return moscow_time

def get_moscow_date_str():
    """Возвращает текущую дату в формате ДД.ММ.ГГГГ (московское время)"""
    return get_moscow_time().strftime("%d.%m.%Y")

def get_moscow_time_str():
    """Возвращает текущее время в формате ЧЧ:ММ (московское время)"""
    return get_moscow_time().strftime("%H:%M")

def get_moscow_datetime_str():
    """Возвращает дату и время для логов (московское время)"""
    return get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")

# ============ НАСТРОЙКА ЛОГИРОВАНИЯ ============
# Создаем папку для логов если её нет
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Формируем имя файла лога с датой и временем (московское)
log_filename = os.path.join(log_dir, f'bot_{get_moscow_time().strftime("%Y%m%d_%H%M%S")}.log')

# Кастомный форматтер для логов с московским временем
class MoscowTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        moscow_dt = dt + timedelta(hours=3)
        if datefmt:
            return moscow_dt.strftime(datefmt)
        return moscow_dt.strftime("%Y-%m-%d %H:%M:%S")

# Настраиваем логирование
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(MoscowTimeFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(MoscowTimeFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, handler]
)

# ============ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ============
BOT_TOKEN = os.environ.get('BOT_TOKEN', "8470567669:AAHfluXsWl38wjRRkzj8MT2m4UYHl-J2NbA")
API_ID = int(os.environ.get('API_ID', '20202213'))
API_HASH = os.environ.get('API_HASH', '1d010061c439082c0d77d1aa7ed95830')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', "1sMvl_M1EnQ14e1AzV5x_HUfuJoR7ZL0ijlTY0mWCv88")
SHEET_NAME = os.environ.get('SHEET_NAME', "ТТ-02.26")
DRIVE_ROOT_FOLDER_ID = os.environ.get('DRIVE_ROOT_FOLDER_ID', "1nO2L3HqshwZ7NzrGsrpuQf8ugBV43Za6")

# Telegram группы
CHAT_IDS_STR = os.environ.get('CHAT_IDS', "-1003849809374,-1003741393561")
CHAT_IDS = [int(x.strip()) for x in CHAT_IDS_STR.split(',')]

# ============ ФУНКЦИЯ ДЛЯ ОЧИСТКИ JSON ============
def clean_json_string(json_str):
    """Очищает JSON строку от лишних символов и пробелов"""
    if not json_str:
        return None
    
    # Удаляем лишние пробелы в начале и конце
    json_str = json_str.strip()
    
    # Удаляем BOM если есть
    if json_str.startswith('\ufeff'):
        json_str = json_str[1:]
    
    # Заменяем одинарные кавычки на двойные если нужно
    if json_str.startswith("'") and json_str.endswith("'"):
        json_str = json_str[1:-1]
    
    # Удаляем лишние пробелы между ключами и значениями
    json_str = re.sub(r'\s+', ' ', json_str)
    
    return json_str

# ============ ЗАГРУЗКА CREDENTIALS ============
SERVICE_ACCOUNT_FILE = None
SERVICE_ACCOUNT_JSON = os.environ.get('SERVICE_ACCOUNT_JSON')

if SERVICE_ACCOUNT_JSON:
    try:
        # Очищаем JSON от возможных проблем
        cleaned_json = clean_json_string(SERVICE_ACCOUNT_JSON)
        
        # Пробуем распарсить JSON для проверки
        json_data = json.loads(cleaned_json)
        log_info("[OK] JSON валидный, сохраняем во временный файл")
        
        # Сохраняем во временный файл
        SERVICE_ACCOUNT_FILE = '/tmp/credentials.json'
        with open(SERVICE_ACCOUNT_FILE, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2)
        
        log_info(f"[OK] Файл сохранен: {SERVICE_ACCOUNT_FILE}")
        
    except json.JSONDecodeError as e:
        log_error(f"Ошибка парсинга JSON: {e}")
        log_error(f"Проблемный JSON: {SERVICE_ACCOUNT_JSON[:200]}...")
        SERVICE_ACCOUNT_FILE = None
    except Exception as e:
        log_error(f"Неизвестная ошибка при обработке JSON: {e}")
        SERVICE_ACCOUNT_FILE = None
else:
    # Если нет переменной окружения, ищем локальный файл
    local_file = os.path.join(os.path.dirname(__file__), 'credentials.json')
    if os.path.exists(local_file):
        SERVICE_ACCOUNT_FILE = local_file
        log_info(f"[OK] Используем локальный файл: {local_file}")
    else:
        log_error("Нет SERVICE_ACCOUNT_JSON и нет локального credentials.json")
        SERVICE_ACCOUNT_FILE = None

# ======================================

# === КОНСТАНТЫ ДЛЯ ИНДЕКСОВ КОЛОНОК ===
COL = {
    'CHECKBOX': 1, 'DATE_OPENED': 2, 'TIME_OPENED': 3, 'DATE_CLOSED': 4,
    'TIME_CLOSED': 5, 'DURATION': 6, 'TT': 7, 'DISTRICT': 8, 'ADDRESS': 9,
    'STATUS': 10, 'REASON_CLOSED': 11, 'DELETE_FLAG': 12, 'COMMENT': 13,
    'CHAT_ID': 14, 'MESSAGE_ID': 15, 'PHOTO_URL': 16, 'USER_ID': 17,
    'ORIGINAL_STATUS': 18
}

# Список округов
DISTRICTS = ["ЮЗАО", "ЗАО", "ТРАО", "НМАО"]

# Словарь для кэширования
chats_cache = {}

# Глобальные переменные
bot_client = None
web_app = None

# ============ ФУНКЦИИ ЛОГИРОВАНИЯ ============
def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(f"[ERROR] {message}")

def log_warn(message):
    logging.warning(f"[WARN] {message}")

# ============ ВЕБ-СЕРВЕР ДЛЯ ПИНГА ============
async def handle_ping(request):
    """Обработчик для пинга"""
    return web.Response(text=f"Bot is running! Moscow time: {get_moscow_datetime_str()}")

async def start_web_server():
    """Запуск веб-сервера"""
    global web_app
    web_app = web.Application()
    web_app.router.add_get('/ping', handle_ping)
    
    port = int(os.environ.get('PORT', 10000))
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    log_info(f"[WEB] Сервер запущен на порту {port}")

# ============ ФУНКЦИИ GOOGLE SHEETS ============
def check_credentials_file():
    """Проверка существования файла credentials.json"""
    if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
        log_info(f"[OK] Файл credentials.json найден: {SERVICE_ACCOUNT_FILE}")
        return True
    else:
        log_error(f"Файл credentials.json НЕ найден")
        return False

def init_google_sheets():
    """Инициализация Google Sheets API"""
    try:
        if not check_credentials_file():
            return None
            
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=credentials)
        log_info("[OK] Подключение к Google Sheets API установлено")
        return service.spreadsheets()
    except Exception as e:
        log_error(f"Ошибка инициализации Google Sheets: {e}")
        return None

def get_last_row(sheets):
    """Получение номера последней заполненной строки"""
    try:
        result = sheets.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A:A'
        ).execute()
        values = result.get('values', [])
        return len(values) + 1
    except Exception as e:
        log_error(f"Ошибка получения последней строки: {e}")
        return 1

def get_sheet_id(sheets):
    """Получение ID листа по имени"""
    try:
        spreadsheet = sheets.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets_list = spreadsheet.get('sheets', [])
        for sheet in sheets_list:
            properties = sheet.get('properties', {})
            if properties.get('title') == SHEET_NAME:
                return properties.get('sheetId')
        return 0
    except Exception as e:
        log_error(f"Ошибка получения sheetId: {e}")
        return 0

def write_to_google_sheets(sheets, data, is_duplicate=False):
    """Запись данных в Google таблицу"""
    try:
        next_row = get_last_row(sheets)
        
        range_name = f'{SHEET_NAME}!A{next_row}:R{next_row}'
        body = {'values': [data]}
        
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        # Установка флажков
        sheet_id = get_sheet_id(sheets)
        requests = []
        
        # Флажок в A
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": next_row - 1,
                    "endRowIndex": next_row,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1
                },
                "cell": {
                    "dataValidation": {"condition": {"type": "BOOLEAN"}},
                    "userEnteredValue": {"boolValue": False}
                },
                "fields": "dataValidation,userEnteredValue"
            }
        })
        
        # Флажок в L
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": next_row - 1,
                    "endRowIndex": next_row,
                    "startColumnIndex": 11,
                    "endColumnIndex": 12
                },
                "cell": {
                    "dataValidation": {"condition": {"type": "BOOLEAN"}},
                    "userEnteredValue": {"boolValue": False}
                },
                "fields": "dataValidation,userEnteredValue"
            }
        })
        
        # Красный цвет для дубликатов
        if is_duplicate:
            for col in [9, 17]:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": next_row - 1,
                            "endRowIndex": next_row,
                            "startColumnIndex": col,
                            "endColumnIndex": col + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "foregroundColor": {"red": 1, "green": 0, "blue": 0},
                                    "bold": True
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat"
                    }
                })
        
        if requests:
            sheets.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": requests}
            ).execute()
        
        status_text = " (ВОЗВРАТ)" if is_duplicate else ""
        log_info(f"[OK] Сообщение от {data[COL['USER_ID']-1]} записано в строку {next_row}{status_text}")
        return next_row
        
    except HttpError as e:
        log_error(f"Ошибка записи в Google Sheets: {e}")
        return None

def add_headers_if_needed(sheets):
    """Добавление заголовков"""
    try:
        result = sheets.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A1:R1'
        ).execute()
        
        values = result.get('values', [])
        
        if not values or not values[0]:
            headers = [[
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R'
            ]]
            sheets.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{SHEET_NAME}!A1:R1',
                valueInputOption='USER_ENTERED',
                body={'values': headers}
            ).execute()
            log_info("[OK] Заголовки добавлены в таблицу")
    except Exception as e:
        log_warn(f"Ошибка при проверке заголовков: {e}")

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============
def extract_district(address):
    """Извлечение округа"""
    if not address:
        return ""
    for district in DISTRICTS:
        if district in address:
            return district
    return ""

def get_user_display_name(sender):
    """Получение имени пользователя"""
    if sender.first_name:
        if sender.last_name:
            return f"{sender.first_name} {sender.last_name}"
        return sender.first_name
    elif sender.username:
        return f"@{sender.username}"
    else:
        return f"User_{sender.id}"

def parse_message_caption(caption):
    """Парсинг сообщения"""
    lines = caption.split("\n")
    lines = [line.strip() for line in lines if line.strip()]
    tt = lines[0] if len(lines) > 0 else ""
    address = lines[1] if len(lines) > 1 else ""
    return tt, address

# ============ ФУНКЦИИ TELEGRAM ============
def send_telegram_message(user_id, text, parse_mode="HTML"):
    """Отправка сообщения в Telegram"""
    import requests
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": user_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            log_info(f"[OK] Сообщение отправлено пользователю {user_id}")
        else:
            log_error(f"Ошибка отправки: {response.text}")
    except Exception as e:
        log_error(f"Ошибка при отправке: {e}")

def send_confirmation(user_id, tt, address, district, photo_link, is_duplicate=False, chat_title=""):
    """Отправка подтверждения"""
    message_text = f"Данные приняты"
    if chat_title:
        message_text += f" из чата {chat_title}"
    message_text += ":\n"
    
    if tt:
        message_text += f"TT: {tt}\n"
    if address:
        message_text += f"Адрес: {address}\n"
    if district:
        message_text += f"Округ: {district}\n"
    if photo_link:
        message_text += f'Фото: {photo_link}\n'
    if is_duplicate:
        message_text += f"\n⚠️ Это дублирующаяся заявка!"
    
    send_telegram_message(user_id, message_text)

def check_for_duplicate(sheets, tt, address):
    """Проверка дубликатов"""
    try:
        result = sheets.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!G:I'
        ).execute()
        
        values = result.get('values', [])
        for row in values[1:]:
            if len(row) >= 2:
                row_tt = row[0].strip() if len(row) > 0 else ""
                row_address = row[2].strip() if len(row) > 2 else ""
                if row_tt == tt.strip() and row_address == address.strip():
                    return True
        return False
    except Exception as e:
        log_error(f"Ошибка проверки дубликатов: {e}")
        return False

def upload_photo_to_drive(photo_data, message_id):
    """Загрузка фото в Google Drive"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        drive_service = build('drive', 'v3', credentials=credentials)
        
        now = get_moscow_time()
        folder_name = now.strftime("%d-%m-%Y")
        
        log_info(f"[INFO] Поиск/создание папки: {folder_name}")
        
        # Поиск папки
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{DRIVE_ROOT_FOLDER_ID}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        
        if folders:
            folder_id = folders[0]['id']
            log_info(f"   [OK] Найдена папка")
        else:
            # Создание папки
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [DRIVE_ROOT_FOLDER_ID]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_id = folder.get('id')
            log_info(f"   [OK] Создана папка")
        
        # Загрузка файла
        file_name = now.strftime("%H%M") + ".jpg"
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        
        media = MediaIoBaseUpload(io.BytesIO(photo_data), mimetype='image/jpeg', resumable=True)
        file = drive_service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields='id, webViewLink'
        ).execute()
        
        file_id = file.get('id')
        web_view_link = file.get('webViewLink')
        
        # Открываем доступ
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        drive_service.permissions().create(
            fileId=file_id,
            body=permission
        ).execute()
        
        log_info(f"   [OK] Файл загружен, ID: {file_id}")
        return web_view_link
        
    except Exception as e:
        log_error(f"Ошибка загрузки в Drive: {e}")
        return ""

# ============ ОБРАБОТЧИК СООБЩЕНИЙ ============
async def message_handler(event):
    """Обработка новых сообщений"""
    message = event.message
    sender = await event.get_sender()
    chat = await event.get_chat()
    
    user_id = sender.id
    chat_id = event.chat_id
    message_id = message.id
    chat_title = getattr(chat, 'title', f'Чат {chat_id}')
    
    display_name = get_user_display_name(sender)
    
    log_info(f"\n{'='*60}")
    log_info(f"[IN] Новое сообщение из чата '{chat_title}' от {display_name}")
    
    sheets = init_google_sheets()
    if not sheets:
        log_error("Нет подключения к Google Sheets")
        return
    
    # Текстовое сообщение
    if message.text:
        caption = message.text
        log_info(f"[TEXT] {caption[:100]}")
        
        tt, address = parse_message_caption(caption)
        
        if not tt or not address:
            error_msg = "Ошибка: Не хватает данных.\n1 строка - TT\n2 строка - Адрес"
            send_telegram_message(user_id, error_msg, parse_mode=None)
            return
        
        district = extract_district(address)
        is_duplicate = check_for_duplicate(sheets, tt, address)
        
        current_date = get_moscow_date_str()
        current_time = get_moscow_time_str()
        
        row_data = [''] * 18
        row_data[COL['DATE_OPENED']-1] = current_date
        row_data[COL['TIME_OPENED']-1] = current_time
        row_data[COL['TT']-1] = tt
        row_data[COL['DISTRICT']-1] = district
        row_data[COL['ADDRESS']-1] = address
        row_data[COL['CHAT_ID']-1] = str(chat_id)
        row_data[COL['MESSAGE_ID']-1] = str(message_id)
        row_data[COL['USER_ID']-1] = str(user_id)
        
        if is_duplicate:
            row_data[COL['STATUS']-1] = "Возврат"
            row_data[COL['ORIGINAL_STATUS']-1] = "Возврат"
        
        write_to_google_sheets(sheets, row_data, is_duplicate)
        send_confirmation(user_id, tt, address, district, "", is_duplicate, chat_title)
    
    # Фото с подписью
    elif message.photo:
        caption = message.caption or "(Без подписи)"
        log_info(f"[PHOTO] Подпись: {caption[:100]}")
        
        tt, address = parse_message_caption(caption)
        
        if not tt or not address:
            error_msg = "Ошибка: Не хватает данных в подписи.\n1 строка - TT\n2 строка - Адрес"
            send_telegram_message(user_id, error_msg, parse_mode=None)
            return
        
        # Скачиваем фото
        file_path = await message.download_media(file=f"/tmp/temp_photo_{message_id}.jpg")
        
        drive_file_url = ""
        if file_path and os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            log_info(f"   [INFO] Фото скачано, размер: {file_size} байт")
            
            with open(file_path, 'rb') as f:
                photo_data = f.read()
            
            drive_file_url = upload_photo_to_drive(photo_data, message_id)
            
            os.remove(file_path)
            log_info(f"   [OK] Временный файл удален")
        else:
            log_error("Не удалось скачать фото")
        
        district = extract_district(address)
        is_duplicate = check_for_duplicate(sheets, tt, address)
        
        current_date = get_moscow_date_str()
        current_time = get_moscow_time_str()
        
        row_data = [''] * 18
        row_data[COL['DATE_OPENED']-1] = current_date
        row_data[COL['TIME_OPENED']-1] = current_time
        row_data[COL['TT']-1] = tt
        row_data[COL['DISTRICT']-1] = district
        row_data[COL['ADDRESS']-1] = address
        row_data[COL['CHAT_ID']-1] = str(chat_id)
        row_data[COL['MESSAGE_ID']-1] = str(message_id)
        row_data[COL['PHOTO_URL']-1] = drive_file_url
        row_data[COL['USER_ID']-1] = str(user_id)
        
        if is_duplicate:
            row_data[COL['STATUS']-1] = "Возврат"
            row_data[COL['ORIGINAL_STATUS']-1] = "Возврат"
        
        write_to_google_sheets(sheets, row_data, is_duplicate)
        send_confirmation(user_id, tt, address, district, drive_file_url, is_duplicate, chat_title)
    
    else:
        log_info("[INFO] Другой тип сообщения")

# ============ ОСНОВНАЯ ФУНКЦИЯ ============
async def main():
    """Основная функция"""
    
    log_info("=" * 70)
    log_info("Telegram Monitor Bot v3.2.0-Render")
    log_info("=" * 70)
    log_info(f"[INFO] Режим: Render.com Cloud")
    log_info(f"[INFO] Google таблица: {SPREADSHEET_ID}")
    log_info(f"[INFO] Лист: {SHEET_NAME}")
    log_info("[INFO] Telegram группы:")
    for i, chat_id in enumerate(CHAT_IDS, 1):
        log_info(f"   {i}. ID: {chat_id}")
    log_info("=" * 70)
    
    # Проверяем наличие credentials
    if not SERVICE_ACCOUNT_FILE:
        log_error("Нет файла credentials.json! Бот не может работать без доступа к Google Sheets.")
        return
    
    # Запускаем веб-сервер
    await start_web_server()
    
    # Создаем клиента Telegram
    client = TelegramClient('bot_session', API_ID, API_HASH)
    
    try:
        await client.start(bot_token=BOT_TOKEN)
        log_info("[OK] Бот успешно подключился к Telegram")
        
        # Проверяем Google Sheets
        sheets = init_google_sheets()
        if sheets:
            add_headers_if_needed(sheets)
            log_info("[OK] Подключение к Google Sheets установлено")
        else:
            log_error("Не удалось подключиться к Google Sheets")
            return
        
        # Проверяем доступ к чатам
        successful_chats = []
        for chat_id in CHAT_IDS:
            try:
                chat = await client.get_entity(chat_id)
                chat_title = getattr(chat, 'title', f'Чат {chat_id}')
                log_info(f"[OK] Подключено к чату: {chat_title}")
                successful_chats.append(chat_id)
            except Exception as e:
                log_error(f"Нет доступа к чату {chat_id}: {e}")
        
        if not successful_chats:
            log_error("Нет доступных чатов для мониторинга")
            return
        
        # Регистрируем обработчик
        @client.on(events.NewMessage(chats=successful_chats))
        async def handler(event):
            await message_handler(event)
        
        log_info(f"\n[OK] Мониторинг {len(successful_chats)} чатов запущен")
        log_info("[INFO] Сервер пинга активен")
        log_info("[INFO] Ctrl+C для остановки")
        log_info("-" * 70)
        
        await client.run_until_disconnected()
        
    except KeyboardInterrupt:
        log_info("\n[STOP] Мониторинг остановлен пользователем")
    except Exception as e:
        log_error(f"{e}")
        traceback.print_exc()
    finally:
        await client.disconnect()
        log_info("[OK] Отключено от Telegram")

# ============ ТОЧКА ВХОДА ============
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info("\n[STOP] Приложение остановлено")