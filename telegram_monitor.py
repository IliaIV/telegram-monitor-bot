"""
Версия: 3.4.0-Render (Релиз от 19.02.2026)
Изменения:
- Убраны значения по умолчанию для переменных окружения
- Все данные только из Environment Variables на Render
- Добавлены проверки наличия обязательных переменных
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

# ============ ФУНКЦИИ ЛОГИРОВАНИЯ ============
def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(f"[ERROR] {message}")

def log_warn(message):
    logging.warning(f"[WARN] {message}")

# ============ ФУНКЦИЯ ДЛЯ МОСКОВСКОГО ВРЕМЕНИ ============
def get_moscow_time():
    utc_time = datetime.now(timezone.utc)
    moscow_time = utc_time + timedelta(hours=3)
    return moscow_time

def get_moscow_date_str():
    return get_moscow_time().strftime("%d.%m.%Y")

def get_moscow_time_str():
    return get_moscow_time().strftime("%H:%M")

def get_moscow_datetime_str():
    return get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")

# ============ НАСТРОЙКА ЛОГИРОВАНИЯ ============
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = os.path.join(log_dir, f'bot_{get_moscow_time().strftime("%Y%m%d_%H%M%S")}.log')

class MoscowTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        moscow_dt = dt + timedelta(hours=3)
        if datefmt:
            return moscow_dt.strftime(datefmt)
        return moscow_dt.strftime("%Y-%m-%d %H:%M:%S")

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(MoscowTimeFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(MoscowTimeFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, handler]
)

# ============ ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ============
required_vars = [
    'BOT_TOKEN',
    'API_ID',
    'API_HASH',
    'SPREADSHEET_ID',
    'SHEET_NAME',
    'CHAT_IDS',
    'DRIVE_ROOT_FOLDER_ID',
    'SERVICE_ACCOUNT_JSON'
]

missing_vars = []
for var in required_vars:
    if not os.environ.get(var):
        missing_vars.append(var)

if missing_vars:
    log_error(f"❌ ОТСУТСТВУЮТ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ: {', '.join(missing_vars)}")
    log_error("Добавьте их в настройках Render → Environment")
    sys.exit(1)

# ============ ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ============
BOT_TOKEN = os.environ.get('BOT_TOKEN')
API_ID = int(os.environ.get('API_ID'))
API_HASH = os.environ.get('API_HASH')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
SHEET_NAME = os.environ.get('SHEET_NAME')
DRIVE_ROOT_FOLDER_ID = os.environ.get('DRIVE_ROOT_FOLDER_ID')

# Парсим ID групп
CHAT_IDS_STR = os.environ.get('CHAT_IDS')
CHAT_IDS = [int(x.strip()) for x in CHAT_IDS_STR.split(',')]

# ============ ЗАГРУЗКА CREDENTIALS ИЗ ПЕРЕМЕННОЙ ОКРУЖЕНИЯ ============
SERVICE_ACCOUNT_JSON = os.environ.get('SERVICE_ACCOUNT_JSON')
SERVICE_ACCOUNT_FILE = '/tmp/credentials.json'

try:
    # Очищаем JSON от возможных проблем
    json_str = SERVICE_ACCOUNT_JSON.strip()
    if json_str.startswith('\ufeff'):
        json_str = json_str[1:]
    
    # Парсим для проверки
    json_data = json.loads(json_str)
    log_info("[OK] JSON валидный")
    
    # Сохраняем во временный файл
    with open(SERVICE_ACCOUNT_FILE, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2)
    
    log_info(f"[OK] Файл credentials.json создан: {SERVICE_ACCOUNT_FILE}")
    
except json.JSONDecodeError as e:
    log_error(f"Ошибка парсинга JSON: {e}")
    sys.exit(1)
except Exception as e:
    log_error(f"Ошибка при обработке JSON: {e}")
    sys.exit(1)

# ======================================

# === КОНСТАНТЫ ===
COL = {
    'CHECKBOX': 1, 'DATE_OPENED': 2, 'TIME_OPENED': 3, 'DATE_CLOSED': 4,
    'TIME_CLOSED': 5, 'DURATION': 6, 'TT': 7, 'DISTRICT': 8, 'ADDRESS': 9,
    'STATUS': 10, 'REASON_CLOSED': 11, 'DELETE_FLAG': 12, 'COMMENT': 13,
    'CHAT_ID': 14, 'MESSAGE_ID': 15, 'PHOTO_URL': 16, 'USER_ID': 17,
    'ORIGINAL_STATUS': 18
}

DISTRICTS = ["ЮЗАО", "ЗАО", "ТРАО", "НМАО"]
chats_cache = {}
bot_client = None
web_app = None

# ============ ВЕБ-СЕРВЕР ============
async def handle_ping(request):
    return web.Response(text=f"Bot is running! Moscow time: {get_moscow_datetime_str()}")

async def start_web_server():
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
def init_google_sheets():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=credentials)
        log_info("[OK] Подключение к Google Sheets API")
        return service.spreadsheets()
    except Exception as e:
        log_error(f"Ошибка Google Sheets: {e}")
        return None

def get_last_row(sheets):
    try:
        result = sheets.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A:A'
        ).execute()
        values = result.get('values', [])
        return len(values) + 1
    except Exception as e:
        log_error(f"Ошибка получения строки: {e}")
        return 1

def get_sheet_id(sheets):
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
        log_error(f"Ошибка записи: {e}")
        return None

def add_headers_if_needed(sheets):
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
            log_info("[OK] Заголовки добавлены")
    except Exception as e:
        log_warn(f"Ошибка заголовков: {e}")

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============
def extract_district(address):
    if not address:
        return ""
    for district in DISTRICTS:
        if district in address:
            return district
    return ""

def get_user_display_name(sender):
    if sender.first_name:
        if sender.last_name:
            return f"{sender.first_name} {sender.last_name}"
        return sender.first_name
    elif sender.username:
        return f"@{sender.username}"
    else:
        return f"User_{sender.id}"

def parse_message_caption(caption):
    lines = caption.split("\n")
    lines = [line.strip() for line in lines if line.strip()]
    tt = lines[0] if len(lines) > 0 else ""
    address = lines[1] if len(lines) > 1 else ""
    return tt, address

# ============ ФУНКЦИИ TELEGRAM ============
def send_telegram_message(user_id, text, parse_mode="HTML"):
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
        log_error(f"Ошибка: {e}")

def send_confirmation(user_id, tt, address, district, photo_link, is_duplicate=False, chat_title=""):
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
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        drive_service = build('drive', 'v3', credentials=credentials)
        
        now = get_moscow_time()
        folder_name = now.strftime("%d-%m-%Y")
        
        log_info(f"[INFO] Поиск папки: {folder_name}")
        
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{DRIVE_ROOT_FOLDER_ID}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        
        if folders:
            folder_id = folders[0]['id']
            log_info(f"   [OK] Найдена папка")
        else:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [DRIVE_ROOT_FOLDER_ID]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_id = folder.get('id')
            log_info(f"   [OK] Создана папка")
        
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
        
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        drive_service.permissions().create(
            fileId=file_id,
            body=permission
        ).execute()
        
        log_info(f"   [OK] Файл загружен")
        return web_view_link
        
    except Exception as e:
        log_error(f"Ошибка загрузки в Drive: {e}")
        return ""

# ============ ОБРАБОТЧИК СООБЩЕНИЙ ============
async def message_handler(event):
    message = event.message
    sender = await event.get_sender()
    chat = await event.get_chat()
    
    user_id = sender.id
    chat_id = event.chat_id
    message_id = message.id
    chat_title = getattr(chat, 'title', f'Чат {chat_id}')
    display_name = get_user_display_name(sender)
    
    log_info(f"\n{'='*60}")
    log_info(f"[IN] Сообщение из '{chat_title}' от {display_name}")
    
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
    
    # Фото
    elif message.photo:
        caption = message.caption or "(Без подписи)"
        log_info(f"[PHOTO] Подпись: {caption[:100]}")
        
        tt, address = parse_message_caption(caption)
        
        if not tt or not address:
            error_msg = "Ошибка: Не хватает данных в подписи"
            send_telegram_message(user_id, error_msg, parse_mode=None)
            return
        
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
    log_info("=" * 70)
    log_info("Telegram Monitor Bot v3.4.0-Render")
    log_info("=" * 70)
    log_info(f"[INFO] Google таблица: {SPREADSHEET_ID}")
    log_info(f"[INFO] Лист: {SHEET_NAME}")
    log_info("[INFO] Telegram группы:")
    for i, chat_id in enumerate(CHAT_IDS, 1):
        log_info(f"   {i}. ID: {chat_id}")
    log_info("=" * 70)
    
    await start_web_server()
    
    client = TelegramClient('bot_session', API_ID, API_HASH)
    
    try:
        await client.start(bot_token=BOT_TOKEN)
        log_info("[OK] Бот подключился к Telegram")
        
        sheets = init_google_sheets()
        if sheets:
            add_headers_if_needed(sheets)
            log_info("[OK] Подключение к Google Sheets")
        else:
            log_error("Ошибка подключения к Google Sheets")
            return
        
        successful_chats = []
        for chat_id in CHAT_IDS:
            try:
                chat = await client.get_entity(chat_id)
                log_info(f"[OK] Подключено к чату: {getattr(chat, 'title', chat_id)}")
                successful_chats.append(chat_id)
            except Exception as e:
                log_error(f"Нет доступа к чату {chat_id}: {e}")
        
        if not successful_chats:
            log_error("Нет доступных чатов")
            return
        
        @client.on(events.NewMessage(chats=successful_chats))
        async def handler(event):
            await message_handler(event)
        
        log_info(f"\n[OK] Мониторинг {len(successful_chats)} чатов")
        log_info("[INFO] Ctrl+C для остановки")
        log_info("-" * 70)
        
        await client.run_until_disconnected()
        
    except KeyboardInterrupt:
        log_info("\n[STOP] Остановлено")
    except Exception as e:
        log_error(f"{e}")
        traceback.print_exc()
    finally:
        await client.disconnect()
        log_info("[OK] Отключено")

# ============ ТОЧКА ВХОДА ============
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info("\n[STOP] Приложение остановлено")
