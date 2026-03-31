"""
Версия: 3.7.0-Render (Релиз от 30.03.2026)
Изменения:
- Добавлена проверка адреса в листе "Обследование МКД"
- Функция clean_address_for_mkd() очищает адрес от подъездов и этажей
- При нахождении адреса:
  * В основном листе в колонку S записывается "Обследование"
  * В листе МКД в колонку K (для найденной строки) ставится отметка "выполнено"
- Добавлена проверка существования листа МКД
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

# Добавляем имя листа для проверки адресов
MKD_SHEET_NAME = "Обследование МКД"

# Парсим ID групп
CHAT_IDS_STR = os.environ.get('CHAT_IDS')
CHAT_IDS = [int(x.strip()) for x in CHAT_IDS_STR.split(',')]

# ============ ЗАГРУЗКА CREDENTIALS ИЗ ПЕРЕМЕННОЙ ОКРУЖЕНИЯ ============
SERVICE_ACCOUNT_JSON = os.environ.get('SERVICE_ACCOUNT_JSON')
SERVICE_ACCOUNT_FILE = '/tmp/credentials.json'

try:
    json_str = SERVICE_ACCOUNT_JSON.strip()
    if json_str.startswith('\ufeff'):
        json_str = json_str[1:]
    
    json_data = json.loads(json_str)
    log_info("[OK] JSON валидный")
    
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
    'ORIGINAL_STATUS': 18, 'MKD_STATUS': 19  # Колонка S для отметки обследования
}

# Колонки для листа "Обследование МКД"
MKD_COL = {
    'ADDRESS': 4,      # Столбец D
    'STATUS': 11       # Столбец K (для отметки выполнено)
}

DISTRICTS = ["ЮЗАО", "ЗАО", "ТРАО", "НМАО"]
chats_cache = {}
bot_client = None
web_app = None

# Кэш для адресов из МКД, чтобы не запрашивать каждый раз
mkd_addresses_cache = None
mkd_addresses_cache_time = None
CACHE_DURATION = 3600  # Кэш на 1 час

# ============ ФУНКЦИЯ ОЧИСТКИ АДРЕСА ОТ ПОДЪЕЗДОВ И ЭТАЖЕЙ ============
def clean_address_for_mkd(address):
    """
    Очищает адрес от информации о подъездах и этажах, СОХРАНЯЯ номер дома
    и НЕ трогая сокращения типа "пос.", "п." в составе слов
    
    Пример:
    "Москва, ТРАО, Михайлово-Ярцевское, пос.Шишкин Лес, 21 корп. 1, 1п., 1 этаж"
    -> "Москва, ТРАО, Михайлово-Ярцевское, пос.Шишкин Лес, 21 корп. 1"
    """
    if not address:
        return ""
    
    # Сначала сохраняем номер дома и корпус (если есть)
    # Ищем номер дома с возможным корпусом
    house_with_corps_pattern = r',\s*(\d+(?:\s*корп\.?\s*\d+)?)'
    
    # Паттерны для удаления информации о подъездах и этажах
    # Важно: не трогаем "пос." (поселок) и "п." в составе "площадь"
    patterns_to_remove = [
        r',\s*\d+п\.?\s*(?:[^,]*,)?',      # удаляет ", 1п.," но не трогает "пос."
        r',\s*\d+\s*этаж\s*,?\s*',         # удаляет ", 1 этаж"
        r',\s*подв\.?\s*,?\s*',            # удаляет ", подв."
        r',\s*эт\.?\s*,?\s*',              # удаляет ", эт."
        r',\s*подъезд\s*,?\s*',            # удаляет ", подъезд"
        r',\s*пом\.?\s*,?\s*',             # удаляет ", пом."
        r',\s*стр\.?\s*\d*\s*,?\s*',       # удаляет ", стр."
        r',\s*лит\.?\s*[А-Я]\s*,?\s*',     # удаляет ", лит. А"
    ]
    
    cleaned = address
    
    # Удаляем только подъезды и этажи
    for pattern in patterns_to_remove:
        cleaned = re.sub(pattern, ',', cleaned)
    
    # Удаляем лишние запятые и пробелы
    cleaned = re.sub(r',\s*,', ',', cleaned)           # удаляем двойные запятые
    cleaned = re.sub(r'\s*,\s*', ', ', cleaned)        # нормализуем пробелы вокруг запятых
    cleaned = re.sub(r',\s*$', '', cleaned)            # удаляем запятую в конце
    cleaned = cleaned.strip()
    
    # Восстанавливаем "пос." если оно было повреждено
    cleaned = re.sub(r',\s*ос\.', ', пос.', cleaned)   # исправляем "ос." -> "пос."
    cleaned = re.sub(r'^\s*ос\.', 'пос.', cleaned)     # исправляем в начале строки
    
    # Удаляем пробелы после запятых внутри скобок
    cleaned = re.sub(r'\(\s*', '(', cleaned)
    cleaned = re.sub(r'\s*\)', ')', cleaned)
    
    log_info(f"[MKD] Очистка адреса:\n   Исходный: {address}\n   Очищенный: {cleaned}")
    
    return cleaned

# ============ ФУНКЦИИ РАБОТЫ С ЛИСТОМ МКД ============
def load_mkd_addresses_with_rows(sheets):
    """
    Загружает адреса из листа Обследование МКД (столбец D) с номерами строк
    Возвращает список словарей: [{'row': номер_строки, 'address': адрес, 'status': статус}, ...]
    """
    global mkd_addresses_cache, mkd_addresses_cache_time
    
    current_time = datetime.now().timestamp()
    if mkd_addresses_cache is not None and mkd_addresses_cache_time is not None:
        if current_time - mkd_addresses_cache_time < CACHE_DURATION:
            log_info("[MKD] Использую кэшированные адреса")
            return mkd_addresses_cache
    
    try:
        # Проверяем существование листа
        spreadsheet = sheets.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets_list = spreadsheet.get('sheets', [])
        
        sheet_exists = False
        for sheet in sheets_list:
            properties = sheet.get('properties', {})
            if properties.get('title') == MKD_SHEET_NAME:
                sheet_exists = True
                break
        
        if not sheet_exists:
            log_warn(f"[MKD] Лист '{MKD_SHEET_NAME}' не найден в таблице. Проверка адресов отключена.")
            mkd_addresses_cache = []
            mkd_addresses_cache_time = current_time
            return []
        
        log_info(f"[MKD] Загрузка адресов из листа '{MKD_SHEET_NAME}', столбцы D и K")
        
        # Получаем все данные из столбцов D (адрес) и K (статус)
        result = sheets.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{MKD_SHEET_NAME}!D:K'
        ).execute()
        
        values = result.get('values', [])
        
        # Извлекаем адреса с номерами строк
        addresses_with_rows = []
        for idx, row in enumerate(values, start=1):
            if row and len(row) >= 1:
                address = row[0].strip() if row[0] else ""
                # Статус из колонки K (индекс 7 в полученном диапазоне)
                status = row[7].strip() if len(row) > 7 else ""
                if address and address.lower() != "адрес" and address.lower() != "address":
                    addresses_with_rows.append({
                        'row': idx,
                        'address': address,
                        'status': status
                    })
        
        log_info(f"[MKD] Загружено {len(addresses_with_rows)} адресов")
        
        # Сохраняем в кэш
        mkd_addresses_cache = addresses_with_rows
        mkd_addresses_cache_time = current_time
        
        return addresses_with_rows
        
    except HttpError as e:
        if e.resp.status == 404:
            log_warn(f"[MKD] Лист '{MKD_SHEET_NAME}' не найден. Проверка адресов отключена.")
        else:
            log_error(f"[MKD] Ошибка загрузки адресов: {e}")
        mkd_addresses_cache = []
        mkd_addresses_cache_time = current_time
        return []
    except Exception as e:
        log_error(f"[MKD] Ошибка загрузки адресов: {e}")
        mkd_addresses_cache = []
        mkd_addresses_cache_time = current_time
        return []

def update_mkd_status(sheets, row_number):
    """Обновляет статус в колонке K листа МКД на 'выполнено'"""
    try:
        range_name = f'{MKD_SHEET_NAME}!K{row_number}'
        body = {'values': [['выполнено']]}
        
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        log_info(f"[MKD] Обновлен статус в строке {row_number} на 'выполнено'")
        return True
    except Exception as e:
        log_error(f"[MKD] Ошибка обновления статуса в строке {row_number}: {e}")
        return False

def check_and_mark_address_in_mkd(sheets, address):
    """
    Проверяет наличие адреса в листе МКД.
    Сравнивает адрес ДО номера дома (улица + номер дома), игнорируя корпус, подъезд, этаж
    """
    if not address:
        return False, None
    
    # Очищаем адрес от подъездов и этажей
    cleaned_address = clean_address_for_mkd(address)
    
    addresses_with_rows = load_mkd_addresses_with_rows(sheets)
    
    if not addresses_with_rows:
        return False, None
    
    # Паттерн для извлечения базового адреса (до номера дома)
    # Убираем корпус, строение и прочее после номера дома
    base_pattern = r'^([^,]+(?:,[^,]+){0,4},\s*\d+)(?:[^\d]|$)'
    
    base_match = re.search(base_pattern, cleaned_address)
    base_address = base_match.group(1) if base_match else cleaned_address
    
    # Убираем лишние пробелы
    base_address = re.sub(r'\s+', ' ', base_address.strip().lower())
    
    log_info(f"[MKD] Поиск по базовому адресу: '{base_address}'")
    
    best_match = None
    best_match_score = 0
    
    for item in addresses_with_rows:
        # Пропускаем уже выполненные
        if item['status'] == 'выполнено':
            continue
        
        clean_mkd = re.sub(r'\s+', ' ', item['address'].lower())
        
        # Извлекаем базовый адрес из адреса МКД (до номера дома)
        mkd_base_match = re.search(base_pattern, clean_mkd)
        mkd_base = mkd_base_match.group(1) if mkd_base_match else clean_mkd
        
        # Проверяем точное совпадение базовых адресов
        if base_address == mkd_base:
            log_info(f"[MKD] Точное совпадение: '{item['address']}'")
            best_match = item
            best_match_score = 100
            break
        
        # Если не точное, проверяем частичное совпадение (улица + номер)
        # Извлекаем номер дома из базового адреса
        house_pattern = r',\s*(\d+(?:[\/\-]\d+)?)\s*$'
        house_match = re.search(house_pattern, base_address)
        house_number = house_match.group(1) if house_match else ""
        
        street_part = re.sub(house_pattern, '', base_address).strip() if house_match else base_address
        
        # Извлекаем номер дома из МКД
        mkd_house_match = re.search(house_pattern, mkd_base)
        mkd_house = mkd_house_match.group(1) if mkd_house_match else ""
        mkd_street = re.sub(house_pattern, '', mkd_base).strip() if mkd_house_match else mkd_base
        
        # Проверка совпадения улицы и номера дома
        if street_part == mkd_street and house_number == mkd_house:
            score = 95
            log_info(f"[MKD] Совпадение улицы и дома: '{street_part}', дом {house_number}")
            if score > best_match_score:
                best_match_score = score
                best_match = item
        
        # Проверка только улицы (если номера домов не указаны)
        elif street_part == mkd_street and (not house_number or not mkd_house):
            score = 70
            log_info(f"[MKD] Совпадение улицы: '{street_part}'")
            if score > best_match_score:
                best_match_score = score
                best_match = item
    
    # Если найдено совпадение
    if best_match and best_match_score >= 90:
        log_info(f"[MKD] Найдено совпадение: '{best_match['address']}' (строка {best_match['row']}, score={best_match_score})")
        
        # Обновляем статус в МКД
        update_mkd_status(sheets, best_match['row'])
        
        return True, best_match['address']
    
    log_info(f"[MKD] Адрес не найден: {cleaned_address}")
    return False, None

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

def write_to_google_sheets(sheets, data, is_duplicate=False, mkd_found=False):
    """Запись данных в Google таблицу"""
    try:
        next_row = get_last_row(sheets)
        
        # Если найден в МКД, добавляем отметку в колонку S
        if mkd_found:
            data[COL['MKD_STATUS']-1] = "Обследование"
            log_info(f"[MKD] Добавлена отметка 'Обследование' в колонку S")
        
        range_name = f'{SHEET_NAME}!A{next_row}:S{next_row}'  # Расширяем до колонки S
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
            range=f'{SHEET_NAME}!A1:S1'
        ).execute()
        
        values = result.get('values', [])
        
        if not values or not values[0]:
            headers = [[
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S'
            ]]
            sheets.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{SHEET_NAME}!A1:S1',
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

def send_confirmation(user_id, tt, address, district, photo_link, is_duplicate=False, chat_title="", mkd_found=False, mkd_address=None):
    """Отправка подтверждения с учетом информации о МКД"""
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
    
    # Добавляем информацию о МКД
    if mkd_found:
        message_text += f"\n📌 Адрес найден в списке МКД!"
        if mkd_address:
            message_text += f"\n   (в списке: {mkd_address})"
    
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
        
        # Проверяем наличие адреса в МКД и обновляем статус
        mkd_found, mkd_address = check_and_mark_address_in_mkd(sheets, address)
        
        current_date = get_moscow_date_str()
        current_time = get_moscow_time_str()
        
        row_data = [''] * 19  # Увеличиваем до 19 колонок (A-S)
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
        
        # Записываем в таблицу
        write_to_google_sheets(sheets, row_data, is_duplicate, mkd_found)
        send_confirmation(user_id, tt, address, district, "", is_duplicate, chat_title, mkd_found, mkd_address)
    
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
        
        # Проверяем наличие адреса в МКД и обновляем статус
        mkd_found, mkd_address = check_and_mark_address_in_mkd(sheets, address)
        
        current_date = get_moscow_date_str()
        current_time = get_moscow_time_str()
        
        row_data = [''] * 19  # Увеличиваем до 19 колонок (A-S)
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
        
        write_to_google_sheets(sheets, row_data, is_duplicate, mkd_found)
        send_confirmation(user_id, tt, address, district, drive_file_url, is_duplicate, chat_title, mkd_found, mkd_address)
    
    else:
        log_info("[INFO] Другой тип сообщения")

# ============ ОСНОВНАЯ ФУНКЦИЯ ============
async def main():
    log_info("=" * 70)
    log_info("Telegram Monitor Bot v3.7.0-Render")
    log_info("=" * 70)
    log_info(f"[INFO] Google таблица: {SPREADSHEET_ID}")
    log_info(f"[INFO] Лист ТТ: {SHEET_NAME}")
    log_info(f"[INFO] Лист МКД: {MKD_SHEET_NAME} (столбец D)")
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
            # Предварительно загружаем адреса из МКД в кэш (если лист существует)
            load_mkd_addresses_with_rows(sheets)
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
