import os
import json
import gspread
import requests
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ============================================================================
# КОНФІГ
# ============================================================================

# Monobank - с единицей и двойкой
MONO_TOKEN_1 = os.getenv("MONO_TOKEN_1")
MONO_IBAN_1 = os.getenv("MONO_IBAN_1")
MONO_TOKEN_2 = os.getenv("MONO_TOKEN_2")
MONO_IBAN_2 = os.getenv("MONO_IBAN_2")

# NovaPay - старые переменные без цифры + новые с двойкой
NOVAPAY_LOGIN = os.getenv("NOVAPAY_LOGIN")
NOVAPAY_PUBLIC_CERTIFICATE = os.getenv("NOVAPAY_PUBLIC_CERTIFICATE")
NOVAPAY_REFRESH_TOKEN = os.getenv("NOVAPAY_REFRESH_TOKEN")

NOVAPAY_LOGIN_2 = os.getenv("NOVAPAY_LOGIN_2")
NOVAPAY_PUBLIC_CERTIFICATE_2 = os.getenv("NOVAPAY_PUBLIC_CERTIFICATE_2")
NOVAPAY_REFRESH_TOKEN_2 = os.getenv("NOVAPAY_REFRESH_TOKEN_2")

# PrivatBank
PB_ID = os.getenv("PB_ID")
PB_TOKEN = os.getenv("PB_TOKEN")
PB_ACC = os.getenv("PB_ACC")

# Google Sheets
GOOGLE_SERVICE_ACCOUNT = os.getenv("GOOGLE_SERVICE_ACCOUNT")
SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"

# Конфіги рахунків Monobank
MONO_ACCOUNTS = [
    {"token": MONO_TOKEN_1, "iban": MONO_IBAN_1, "sheet": "Monobank"},
    {"token": MONO_TOKEN_2, "iban": MONO_IBAN_2, "sheet": "MonoBank Сергій"}
]

# Конфіги рахунків NovaPay
NOVAPAY_ACCOUNTS = [
    {
        "login": NOVAPAY_LOGIN,
        "sheet": "NovaPay Анастасія",
        "certificate": NOVAPAY_PUBLIC_CERTIFICATE,
        "refresh_token": NOVAPAY_REFRESH_TOKEN
    },
    {
        "login": NOVAPAY_LOGIN_2,
        "sheet": "NovaPay Сергій",
        "certificate": NOVAPAY_PUBLIC_CERTIFICATE_2,
        "refresh_token": NOVAPAY_REFRESH_TOKEN_2
    }
]

# ============================================================================
# GOOGLE SHEETS
# ============================================================================

@lru_cache(maxsize=1)
def get_gsheet():
    """Підключитися до Google Sheets"""
    creds_json = json.loads(GOOGLE_SERVICE_ACCOUNT)
    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)

def worksheet(sheet_name):
    """Отримати лист за назвою"""
    try:
        return get_gsheet().worksheet(sheet_name)
    except:
        return get_gsheet().add_worksheet(title=sheet_name, rows=1000, cols=20)

def write_to_sheet(ws, row_data):
    """Записати рядок в таблицю"""
    try:
        ws.append_row(row_data)
        return True
    except Exception as e:
        logger.error(f"Error writing to sheet: {e}")
        return False

# ============================================================================
# MONOBANK
# ============================================================================

def get_monobank_account_id(token, iban):
    """Отримати ID рахунку за IBAN"""
    try:
        headers = {"X-Token": token}
        resp = requests.get("https://api.monobank.ua/personal/client-info", headers=headers, timeout=10)
        resp.raise_for_status()
        
        data = resp.json()
        for account in data.get("accounts", []):
            if account.get("iban") == iban:
                return account.get("id")
        return None
    except Exception as e:
        logger.error(f"Error getting Monobank account ID: {e}")
        return None

def get_monobank_statements(token, account_id):
    """Отримати виписку з Monobank"""
    try:
        headers = {"X-Token": token}
        from_time = int((datetime.now() - timedelta(days=30)).timestamp())
        
        resp = requests.get(
            f"https://api.monobank.ua/personal/statement/{account_id}/{from_time}",
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error getting Monobank statements: {e}")
        return []

def import_mono_single(account):
    """Імпортувати платежі з одного Monobank рахунку"""
    token = account.get("token")
    iban = account.get("iban")
    sheet_name = account.get("sheet")
    
    if not token or not iban:
        logger.warning(f"Missing token or IBAN for account: {iban}")
        return 0
    
    try:
        account_id = get_monobank_account_id(token, iban)
        if not account_id:
            logger.warning(f"Monobank account {iban} not found")
            return 0
        
        statements = get_monobank_statements(token, account_id)
        if not statements:
            return 0
        
        ws = worksheet(sheet_name)
        added = 0
        
        for s in statements:
            row_id = f"mono_{account_id}_{s.get('id')}"
            
            # Проверить только один раз - есть ли этот платеж
            try:
                ws.find(row_id)
                # Если нашли - значит этот и все последующие уже записаны
                break
            except:
                # Не нашли - записываем и продолжаем
                row_data = [
                    row_id,
                    datetime.fromtimestamp(s.get('time')).strftime("%Y-%m-%d %H:%M:%S"),
                    s.get('description', ''),
                    s.get('amount', 0) / 100,
                    s.get('currency', ''),
                    s.get('mcc', ''),
                    s.get('hold', False)
                ]
                if write_to_sheet(ws, row_data):
                    added += 1
        
        logger.info(f"✓ {sheet_name}: {added} rows added")
        return added
    except Exception as e:
        logger.error(f"Error importing Monobank {iban}: {e}")
        return 0

def import_mono():
    """Імпортувати платежі з усіх Monobank рахунків"""
    total = 0
    for account in MONO_ACCOUNTS:
        if account.get("token") and account.get("iban"):
            total += import_mono_single(account)
    return total

# ============================================================================
# NOVAPAY
# ============================================================================

def get_novapay_jwt(login, certificate, refresh_token):
    """Отримати JWT токен для NovaPay"""
    try:
        headers = {"Content-Type": "application/json"}
        payload = {
            "login": login,
            "certificate": certificate,
            "refresh_token": refresh_token
        }
        
        resp = requests.post(
            "https://business.novapay.ua/api/auth/jwt",
            json=payload,
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json().get("jwt")
    except Exception as e:
        logger.error(f"Error getting NovaPay JWT: {e}")
        return None

def get_novapay_statements(jwt_token):
    """Отримати виписку з NovaPay"""
    try:
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }
        
        from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        payload = {
            "from": from_date,
            "to": datetime.now().strftime("%Y-%m-%d")
        }
        
        resp = requests.post(
            "https://business.novapay.ua/api/transactions",
            json=payload,
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json().get("transactions", [])
    except Exception as e:
        logger.error(f"Error getting NovaPay statements: {e}")
        return []

def import_novapay_single(account):
    """Імпортувати платежі з одного NovaPay рахунку"""
    login = account.get("login")
    sheet_name = account.get("sheet")
    certificate = account.get("certificate")
    refresh_token = account.get("refresh_token")
    
    if not all([login, certificate, refresh_token]):
        logger.warning(f"Missing NovaPay credentials for {login}")
        return 0
    
    try:
        jwt = get_novapay_jwt(login, certificate, refresh_token)
        if not jwt:
            return 0
        
        statements = get_novapay_statements(jwt)
        if not statements:
            return 0
        
        ws = worksheet(sheet_name)
        added = 0
        
        for s in statements:
            row_id = f"nova_{login}_{s.get('id')}"
            
            # Проверить только один раз - есть ли этот платеж
            try:
                ws.find(row_id)
                # Если нашли - значит этот и все последующие уже записаны
                break
            except:
                # Не нашли - записываем и продолжаем
                row_data = [
                    row_id,
                    s.get('date', ''),
                    s.get('type', ''),
                    s.get('description', ''),
                    s.get('amount', 0),
                    s.get('currency', ''),
                    s.get('status', '')
                ]
                if write_to_sheet(ws, row_data):
                    added += 1
        
        logger.info(f"✓ {sheet_name}: {added} rows added")
        return added
    except Exception as e:
        logger.error(f"Error importing NovaPay {login}: {e}")
        return 0

def import_novapay():
    """Імпортувати платежі з усіх NovaPay рахунків"""
    total = 0
    for account in NOVAPAY_ACCOUNTS:
        if account.get("login") and account.get("certificate") and account.get("refresh_token"):
            total += import_novapay_single(account)
    return total

# ============================================================================
# PRIVATBANK
# ============================================================================

def import_privat():
    """Імпортувати платежі з PrivatBank"""
    # Ваш існуючий код для PrivatBank
    return 0

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Основна функція"""
    try:
        logger.info("Starting import...")
        
        privat_added = import_privat()
        mono_added = import_mono()
        novapay_added = import_novapay()
        
        logger.info(f"✓ Success")
        logger.info(f"  Privat: {privat_added}")
        logger.info(f"  Monobank total: {mono_added}")
        logger.info(f"  NovaPay total: {novapay_added}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
