import os
import json
import gspread
import requests
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import logging
from functools import lru_cache
from zeep import Client as SOAPClient
from zeep.wsse.username import UsernameToken
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================================================
# КОНФІГ
# ============================================================================

MONO_TOKEN_1 = os.getenv("MONO_TOKEN_1")
MONO_IBAN_1 = os.getenv("MONO_IBAN_1")
MONO_TOKEN_2 = os.getenv("MONO_TOKEN_2")
MONO_IBAN_2 = os.getenv("MONO_IBAN_2")

# PrivatBank (новые параметры)
PB_CLIENT_ID = os.getenv("PB_CLIENT_ID")
PB_TOKEN = os.getenv("PB_TOKEN")
PB_ACC_1 = os.getenv("PB_ACC_1")  # IBAN первого счета
PB_ACC_2 = os.getenv("PB_ACC_2")  # IBAN второго счета (опционально)

# NovaPay (новые параметры)
NOVAPAY_MERCHANT_ID = os.getenv("NOVAPAY_MERCHANT_ID")
NOVAPAY_PRIVATE_KEY = os.getenv("NOVAPAY_PRIVATE_KEY")  # RSA приватный ключ
NOVAPAY_MERCHANT_ID_2 = os.getenv("NOVAPAY_MERCHANT_ID_2")
NOVAPAY_PRIVATE_KEY_2 = os.getenv("NOVAPAY_PRIVATE_KEY_2")

GOOGLE_SERVICE_ACCOUNT = os.getenv("GOOGLE_SERVICE_ACCOUNT")
SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"

MONO_ACCOUNTS = [
    {"token": MONO_TOKEN_1, "iban": MONO_IBAN_1, "sheet": "Monobank"},
    {"token": MONO_TOKEN_2, "iban": MONO_IBAN_2, "sheet": "MonoBank Сергій"}
]

PRIVAT_ACCOUNTS = [
    {"client_id": PB_CLIENT_ID, "token": PB_TOKEN, "acc": PB_ACC_1, "sheet": "Privat"},
    {"client_id": PB_CLIENT_ID, "token": PB_TOKEN, "acc": PB_ACC_2, "sheet": "Privat Сергій"}
]

NOVAPAY_ACCOUNTS = [
    {"merchant_id": NOVAPAY_MERCHANT_ID, "private_key": NOVAPAY_PRIVATE_KEY, "sheet": "NovaPay Анастасія"},
    {"merchant_id": NOVAPAY_MERCHANT_ID_2, "private_key": NOVAPAY_PRIVATE_KEY_2, "sheet": "NovaPay Сергій"}
]

# ============================================================================
# GOOGLE SHEETS
# ============================================================================

@lru_cache(maxsize=1)
def get_gsheet():
    """Підключитися до Google Sheets"""
    try:
        creds_json = json.loads(GOOGLE_SERVICE_ACCOUNT)
        creds = Credentials.from_service_account_info(
            creds_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        logger.info("✓ Google Sheets connected successfully")
        return client.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"✗ Failed to connect to Google Sheets: {e}")
        raise

def worksheet(sheet_name):
    """Отримати лист за назвою"""
    try:
        ws = get_gsheet().worksheet(sheet_name)
        logger.info(f"✓ Worksheet '{sheet_name}' opened")
        return ws
    except Exception as e:
        logger.warning(f"! Worksheet '{sheet_name}' not found, creating new one")
        try:
            ws = get_gsheet().add_worksheet(title=sheet_name, rows=1000, cols=20)
            logger.info(f"✓ Created new worksheet '{sheet_name}'")
            return ws
        except Exception as e2:
            logger.error(f"✗ Failed to create worksheet '{sheet_name}': {e2}")
            raise

def write_to_sheet(ws, row_data):
    """Записати рядок в таблицю"""
    try:
        ws.append_row(row_data)
        return True
    except Exception as e:
        logger.error(f"✗ Error writing to sheet: {e}")
        return False

# ============================================================================
# MONOBANK
# ============================================================================

def get_monobank_account_id(token, iban):
    """Отримати ID рахунку за IBAN"""
    try:
        headers = {"X-Token": token}
        logger.debug(f"  Fetching Monobank account ID for IBAN: {iban}")
        resp = requests.get("https://api.monobank.ua/personal/client-info", headers=headers, timeout=10)
        resp.raise_for_status()

        data = resp.json()
        for account in data.get("accounts", []):
            if account.get("iban") == iban:
                account_id = account.get("id")
                logger.info(f"  ✓ Found Monobank account ID: {account_id}")
                return account_id

        logger.error(f"  ✗ IBAN {iban} not found in Monobank accounts")
        return None
    except Exception as e:
        logger.error(f"  ✗ Error getting Monobank account ID: {e}")
        return None

def get_monobank_statements(token, account_id):
    """Отримати виписку з Monobank (останні 7 днів)"""
    try:
        headers = {"X-Token": token}
        from_time = int((datetime.now() - timedelta(days=7)).timestamp())
        to_time = int(datetime.now().timestamp())

        from_date = datetime.fromtimestamp(from_time).strftime("%Y-%m-%d")
        to_date = datetime.fromtimestamp(to_time).strftime("%Y-%m-%d")

        logger.info(f"  Fetching Monobank statements from {from_date} to {to_date}...")
        logger.debug(f"  URL: https://api.monobank.ua/personal/statement/{account_id}/{from_time}/{to_time}")

        resp = requests.get(
            f"https://api.monobank.ua/personal/statement/{account_id}/{from_time}/{to_time}",
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()

        statements = resp.json()
        logger.info(f"  ✓ Got {len(statements)} statements from Monobank")
        return statements
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            logger.error(f"  ✗ 400 Bad Request - может быть проблема с форматом запроса или IBAN неправильный")
            logger.debug(f"    Response: {e.response.text}")
        elif e.response.status_code == 403:
            logger.error(f"  ✗ 403 Forbidden - токен не имеет доступа к этому счету или истек срок действия")
        elif e.response.status_code == 429:
            logger.error(f"  ✗ 429 Too Many Requests - превышен лимит запросов Monobank API")
        else:
            logger.error(f"  ✗ HTTP Error {e.response.status_code}: {e}")
        return []
    except Exception as e:
        logger.error(f"  ✗ Error getting Monobank statements: {e}")
        return []

def import_mono_single(account):
    """Імпортувати платежі з одного Monobank рахунку"""
    token = account.get("token")
    iban = account.get("iban")
    sheet_name = account.get("sheet")

    logger.info(f"\n📱 Processing Monobank: {sheet_name}")

    if not token or not iban:
        logger.error(f"✗ Missing token or IBAN for account: {iban}")
        return 0

    try:
        account_id = get_monobank_account_id(token, iban)
        if not account_id:
            logger.error(f"✗ Could not get account ID for {iban}")
            return 0

        statements = get_monobank_statements(token, account_id)
        if not statements:
            logger.warning(f"⚠ No statements found for {sheet_name}")
            return 0

        ws = worksheet(sheet_name)
        added = 0

        logger.info(f"  Processing {len(statements)} statements...")
        for i, s in enumerate(statements):
            row_id = f"mono_{account_id}_{s.get('id')}"

            try:
                ws.find(row_id)
                logger.info(f"  ✓ Found existing payment at position {i}, stopping search")
                break
            except:
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
                    logger.debug(f"    ✓ Wrote payment: {row_id}")

        logger.info(f"✓ {sheet_name}: {added} rows added")
        return added
    except Exception as e:
        logger.error(f"✗ Error importing Monobank {iban}: {e}", exc_info=True)
        return 0

def import_mono():
    """Імпортувати платежі з усіх Monobank рахунків"""
    logger.info("\n🔍 Checking Monobank accounts...")
    mono_accounts_to_process = [a for a in MONO_ACCOUNTS if a.get("token") and a.get("iban")]
    logger.info(f"  Found {len(mono_accounts_to_process)} Monobank account(s) to process")

    total = 0
    for account in mono_accounts_to_process:
        total += import_mono_single(account)
    return total

# ============================================================================
# PRIVATBANK - AUTOCLIENT API
# ============================================================================

def get_privat_statements(client_id, token, account, from_date, to_date):
    """Получить выписку из PrivatBank (Autoclient API)"""
    try:
        logger.debug(f"  Fetching PrivatBank statements for account: {account}")
        
        # SOAP запрос к PrivatBank
        wsdl = 'https://api.privatbank.ua/webservices/autoClientsInfo.asmx?wsdl'
        client = SOAPClient(wsdl=wsdl)
        
        # Формируем SOAP запрос
        result = client.service.getPreviousTransactions(
            clientId=client_id,
            token=token,
            accountNumber=account,
            from=from_date.strftime("%Y-%m-%d"),
            to=to_date.strftime("%Y-%m-%d")
        )
        
        statements = result if isinstance(result, list) else []
        logger.info(f"  ✓ Got {len(statements)} statements from PrivatBank")
        return statements
        
    except Exception as e:
        logger.error(f"  ✗ Error getting PrivatBank statements: {e}")
        return []

def import_privat_single(account):
    """Імпортувати платежі з одного PrivatBank рахунку"""
    client_id = account.get("client_id")
    token = account.get("token")
    acc = account.get("acc")
    sheet_name = account.get("sheet")

    logger.info(f"\n🏦 Processing PrivatBank: {sheet_name}")

    if not client_id or not token or not acc:
        logger.error(f"✗ Missing PrivatBank credentials for account: {acc}")
        return 0

    try:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=7)

        statements = get_privat_statements(client_id, token, acc, from_date, to_date)
        if not statements:
            logger.warning(f"⚠ No statements found for {sheet_name}")
            return 0

        ws = worksheet(sheet_name)
        added = 0

        logger.info(f"  Processing {len(statements)} statements...")
        for s in statements:
            # PrivatBank возвращает другой формат
            row_id = f"privat_{acc}_{s.get('ref_no') or s.get('id')}"

            try:
                ws.find(row_id)
                logger.info(f"  ✓ Found existing payment, stopping search")
                break
            except:
                row_data = [
                    row_id,
                    s.get('dat', ''),  # PrivatBank формат даты
                    s.get('description') or s.get('desc', ''),
                    float(s.get('summ') or s.get('amount', 0)),
                    s.get('comdiv') or s.get('currency', 'UAH'),
                    s.get('type', '')
                ]
                if write_to_sheet(ws, row_data):
                    added += 1
                    logger.debug(f"    ✓ Wrote payment: {row_id}")

        logger.info(f"✓ {sheet_name}: {added} rows added")
        return added
    except Exception as e:
        logger.error(f"✗ Error importing PrivatBank {acc}: {e}", exc_info=True)
        return 0

def import_privat():
    """Імпортувати платежі з усіх PrivatBank рахунків"""
    logger.info("\n🔍 Checking PrivatBank accounts...")
    privat_accounts_to_process = [a for a in PRIVAT_ACCOUNTS if a.get("client_id") and a.get("token") and a.get("acc")]
    logger.info(f"  Found {len(privat_accounts_to_process)} PrivatBank account(s) to process")

    total = 0
    for account in privat_accounts_to_process:
        total += import_privat_single(account)
    return total

# ============================================================================
# NOVAPAY - REST API WITH RSA SIGNATURES
# ============================================================================

def sign_novapay_request(data, private_key_pem):
    """Подписать запрос для NovaPay с помощью RSA"""
    try:
        from cryptography.hazmat.primitives import serialization
        
        # Загружаем приватный ключ
        private_key = serialization.load_pem_private_key(
            private_key_pem.encode(),
            password=None,
            backend=default_backend()
        )
        
        # Подписываем данные
        signature = private_key.sign(
            data.encode(),
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        
        # Возвращаем base64-кодированную подпись
        return base64.b64encode(signature).decode()
    except Exception as e:
        logger.error(f"✗ Error signing NovaPay request: {e}")
        return None

def get_novapay_payments(merchant_id, private_key, from_date, to_date):
    """Получить платежи из NovaPay"""
    try:
        logger.debug(f"  Fetching NovaPay payments from {from_date} to {to_date}")
        
        # Формируем JSON тело запроса
        request_body = json.dumps({
            "merchant_id": merchant_id,
            "from": from_date.strftime("%Y-%m-%d"),
            "to": to_date.strftime("%Y-%m-%d"),
            "limit": 1000
        })
        
        # Подписываем запрос
        signature = sign_novapay_request(request_body, private_key)
        if not signature:
            return []
        
        # Отправляем запрос
        headers = {
            "Content-Type": "application/json",
            "x-sign": signature
        }
        
        resp = requests.post(
            "https://api.novapay.ua/payments/export",
            data=request_body,
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        
        data = resp.json()
        payments = data.get('payments', []) if isinstance(data, dict) else []
        logger.info(f"  ✓ Got {len(payments)} payments from NovaPay")
        return payments
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ✗ NovaPay API Error {e.response.status_code}: {e.response.text}")
        return []
    except Exception as e:
        logger.error(f"  ✗ Error getting NovaPay payments: {e}")
        return []

def import_novapay_single(account):
    """Імпортувати платежі з одного NovaPay рахунку"""
    merchant_id = account.get("merchant_id")
    private_key = account.get("private_key")
    sheet_name = account.get("sheet")

    logger.info(f"\n💳 Processing NovaPay: {sheet_name}")

    if not merchant_id or not private_key:
        logger.error(f"✗ Missing NovaPay credentials for {sheet_name}")
        return 0

    try:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=7)

        payments = get_novapay_payments(merchant_id, private_key, from_date, to_date)
        if not payments:
            logger.warning(f"⚠ No payments found for {sheet_name}")
            return 0

        ws = worksheet(sheet_name)
        added = 0

        logger.info(f"  Processing {len(payments)} payments...")
        for p in payments:
            row_id = f"nova_{merchant_id}_{p.get('id') or p.get('code', '')}"

            try:
                ws.find(row_id)
                logger.info(f"  ✓ Found existing payment, stopping search")
                break
            except:
                row_data = [
                    row_id,
                    p.get('date') or p.get('created_at', ''),
                    p.get('description') or p.get('purpose', ''),
                    float(p.get('amount', 0)),
                    p.get('currency', 'UAH'),
                    p.get('status', 'unknown')
                ]
                if write_to_sheet(ws, row_data):
                    added += 1
                    logger.debug(f"    ✓ Wrote payment: {row_id}")

        logger.info(f"✓ {sheet_name}: {added} rows added")
        return added
    except Exception as e:
        logger.error(f"✗ Error importing NovaPay {sheet_name}: {e}", exc_info=True)
        return 0

def import_novapay():
    """Імпортувати платежі з усіх NovaPay рахунків"""
    logger.info("\n🔍 Checking NovaPay accounts...")
    novapay_accounts_to_process = [a for a in NOVAPAY_ACCOUNTS if a.get("merchant_id") and a.get("private_key")]
    logger.info(f"  Found {len(novapay_accounts_to_process)} NovaPay account(s) to process")

    total = 0
    for account in novapay_accounts_to_process:
        total += import_novapay_single(account)
    return total

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Основна функція"""
    logger.info("="*60)
    logger.info("🚀 STARTING PAYMENT IMPORT")
    logger.info("="*60)

    try:
        mono_added = import_mono()
        privat_added = import_privat()
        novapay_added = import_novapay()

        logger.info("\n" + "="*60)
        logger.info("✓ IMPORT COMPLETED")
        logger.info("="*60)
        logger.info(f"  📊 Summary:")
        logger.info(f"     Monobank:      {mono_added} transactions")
        logger.info(f"     PrivatBank:    {privat_added} transactions")
        logger.info(f"     NovaPay:       {novapay_added} transactions")
        logger.info(f"     TOTAL:         {mono_added + privat_added + novapay_added} transactions")
        logger.info("="*60)

    except Exception as e:
        logger.error("="*60)
        logger.error("✗ FATAL ERROR")
        logger.error("="*60)
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("="*60)

if __name__ == "__main__":
    main()
