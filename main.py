import os
import json
import gspread
import requests
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import logging
from functools import lru_cache
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================================================
# КОНФІГ
# ============================================================================

MONO_TOKEN_1 = os.getenv("MONO_TOKEN_1")
MONO_IBAN_1 = os.getenv("MONO_IBAN_1")
MONO_TOKEN_2 = os.getenv("MONO_TOKEN_2")
MONO_IBAN_2 = os.getenv("MONO_IBAN_2")

NOVAPAY_LOGIN = os.getenv("NOVAPAY_LOGIN")
NOVAPAY_PUBLIC_CERTIFICATE = os.getenv("NOVAPAY_PUBLIC_CERTIFICATE")
NOVAPAY_REFRESH_TOKEN = os.getenv("NOVAPAY_REFRESH_TOKEN")

NOVAPAY_LOGIN_2 = os.getenv("NOVAPAY_LOGIN_2")
NOVAPAY_PUBLIC_CERTIFICATE_2 = os.getenv("NOVAPAY_PUBLIC_CERTIFICATE_2")
NOVAPAY_REFRESH_TOKEN_2 = os.getenv("NOVAPAY_REFRESH_TOKEN_2")

PB_ID = os.getenv("PB_ID")
PB_TOKEN = os.getenv("PB_TOKEN")
PB_ACC = os.getenv("PB_ACC")

GOOGLE_SERVICE_ACCOUNT = os.getenv("GOOGLE_SERVICE_ACCOUNT")
SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"

MONO_ACCOUNTS = [
    {"token": MONO_TOKEN_1, "iban": MONO_IBAN_1, "sheet": "Monobank"},
    {"token": MONO_TOKEN_2, "iban": MONO_IBAN_2, "sheet": "MonoBank Сергій"}
]

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
# ЛОГИРОВАНИЕ ИНИЦИАЛИЗАЦИИ
# ============================================================================

def log_initialization():
    """Логирование переменных окружения для отладки"""
    logger.info("📋 ENVIRONMENT VARIABLES CHECK:")
    logger.info(f"  MONO_TOKEN_1:              {'✓ SET' if MONO_TOKEN_1 else '✗ NOT SET'}")
    logger.info(f"  MONO_IBAN_1:               {'✓ SET' if MONO_IBAN_1 else '✗ NOT SET'}")
    logger.info(f"  MONO_TOKEN_2:              {'✓ SET' if MONO_TOKEN_2 else '✗ NOT SET'}")
    logger.info(f"  MONO_IBAN_2:               {'✓ SET' if MONO_IBAN_2 else '✗ NOT SET'}")
    logger.info(f"  NOVAPAY_LOGIN:             {'✓ SET' if NOVAPAY_LOGIN else '✗ NOT SET'}")
    logger.info(f"  NOVAPAY_PUBLIC_CERT:       {'✓ SET' if NOVAPAY_PUBLIC_CERTIFICATE else '✗ NOT SET'}")
    logger.info(f"  NOVAPAY_REFRESH_TOKEN:     {'✓ SET' if NOVAPAY_REFRESH_TOKEN else '✗ NOT SET'}")
    logger.info(f"  NOVAPAY_LOGIN_2:           {'✓ SET' if NOVAPAY_LOGIN_2 else '✗ NOT SET'}")
    logger.info(f"  NOVAPAY_PUBLIC_CERT_2:     {'✓ SET' if NOVAPAY_PUBLIC_CERTIFICATE_2 else '✗ NOT SET'}")
    logger.info(f"  NOVAPAY_REFRESH_TOKEN_2:   {'✓ SET' if NOVAPAY_REFRESH_TOKEN_2 else '✗ NOT SET'}")
    logger.info(f"  PB_ID:                     {'✓ SET' if PB_ID else '✗ NOT SET'}")
    logger.info(f"  PB_TOKEN:                  {'✓ SET' if PB_TOKEN else '✗ NOT SET'}")
    logger.info(f"  PB_ACC:                    {'✓ SET' if PB_ACC else '✗ NOT SET'}")
    logger.info(f"  GOOGLE_SERVICE_ACCOUNT:    {'✓ SET' if GOOGLE_SERVICE_ACCOUNT else '✗ NOT SET'}")

    logger.info("\n📦 ACCOUNTS CONFIGURATION:")
    logger.info(f"  Monobank accounts configured: {len([a for a in MONO_ACCOUNTS if a.get('token') and a.get('iban')])}/{len(MONO_ACCOUNTS)}")
    for i, acc in enumerate(MONO_ACCOUNTS):
        has_token = bool(acc.get('token'))
        has_iban = bool(acc.get('iban'))
        logger.info(f"    Account {i+1} ({acc.get('sheet')}): token={has_token}, iban={has_iban}")

    logger.info(f"  NovaPay accounts configured: {len([a for a in NOVAPAY_ACCOUNTS if a.get('login') and a.get('certificate') and a.get('refresh_token')])}/{len(NOVAPAY_ACCOUNTS)}")
    for i, acc in enumerate(NOVAPAY_ACCOUNTS):
        has_login = bool(acc.get('login'))
        has_cert = bool(acc.get('certificate'))
        has_token = bool(acc.get('refresh_token'))
        logger.info(f"    Account {i+1} ({acc.get('sheet')}): login={has_login}, cert={has_cert}, token={has_token}")

    logger.info(f"  PrivatBank configured: {'✓ YES' if all([PB_ID, PB_TOKEN, PB_ACC]) else '✗ NO'}")

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
    """Отримати виписку з Monobank"""
    try:
        headers = {"X-Token": token}
        from_time = int((datetime.now() - timedelta(days=60)).timestamp())
        to_time = int(datetime.now().timestamp())
        from_date = datetime.fromtimestamp(from_time).strftime("%Y-%m-%d")
        to_date = datetime.fromtimestamp(to_time).strftime("%Y-%m-%d")

        logger.info(f"  Fetching Monobank statements from {from_date} to {to_date}...")
        resp = requests.get(
            f"https://api.monobank.ua/personal/statement/{account_id}/{from_time}/{to_time}",
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()

        statements = resp.json()
        logger.info(f"  ✓ Got {len(statements)} statements from Monobank")
        return statements
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

        logger.debug(f"  Getting JWT for NovaPay account: {login}")

        # Try the correct endpoint with /v1/
        url = "https://api.novapay.ua/v1/auth/jwt"
        logger.debug(f"  Trying endpoint: {url}")

        resp = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        jwt = resp.json().get("jwt")
        logger.info(f"  ✓ Got JWT token for {login}")
        return jwt
    except Exception as e:
        logger.error(f"  ✗ Error getting NovaPay JWT: {e}")
        logger.error(f"  Response status: {resp.status_code if 'resp' in locals() else 'N/A'}")
        if 'resp' in locals():
            logger.error(f"  Response body: {resp.text[:200]}")
        return None

def get_novapay_statements(jwt_token):
    """Отримати виписку з NovaPay"""
    try:
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }

        from_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"  Fetching NovaPay transactions from {from_date} to {to_date}...")
        payload = {
            "from": from_date,
            "to": to_date
        }

        resp = requests.post(
            "https://business.novapay.ua/api/transactions",
            json=payload,
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()

        transactions = resp.json().get("transactions", [])
        logger.info(f"  ✓ Got {len(transactions)} transactions from NovaPay")
        return transactions
    except Exception as e:
        logger.error(f"  ✗ Error getting NovaPay statements: {e}")
        return []

def import_novapay_single(account):
    """Імпортувати платежі з одного NovaPay рахунку"""
    login = account.get("login")
    sheet_name = account.get("sheet")
    certificate = account.get("certificate")
    refresh_token = account.get("refresh_token")

    logger.info(f"\n💳 Processing NovaPay: {sheet_name}")

    if not all([login, certificate, refresh_token]):
        logger.error(f"✗ Missing credentials for {login}")
        return 0

    try:
        jwt = get_novapay_jwt(login, certificate, refresh_token)
        if not jwt:
            logger.error(f"✗ Failed to get JWT for {login}")
            return 0

        statements = get_novapay_statements(jwt)
        if not statements:
            logger.warning(f"⚠ No transactions found for {sheet_name}")
            return 0

        ws = worksheet(sheet_name)
        added = 0

        logger.info(f"  Processing {len(statements)} transactions...")
        for i, s in enumerate(statements):
            row_id = f"nova_{login}_{s.get('id')}"

            try:
                ws.find(row_id)
                logger.info(f"  ✓ Found existing transaction at position {i}, stopping search")
                break
            except:
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
                    logger.debug(f"    ✓ Wrote transaction: {row_id}")

        logger.info(f"✓ {sheet_name}: {added} rows added")
        return added
    except Exception as e:
        logger.error(f"✗ Error importing NovaPay {login}: {e}", exc_info=True)
        return 0

def import_novapay():
    """Імпортувати платежі з усіх NovaPay рахунків"""
    logger.info("\n🔍 Checking NovaPay accounts...")
    novapay_accounts_to_process = [a for a in NOVAPAY_ACCOUNTS if a.get("login") and a.get("certificate") and a.get("refresh_token")]
    logger.info(f"  Found {len(novapay_accounts_to_process)} NovaPay account(s) to process")

    total = 0
    for account in novapay_accounts_to_process:
        total += import_novapay_single(account)
    return total

# ============================================================================
# PRIVATBANK
# ============================================================================

def get_privatbank_statement():
    """Отримати виписку з PrivatBank"""
    try:
        logger.info(f"  Fetching PrivatBank statements for account: {PB_ACC}...")

        from_time = int((datetime.now() - timedelta(days=60)).timestamp())
        to_time = int(datetime.now().timestamp())

        url = f"https://api.privatbank.ua/p24api/statementxml"
        params = {
            "login": PB_ID,
            "password": PB_TOKEN,
            "account": PB_ACC,
            "startDate": datetime.fromtimestamp(from_time).strftime("%d.%m.%Y"),
            "endDate": datetime.fromtimestamp(to_time).strftime("%d.%m.%Y")
        }

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()

        logger.info(f"  ✓ Got PrivatBank statement")
        return resp.text
    except Exception as e:
        logger.error(f"  ✗ Error getting PrivatBank statement: {e}")
        return None

def import_privat():
    """Імпортувати платежі з PrivatBank"""
    logger.info("\n🏦 Processing PrivatBank")

    if not all([PB_ID, PB_TOKEN, PB_ACC]):
        logger.warning("⚠ PrivatBank credentials not fully configured, skipping")
        return 0

    try:
        statement = get_privatbank_statement()
        if not statement:
            logger.warning("⚠ No PrivatBank statement received")
            return 0

        # Парсим XML
        try:
            root = ET.fromstring(statement)
            transactions = root.findall('.//transaction')
            logger.info(f"  ✓ Parsed {len(transactions)} transactions from XML")

            if not transactions:
                logger.warning("⚠ No transactions found in PrivatBank statement")
                return 0

            ws = worksheet("PrivatBank")
            added = 0

            logger.info(f"  Processing {len(transactions)} transactions...")
            for i, trans in enumerate(transactions):
                trans_id = trans.get('id')
                row_id = f"pb_{trans_id}"

                try:
                    ws.find(row_id)
                    logger.info(f"  ✓ Found existing transaction at position {i}, stopping search")
                    break
                except:
                    row_data = [
                        row_id,
                        trans.get('post'),
                        trans.get('description', ''),
                        float(trans.findtext('amount', 0)),
                        trans.findtext('currency', ''),
                        trans.get('status', '')
                    ]
                    if write_to_sheet(ws, row_data):
                        added += 1
                        logger.debug(f"    ✓ Wrote transaction: {row_id}")

            logger.info(f"✓ PrivatBank: {added} rows added")
            return added

        except ET.ParseError as e:
            logger.error(f"  ✗ Failed to parse XML: {e}")
            return 0

    except Exception as e:
        logger.error(f"✗ Error importing PrivatBank: {e}", exc_info=True)
        return 0

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Основна функція"""
    logger.info("="*60)
    logger.info("🚀 STARTING PAYMENT IMPORT")
    logger.info("="*60)

    try:
        log_initialization()

        privat_added = import_privat()
        mono_added = import_mono()
        novapay_added = import_novapay()

        logger.info("\n" + "="*60)
        logger.info("✓ IMPORT COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"  📊 Summary:")
        logger.info(f"     PrivatBank:    {privat_added} transactions")
        logger.info(f"     Monobank:      {mono_added} transactions")
        logger.info(f"     NovaPay:       {novapay_added} transactions")
        logger.info(f"     TOTAL:         {privat_added + mono_added + novapay_added} transactions")
        logger.info("="*60)

    except Exception as e:
        logger.error("="*60)
        logger.error("✗ FATAL ERROR")
        logger.error("="*60)
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("="*60)

if __name__ == "__main__":
    main()
