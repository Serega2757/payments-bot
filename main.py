import hashlib
import html
import json
import logging
import os
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo
import gspread
import requests
from google.oauth2.service_account import Credentials
# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
# =============================================================================
# WORKSHEET CACHING - PREVENTS [429] API QUOTA ERRORS
# =============================================================================
_WORKSHEET_CACHE = {}

def clear_worksheet_cache():
    """Clear the worksheet cache (call at start of main run)"""
    global _WORKSHEET_CACHE
    _WORKSHEET_CACHE = {}

def get_cached_worksheet_values(worksheet_key, fetch_func):
    """
    Get worksheet values with caching to avoid repeated API calls.

    Args:
        worksheet_key: Unique identifier for the worksheet (e.g., sheet name)
        fetch_func: Function that fetches the values (e.g., worksheet.get_all_values)

    Returns:
        List of worksheet rows
    """
    global _WORKSHEET_CACHE

    if worksheet_key not in _WORKSHEET_CACHE:
        # First call - fetch from API and cache
        _WORKSHEET_CACHE[worksheet_key] = fetch_func()

    return _WORKSHEET_CACHE[worksheet_key]

# =============================================================================
# GENERAL CONFIGURATION
# =============================================================================
SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"  # Import/Source table
CONSOLIDATION_SPREADSHEET_ID = "1_6FTp38Spb-TcZl2UWkp61TpnT26FXfu-XHeeFsj8Mw"  # Target table
KYIV_TZ = ZoneInfo("Europe/Kyiv")
IMPORT_DAYS = 30
HTTP_TIMEOUT_SECONDS = 90
HTTP_RETRIES = 5
RETRYABLE_HTTP_CODES = {
    408,
    425,
    429,
    500,
    502,
    503,
    504,
}
NON_RETRYABLE_HTTP_CODES = {
    400,
    401,
    403,
    404,
    405,
    409,
    422,
}
PRIVAT_SHEET = "Privat"
MONO_SHEET_1 = "Monobank"
MONO_SHEET_2 = "MonoBank Сергій"
NOVAPAY_SHEET_1 = "NovaPay Анастасія"
NOVAPAY_SHEET_2 = "NovaPay Сергій"
NOVAPAY_SHEET_3 = "NovaPay Олександра"
NOVAPAY_CONFIG_SHEET = "NovaPay_Config"
LOG_SHEET = "Logs"
STATE_SHEET = "Import_State"
NOVAPAY_ENDPOINT = (
    "https://business.novapay.ua/"
    "Services/ClientAPIService.svc"
)
# =============================================================================
# ENVIRONMENT VARIABLES
# =============================================================================
GOOGLE_SERVICE_ACCOUNT = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT",
    "",
).strip()
PB_ID = os.getenv("PB_ID", "").strip()
PB_TOKEN = os.getenv("PB_TOKEN", "").strip()
PB_ACC = os.getenv("PB_ACC", "").strip()
MONO_TOKEN_1 = os.getenv("MONO_TOKEN_1", "").strip()
MONO_IBAN_1 = os.getenv("MONO_IBAN_1", "").strip()
MONO_TOKEN_2 = os.getenv("MONO_TOKEN_2", "").strip()
MONO_IBAN_2 = os.getenv("MONO_IBAN_2", "").strip()
NOVAPAY_LOGIN = os.getenv(
    "NOVAPAY_LOGIN",
    "",
).strip()
NOVAPAY_REFRESH_TOKEN = os.getenv(
    "NOVAPAY_REFRESH_TOKEN",
    "",
).strip()
NOVAPAY_PUBLIC_CERTIFICATE = os.getenv(
    "NOVAPAY_PUBLIC_CERTIFICATE",
    "",
).strip()
NOVAPAY_LOGIN_2 = os.getenv(
    "NOVAPAY_LOGIN_2",
    "",
).strip()
NOVAPAY_REFRESH_TOKEN_2 = os.getenv(
    "NOVAPAY_REFRESH_TOKEN_2",
    "",
).strip()
NOVAPAY_PUBLIC_CERTIFICATE_2 = os.getenv(
    "NOVAPAY_PUBLIC_CERTIFICATE_2",
    "",
).strip()
NOVAPAY_LOGIN_3 = os.getenv(
    "NOVAPAY_LOGIN_3",
    "",
).strip()
NOVAPAY_REFRESH_TOKEN_3 = os.getenv(
    "NOVAPAY_REFRESH_TOKEN_3",
    "",
).strip()
NOVAPAY_PUBLIC_CERTIFICATE_3 = os.getenv(
    "NOVAPAY_PUBLIC_CERTIFICATE_3",
    "",
).strip()
# =============================================================================
# ACCOUNT CONFIGURATION
# =============================================================================
MONO_ACCOUNTS = [
    {
        "integration": "Monobank",
        "sheet": MONO_SHEET_1,
        "token": MONO_TOKEN_1,
        "iban": MONO_IBAN_1,
    },
    {
        "integration": "MonoBank Сергій",
        "sheet": MONO_SHEET_2,
        "token": MONO_TOKEN_2,
        "iban": MONO_IBAN_2,
    },
]
NOVAPAY_ACCOUNTS = [
    {
        "integration": "NovaPay Анастасія",
        "sheet": NOVAPAY_SHEET_1,
        "login": NOVAPAY_LOGIN,
        "config_column": 2,
        "initial_refresh_token": NOVAPAY_REFRESH_TOKEN,
        "initial_certificate": NOVAPAY_PUBLIC_CERTIFICATE,
    },
    {
        "integration": "NovaPay Сергій",
        "sheet": NOVAPAY_SHEET_2,
        "login": NOVAPAY_LOGIN_2,
        "config_column": 3,
        "initial_refresh_token": NOVAPAY_REFRESH_TOKEN_2,
        "initial_certificate": NOVAPAY_PUBLIC_CERTIFICATE_2,
    },
    {
        "integration": "NovaPay Олександра",
        "sheet": NOVAPAY_SHEET_3,
        "login": NOVAPAY_LOGIN_3,
        "config_column": 4,
        "initial_refresh_token": NOVAPAY_REFRESH_TOKEN_3,
        "initial_certificate": NOVAPAY_PUBLIC_CERTIFICATE_3,
    },
]
ALL_INTEGRATIONS = [
    "Privat",
    "Monobank",
    "MonoBank Сергій",
    "NovaPay Анастасія",
    "NovaPay Сергій",
    "NovaPay Олександра",
]
# =============================================================================
# GENERAL HELPERS
# =============================================================================
def kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)
def today_key() -> str:
    return kyiv_now().strftime("%d.%m.%Y")
def current_datetime_text() -> str:
    return kyiv_now().strftime("%d.%m.%Y %H:%M:%S")
def get_import_range() -> tuple[datetime, datetime]:
    end = kyiv_now()
    start = end - timedelta(days=IMPORT_DAYS - 1)
    return start, end
def require_value(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(
            f"Не задана обязательная переменная: {name}"
        )
def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
def truncate_text(
    value: Any,
    max_length: int = 1500,
) -> str:
    text = clean_text(value)
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."
def secret_status(value: str) -> str:
    return "✓ SET" if value else "✗ NOT SET"
def safe_fingerprint(value: Any) -> str:
    """
    Не выводит сам секрет.
    Показывает только:
    - длину;
    - первые 10 символов SHA-256.
    """
    text = clean_text(value)
    if not text:
        return "EMPTY"
    digest = hashlib.sha256(
        text.encode("utf-8")
    ).hexdigest()[:10]
    return f"len={len(text)}, sha256={digest}"
def parse_decimal(value: Any) -> float:
    text = clean_text(value)
    if not text:
        return 0.0
    text = (
        text
        .replace(" ", "")
        .replace(",", ".")
    )
    return float(text)
def parse_date_value(value: Any) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None
    patterns = [
        "%d.%m.%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue
    return None
def format_date(value: Any) -> str:
    parsed = parse_date_value(value)
    if parsed:
        return parsed.strftime("%d.%m.%Y")
    text = clean_text(value)
    if len(text) >= 10:
        possible_iso_date = text[:10]
        try:
            parsed = datetime.strptime(
                possible_iso_date,
                "%Y-%m-%d",
            )
            return parsed.strftime("%d.%m.%Y")
        except ValueError:
            pass
    return text
def local_xml_name(tag: str) -> str:
    return tag.split("}")[-1]
def direct_children_map(
    element: ET.Element,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for child in list(element):
        result[local_xml_name(child.tag)] = clean_text(
            child.text
        )
    return result
def find_xml_text(
    root: ET.Element,
    tag_name: str,
) -> str:
    for element in root.iter():
        if local_xml_name(element.tag) == tag_name:
            return clean_text(element.text)
    return ""
def find_direct_child_text(
    parent: ET.Element,
    tag_name: str,
) -> str:
    for child in list(parent):
        if local_xml_name(child.tag) == tag_name:
            return clean_text(child.text)
    return ""
def xml_to_string(root: ET.Element) -> str:
    return ET.tostring(
        root,
        encoding="unicode",
    )
# =============================================================================
# HTTP
# =============================================================================
def request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    data: bytes | str | None = None,
    json_body: Any = None,
    timeout: int = HTTP_TIMEOUT_SECONDS,
    retries: int = HTTP_RETRIES,
    allow_soap_fault_500: bool = False,
) -> requests.Response:
    delay = 5
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                data=data,
                json=json_body,
                timeout=timeout,
            )
            if (
                allow_soap_fault_500
                and response.status_code == 500
                and (
                    "<Fault" in response.text
                    or ":Fault" in response.text
                )
            ):
                return response
            if response.status_code in NON_RETRYABLE_HTTP_CODES:
                raise RuntimeError(
                    f"HTTP {response.status_code}: "
                    f"{truncate_text(response.text)}"
                )
            if response.status_code in RETRYABLE_HTTP_CODES:
                last_error = RuntimeError(
                    f"HTTP {response.status_code}: "
                    f"{truncate_text(response.text)}"
                )
                if attempt < retries:
                    retry_after = response.headers.get(
                        "Retry-After"
                    )
                    if (
                        retry_after
                        and retry_after.isdigit()
                    ):
                        wait_seconds = max(
                            int(retry_after),
                            delay,
                        )
                    else:
                        wait_seconds = delay
                    logger.warning(
                        "HTTP %s. Повтор %s/%s через %s секунд",
                        response.status_code,
                        attempt,
                        retries,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    delay = min(
                        delay * 2,
                        120,
                    )
                    continue
            response.raise_for_status()
            return response
        except RuntimeError:
            raise
        except requests.HTTPError as exc:
            status_code = (
                exc.response.status_code
                if exc.response is not None
                else None
            )
            response_text = (
                exc.response.text
                if exc.response is not None
                else str(exc)
            )
            if status_code in NON_RETRYABLE_HTTP_CODES:
                raise RuntimeError(
                    f"HTTP {status_code}: "
                    f"{truncate_text(response_text)}"
                ) from exc
            last_error = exc
            if attempt >= retries:
                break
            logger.warning(
                "HTTP error. Retry %s/%s in %s seconds: %s",
                attempt,
                retries,
                delay,
                exc,
            )
            time.sleep(delay)
            delay = min(
                delay * 2,
                120,
            )
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            logger.warning(
                "Network error. Retry %s/%s in %s seconds: %s",
                attempt,
                retries,
                delay,
                exc,
            )
            time.sleep(delay)
            delay = min(
                delay * 2,
                120,
            )
    raise RuntimeError(
        f"HTTP request failed after {retries} attempts: "
        f"{last_error}"
    )
# =============================================================================
# GOOGLE SHEETS
# =============================================================================
@lru_cache(maxsize=1)
def get_spreadsheet() -> gspread.Spreadsheet:
    require_value(
        "GOOGLE_SERVICE_ACCOUNT",
        GOOGLE_SERVICE_ACCOUNT,
    )
    try:
        service_account_info = json.loads(
            GOOGLE_SERVICE_ACCOUNT
        )
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT содержит "
            "невалидный JSON"
        ) from exc
    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(credentials)
    return client.open_by_key(SPREADSHEET_ID)
def get_or_create_worksheet(
    sheet_name: str,
    *,
    rows: int = 2000,
    cols: int = 20,
) -> gspread.Worksheet:
    spreadsheet = get_spreadsheet()
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        logger.warning(
            "Worksheet '%s' not found. Creating.",
            sheet_name,
        )
        return spreadsheet.add_worksheet(
            title=sheet_name,
            rows=rows,
            cols=cols,
        )
def append_rows_batch(
    worksheet: gspread.Worksheet,
    rows: list[list[Any]],
) -> None:
    if not rows:
        return
    worksheet.append_rows(
        rows,
        value_input_option="USER_ENTERED",
    )
def load_existing_values(
    worksheet: gspread.Worksheet,
    column_number: int,
    *,
    has_header: bool = True,
) -> set[str]:
    values = worksheet.col_values(column_number)
    if has_header and values:
        values = values[1:]
    return {
        clean_text(value)
        for value in values
        if clean_text(value)
    }
# =============================================================================
# SHEET HEADERS
# =============================================================================
def ensure_privat_header(
    worksheet: gspread.Worksheet,
) -> None:
    values = worksheet.get_all_values()
    if values:
        return
    worksheet.append_row([
        "№",
        "Дата проводки",
        "Тип операції",
        "Сума",
        "Валюта",
        "Назва контрагента",
        "Призначення платежу",
        "Рахунок контрагента",
    ])
def ensure_mono_header(
    worksheet: gspread.Worksheet,
) -> None:
    values = worksheet.get_all_values()
    if values:
        return
    worksheet.append_row([
        "id",
        "iban",
        "time",
        "description",
        "amount",
        "direction",
        "currencyCode",
        "balance",
        "mcc",
        "comment",
        "counterEdrpou",
        "counterIban",
    ])
def ensure_novapay_header(
    worksheet: gspread.Worksheet,
) -> None:
    expected_header = [
        "Дата платежу",
        "Номер транзакції",
        "Сума",
        "Тип",
        "Призначення платежу",
        "Контрагент",
    ]
    values = worksheet.get_all_values()
    if not values:
        worksheet.append_row(expected_header)
        return
    current_header = values[0]
    if current_header[:6] != expected_header:
        worksheet.update(
            range_name="A1:F1",
            values=[expected_header],
        )
def ensure_logs_header(
    worksheet: gspread.Worksheet,
) -> None:
    expected_header = [
        "Дата виконання",
        "Privat",
        "Monobank",
        "MonoBank Сергій",
        "NovaPay Анастасія",
        "NovaPay Сергій",
        "NovaPay Олександра",
        "Статус",
    ]
    values = worksheet.get_all_values()
    if not values:
        worksheet.append_row(expected_header)
        return
    if values[0][:8] != expected_header:
        worksheet.update(
            range_name="A1:H1",
            values=[expected_header],
        )
def ensure_state_header(
    worksheet: gspread.Worksheet,
) -> None:
    expected_header = [
        "Дата",
        "Интеграция",
        "Статус",
        "Добавлено строк",
        "Время обновления",
        "Ошибка",
    ]
    values = worksheet.get_all_values()
    if not values:
        worksheet.append_row(expected_header)
        return
    if values[0][:6] != expected_header:
        worksheet.update(
            range_name="A1:F1",
            values=[expected_header],
        )
# =============================================================================
# DAILY INTEGRATION STATE (WITH CACHING)
# =============================================================================
def get_state_sheet() -> gspread.Worksheet:
    worksheet = get_or_create_worksheet(
        STATE_SHEET,
        rows=5000,
        cols=10,
    )
    ensure_state_header(worksheet)
    return worksheet
def find_today_state_row(
    integration_name: str,
) -> tuple[int | None, list[str] | None]:
    worksheet = get_state_sheet()
    # Use cache to avoid repeated API calls
    rows = get_cached_worksheet_values(
        STATE_SHEET,
        lambda: worksheet.get_all_values()
    )
    date_value = today_key()
    for row_number in range(
        len(rows),
        1,
        -1,
    ):
        row = rows[row_number - 1]
        if len(row) < 3:
            continue
        row_date = clean_text(row[0])
        row_integration = clean_text(row[1])
        if (
            row_date == date_value
            and row_integration == integration_name
        ):
            return row_number, row
    return None, None
def integration_succeeded_today(
    integration_name: str,
) -> bool:
    _, row = find_today_state_row(
        integration_name
    )
    if not row or len(row) < 3:
        return False
    return clean_text(row[2]).upper() == "OK"
def save_integration_state(
    integration_name: str,
    status: str,
    added_rows: int,
    error: str = "",
) -> None:
    worksheet = get_state_sheet()
    row_number, _ = find_today_state_row(
        integration_name
    )
    row_values = [
        today_key(),
        integration_name,
        status,
        added_rows,
        current_datetime_text(),
        truncate_text(error, 40000),
    ]
    if row_number is None:
        worksheet.append_row(
            row_values,
            value_input_option="USER_ENTERED",
        )
        return
    worksheet.update(
        range_name=f"A{row_number}:F{row_number}",
        values=[row_values],
    )
def all_integrations_succeeded_today() -> bool:
    return all(
        integration_succeeded_today(name)
        for name in ALL_INTEGRATIONS
    )
# =============================================================================
# GENERAL EXECUTION LOG
# =============================================================================
def write_execution_log(
    results: dict[str, int],
    status: str,
) -> None:
    worksheet = get_or_create_worksheet(
        LOG_SHEET,
        rows=5000,
        cols=10,
    )
    ensure_logs_header(worksheet)
    worksheet.append_row(
        [
            current_datetime_text(),
            results.get("Privat", 0),
            results.get("Monobank", 0),
            results.get("MonoBank Сергій", 0),
            results.get("NovaPay Анастасія", 0),
            results.get("NovaPay Сергій", 0),
            results.get("NovaPay Олександра", 0),
            truncate_text(status, 40000),
        ],
        value_input_option="USER_ENTERED",
    )
# =============================================================================
# PRIVATBANK
# =============================================================================
def build_privat_transaction_id(
    transaction: dict[str, Any],
) -> str:
    parts = [
        clean_text(transaction.get("REF")),
        clean_text(transaction.get("REFN")),
        clean_text(
            transaction.get(
                "DATE_TIME_DAT_OD_TIM_P"
            )
        ),
        clean_text(transaction.get("SUM")),
    ]
    if not any(parts):
        return ""
    return "_".join(parts)
def privat_sort_key(
    transaction: dict[str, Any],
) -> datetime:
    parsed = parse_date_value(
        transaction.get(
            "DATE_TIME_DAT_OD_TIM_P",
            "",
        )
    )
    return parsed or datetime.min
def import_privatbank() -> int:
    require_value("PB_ID", PB_ID)
    require_value("PB_TOKEN", PB_TOKEN)
    require_value("PB_ACC", PB_ACC)
    logger.info("")
    logger.info("🏦 Processing PrivatBank")
    worksheet = get_or_create_worksheet(
        PRIVAT_SHEET,
        rows=5000,
        cols=10,
    )
    ensure_privat_header(worksheet)
    existing_ids = load_existing_values(
        worksheet,
        1,
    )
    start, end = get_import_range()
    response = request_with_retry(
        "GET",
        (
            "https://acp.privatbank.ua/"
            "api/statements/transactions"
        ),
        params={
            "acc": PB_ACC,
            "startDate": start.strftime("%d-%m-%Y"),
            "endDate": end.strftime("%d-%m-%Y"),
            "limit": 500,
        },
        headers={
            "id": PB_ID,
            "token": PB_TOKEN,
            "Accept": "application/json",
            "User-Agent": "payments-bot/2.0",
        },
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "PrivatBank returned non-JSON response: "
            f"{truncate_text(response.text)}"
        ) from exc
    if payload.get("status") != "SUCCESS":
        raise RuntimeError(
            "PrivatBank API error: "
            f"{truncate_text(json.dumps(payload, ensure_ascii=False))}"
        )
    transactions = payload.get("transactions")
    if not isinstance(transactions, list):
        raise RuntimeError(
            "PrivatBank response has no transactions list"
        )
    rows: list[list[Any]] = []
    for transaction in sorted(
        transactions,
        key=privat_sort_key,
    ):
        transaction_id = build_privat_transaction_id(
            transaction
        )
        if not transaction_id:
            logger.warning(
                "PrivatBank transaction without ID skipped"
            )
            continue
        if transaction_id in existing_ids:
            continue
        rows.append([
            transaction_id,
            format_date(
                transaction.get(
                    "DATE_TIME_DAT_OD_TIM_P",
                    "",
                )
            ),
            clean_text(
                transaction.get("TRANTYPE")
            ),
            parse_decimal(
                transaction.get("SUM")
            ),
            clean_text(
                transaction.get("CCY")
            ),
            clean_text(
                transaction.get("AUT_CNTR_NAM")
            ),
            clean_text(
                transaction.get("OSND")
            ),
            clean_text(
                transaction.get("AUT_CNTR_ACC")
            ),
        ])
        existing_ids.add(transaction_id)
    append_rows_batch(
        worksheet,
        rows,
    )
    logger.info(
        "✓ PrivatBank: added %s row(s)",
        len(rows),
    )
    return len(rows)
# =============================================================================
# MONOBANK
# =============================================================================
def get_monobank_account_id(
    token: str,
    iban: str,
) -> str:
    response = request_with_retry(
        "GET",
        "https://api.monobank.ua/personal/client-info",
        headers={
            "X-Token": token,
            "Accept": "application/json",
            "User-Agent": "payments-bot/2.0",
        },
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "Monobank client-info returned non-JSON"
        ) from exc
    accounts = payload.get("accounts", [])
    for account in accounts:
        if clean_text(account.get("iban")) == iban:
            account_id = clean_text(
                account.get("id")
            )
            if not account_id:
                raise RuntimeError(
                    f"Monobank account {iban} has no id"
                )
            return account_id
    available_ibans = [
        clean_text(account.get("iban"))
        for account in accounts
        if clean_text(account.get("iban"))
    ]
    raise RuntimeError(
        f"Monobank IBAN {iban} not found. "
        f"Available: {available_ibans}"
    )
def get_monobank_statements(
    token: str,
    account_id: str,
) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc)
    from_utc = now_utc - timedelta(
        days=IMPORT_DAYS - 1
    )
    from_timestamp = int(
        from_utc.timestamp()
    )
    to_timestamp = int(
        now_utc.timestamp()
    )
    response = request_with_retry(
        "GET",
        (
            "https://api.monobank.ua/"
            f"personal/statement/{account_id}/"
            f"{from_timestamp}/{to_timestamp}"
        ),
        headers={
            "X-Token": token,
            "Accept": "application/json",
            "User-Agent": "payments-bot/2.0",
        },
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "Monobank statement returned non-JSON"
        ) from exc
    if not isinstance(payload, list):
        raise RuntimeError(
            "Monobank statement returned unexpected data: "
            f"{truncate_text(json.dumps(payload, ensure_ascii=False))}"
        )
    return payload
def build_mono_transaction_id(
    iban: str,
    transaction: dict[str, Any],
) -> str:
    original_id = clean_text(
        transaction.get("id")
    )
    if original_id:
        return f"{iban}_{original_id}"
    fallback = "_".join([
        clean_text(transaction.get("time")),
        clean_text(transaction.get("amount")),
        clean_text(
            transaction.get("description")
        ),
    ])
    if not fallback.replace("_", ""):
        return ""
    return f"{iban}_{fallback}"
def import_monobank_account(
    account: dict[str, str],
) -> int:
    integration_name = account["integration"]
    sheet_name = account["sheet"]
    token = clean_text(account["token"])
    iban = clean_text(account["iban"])
    require_value(
        f"{integration_name} token",
        token,
    )
    require_value(
        f"{integration_name} IBAN",
        iban,
    )
    logger.info("")
    logger.info(
        "📱 Processing Monobank: %s",
        integration_name,
    )
    logger.info(
        "  Token fingerprint: %s",
        safe_fingerprint(token),
    )
    logger.info(
        "  IBAN fingerprint: %s",
        safe_fingerprint(iban),
    )
    account_id = get_monobank_account_id(
        token,
        iban,
    )
    statements = get_monobank_statements(
        token,
        account_id,
    )
    worksheet = get_or_create_worksheet(
        sheet_name,
        rows=5000,
        cols=15,
    )
    ensure_mono_header(worksheet)
    existing_ids = load_existing_values(
        worksheet,
        1,
    )
    rows_with_timestamp: list[
        tuple[int, list[Any]]
    ] = []
    for transaction in statements:
        transaction_id = build_mono_transaction_id(
            iban,
            transaction,
        )
        if not transaction_id:
            logger.warning(
                "%s transaction without ID skipped",
                integration_name,
            )
            continue
        if transaction_id in existing_ids:
            continue
        timestamp = int(
            transaction.get("time", 0) or 0
        )
        transaction_datetime = datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        ).astimezone(KYIV_TZ)
        amount = (
            parse_decimal(
                transaction.get("amount", 0)
            )
            / 100
        )
        balance = (
            parse_decimal(
                transaction.get("balance", 0)
            )
            / 100
        )
        currency_code = transaction.get(
            "currencyCode"
        )
        if currency_code is None:
            currency_code = transaction.get(
                "currency",
                "",
            )
        row = [
            transaction_id,
            iban,
            transaction_datetime.strftime(
                "%d.%m.%Y"
            ),
            clean_text(
                transaction.get("description")
            ),
            amount,
            "IN" if amount >= 0 else "OUT",
            clean_text(currency_code),
            balance,
            clean_text(
                transaction.get("mcc")
            ),
            clean_text(
                transaction.get("comment")
            ),
            clean_text(
                transaction.get("counterEdrpou")
            ),
            clean_text(
                transaction.get("counterIban")
            ),
        ]
        rows_with_timestamp.append(
            (timestamp, row)
        )
        existing_ids.add(transaction_id)
    rows_with_timestamp.sort(
        key=lambda item: item[0]
    )
    rows = [
        row
        for _, row in rows_with_timestamp
    ]
    append_rows_batch(
        worksheet,
        rows,
    )
    logger.info(
        "✓ %s: added %s row(s)",
        integration_name,
        len(rows),
    )
    return len(rows)
# =============================================================================
# NOVAPAY CONFIG (WITH CACHING)
# =============================================================================
def ensure_novapay_config_sheet() -> gspread.Worksheet:
    """Get or create NovaPay_Config sheet - uses caching to avoid API limit"""
    worksheet = get_or_create_worksheet(
        NOVAPAY_CONFIG_SHEET,
        rows=20,
        cols=5,
    )
    expected_header = [
        "key",
        NOVAPAY_SHEET_1,
        NOVAPAY_SHEET_2,
        NOVAPAY_SHEET_3,
    ]
    # Use cache to avoid repeated API calls
    values = get_cached_worksheet_values(
        NOVAPAY_CONFIG_SHEET,
        lambda: worksheet.get_all_values()
    )

    if not values:
        worksheet.update(
            range_name="A1:D3",
            values=[
                expected_header,
                [
                    "refresh_token",
                    NOVAPAY_REFRESH_TOKEN,
                    NOVAPAY_REFRESH_TOKEN_2,
                    NOVAPAY_REFRESH_TOKEN_3,
                ],
                [
                    "public_certificate",
                    NOVAPAY_PUBLIC_CERTIFICATE,
                    NOVAPAY_PUBLIC_CERTIFICATE_2,
                    NOVAPAY_PUBLIC_CERTIFICATE_3,
                ],
            ],
        )
        return worksheet
    current_header = values[0]
    if current_header[:4] == expected_header:
        return worksheet
    legacy_refresh_token = ""
    legacy_certificate = ""
    for row in values:
        if len(row) < 2:
            continue
        key = clean_text(row[0]).lower()
        value = clean_text(row[1])
        if key == "refresh_token":
            legacy_refresh_token = value
        elif key == "public_certificate":
            legacy_certificate = value
    worksheet.clear()
    worksheet.update(
        range_name="A1:D3",
        values=[
            expected_header,
            [
                "refresh_token",
                (
                    legacy_refresh_token
                    or NOVAPAY_REFRESH_TOKEN
                ),
                NOVAPAY_REFRESH_TOKEN_2,
                NOVAPAY_REFRESH_TOKEN_3,
            ],
            [
                "public_certificate",
                (
                    legacy_certificate
                    or NOVAPAY_PUBLIC_CERTIFICATE
                ),
                NOVAPAY_PUBLIC_CERTIFICATE_2,
                NOVAPAY_PUBLIC_CERTIFICATE_3,
            ],
        ],
    )
    return worksheet
def read_novapay_credentials(
    account: dict[str, Any],
) -> tuple[str, str]:
    worksheet = ensure_novapay_config_sheet()
    column_number = int(
        account["config_column"]
    )
    sheet_refresh_token = clean_text(
        worksheet.cell(
            2,
            column_number,
        ).value
    )
    sheet_certificate = clean_text(
        worksheet.cell(
            3,
            column_number,
        ).value
    )
    initial_refresh_token = clean_text(
        account.get("initial_refresh_token")
    )
    initial_certificate = clean_text(
        account.get("initial_certificate")
    )
    if sheet_refresh_token:
        refresh_token = sheet_refresh_token
        refresh_source = (
            f"{NOVAPAY_CONFIG_SHEET}!"
            f"{gspread.utils.rowcol_to_a1(2, column_number)}"
        )
    else:
        refresh_token = initial_refresh_token
        refresh_source = "GitHub Secret"
    if sheet_certificate:
        certificate = sheet_certificate
        certificate_source = (
            f"{NOVAPAY_CONFIG_SHEET}!"
            f"{gspread.utils.rowcol_to_a1(3, column_number)}"
        )
    else:
        certificate = initial_certificate
        certificate_source = "GitHub Secret"
    logger.info(
        "  %s: Refresh Token source: %s",
        account["integration"],
        refresh_source,
    )
    logger.info(
        "  %s: Certificate source: %s",
        account["integration"],
        certificate_source,
    )
    if not refresh_token:
        raise RuntimeError(
            f"{account['integration']}: "
            "Refresh Token is missing"
        )
    if not certificate:
        raise RuntimeError(
            f"{account['integration']}: "
            "Public Certificate is missing"
        )
    return refresh_token, certificate
def save_novapay_credentials(
    account: dict[str, Any],
    refresh_token: str,
    certificate: str,
) -> None:
    worksheet = ensure_novapay_config_sheet()
    column_number = int(
        account["config_column"]
    )
    worksheet.update(
        range_name=(
            f"{gspread.utils.rowcol_to_a1(2, column_number)}:"
            f"{gspread.utils.rowcol_to_a1(3, column_number)}"
        ),
        values=[
            [refresh_token],
            [certificate],
        ],
    )
    logger.info(
        "  ✓ %s: rotated NovaPay credentials saved",
        account["integration"],
    )
# =============================================================================
# NOVAPAY SOAP
# =============================================================================
def build_soap_envelope(
    method_name: str,
    request_body: str,
) -> str:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:tem="http://tempuri.org/">
  <soapenv:Header/>
  <soapenv:Body>
    <tem:{method_name}>
      {request_body}
    </tem:{method_name}>
  </soapenv:Body>
</soapenv:Envelope>"""
def parse_soap_response(
    response: requests.Response,
    method_name: str,
) -> ET.Element:
    response_text = response.text or ""
    if not response_text.strip():
        raise RuntimeError(
            f"NovaPay {method_name}: empty response"
        )
    try:
        root = ET.fromstring(response_text)
    except ET.ParseError as exc:
        raise RuntimeError(
            f"NovaPay {method_name}: invalid XML: "
            f"{truncate_text(response_text)}"
        ) from exc
    fault = None
    for element in root.iter():
        if local_xml_name(element.tag) == "Fault":
            fault = element
            break
    if fault is not None:
        fault_text = (
            find_xml_text(fault, "faultstring")
            or find_xml_text(fault, "Text")
            or xml_to_string(fault)
        )
        raise RuntimeError(
            f"NovaPay {method_name} SOAP Fault: "
            f"{truncate_text(fault_text)}"
        )
    return root
def novapay_soap_call(
    method_name: str,
    request_body: str,
) -> ET.Element:
    envelope = build_soap_envelope(
        method_name,
        request_body,
    )
    soap_action = (
        "http://tempuri.org/"
        f"IClientAPIService/{method_name}"
    )
    response = request_with_retry(
        "POST",
        NOVAPAY_ENDPOINT,
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f'"{soap_action}"',
            "Accept": "text/xml",
            "User-Agent": "payments-bot/2.0",
        },
        data=envelope.encode("utf-8"),
        allow_soap_fault_500=True,
    )
    return parse_soap_response(
        response,
        method_name,
    )
def novapay_check_result(
    root: ET.Element,
    method_name: str,
) -> None:
    result = find_xml_text(
        root,
        "result",
    )
    if not result:
        return
    if result.lower() == "ok":
        return
    error_status = find_xml_text(
        root,
        "status",
    )
    error_title = find_xml_text(
        root,
        "title",
    )
    error_details = " | ".join(
        value
        for value in [
            error_status,
            error_title,
        ]
        if value
    )
    if not error_details:
        error_details = truncate_text(
            xml_to_string(root)
        )
    raise RuntimeError(
        f"NovaPay {method_name}: {error_details}"
    )
def novapay_authenticate(
    account: dict[str, Any],
) -> str:
    login = clean_text(
        account["login"]
    )
    require_value(
        f"{account['integration']} login",
        login,
    )
    refresh_token, certificate = (
        read_novapay_credentials(account)
    )
    logger.info(
        "  NovaPay credentials for %s:",
        account["integration"],
    )
    logger.info(
        "    login: %s",
        safe_fingerprint(login),
    )
    logger.info(
        "    refresh_token: %s",
        safe_fingerprint(refresh_token),
    )
    logger.info(
        "    certificate: %s",
        safe_fingerprint(certificate),
    )
    logger.info(
        "    config column: %s",
        account["config_column"],
    )
    request_body = f"""
<tem:request>
  <tem:request_ref>{html.escape(str(uuid.uuid4()))}</tem:request_ref>
  <tem:refresh_token>{html.escape(refresh_token)}</tem:refresh_token>
  <tem:login>{html.escape(login)}</tem:login>
  <tem:public_certificate>{html.escape(certificate)}</tem:public_certificate>
</tem:request>
"""
    root = novapay_soap_call(
        "UserAuthenticationJWT",
        request_body,
    )
    novapay_check_result(
        root,
        "UserAuthenticationJWT",
    )
    jwt = find_xml_text(
        root,
        "jwt",
    )
    new_refresh_token = find_xml_text(
        root,
        "refresh_token",
    )
    new_certificate = find_xml_text(
        root,
        "public_certificate",
    )
    if not jwt:
        raise RuntimeError(
            f"{account['integration']}: "
            "NovaPay did not return JWT"
        )
    if not new_refresh_token:
        raise RuntimeError(
            f"{account['integration']}: "
            "NovaPay did not return new Refresh Token"
        )
    if not new_certificate:
        raise RuntimeError(
            f"{account['integration']}: "
            "NovaPay did not return new certificate"
        )
    # Сохраняем сразу после успешной ротации.
    # Старый Refresh Token уже недействителен.
    save_novapay_credentials(
        account,
        new_refresh_token,
        new_certificate,
    )
    return jwt
def find_client_records(
    root: ET.Element,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for element in root.iter():
        children = direct_children_map(element)
        client_id = children.get("id", "")
        client_name = children.get("name", "")
        state_code = children.get("statecode", "")
        if client_id and (
            client_name
            or state_code
        ):
            records.append(children)
    unique_records: dict[
        str,
        dict[str, str],
    ] = {}
    for record in records:
        record_id = clean_text(
            record.get("id")
        )
        if record_id:
            unique_records[record_id] = record
    return list(
        unique_records.values()
    )
def novapay_get_client_id(
    jwt: str,
    integration_name: str,
) -> str:
    request_body = f"""
<tem:request>
  <tem:request_ref>{html.escape(str(uuid.uuid4()))}</tem:request_ref>
  <tem:jwt>{html.escape(jwt)}</tem:jwt>
</tem:request>
"""
    root = novapay_soap_call(
        "GetClientsList",
        request_body,
    )
    novapay_check_result(
        root,
        "GetClientsList",
    )
    clients = find_client_records(root)
    if not clients:
        raise RuntimeError(
            f"{integration_name}: "
            "NovaPay returned no clients"
        )
    if len(clients) > 1:
        logger.warning(
            "%s: found %s clients. Using first: %s",
            integration_name,
            len(clients),
            clients[0],
        )
    client_id = clean_text(
        clients[0].get("id")
    )
    if not client_id:
        raise RuntimeError(
            f"{integration_name}: "
            "NovaPay client has no id"
        )
    return client_id
def find_account_records(
    root: ET.Element,
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for element in root.iter():
        children = direct_children_map(element)
        account_id = children.get("id", "")
        iban = (
            children.get("IBAN", "")
            or children.get("iban", "")
        )
        if account_id and iban:
            records.append(children)
    unique_records: dict[
        str,
        dict[str, str],
    ] = {}
    for record in records:
        record_id = clean_text(
            record.get("id")
        )
        if record_id:
            unique_records[record_id] = record
    return list(
        unique_records.values()
    )
def novapay_get_account_id(
    jwt: str,
    client_id: str,
    integration_name: str,
) -> str:
    request_body = f"""
<tem:request>
  <tem:request_ref>{html.escape(str(uuid.uuid4()))}</tem:request_ref>
  <tem:jwt>{html.escape(jwt)}</tem:jwt>
  <tem:client_id>{html.escape(client_id)}</tem:client_id>
</tem:request>
"""
    root = novapay_soap_call(
        "GetAccountsList",
        request_body,
    )
    novapay_check_result(
        root,
        "GetAccountsList",
    )
    accounts = find_account_records(root)
    if not accounts:
        raise RuntimeError(
            f"{integration_name}: "
            "NovaPay returned no accounts"
        )
    active_accounts = []
    for item in accounts:
        status_code = clean_text(
            item.get("statuscode")
        ).lower()
        if (
            not status_code
            or status_code == "active"
        ):
            active_accounts.append(item)
    selected_accounts = (
        active_accounts
        if active_accounts
        else accounts
    )
    if len(selected_accounts) > 1:
        logger.warning(
            "%s: found %s accounts. Using first: %s",
            integration_name,
            len(selected_accounts),
            selected_accounts[0],
        )
    account_id = clean_text(
        selected_accounts[0].get("id")
    )
    if not account_id:
        raise RuntimeError(
            f"{integration_name}: "
            "NovaPay account has no id"
        )
    return account_id
def novapay_get_payment_elements(
    jwt: str,
    account_id: str,
    integration_name: str,
) -> list[ET.Element]:
    start, end = get_import_range()
    request_body = f"""
<tem:request>
  <tem:request_ref>{html.escape(str(uuid.uuid4()))}</tem:request_ref>
  <tem:jwt>{html.escape(jwt)}</tem:jwt>
  <tem:account_id>{html.escape(account_id)}</tem:account_id>
  <tem:date_from>{start.strftime("%d.%m.%Y")}</tem:date_from>
  <tem:date_to>{end.strftime("%d.%m.%Y")}</tem:date_to>
  <tem:date_type>0</tem:date_type>
</tem:request>
"""
    root = novapay_soap_call(
        "GetPaymentsList",
        request_body,
    )
    result = find_xml_text(
        root,
        "result",
    )
    if result and result.lower() != "ok":
        error_status = find_xml_text(
            root,
            "status",
        )
        error_title = find_xml_text(
            root,
            "title",
        )
        combined_error = (
            f"{error_status} {error_title}"
        ).lower()
        no_documents_indicators = [
            "no documents",
            "not found",
            "відсутні платежі",
            "документи відсутні",
        ]
        if any(
            indicator in combined_error
            for indicator in no_documents_indicators
        ):
            return []
        novapay_check_result(
            root,
            "GetPaymentsList",
        )
    payments_xml = find_xml_text(
        root,
        "payments",
    )
    if not payments_xml:
        return []
    try:
        payments_root = ET.fromstring(
            payments_xml
        )
    except ET.ParseError as exc:
        raise RuntimeError(
            f"{integration_name}: "
            "NovaPay payments contains invalid XML: "
            f"{truncate_text(payments_xml)}"
        ) from exc
    return [
        element
        for element in payments_root.iter()
        if local_xml_name(element.tag) == "Docs"
    ]
def novapay_payment_date(
    document: ET.Element,
) -> str:
    date_value = (
        find_direct_child_text(
            document,
            "DayDate",
        )
        or find_direct_child_text(
            document,
            "OrgDate",
        )
        or find_direct_child_text(
            document,
            "PayDate",
        )
    )
    return format_date(date_value)
def novapay_payment_sort_key(
    document: ET.Element,
) -> datetime:
    date_value = (
        find_direct_child_text(
            document,
            "DayDate",
        )
        or find_direct_child_text(
            document,
            "OrgDate",
        )
        or find_direct_child_text(
            document,
            "PayDate",
        )
    )
    return (
        parse_date_value(date_value)
        or datetime.min
    )
def novapay_counterparty_name(
    document: ET.Element,
    payment_type: str,
) -> str:
    debit_name = find_direct_child_text(
        document,
        "DebitName",
    )
    credit_name = find_direct_child_text(
        document,
        "CreditName",
    )
    normalized_type = clean_text(
        payment_type
    ).lower()
    # Debit — деньги списаны с нашего счёта.
    # Контрагентом является получатель.
    if normalized_type == "debit":
        return credit_name or debit_name
    # Credit — деньги поступили на наш счёт.
    # Контрагентом является плательщик.
    if normalized_type == "credit":
        return debit_name or credit_name
    return debit_name or credit_name
def import_novapay_account(
    account: dict[str, Any],
) -> int:
    integration_name = account["integration"]
    logger.info("")
    logger.info(
        "💳 Processing NovaPay: %s",
        integration_name,
    )
    worksheet = get_or_create_worksheet(
        account["sheet"],
        rows=5000,
        cols=10,
    )
    ensure_novapay_header(worksheet)
    # NovaPay duplicate check is by column B.
    existing_codes = load_existing_values(
        worksheet,
        2,
    )
    jwt = novapay_authenticate(account)
    client_id = novapay_get_client_id(
        jwt,
        integration_name,
    )
    account_id = novapay_get_account_id(
        jwt,
        client_id,
        integration_name,
    )
    documents = novapay_get_payment_elements(
        jwt,
        account_id,
        integration_name,
    )
    rows: list[list[Any]] = []
    for document in sorted(
        documents,
        key=novapay_payment_sort_key,
    ):
        code = find_direct_child_text(
            document,
            "Code",
        )
        if not code:
            logger.warning(
                "%s payment without Code skipped",
                integration_name,
            )
            continue
        if code in existing_codes:
            continue
        payment_date = novapay_payment_date(
            document
        )
        amount = parse_decimal(
            document.attrib.get("Amount", 0)
        )
        payment_type = find_direct_child_text(
            document,
            "PaymentType",
        )
        purpose = find_direct_child_text(
            document,
            "Purpose",
        )
        counterparty_name = novapay_counterparty_name(
            document,
            payment_type,
        )
        rows.append([
            payment_date,
            code,
            amount,
            payment_type,
            purpose,
            counterparty_name,
        ])
        existing_codes.add(code)
    append_rows_batch(
        worksheet,
        rows,
    )
    logger.info(
        "✓ %s: added %s row(s)",
        integration_name,
        len(rows),
    )
    return len(rows)
# =============================================================================
# INITIALIZATION LOG
# =============================================================================
def log_initialization() -> None:
    logger.info(
        "📋 ENVIRONMENT VARIABLES CHECK:"
    )
    variables = [
        ("MONO_TOKEN_1", MONO_TOKEN_1),
        ("MONO_IBAN_1", MONO_IBAN_1),
        ("MONO_TOKEN_2", MONO_TOKEN_2),
        ("MONO_IBAN_2", MONO_IBAN_2),
        ("NOVAPAY_LOGIN", NOVAPAY_LOGIN),
        (
            "NOVAPAY_PUBLIC_CERTIFICATE",
            NOVAPAY_PUBLIC_CERTIFICATE,
        ),
        (
            "NOVAPAY_REFRESH_TOKEN",
            NOVAPAY_REFRESH_TOKEN,
        ),
        (
            "NOVAPAY_LOGIN_2",
            NOVAPAY_LOGIN_2,
        ),
        (
            "NOVAPAY_PUBLIC_CERTIFICATE_2",
            NOVAPAY_PUBLIC_CERTIFICATE_2,
        ),
        (
            "NOVAPAY_REFRESH_TOKEN_2",
            NOVAPAY_REFRESH_TOKEN_2,
        ),
        (
            "NOVAPAY_LOGIN_3",
            NOVAPAY_LOGIN_3,
        ),
        (
            "NOVAPAY_PUBLIC_CERTIFICATE_3",
            NOVAPAY_PUBLIC_CERTIFICATE_3,
        ),
        (
            "NOVAPAY_REFRESH_TOKEN_3",
            NOVAPAY_REFRESH_TOKEN_3,
        ),
        ("PB_ID", PB_ID),
        ("PB_TOKEN", PB_TOKEN),
        ("PB_ACC", PB_ACC),
        (
            "GOOGLE_SERVICE_ACCOUNT",
            GOOGLE_SERVICE_ACCOUNT,
        ),
    ]
    for name, value in variables:
        logger.info(
            "  %-34s %s",
            f"{name}:",
            secret_status(value),
        )
    logger.info("")
    logger.info(
        "📦 ACCOUNTS CONFIGURATION:"
    )
    for account in MONO_ACCOUNTS:
        logger.info(
            "  %s: token=%s, iban=%s",
            account["integration"],
            bool(account["token"]),
            bool(account["iban"]),
        )
    for account in NOVAPAY_ACCOUNTS:
        logger.info(
            "  %s: login=%s, initial_token=%s, "
            "initial_certificate=%s, config_column=%s",
            account["integration"],
            bool(account["login"]),
            bool(account["initial_refresh_token"]),
            bool(account["initial_certificate"]),
            account["config_column"],
        )
# =============================================================================
# TASK RUNNER
# =============================================================================
def run_integration(
    integration_name: str,
    task_function,
    results: dict[str, int],
    errors: list[str],
) -> None:
    """
    Запустить интеграцию с обработкой ошибок.
    Ошибка в одной интеграции НЕ блокирует другие.
    """
    if integration_succeeded_today(
        integration_name
    ):
        logger.info("")
        logger.info(
            "⏭ %s already completed successfully today. Skipping.",
            integration_name,
        )
        results[integration_name] = 0
        return
    try:
        added_rows = task_function()
        results[integration_name] = added_rows
        save_integration_state(
            integration_name=integration_name,
            status="OK",
            added_rows=added_rows,
            error="",
        )
    except Exception as exc:
        error_message = (
            f"{integration_name}: "
            f"{type(exc).__name__}: {exc}"
        )
        errors.append(error_message)
        results[integration_name] = 0  # Важно: установить 0 для ошибочных интеграций
        save_integration_state(
            integration_name=integration_name,
            status="ERROR",
            added_rows=0,
            error=error_message,
        )
        logger.exception(
            "✗ %s failed",
            integration_name,
        )
        # ВАЖНО: НЕ прерываем программу - продолжаем обработку других интеграций
# =============================================================================
# MAIN
# =============================================================================
# =============================================================================
# CASH FLOW CONSOLIDATION - NEW UNIFIED FUNCTIONS
# =============================================================================

TARGET_SHEET_GID = 2132203111  # "Грошовий Потік" in Table 2
BASE_SHEET_NAME = "База"
LOOKUP_TABLE_HEADER_ROW = 260

# Source sheets configuration
CONSOLIDATION_SOURCES = [
    {"sheet": "NovaPay Анастасія", "mark_col": 7, "type": "novapay"},
    {"sheet": "NovaPay Сергій", "mark_col": 7, "type": "novapay"},
    {"sheet": "NovaPay Олександра", "mark_col": 7, "type": "novapay"},
    {"sheet": "Monobank", "mark_col": 13, "type": "monoA"},
    {"sheet": "MonoBank Сергій", "mark_col": 13, "type": "monoS"},
]

# Global cache for lookup tables
_lookup_tables_cache = None

def clear_lookup_tables_cache():
    """Clear the lookup tables cache"""
    global _lookup_tables_cache
    _lookup_tables_cache = None

def load_lookup_tables_():
    """Load classification tables from 'База' sheet in Table 2"""
    global _lookup_tables_cache
    if _lookup_tables_cache:
        return _lookup_tables_cache

    try:
        # Load from consolidation table (Table 2)
        creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT)
        creds = Credentials.from_service_account_info(creds_dict)
        client = gspread.authorize(creds)
        ss = client.open_by_key(CONSOLIDATION_SPREADSHEET_ID)

        base_sheet = ss.worksheet(BASE_SHEET_NAME)

        # Load contragent and phrase lookup tables
        _lookup_tables_cache = {
            "contragent": load_lookup_block_(base_sheet, 3, 4, 5),  # C, D, E
            "phrase": load_lookup_block_(base_sheet, 7, 8, 9),      # G, H, I
        }
        logger.info("✓ Loaded lookup tables from '%s'", BASE_SHEET_NAME)
        return _lookup_tables_cache
    except Exception as e:
        logger.error("Failed to load lookup tables: %s", e)
        return {"contragent": [], "phrase": []}

def load_lookup_block_(sheet, col_type, col_key, col_article):
    """Load a lookup block from sheet"""
    try:
        start_row = LOOKUP_TABLE_HEADER_ROW + 1
        max_row = sheet.row_count
        num_rows = max(max_row - start_row + 1, 0)
        if num_rows == 0:
            return []

        min_col = min(col_type, col_key, col_article)
        max_col = max(col_type, col_key, col_article)

        values = sheet.range(
            start_row, min_col,
            start_row + num_rows - 1, max_col
        )

        entries = []
        type_offset = col_type - min_col
        key_offset = col_key - min_col
        article_offset = col_article - min_col

        # Convert range to 2D array
        rows = []
        current_row = []
        for cell in values:
            current_row.append(cell.value or "")
            if len(current_row) == (max_col - min_col + 1):
                rows.append(current_row)
                current_row = []

        for row in rows:
            key = normalize_text_(row[key_offset] if key_offset < len(row) else "")
            if not key:
                break

            entry_type = normalize_text_(row[type_offset] if type_offset < len(row) else "")
            article = str(row[article_offset] if article_offset < len(row) else "").strip()

            entries.append({
                "type": entry_type,
                "key": key,
                "article": article
            })

        return entries
    except Exception as e:
        logger.warning("Failed to load lookup block: %s", e)
        return []

def detect_article_(inc_exp_type, counterparty_text, comment_text):
    """Detect article based on lookup tables"""
    tables = load_lookup_tables_()

    # Try contragent match first
    by_contragent = match_lookup_table_(
        tables, "contragent", inc_exp_type, counterparty_text
    )
    if by_contragent:
        return by_contragent

    # Then try phrase match
    by_phrase = match_lookup_table_(
        tables, "phrase", inc_exp_type, comment_text
    )
    if by_phrase:
        return by_phrase

    # Fallback
    return "Продаж Роздріб" if inc_exp_type == "Дохід" else "Невизначена Витрата"

def match_lookup_table_(tables, table_name, inc_exp_type, search_text):
    """Match text in lookup table"""
    entries = tables.get(table_name, [])
    if not entries:
        return ""

    search = normalize_text_(search_text)
    if not search:
        return ""

    type_norm = normalize_text_(inc_exp_type)
    best_article = ""
    best_key_length = -1

    for entry in entries:
        if entry.get("type") and entry["type"] != type_norm:
            continue
        if not entry.get("key"):
            continue

        key = entry["key"]
        if search.find(key) >= 0 or key.find(search) >= 0:
            if len(key) > best_key_length:
                best_key_length = len(key)
                best_article = entry.get("article", "")

    return best_article

def normalize_text_(value):
    """Normalize text for comparison"""
    text = str(value or "").strip().lower()
    text = " ".join(text.split())  # Normalize whitespace
    return text

def parse_consolidation_row_(row, source_config, source_row_index):
    """Parse a row from source sheet"""
    result = {
        "account": source_config.get("sheet", ""),
        "date": "",
        "amount": "",
        "comment": "",
        "article": "",
        "transaction_id": "",
        "counterparty": "",
        "inc_exp": "",
        "source_row_index": source_row_index,
    }

    source_type = source_config.get("type", "")

    if source_type == "novapay":
        # NovaPay: [date, txn_id, amount, operation, comment, counterparty, ...]
        result["date"] = parse_date_(row[0] if len(row) > 0 else "")
        result["transaction_id"] = str(row[1] if len(row) > 1 else "").strip()
        if not result["transaction_id"]:
            return None
        result["amount"] = normalize_amount_(row[2] if len(row) > 2 else "")
        operation = str(row[3] if len(row) > 3 else "").strip().lower()
        result["comment"] = row[4] if len(row) > 4 else ""
        result["counterparty"] = row[5] if len(row) > 5 else ""
        result["inc_exp"] = "Дохід" if operation == "credit" else "Витрата"
        result["article"] = detect_article_(
            result["inc_exp"],
            result["counterparty"],
            result["comment"]
        )
        return result

    elif source_type == "monoA":
        # Monobank A: [..., date=C, ..., description=D, amount=E, direction=F, ...]
        result["date"] = parse_date_(row[2] if len(row) > 2 else "")
        result["amount"] = normalize_amount_(row[4] if len(row) > 4 else "")
        result["comment"] = row[9] if len(row) > 9 else ""
        result["transaction_id"] = str(row[10] if len(row) > 10 else "").strip()
        result["counterparty"] = row[3] if len(row) > 3 else ""
        direction = str(row[5] if len(row) > 5 else "").strip().upper()
        result["inc_exp"] = "Дохід" if direction == "IN" else "Витрата"
        result["article"] = detect_article_(
            result["inc_exp"],
            result["counterparty"],
            result["comment"]
        )
        return result

    elif source_type == "monoS":
        # Monobank S: same as monoA
        result["date"] = parse_date_(row[2] if len(row) > 2 else "")
        result["amount"] = normalize_amount_(row[4] if len(row) > 4 else "")
        result["comment"] = row[9] if len(row) > 9 else ""
        result["transaction_id"] = str(row[10] if len(row) > 10 else "").strip()
        result["counterparty"] = row[3] if len(row) > 3 else ""
        direction = str(row[5] if len(row) > 5 else "").strip().upper()
        result["inc_exp"] = "Дохід" if direction == "IN" else "Витрата"
        result["article"] = detect_article_(
            result["inc_exp"],
            result["counterparty"],
            result["comment"]
        )
        return result

    return None

def parse_date_(value):
    """Parse date from various formats"""
    if isinstance(value, datetime):
        return value.date()

    text = str(value or "").strip()
    if not text:
        return None

    # Try dd.MM.yyyy format
    parts = text.split(".")
    if len(parts) == 3:
        try:
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2])
            return datetime(year, month, day).date()
        except:
            pass

    return None

def normalize_amount_(value):
    """Normalize amount to number"""
    if value == "" or value is None:
        return ""

    if isinstance(value, (int, float)):
        num = float(value)
    else:
        text = str(value).replace(" ", "").replace(",", ".")
        try:
            num = float(text)
        except:
            return ""

    if num < 0:
        num = -num
    return num

def is_cash_withdrawal_(comment_text):
    """Check if this is a cash withdrawal payment"""
    if not comment_text:
        return False
    text = str(comment_text).lower()
    return "видача готівки з платіжного рахунку від суми отриманого доходу" in text

def expand_cash_withdrawal_rows_(parsed, target_last_col):
    """Create 3 rows from 1 cash withdrawal row"""
    rows = []

    # Row 1: Original payment with modified article
    row1 = [""] * target_last_col
    row1[1] = parsed.get("account", "")           # B
    row1[2] = parsed.get("date", "")              # C
    row1[5] = parsed.get("amount", "")            # F
    row1[7] = parsed.get("comment", "")           # H
    row1[8] = "Списання перерахування власних коштів"  # I
    row1[9] = parsed.get("transaction_id", "")    # J
    row1[10] = parsed.get("counterparty", "")     # K
    row1[11] = parsed.get("inc_exp", "")          # L
    rows.append(row1)

    # Row 2: Cash - Income - Incoming transfer
    row2 = [""] * target_last_col
    row2[1] = "Готівка"                           # B
    row2[2] = parsed.get("date", "")              # C
    row2[5] = parsed.get("amount", "")            # F
    row2[7] = ""                                   # H
    row2[8] = "Надходження від перерахування власних коштів"  # I
    row2[9] = ""                                   # J
    row2[10] = ""                                  # K
    row2[11] = "Дохід"                            # L
    rows.append(row2)

    # Row 3: Cash - Expense - no comment
    row3 = [""] * target_last_col
    row3[1] = "Готівка"                           # B
    row3[2] = parsed.get("date", "")              # C
    row3[5] = parsed.get("amount", "")            # F
    row3[7] = ""                                   # H
    row3[8] = ""                                   # I - EMPTY!
    row3[9] = ""                                   # J
    row3[10] = ""                                  # K
    row3[11] = "Витрата"                          # L
    rows.append(row3)

    return rows

def consolidate_cash_flow_():
    """Main consolidation function - works ONLY with Table 2"""
    logger.info("")
    logger.info("=" * 70)
    logger.info("🔄 STARTING CASH FLOW CONSOLIDATION (Table 2 only)")
    logger.info("=" * 70)

    # Clear lookup tables cache to ensure fresh data
    clear_lookup_tables_cache()

    try:
        # Connect ONLY to consolidation table (Table 2)
        creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT)
        creds = Credentials.from_service_account_info(creds_dict)
        client = gspread.authorize(creds)
        ss = client.open_by_key(CONSOLIDATION_SPREADSHEET_ID)
        logger.info("  ✓ Connected to consolidation table (Table 2)")

        # Debug: print all sheets and their GIDs
        logger.info("📋 Available sheets in Table 2:")
        for sheet in ss.worksheets():
            logger.info("   - '%s' (gid=%d)", sheet.title, sheet.id)

        # Find target sheet
        target_sheet = None
        for sheet in ss.worksheets():
            if sheet.id == TARGET_SHEET_GID:
                target_sheet = sheet
                break
        if not target_sheet:
            logger.error("✗ Target sheet (gid=%d) not found", TARGET_SHEET_GID)
            logger.error("  Available sheets:")
            for sheet in ss.worksheets():
                logger.error("    - '%s' (gid=%d)", sheet.title, sheet.id)
            return

        target_last_col = len(target_sheet.row_values(1))
        rows_to_write = []
        source_marks = []  # Track which source rows need marking

        # Process each source sheet (all in Table 2)
        for source_config in CONSOLIDATION_SOURCES:
            sheet_name = source_config.get("sheet", "")
            mark_col = source_config.get("mark_col", 0)

            try:
                source_sheet = ss.worksheet(sheet_name)
            except:
                logger.warning("⚠ Source sheet '%s' not found, skipping", sheet_name)
                continue

            # Get all data
            try:
                all_data = get_cached_worksheet_values(
                    f"consolidation_{sheet_name}",
                    lambda: source_sheet.get_all_values()
                )
            except:
                logger.warning("⚠ Failed to read '%s', skipping", sheet_name)
                continue

            if len(all_data) < 2:
                continue

            logger.info("📌 Processing source sheet '%s' (mark_col=%d)", sheet_name, mark_col)

            # Process rows bottom-to-top
            last_row = len(all_data)
            for i in range(last_row - 1, 0, -1):
                row = all_data[i]
                source_row_index = i + 1  # 1-based

                # Check column A (must be filled)
                col_a_value = str(row[0] if len(row) > 0 else "").strip()
                if not col_a_value:
                    continue

                # Check mark column
                mark_value = str(row[mark_col - 1] if len(row) >= mark_col else "").strip()
                if mark_value:
                    break  # Stop when we hit marked rows

                # Parse row
                parsed = parse_consolidation_row_(row, source_config, source_row_index)
                if not parsed:
                    continue

                # Check for cash withdrawal
                if is_cash_withdrawal_(parsed.get("comment", "")):
                    # Create 3 rows
                    expanded = expand_cash_withdrawal_rows_(parsed, target_last_col)
                    rows_to_write.extend(expanded)
                    logger.info("   → Expanded cash withdrawal row %d into 3 rows", source_row_index)
                    # Mark only the first (original) row
                    source_marks.append({
                        "sheet": source_sheet,
                        "row_index": source_row_index,
                        "mark_col": mark_col
                    })
                else:
                    # Regular row - apply article classification
                    inc_exp_type = parsed.get("inc_exp", "")
                    counterparty = parsed.get("counterparty", "")
                    comment = parsed.get("comment", "")

                    # Detect article using lookup tables
                    article = detect_article_(inc_exp_type, counterparty, comment)
                    parsed["article"] = article

                    row_data = [""] * target_last_col
                    row_data[1] = parsed.get("account", "")       # B
                    row_data[2] = parsed.get("date", "")          # C
                    row_data[5] = parsed.get("amount", "")        # F
                    row_data[7] = parsed.get("comment", "")       # H
                    row_data[8] = parsed.get("article", "")       # I
                    row_data[9] = parsed.get("transaction_id", "") # J
                    row_data[10] = parsed.get("counterparty", "") # K
                    row_data[11] = parsed.get("inc_exp", "")      # L
                    rows_to_write.append(row_data)

                    source_marks.append({
                        "sheet": source_sheet,
                        "row_index": source_row_index,
                        "mark_col": mark_col
                    })

        if not rows_to_write:
            logger.info("✓ No new rows to consolidate")
            return

        # Write to target sheet
        write_to_target_sheet_(target_sheet, rows_to_write, target_last_col)

        # Mark source rows
        mark_source_rows_(source_marks)

        logger.info("")
        logger.info("✓ CONSOLIDATION COMPLETED")
        logger.info("  ✓ Processed %d rows", len(rows_to_write))
        logger.info("  ✓ Marked %d source rows", len(source_marks))

    except Exception as e:
        logger.error("✗ Consolidation failed: %s", e)
        import traceback
        logger.error(traceback.format_exc())

def write_to_target_sheet_(target_sheet, rows_to_write, target_last_col):
    """Write rows to target sheet"""
    try:
        # Find first empty row (check column B)
        all_values = target_sheet.get_all_values()
        start_row = 2
        for i in range(1, len(all_values)):
            if not str(all_values[i][1] if len(all_values[i]) > 1 else "").strip():
                start_row = i + 1
                break

        # Ensure enough rows
        required_rows = start_row + len(rows_to_write) - 1
        if required_rows > target_sheet.row_count:
            target_sheet.add_rows(required_rows - target_sheet.row_count)

        # Add row numbers to column M
        for i in range(len(rows_to_write)):
            rows_to_write[i][12] = start_row + i  # M column

        # Write data
        target_sheet.update(
            f"A{start_row}:{chr(64 + target_last_col)}{start_row + len(rows_to_write) - 1}",
            rows_to_write
        )

        # Format dates (column C)
        target_sheet.format(
            f"C{start_row}:C{start_row + len(rows_to_write) - 1}",
            {"numberFormat": {"type": "DATE", "pattern": "dd.MM.yyyy"}}
        )

        # Format amounts (column F)
        target_sheet.format(
            f"F{start_row}:F{start_row + len(rows_to_write) - 1}",
            {"numberFormat": {"type": "NUMBER", "pattern": "# ##0,00"}}
        )

        logger.info("  ✓ Written %d rows to target sheet starting at row %d",
                   len(rows_to_write), start_row)

    except Exception as e:
        logger.error("✗ Failed to write to target sheet: %s", e)

def mark_source_rows_(source_marks):
    """Mark processed rows in source sheets"""
    try:
        for mark_info in source_marks:
            sheet = mark_info.get("sheet")
            row_index = mark_info.get("row_index")
            mark_col = mark_info.get("mark_col")

            if not sheet or not row_index or not mark_col:
                continue

            # Find corresponding target row number
            # The target row number is stored in column M of target_sheet
            # We'll use the row index as the marker
            try:
                sheet.update_cell(row_index, mark_col, str(row_index))
            except:
                pass

        logger.info("  ✓ Marked source rows")
    except Exception as e:
        logger.warning("⚠ Failed to mark source rows: %s", e)


def main() -> None:
    # Clear cache at start of each run
    clear_worksheet_cache()

    logger.info("=" * 70)
    logger.info("🚀 STARTING PAYMENT IMPORT")
    logger.info("=" * 70)
    log_initialization()
    # Проверяем подключение до запуска банков.
    get_spreadsheet()

    # ═════════════════════════════════════════════════════════════════════════
    # ЭТАП 0: КОНСОЛИДАЦИЯ СУЩЕСТВУЮЩИХ ПЛАТЕЖЕЙ (всегда выполняем!)
    # ═════════════════════════════════════════════════════════════════════════
    consolidate_cash_flow_()

    if all_integrations_succeeded_today():
        logger.info("")
        logger.info(
            "✓ All six integrations have already "
            "completed successfully today."
        )
        logger.info(
            "No bank API requests will be sent."
        )
        return
    results: dict[str, int] = {
        "Privat": 0,
        "Monobank": 0,
        "MonoBank Сергій": 0,
        "NovaPay Анастасія": 0,
        "NovaPay Сергій": 0,
        "NovaPay Олександра": 0,
    }
    errors: list[str] = []
    run_integration(
        integration_name="Privat",
        task_function=import_privatbank,
        results=results,
        errors=errors,
    )
    run_integration(
        integration_name="Monobank",
        task_function=lambda: import_monobank_account(
            MONO_ACCOUNTS[0]
        ),
        results=results,
        errors=errors,
    )
    run_integration(
        integration_name="MonoBank Сергій",
        task_function=lambda: import_monobank_account(
            MONO_ACCOUNTS[1]
        ),
        results=results,
        errors=errors,
    )
    run_integration(
        integration_name="NovaPay Анастасія",
        task_function=lambda: import_novapay_account(
            NOVAPAY_ACCOUNTS[0]
        ),
        results=results,
        errors=errors,
    )
    run_integration(
        integration_name="NovaPay Сергій",
        task_function=lambda: import_novapay_account(
            NOVAPAY_ACCOUNTS[1]
        ),
        results=results,
        errors=errors,
    )
    run_integration(
        integration_name="NovaPay Олександра",
        task_function=lambda: import_novapay_account(
            NOVAPAY_ACCOUNTS[2]
        ),
        results=results,
        errors=errors,
    )
    logger.info("")
    logger.info("=" * 70)
    logger.info("📊 IMPORT SUMMARY")
    logger.info("=" * 70)
    total_rows = 0
    for integration_name, added_rows in results.items():
        logger.info(
            "  %-25s %s row(s)",
            f"{integration_name}:",
            added_rows,
        )
        total_rows += added_rows
    logger.info(
        "  %-25s %s row(s)",
        "TOTAL:",
        total_rows,
    )
    if errors:
        logger.error("")
        logger.error(
            "⚠ IMPORT FINISHED WITH %s ERROR(S):",
            len(errors),
        )
        for error in errors:
            logger.error(
                "  - %s",
                error,
            )
        # Если были ошибки но кое-что импортировалось - это не критично
        if total_rows > 0:
            status = (
                "PARTIAL | "
                + " || ".join(errors)
            )
            write_execution_log(
                results,
                status,
            )
            logger.info("")
            logger.info(
                "✓ IMPORT COMPLETED WITH PARTIAL SUCCESS"
            )
            logger.info(
                "  ✓ Imported %s rows despite %s error(s)",
                total_rows,
                len(errors),
            )
            logger.info("=" * 70)
            # Возвращаемся успешно, потому что часть данных импортировалась
            return
        # Если не импортировалось НИЧЕГО и были ошибки - это ошибка
        else:
            status = (
                "ERROR | "
                + " || ".join(errors)
            )
            write_execution_log(
                results,
                status,
            )
            logger.error("")
            logger.error(
                "✗ IMPORT COMPLETED WITH NO SUCCESSFUL INTEGRATIONS"
            )
            logger.info("=" * 70)
            raise RuntimeError(
                f"Failed integrations: {len(errors)}. "
                + " | ".join(errors)
            )
    write_execution_log(
        results,
        "OK",
    )
    logger.info("")
    logger.info(
        "✓ IMPORT COMPLETED SUCCESSFULLY"
    )
    logger.info("=" * 70)
if __name__ == "__main__":
    main()
