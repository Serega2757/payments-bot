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
SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"
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
def main() -> None:
    # Clear cache at start of each run
    clear_worksheet_cache()

    logger.info("=" * 70)
    logger.info("🚀 STARTING PAYMENT IMPORT")
    logger.info("=" * 70)
    log_initialization()
    # Проверяем подключение до запуска банков.
    get_spreadsheet()
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
