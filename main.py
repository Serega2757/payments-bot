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
from urllib.parse import urlencode
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
# CONSTANTS
# =============================================================================

SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"

KYIV_TZ = ZoneInfo("Europe/Kyiv")
IMPORT_DAYS = 30

PRIVAT_SHEET = "Privat"
MONO_SHEET_1 = "Monobank"
MONO_SHEET_2 = "MonoBank Сергій"
NOVAPAY_SHEET_1 = "NovaPay Анастасія"
NOVAPAY_SHEET_2 = "NovaPay Сергій"

NOVAPAY_CONFIG_SHEET = "NovaPay_Config"
LOG_SHEET = "Logs"

NOVAPAY_ENDPOINT = (
    "https://business.novapay.ua/Services/ClientAPIService.svc"
)

RETRYABLE_HTTP_CODES = {
    408,
    425,
    429,
    500,
    502,
    503,
    504,
}

HTTP_TIMEOUT_SECONDS = 90
HTTP_RETRIES = 5


# =============================================================================
# ENVIRONMENT VARIABLES
# =============================================================================

GOOGLE_SERVICE_ACCOUNT = os.getenv("GOOGLE_SERVICE_ACCOUNT", "").strip()

PB_ID = os.getenv("PB_ID", "").strip()
PB_TOKEN = os.getenv("PB_TOKEN", "").strip()
PB_ACC = os.getenv("PB_ACC", "").strip()

MONO_TOKEN_1 = os.getenv("MONO_TOKEN_1", "").strip()
MONO_IBAN_1 = os.getenv("MONO_IBAN_1", "").strip()

MONO_TOKEN_2 = os.getenv("MONO_TOKEN_2", "").strip()
MONO_IBAN_2 = os.getenv("MONO_IBAN_2", "").strip()

NOVAPAY_LOGIN = os.getenv("NOVAPAY_LOGIN", "").strip()
NOVAPAY_REFRESH_TOKEN = os.getenv(
    "NOVAPAY_REFRESH_TOKEN",
    "",
).strip()
NOVAPAY_PUBLIC_CERTIFICATE = os.getenv(
    "NOVAPAY_PUBLIC_CERTIFICATE",
    "",
).strip()

NOVAPAY_LOGIN_2 = os.getenv("NOVAPAY_LOGIN_2", "").strip()
NOVAPAY_REFRESH_TOKEN_2 = os.getenv(
    "NOVAPAY_REFRESH_TOKEN_2",
    "",
).strip()
NOVAPAY_PUBLIC_CERTIFICATE_2 = os.getenv(
    "NOVAPAY_PUBLIC_CERTIFICATE_2",
    "",
).strip()


MONO_ACCOUNTS = [
    {
        "name": "Monobank",
        "sheet": MONO_SHEET_1,
        "token": MONO_TOKEN_1,
        "iban": MONO_IBAN_1,
    },
    {
        "name": "Monobank Сергій",
        "sheet": MONO_SHEET_2,
        "token": MONO_TOKEN_2,
        "iban": MONO_IBAN_2,
    },
]


NOVAPAY_ACCOUNTS = [
    {
        "config_column": 2,
        "name": "NovaPay Анастасія",
        "sheet": NOVAPAY_SHEET_1,
        "login": NOVAPAY_LOGIN,
        "initial_refresh_token": NOVAPAY_REFRESH_TOKEN,
        "initial_certificate": NOVAPAY_PUBLIC_CERTIFICATE,
    },
    {
        "config_column": 3,
        "name": "NovaPay Сергій",
        "sheet": NOVAPAY_SHEET_2,
        "login": NOVAPAY_LOGIN_2,
        "initial_refresh_token": NOVAPAY_REFRESH_TOKEN_2,
        "initial_certificate": NOVAPAY_PUBLIC_CERTIFICATE_2,
    },
]


# =============================================================================
# GENERAL HELPERS
# =============================================================================

def kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def date_range_30_days() -> tuple[datetime, datetime]:
    end = kyiv_now()
    start = end - timedelta(days=IMPORT_DAYS - 1)
    return start, end


def require_value(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(
            f"Не задано обов'язкову змінну середовища: {name}"
        )


def secret_status(value: str) -> str:
    return "✓ SET" if value else "✗ NOT SET"


def clean_text(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def parse_decimal(value: Any) -> float:
    text = clean_text(value).replace(" ", "").replace(",", ".")

    if not text:
        return 0.0

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
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
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
        first_ten = text[:10]

        if (
            len(first_ten) == 10
            and first_ten[4] == "-"
            and first_ten[7] == "-"
        ):
            try:
                parsed = datetime.strptime(
                    first_ten,
                    "%Y-%m-%d",
                )
                return parsed.strftime("%d.%m.%Y")
            except ValueError:
                pass

    return text


def format_log_date() -> str:
    return kyiv_now().strftime("%d.%m.%Y %H:%M:%S")


def local_xml_name(tag: str) -> str:
    return tag.split("}")[-1]


def direct_children_map(element: ET.Element) -> dict[str, str]:
    result: dict[str, str] = {}

    for child in list(element):
        result[local_xml_name(child.tag)] = clean_text(child.text)

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


def truncate_text(
    value: str,
    max_length: int = 1200,
) -> str:
    value = clean_text(value)

    if len(value) <= max_length:
        return value

    return value[:max_length] + "..."


# =============================================================================
# HTTP WITH RETRIES
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
    return_soap_fault: bool = False,
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
                return_soap_fault
                and response.status_code == 500
                and (
                    "<Fault" in response.text
                    or ":Fault" in response.text
                )
            ):
                return response

            if response.status_code in RETRYABLE_HTTP_CODES:
                last_error = RuntimeError(
                    f"HTTP {response.status_code}: "
                    f"{truncate_text(response.text)}"
                )

                if attempt < retries:
                    retry_after = response.headers.get(
                        "Retry-After",
                    )

                    if retry_after and retry_after.isdigit():
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
                    delay = min(delay * 2, 120)
                    continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            last_error = exc

            if attempt >= retries:
                break

            logger.warning(
                "Помилка HTTP-запиту. Повтор %s/%s "
                "через %s секунд: %s",
                attempt,
                retries,
                delay,
                exc,
            )

            time.sleep(delay)
            delay = min(delay * 2, 120)

    raise RuntimeError(
        f"HTTP-запит не виконано після {retries} спроб: "
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
        credentials_info = json.loads(
            GOOGLE_SERVICE_ACCOUNT
        )
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT містить невалідний JSON"
        ) from exc

    credentials = Credentials.from_service_account_info(
        credentials_info,
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
            "Аркуш '%s' не знайдений. Створюю.",
            sheet_name,
        )

        return spreadsheet.add_worksheet(
            title=sheet_name,
            rows=rows,
            cols=cols,
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


# =============================================================================
# HEADERS
# =============================================================================

def ensure_privat_header(
    worksheet: gspread.Worksheet,
) -> None:
    if not worksheet.get_all_values():
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
    if not worksheet.get_all_values():
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
    if not worksheet.get_all_values():
        worksheet.append_row([
            "Дата платежу",
            "Номер транзакції",
            "Сума",
            "Тип",
            "Призначення платежу",
        ])


def ensure_log_header(
    worksheet: gspread.Worksheet,
) -> None:
    expected = [
        "Дата виконання",
        "Privat",
        "Monobank",
        "MonoBank Сергій",
        "NovaPay Анастасія",
        "NovaPay Сергій",
        "Статус",
    ]

    values = worksheet.get_all_values()

    if not values:
        worksheet.append_row(expected)
        return

    current_header = values[0]

    if current_header != expected:
        worksheet.update(
            range_name="A1:G1",
            values=[expected],
        )


# =============================================================================
# EXECUTION LOG
# =============================================================================

def already_success_today() -> bool:
    worksheet = get_or_create_worksheet(
        LOG_SHEET,
        rows=5000,
        cols=10,
    )

    ensure_log_header(worksheet)

    rows = worksheet.get_all_values()

    if len(rows) <= 1:
        return False

    today = kyiv_now().strftime("%d.%m.%Y")

    for row in reversed(rows[1:]):
        if len(row) < 7:
            continue

        date_value = clean_text(row[0])
        status = clean_text(row[6])

        if date_value.startswith(today) and status == "OK":
            return True

    return False


def write_execution_log(
    results: dict[str, int],
    status: str,
) -> None:
    worksheet = get_or_create_worksheet(
        LOG_SHEET,
        rows=5000,
        cols=10,
    )

    ensure_log_header(worksheet)

    worksheet.append_row(
        [
            format_log_date(),
            results.get(PRIVAT_SHEET, 0),
            results.get(MONO_SHEET_1, 0),
            results.get(MONO_SHEET_2, 0),
            results.get(NOVAPAY_SHEET_1, 0),
            results.get(NOVAPAY_SHEET_2, 0),
            status,
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
            transaction.get("DATE_TIME_DAT_OD_TIM_P")
        ),
        clean_text(transaction.get("SUM")),
    ]

    if not any(parts):
        return ""

    return "_".join(parts)


def privat_sort_key(
    transaction: dict[str, Any],
) -> datetime:
    raw_date = transaction.get(
        "DATE_TIME_DAT_OD_TIM_P",
        "",
    )

    parsed = parse_date_value(raw_date)

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

    start, end = date_range_30_days()

    endpoint = (
        "https://acp.privatbank.ua/api/"
        "statements/transactions"
    )

    params = {
        "acc": PB_ACC,
        "startDate": start.strftime("%d-%m-%Y"),
        "endDate": end.strftime("%d-%m-%Y"),
        "limit": 500,
    }

    response = request_with_retry(
        "GET",
        endpoint,
        params=params,
        headers={
            "id": PB_ID,
            "token": PB_TOKEN,
            "Accept": "application/json",
            "User-Agent": "payments-bot/1.0",
        },
    )

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "PrivatBank повернув не JSON: "
            f"{truncate_text(response.text)}"
        ) from exc

    if payload.get("status") != "SUCCESS":
        raise RuntimeError(
            "Помилка PrivatBank API: "
            f"{truncate_text(json.dumps(payload, ensure_ascii=False))}"
        )

    transactions = payload.get("transactions")

    if not isinstance(transactions, list):
        raise RuntimeError(
            "PrivatBank API не повернув масив transactions"
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
                "PrivatBank: пропущено операцію без ID"
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
            clean_text(transaction.get("TRANTYPE")),
            parse_decimal(transaction.get("SUM")),
            clean_text(transaction.get("CCY")),
            clean_text(
                transaction.get("AUT_CNTR_NAM")
            ),
            clean_text(transaction.get("OSND")),
            clean_text(
                transaction.get("AUT_CNTR_ACC")
            ),
        ])

        existing_ids.add(transaction_id)

    append_rows_batch(worksheet, rows)

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
            "User-Agent": "payments-bot/1.0",
        },
    )

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "Monobank client-info повернув не JSON"
        ) from exc

    accounts = payload.get("accounts", [])

    for account in accounts:
        account_iban = clean_text(account.get("iban"))

        if account_iban == iban:
            account_id = clean_text(account.get("id"))

            if not account_id:
                raise RuntimeError(
                    f"Monobank: у рахунку {iban} немає id"
                )

            return account_id

    available_ibans = [
        clean_text(account.get("iban"))
        for account in accounts
        if clean_text(account.get("iban"))
    ]

    raise RuntimeError(
        f"Monobank: IBAN {iban} не знайдений. "
        f"Доступні IBAN: {available_ibans}"
    )


def get_monobank_statements(
    token: str,
    account_id: str,
) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc)
    from_utc = now_utc - timedelta(
        days=IMPORT_DAYS - 1
    )

    from_timestamp = int(from_utc.timestamp())
    to_timestamp = int(now_utc.timestamp())

    endpoint = (
        "https://api.monobank.ua/personal/statement/"
        f"{account_id}/{from_timestamp}/{to_timestamp}"
    )

    response = request_with_retry(
        "GET",
        endpoint,
        headers={
            "X-Token": token,
            "Accept": "application/json",
            "User-Agent": "payments-bot/1.0",
        },
    )

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "Monobank statement повернув не JSON"
        ) from exc

    if not isinstance(payload, list):
        raise RuntimeError(
            "Monobank statement повернув не масив: "
            f"{truncate_text(json.dumps(payload, ensure_ascii=False))}"
        )

    return payload


def build_mono_transaction_id(
    iban: str,
    transaction: dict[str, Any],
) -> str:
    original_id = clean_text(transaction.get("id"))

    if original_id:
        return f"{iban}_{original_id}"

    fallback = "_".join([
        clean_text(transaction.get("time")),
        clean_text(transaction.get("amount")),
        clean_text(transaction.get("description")),
    ])

    if not fallback.replace("_", ""):
        return ""

    return f"{iban}_{fallback}"


def import_monobank_account(
    account: dict[str, str],
) -> int:
    sheet_name = account["sheet"]
    token = clean_text(account["token"])
    iban = clean_text(account["iban"])

    require_value(
        f"token for {sheet_name}",
        token,
    )
    require_value(
        f"IBAN for {sheet_name}",
        iban,
    )

    logger.info("")
    logger.info(
        "📱 Processing Monobank: %s",
        sheet_name,
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

    rows_with_time: list[
        tuple[int, list[Any]]
    ] = []

    for transaction in statements:
        transaction_id = build_mono_transaction_id(
            iban,
            transaction,
        )

        if not transaction_id:
            logger.warning(
                "%s: пропущено операцію без ID",
                sheet_name,
            )
            continue

        if transaction_id in existing_ids:
            continue

        transaction_timestamp = int(
            transaction.get("time", 0) or 0
        )

        transaction_date = datetime.fromtimestamp(
            transaction_timestamp,
            tz=timezone.utc,
        ).astimezone(KYIV_TZ)

        amount = parse_decimal(
            transaction.get("amount", 0)
        ) / 100

        balance = parse_decimal(
            transaction.get("balance", 0)
        ) / 100

        currency_code = (
            transaction.get("currencyCode")
            if transaction.get("currencyCode") is not None
            else transaction.get("currency", "")
        )

        row = [
            transaction_id,
            iban,
            transaction_date.strftime("%d.%m.%Y"),
            clean_text(
                transaction.get("description")
            ),
            amount,
            "IN" if amount >= 0 else "OUT",
            clean_text(currency_code),
            balance,
            clean_text(transaction.get("mcc")),
            clean_text(transaction.get("comment")),
            clean_text(
                transaction.get("counterEdrpou")
            ),
            clean_text(
                transaction.get("counterIban")
            ),
        ]

        rows_with_time.append(
            (transaction_timestamp, row)
        )

        existing_ids.add(transaction_id)

    rows_with_time.sort(key=lambda item: item[0])

    rows = [
        row
        for _, row in rows_with_time
    ]

    append_rows_batch(worksheet, rows)

    logger.info(
        "✓ %s: added %s row(s)",
        sheet_name,
        len(rows),
    )

    return len(rows)


# =============================================================================
# NOVAPAY CONFIG
# =============================================================================

def ensure_novapay_config_sheet() -> gspread.Worksheet:
    worksheet = get_or_create_worksheet(
        NOVAPAY_CONFIG_SHEET,
        rows=20,
        cols=5,
    )

    expected_header = [
        "key",
        NOVAPAY_SHEET_1,
        NOVAPAY_SHEET_2,
    ]

    values = worksheet.get_all_values()

    if not values:
        worksheet.update(
            range_name="A1:C3",
            values=[
                expected_header,
                [
                    "refresh_token",
                    NOVAPAY_REFRESH_TOKEN,
                    NOVAPAY_REFRESH_TOKEN_2,
                ],
                [
                    "public_certificate",
                    NOVAPAY_PUBLIC_CERTIFICATE,
                    NOVAPAY_PUBLIC_CERTIFICATE_2,
                ],
            ],
        )

        return worksheet

    current_header = values[0]

    if len(current_header) < 3 or current_header[:3] != expected_header:
        legacy_refresh = ""
        legacy_certificate = ""

        for row in values:
            if len(row) < 2:
                continue

            key = clean_text(row[0]).lower()
            value = clean_text(row[1])

            if key == "refresh_token":
                legacy_refresh = value
            elif key == "public_certificate":
                legacy_certificate = value

        account_1_refresh = (
            legacy_refresh
            or NOVAPAY_REFRESH_TOKEN
        )

        account_1_certificate = (
            legacy_certificate
            or NOVAPAY_PUBLIC_CERTIFICATE
        )

        worksheet.clear()

        worksheet.update(
            range_name="A1:C3",
            values=[
                expected_header,
                [
                    "refresh_token",
                    account_1_refresh,
                    NOVAPAY_REFRESH_TOKEN_2,
                ],
                [
                    "public_certificate",
                    account_1_certificate,
                    NOVAPAY_PUBLIC_CERTIFICATE_2,
                ],
            ],
        )

    return worksheet


def read_novapay_credentials(
    account: dict[str, Any],
) -> tuple[str, str]:
    worksheet = ensure_novapay_config_sheet()

    column_number = int(account["config_column"])

    refresh_token = clean_text(
        worksheet.cell(2, column_number).value
    )

    certificate = clean_text(
        worksheet.cell(3, column_number).value
    )

    if not refresh_token:
        refresh_token = clean_text(
            account.get("initial_refresh_token")
        )

    if not certificate:
        certificate = clean_text(
            account.get("initial_certificate")
        )

    if not refresh_token:
        raise RuntimeError(
            f"{account['name']}: відсутній Refresh Token "
            f"у {NOVAPAY_CONFIG_SHEET}"
        )

    if not certificate:
        raise RuntimeError(
            f"{account['name']}: відсутній Public Certificate "
            f"у {NOVAPAY_CONFIG_SHEET}"
        )

    return refresh_token, certificate


def save_novapay_credentials(
    account: dict[str, Any],
    refresh_token: str,
    certificate: str,
) -> None:
    worksheet = ensure_novapay_config_sheet()

    column_number = int(account["config_column"])

    worksheet.update_cell(
        2,
        column_number,
        refresh_token,
    )

    worksheet.update_cell(
        3,
        column_number,
        certificate,
    )

    logger.info(
        "  ✓ %s: rotated NovaPay credentials saved",
        account["name"],
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
            f"NovaPay {method_name}: порожня відповідь"
        )

    try:
        root = ET.fromstring(response_text)
    except ET.ParseError as exc:
        raise RuntimeError(
            f"NovaPay {method_name}: невалідний XML: "
            f"{truncate_text(response_text)}"
        ) from exc

    fault = None

    for element in root.iter():
        if local_xml_name(element.tag) == "Fault":
            fault = element
            break

    if fault is not None:
        fault_string = (
            find_xml_text(fault, "faultstring")
            or find_xml_text(fault, "Text")
            or xml_to_string(fault)
        )

        raise RuntimeError(
            f"NovaPay {method_name} SOAP Fault: "
            f"{truncate_text(fault_string)}"
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
            "User-Agent": "payments-bot/1.0",
        },
        data=envelope.encode("utf-8"),
        return_soap_fault=True,
    )

    return parse_soap_response(
        response,
        method_name,
    )


def novapay_check_result(
    root: ET.Element,
    method_name: str,
) -> None:
    result = find_xml_text(root, "result")

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

    details = " | ".join(
        value
        for value in [
            error_status,
            error_title,
        ]
        if value
    )

    if not details:
        details = truncate_text(
            xml_to_string(root)
        )

    raise RuntimeError(
        f"NovaPay {method_name}: {details}"
    )


def novapay_authenticate(
    account: dict[str, Any],
) -> str:
    login = clean_text(account["login"])

    require_value(
        f"login for {account['name']}",
        login,
    )

    refresh_token, certificate = (
        read_novapay_credentials(account)
    )

    request_ref = str(uuid.uuid4())

    request_body = f"""
<tem:request>
  <tem:request_ref>{html.escape(request_ref)}</tem:request_ref>
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

    jwt = find_xml_text(root, "jwt")
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
            f"{account['name']}: NovaPay не повернув JWT"
        )

    if not new_refresh_token:
        raise RuntimeError(
            f"{account['name']}: NovaPay не повернув "
            "новий Refresh Token"
        )

    if not new_certificate:
        raise RuntimeError(
            f"{account['name']}: NovaPay не повернув "
            "новий Public Certificate"
        )

    # Зберігаємо відразу після успішної ротації.
    # Старий Refresh Token уже недійсний.
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

    unique: dict[str, dict[str, str]] = {}

    for record in records:
        record_id = clean_text(record.get("id"))

        if record_id:
            unique[record_id] = record

    return list(unique.values())


def novapay_get_client_id(
    jwt: str,
    account_name: str,
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
            f"{account_name}: NovaPay не повернув "
            "доступних підприємств"
        )

    if len(clients) > 1:
        logger.warning(
            "%s: доступно %s підприємств. "
            "Використовую перше: %s",
            account_name,
            len(clients),
            clients[0],
        )

    client_id = clean_text(
        clients[0].get("id")
    )

    if not client_id:
        raise RuntimeError(
            f"{account_name}: у підприємства немає id"
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

    unique: dict[str, dict[str, str]] = {}

    for record in records:
        record_id = clean_text(record.get("id"))

        if record_id:
            unique[record_id] = record

    return list(unique.values())


def novapay_get_account_id(
    jwt: str,
    client_id: str,
    account_name: str,
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
            f"{account_name}: NovaPay не повернув рахунків"
        )

    active_accounts = []

    for item in accounts:
        status_code = clean_text(
            item.get("statuscode")
        ).lower()

        if not status_code or status_code == "active":
            active_accounts.append(item)

    selected_accounts = (
        active_accounts
        if active_accounts
        else accounts
    )

    if len(selected_accounts) > 1:
        logger.warning(
            "%s: знайдено %s рахунків. "
            "Використовую перший: %s",
            account_name,
            len(selected_accounts),
            selected_accounts[0],
        )

    account_id = clean_text(
        selected_accounts[0].get("id")
    )

    if not account_id:
        raise RuntimeError(
            f"{account_name}: рахунок NovaPay не має id"
        )

    return account_id


def novapay_get_payment_elements(
    jwt: str,
    account_id: str,
    account_name: str,
) -> list[ET.Element]:
    start, end = date_range_30_days()

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

    result = find_xml_text(root, "result")

    if result and result.lower() != "ok":
        error_title = find_xml_text(root, "title")
        error_status = find_xml_text(root, "status")

        combined_error = (
            f"{error_status} {error_title}"
        ).lower()

        empty_indicators = [
            "no documents",
            "not found",
            "відсутні платежі",
            "документи відсутні",
        ]

        if any(
            indicator in combined_error
            for indicator in empty_indicators
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
            f"{account_name}: NovaPay payments містить "
            "невалідний XML: "
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
        find_direct_child_text(document, "DayDate")
        or find_direct_child_text(document, "OrgDate")
        or find_direct_child_text(document, "PayDate")
    )

    return format_date(date_value)


def novapay_payment_sort_key(
    document: ET.Element,
) -> datetime:
    date_value = (
        find_direct_child_text(document, "DayDate")
        or find_direct_child_text(document, "OrgDate")
        or find_direct_child_text(document, "PayDate")
    )

    return parse_date_value(date_value) or datetime.min


def import_novapay_account(
    account: dict[str, Any],
) -> int:
    logger.info("")
    logger.info(
        "💳 Processing NovaPay: %s",
        account["name"],
    )

    login = clean_text(account.get("login"))

    require_value(
        f"login for {account['name']}",
        login,
    )

    worksheet = get_or_create_worksheet(
        account["sheet"],
        rows=5000,
        cols=10,
    )

    ensure_novapay_header(worksheet)

    # Для NovaPay унікальний номер знаходиться в колонці B.
    existing_codes = load_existing_values(
        worksheet,
        2,
    )

    jwt = novapay_authenticate(account)

    client_id = novapay_get_client_id(
        jwt,
        account["name"],
    )

    account_id = novapay_get_account_id(
        jwt,
        client_id,
        account["name"],
    )

    documents = novapay_get_payment_elements(
        jwt,
        account_id,
        account["name"],
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
                "%s: пропущено платіж без Code",
                account["name"],
            )
            continue

        if code in existing_codes:
            continue

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

        payment_date = novapay_payment_date(
            document
        )

        rows.append([
            payment_date,
            code,
            amount,
            payment_type,
            purpose,
        ])

        existing_codes.add(code)

    append_rows_batch(worksheet, rows)

    logger.info(
        "✓ %s: added %s row(s)",
        account["name"],
        len(rows),
    )

    return len(rows)


# =============================================================================
# INITIALIZATION LOG
# =============================================================================

def log_initialization() -> None:
    logger.info("📋 ENVIRONMENT VARIABLES CHECK:")

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
        ("NOVAPAY_LOGIN_2", NOVAPAY_LOGIN_2),
        (
            "NOVAPAY_PUBLIC_CERTIFICATE_2",
            NOVAPAY_PUBLIC_CERTIFICATE_2,
        ),
        (
            "NOVAPAY_REFRESH_TOKEN_2",
            NOVAPAY_REFRESH_TOKEN_2,
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
            "  %-32s %s",
            f"{name}:",
            secret_status(value),
        )

    logger.info("")
    logger.info("📦 ACCOUNTS CONFIGURATION:")

    for account in MONO_ACCOUNTS:
        logger.info(
            "  %s: token=%s, iban=%s",
            account["sheet"],
            bool(account["token"]),
            bool(account["iban"]),
        )

    for account in NOVAPAY_ACCOUNTS:
        logger.info(
            "  %s: login=%s, initial_token=%s, "
            "initial_certificate=%s",
            account["sheet"],
            bool(account["login"]),
            bool(account["initial_refresh_token"]),
            bool(account["initial_certificate"]),
        )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    logger.info("=" * 70)
    logger.info("🚀 STARTING PAYMENT IMPORT")
    logger.info("=" * 70)

    log_initialization()

    # Гарантуємо підключення до таблиці до перевірки логів.
    get_spreadsheet()

    if already_success_today():
        logger.info(
            "✓ Сьогодні вже був повністю успішний запуск. "
            "Повторний імпорт не потрібен."
        )
        return

    results: dict[str, int] = {
        PRIVAT_SHEET: 0,
        MONO_SHEET_1: 0,
        MONO_SHEET_2: 0,
        NOVAPAY_SHEET_1: 0,
        NOVAPAY_SHEET_2: 0,
    }

    errors: list[str] = []

    tasks = [
        (
            PRIVAT_SHEET,
            import_privatbank,
        ),
        (
            MONO_SHEET_1,
            lambda: import_monobank_account(
                MONO_ACCOUNTS[0]
            ),
        ),
        (
            MONO_SHEET_2,
            lambda: import_monobank_account(
                MONO_ACCOUNTS[1]
            ),
        ),
        (
            NOVAPAY_SHEET_1,
            lambda: import_novapay_account(
                NOVAPAY_ACCOUNTS[0]
            ),
        ),
        (
            NOVAPAY_SHEET_2,
            lambda: import_novapay_account(
                NOVAPAY_ACCOUNTS[1]
            ),
        ),
    ]

    for task_name, task_function in tasks:
        try:
            results[task_name] = task_function()
        except Exception as exc:
            error_message = (
                f"{task_name}: {type(exc).__name__}: {exc}"
            )

            errors.append(error_message)

            logger.exception(
                "✗ %s failed",
                task_name,
            )

    logger.info("")
    logger.info("=" * 70)
    logger.info("📊 IMPORT SUMMARY")
    logger.info("=" * 70)

    for sheet_name, added_count in results.items():
        logger.info(
            "  %-25s %s row(s)",
            f"{sheet_name}:",
            added_count,
        )

    logger.info(
        "  %-25s %s row(s)",
        "TOTAL:",
        sum(results.values()),
    )

    if errors:
        status = "ERROR | " + " || ".join(errors)

        write_execution_log(
            results,
            truncate_text(status, 45000),
        )

        logger.error("")
        logger.error(
            "✗ IMPORT FINISHED WITH %s ERROR(S)",
            len(errors),
        )

        for error in errors:
            logger.error("  - %s", error)

        # Критично: workflow має бути червоним.
        # Тоді наступний погодинний запуск повторить імпорт.
        raise RuntimeError(
            f"Не відпрацювали {len(errors)} інтеграції: "
            + " | ".join(errors)
        )

    write_execution_log(
        results,
        "OK",
    )

    logger.info("")
    logger.info("✓ IMPORT COMPLETED SUCCESSFULLY")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
