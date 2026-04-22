import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"

PRIVAT_SHEET = "Privat"
MONO_SHEET = "Monobank"
LOG_SHEET = "Logs"

PB_ID = os.getenv("PB_ID")
PB_TOKEN = os.getenv("PB_TOKEN")
PB_ACC = os.getenv("PB_ACC")

MONO_TOKEN = os.getenv("MONO_TOKEN")
MONO_IBAN = os.getenv("MONO_IBAN")

GOOGLE_SERVICE_ACCOUNT = os.getenv("GOOGLE_SERVICE_ACCOUNT")

KYIV = timezone(timedelta(hours=3))


# =========================
# GOOGLE
# =========================
def client():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    return gspread.authorize(creds)


def spreadsheet():
    return client().open_by_key(SPREADSHEET_ID)


def worksheet(name):
    sh = spreadsheet()
    try:
        return sh.worksheet(name)
    except:
        return sh.add_worksheet(title=name, rows=1000, cols=20)


def ensure_log_header(ws):
    vals = ws.get_all_values()
    if not vals:
        ws.append_row(["Date", "Privat Added", "Monobank Added", "Status"])


def existing_ids(ws):
    vals = ws.col_values(1)
    if len(vals) <= 1:
        return set()
    return set(str(v).strip() for v in vals[1:] if str(v).strip())


def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


# =========================
# RETRY REQUESTS
# =========================
def api_get(url, headers=None, retries=5):
    delay = 5

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=60)

            if r.status_code in (429, 500, 502, 503, 504):
                print(f"Retry {attempt+1}: status {r.status_code}")
                time.sleep(delay)
                delay *= 2
                continue

            r.raise_for_status()
            return r

        except Exception as e:
            if attempt == retries - 1:
                raise e
            time.sleep(delay)
            delay *= 2


# =========================
# LOGIC
# =========================
def already_success_today():
    ws = worksheet(LOG_SHEET)
    ensure_log_header(ws)

    rows = ws.get_all_values()
    if len(rows) <= 1:
        return False

    today = datetime.now(KYIV).strftime("%Y-%m-%d")

    for row in reversed(rows[1:]):
        if len(row) < 4:
            continue
        if row[0].startswith(today) and row[3] == "OK":
            return True

    return False


def write_log(privat_count, mono_count, status):
    ws = worksheet(LOG_SHEET)
    ensure_log_header(ws)

    ws.append_row([
        datetime.now(KYIV).strftime("%Y-%m-%d %H:%M:%S"),
        privat_count,
        mono_count,
        status
    ])
def format_privat_date(value):
    if not value:
        return ""

    patterns = [
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
    ]

    for fmt in patterns:
        try:
            dt = datetime.strptime(str(value), fmt)
            return dt.strftime("%d.%m.%Y")
        except:
            pass

    return str(value)

# =========================
# PRIVAT
# =========================
def privat_uid(tx):
    return "_".join([
        str(tx.get("REF", "")),
        str(tx.get("REFN", "")),
        str(tx.get("DATE_TIME_DAT_OD_TIM_P", "")),
        str(tx.get("SUM", ""))
    ])


def import_privat():
    ws = worksheet(PRIVAT_SHEET)
    ids = existing_ids(ws)

    today = datetime.now(KYIV)
    start = today - timedelta(days=29)

    url = (
        "https://acp.privatbank.ua/api/statements/transactions"
        f"?acc={PB_ACC}"
        f"&startDate={start.strftime('%d-%m-%Y')}"
        f"&endDate={today.strftime('%d-%m-%Y')}"
        "&limit=500"
    )

    r = api_get(url, headers={
        "id": PB_ID,
        "token": PB_TOKEN
    })

    data = r.json()
    rows = []

    if data.get("status") == "SUCCESS":
        for tx in data.get("transactions", []):
            uid = privat_uid(tx)
            if not uid or uid in ids:
                continue

            rows.append([
                uid,
                format_privat_date(tx.get("DATE_TIME_DAT_OD_TIM_P", "")),
                tx.get("TRANTYPE", ""),
                float(tx.get("SUM", 0) or 0),
                tx.get("CCY", ""),
                tx.get("AUT_CNTR_NAM", ""),
                tx.get("OSND", ""),
                tx.get("AUT_CNTR_ACC", "")
            ])
            ids.add(uid)

    append_rows(ws, rows)
    return len(rows)


# =========================
# MONO
# =========================
def mono_account():
    r = api_get(
        "https://api.monobank.ua/personal/client-info",
        headers={"X-Token": MONO_TOKEN}
    )

    data = r.json()

    for acc in data.get("accounts", []):
        if acc.get("iban") == MONO_IBAN:
            return acc["id"]

    raise Exception("Monobank account not found")


def mono_uid(tx):
    base = tx.get("id") or f'{tx.get("time")}_{tx.get("amount")}'
    return f"{MONO_IBAN}_{base}"


def import_mono():
    ws = worksheet(MONO_SHEET)
    ids = existing_ids(ws)

    account_id = mono_account()

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)

    url = f"https://api.monobank.ua/personal/statement/{account_id}/{int(start.timestamp())}/{int(now.timestamp())}"

    r = api_get(url, headers={"X-Token": MONO_TOKEN})
    data = r.json()

    rows = []

    for tx in data:
        uid = mono_uid(tx)
        if uid in ids:
            continue

        dt = datetime.fromtimestamp(
            tx["time"],
            timezone.utc
        ).astimezone(KYIV)

        amount = tx.get("amount", 0) / 100
        balance = tx.get("balance", 0) / 100

        rows.append([
            uid,
            MONO_IBAN,
            dt.strftime("%d.%m.%Y"),
            tx.get("description", ""),
            amount,
            "IN" if amount >= 0 else "OUT",
            tx.get("currencyCode", ""),
            balance,
            tx.get("mcc", ""),
            tx.get("comment", ""),
            tx.get("counterEdrpou", ""),
            tx.get("counterIban", "")
        ])

        ids.add(uid)

    rows.sort(key=lambda x: x[2])
    append_rows(ws, rows)
    return len(rows)


# =========================
# MAIN
# =========================
def main():
    if already_success_today():
        print("Already completed today")
        return

    privat_added = 0
    mono_added = 0

    try:
        privat_added = import_privat()
        mono_added = import_mono()
        write_log(privat_added, mono_added, "OK")
        print("Success")

    except Exception as e:
        write_log(privat_added, mono_added, str(e))
        raise


if __name__ == "__main__":
    main()
