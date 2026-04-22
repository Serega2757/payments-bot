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


# ---------------------------
# GOOGLE
# ---------------------------
def client():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def worksheet(name):
    gc = client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        return sh.worksheet(name)
    except:
        ws = sh.add_worksheet(title=name, rows=1000, cols=20)
        return ws


def existing_ids(ws):
    vals = ws.col_values(1)
    if len(vals) <= 1:
        return set()
    return set(str(x).strip() for x in vals[1:] if str(x).strip())


def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


# ---------------------------
# RETRY REQUEST
# ---------------------------
def api_get(url, headers=None, retries=5):
    delay = 5

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=60)

            if r.status_code == 429:
                time.sleep(delay)
                delay *= 2
                continue

            if r.status_code >= 500:
                time.sleep(delay)
                delay *= 2
                continue

            r.raise_for_status()
            return r

        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= 2


# ---------------------------
# PRIVAT
# ---------------------------
def privat_id(tx):
    return "_".join([
        str(tx.get("REF", "")),
        str(tx.get("REFN", "")),
        str(tx.get("DATE_TIME_DAT_OD_TIM_P", "")),
        str(tx.get("SUM", "")),
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
            uid = privat_id(tx)
            if not uid or uid in ids:
                continue

            rows.append([
                uid,
                tx.get("DATE_TIME_DAT_OD_TIM_P", ""),
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


# ---------------------------
# MONO
# ---------------------------
def mono_account_id():
    r = api_get(
        "https://api.monobank.ua/personal/client-info",
        headers={"X-Token": MONO_TOKEN}
    )

    data = r.json()

    for acc in data.get("accounts", []):
        if acc.get("iban") == MONO_IBAN:
            return acc["id"]

    raise Exception("Mono account not found")


def mono_id(tx):
    base = tx.get("id") or f'{tx["time"]}_{tx["amount"]}'
    return f"{MONO_IBAN}_{base}"


def import_mono():
    ws = worksheet(MONO_SHEET)
    ids = existing_ids(ws)

    acc_id = mono_account_id()

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)

    url = f"https://api.monobank.ua/personal/statement/{acc_id}/{int(start.timestamp())}/{int(now.timestamp())}"

    r = api_get(url, headers={"X-Token": MONO_TOKEN})
    data = r.json()

    rows = []

    for tx in data:
        uid = mono_id(tx)
        if uid in ids:
            continue

        dt = datetime.fromtimestamp(tx["time"], timezone.utc).astimezone(KYIV)

        amount = tx.get("amount", 0) / 100
        balance = tx.get("balance", 0) / 100

        rows.append([
            uid,
            MONO_IBAN,
            dt.strftime("%Y-%m-%d %H:%M:%S"),
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


# ---------------------------
# LOGS
# ---------------------------
def write_log(privat_count, mono_count, status="OK"):
    ws = worksheet(LOG_SHEET)

    if ws.row_count == 1000 and ws.get_all_values() == []:
        ws.append_row(["Date", "Privat Added", "Monobank Added", "Status"])

    ws.append_row([
        datetime.now(KYIV).strftime("%Y-%m-%d %H:%M:%S"),
        privat_count,
        mono_count,
        status
    ])


# ---------------------------
# MAIN
# ---------------------------
def main():
    privat_added = 0
    mono_added = 0

    try:
        privat_added = import_privat()
        mono_added = import_mono()
        write_log(privat_added, mono_added, "OK")

    except Exception as e:
        write_log(privat_added, mono_added, str(e))
        raise


if __name__ == "__main__":
    main()
