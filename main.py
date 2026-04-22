# main.py
import os
import json
import requests
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"

PRIVAT_SHEET = "Privat"
MONO_SHEET = "Monobank"

PB_ID = os.getenv("PB_ID")
PB_TOKEN = os.getenv("PB_TOKEN")
PB_ACC = os.getenv("PB_ACC")

MONO_TOKEN = os.getenv("MONO_TOKEN")
MONO_IBAN = os.getenv("MONO_IBAN")

GOOGLE_SERVICE_ACCOUNT = os.getenv("GOOGLE_SERVICE_ACCOUNT")

KYIV_TZ = timezone(timedelta(hours=3))


# =========================
# GOOGLE SHEETS
# =========================
def get_client():
    creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


def get_sheet(name):
    gc = get_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(name)


def existing_ids(ws):
    col = ws.col_values(1)
    if not col:
        return set()
    return set(x.strip() for x in col[1:] if str(x).strip())


def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


# =========================
# PRIVATBANK
# =========================
def build_privat_id(tx):
    return "_".join([
        str(tx.get("REF", "")),
        str(tx.get("REFN", "")),
        str(tx.get("DATE_TIME_DAT_OD_TIM_P", "")),
        str(tx.get("SUM", "")),
    ])


def import_privat():
    ws = get_sheet(PRIVAT_SHEET)
    ids = existing_ids(ws)

    today = datetime.now(KYIV_TZ)
    start = today - timedelta(days=29)

    start_date = start.strftime("%d-%m-%Y")
    end_date = today.strftime("%d-%m-%Y")

    url = (
        "https://acp.privatbank.ua/api/statements/transactions"
        f"?acc={PB_ACC}"
        f"&startDate={start_date}"
        f"&endDate={end_date}"
        f"&limit=500"
    )

    r = requests.get(
        url,
        headers={
            "id": PB_ID,
            "token": PB_TOKEN,
            "User-Agent": "GitHubActions",
        },
        timeout=60,
    )
    r.raise_for_status()

    data = r.json()

    if data.get("status") != "SUCCESS":
        print("Privat API error")
        return

    rows = []

    for tx in data.get("transactions", []):
        uid = build_privat_id(tx)
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
            tx.get("AUT_CNTR_ACC", ""),
        ])
        ids.add(uid)

    append_rows(ws, rows)
    print(f"Privat added: {len(rows)}")


# =========================
# MONOBANK
# =========================
def get_mono_account_id():
    r = requests.get(
        "https://api.monobank.ua/personal/client-info",
        headers={"X-Token": MONO_TOKEN},
        timeout=60,
    )
    r.raise_for_status()

    data = r.json()
    for acc in data.get("accounts", []):
        if acc.get("iban") == MONO_IBAN:
            return acc.get("id")

    raise Exception("Monobank account not found")


def build_mono_id(tx):
    base = tx.get("id") or f'{tx.get("time")}_{tx.get("amount")}_{tx.get("description","")}'
    return f"{MONO_IBAN}_{base}"


def import_mono():
    ws = get_sheet(MONO_SHEET)
    ids = existing_ids(ws)

    account_id = get_mono_account_id()

    now = datetime.now(timezone.utc)
    from_dt = now - timedelta(days=30)

    from_ts = int(from_dt.timestamp())
    to_ts = int(now.timestamp())

    url = f"https://api.monobank.ua/personal/statement/{account_id}/{from_ts}/{to_ts}"

    r = requests.get(
        url,
        headers={"X-Token": MONO_TOKEN},
        timeout=60,
    )
    r.raise_for_status()

    data = r.json()
    rows = []

    for tx in data:
        uid = build_mono_id(tx)
        if uid in ids:
            continue

        dt = datetime.fromtimestamp(tx["time"], tz=timezone.utc).astimezone(KYIV_TZ)

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
            tx.get("counterIban", ""),
        ])

        ids.add(uid)

    rows.sort(key=lambda x: x[2])
    append_rows(ws, rows)
    print(f"Monobank added: {len(rows)}")


# =========================
# MAIN
# =========================
def main():
    import_privat()
    import_mono()


if __name__ == "__main__":
    main()
