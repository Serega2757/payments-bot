import os
import json
import time
import uuid
import html
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials


SPREADSHEET_ID = "1KujvD6_Z6r0474URqHbjlWZthEW_XDqHa1IwtZ0PsqY"

PRIVAT_SHEET = "Privat"
MONO_SHEET = "Monobank"
NOVAPAY_SHEET = "NovaPay Анастасія"
NOVAPAY_CONFIG_SHEET = "NovaPay_Config"
LOG_SHEET = "Logs"

PB_ID = os.getenv("PB_ID")
PB_TOKEN = os.getenv("PB_TOKEN")
PB_ACC = os.getenv("PB_ACC")

MONO_TOKEN = os.getenv("MONO_TOKEN")
MONO_IBAN = os.getenv("MONO_IBAN")

NOVAPAY_LOGIN = os.getenv("NOVAPAY_LOGIN")

GOOGLE_SERVICE_ACCOUNT = os.getenv("GOOGLE_SERVICE_ACCOUNT")

KYIV = timezone(timedelta(hours=3))
NOVAPAY_ENDPOINT = "https://business.novapay.ua/Services/ClientAPIService.svc"


# =========================
# GOOGLE SHEETS
# =========================

def gs_client():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def spreadsheet():
    return gs_client().open_by_key(SPREADSHEET_ID)


def worksheet(name, rows=1000, cols=20):
    sh = spreadsheet()
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=name, rows=rows, cols=cols)


def existing_ids(ws, column=1):
    vals = ws.col_values(column)
    if len(vals) <= 1:
        return set()
    return set(str(v).strip() for v in vals[1:] if str(v).strip())


def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


# =========================
# COMMON
# =========================

def api_get(url, headers=None, retries=5):
    delay = 5

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=60)

            if r.status_code in (429, 500, 502, 503, 504):
                print(f"Retry GET {attempt + 1}: HTTP {r.status_code}")
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


def api_post(url, data, headers=None, retries=5):
    delay = 5
    last_status = None
    last_text = ""

    for attempt in range(retries):
        try:
            r = requests.post(
                url,
                data=data.encode("utf-8"),
                headers=headers,
                timeout=90
            )

            last_status = r.status_code
            last_text = r.text or ""

            # SOAP часто повертає 500 разом із XML Fault.
            # Тому не можна просто мовчки continue без збереження тіла відповіді.
            if r.status_code in (429, 502, 503, 504):
                print(f"Retry POST {attempt + 1}: HTTP {r.status_code}")
                time.sleep(delay)
                delay *= 2
                continue

            if r.status_code >= 400:
                raise Exception(
                    f"NovaPay HTTP {r.status_code}: {last_text[:3000]}"
                )

            return last_text

        except Exception as e:
            if attempt == retries - 1:
                raise e
            print(f"Retry POST {attempt + 1}: {e}")
            time.sleep(delay)
            delay *= 2

    raise Exception(f"NovaPay POST failed. Last HTTP {last_status}: {last_text[:3000]}")


def normalize_date(value):
    if not value:
        return ""

    value = str(value).strip()

    patterns = [
        "%d.%m.%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]

    for p in patterns:
        try:
            return datetime.strptime(value, p).strftime("%d.%m.%Y")
        except ValueError:
            pass

    return value[:10].replace("-", ".")


def already_success_today():
    ws = worksheet(LOG_SHEET)
    ensure_log_header(ws)

    rows = ws.get_all_values()
    today = datetime.now(KYIV).strftime("%Y-%m-%d")

    for row in reversed(rows[1:]):
        if len(row) >= 5 and row[0].startswith(today) and row[4] == "OK":
            return True

    return False


def ensure_log_header(ws):
    if not ws.get_all_values():
        ws.append_row(["Date", "Privat Added", "Monobank Added", "NovaPay Added", "Status"])


def write_log(privat_count, mono_count, novapay_count, status):
    ws = worksheet(LOG_SHEET)
    ensure_log_header(ws)

    ws.append_row([
        datetime.now(KYIV).strftime("%Y-%m-%d %H:%M:%S"),
        privat_count,
        mono_count,
        novapay_count,
        status,
    ])


# =========================
# PRIVATBANK
# =========================

def privat_uid(tx):
    return "_".join([
        str(tx.get("REF", "")),
        str(tx.get("REFN", "")),
        str(tx.get("DATE_TIME_DAT_OD_TIM_P", "")),
        str(tx.get("SUM", "")),
    ])


def import_privat():
    ws = worksheet(PRIVAT_SHEET)
    ids = existing_ids(ws, column=1)

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
        "token": PB_TOKEN,
        "User-Agent": "GitHubActions",
    })

    data = r.json()
    rows = []

    if data.get("status") != "SUCCESS":
        raise Exception(f"Privat API error: {data}")

    for tx in data.get("transactions", []):
        uid = privat_uid(tx)
        if not uid or uid in ids:
            continue

        rows.append([
            uid,
            normalize_date(tx.get("DATE_TIME_DAT_OD_TIM_P", "")),
            tx.get("TRANTYPE", ""),
            float(tx.get("SUM", 0) or 0),
            tx.get("CCY", ""),
            tx.get("AUT_CNTR_NAM", ""),
            tx.get("OSND", ""),
            tx.get("AUT_CNTR_ACC", ""),
        ])

        ids.add(uid)

    append_rows(ws, rows)
    return len(rows)


# =========================
# MONOBANK
# =========================

def mono_account_id():
    r = api_get(
        "https://api.monobank.ua/personal/client-info",
        headers={
            "X-Token": MONO_TOKEN,
            "Accept": "application/json",
            "User-Agent": "GitHubActions",
        },
    )

    data = r.json()

    for acc in data.get("accounts", []):
        if acc.get("iban") == MONO_IBAN:
            return acc["id"]

    raise Exception("Monobank account not found")


def mono_uid(tx):
    base = tx.get("id") or f'{tx.get("time")}_{tx.get("amount")}_{tx.get("description", "")}'
    return f"{MONO_IBAN}_{base}"


def import_mono():
    ws = worksheet(MONO_SHEET)
    ids = existing_ids(ws, column=1)

    account_id = mono_account_id()

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)

    url = f"https://api.monobank.ua/personal/statement/{account_id}/{int(start.timestamp())}/{int(now.timestamp())}"

    r = api_get(
        url,
        headers={
            "X-Token": MONO_TOKEN,
            "Accept": "application/json",
            "User-Agent": "GitHubActions",
        },
    )

    data = r.json()
    rows = []

    for tx in data:
        uid = mono_uid(tx)
        if uid in ids:
            continue

        dt = datetime.fromtimestamp(tx["time"], timezone.utc).astimezone(KYIV)
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
            tx.get("counterIban", ""),
        ])

        ids.add(uid)

    rows.sort(key=lambda x: x[2])
    append_rows(ws, rows)
    return len(rows)


# =========================
# NOVAPAY
# =========================

def xml_text(root, tag_name):
    for el in root.iter():
        if el.tag.split("}")[-1] == tag_name:
            return el.text or ""
    return ""


def xml_elements(root, tag_name):
    return [el for el in root.iter() if el.tag.split("}")[-1] == tag_name]


def novapay_config_read():
    ws = worksheet(NOVAPAY_CONFIG_SHEET, rows=20, cols=2)
    values = ws.get_all_values()

    data = {}
    for row in values:
        if len(row) >= 2 and row[0].strip():
            data[row[0].strip()] = row[1].strip()

    refresh_token = data.get("refresh_token")
    public_certificate = data.get("public_certificate")

    if not refresh_token or not public_certificate:
        raise Exception(
            "NovaPay_Config не заповнений. Потрібно A1=refresh_token, B1=токен; "
            "A2=public_certificate, B2=сертифікат PEM"
        )

    return refresh_token, public_certificate


def novapay_config_write(refresh_token, public_certificate):
    ws = worksheet(NOVAPAY_CONFIG_SHEET, rows=20, cols=2)
    ws.update("A1:B2", [
        ["refresh_token", refresh_token],
        ["public_certificate", public_certificate],
    ])


def soap_envelope(method_name, body_xml):
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
  <soapenv:Header/>
  <soapenv:Body>
    <tem:{method_name}>
      {body_xml}
    </tem:{method_name}>
  </soapenv:Body>
</soapenv:Envelope>"""


def novapay_call(method_name, body_xml):
    xml = soap_envelope(method_name, body_xml)

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": f'"http://tempuri.org/IClientAPIService/{method_name}"',
        "User-Agent": "GitHubActions",
    }

    text = api_post(NOVAPAY_ENDPOINT, xml, headers=headers)

    if not text:
        raise Exception(f"NovaPay empty response for {method_name}")

    return ET.fromstring(text)


def novapay_auth_jwt():
    refresh_token, public_certificate = novapay_config_read()

    request_ref = str(uuid.uuid4())

    body = f"""
<tem:request>
  <tem:request_ref>{html.escape(request_ref)}</tem:request_ref>
  <tem:refresh_token>{html.escape(refresh_token)}</tem:refresh_token>
  <tem:login>{html.escape(NOVAPAY_LOGIN)}</tem:login>
  <tem:public_certificate>{html.escape(public_certificate)}</tem:public_certificate>
</tem:request>
"""

    root = novapay_call("UserAuthenticationJWT", body)

    result = xml_text(root, "result")
    if result and result.lower() != "ok":
        raise Exception(f"NovaPay auth error: {ET.tostring(root, encoding='unicode')}")

    jwt = xml_text(root, "jwt")
    new_refresh_token = xml_text(root, "refresh_token")
    new_public_certificate = xml_text(root, "public_certificate")

    if not jwt or not new_refresh_token or not new_public_certificate:
        raise Exception(f"NovaPay auth response missing data: {ET.tostring(root, encoding='unicode')}")

    novapay_config_write(new_refresh_token, new_public_certificate)

    return jwt


def novapay_get_clients(jwt):
    body = f"""
<tem:request>
  <tem:request_ref>{html.escape(str(uuid.uuid4()))}</tem:request_ref>
  <tem:jwt>{html.escape(jwt)}</tem:jwt>
</tem:request>
"""

    root = novapay_call("GetClientsList", body)

    result = xml_text(root, "result")
    if result and result.lower() != "ok":
        raise Exception(f"NovaPay GetClientsList error: {ET.tostring(root, encoding='unicode')}")

    clients = xml_elements(root, "Clients")
    if not clients:
        clients = xml_elements(root, "Client")

    ids = []
    for client in clients:
        cid = ""
        for child in list(client):
            if child.tag.split("}")[-1].lower() == "id":
                cid = child.text or ""
                break
        if cid:
            ids.append(cid)

    if not ids:
        for el in xml_elements(root, "id"):
            if el.text and el.text.strip().isdigit():
                ids.append(el.text.strip())

    if not ids:
        raise Exception(f"NovaPay clients not found: {ET.tostring(root, encoding='unicode')}")

    return ids[0]


def novapay_get_single_account(jwt, client_id):
    body = f"""
<tem:request>
  <tem:request_ref>{html.escape(str(uuid.uuid4()))}</tem:request_ref>
  <tem:jwt>{html.escape(jwt)}</tem:jwt>
  <tem:client_id>{html.escape(str(client_id))}</tem:client_id>
</tem:request>
"""

    root = novapay_call("GetAccountsList", body)

    result = xml_text(root, "result")
    if result and result.lower() != "ok":
        raise Exception(f"NovaPay GetAccountsList error: {ET.tostring(root, encoding='unicode')}")

    accounts = xml_elements(root, "Accounts")
    if not accounts:
        accounts = xml_elements(root, "Account")

    account_ids = []
    for acc in accounts:
        status = ""
        acc_id = ""

        for child in list(acc):
            name = child.tag.split("}")[-1].lower()
            if name == "id":
                acc_id = child.text or ""
            elif name == "statuscode":
                status = child.text or ""

        if acc_id and (not status or status.lower() == "active"):
            account_ids.append(acc_id)

    if not account_ids:
        ids = [el.text.strip() for el in xml_elements(root, "id") if el.text and el.text.strip().isdigit()]
        account_ids = ids

    if not account_ids:
        raise Exception(f"NovaPay account not found: {ET.tostring(root, encoding='unicode')}")

    return account_ids[0]


def novapay_get_payments(jwt, account_id):
    today = datetime.now(KYIV)
    start = today - timedelta(days=29)

    date_from = start.strftime("%d.%m.%Y")
    date_to = today.strftime("%d.%m.%Y")

    body = f"""
<tem:request>
  <tem:request_ref>{html.escape(str(uuid.uuid4()))}</tem:request_ref>
  <tem:jwt>{html.escape(jwt)}</tem:jwt>
  <tem:account_id>{html.escape(str(account_id))}</tem:account_id>
  <tem:date_from>{date_from}</tem:date_from>
  <tem:date_to>{date_to}</tem:date_to>
  <tem:date_type>0</tem:date_type>
</tem:request>
"""

    root = novapay_call("GetPaymentsList", body)

    result = xml_text(root, "result")
    if result and result.lower() != "ok":
        response_text = ET.tostring(root, encoding="unicode")
        if "No documents found" in response_text or "відсутні платежі" in response_text:
            return []
        raise Exception(f"NovaPay GetPaymentsList error: {response_text}")

    payments_xml = xml_text(root, "payments")
    if not payments_xml.strip():
        return []

    payments_root = ET.fromstring(payments_xml)
    return xml_elements(payments_root, "Docs")


def child_text(parent, tag_name):
    for child in list(parent):
        if child.tag.split("}")[-1] == tag_name:
            return child.text or ""
    return ""


def import_novapay():
    ws = worksheet(NOVAPAY_SHEET, rows=1000, cols=10)

    existing_codes = existing_ids(ws, column=2)

    jwt = novapay_auth_jwt()
    client_id = novapay_get_clients(jwt)
    account_id = novapay_get_single_account(jwt, client_id)
    docs = novapay_get_payments(jwt, account_id)

    rows = []

    for doc in docs:
        code = child_text(doc, "Code").strip()
        if not code or code in existing_codes:
            continue

        amount = doc.attrib.get("Amount", "")
        payment_type = child_text(doc, "PaymentType").strip()
        purpose = child_text(doc, "Purpose").strip()

        date_value = (
            child_text(doc, "DayDate")
            or child_text(doc, "OrgDate")
            or child_text(doc, "PayDate")
        )

        rows.append([
            normalize_date(date_value),
            code,
            float(amount or 0),
            payment_type,
            purpose,
        ])

        existing_codes.add(code)

    rows.sort(key=lambda x: x[0])
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
    novapay_added = 0

    try:
        privat_added = import_privat()
        mono_added = import_mono()
        novapay_added = import_novapay()

        write_log(privat_added, mono_added, novapay_added, "OK")

        print(f"Privat added: {privat_added}")
        print(f"Monobank added: {mono_added}")
        print(f"NovaPay added: {novapay_added}")
        print("Success")

    except Exception as e:
        write_log(privat_added, mono_added, novapay_added, str(e))
        raise


if __name__ == "__main__":
    main()
