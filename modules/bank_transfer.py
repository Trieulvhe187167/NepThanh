import os
from urllib.parse import quote_plus


def _env_value(name, default=""):
    return (os.environ.get(name) or default).strip().strip('"').strip("'").strip()


def bank_transfer_enabled():
    return bool(bank_transfer_account_number())


def bank_transfer_bank_code():
    return _env_value("BANK_TRANSFER_BANK_CODE", "VCB").upper()


def bank_transfer_account_number():
    return _env_value("BANK_TRANSFER_ACCOUNT_NUMBER")


def bank_transfer_account_name():
    return _env_value("BANK_TRANSFER_ACCOUNT_NAME", "NEP THANH")


def bank_transfer_content(order_number):
    prefix = _env_value("BANK_TRANSFER_CONTENT_PREFIX", "NEPTHANH")
    return f"{prefix} {order_number}".strip()


def build_bank_transfer_instructions(order):
    if not order or not bank_transfer_enabled():
        return None

    amount = max(int(order["total"] or 0), 0)
    content = bank_transfer_content(order["order_number"])
    bank_code = bank_transfer_bank_code()
    account_number = bank_transfer_account_number()
    account_name = bank_transfer_account_name()
    template = _env_value("BANK_TRANSFER_QR_TEMPLATE", "compact2")
    qr_url = (
        f"https://img.vietqr.io/image/{quote_plus(bank_code)}-"
        f"{quote_plus(account_number)}-{quote_plus(template)}.png"
        f"?amount={amount}"
        f"&addInfo={quote_plus(content)}"
        f"&accountName={quote_plus(account_name)}"
    )

    return {
        "bank_code": bank_code,
        "account_number": account_number,
        "account_name": account_name,
        "amount": amount,
        "content": content,
        "qr_url": qr_url,
    }
