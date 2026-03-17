import hashlib
import hmac
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode


def vnpay_enabled():
    return bool(_tmn_code() and _hash_secret())


def _tmn_code():
    return (os.environ.get("VNPAY_TMN_CODE") or "").strip()


def _hash_secret():
    return (os.environ.get("VNPAY_HASH_SECRET") or "").strip()


def _payment_url():
    return (
        os.environ.get("VNPAY_PAYMENT_URL")
        or "https://sandbox.vnpayment.vn/paymentv2/vpcpay.html"
    ).strip()


def _canonical_query(params):
    pairs = []
    for key in sorted(params.keys()):
        value = params[key]
        if value is None or value == "":
            continue
        pairs.append((key, value))
    return urlencode(pairs)


def _secure_hash(canonical_query):
    return hmac.new(
        _hash_secret().encode("utf-8"),
        canonical_query.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()


def build_vnpay_payment_url(order_number, amount_vnd, return_url, ip_addr):
    if not vnpay_enabled():
        return None
    now = datetime.utcnow()
    params = {
        "vnp_Version": "2.1.0",
        "vnp_Command": "pay",
        "vnp_TmnCode": _tmn_code(),
        "vnp_Amount": str(max(int(amount_vnd), 0) * 100),
        "vnp_CreateDate": now.strftime("%Y%m%d%H%M%S"),
        "vnp_CurrCode": "VND",
        "vnp_IpAddr": ip_addr or "127.0.0.1",
        "vnp_Locale": "vn",
        "vnp_OrderInfo": f"Thanh toan don hang {order_number}",
        "vnp_OrderType": "other",
        "vnp_ReturnUrl": return_url,
        "vnp_TxnRef": order_number,
        "vnp_ExpireDate": (now + timedelta(minutes=15)).strftime("%Y%m%d%H%M%S"),
    }
    canonical_query = _canonical_query(params)
    secure_hash = _secure_hash(canonical_query)
    return f"{_payment_url()}?{canonical_query}&vnp_SecureHash={secure_hash}"


def verify_vnpay_response(params):
    provided_hash = (params.get("vnp_SecureHash") or "").strip().lower()
    if not provided_hash:
        return False
    filtered = {}
    for key, value in params.items():
        if key in {"vnp_SecureHash", "vnp_SecureHashType"}:
            continue
        if not key.startswith("vnp_"):
            continue
        filtered[key] = value
    canonical_query = _canonical_query(filtered)
    expected_hash = _secure_hash(canonical_query).lower()
    return hmac.compare_digest(expected_hash, provided_hash)


def is_vnpay_success(params):
    response_code = (params.get("vnp_ResponseCode") or "").strip()
    txn_status = (params.get("vnp_TransactionStatus") or "").strip()
    if txn_status:
        return response_code == "00" and txn_status == "00"
    return response_code == "00"


def parse_vnpay_amount(params):
    raw = str(params.get("vnp_Amount") or "0").strip()
    try:
        return int(raw) // 100
    except ValueError:
        return 0
