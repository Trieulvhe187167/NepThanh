import hashlib
import io
import os
import re
import secrets
import unicodedata
import urllib.parse
from datetime import datetime
from werkzeug.utils import secure_filename

from modules.config import BASE_DIR, UPLOAD_DIR


def _normalize_static_path(value):
    if not value:
        return None
    value = value.strip().replace("\\", "/")
    if value.startswith("/static/"):
        value = value[len("/static/"):]
    elif value.startswith("static/"):
        value = value[len("static/"):]
    return value.lstrip("/")


def _static_path_exists(relative_path):
    return os.path.exists(os.path.join(BASE_DIR, "static", *relative_path.split("/")))


def _slugify(value):
    value = (value or "").strip()
    if not value:
        return secrets.token_hex(4)
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value).strip("-").lower()
    return cleaned or secrets.token_hex(4)


def _parse_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _build_order_number():
    stamp = datetime.utcnow().strftime("%Y%m%d")
    token = secrets.token_hex(2).upper()
    return f"NT{stamp}{token}"


def _save_upload(file_storage, subdir):
    if not file_storage or not file_storage.filename:
        return None
    filename = secure_filename(file_storage.filename)
    if not filename:
        return None
    target_dir = os.path.join(UPLOAD_DIR, subdir)
    os.makedirs(target_dir, exist_ok=True)
    unique = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(3)}_{filename}"
    path = os.path.join(target_dir, unique)
    file_storage.save(path)
    rel_path = os.path.relpath(path, os.path.join(BASE_DIR, "static"))
    return rel_path.replace("\\", "/")


def _hash_ip(ip_address):
    if not ip_address:
        return None
    return hashlib.sha256(ip_address.encode("utf-8")).hexdigest()[:24]


def _generate_qr_png(data):
    try:
        import qrcode
    except ImportError:
        return None
    img = qrcode.make(data)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _safe_next_url(value):
    if value and value.startswith("/"):
        return value
    return None


def _safe_background_url(candidate):
    if not candidate:
        return None
    parsed = urllib.parse.urlparse(candidate)
    if parsed.scheme or parsed.netloc:
        return None
    path = parsed.path or "/"
    blocked = {
        "/login",
        "/signup",
        "/logout",
        "/auth/google",
        "/auth/google/callback",
    }
    if path in blocked or path.startswith("/auth/"):
        return None
    if parsed.query:
        return f"{path}?{parsed.query}"
    return path


def _background_from_referrer(referrer, host):
    if not referrer:
        return None
    try:
        parsed = urllib.parse.urlparse(referrer)
    except ValueError:
        return None
    if parsed.netloc and parsed.netloc != host:
        return None
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return _safe_background_url(path)
