import os
import smtplib
from email.message import EmailMessage


def _smtp_config():
    host = (os.environ.get("SMTP_HOST") or "").strip()
    port = int((os.environ.get("SMTP_PORT") or "587").strip() or "587")
    user = (os.environ.get("SMTP_USER") or "").strip()
    password = (os.environ.get("SMTP_PASSWORD") or "").strip()
    sender = (os.environ.get("SMTP_FROM") or user or "").strip()
    use_tls = (os.environ.get("SMTP_USE_TLS") or "1").strip() != "0"
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "sender": sender,
        "use_tls": use_tls,
    }


def _send_email(to_email, subject, body_text):
    config = _smtp_config()
    if not config["host"] or not config["sender"] or not to_email:
        return False
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config["sender"]
    message["To"] = to_email
    message.set_content(body_text)
    try:
        with smtplib.SMTP(config["host"], config["port"], timeout=15) as smtp:
            if config["use_tls"]:
                smtp.starttls()
            if config["user"] and config["password"]:
                smtp.login(config["user"], config["password"])
            smtp.send_message(message)
        return True
    except (OSError, smtplib.SMTPException):
        return False


def _order_items_lines(items):
    lines = []
    for item in items:
        lines.append(
            f"- {item['product_name']} ({item['variant_label'] or '-'}) x{item['qty']}: {item['total_price']:,} VND"
        )
    return "\n".join(lines) if lines else "- Chua co san pham"


def send_order_confirmation_email(order, items):
    if not order or not order.get("email"):
        return False
    subject = f"[Nep Thanh] Xac nhan dat hang {order['order_number']}"
    body = (
        f"Xin chao {order.get('recipient_name') or 'ban'},\n\n"
        f"Cam on ban da dat hang tai Nep Thanh.\n"
        f"Ma don hang: {order['order_number']}\n"
        f"Trang thai don: {order.get('status')}\n"
        f"Trang thai thanh toan: {order.get('payment_status')}\n\n"
        f"San pham:\n{_order_items_lines(items)}\n\n"
        f"Tam tinh: {order.get('subtotal', 0):,} VND\n"
        f"Giam gia: {order.get('discount_amount', 0):,} VND\n"
        f"Van chuyen: {order.get('shipping_fee', 0):,} VND\n"
        f"Tong thanh toan: {order.get('total', 0):,} VND\n\n"
        f"Dia chi nhan hang: {order.get('line1') or ''} {order.get('line2') or ''}, "
        f"{order.get('ward') or ''}, {order.get('district') or ''}, {order.get('province') or ''}\n"
        f"Sdt: {order.get('phone') or ''}\n\n"
        f"Don vi van chuyen: {order.get('shipping_provider') or '-'}\n"
        f"Ma van don: {order.get('tracking_code') or '-'}\n\n"
        "Chung toi se cap nhat trang thai don hang cho ban qua email.\n"
    )
    return _send_email(order["email"], subject, body)


def send_order_status_email(order, items, status_note=None):
    if not order or not order.get("email"):
        return False
    subject = f"[Nep Thanh] Cap nhat don hang {order['order_number']}"
    body = (
        f"Xin chao {order.get('recipient_name') or 'ban'},\n\n"
        f"Don hang {order['order_number']} vua duoc cap nhat.\n"
        f"Trang thai don: {order.get('status')}\n"
        f"Trang thai thanh toan: {order.get('payment_status')}\n"
        f"Don vi van chuyen: {order.get('shipping_provider') or '-'}\n"
        f"Ma van don: {order.get('tracking_code') or '-'}\n"
        f"Ghi chu: {status_note or '-'}\n\n"
        f"San pham:\n{_order_items_lines(items)}\n\n"
        f"Tong thanh toan: {order.get('total', 0):,} VND\n"
    )
    return _send_email(order["email"], subject, body)
