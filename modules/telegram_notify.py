"""Telegram notification for new bot orders."""

import os
import json
import requests


def _telegram_config():
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    return token, chat_id


def send_order_notification(order_data):
    """Send order summary to Telegram.  Returns True on success."""
    token, chat_id = _telegram_config()
    if not token or not chat_id:
        return False

    product = order_data.get("product_name", "?")
    size = order_data.get("size", "?")
    color = order_data.get("color", "")
    price = order_data.get("price", 0)
    ship_fee = order_data.get("ship_fee", 0)
    total = price + ship_fee
    name = order_data.get("customer_name", "?")
    phone = order_data.get("phone", "?")
    address = order_data.get("address", "?")
    order_number = order_data.get("order_number", "BOT-???")

    text = (
        f"🛒 *ĐƠN HÀNG MỚI TỪ BOT*\n\n"
        f"📦 Mã đơn: `{order_number}`\n"
        f"🏷️ Sản phẩm: *{product}*\n"
        f"📏 Size: {size}"
        + (f" / Màu: {color}" if color else "")
        + f"\n💰 Giá: {price:,} VND\n"
        f"🚚 Ship: {ship_fee:,} VND\n"
        f"💵 *Tổng: {total:,} VND*\n\n"
        f"👤 {name}\n"
        f"📞 {phone}\n"
        f"📍 {address}"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
            },
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False
