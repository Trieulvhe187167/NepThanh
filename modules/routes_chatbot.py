"""Routes blueprint for the AI Shop Assistant chatbot."""

import json
import uuid
from datetime import datetime

from flask import Blueprint, jsonify, render_template, request, session

from modules.auth import _get_current_user
from modules.chatbot import (
    chat,
    ensure_chatbot_tables,
    reset_session,
    _delete_draft,
    _get_draft,
)
from modules.db import _get_db
from modules.telegram_notify import send_order_notification
from modules.utils import _build_order_number

chatbot_bp = Blueprint("chatbot", __name__)


def register_chatbot_routes(app):
    ensure_chatbot_tables()
    app.register_blueprint(chatbot_bp)


# ---------------------------------------------------------------------------
# Helper – create a real order from draft data
# ---------------------------------------------------------------------------


def _create_bot_order(draft_data, session_id):
    """Create an order record from bot-collected data.  Returns order dict."""
    conn = _get_db()
    now = datetime.utcnow().isoformat()
    order_number = _build_order_number()
    price = draft_data.get("price", 0)
    ship_fee = draft_data.get("ship_fee", 0)
    total = price + ship_fee

    order_cursor = conn.execute(
        """
        INSERT INTO orders (
            order_number, status, subtotal, shipping_fee, discount_amount,
            total, payment_status, recipient_name, phone, line1,
            notes, created_at, updated_at
        ) VALUES (?, 'new', ?, ?, 0, ?, 'unpaid', ?, ?, ?, ?, ?, ?)
        """,
        (
            order_number,
            price,
            ship_fee,
            total,
            draft_data.get("customer_name", ""),
            draft_data.get("phone", ""),
            draft_data.get("address", ""),
            f"Đơn từ AI Bot (session: {session_id})",
            now,
            now,
        ),
    )
    order_id = order_cursor.lastrowid

    # Insert order item
    product_name = draft_data.get("product_name", "Sản phẩm Nếp Thanh")
    variant_label = f"{draft_data.get('size', '')} / {draft_data.get('color', '')}".strip(" /")
    conn.execute(
        """
        INSERT INTO order_items (
            order_id, product_id, variant_id, product_name, variant_label,
            sku, qty, unit_price, total_price
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            order_id,
            draft_data.get("product_id"),
            draft_data.get("variant_id"),
            product_name,
            variant_label,
            draft_data.get("sku", ""),
            price,
            price,
        ),
    )

    # Status event
    conn.execute(
        """
        INSERT INTO order_status_events (order_id, status, note, created_at)
        VALUES (?, 'new', 'Đơn hàng tạo từ AI Shop Assistant', ?)
        """,
        (order_id, now),
    )

    conn.commit()
    conn.close()

    draft_data["order_number"] = order_number
    draft_data["order_id"] = order_id
    return draft_data


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@chatbot_bp.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.get_json(force=True, silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "Vui lòng nhập tin nhắn"}), 400

    session_id = data.get("session_id") or session.get("chat_session_id")
    if not session_id:
        session_id = str(uuid.uuid4())
        session["chat_session_id"] = session_id

    user = _get_current_user()
    user_id = user["id"] if user else None

    result = chat(session_id, message, user_id)

    # If action is create_order, actually create it
    if result.get("action") == "create_order":
        draft_data = result.get("entities", {})
        try:
            order = _create_bot_order(draft_data, session_id)
            _delete_draft(session_id)
            order_number = order["order_number"]
            total = order.get("price", 0) + order.get("ship_fee", 0)
            result["reply"] = (
                f"✅ **Đặt hàng thành công!**\n\n"
                f"📦 Mã đơn hàng: **{order_number}**\n"
                f"💵 Tổng: **{total:,} VND** (COD – thanh toán khi nhận hàng)\n\n"
                f"Shop sẽ xác nhận và giao hàng trong 1-3 ngày. "
                f"Bạn có thể theo dõi đơn tại mục **Theo dõi đơn hàng** trên website.\n\n"
                f"Cảm ơn bạn đã mua hàng tại Nếp Thanh! 🎉"
            )
            # Send Telegram notification
            send_order_notification(order)
        except Exception as e:
            result["reply"] = (
                "Xin lỗi, mình gặp lỗi khi tạo đơn hàng. "
                "Bạn vui lòng thử lại hoặc liên hệ shop qua email nepthanh6886@gmail.com nhé! 🙏"
            )
            result["action"] = "handoff"

    result["session_id"] = session_id
    return jsonify(result)


@chatbot_bp.route("/api/chat/reset", methods=["POST"])
def api_chat_reset():
    data = request.get_json(force=True, silent=True) or {}
    session_id = data.get("session_id") or session.get("chat_session_id")
    if session_id:
        reset_session(session_id)
    new_session_id = str(uuid.uuid4())
    session["chat_session_id"] = new_session_id
    return jsonify({"session_id": new_session_id, "message": "Đã reset cuộc trò chuyện."})


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------


@chatbot_bp.route("/assistant")
def assistant_page():
    return render_template(
        "assistant.html",
        title="AI Shop Assistant – Nếp Thanh",
        description="Trợ lý mua hàng AI 24/7 – hỏi giá, xem size, chốt đơn ngay trên website.",
    )
