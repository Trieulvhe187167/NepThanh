import uuid
from flask import Blueprint, jsonify, render_template, request, session

from modules.auth import _get_current_user
from modules.chatbot import (
    chat,
    ensure_chatbot_tables,
    reset_session,
    _delete_draft,
)
from modules.checkout import place_order_from_bot_draft
from modules.telegram_notify import send_order_notification

chatbot_bp = Blueprint("chatbot", __name__)


def register_chatbot_routes(app):
    app.register_blueprint(chatbot_bp)


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
            order_result = place_order_from_bot_draft(draft_data, session_id, user=user)
            if not order_result.get("ok"):
                raise ValueError(order_result.get("error") or "Could not create bot order")
            _delete_draft(session_id)
            order_number = order_result["order_number"]
            total = order_result.get("total", 0)
            result["reply"] = (
                f"✅ **Đặt hàng thành công!**\n\n"
                f"📦 Mã đơn hàng: **{order_number}**\n"
                f"💵 Tổng: **{total:,} VND** (COD – thanh toán khi nhận hàng)\n\n"
                f"Shop sẽ xác nhận và giao hàng trong 1-3 ngày. "
                f"Bạn có thể theo dõi đơn tại mục **Theo dõi đơn hàng** trên website.\n\n"
                f"Cảm ơn bạn đã mua hàng tại Nếp Thanh! 🎉"
            )
            # Gửi Telegram (nếu có token)
            telegram_order = dict(draft_data)
            telegram_order.update({
                "order_number": order_number,
                "order_id": order_result["order_id"],
                "ship_fee": order_result["order"].get("shipping_fee", 0),
                "price": order_result["order"].get("subtotal", draft_data.get("price", 0)),
            })
            send_order_notification(telegram_order)
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
    ensure_chatbot_tables()
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
