import os
import smtplib
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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


def _send_html_email(to_email, subject, body_html, body_text=None):
    """Send an HTML email (with plain-text fallback)."""
    config = _smtp_config()
    if not config["host"] or not config["sender"] or not to_email:
        return False
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Nếp Thanh <{config['sender']}>"
    msg["To"] = to_email
    if body_text:
        msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))
    try:
        with smtplib.SMTP(config["host"], config["port"], timeout=15) as smtp:
            if config["use_tls"]:
                smtp.starttls()
            if config["user"] and config["password"]:
                smtp.login(config["user"], config["password"])
            smtp.send_message(msg)
        return True
    except (OSError, smtplib.SMTPException):
        return False


def send_email_verification_email(to_email, verify_url):
    subject = "[Nếp Thanh] Xác thực email tài khoản"
    body_text = (
        "Xác thực email tài khoản Nếp Thanh\n\n"
        f"Mở liên kết này để xác thực email: {verify_url}\n"
        "Liên kết sẽ hết hạn sau 24 giờ."
    )
    body_html = f"""
    <p>Xin chào,</p>
    <p>Bạn vừa tạo tài khoản tại <strong>Nếp Thanh</strong>. Vui lòng xác thực email để hoàn tất bảo mật tài khoản.</p>
    <p><a href="{verify_url}" style="display:inline-block;background:#B22222;color:#fff;padding:10px 16px;border-radius:6px;text-decoration:none;">Xác thực email</a></p>
    <p>Nếu nút không hoạt động, hãy mở liên kết này:<br>{verify_url}</p>
    <p>Liên kết sẽ hết hạn sau 24 giờ.</p>
    """
    return _send_html_email(to_email, subject, body_html, body_text)


def send_password_reset_email(to_email, reset_url):
    subject = "[Nếp Thanh] Đặt lại mật khẩu"
    body_text = (
        "Đặt lại mật khẩu Nếp Thanh\n\n"
        f"Mở liên kết này để đặt lại mật khẩu: {reset_url}\n"
        "Liên kết sẽ hết hạn sau 60 phút. Nếu bạn không yêu cầu, hãy bỏ qua email này."
    )
    body_html = f"""
    <p>Xin chào,</p>
    <p>Chúng tôi nhận được yêu cầu đặt lại mật khẩu cho tài khoản Nếp Thanh của bạn.</p>
    <p><a href="{reset_url}" style="display:inline-block;background:#B22222;color:#fff;padding:10px 16px;border-radius:6px;text-decoration:none;">Đặt lại mật khẩu</a></p>
    <p>Nếu nút không hoạt động, hãy mở liên kết này:<br>{reset_url}</p>
    <p>Liên kết sẽ hết hạn sau 60 phút. Nếu bạn không yêu cầu, hãy bỏ qua email này.</p>
    """
    return _send_html_email(to_email, subject, body_html, body_text)


def _order_items_table_html(items):
    """Trả về HTML table các sản phẩm trong email."""
    if not items:
        return "<tr><td colspan='4' style='padding:12px;text-align:center;color:#888;font-style:italic;'>Chưa có chi tiết sản phẩm.</td></tr>"
    rows = ""
    for it in items:
        name    = it.get("product_name", "?")
        variant = it.get("variant_label") or "-"
        qty     = it.get("qty", 1)
        total   = it.get("total_price", 0) or 0
        rows += (
            f"<tr>"
            f"<td style='padding:10px 8px;border-bottom:1px solid #F5EBD8;color:#2B1F1F;font-size:14px;'>{name}<br>"
            f"<span style='color:#6B5C5C;font-size:12px;'>{variant}</span></td>"
            f"<td style='padding:10px 8px;border-bottom:1px solid #F5EBD8;text-align:center;color:#2B1F1F;font-size:14px;'>{qty}</td>"
            f"<td style='padding:10px 8px;border-bottom:1px solid #F5EBD8;text-align:right;color:#B22222;font-weight:600;font-size:14px;'>{total:,} ₫</td>"
            f"</tr>"
        )
    return rows


def _order_items_lines(items):
    lines = []
    for item in items:
        lines.append(
            f"- {item['product_name']} ({item['variant_label'] or '-'}) x{item['qty']}: {item['total_price']:,} VND"
        )
    return "\n".join(lines) if lines else "- Chưa có sản phẩm"


def _email_base_html(header_title, header_sub, content_html):
    """Template HTML cơ sở theo brand Nếp Thanh."""
    return f"""
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=Be+Vietnam+Pro:wght@300;400;600;700;800&display=swap" rel="stylesheet">
<title>{header_title}</title>
</head>
<body style="margin:0;padding:0;background:#FFF8F0;font-family:'Be Vietnam Pro','Segoe UI',Arial,sans-serif;">

<!-- Wrapper -->
<table width="100%" cellpadding="0" cellspacing="0" bgcolor="#FFF8F0">
  <tr><td align="center" style="padding:32px 16px 40px;">

    <!-- Email card -->
    <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#FFFFFF;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(43,31,31,0.10);">

      <!-- Top gold bar -->
      <tr><td style="background:linear-gradient(90deg,#D4A843,#B22222,#D4A843);height:4px;"></td></tr>

      <!-- Header -->
      <tr>
        <td style="background:linear-gradient(135deg,#8B0000 0%,#B22222 60%,#8B0000 100%);padding:32px 36px 28px;text-align:center;">
          <!-- Logo text -->
          <div style="margin-bottom:16px;">
            <span style="font-size:26px;font-weight:800;color:#D4A843;letter-spacing:0.06em;text-transform:uppercase;">Nếp Thanh</span>
            <div style="width:60px;height:2px;background:linear-gradient(90deg,transparent,#D4A843,transparent);margin:8px auto 0;"></div>
          </div>
          <h1 style="margin:0;color:#FFFFFF;font-size:20px;font-weight:700;line-height:1.4;">{header_title}</h1>
          <p style="margin:8px 0 0;color:rgba(255,248,240,0.80);font-size:13px;font-weight:300;">{header_sub}</p>
        </td>
      </tr>

      <!-- Content -->
      <tr><td style="padding:32px 36px;">{content_html}</td></tr>

      <!-- Bottom gold bar -->
      <tr><td style="background:linear-gradient(90deg,#D4A843,#B22222,#D4A843);height:2px;"></td></tr>

      <!-- Footer -->
      <tr>
        <td style="background:#2B1F1F;padding:20px 36px;text-align:center;">
          <p style="margin:0 0 4px;color:#D4A843;font-size:13px;font-weight:600;letter-spacing:0.04em;">Nếp Thanh – Dòng chảy thanh âm Việt</p>
          <p style="margin:0;color:rgba(255,248,240,0.45);font-size:11px;">Email tự động – vui lòng không trả lời | nepthanh6886@gmail.com</p>
        </td>
      </tr>

    </table>
  </td></tr>
</table>

</body>
</html>"""


def send_order_confirmation_email(order, items):
    """Email xác nhận đặt hàng gửi tới khách hàng sau khi tạo đơn thành công."""
    if not order or not order.get("email"):
        return False

    name        = order.get("recipient_name") or "bạn"
    order_num   = order["order_number"]
    subtotal    = order.get("subtotal", 0) or 0
    discount    = order.get("discount_amount", 0) or 0
    ship_fee    = order.get("shipping_fee", 0) or 0
    total       = order.get("total", 0) or 0
    payment     = order.get("payment_status", "-")
    status      = order.get("status", "-")
    phone       = order.get("phone") or "-"
    provider    = order.get("shipping_provider") or "-"
    tracking    = order.get("tracking_code") or "-"

    addr_parts  = [
        order.get("line1"), order.get("line2"), order.get("ward"),
        order.get("district"), order.get("province"),
    ]
    address = ", ".join(p for p in addr_parts if p) or "-"

    payment_badge_color = "#2e7d32" if payment == "paid" else "#B22222"
    payment_label = {
        "paid": "Đã thanh toán", "unpaid": "Chưa thanh toán",
        "pending": "Đang chờ", "failed": "Thất bại",
    }.get(payment, payment)

    items_rows = _order_items_table_html(items)
    subject = f"🛒 [Nếp Thanh] Xác nhận đơn hàng {order_num}"

    content = f"""
      <!-- Greeting -->
      <p style="margin:0 0 20px;color:#2B1F1F;font-size:16px;line-height:1.7;">
        Xin chào <strong>{name}</strong>,<br>
        Cảm ơn bạn đã tin tưởng và đặt hàng tại <strong style="color:#B22222;">Nếp Thanh</strong>.
        Chúng tôi đã ghi nhận đơn hàng của bạn.
      </p>

      <!-- Order code -->
      <div style="background:#FFF8F0;border:1px solid #F5EBD8;border-left:4px solid #D4A843;border-radius:8px;padding:14px 18px;margin-bottom:24px;">
        <span style="font-size:12px;color:#6B5C5C;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;">Mã đơn hàng</span><br>
        <span style="font-size:22px;font-weight:800;color:#B22222;letter-spacing:0.04em;">{order_num}</span>
        <span style="margin-left:12px;background:{payment_badge_color};color:#fff;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;">{payment_label}</span>
      </div>

      <!-- Products table -->
      <h2 style="margin:0 0 12px;font-size:15px;color:#8B0000;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;">🏷️ Sản phẩm</h2>
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:20px;">
        <thead>
          <tr style="background:#FFF8F0;">
            <th style="padding:10px 8px;text-align:left;font-size:12px;color:#6B5C5C;font-weight:600;text-transform:uppercase;letter-spacing:0.04em;border-bottom:2px solid #D4A843;">Sản phẩm</th>
            <th style="padding:10px 8px;text-align:center;font-size:12px;color:#6B5C5C;font-weight:600;text-transform:uppercase;letter-spacing:0.04em;border-bottom:2px solid #D4A843;">SL</th>
            <th style="padding:10px 8px;text-align:right;font-size:12px;color:#6B5C5C;font-weight:600;text-transform:uppercase;letter-spacing:0.04em;border-bottom:2px solid #D4A843;">Thành tiền</th>
          </tr>
        </thead>
        <tbody>{items_rows}</tbody>
      </table>

      <!-- Totals -->
      <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
        <tr><td style="padding:4px 0;color:#6B5C5C;font-size:14px;">Tạm tính</td><td style="text-align:right;color:#2B1F1F;font-size:14px;">{subtotal:,} ₫</td></tr>
        <tr><td style="padding:4px 0;color:#6B5C5C;font-size:14px;">Giảm giá</td><td style="text-align:right;color:#2e7d32;font-size:14px;">-{discount:,} ₫</td></tr>
        <tr><td style="padding:4px 0;color:#6B5C5C;font-size:14px;">Phí vận chuyển</td><td style="text-align:right;color:#2B1F1F;font-size:14px;">{ship_fee:,} ₫</td></tr>
        <tr><td colspan="2"><div style="height:1px;background:linear-gradient(90deg,#D4A843,transparent);margin:8px 0;"></div></td></tr>
        <tr>
          <td style="padding:4px 0;color:#8B0000;font-size:17px;font-weight:800;">TỔNG CỘNG</td>
          <td style="text-align:right;color:#B22222;font-size:20px;font-weight:800;">{total:,} ₫</td>
        </tr>
      </table>

      <!-- Shipping info -->
      <h2 style="margin:0 0 12px;font-size:15px;color:#8B0000;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;">📦 Thông tin giao hàng</h2>
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#FFF8F0;border-radius:10px;padding:16px;margin-bottom:24px;font-size:14px;">
        <tr><td style="color:#6B5C5C;padding:4px 0;width:130px;">Số điện thoại:</td><td style="color:#2B1F1F;font-weight:600;">{phone}</td></tr>
        <tr><td style="color:#6B5C5C;padding:4px 0;">Địa chỉ:</td><td style="color:#2B1F1F;">{address}</td></tr>
        <tr><td style="color:#6B5C5C;padding:4px 0;">Đơn vị VC:</td><td style="color:#2B1F1F;">{provider}</td></tr>
        <tr><td style="color:#6B5C5C;padding:4px 0;">Mã vận đơn:</td><td style="color:#2B1F1F;">{tracking}</td></tr>
      </table>

      <!-- Note -->
      <p style="margin:0;color:#6B5C5C;font-size:13px;line-height:1.7;font-style:italic;">
        Chúng tôi sẽ cập nhật trạng thái đơn hàng qua email khi có thay đổi.<br>
        Mọi thắc mắc, bạn vui lòng liên hệ <strong style="color:#B22222;">nepthanh6886@gmail.com</strong>.
      </p>
    """

    html = _email_base_html(
        header_title=f"Xác nhận đơn hàng {order_num}",
        header_sub="Chúng tôi đã nhận đơn hàng của bạn – cảm ơn đã tin tưởng Nếp Thanh!",
        content_html=content,
    )
    body_text = (
        f"Xác nhận đơn hàng {order_num}\n"
        f"Khách hàng: {name}\nĐịa chỉ: {address}\nSĐT: {phone}\n\n"
        f"Sản phẩm:\n{_order_items_lines(items)}\n\n"
        f"Tạm tính: {subtotal:,} VND | Giảm: -{discount:,} | Ship: {ship_fee:,}\n"
        f"TỔNG: {total:,} VND\nThanh toán: {payment_label}\n"
    )
    return _send_html_email(order["email"], subject, html, body_text)


def send_order_status_email(order, items, status_note=None):
    """Email cập nhật trạng thái đơn được gửi khi admin chỉnh sửa đơn."""
    if not order or not order.get("email"):
        return False

    name        = order.get("recipient_name") or "bạn"
    order_num   = order["order_number"]
    total       = order.get("total", 0) or 0
    subtotal    = order.get("subtotal", 0) or 0
    discount    = order.get("discount_amount", 0) or 0
    ship_fee    = order.get("shipping_fee", 0) or 0
    status      = order.get("status", "-")
    payment     = order.get("payment_status", "-")
    provider    = order.get("shipping_provider") or "-"
    tracking    = order.get("tracking_code") or "-"
    note_text   = status_note or ""

    # Emoji và nhãn trạng thái tách riêng để tránh lỗi split
    _status_info = {
        "new":        ("📥", "Mới tạo"),
        "confirmed":  ("✅", "Đã xác nhận"),
        "processing": ("⏳", "Đang xử lý"),
        "shipped":    ("🚚", "Đã gửi hàng"),
        "completed":  ("🎉", "Hoàn thành"),
        "cancelled":  ("❌", "Đã hủy"),
        "refunded":   ("💰", "Hoàn tiền"),
        "returned":   ("🔄", "Trả hàng"),
    }
    status_emoji, status_text = _status_info.get(status, ("📋", status))

    # Màu banner theo brand Nếp Thanh
    status_color = {
        "new":        "#6B5C5C",   # nâu nhạt (chờ xử lý)
        "confirmed":  "#2E7D32",   # xanh lá (đã nhận)
        "processing": "#A8842F",   # vàng nâu brand (vn-gold-dark)
        "shipped":    "#B22222",   # đỏ brand (đang giao)
        "completed":  "#8B0000",   # đỏ đậm brand (hoàn thành)
        "cancelled":  "#7B1C1C",   # đỏ sẫn độc
        "refunded":   "#5C4033",   # nâu đậm
        "returned":   "#4E342E",   # nâu gỗ
    }.get(status, "#2B1F1F")

    payment_label = {
        "paid": "Đã thanh toán", "unpaid": "Chưa thanh toán",
        "pending": "Đang chờ", "failed": "Thất bại",
    }.get(payment, payment)

    items_rows = _order_items_table_html(items)
    subject = f"📦 [Nếp Thanh] Cập nhật đơn hàng {order_num}"

    content = f"""
      <!-- Greeting -->
      <p style="margin:0 0 20px;color:#2B1F1F;font-size:16px;line-height:1.7;">
        Xin chào <strong>{name}</strong>,<br>
        Đơn hàng <strong style="color:#B22222;">{order_num}</strong> của bạn vừa được cập nhật trạng thái.
      </p>

      <!-- Status banner -->
      <div style="background:{status_color};border-radius:10px;padding:20px 24px;margin-bottom:24px;text-align:center;border-bottom:3px solid rgba(212,168,67,0.4);">
        <div style="font-size:32px;margin-bottom:8px;line-height:1;">{status_emoji}</div>
        <div style="color:#FFFFFF;font-size:18px;font-weight:700;letter-spacing:0.02em;">{status_text}</div>
        <div style="display:inline-block;margin-top:10px;background:rgba(255,255,255,0.15);border-radius:20px;padding:4px 14px;">
          <span style="color:rgba(255,248,240,0.9);font-size:12px;font-weight:500;">Thanh toán: {payment_label}</span>
        </div>
      </div>

      <!-- Note from admin -->
      {'<div style="background:#FFF8F0;border-left:4px solid #D4A843;border-radius:6px;padding:12px 16px;margin-bottom:24px;"><p style="margin:0;color:#6B5C5C;font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;">Ghi chú từ chúng tôi</p><p style="margin:6px 0 0;color:#2B1F1F;font-size:14px;line-height:1.6;">' + note_text + '</p></div>' if note_text else ''}

      <!-- Shipping info -->
      <h2 style="margin:0 0 12px;font-size:15px;color:#8B0000;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;">📦 Vận chuyển &amp; theo dõi</h2>
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#FFF8F0;border-radius:10px;padding:16px;margin-bottom:24px;font-size:14px;">
        <tr><td style="color:#6B5C5C;padding:4px 0;width:130px;">Đơn vị VC:</td><td style="color:#2B1F1F;font-weight:600;">{provider}</td></tr>
        <tr><td style="color:#6B5C5C;padding:4px 0;">Mã vận đơn:</td><td style="color:#B22222;font-weight:700;font-size:15px;">{tracking}</td></tr>
      </table>

      <!-- Products -->
      <h2 style="margin:0 0 12px;font-size:15px;color:#8B0000;font-weight:700;text-transform:uppercase;letter-spacing:0.05em;">🏷️ Sản phẩm trong đơn</h2>
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:20px;">
        <thead>
          <tr style="background:#FFF8F0;">
            <th style="padding:10px 8px;text-align:left;font-size:12px;color:#6B5C5C;font-weight:600;text-transform:uppercase;border-bottom:2px solid #D4A843;">Sản phẩm</th>
            <th style="padding:10px 8px;text-align:center;font-size:12px;color:#6B5C5C;font-weight:600;text-transform:uppercase;border-bottom:2px solid #D4A843;">SL</th>
            <th style="padding:10px 8px;text-align:right;font-size:12px;color:#6B5C5C;font-weight:600;text-transform:uppercase;border-bottom:2px solid #D4A843;">Thành tiền</th>
          </tr>
        </thead>
        <tbody>{items_rows}</tbody>
      </table>

      <!-- Totals breakdown -->
      <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
        <tr><td style="padding:4px 0;color:#6B5C5C;font-size:14px;">Tạm tính</td><td style="text-align:right;color:#2B1F1F;font-size:14px;">{subtotal:,} ₫</td></tr>
        {'<tr><td style="padding:4px 0;color:#6B5C5C;font-size:14px;">Giảm giá</td><td style="text-align:right;color:#2e7d32;font-size:14px;">-' + f"{discount:,}" + ' ₫</td></tr>' if discount else ''}
        <tr><td style="padding:4px 0;color:#6B5C5C;font-size:14px;">Phí vận chuyển</td><td style="text-align:right;color:#2B1F1F;font-size:14px;">{ship_fee:,} ₫</td></tr>
        <tr><td colspan="2"><div style="height:1px;background:linear-gradient(90deg,#D4A843,transparent);margin:8px 0;"></div></td></tr>
        <tr>
          <td style="color:#8B0000;font-size:17px;font-weight:800;">TỔNG CỘNG</td>
          <td style="text-align:right;color:#B22222;font-size:20px;font-weight:800;">{total:,} ₫</td>
        </tr>
      </table>

      <p style="margin:0;color:#6B5C5C;font-size:13px;line-height:1.7;font-style:italic;">
        Mọi thắc mắc, vui lòng liên hệ <strong style="color:#B22222;">nepthanh6886@gmail.com</strong> hoặc gọi hotline trên website.
      </p>
    """

    html = _email_base_html(
        header_title=f"Cập nhật đơn hàng {order_num}",
        header_sub=f"Trạng thái đơn hàng của bạn đã được cập nhật bởi Nếp Thanh",
        content_html=content,
    )
    body_text = (
        f"Cập nhật đơn hàng {order_num}\nTrạng thái: {status_emoji} {status_text}\nThanh toán: {payment_label}\n"

        f"Vận chuyển: {provider} – Mã vận đơn: {tracking}\n"
        f"Ghi chú: {note_text or '-'}\n\n"
        f"Sản phẩm:\n{_order_items_lines(items)}\n"
        f"TỔNG: {total:,} VND\n"
    )
    return _send_html_email(order["email"], subject, html, body_text)


def send_new_order_alert_to_owner(order, items):
    """
    Gửi email thông báo đơn hàng MỚI tới chủ shop (SMTP_USER / SMTP_FROM).
    Được gọi khi khách đặt hàng thành công qua website hoặc chatbot bot.
    """
    config = _smtp_config()
    owner_email = config["sender"] or config["user"]
    if not owner_email:
        return False

    order_number = order.get("order_number", "?")
    recipient   = order.get("recipient_name") or "Khách"
    phone       = order.get("phone") or "-"
    customer_email = order.get("email") or "-"
    status      = order.get("status", "new")
    payment     = order.get("payment_status", "unpaid")
    subtotal    = order.get("subtotal", 0) or 0
    ship_fee    = order.get("shipping_fee", 0) or 0
    discount    = order.get("discount_amount", 0) or 0
    total       = order.get("total", 0) or 0
    notes       = order.get("notes") or "-"

    # Địa chỉ – lọc None
    addr_parts = [
        order.get("line1"), order.get("line2"), order.get("ward"),
        order.get("district"), order.get("province"),
    ]
    address = ", ".join(p for p in addr_parts if p)

    # Danh sách sản phẩm
    items_rows_html = ""
    items_rows_text = ""
    if items:
        for it in items:
            name     = it.get("product_name", "?")
            variant  = it.get("variant_label") or "-"
            qty      = it.get("qty", 1)
            unit     = it.get("unit_price", 0) or 0
            subtot_i = it.get("total_price", 0) or 0
            items_rows_html += (
                f"<tr><td style='padding:6px 8px;border-bottom:1px solid #eee;'>{name}</td>"
                f"<td style='padding:6px 8px;border-bottom:1px solid #eee;text-align:center;'>{variant}</td>"
                f"<td style='padding:6px 8px;border-bottom:1px solid #eee;text-align:center;'>{qty}</td>"
                f"<td style='padding:6px 8px;border-bottom:1px solid #eee;text-align:right;'>{unit:,} ₫</td>"
                f"<td style='padding:6px 8px;border-bottom:1px solid #eee;text-align:right;'>{subtot_i:,} ₫</td></tr>"
            )
            items_rows_text += f"  - {name} ({variant}) x{qty}: {subtot_i:,} VND\n"
    else:
        items_rows_html = "<tr><td colspan=5 style='padding:8px;color:#888;'>Chưa có chi tiết sản phẩm.</td></tr>"
        items_rows_text = "  - (Chưa có chi tiết)\n"

    subject = f"🛒 [Nếp Thanh] ĐƠN HÀNG MỚI – {order_number}"

    body_html = f"""
<!DOCTYPE html>
<html lang="vi">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" bgcolor="#f4f4f4">
    <tr><td align="center" style="padding:30px 10px;">
      <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08);">
        <!-- Header -->
        <tr><td style="background:#c0392b;padding:24px 32px;">
          <h1 style="margin:0;color:#fff;font-size:22px;">🛒 Đơn hàng mới</h1>
          <p style="margin:6px 0 0;color:#f9c9c9;font-size:14px;">Mã đơn: <strong>{order_number}</strong></p>
        </td></tr>
        <!-- Body -->
        <tr><td style="padding:28px 32px;">
          <h2 style="margin:0 0 16px;font-size:16px;color:#333;">📋 Thông tin khách hàng</h2>
          <table width="100%" cellpadding="0" cellspacing="0" style="font-size:14px;">
            <tr><td style="color:#666;padding:4px 0;width:140px;">Người nhận:</td><td style="color:#111;font-weight:bold;">{recipient}</td></tr>
            <tr><td style="color:#666;padding:4px 0;">Email:</td><td style="color:#111;">{customer_email}</td></tr>
            <tr><td style="color:#666;padding:4px 0;">Điện thoại:</td><td style="color:#111;">{phone}</td></tr>
            <tr><td style="color:#666;padding:4px 0;">Địa chỉ:</td><td style="color:#111;">{address or '-'}</td></tr>
            <tr><td style="color:#666;padding:4px 0;">Ghi chú:</td><td style="color:#111;">{notes}</td></tr>
          </table>

          <h2 style="margin:24px 0 12px;font-size:16px;color:#333;">🏷️ Sản phẩm</h2>
          <table width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;border-collapse:collapse;">
            <thead>
              <tr style="background:#f8f8f8;">
                <th style="padding:8px;text-align:left;color:#555;">Sản phẩm</th>
                <th style="padding:8px;text-align:center;color:#555;">Biến thể</th>
                <th style="padding:8px;text-align:center;color:#555;">SL</th>
                <th style="padding:8px;text-align:right;color:#555;">Đơn giá</th>
                <th style="padding:8px;text-align:right;color:#555;">Thành tiền</th>
              </tr>
            </thead>
            <tbody>{items_rows_html}</tbody>
          </table>

          <table width="100%" cellpadding="0" cellspacing="0" style="font-size:14px;margin-top:16px;">
            <tr><td style="color:#666;padding:3px 0;">Tạm tính:</td><td style="text-align:right;">{subtotal:,} ₫</td></tr>
            <tr><td style="color:#666;padding:3px 0;">Phí vận chuyển:</td><td style="text-align:right;">{ship_fee:,} ₫</td></tr>
            <tr><td style="color:#666;padding:3px 0;">Giảm giá:</td><td style="text-align:right;">-{discount:,} ₫</td></tr>
            <tr><td style="font-weight:bold;color:#c0392b;font-size:16px;padding:8px 0 3px;">TỔNG:</td><td style="text-align:right;font-weight:bold;color:#c0392b;font-size:16px;">{total:,} ₫</td></tr>
          </table>

          <table width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;margin-top:16px;background:#fef9f0;border-radius:6px;padding:12px;">
            <tr><td style="color:#666;padding:2px 0;">Trạng thái đơn:</td><td><span style="background:#e8f5e9;color:#2e7d32;border-radius:4px;padding:2px 8px;font-size:12px;">{status}</span></td></tr>
            <tr><td style="color:#666;padding:2px 0;">Thanh toán:</td><td><span style="background:#fff3e0;color:#e65100;border-radius:4px;padding:2px 8px;font-size:12px;">{payment}</span></td></tr>
          </table>
        </td></tr>
        <!-- Footer -->
        <tr><td style="background:#f8f8f8;padding:16px 32px;text-align:center;border-top:1px solid #eee;">
          <p style="margin:0;color:#999;font-size:12px;">Email tự động từ hệ thống <strong>Nếp Thanh</strong> – nepthanh6886@gmail.com</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    body_text = (
        f"ĐƠN HÀNG MỚI – {order_number}\n"
        f"{'='*40}\n"
        f"Người nhận : {recipient}\n"
        f"Email      : {customer_email}\n"
        f"Điện thoại : {phone}\n"
        f"Địa chỉ   : {address or '-'}\n"
        f"Ghi chú    : {notes}\n\n"
        f"Sản phẩm:\n{items_rows_text}\n"
        f"Tạm tính   : {subtotal:,} VND\n"
        f"Phí ship   : {ship_fee:,} VND\n"
        f"Giảm giá   : -{discount:,} VND\n"
        f"TỔNG       : {total:,} VND\n\n"
        f"Trạng thái : {status} / Thanh toán: {payment}\n"
    )

    return _send_html_email(owner_email, subject, body_html, body_text)
