import json
from datetime import datetime

from modules.cart import clear_cart, get_cart_snapshot, set_shipping_zone
from modules.customer_account import ensure_account_tables, get_address_by_id, get_default_address
from modules.db import _get_db
from modules.notifications import send_order_confirmation_email, send_order_status_email
from modules.payments_vnpay import (
    build_vnpay_payment_url,
    is_vnpay_success,
    parse_vnpay_amount,
    verify_vnpay_response,
    vnpay_enabled,
)
from modules.utils import _build_order_number, _parse_int

_CHECKOUT_TABLES_READY = False
_ORDER_CANCEL_STATUSES = {"cancelled", "refunded", "returned"}


def ensure_checkout_tables():
    global _CHECKOUT_TABLES_READY
    if _CHECKOUT_TABLES_READY:
        return
    ensure_account_tables()
    conn = _get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            gateway TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT,
            txn_ref TEXT,
            transaction_no TEXT,
            amount INTEGER DEFAULT 0,
            payload_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_payment_transactions_order_id ON payment_transactions(order_id)"
    )
    conn.commit()
    conn.close()
    _CHECKOUT_TABLES_READY = True


def get_checkout_prefill(user):
    ensure_account_tables()
    if not user:
        return {
            "full_name": "",
            "email": "",
            "phone": "",
            "line1": "",
            "line2": "",
            "ward": "",
            "district": "",
            "province": "",
            "country": "VN",
            "address_id": "",
        }
    conn = _get_db()
    row = conn.execute(
        "SELECT full_name, email, phone FROM users WHERE id = ?",
        (user["id"],),
    ).fetchone()
    conn.close()
    if row is None:
        base = {"full_name": "", "email": "", "phone": ""}
    else:
        base = {
            "full_name": row["full_name"] or "",
            "email": row["email"] or "",
            "phone": row["phone"] or "",
        }
    address = get_default_address(user["id"])
    if address is None:
        base.update(
            {
                "line1": "",
                "line2": "",
                "ward": "",
                "district": "",
                "province": "",
                "country": "VN",
                "address_id": "",
            }
        )
        return base
    base.update(
        {
            "line1": address["line1"] or "",
            "line2": address["line2"] or "",
            "ward": address["ward"] or "",
            "district": address["district"] or "",
            "province": address["province"] or "",
            "country": address["country"] or "VN",
            "address_id": str(address["id"]),
        }
    )
    return base


def _address_or_form_value(address, key, form_data):
    submitted = (form_data.get(key) or "").strip()
    if submitted:
        return submitted
    if address is None:
        return submitted
    value = address[key]
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return value


def place_order_from_cart(user, form_data, remote_addr, vnpay_return_url):
    ensure_checkout_tables()
    cart = get_cart_snapshot(user)
    if not cart["items"]:
        return {"ok": False, "error": "Gio hang trong, vui long them san pham truoc khi thanh toan."}

    user_id = user["id"] if user else None
    selected_address = None
    address_id = (form_data.get("address_id") or "").strip()
    if user_id and address_id.isdigit():
        selected_address = get_address_by_id(user_id, int(address_id))

    recipient_name = _address_or_form_value(selected_address, "recipient_name", form_data)
    email = (form_data.get("email") or "").strip().lower()
    phone = _address_or_form_value(selected_address, "phone", form_data)
    line1 = _address_or_form_value(selected_address, "line1", form_data)
    line2 = _address_or_form_value(selected_address, "line2", form_data)
    ward = _address_or_form_value(selected_address, "ward", form_data)
    district = _address_or_form_value(selected_address, "district", form_data)
    province = _address_or_form_value(selected_address, "province", form_data)
    notes = (form_data.get("notes") or "").strip()
    payment_method = (form_data.get("payment_method") or "cod").strip().lower()
    shipping_zone = (form_data.get("shipping_zone") or cart.get("shipping_zone") or "").strip()

    if payment_method not in {"cod", "vnpay"}:
        return {"ok": False, "error": "Phuong thuc thanh toan khong hop le."}
    if payment_method == "vnpay" and not vnpay_enabled():
        return {"ok": False, "error": "VNPay chua duoc cau hinh."}
    if not recipient_name:
        return {"ok": False, "error": "Vui long nhap ten nguoi nhan."}
    if not email or "@" not in email:
        return {"ok": False, "error": "Vui long nhap email hop le de nhan thong bao don hang."}
    if not phone:
        return {"ok": False, "error": "Vui long nhap so dien thoai nguoi nhan."}
    if not line1 or not district or not province:
        return {"ok": False, "error": "Vui long nhap day du dia chi giao hang."}
    if not shipping_zone:
        return {"ok": False, "error": "Vui long chon phuong thuc van chuyen."}
    ok_shipping, shipping_error = set_shipping_zone(user, shipping_zone)
    if not ok_shipping:
        return {"ok": False, "error": shipping_error}

    cart = get_cart_snapshot(user)
    coupon_code = cart.get("coupon_code")

    conn = _get_db()
    try:
        item_map = _variant_catalog(conn, [item["variant_id"] for item in cart["items"]])
        for item in cart["items"]:
            variant = item_map.get(item["variant_id"])
            if variant is None:
                return {"ok": False, "error": "Mot so san pham da khong con ton tai."}
            stock_qty = _parse_int(variant["stock_qty"], 0)
            if stock_qty < item["qty"]:
                return {
                    "ok": False,
                    "error": f"Size {variant['size']} cua {item['product_name']} chi con {stock_qty}.",
                }

        order_number = _build_order_number()
        now = datetime.utcnow().isoformat()
        full_notes = notes
        if coupon_code:
            coupon_text = f"Coupon: {coupon_code}"
            full_notes = f"{full_notes}\n{coupon_text}".strip() if full_notes else coupon_text

        cur = conn.execute(
            """
            INSERT INTO orders (
                order_number, user_id, status, subtotal, shipping_fee, discount_amount, total,
                payment_status, recipient_name, email, phone, line1, line2, ward, district, province,
                notes, created_at, updated_at
            )
            VALUES (?, ?, 'new', ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_number,
                user_id,
                cart["subtotal"],
                cart["shipping_fee"],
                cart["discount_amount"],
                cart["total"],
                recipient_name,
                email,
                phone,
                line1,
                line2,
                ward,
                district,
                province,
                full_notes,
                now,
                now,
            ),
        )
        order_id = cur.lastrowid

        for item in cart["items"]:
            variant = item_map[item["variant_id"]]
            variant_label = f"{variant['size']} / {variant['color']}"
            conn.execute(
                """
                INSERT INTO order_items (order_id, product_id, variant_id, product_name, variant_label, sku, qty, unit_price, total_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    item["product_id"],
                    item["variant_id"],
                    item["product_name"],
                    variant_label,
                    variant["sku"],
                    item["qty"],
                    item["unit_price"],
                    item["line_total"],
                ),
            )
            conn.execute(
                "UPDATE product_variants SET stock_qty = stock_qty - ? WHERE id = ?",
                (item["qty"], item["variant_id"]),
            )
            conn.execute(
                """
                INSERT INTO inventory_movements (variant_id, change_qty, reason, note, admin_id, created_at)
                VALUES (?, ?, 'order', ?, NULL, ?)
                """,
                (
                    item["variant_id"],
                    -item["qty"],
                    f"Checkout {order_number}",
                    now,
                ),
            )

        conn.execute(
            """
            INSERT INTO order_status_events (order_id, status, note, admin_id, created_at)
            VALUES (?, 'new', 'Khach dat hang qua checkout', NULL, ?)
            """,
            (order_id, now),
        )

        if coupon_code:
            conn.execute(
                "UPDATE coupons SET used_count = used_count + 1 WHERE UPPER(code) = ?",
                (coupon_code.upper(),),
            )

        if user_id:
            conn.execute(
                """
                UPDATE users
                SET full_name = ?, phone = ?, updated_at = ?
                WHERE id = ?
                """,
                (recipient_name, phone, now, user_id),
            )

        payment_url = None
        if payment_method == "vnpay" and cart["total"] > 0:
            payment_url = build_vnpay_payment_url(
                order_number=order_number,
                amount_vnd=cart["total"],
                return_url=vnpay_return_url,
                ip_addr=remote_addr,
            )
            _log_payment(
                conn,
                order_id=order_id,
                status="pending",
                source="checkout",
                txn_ref=order_number,
                amount=cart["total"],
                payload={"phase": "redirect_created"},
            )
        elif cart["total"] <= 0:
            conn.execute(
                """
                UPDATE orders
                SET payment_status = 'paid', status = 'confirmed', updated_at = ?
                WHERE id = ?
                """,
                (now, order_id),
            )
            conn.execute(
                """
                INSERT INTO order_status_events (order_id, status, note, admin_id, created_at)
                VALUES (?, 'confirmed', 'Don hang 0 dong, xac nhan thanh toan tu dong', NULL, ?)
                """,
                (order_id, now),
            )

        conn.commit()
        order_row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        item_rows = conn.execute(
            "SELECT * FROM order_items WHERE order_id = ? ORDER BY id",
            (order_id,),
        ).fetchall()
    finally:
        conn.close()

    clear_cart(user)
    send_order_confirmation_email(_row_to_dict(order_row), [_row_to_dict(row) for row in item_rows])

    return {
        "ok": True,
        "order_number": order_row["order_number"],
        "payment_method": payment_method,
        "payment_url": payment_url,
    }


def get_order_details(order_number):
    conn = _get_db()
    order = conn.execute(
        "SELECT * FROM orders WHERE order_number = ?",
        (order_number,),
    ).fetchone()
    if order is None:
        conn.close()
        return None, []
    items = conn.execute(
        "SELECT * FROM order_items WHERE order_id = ? ORDER BY id",
        (order["id"],),
    ).fetchall()
    conn.close()
    return order, items


def send_order_status_update(order_id, status_note=None):
    conn = _get_db()
    order = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    if order is None:
        conn.close()
        return False
    items = conn.execute(
        "SELECT * FROM order_items WHERE order_id = ? ORDER BY id",
        (order_id,),
    ).fetchall()
    conn.close()
    return send_order_status_email(
        _row_to_dict(order),
        [_row_to_dict(row) for row in items],
        status_note=status_note,
    )


def handle_vnpay_callback(params, source="return"):
    ensure_checkout_tables()
    if not verify_vnpay_response(params):
        return {
            "ok": False,
            "success": False,
            "code": "97",
            "message": "Invalid signature",
            "order_number": (params.get("vnp_TxnRef") or "").strip(),
        }

    order_number = (params.get("vnp_TxnRef") or "").strip()
    if not order_number:
        return {"ok": False, "success": False, "code": "01", "message": "Order not found", "order_number": ""}

    conn = _get_db()
    order = conn.execute(
        "SELECT * FROM orders WHERE order_number = ?",
        (order_number,),
    ).fetchone()
    if order is None:
        conn.close()
        return {"ok": False, "success": False, "code": "01", "message": "Order not found", "order_number": order_number}

    amount = parse_vnpay_amount(params)
    txn_no = (params.get("vnp_TransactionNo") or "").strip()
    now = datetime.utcnow().isoformat()
    success = is_vnpay_success(params)
    status_note = None
    status_changed = False

    try:
        _log_payment(
            conn,
            order_id=order["id"],
            status="success" if success else "failed",
            source=source,
            txn_ref=order_number,
            transaction_no=txn_no,
            amount=amount,
            payload=params,
        )

        if success:
            if order["payment_status"] != "paid":
                new_status = "confirmed" if order["status"] == "new" else order["status"]
                conn.execute(
                    """
                    UPDATE orders
                    SET payment_status = 'paid', status = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (new_status, now, order["id"]),
                )
                conn.execute(
                    """
                    INSERT INTO order_status_events (order_id, status, note, admin_id, created_at)
                    VALUES (?, ?, ?, NULL, ?)
                    """,
                    (order["id"], new_status, "Thanh toan VNPay thanh cong", now),
                )
                status_note = "Thanh toan VNPay thanh cong"
                status_changed = True
        else:
            if order["payment_status"] != "paid" and order["status"] not in _ORDER_CANCEL_STATUSES:
                _restore_stock_for_order(conn, order["id"], order["order_number"], now)
                conn.execute(
                    """
                    UPDATE orders
                    SET payment_status = 'failed', status = 'cancelled', canceled_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, order["id"]),
                )
                conn.execute(
                    """
                    INSERT INTO order_status_events (order_id, status, note, admin_id, created_at)
                    VALUES (?, 'cancelled', 'Thanh toan VNPay that bai', NULL, ?)
                    """,
                    (order["id"], now),
                )
                status_note = "Thanh toan VNPay that bai"
                status_changed = True
            elif order["payment_status"] not in {"paid", "failed"}:
                conn.execute(
                    "UPDATE orders SET payment_status = 'failed', updated_at = ? WHERE id = ?",
                    (now, order["id"]),
                )
                status_note = "Thanh toan VNPay that bai"
                status_changed = True

        conn.commit()
        order = conn.execute("SELECT * FROM orders WHERE id = ?", (order["id"],)).fetchone()
        item_rows = conn.execute(
            "SELECT * FROM order_items WHERE order_id = ? ORDER BY id",
            (order["id"],),
        ).fetchall()
    finally:
        conn.close()

    if status_changed:
        send_order_status_email(
            _row_to_dict(order),
            [_row_to_dict(row) for row in item_rows],
            status_note=status_note,
        )
    final_success = order["payment_status"] == "paid"

    return {
        "ok": True,
        "success": final_success,
        "code": "00",
        "message": "Confirm Success",
        "order_number": order_number,
    }


def _variant_catalog(conn, variant_ids):
    if not variant_ids:
        return {}
    placeholders = ",".join("?" for _ in variant_ids)
    rows = conn.execute(
        f"""
        SELECT
            v.id,
            v.product_id,
            v.sku,
            v.size,
            v.color,
            v.stock_qty,
            v.is_active,
            p.status AS product_status
        FROM product_variants v
        JOIN products p ON p.id = v.product_id
        WHERE v.id IN ({placeholders})
        """,
        tuple(variant_ids),
    ).fetchall()
    data = {}
    for row in rows:
        if row["product_status"] != "active" or not row["is_active"]:
            continue
        data[row["id"]] = row
    return data


def _restore_stock_for_order(conn, order_id, order_number, now):
    items = conn.execute(
        """
        SELECT variant_id, qty
        FROM order_items
        WHERE order_id = ? AND variant_id IS NOT NULL
        """,
        (order_id,),
    ).fetchall()
    for item in items:
        conn.execute(
            "UPDATE product_variants SET stock_qty = stock_qty + ? WHERE id = ?",
            (item["qty"], item["variant_id"]),
        )
        conn.execute(
            """
            INSERT INTO inventory_movements (variant_id, change_qty, reason, note, admin_id, created_at)
            VALUES (?, ?, 'payment_failed', ?, NULL, ?)
            """,
            (
                item["variant_id"],
                item["qty"],
                f"Rollback {order_number}",
                now,
            ),
        )


def _log_payment(
    conn,
    order_id,
    status,
    source,
    txn_ref=None,
    transaction_no=None,
    amount=0,
    payload=None,
):
    conn.execute(
        """
        INSERT INTO payment_transactions (
            order_id, gateway, status, source, txn_ref, transaction_no, amount, payload_json, created_at
        )
        VALUES (?, 'vnpay', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            order_id,
            status,
            source,
            txn_ref,
            transaction_no,
            amount,
            json.dumps(payload, ensure_ascii=False) if payload else None,
            datetime.utcnow().isoformat(),
        ),
    )


def _row_to_dict(row):
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}
