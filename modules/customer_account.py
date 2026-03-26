from datetime import datetime

from modules.db import _get_db

_ACCOUNT_TABLES_READY = False


def ensure_account_tables():
    global _ACCOUNT_TABLES_READY
    if _ACCOUNT_TABLES_READY:
        return
    conn = _get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            recipient_name TEXT,
            phone TEXT,
            line1 TEXT NOT NULL,
            line2 TEXT,
            ward TEXT,
            district TEXT NOT NULL,
            province TEXT NOT NULL,
            country TEXT DEFAULT 'VN',
            is_default INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    _ensure_column(conn, "users", "phone", "TEXT")
    _ensure_column(conn, "orders", "shipping_provider", "TEXT")
    _ensure_column(conn, "orders", "tracking_code", "TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_addresses_user_id ON addresses(user_id)"
    )
    conn.commit()
    conn.close()
    _ACCOUNT_TABLES_READY = True


def get_user_profile(user_id):
    ensure_account_tables()
    conn = _get_db()
    row = conn.execute(
        "SELECT id, full_name, email, phone FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    conn.close()
    return row


def update_user_profile(user_id, full_name, phone):
    ensure_account_tables()
    conn = _get_db()
    conn.execute(
        "UPDATE users SET full_name = ?, phone = ?, updated_at = ? WHERE id = ?",
        (
            (full_name or "").strip() or None,
            (phone or "").strip() or None,
            datetime.utcnow().isoformat(),
            user_id,
        ),
    )
    conn.commit()
    conn.close()


def list_user_addresses(user_id):
    ensure_account_tables()
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT *
        FROM addresses
        WHERE user_id = ?
        ORDER BY is_default DESC, id DESC
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def get_default_address(user_id):
    ensure_account_tables()
    conn = _get_db()
    row = conn.execute(
        """
        SELECT *
        FROM addresses
        WHERE user_id = ?
        ORDER BY is_default DESC, id DESC
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    conn.close()
    return row


def get_address_by_id(user_id, address_id):
    ensure_account_tables()
    conn = _get_db()
    row = conn.execute(
        "SELECT * FROM addresses WHERE id = ? AND user_id = ?",
        (address_id, user_id),
    ).fetchone()
    conn.close()
    return row


def add_user_address(user_id, data):
    ensure_account_tables()
    line1 = (data.get("line1") or "").strip()
    district = (data.get("district") or "").strip()
    province = (data.get("province") or "").strip()
    if not line1 or not district or not province:
        return False, "Vui long nhap day du dia chi, quan/huyen va tinh/thanh."

    conn = _get_db()
    has_address = conn.execute(
        "SELECT 1 FROM addresses WHERE user_id = ? LIMIT 1",
        (user_id,),
    ).fetchone()
    is_default = 1 if (data.get("is_default") or not has_address) else 0
    if is_default:
        conn.execute("UPDATE addresses SET is_default = 0 WHERE user_id = ?", (user_id,))
    conn.execute(
        """
        INSERT INTO addresses (
            user_id, recipient_name, phone, line1, line2, ward, district, province, country, is_default, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            (data.get("recipient_name") or "").strip() or None,
            (data.get("phone") or "").strip() or None,
            line1,
            (data.get("line2") or "").strip() or None,
            (data.get("ward") or "").strip() or None,
            district,
            province,
            (data.get("country") or "VN").strip() or "VN",
            is_default,
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()
    return True, "Da them dia chi giao hang."


def update_user_address(user_id, address_id, data):
    ensure_account_tables()
    line1 = (data.get("line1") or "").strip()
    district = (data.get("district") or "").strip()
    province = (data.get("province") or "").strip()
    if not line1 or not district or not province:
        return False, "Vui long nhap day du dia chi, quan/huyen va tinh/thanh."

    conn = _get_db()
    row = conn.execute(
        "SELECT id FROM addresses WHERE id = ? AND user_id = ?",
        (address_id, user_id),
    ).fetchone()
    if row is None:
        conn.close()
        return False, "Dia chi khong ton tai."

    is_default = 1 if data.get("is_default") else 0
    if is_default:
        conn.execute("UPDATE addresses SET is_default = 0 WHERE user_id = ?", (user_id,))
    conn.execute(
        """
        UPDATE addresses
        SET recipient_name = ?, phone = ?, line1 = ?, line2 = ?, ward = ?, district = ?, province = ?, country = ?, is_default = ?
        WHERE id = ? AND user_id = ?
        """,
        (
            (data.get("recipient_name") or "").strip() or None,
            (data.get("phone") or "").strip() or None,
            line1,
            (data.get("line2") or "").strip() or None,
            (data.get("ward") or "").strip() or None,
            district,
            province,
            (data.get("country") or "VN").strip() or "VN",
            is_default,
            address_id,
            user_id,
        ),
    )
    conn.commit()
    conn.close()
    return True, "Da cap nhat dia chi."


def delete_user_address(user_id, address_id):
    ensure_account_tables()
    conn = _get_db()
    row = conn.execute(
        "SELECT is_default FROM addresses WHERE id = ? AND user_id = ?",
        (address_id, user_id),
    ).fetchone()
    if row is None:
        conn.close()
        return False, "Dia chi khong ton tai."
    conn.execute("DELETE FROM addresses WHERE id = ? AND user_id = ?", (address_id, user_id))
    if row["is_default"]:
        first = conn.execute(
            "SELECT id FROM addresses WHERE user_id = ? ORDER BY id LIMIT 1",
            (user_id,),
        ).fetchone()
        if first:
            conn.execute(
                "UPDATE addresses SET is_default = 1 WHERE id = ?",
                (first["id"],),
            )
    conn.commit()
    conn.close()
    return True, "Da xoa dia chi."


def set_default_user_address(user_id, address_id):
    ensure_account_tables()
    conn = _get_db()
    row = conn.execute(
        "SELECT id FROM addresses WHERE id = ? AND user_id = ?",
        (address_id, user_id),
    ).fetchone()
    if row is None:
        conn.close()
        return False, "Dia chi khong ton tai."
    conn.execute("UPDATE addresses SET is_default = 0 WHERE user_id = ?", (user_id,))
    conn.execute(
        "UPDATE addresses SET is_default = 1 WHERE id = ? AND user_id = ?",
        (address_id, user_id),
    )
    conn.commit()
    conn.close()
    return True, "Da dat dia chi mac dinh."


def list_user_orders(user_id):
    ensure_account_tables()
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT id, order_number, status, payment_status, total, created_at, shipping_provider, tracking_code
        FROM orders
        WHERE user_id = ?
        ORDER BY created_at DESC
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def get_user_order_detail(user_id, order_number):
    ensure_account_tables()
    conn = _get_db()
    order = conn.execute(
        """
        SELECT *
        FROM orders
        WHERE user_id = ? AND order_number = ?
        """,
        (user_id, order_number),
    ).fetchone()
    if order is None:
        conn.close()
        return None, [], []
    items = conn.execute(
        "SELECT * FROM order_items WHERE order_id = ? ORDER BY id",
        (order["id"],),
    ).fetchall()
    events = conn.execute(
        """
        SELECT order_status_events.*, users.full_name AS admin_name, users.email AS admin_email
        FROM order_status_events
        LEFT JOIN users ON users.id = order_status_events.admin_id
        WHERE order_id = ?
        ORDER BY created_at DESC
        """,
        (order["id"],),
    ).fetchall()
    conn.close()
    return order, items, events


def get_order_by_number_and_email(order_number, email):
    ensure_account_tables()
    conn = _get_db()
    order = conn.execute(
        """
        SELECT *
        FROM orders
        WHERE order_number = ? AND LOWER(email) = ?
        """,
        (order_number, (email or "").strip().lower()),
    ).fetchone()
    if order is None:
        conn.close()
        return None, [], []
    items = conn.execute(
        "SELECT * FROM order_items WHERE order_id = ? ORDER BY id",
        (order["id"],),
    ).fetchall()
    events = conn.execute(
        """
        SELECT order_status_events.*, users.full_name AS admin_name, users.email AS admin_email
        FROM order_status_events
        LEFT JOIN users ON users.id = order_status_events.admin_id
        WHERE order_id = ?
        ORDER BY created_at DESC
        """,
        (order["id"],),
    ).fetchall()
    conn.close()
    return order, items, events


def _ensure_column(conn, table, column, column_sql):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if not rows:
        return
    if any(row["name"] == column for row in rows):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_sql}")
