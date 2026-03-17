import io
import secrets
import sqlite3
import zipfile
from datetime import datetime, timedelta

from modules.db import _get_db
from modules.utils import _generate_qr_png


_UNIQUE_SCAN_EXPR = """
CASE
    WHEN qr_scans.user_id IS NOT NULL THEN 'u:' || qr_scans.user_id
    WHEN qr_scans.ip_hash IS NOT NULL THEN 'ip:' || qr_scans.ip_hash
    ELSE 'scan:' || qr_scans.id
END
"""


def _rows_to_dicts(rows):
    return [dict(row) for row in rows]


def _sanitize_filename(value, fallback):
    raw = (value or fallback or "qr").strip()
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in raw)
    cleaned = cleaned.strip("_")
    return cleaned or fallback or "qr"


def _build_scan_filters(batch_code=None, character_id=None, date_from=None, date_to=None):
    clauses = []
    params = []
    if batch_code:
        clauses.append("qr_tags.batch_code = ?")
        params.append(batch_code)
    if character_id:
        clauses.append("qr_tags.character_id = ?")
        params.append(character_id)
    if date_from:
        clauses.append("date(qr_scans.scanned_at) >= date(?)")
        params.append(date_from)
    if date_to:
        clauses.append("date(qr_scans.scanned_at) <= date(?)")
        params.append(date_to)
    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(clauses), params


def create_qr_batch(variant_id, character_id, batch_code, quantity):
    if not variant_id or not character_id:
        raise ValueError("variant_id and character_id are required.")
    if not batch_code:
        raise ValueError("batch_code is required.")
    if quantity <= 0:
        raise ValueError("quantity must be greater than 0.")

    conn = _get_db()
    variant_row = conn.execute(
        "SELECT id FROM product_variants WHERE id = ?",
        (variant_id,),
    ).fetchone()
    character_row = conn.execute(
        "SELECT id FROM characters WHERE id = ?",
        (character_id,),
    ).fetchone()
    if variant_row is None or character_row is None:
        conn.close()
        raise ValueError("variant_id or character_id does not exist.")

    serial_start = conn.execute(
        "SELECT COUNT(*) AS count FROM qr_tags WHERE batch_code = ?",
        (batch_code,),
    ).fetchone()["count"] + 1

    now = datetime.utcnow().isoformat()
    created = []
    try:
        for offset in range(quantity):
            serial_no = f"{batch_code}-{serial_start + offset:04d}"
            token = None
            for _ in range(8):
                candidate = secrets.token_urlsafe(10)
                try:
                    conn.execute(
                        """
                        INSERT INTO qr_tags (token, variant_id, character_id, batch_code, serial_no, status, created_at)
                        VALUES (?, ?, ?, ?, ?, 'active', ?)
                        """,
                        (
                            candidate,
                            variant_id,
                            character_id,
                            batch_code,
                            serial_no,
                            now,
                        ),
                    )
                    token = candidate
                    break
                except sqlite3.IntegrityError:
                    continue
            if not token:
                raise RuntimeError("Unable to generate unique QR token.")
            created.append({"token": token, "serial_no": serial_no})
        conn.commit()
    finally:
        conn.close()
    return created


def disable_qr_token(token):
    if not token:
        return False
    conn = _get_db()
    cursor = conn.execute(
        "UPDATE qr_tags SET status = 'disabled' WHERE token = ?",
        (token,),
    )
    conn.commit()
    conn.close()
    return cursor.rowcount > 0


def get_batch_tokens(batch_code, limit=500):
    if not batch_code:
        return []
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT qr_tags.*, product_variants.sku, product_variants.size, product_variants.color,
               products.name AS product_name, characters.name AS character_name, characters.slug AS character_slug,
               COUNT(qr_scans.id) AS scan_count, MAX(qr_scans.scanned_at) AS last_scanned_at
        FROM qr_tags
        JOIN product_variants ON product_variants.id = qr_tags.variant_id
        JOIN products ON products.id = product_variants.product_id
        JOIN characters ON characters.id = qr_tags.character_id
        LEFT JOIN qr_scans ON qr_scans.qr_tag_id = qr_tags.id
        WHERE qr_tags.batch_code = ?
        GROUP BY qr_tags.id
        ORDER BY qr_tags.serial_no, qr_tags.created_at
        LIMIT ?
        """,
        (batch_code, limit),
    ).fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def list_qr_batches(limit=50):
    conn = _get_db()
    rows = conn.execute(
        f"""
        SELECT COALESCE(qr_tags.batch_code, '') AS batch_code,
               COUNT(*) AS token_count,
               SUM(CASE WHEN qr_tags.status = 'active' THEN 1 ELSE 0 END) AS active_count,
               SUM(CASE WHEN qr_tags.status = 'disabled' THEN 1 ELSE 0 END) AS disabled_count,
               SUM(CASE WHEN qr_tags.status = 'expired' THEN 1 ELSE 0 END) AS expired_count,
               MAX(qr_tags.created_at) AS created_at,
               COUNT(qr_scans.id) AS scan_count,
               COUNT(DISTINCT {_UNIQUE_SCAN_EXPR}) AS unique_scans
        FROM qr_tags
        LEFT JOIN qr_scans ON qr_scans.qr_tag_id = qr_tags.id
        GROUP BY qr_tags.batch_code
        ORDER BY MAX(qr_tags.created_at) DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def get_qr_stats(batch_code=None, character_id=None, date_from=None, date_to=None):
    where_sql, where_params = _build_scan_filters(
        batch_code=batch_code,
        character_id=character_id,
        date_from=date_from,
        date_to=date_to,
    )
    conn = _get_db()

    totals = conn.execute(
        f"""
        SELECT COUNT(*) AS total_scans,
               COUNT(DISTINCT {_UNIQUE_SCAN_EXPR}) AS unique_scans
        FROM qr_scans
        JOIN qr_tags ON qr_tags.id = qr_scans.qr_tag_id
        {where_sql}
        """,
        where_params,
    ).fetchone()

    today = datetime.utcnow().date()
    day_7 = (today - timedelta(days=6)).isoformat()
    day_30 = (today - timedelta(days=29)).isoformat()
    window_params = [today.isoformat(), day_7, day_30, *where_params]
    windows = conn.execute(
        f"""
        SELECT
            COALESCE(SUM(CASE WHEN date(qr_scans.scanned_at) = ? THEN 1 ELSE 0 END), 0) AS scans_today,
            COALESCE(SUM(CASE WHEN date(qr_scans.scanned_at) >= ? THEN 1 ELSE 0 END), 0) AS scans_7d,
            COALESCE(SUM(CASE WHEN date(qr_scans.scanned_at) >= ? THEN 1 ELSE 0 END), 0) AS scans_30d
        FROM qr_scans
        JOIN qr_tags ON qr_tags.id = qr_scans.qr_tag_id
        {where_sql}
        """,
        window_params,
    ).fetchone()

    scans_by_day = conn.execute(
        f"""
        SELECT date(qr_scans.scanned_at) AS day, COUNT(*) AS scans
        FROM qr_scans
        JOIN qr_tags ON qr_tags.id = qr_scans.qr_tag_id
        {where_sql}
        GROUP BY date(qr_scans.scanned_at)
        ORDER BY day DESC
        LIMIT 30
        """,
        where_params,
    ).fetchall()

    scans_by_character = conn.execute(
        f"""
        SELECT characters.id AS character_id, characters.slug, characters.name AS character_name, COUNT(*) AS scans
        FROM qr_scans
        JOIN qr_tags ON qr_tags.id = qr_scans.qr_tag_id
        JOIN characters ON characters.id = qr_tags.character_id
        {where_sql}
        GROUP BY characters.id
        ORDER BY scans DESC
        LIMIT 6
        """,
        where_params,
    ).fetchall()

    scans_by_batch = conn.execute(
        f"""
        SELECT COALESCE(qr_tags.batch_code, '') AS batch_code, COUNT(*) AS scans
        FROM qr_scans
        JOIN qr_tags ON qr_tags.id = qr_scans.qr_tag_id
        {where_sql}
        GROUP BY qr_tags.batch_code
        ORDER BY scans DESC
        LIMIT 20
        """,
        where_params,
    ).fetchall()

    top_tokens = conn.execute(
        f"""
        SELECT qr_tags.token, qr_tags.serial_no, qr_tags.batch_code, qr_tags.status,
               characters.name AS character_name,
               COUNT(qr_scans.id) AS scans,
               COUNT(DISTINCT {_UNIQUE_SCAN_EXPR}) AS unique_scans
        FROM qr_scans
        JOIN qr_tags ON qr_tags.id = qr_scans.qr_tag_id
        JOIN characters ON characters.id = qr_tags.character_id
        {where_sql}
        GROUP BY qr_tags.id
        ORDER BY scans DESC, unique_scans DESC
        LIMIT 20
        """,
        where_params,
    ).fetchall()
    conn.close()

    return {
        "total_scans": totals["total_scans"] if totals else 0,
        "unique_scans": totals["unique_scans"] if totals else 0,
        "scans_today": windows["scans_today"] if windows else 0,
        "scans_7d": windows["scans_7d"] if windows else 0,
        "scans_30d": windows["scans_30d"] if windows else 0,
        "scans_by_day": _rows_to_dicts(scans_by_day),
        "scans_by_character": _rows_to_dicts(scans_by_character),
        "scans_by_batch": _rows_to_dicts(scans_by_batch),
        "top_tokens": _rows_to_dicts(top_tokens),
    }


def export_qr_png_zip(batch_code, base_url):
    if not batch_code:
        raise ValueError("batch_code is required.")

    conn = _get_db()
    tags = conn.execute(
        """
        SELECT token, serial_no
        FROM qr_tags
        WHERE batch_code = ?
        ORDER BY serial_no, created_at
        """,
        (batch_code,),
    ).fetchall()
    conn.close()
    if not tags:
        raise ValueError("Batch not found or has no QR tags.")

    base = base_url.rstrip("/")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for tag in tags:
            qr_url = f"{base}/q/{tag['token']}"
            png_bytes = _generate_qr_png(qr_url)
            if png_bytes is None:
                raise RuntimeError("Python package 'qrcode' is required for PNG export.")
            file_stem = _sanitize_filename(tag["serial_no"], tag["token"])
            archive.writestr(f"{file_stem}.png", png_bytes)
    buffer.seek(0)
    return buffer, len(tags)


def export_qr_pdf_sheet(batch_code, base_url=None):
    # Advanced export can be added later.
    return None
