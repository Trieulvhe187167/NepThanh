import hashlib
import secrets
from datetime import datetime, timedelta

from modules.db import _get_db

TOKEN_PURPOSE_EMAIL_VERIFY = "email_verify"
TOKEN_PURPOSE_PASSWORD_RESET = "password_reset"

_SECURITY_TABLES_READY = False


def ensure_security_tables():
    global _SECURITY_TABLES_READY
    if _SECURITY_TABLES_READY:
        return
    conn = _get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            purpose TEXT NOT NULL,
            token_hash TEXT UNIQUE NOT NULL,
            expires_at TEXT NOT NULL,
            used_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_account_tokens_lookup ON account_tokens(purpose, token_hash)"
    )
    conn.commit()
    conn.close()
    _SECURITY_TABLES_READY = True


def create_account_token(user_id, purpose, expires_in_minutes=60):
    ensure_security_tables()
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    conn = _get_db()
    conn.execute(
        """
        INSERT INTO account_tokens (user_id, purpose, token_hash, expires_at, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            user_id,
            purpose,
            _hash_token(token),
            (now + timedelta(minutes=expires_in_minutes)).isoformat(),
            now.isoformat(),
        ),
    )
    conn.commit()
    conn.close()
    return token


def consume_account_token(token, purpose):
    ensure_security_tables()
    token_hash = _hash_token(token)
    now = datetime.utcnow()
    conn = _get_db()
    row = conn.execute(
        """
        SELECT account_tokens.*, users.email, users.password_hash, users.is_verified
        FROM account_tokens
        JOIN users ON users.id = account_tokens.user_id
        WHERE account_tokens.purpose = ?
          AND account_tokens.token_hash = ?
          AND account_tokens.used_at IS NULL
        """,
        (purpose, token_hash),
    ).fetchone()
    if row is None:
        conn.close()
        return None
    try:
        expires_at = datetime.fromisoformat(row["expires_at"])
    except (TypeError, ValueError):
        expires_at = now - timedelta(seconds=1)
    if expires_at < now:
        conn.close()
        return None
    conn.execute(
        "UPDATE account_tokens SET used_at = ? WHERE id = ?",
        (now.isoformat(), row["id"]),
    )
    conn.commit()
    conn.close()
    return row


def mark_user_email_verified(user_id):
    conn = _get_db()
    conn.execute(
        "UPDATE users SET is_verified = 1, updated_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), user_id),
    )
    conn.commit()
    conn.close()


def update_user_password(user_id, password_hash):
    conn = _get_db()
    conn.execute(
        "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
        (password_hash, datetime.utcnow().isoformat(), user_id),
    )
    conn.execute(
        """
        UPDATE account_tokens
        SET used_at = COALESCE(used_at, ?)
        WHERE user_id = ? AND purpose = ? AND used_at IS NULL
        """,
        (datetime.utcnow().isoformat(), user_id, TOKEN_PURPOSE_PASSWORD_RESET),
    )
    conn.commit()
    conn.close()


def get_security_profile(user_id):
    ensure_security_tables()
    conn = _get_db()
    profile = conn.execute(
        "SELECT id, email, password_hash, is_verified, updated_at, created_at FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    conn.close()
    return profile


def _hash_token(token):
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()
