import json
import os
import urllib.parse
import urllib.request
from datetime import datetime
from functools import wraps
from flask import redirect, request, session, url_for

from modules.config import GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, ROLE_PERMISSIONS
from modules.db import _get_db


def _admin_email_allowlist():
    raw = os.environ.get("ADMIN_EMAILS") or os.environ.get("ADMIN_EMAIL") or ""
    emails = {item.strip().lower() for item in raw.split(",") if item.strip()}
    return emails


def _get_user_by_id(user_id):
    conn = _get_db()
    row = conn.execute(
        "SELECT id, email, full_name, phone, password_hash, role, is_blocked, customer_group FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    conn.close()
    return row


def _get_user_by_email(email):
    conn = _get_db()
    row = conn.execute(
        "SELECT id, email, full_name, phone, password_hash, role, is_blocked, customer_group FROM users WHERE email = ?",
        (email,),
    ).fetchone()
    conn.close()
    return row


def _create_user(email, password_hash, full_name=None, is_verified=0, role="customer"):
    conn = _get_db()
    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        """
        INSERT INTO users (email, password_hash, full_name, is_verified, role, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (email, password_hash, full_name, is_verified, role, now, now),
    )
    conn.commit()
    user_id = cur.lastrowid
    conn.close()
    return user_id


def _get_current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    row = _get_user_by_id(user_id)
    if row is None:
        session.pop("user_id", None)
        return None
    return {
        "id": row["id"],
        "email": row["email"],
        "full_name": row["full_name"],
        "phone": row["phone"],
        "role": row["role"],
        "is_blocked": row["is_blocked"],
        "customer_group": row["customer_group"],
    }


def _google_enabled():
    return bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)


def _google_oauth_url(state, redirect_uri):
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "response_type": "code",
        "scope": "openid email profile",
        "redirect_uri": redirect_uri,
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }
    return f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"


def _exchange_google_code(code, redirect_uri):
    data = urllib.parse.urlencode(
        {
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _fetch_google_userinfo(access_token):
    req = urllib.request.Request(
        "https://openidconnect.googleapis.com/v1/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _is_admin_user(user):
    if not user:
        return False
    if user.get("email") in _admin_email_allowlist():
        return True
    role = (user.get("role") or "customer").lower()
    return role in ROLE_PERMISSIONS


def _has_permission(user, permission):
    if not user or not permission:
        return False
    if user.get("email") in _admin_email_allowlist():
        return True
    role = (user.get("role") or "customer").lower()
    perms = ROLE_PERMISSIONS.get(role, set())
    return "all" in perms or permission in perms


def admin_required(permission=None):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            user = _get_current_user()
            if not user or user.get("is_blocked"):
                return redirect(url_for("admin_login", next=request.full_path))
            if not _is_admin_user(user) or (permission and not _has_permission(user, permission)):
                return redirect(url_for("admin_login", next=request.full_path))
            return view(*args, **kwargs)

        return wrapped

    return decorator
