import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if os.environ.get("VERCEL"):
    DB_PATH = "/tmp/nepthanh.db"
    UPLOAD_DIR = "/tmp/uploads"
else:
    DB_PATH = os.path.join(BASE_DIR, "data", "nepthanh.db")
    UPLOAD_DIR = os.path.join(BASE_DIR, "static", "uploads")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")

ORDER_STATUSES = [
    "new",
    "confirmed",
    "packed",
    "shipping",
    "completed",
    "cancelled",
    "refunded",
    "returned",
]
PROCESSING_STATUSES = ["confirmed", "packed", "shipping"]
REVENUE_STATUSES = ["confirmed", "packed", "shipping", "completed"]
LOW_STOCK_DEFAULT = 5

ROLE_PERMISSIONS = {
    "admin": {"all"},
    "staff": {
        "dashboard",
        "orders",
        "products",
        "inventory",
        "customers",
        "content",
        "marketing",
        "qr",
        "reports",
        "settings",
        "users",
    },
    "orders": {"dashboard", "orders", "customers", "reports"},
    "products": {"dashboard", "products", "inventory"},
    "content": {"dashboard", "content", "marketing", "qr"},
    "marketing": {"dashboard", "marketing", "reports"},
}
