"""
Flask application for the Nếp Thanh – Dòng chảy thanh âm Việt project.
"""

from datetime import datetime

from flask import Flask, render_template
from dotenv import load_dotenv
load_dotenv()

from modules.auth import _get_current_user, _is_admin_user, _google_enabled
from modules.cart import get_cart_item_count
from modules.config import SECRET_KEY
from modules.db import init_db
from modules.routes_admin import register_admin_routes
from modules.routes_chatbot import register_chatbot_routes
from modules.routes_public import register_public_routes



app = Flask(__name__)
app.secret_key = SECRET_KEY

# Ensure a usable schema exists for both local runs and Vercel cold starts.
init_db()


@app.context_processor
def inject_globals():
    current_user = _get_current_user()
    return {
        "site_name": "Nếp Thanh – Dòng chảy thanh âm Việt",
        "current_year": datetime.now().year,
        "current_user": current_user,
        "is_admin": _is_admin_user(current_user),
        "google_enabled": _google_enabled(),
        "cart_count": get_cart_item_count(current_user),
    }


register_public_routes(app)
register_admin_routes(app)
register_chatbot_routes(app)


@app.errorhandler(404)
def page_not_found(error):
    return (
        render_template(
            "404.html",
            title="Không tìm thấy trang",
            description="Trang bạn tìm kiếm không tồn tại. Vui lòng quay lại trang chủ.",
        ),
        404,
    )
    


if __name__ == "__main__":
    app.run(debug=True)
