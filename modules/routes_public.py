import secrets
import urllib.error
from datetime import datetime, timedelta

from flask import Response, abort, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from modules.auth import (
    _create_user,
    _exchange_google_code,
    _fetch_google_userinfo,
    _get_current_user,
    _get_user_by_email,
    _google_enabled,
    _google_oauth_url,
)
from modules.cart import (
    add_item_to_cart,
    apply_coupon_to_cart,
    clear_cart_coupon,
    get_cart_snapshot,
    merge_guest_cart_into_user,
    remove_cart_item,
    set_shipping_zone,
    update_cart_item,
)
from modules.checkout import (
    get_checkout_prefill,
    get_order_details,
    handle_vnpay_callback,
    place_order_from_cart,
)
from modules.customer_account import (
    add_user_address,
    delete_user_address,
    get_order_by_number_and_email,
    get_user_order_detail,
    get_user_profile,
    list_user_addresses,
    list_user_orders,
    set_default_user_address,
    update_user_address,
    update_user_profile,
)
from modules.data_access import load_characters, load_products
from modules.db import _get_db
from modules.utils import (
    _background_from_referrer,
    _hash_ip,
    _parse_int,
    _safe_background_url,
    _safe_next_url,
)


def register_public_routes(app):
    @app.route("/")
    def home():
        products = load_products()
        characters = load_characters()
        featured_products = [product for product in products if product.get("is_featured")]
        if not featured_products:
            featured_products = products[:4]
        featured_characters = characters[:3]
        return render_template(
            "home.html",
            title="Trang chủ – Mặc di sản, sống hiện đại",
            description=(
                "Khám phá bộ sưu tập áo phông minh hoạ 6 loại hình nghệ thuật "
                "truyền thống miền Bắc. Mỗi chiếc áo đi kèm mã QR dẫn tới câu chuyện "
                "và âm thanh di sản, giúp bạn trở thành đại sứ văn hoá Việt."
            ),
            featured_products=featured_products,
            featured_characters=featured_characters,
        )

    @app.route("/products")
    def product_list():
        products = load_products()
        characters = load_characters()

        search_query = request.args.get('q', '').strip().lower()
        if search_query:
            products = [p for p in products if search_query in p.get('name', '').lower() or search_query in p.get('short_description', '').lower()]

        selected_character_slugs = request.args.getlist('character')
        if selected_character_slugs:
            selected_char_ids = [c['id'] for c in characters if c.get('slug') in selected_character_slugs]
            if selected_char_ids:
                products = [p for p in products if p.get('character_id') in selected_char_ids]

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return render_template("partials/product_grid.html", products=products)

        return render_template(
            "products.html",
            title="Sản phẩm – Áo phông di sản Việt",
            description="Mua những thiết kế áo phông mang 6 loại hình nghệ thuật truyền thống Việt Nam.",
            products=products,
            characters=characters,
        )

    @app.route("/product/<slug>")
    def product_detail(slug: str):
        products = load_products()
        product = next((p for p in products if p["slug"] == slug), None)
        if product is None:
            abort(404)
        conn = _get_db()
        variants = conn.execute(
            """
            SELECT id, size, color, price, stock_qty
            FROM product_variants
            WHERE product_id = ? AND is_active = 1
            ORDER BY id
            """,
            (product["id"],),
        ).fetchall()
        variant_ids = [variant["id"] for variant in variants]
        qr_batch_ready = False
        qr_demo_url = None
        if variant_ids:
            placeholders = ",".join(["?"] * len(variant_ids))
            sample_tag = conn.execute(
                f"""
                SELECT token
                FROM qr_tags
                WHERE variant_id IN ({placeholders})
                ORDER BY created_at DESC
                LIMIT 1
                """,
                tuple(variant_ids),
            ).fetchone()
            qr_batch_ready = sample_tag is not None
            if sample_tag:
                qr_demo_url = url_for("qr_short_redirect", token=sample_tag["token"])
        conn.close()
        characters = load_characters()
        character = next((c for c in characters if c["id"] == product["character_id"]), None)
        return render_template(
            "product_detail.html",
            title=f"{product['name']} – Áo phông di sản Việt",
            description=product.get(
                "seo_description",
                product.get("short_description", "Áo phông lấy cảm hứng từ di sản Việt."),
            ),
            product=product,
            character=character,
            variants=variants,
            qr_batch_ready=qr_batch_ready,
            qr_demo_url=qr_demo_url,
        )

    def _redirect_cart_back(default_endpoint="cart_view"):
        next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
        if next_url:
            return redirect(next_url)
        fallback = _background_from_referrer(request.referrer, request.host)
        if fallback:
            return redirect(fallback)
        return redirect(url_for(default_endpoint))

    def _require_login():
        user = _get_current_user()
        if user:
            return user, None
        return None, redirect(url_for("login", next=request.full_path.rstrip("?")))

    def _client_ip_from_request():
        forwarded_for = (request.headers.get("X-Forwarded-For") or "").strip()
        if forwarded_for:
            first_ip = forwarded_for.split(",")[0].strip()
            if first_ip:
                return first_ip
        cloudflare_ip = (request.headers.get("CF-Connecting-IP") or "").strip()
        if cloudflare_ip:
            return cloudflare_ip
        return (request.remote_addr or "").strip()

    def _allow_qr_scan_log(conn, qr_tag_id, ip_hash):
        if not ip_hash:
            return True
        now = datetime.utcnow()
        last_scan = conn.execute(
            """
            SELECT scanned_at
            FROM qr_scans
            WHERE qr_tag_id = ? AND ip_hash = ?
            ORDER BY scanned_at DESC
            LIMIT 1
            """,
            (qr_tag_id, ip_hash),
        ).fetchone()
        if last_scan:
            try:
                last_scanned_at = datetime.fromisoformat(last_scan["scanned_at"])
            except (TypeError, ValueError):
                last_scanned_at = None
            if last_scanned_at and (now - last_scanned_at).total_seconds() < 5:
                return False
        window_start = (now - timedelta(minutes=5)).isoformat()
        recent_count = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM qr_scans
            WHERE qr_tag_id = ? AND ip_hash = ? AND scanned_at >= ?
            """,
            (qr_tag_id, ip_hash, window_start),
        ).fetchone()
        return recent_count["count"] < 10

    @app.route("/cart")
    def cart_view():
        cart = get_cart_snapshot(_get_current_user())
        return render_template(
            "cart.html",
            title="Giỏ hàng",
            description="Quản lý giỏ hàng, mã giảm giá và phí vận chuyển tạm tính.",
            cart=cart,
        )

    @app.route("/cart/add", methods=["POST"])
    def cart_add():
        variant_id = _parse_int(request.form.get("variant_id"), 0)
        quantity = _parse_int(request.form.get("quantity"), 1)
        ok, message = add_item_to_cart(_get_current_user(), variant_id, quantity)
        flash(message, "success" if ok else "error")
        return _redirect_cart_back()

    @app.route("/cart/item/<int:variant_id>/update", methods=["POST"])
    def cart_update(variant_id):
        quantity = _parse_int(request.form.get("quantity"), 1)
        ok, message = update_cart_item(_get_current_user(), variant_id, quantity)
        flash(message, "success" if ok else "error")
        return _redirect_cart_back()

    @app.route("/cart/item/<int:variant_id>/remove", methods=["POST"])
    def cart_remove(variant_id):
        ok, message = remove_cart_item(_get_current_user(), variant_id)
        flash(message, "success" if ok else "error")
        return _redirect_cart_back()

    @app.route("/cart/coupon", methods=["POST"])
    def cart_coupon():
        action = (request.form.get("action") or "apply").strip().lower()
        if action == "remove":
            ok, message = clear_cart_coupon(_get_current_user())
        else:
            coupon_code = request.form.get("coupon_code", "")
            ok, message = apply_coupon_to_cart(_get_current_user(), coupon_code)
        flash(message, "success" if ok else "error")
        return _redirect_cart_back()

    @app.route("/cart/shipping", methods=["POST"])
    def cart_shipping():
        shipping_zone = request.form.get("shipping_zone", "")
        ok, message = set_shipping_zone(_get_current_user(), shipping_zone)
        flash(message, "success" if ok else "error")
        return _redirect_cart_back()

    @app.route("/checkout", methods=["GET", "POST"])
    def checkout():
        user = _get_current_user()
        cart = get_cart_snapshot(user)
        if not cart["items"]:
            flash("Gio hang trong, vui long them san pham truoc khi thanh toan.", "error")
            return redirect(url_for("cart_view"))

        profile = get_checkout_prefill(user)
        addresses = list_user_addresses(user["id"]) if user else []
        form_values = {
            "recipient_name": profile.get("full_name", ""),
            "email": profile.get("email", ""),
            "phone": profile.get("phone", ""),
            "line1": profile.get("line1", ""),
            "line2": profile.get("line2", ""),
            "ward": profile.get("ward", ""),
            "district": profile.get("district", ""),
            "province": profile.get("province", ""),
            "notes": "",
            "shipping_zone": cart.get("shipping_zone") or "",
            "payment_method": "cod",
            "address_id": profile.get("address_id", ""),
        }
        if request.method == "POST":
            for key in form_values:
                form_values[key] = (request.form.get(key) or "").strip()
            result = place_order_from_cart(
                user=user,
                form_data=request.form,
                remote_addr=request.remote_addr or "",
                vnpay_return_url=url_for("vnpay_return", _external=True),
            )
            if result["ok"]:
                if result["payment_method"] == "vnpay" and result.get("payment_url"):
                    return redirect(result["payment_url"])
                return redirect(url_for("checkout_success", order_number=result["order_number"]))
            flash(result["error"], "error")
            cart = get_cart_snapshot(user)

        return render_template(
            "checkout.html",
            title="Thanh toán",
            description="Hoàn tất đặt hàng và chọn phương thức thanh toán.",
            cart=cart,
            form_values=form_values,
            profile=profile,
            addresses=addresses,
        )

    @app.route("/checkout/success")
    def checkout_success():
        order_number = (request.args.get("order_number") or "").strip()
        if not order_number:
            return redirect(url_for("home"))
        order, items = get_order_details(order_number)
        if order is None:
            abort(404)
        return render_template(
            "checkout_result.html",
            title=f"Dat hang thanh cong - {order_number}",
            description="Don hang cua ban da duoc ghi nhan.",
            order=order,
            items=items,
            is_success=True,
        )

    @app.route("/checkout/failure")
    def checkout_failure():
        order_number = (request.args.get("order_number") or "").strip()
        order = None
        items = []
        if order_number:
            order, items = get_order_details(order_number)
        return render_template(
            "checkout_result.html",
            title="Thanh toan that bai",
            description="Giao dich khong thanh cong, vui long thu lai.",
            order=order,
            items=items,
            is_success=False,
        )

    @app.route("/payment/vnpay/return")
    def vnpay_return():
        result = handle_vnpay_callback(dict(request.args), source="return")
        if result["success"]:
            return redirect(url_for("checkout_success", order_number=result["order_number"]))
        flash("Thanh toan online khong thanh cong. Don hang da duoc cap nhat.", "error")
        return redirect(url_for("checkout_failure", order_number=result["order_number"]))

    @app.route("/payment/vnpay/ipn")
    def vnpay_ipn():
        result = handle_vnpay_callback(dict(request.args), source="ipn")
        return jsonify({"RspCode": result["code"], "Message": result["message"]})

    @app.route("/account/profile", methods=["GET", "POST"])
    def account_profile():
        user, redirect_response = _require_login()
        if redirect_response:
            return redirect_response
        if request.method == "POST":
            full_name = (request.form.get("full_name") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            update_user_profile(user["id"], full_name, phone)
            flash("Da cap nhat thong tin tai khoan.", "success")
            return redirect(url_for("account_profile"))
        profile = get_user_profile(user["id"])
        if profile is None:
            flash("Khong tim thay thong tin tai khoan.", "error")
            return redirect(url_for("home"))
        addresses = list_user_addresses(user["id"])
        return render_template(
            "account_profile.html",
            title="Tai khoan ca nhan",
            description="Quan ly thong tin ca nhan va dia chi giao hang.",
            profile=profile,
            addresses=addresses,
        )

    @app.route("/account/addresses/add", methods=["POST"])
    def account_address_add():
        user, redirect_response = _require_login()
        if redirect_response:
            return redirect_response
        ok, message = add_user_address(user["id"], request.form)
        flash(message, "success" if ok else "error")
        return redirect(url_for("account_profile"))

    @app.route("/account/addresses/<int:address_id>/update", methods=["POST"])
    def account_address_update(address_id):
        user, redirect_response = _require_login()
        if redirect_response:
            return redirect_response
        ok, message = update_user_address(user["id"], address_id, request.form)
        flash(message, "success" if ok else "error")
        return redirect(url_for("account_profile"))

    @app.route("/account/addresses/<int:address_id>/delete", methods=["POST"])
    def account_address_delete(address_id):
        user, redirect_response = _require_login()
        if redirect_response:
            return redirect_response
        ok, message = delete_user_address(user["id"], address_id)
        flash(message, "success" if ok else "error")
        return redirect(url_for("account_profile"))

    @app.route("/account/addresses/<int:address_id>/default", methods=["POST"])
    def account_address_default(address_id):
        user, redirect_response = _require_login()
        if redirect_response:
            return redirect_response
        ok, message = set_default_user_address(user["id"], address_id)
        flash(message, "success" if ok else "error")
        return redirect(url_for("account_profile"))

    @app.route("/account/orders")
    def account_orders():
        user, redirect_response = _require_login()
        if redirect_response:
            return redirect_response
        orders = list_user_orders(user["id"])
        return render_template(
            "account_orders.html",
            title="Lich su don hang",
            description="Theo doi trang thai don hang va ma van don.",
            orders=orders,
        )

    @app.route("/account/orders/<order_number>")
    def account_order_detail(order_number):
        user, redirect_response = _require_login()
        if redirect_response:
            return redirect_response
        order, items, events = get_user_order_detail(user["id"], order_number)
        if order is None:
            abort(404)
        return render_template(
            "account_order_detail.html",
            title=f"Don hang {order_number}",
            description="Chi tiet don hang va lich su trang thai.",
            order=order,
            items=items,
            events=events,
        )

    @app.route("/order-tracking", methods=["GET", "POST"])
    def order_tracking():
        current_user = _get_current_user()
        order = None
        items = []
        events = []
        order_number = ""
        email = (current_user.get("email") if current_user else "") or ""
        if request.method == "POST":
            order_number = (request.form.get("order_number") or "").strip()
            email = (request.form.get("email") or "").strip().lower()
            if not order_number or not email:
                flash("Vui long nhap ma don va email dat hang.", "error")
            else:
                order, items, events = get_order_by_number_and_email(order_number, email)
                if order is None:
                    flash("Khong tim thay don hang phu hop.", "error")
        return render_template(
            "order_tracking.html",
            title="Theo doi don hang",
            description="Tra cuu don hang bang ma don va email.",
            order=order,
            items=items,
            events=events,
            order_number=order_number,
            email=email,
        )

    @app.route("/character/<slug>")
    def character_page(slug: str):
        characters = load_characters()
        character = next((c for c in characters if c["slug"] == slug), None)
        if character is None:
            abort(404)
        products = load_products()
        from_qr = request.args.get("from_qr") == "1"
        qr_token = (request.args.get("token") or "").strip() if from_qr else ""
        return render_template(
            "character.html",
            title=f"{character['name']} - Di san Viet",
            description=character.get(
                "seo_description",
                character.get("description", "Di san Viet."),
            ),
            character=character,
            characters=characters,
            products=products,
            from_qr=from_qr,
            qr_token=qr_token,
            body_class="character-detail-page",
            main_class="character-main",
        )

    @app.route("/about")
    def about():
        return render_template(
            "about.html",
            title="Về chúng tôi – Nếp Thanh",
            description="Tìm hiểu câu chuyện và sứ mệnh của Nếp Thanh trong việc đưa di sản tới cộng đồng.",
        )

    @app.route("/contact")
    def contact():
        return render_template(
            "contact.html",
            title="Liên hệ – Nếp Thanh",
            description="Liên hệ với chúng tôi để hợp tác hoặc hỗ trợ mua hàng.",
        )

    @app.route("/signup", methods=["GET", "POST"])
    def signup():
        if _get_current_user():
            return redirect(url_for("home"))
        error = request.args.get("error")
        next_url = _safe_next_url(request.args.get("next") or request.form.get("next"))
        background_url = _safe_background_url(next_url)
        if not background_url:
            background_url = _background_from_referrer(request.referrer, request.host)
        if not background_url:
            background_url = url_for("home")
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            confirm = request.form.get("confirm_password", "")
            if not email or "@" not in email:
                error = "Vui lòng nhập email hợp lệ."
            elif len(password) < 6:
                error = "Mật khẩu phải có ít nhất 6 ký tự."
            elif password != confirm:
                error = "Mật khẩu xác nhận không khớp."
            elif _get_user_by_email(email):
                error = "Email đã được đăng ký."
            else:
                user_id = _create_user(email, generate_password_hash(password))
                session["user_id"] = user_id
                merge_guest_cart_into_user(user_id)
                return redirect(next_url or url_for("home"))
        return render_template(
            "signup.html",
            title="Đăng ký tài khoản",
            description="Tạo tài khoản để theo dõi sản phẩm mới và lưu thông tin mua hàng.",
            error=error,
            google_enabled=_google_enabled(),
            next_url=next_url,
            background_url=background_url,
        )

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if _get_current_user():
            return redirect(url_for("home"))
        error = request.args.get("error")
        next_url = _safe_next_url(request.args.get("next") or request.form.get("next"))
        background_url = _safe_background_url(next_url)
        if not background_url:
            background_url = _background_from_referrer(request.referrer, request.host)
        if not background_url:
            background_url = url_for("home")
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            user = _get_user_by_email(email) if email else None
            if not user or not check_password_hash(user["password_hash"], password):
                error = "Email hoặc mật khẩu không đúng."
            elif user["is_blocked"]:
                error = "Tài khoản của bạn đang bị khóa."
            else:
                session["user_id"] = user["id"]
                merge_guest_cart_into_user(user["id"])
                return redirect(next_url or url_for("home"))
        return render_template(
            "login.html",
            title="Đăng nhập",
            description="Đăng nhập để theo dõi sản phẩm mới và ưu đãi.",
            error=error,
            google_enabled=_google_enabled(),
            next_url=next_url,
            background_url=background_url,
        )

    @app.route("/logout")
    def logout():
        session.pop("user_id", None)
        return redirect(url_for("home"))

    @app.route("/auth/google")
    def google_login():
        if not _google_enabled():
            return redirect(url_for("login", error="Google chưa được cấu hình."))
        state = secrets.token_urlsafe(24)
        session["google_oauth_state"] = state
        next_url = _safe_next_url(request.args.get("next"))
        if next_url:
            session["google_oauth_next"] = next_url
        redirect_uri = url_for("google_callback", _external=True)
        return redirect(_google_oauth_url(state, redirect_uri))

    @app.route("/auth/google/callback")
    def google_callback():
        if not _google_enabled():
            return redirect(url_for("login", error="Google chưa được cấu hình."))
        state = request.args.get("state")
        code = request.args.get("code")
        if not state or state != session.get("google_oauth_state"):
            return redirect(url_for("login", error="Phiên đăng nhập không hợp lệ."))
        if not code:
            return redirect(url_for("login", error="Không nhận được mã xác thực Google."))
        redirect_uri = url_for("google_callback", _external=True)
        try:
            token_data = _exchange_google_code(code, redirect_uri)
            access_token = token_data.get("access_token")
            if not access_token:
                return redirect(url_for("login", error="Không lấy được access token."))
            userinfo = _fetch_google_userinfo(access_token)
        except urllib.error.URLError:
            return redirect(url_for("login", error="Không kết nối được Google OAuth."))
        email = (userinfo.get("email") or "").strip().lower()
        name = userinfo.get("name")
        if not email:
            return redirect(url_for("login", error="Google không trả về email hợp lệ."))
        user = _get_user_by_email(email)
        if user:
            user_id = user["id"]
        else:
            user_id = _create_user(
                email,
                generate_password_hash(secrets.token_urlsafe(32)),
                full_name=name,
                is_verified=1,
            )
        session["user_id"] = user_id
        merge_guest_cart_into_user(user_id)
        session.pop("google_oauth_state", None)
        next_url = _safe_next_url(session.pop("google_oauth_next", None))
        return redirect(next_url or url_for("home"))

    @app.route("/sitemap.xml")
    def sitemap():
        pages = []
        lastmod = datetime.utcnow().date().isoformat()
        base_url = request.url_root.rstrip("/")
        products = load_products()
        characters = load_characters()
        static_paths = [
            ("/", "home"),
            ("/products", "product list"),
            ("/about", "about"),
            ("/contact", "contact"),
        ]
        for path, _ in static_paths:
            url = f"{base_url}{path}"
            pages.append(f"<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>")
        for product in products:
            url = f"{base_url}/product/{product['slug']}"
            pages.append(f"<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>")
        for char in characters:
            url = f"{base_url}/character/{char['slug']}"
            pages.append(f"<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>")
        conn = _get_db()
        page_rows = conn.execute(
            "SELECT slug FROM pages WHERE status = 'published'"
        ).fetchall()
        post_rows = conn.execute(
            "SELECT slug FROM posts WHERE status = 'published'"
        ).fetchall()
        conn.close()
        for page in page_rows:
            url = f"{base_url}/page/{page['slug']}"
            pages.append(f"<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>")
        for post in post_rows:
            url = f"{base_url}/blog/{post['slug']}"
            pages.append(f"<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>")
        xml = "<?xml version='1.0' encoding='UTF-8'?>\n" + (
            "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
            + "".join(pages)
            + "</urlset>"
        )
        return Response(xml, mimetype="application/xml")

    @app.route("/page/<slug>")
    def page_detail(slug: str):
        conn = _get_db()
        page = conn.execute(
            "SELECT * FROM pages WHERE slug = ? AND status = 'published'", (slug,)
        ).fetchone()
        conn.close()
        if page is None:
            abort(404)
        return render_template(
            "page.html",
            title=page["seo_title"] or page["title"],
            description=page["seo_description"],
            page=page,
        )

    @app.route("/blog")
    def blog_list():
        conn = _get_db()
        posts = conn.execute(
            "SELECT * FROM posts WHERE status = 'published' ORDER BY published_at DESC, created_at DESC"
        ).fetchall()
        conn.close()
        return render_template(
            "blog_list.html",
            title="Blog – Nếp Thanh",
            description="Chia sẻ câu chuyện di sản và hành trình sáng tạo của Nếp Thanh.",
            posts=posts,
        )

    @app.route("/blog/<slug>")
    def blog_detail(slug: str):
        conn = _get_db()
        post = conn.execute(
            "SELECT * FROM posts WHERE slug = ? AND status = 'published'", (slug,)
        ).fetchone()
        conn.close()
        if post is None:
            abort(404)
        return render_template(
            "blog_detail.html",
            title=post["seo_title"] or post["title"],
            description=post["seo_description"] or post["excerpt"],
            post=post,
        )

    @app.route("/q/<token>")
    def qr_short_redirect(token: str):
        target = url_for("qr_redirect", token=token)
        query = request.query_string.decode("utf-8")
        if query:
            target = f"{target}?{query}"
        return redirect(target)

    @app.route("/qr/<token>")
    def qr_redirect(token: str):
        conn = _get_db()
        tag = conn.execute(
            """
            SELECT qr_tags.*, characters.slug AS character_slug
            FROM qr_tags
            JOIN characters ON characters.id = qr_tags.character_id
            WHERE qr_tags.token = ?
            """,
            (token,),
        ).fetchone()
        if tag is None or (tag["status"] or "").strip().lower() != "active":
            conn.close()
            abort(404)

        user = _get_current_user()
        ip_hash = _hash_ip(_client_ip_from_request())
        if _allow_qr_scan_log(conn, tag["id"], ip_hash):
            conn.execute(
                """
                INSERT INTO qr_scans (qr_tag_id, user_id, scanned_at, ip_hash, user_agent, referrer, utm_source, utm_campaign)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tag["id"],
                    user["id"] if user else None,
                    datetime.utcnow().isoformat(),
                    ip_hash,
                    request.headers.get("User-Agent"),
                    request.referrer,
                    request.args.get("utm_source"),
                    request.args.get("utm_campaign"),
                ),
            )
            conn.commit()
        conn.close()
        return redirect(
            url_for(
                "character_page",
                slug=tag["character_slug"],
                from_qr=1,
                token=tag["token"],
            )
        )
