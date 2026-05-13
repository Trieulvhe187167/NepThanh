# Nếp Thanh

Website thương mại điện tử cho thương hiệu áo phông di sản Việt Nam **Nếp Thanh - Dòng chảy thanh âm Việt**. Dự án được xây bằng Flask, có trang bán hàng public, giỏ hàng, checkout, quản trị sản phẩm/đơn hàng/kho, mã giảm giá, flash sale, QR nhân vật, chatbot CSKH, email, VNPay và tính phí vận chuyển qua đơn vị giao hàng.

## Tính năng chính

- Trang public: trang chủ, danh sách sản phẩm, chi tiết sản phẩm, nhân vật di sản, giỏ hàng, thanh toán, tra cứu đơn hàng.
- Tài khoản khách hàng: đăng ký, đăng nhập, quên mật khẩu, đổi mật khẩu, xác thực email, quản lý địa chỉ giao hàng.
- Quản trị: dashboard, sản phẩm, biến thể size/màu, tồn kho, đơn hàng, khách hàng, nội dung, marketing, coupon, flash sale, QR/nhân vật, báo cáo, phân quyền.
- Bán hàng: áp mã giảm giá, flash sale, trừ/hoàn tồn kho, email xác nhận đơn, cập nhật trạng thái đơn.
- Thanh toán: COD và VNPay.
- Vận chuyển: tính phí qua GHN, có cấu trúc mở rộng cho GHTK/Viettel Post và phí dự phòng.
- Chatbot: hỗ trợ tư vấn sản phẩm/chính sách bằng RAG và Gemini nếu cấu hình API key.

## Công nghệ

- Python 3.10+
- Flask
- SQLite cho local/dev
- Turso/libSQL cho deploy tùy cấu hình
- Jinja2 templates
- CSS/JavaScript thuần

## Cấu trúc thư mục

```text
NepThanh/
├─ app.py                  # Entry point Flask
├─ modules/                # Logic backend, routes, DB, checkout, shipping, auth
├─ templates/              # Giao diện Jinja2
├─ static/                 # CSS, JS, ảnh, uploads
├─ data/                   # SQLite database local
├─ scripts/                # Script hỗ trợ, ví dụ migrate SQLite sang Turso
├─ requirements.txt        # Python dependencies
├─ vercel.json             # Cấu hình deploy Vercel
└─ .env                    # Biến môi trường local, không commit
```

## Cài đặt local

Tạo môi trường ảo:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Cài thư viện:

```powershell
pip install -r requirements.txt
```

Tạo file `.env` ở thư mục gốc. Cấu hình tối thiểu để chạy local:

```env
FLASK_SECRET_KEY=change-me
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=change-this-password
```

Chạy dự án:

```powershell
python app.py
```

Mở website:

```text
http://127.0.0.1:5000
```

Trang quản trị:

```text
http://127.0.0.1:5000/admin
```

Database SQLite local sẽ được tự tạo tại:

```text
data/nepthanh.db
```

## Biến môi trường

### Bắt buộc nên có

```env
FLASK_SECRET_KEY=
ADMIN_EMAIL=
ADMIN_PASSWORD=
```

### Database Turso/libSQL khi deploy

```env
TURSO_DATABASE_URL=
TURSO_AUTH_TOKEN=
TURSO_SYNC_INTERVAL_SECONDS=15
```

Nếu không cấu hình Turso, dự án dùng SQLite local. Với website bán hàng thật, nên dùng database server/cloud thay vì chỉ dùng SQLite file.

### Email SMTP

Dùng cho xác thực email, quên mật khẩu và thông báo đơn hàng.

```env
SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=
SMTP_FROM=
SMTP_USE_TLS=1
```

### Google OAuth

```env
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
```

### VNPay

```env
VNPAY_TMN_CODE=
VNPAY_HASH_SECRET=
VNPAY_PAYMENT_URL=
```

### GHN

```env
GHN_ENV=production
GHN_TOKEN=
GHN_SHOP_ID=
GHN_SERVICE_TYPE_ID=2
GHN_FROM_DISTRICT_ID=
GHN_FROM_WARD_CODE=
SHIPPING_ORIGIN_DISTRICT=
SHIPPING_ORIGIN_WARD=
SHIPPING_ORIGIN_ADDRESS=
```

### GHTK

```env
GHTK_TOKEN=
GHTK_PARTNER_CODE=
GHTK_PICK_PROVINCE=
GHTK_PICK_DISTRICT=
GHTK_PICK_WARD=
GHTK_PICK_ADDRESS=
GHTK_TRANSPORT=road
```

### Viettel Post

```env
VIETTELPOST_TOKEN=
VIETTELPOST_FEE_ENDPOINT=
VIETTELPOST_PICK_PROVINCE=
VIETTELPOST_PICK_DISTRICT=
```

### Chatbot và thông báo

```env
GEMINI_API_KEY=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
CONTENT_CACHE_TTL_SECONDS=30
```

## Lệnh kiểm tra nhanh

Kiểm tra cú pháp Python:

```powershell
python -m py_compile app.py modules\*.py
```

Chạy app local sau khi sửa:

```powershell
python app.py
```

## Deploy

Dự án có sẵn `vercel.json` cho Vercel Python runtime:

```powershell
vercel
```

Khi deploy, cần cấu hình biến môi trường trên nền tảng deploy, tối thiểu:

- `FLASK_SECRET_KEY`
- `ADMIN_EMAIL`
- `ADMIN_PASSWORD`
- Database production như `TURSO_DATABASE_URL` và `TURSO_AUTH_TOKEN`
- SMTP nếu dùng xác thực email/quên mật khẩu
- VNPay nếu bật thanh toán online
- GHN nếu muốn tính phí vận chuyển thật

Lưu ý: trên Vercel, thư mục `/tmp` là tạm thời. Không nên dùng SQLite file làm database chính cho production trên serverless. Hãy dùng Turso/libSQL hoặc database cloud phù hợp.

## Ghi chú vận hành

- Không commit `.env`, database SQLite hoặc file bí mật.
- Backup database thường xuyên trước khi deploy thay đổi lớn.
- Với shop thật có đơn hàng và tồn kho, nên dùng database production có backup/monitoring.
- Sau khi đổi cấu hình GHN/VNPay/SMTP, nên test lại toàn bộ luồng checkout: giỏ hàng, tính ship, đặt COD, thanh toán VNPay, email xác nhận và cập nhật tồn kho.
