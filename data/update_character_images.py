import sqlite3

DB_PATH = "data/nepthanh.db"

mapping = {
    "chang-khen": "images/characters/chang_khen.jpg",
    "nang-then":  "images/characters/nang_then.jpg",
    "anh-hai":    "images/characters/anh_hai.jpg",
    "chu-xam":    "images/characters/chu_xam.jpg",
    "co-cheo":    "images/characters/co_cheo.jpg",
    "be-roi":     "images/characters/be_roi.jpg",
}

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 1) Thêm cột image_url nếu chưa có
cols = [row[1] for row in cur.execute("PRAGMA table_info(characters)").fetchall()]
if "image_url" not in cols:
    cur.execute("ALTER TABLE characters ADD COLUMN image_url TEXT")
    print("✅ Added column: image_url")

# 2) Update ảnh cho 6 nhân vật theo slug
for slug, img_path in mapping.items():
    cur.execute(
        "UPDATE characters SET image_url = ? WHERE slug = ?",
        (img_path, slug)
    )

conn.commit()
conn.close()
print("✅ Updated image_url for 6 characters!")
