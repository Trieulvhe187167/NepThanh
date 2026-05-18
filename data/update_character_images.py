import os
import sqlite3

DB_PATH = "data/nepthanh.db"
STATIC_DIR = "static"
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")


def find_image(folder, stem):
    target_dir = os.path.join(STATIC_DIR, *folder.split("/"))
    if not os.path.isdir(target_dir):
        return None
    matches = []
    for name in sorted(os.listdir(target_dir)):
        file_stem, ext = os.path.splitext(name)
        if file_stem == stem and ext.lower() in IMAGE_EXTENSIONS:
            path = os.path.join(target_dir, name)
            matches.append((os.path.getsize(path), f"{folder}/{name}"))
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], item[1]))
    return matches[0][1]


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cols = [row[1] for row in cur.execute("PRAGMA table_info(characters)").fetchall()]
if "image_url" not in cols:
    cur.execute("ALTER TABLE characters ADD COLUMN image_url TEXT")
    print("Added column: image_url")

for row in cur.execute("SELECT slug FROM characters ORDER BY id").fetchall():
    stem = row["slug"].replace("-", "_")
    image_path = find_image("images/characters", stem)
    if image_path:
        cur.execute(
            "UPDATE characters SET image_url = ? WHERE slug = ?",
            (image_path, row["slug"]),
        )
        print(f"{row['slug']}: {image_path}")

conn.commit()
conn.close()
print("Updated character image paths.")
