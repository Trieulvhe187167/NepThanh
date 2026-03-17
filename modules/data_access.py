import glob
import os

from modules.config import BASE_DIR
from modules.db import _get_db
from modules.utils import _normalize_static_path, _static_path_exists


STATIC_DIR = os.path.join(BASE_DIR, "static")


def _character_asset_type(path):
    ext = os.path.splitext((path or "").lower())[1]
    if ext in {".glb", ".gltf"}:
        return "model"
    return "image"


def _first_existing(candidates):
    for candidate in candidates:
        if candidate and _static_path_exists(candidate):
            return candidate
    return None


def _glob_relative(patterns):
    for pattern in patterns:
        matches = sorted(glob.glob(os.path.join(STATIC_DIR, *pattern.split("/"))))
        if matches:
            return os.path.relpath(matches[0], STATIC_DIR).replace("\\", "/")
    return None


def _resolve_character_asset_path(candidate):
    normalized = _normalize_static_path(candidate)
    if not normalized:
        return None
    if _static_path_exists(normalized):
        return normalized
    if "/" not in normalized:
        for prefix in (
            "images/characters/",
            "models/characters/",
            "images/",
            "models/",
        ):
            resolved = f"{prefix}{normalized}"
            if _static_path_exists(resolved):
                return resolved
    return normalized


def _character_preview_for_stem(stem):
    base = stem.replace("-", "_")
    exact = _first_existing(
        [
            f"images/characters/{base}.jpg",
            f"images/characters/{base}.jpeg",
            f"images/characters/{base}.png",
            f"images/characters/{base}.webp",
            f"images/{base}.jpg",
            f"images/{base}.jpeg",
            f"images/{base}.png",
            f"images/{base}.webp",
        ]
    )
    if exact:
        return exact
    return _glob_relative(
        [
            f"images/characters/{base}*.jpg",
            f"images/characters/{base}*.jpeg",
            f"images/characters/{base}*.png",
            f"images/characters/{base}*.webp",
            f"images/{base}*.jpg",
            f"images/{base}*.jpeg",
            f"images/{base}*.png",
            f"images/{base}*.webp",
        ]
    )


def _character_preview_for_asset(asset_path, slug):
    stem = os.path.splitext(os.path.basename(asset_path or ""))[0]
    preview = _character_preview_for_stem(stem)
    if preview:
        return preview
    return _character_preview_for_stem(slug)


def _character_intro_video_for_slug(slug):
    stem = slug.replace("-", "_")
    return _first_existing(
        [
            f"images/characters/{stem}.mp4",
            f"images/characters/{slug}.mp4",
            f"videos/characters/{stem}.mp4",
            f"videos/characters/{slug}.mp4",
        ]
    )


def _character_asset_for_slug(slug):
    return _first_existing(
        [
            f"images/characters/{slug.replace('-', '_')}.glb",
            f"images/characters/{slug}.glb",
            f"models/characters/{slug.replace('-', '_')}.glb",
            f"models/characters/{slug}.glb",
            f"images/characters/{slug.replace('-', '_')}.gltf",
            f"images/characters/{slug}.gltf",
            f"models/characters/{slug.replace('-', '_')}.gltf",
            f"models/characters/{slug}.gltf",
            f"images/characters/{slug.replace('-', '_')}.jpg",
            f"images/characters/{slug}.jpg",
            f"image/characters/{slug}.jpg",
            f"images/{slug.replace('-', '_')}.jpg",
        ]
    ) or f"images/{slug.replace('-', '_')}.jpg"


def _map_character(row):
    story_text = row["story_text"] or ""
    description = story_text or row["origin"] or ""
    bio_parts = [row["origin"], row["personality"], row["symbol"], row["role"]]
    bio = " ".join(part for part in bio_parts if part)
    audio_source = row["audio_url"] or row["music_sample_url"]
    requested_asset = _resolve_character_asset_path(row["image_url"]) if row["image_url"] else None
    asset_path = requested_asset if requested_asset and _static_path_exists(requested_asset) else _character_asset_for_slug(row["slug"])
    asset_type = _character_asset_type(asset_path)
    preview_image = asset_path if asset_type == "image" else (_character_preview_for_asset(asset_path, row["slug"]) or asset_path)
    intro_video = _character_intro_video_for_slug(row["slug"])

    character = {
        "id": row["id"],
        "slug": row["slug"],
        "name": row["name"],
        "nickname": row["nickname"] or "",
        "origin": row["origin"] or "",
        "personality": row["personality"] or "",
        "symbol": row["symbol"] or "",
        "role": row["role"] or "",
        "description": description,
        "story_text": story_text,
        "bio": bio,
        "audio_file": _normalize_static_path(audio_source) if audio_source else None,
        "audio_url": _normalize_static_path(audio_source) if audio_source else None,
        "image": preview_image,
        "asset_path": asset_path,
        "asset_type": asset_type,
        "intro_video": intro_video,
    }
    if row["seo_description"]:
        character["seo_description"] = row["seo_description"]
    if row["seo_title"]:
        character["seo_title"] = row["seo_title"]
    character["is_active"] = row["is_active"] if "is_active" in row.keys() else 1
    return character


def _map_product(row, image_url):
    image = _normalize_static_path(image_url)
    if not image:
        image = f"images/{row['slug'].replace('-', '_')}.jpg"
    product = {
        "id": row["id"],
        "slug": row["slug"],
        "name": row["name"],
        "price": row["base_price"] or 0,
        "character_id": row["character_id"],
        "image": image,
        "short_description": row["description"] or "",
        "long_description": row["long_description"] if "long_description" in row.keys() else "",
        "status": row["status"],
        "is_featured": row["is_featured"] if "is_featured" in row.keys() else 0,
        "collection": row["collection"] if "collection" in row.keys() else None,
    }
    if row["description"]:
        product["seo_description"] = row["description"]
    if row["seo_description"]:
        product["seo_description"] = row["seo_description"]
    if row["seo_title"]:
        product["seo_title"] = row["seo_title"]
    return product


def load_characters():
    conn = _get_db()
    rows = conn.execute(
        "SELECT * FROM characters WHERE is_active = 1 ORDER BY id"
    ).fetchall()
    conn.close()
    return [_map_character(row) for row in rows]


def load_products():
    conn = _get_db()
    products = conn.execute(
        "SELECT * FROM products WHERE status = 'active' ORDER BY id"
    ).fetchall()
    image_rows = conn.execute(
        "SELECT product_id, url FROM product_images ORDER BY sort_order, id"
    ).fetchall()
    conn.close()
    image_map = {}
    for row in image_rows:
        if row["product_id"] not in image_map and row["url"]:
            image_map[row["product_id"]] = row["url"]
    return [_map_product(row, image_map.get(row["id"])) for row in products]


def load_all_products():
    conn = _get_db()
    products = conn.execute("SELECT * FROM products ORDER BY id").fetchall()
    image_rows = conn.execute(
        "SELECT product_id, url FROM product_images ORDER BY sort_order, id"
    ).fetchall()
    conn.close()
    image_map = {}
    for row in image_rows:
        if row["product_id"] not in image_map and row["url"]:
            image_map[row["product_id"]] = row["url"]
    return [_map_product(row, image_map.get(row["id"])) for row in products]
