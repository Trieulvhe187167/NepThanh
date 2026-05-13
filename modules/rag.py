"""Local RAG engine – ChromaDB + sentence-transformers.

Ingests FAQ.md + product/character data from the DB into a ChromaDB
collection.  At query time, retrieves top-k relevant chunks and
assembles a grounded, role-constrained customer-support answer.

ROLE: Trợ lý CSKH của Nếp Thanh – CHỈ hỗ trợ các câu hỏi liên quan đến
sản phẩm, chính sách, đặt hàng, nhân vật di sản và văn hoá dân gian Việt Nam.
KHÔNG trả lời các chủ đề ngoài phạm vi (chính trị, y tế, công nghệ, v.v.).
"""

import os
import re
import hashlib
from datetime import datetime

from modules.config import DB_PATH
from modules.db import _get_db

# chromadb is imported lazily – not available on Vercel serverless
try:
    import chromadb
    from chromadb.config import Settings
    _CHROMADB_AVAILABLE = True
except ImportError:
    _CHROMADB_AVAILABLE = False

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
_DATA_DIR = os.path.dirname(DB_PATH)  # data/
_CHROMA_DIR = os.path.join(_DATA_DIR, "chroma_store")
_FAQ_PATH = os.path.join(_DATA_DIR, "faq.md")
_COLLECTION_NAME = "nepthanh_knowledge"

# Embedding model – multilingual, 384-dim, fast
_EMBED_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# ---------------------------------------------------------------------------
# Lazy singletons
# ---------------------------------------------------------------------------
_chroma_client = None
_collection = None
_embed_fn = None


def _get_embed_fn():
    """Lazily load sentence-transformers embedding function."""
    global _embed_fn
    if _embed_fn is not None:
        return _embed_fn
    from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
    _embed_fn = SentenceTransformerEmbeddingFunction(
        model_name=_EMBED_MODEL_NAME,
    )
    return _embed_fn


def _get_collection():
    """Return the ChromaDB collection (create if needed)."""
    global _chroma_client, _collection
    if not _CHROMADB_AVAILABLE:
        raise RuntimeError("ChromaDB is not available in this environment.")
    if _collection is not None:
        return _collection
    _chroma_client = chromadb.PersistentClient(
        path=_CHROMA_DIR,
        settings=Settings(anonymized_telemetry=False),
    )
    _collection = _chroma_client.get_or_create_collection(
        name=_COLLECTION_NAME,
        embedding_function=_get_embed_fn(),
        metadata={"hnsw:space": "cosine"},
    )
    return _collection


# ---------------------------------------------------------------------------
# Role-guard: phát hiện câu hỏi ngoài phạm vi hỗ trợ
# ---------------------------------------------------------------------------

_VN_NORMALIZE = str.maketrans(
    "àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ",
    "aaaaaaaaaaaaaaaaaeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyd",
)


def _norm(text: str) -> str:
    return text.lower().translate(_VN_NORMALIZE).strip()


# Từ khoá thuộc phạm vi của Nếp Thanh (whitelist chủ đề)
_IN_SCOPE_KEYWORDS = [
    # Sản phẩm & mua hàng
    "ao", "san pham", "size", "mau", "gia", "bao nhieu", "dat hang", "mua",
    "order", "don hang", "cart", "gio hang", "chot", "thanh toan", "cod",
    "chuyen khoan", "vnpay", "hoa don",
    # Vận chuyển & đổi trả
    "ship", "giao hang", "doi tra", "hoan tien", "doi size", "doi mau",
    "loi", "nguyen tem", "phi ship", "freeship", "van chuyen",
    # Nhân vật & văn hoá
    "nhan vat", "di san", "xam", "cheo", "quan ho", "then", "khen",
    "van hoa", "truyen thong", "nghe thuat", "qr", "podcast", "nhac",
    # Thương hiệu
    "nep thanh", "nepthanh", "shop", "cua hang", "lien he", "email",
    "facebook", "instagram", "ho tro",
    # Chung về áo / thời trang liên quan
    "chat lieu", "cotton", "dtg", "in ao", "giat", "bao quan",
]

# Từ khoá ngoài phạm vi (blacklist chủ đề) – từ chối nhẹ nhàng
_OUT_OF_SCOPE_KEYWORDS = [
    # Chính trị / xã hội
    "chinh tri", "chinh phu", "dang", "bau cu", "bien gioi", "chien tranh",
    # Y tế / thuốc
    "benh", "thuoc", "bac si", "chay troi", "ung thu", "covid", "vaccine",
    # Tài chính / đầu tư (không liên quan mua hàng)
    "co phieu", "bitcoin", "crypto", "dau tu", "chung khoan",
    # Kỹ thuật / lập trình
    "code", "lap trinh", "python", "sql", "ai model", "chatgpt", "llm",
    "hack", "password", "bao mat he thong",
    # Tình cảm / giải trí không liên quan
    "tinh yeu", "ban gai", "ban trai", "nguoi yeu", "hon nhan",
    "phim", "phim han", "idol", "kpop", "game",
    # Thực phẩm / dịch vụ khác
    "do an", "tiem an", "nha hang", "khach san", "du lich",
]


def _check_scope(query: str):
    """
    Returns ('in_scope', None) nếu câu hỏi thuộc phạm vi Nếp Thanh,
    hoặc ('out_of_scope', reply_text) nếu ngoài phạm vi.
    """
    norm = _norm(query)

    # Kiểm tra blacklist trước (ưu tiên từ chối)
    for kw in _OUT_OF_SCOPE_KEYWORDS:
        if kw in norm:
            return "out_of_scope", (
                "Mình là trợ lý của **Nếp Thanh** – chuyên hỗ trợ về sản phẩm áo phông di sản, "
                "đặt hàng, vận chuyển và tư vấn nhân vật văn hoá Việt Nam. "
                "Câu hỏi này nằm ngoài phạm vi mình có thể hỗ trợ. 🙏\n\n"
                "Bạn có muốn hỏi về sản phẩm, size, chính sách đổi trả hay nhân vật di sản không?"
            )

    return "in_scope", None


# ---------------------------------------------------------------------------
# Chunking helpers
# ---------------------------------------------------------------------------


def _chunk_faq(faq_text):
    """
    Split FAQ into chunks per subsection (### heading).
    Each chunk carries metadata: source, section_id.
    """
    chunks = []
    current_section = "general"
    current_subsection = ""
    lines_buf = []

    for line in faq_text.split("\n"):
        # Detect section: ## name {#id}
        sec_match = re.match(r"^##\s+(.*?)\s*\{#([\w-]+)\}", line)
        if sec_match:
            # Flush previous
            if lines_buf:
                text = "\n".join(lines_buf).strip()
                if text:
                    doc_id = f"faq:{current_section}:{current_subsection or 'intro'}"
                    chunks.append({
                        "id": hashlib.md5(doc_id.encode()).hexdigest(),
                        "text": text,
                        "source": f"faq:{current_section}",
                        "section": current_section,
                        "subsection": current_subsection,
                        "type": "faq",
                    })
                lines_buf = []
            current_section = sec_match.group(2)
            current_subsection = ""
            lines_buf.append(line)
            continue

        # Detect subsection: ### heading
        sub_match = re.match(r"^###\s+(.+)", line)
        if sub_match:
            # Flush previous subsection
            if lines_buf:
                text = "\n".join(lines_buf).strip()
                if text:
                    doc_id = f"faq:{current_section}:{current_subsection or 'intro'}"
                    chunks.append({
                        "id": hashlib.md5(doc_id.encode()).hexdigest(),
                        "text": text,
                        "source": f"faq:{current_section}",
                        "section": current_section,
                        "subsection": current_subsection,
                        "type": "faq",
                    })
                lines_buf = []
            current_subsection = sub_match.group(1).strip().lower().replace(" ", "-")
            lines_buf.append(line)
            continue

        lines_buf.append(line)

    # Flush last
    if lines_buf:
        text = "\n".join(lines_buf).strip()
        if text:
            doc_id = f"faq:{current_section}:{current_subsection or 'intro'}"
            chunks.append({
                "id": hashlib.md5(doc_id.encode()).hexdigest(),
                "text": text,
                "source": f"faq:{current_section}",
                "section": current_section,
                "subsection": current_subsection,
                "type": "faq",
            })

    return chunks


def _chunk_products():
    """Create one chunk per product from DB."""
    conn = _get_db()
    products = conn.execute(
        """
        SELECT p.id, p.slug, p.name, p.base_price, p.description,
               c.name AS character_name, c.slug AS character_slug
        FROM products p
        LEFT JOIN characters c ON c.id = p.character_id
        WHERE p.status = 'active'
        ORDER BY p.id
        """
    ).fetchall()
    variants = conn.execute(
        """
        SELECT pv.product_id, pv.size, pv.color, pv.price, pv.stock_qty, pv.sku
        FROM product_variants pv
        WHERE pv.is_active = 1
        ORDER BY pv.product_id
        """
    ).fetchall()
    conn.close()

    variant_map = {}
    for v in variants:
        variant_map.setdefault(v["product_id"], []).append(v)

    chunks = []
    for p in products:
        vs = variant_map.get(p["id"], [])
        variant_lines = []
        for v in vs:
            price = f"{int(v['price']):,} VND" if v["price"] else "Liên hệ"
            stock = f"còn {v['stock_qty']}" if (v["stock_qty"] or 0) > 0 else "hết hàng"
            variant_lines.append(
                f"  - Size {v['size'] or '?'} / Màu {v['color'] or '?'}: {price} ({stock})"
            )

        if p['base_price']:
            text = (
                f"Sản phẩm: {p['name']}\n"
                f"Nhân vật: {p['character_name'] or 'Không'}\n"
                f"Giá niêm yết: {int(p['base_price']):,} VND\n"
            )
        else:
            text = (
                f"Sản phẩm: {p['name']}\n"
                f"Nhân vật: {p['character_name'] or 'Không'}\n"
                f"Giá niêm yết: Liên hệ\n"
            )
        if p["description"]:
            text += f"Mô tả: {p['description'][:300]}\n"
        if variant_lines:
            text += "Các phiên bản:\n" + "\n".join(variant_lines)

        chunks.append({
            "id": f"product:{p['id']}",
            "text": text,
            "source": f"db:products:{p['slug']}",
            "section": "products",
            "subsection": p["slug"],
            "type": "product",
        })

    return chunks


def _chunk_characters():
    """Create one chunk per character from DB."""
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT id, slug, name, nickname, story_text, origin,
               personality, symbol, role
        FROM characters
        WHERE is_active = 1
        ORDER BY id
        """
    ).fetchall()
    conn.close()

    chunks = []
    for r in rows:
        story = r["story_text"] or r["origin"] or ""
        text = (
            f"Nhân vật di sản: {r['name']}\n"
            f"Biệt danh: {r['nickname'] or ''}\n"
            f"Vai trò: {r['role'] or ''}\n"
            f"Tính cách: {r['personality'] or ''}\n"
            f"Biểu tượng: {r['symbol'] or ''}\n"
            f"Câu chuyện: {story[:500]}"
        )
        chunks.append({
            "id": f"character:{r['id']}",
            "text": text,
            "source": f"db:characters:{r['slug']}",
            "section": "characters",
            "subsection": r["slug"],
            "type": "character",
        })

    return chunks


# ---------------------------------------------------------------------------
# Ingest – index all data into ChromaDB
# ---------------------------------------------------------------------------

_INDEXED = False


def ingest(force=False):
    """
    Parse all data sources and upsert into ChromaDB.
    Called once at startup or on admin re-index.
    """
    global _INDEXED
    if not _CHROMADB_AVAILABLE:
        return  # silently skip on environments without ChromaDB
    if _INDEXED and not force:
        return
    collection = _get_collection()

    all_chunks = []

    # FAQ
    if os.path.exists(_FAQ_PATH):
        with open(_FAQ_PATH, "r", encoding="utf-8") as f:
            faq_text = f.read()
        all_chunks.extend(_chunk_faq(faq_text))

    # Products + Characters from DB
    all_chunks.extend(_chunk_products())
    all_chunks.extend(_chunk_characters())

    if not all_chunks:
        _INDEXED = True
        return

    # Upsert in batches (Chroma max batch default = 5461)
    batch = 100
    for i in range(0, len(all_chunks), batch):
        chunk_batch = all_chunks[i : i + batch]
        collection.upsert(
            ids=[c["id"] for c in chunk_batch],
            documents=[c["text"] for c in chunk_batch],
            metadatas=[
                {
                    "source": c["source"],
                    "section": c["section"],
                    "subsection": c["subsection"],
                    "type": c["type"],
                }
                for c in chunk_batch
            ],
        )

    _INDEXED = True
    print(f"[RAG] Indexed {len(all_chunks)} chunks into ChromaDB.")


def reindex():
    """Force re-index (called from admin upload)."""
    global _INDEXED
    _INDEXED = False
    # Delete old collection
    try:
        client = _get_collection()._client
        client.delete_collection(_COLLECTION_NAME)
    except Exception:
        pass
    global _collection
    _collection = None
    ingest(force=True)


# ---------------------------------------------------------------------------
# Retrieve – find relevant chunks
# ---------------------------------------------------------------------------


def retrieve(query, top_k=5, type_filter=None):
    """
    Retrieve top-k relevant chunks for a query.
    Returns list of dicts: {text, source, section, subsection, type, distance}.
    """
    if not _CHROMADB_AVAILABLE:
        return []  # no RAG available
    ingest()  # ensure indexed
    collection = _get_collection()

    where_filter = None
    if type_filter:
        where_filter = {"type": type_filter}

    results = collection.query(
        query_texts=[query],
        n_results=top_k,
        where=where_filter,
        include=["documents", "metadatas", "distances"],
    )

    hits = []
    if results and results["documents"] and results["documents"][0]:
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            hits.append({
                "text": doc,
                "source": meta.get("source", ""),
                "section": meta.get("section", ""),
                "subsection": meta.get("subsection", ""),
                "type": meta.get("type", ""),
                "distance": dist,
            })

    return hits


# ---------------------------------------------------------------------------
# Context builder – lấy thêm dữ liệu thực tế từ DB để làm giàu câu trả lời
# ---------------------------------------------------------------------------


def _get_live_product_context(query: str) -> str:
    """
    Truy vấn DB trực tiếp để lấy thông tin sản phẩm phù hợp với câu hỏi.
    Trả về chuỗi ngắn mô tả sản phẩm liên quan (dùng để bổ sung vào reply).
    """
    try:
        conn = _get_db()
        products = conn.execute(
            """
            SELECT p.name, p.base_price, c.name AS character_name,
                   GROUP_CONCAT(
                       pv.size || '/' || COALESCE(pv.color,'?') || '=' ||
                       COALESCE(CAST(pv.price AS TEXT), CAST(p.base_price AS TEXT)) ||
                       'VND(còn ' || COALESCE(pv.stock_qty, 0) || ')',
                       ', '
                   ) AS variants_summary
            FROM products p
            LEFT JOIN characters c ON c.id = p.character_id
            LEFT JOIN product_variants pv ON pv.product_id = p.id AND pv.is_active = 1
            WHERE p.status = 'active'
            GROUP BY p.id
            ORDER BY p.id
            LIMIT 10
            """
        ).fetchall()
        conn.close()

        if not products:
            return ""

        norm_q = _norm(query)
        matched = []
        for p in products:
            name_norm = _norm(p["name"] or "")
            char_norm = _norm(p["character_name"] or "")
            # Chỉ lấy sản phẩm liên quan đến câu hỏi (hoặc tất cả nếu câu chung)
            if (name_norm in norm_q or char_norm in norm_q
                    or any(w in norm_q for w in ["san pham", "ao", "mua", "co gi", "list", "danh sach"])):
                matched.append(p)

        if not matched:
            matched = products[:3]  # Lấy 3 sản phẩm đầu nếu không match cụ thể

        lines = []
        for p in matched[:5]:
            price = f"{int(p['base_price']):,} VND" if p["base_price"] else "Liên hệ"
            char_info = f" (nhân vật: {p['character_name']})" if p["character_name"] else ""
            variants_info = f"\n   Phiên bản: {p['variants_summary']}" if p["variants_summary"] else ""
            lines.append(f"• **{p['name']}**{char_info} – từ {price}{variants_info}")

        return "\n".join(lines)
    except Exception:
        return ""


def _fmt_faq_chunk(chunk_text: str) -> str:
    """Làm sạch và format chunk FAQ cho hiển thị thân thiện."""
    lines = []
    for line in chunk_text.split("\n"):
        if line.startswith("## ") or line.startswith("# "):
            continue  # bỏ headers cấp cao
        if line.startswith("### "):
            lines.append(f"**{line.lstrip('# ').strip()}**")
        elif line.strip().startswith("- "):
            lines.append(line)
        elif line.strip().startswith("| "):
            lines.append(line)
        elif line.strip() == "---":
            continue
        elif line.strip():
            lines.append(line)
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# RAG Answer – role-constrained customer support response
# ---------------------------------------------------------------------------


# Danh sách câu chào/hỏi thăm thông thường
_GREETING_PATTERNS = [
    r"^(xin chao|chao|hello|hi|hey|alo|chao shop|chao ban)\b",
    r"^(ban co the|ban giup|minh can|can tu van|tu van cho minh)\b",
]

# Câu hỏi về shop/thương hiệu
_BRAND_PATTERNS = [
    r"(nep thanh|nepthanh|shop|cua hang) (la gi|thu+c su+|gioi thieu|nhu the nao|o dau)",
    r"(tim hieu|biet them|thong tin) (ve|cua) (nep thanh|shop)",
]


def _is_greeting(query: str) -> bool:
    norm = _norm(query)
    return any(re.search(p, norm) for p in _GREETING_PATTERNS)


def _is_brand_inquiry(query: str) -> bool:
    norm = _norm(query)
    return any(re.search(p, norm) for p in _BRAND_PATTERNS)


_GREETING_REPLY = (
    "Xin chào! 👋 Mình là trợ lý CSKH của **Nếp Thanh** – thương hiệu áo phông di sản văn hoá Việt Nam.\n\n"
    "Mình có thể giúp bạn:\n"
    "• 🛍️ Tra cứu sản phẩm, giá, size, màu sắc\n"
    "• 📦 Hỗ trợ đặt hàng và theo dõi đơn\n"
    "• 🔄 Chính sách đổi trả, vận chuyển\n"
    "• 🎭 Tìm hiểu về nhân vật di sản trên áo\n\n"
    "Bạn cần mình tư vấn điều gì? 😊"
)

_BRAND_REPLY = (
    "**Nếp Thanh** là thương hiệu áo phông di sản văn hoá Việt Nam – "
    "mỗi chiếc áo mang một câu chuyện về nghệ thuật truyền thống miền Bắc "
    "như Xẩm, Chèo, Quan Họ, Then, Khèn…\n\n"
    "🎭 Mỗi nhân vật trên áo đại diện cho một loại hình nghệ thuật. "
    "Quét QR trên mác áo để khám phá câu chuyện, podcast và nhạc truyền thống!\n\n"
    "📬 Liên hệ: **nepthanh6886@gmail.com** | Facebook & Instagram: **@nepthanh**\n"
    "⏰ Hỗ trợ: 8:00 – 17:00 các ngày trong tuần."
)


def rag_answer(query, top_k=4):
    """
    Main entry: role-guard → retrieve relevant chunks → assemble grounded,
    customer-support-focused answer for Nếp Thanh.
    Returns a dict compatible with the chatbot response format.
    """

    # ── 0. Xử lý câu chào thông thường ──────────────────────────────────────
    if _is_greeting(query):
        return {
            "reply": _GREETING_REPLY,
            "intent": "greeting",
            "action": "none",
            "entities": {},
            "confidence": 1.0,
            "sources": ["system:greeting"],
        }

    # ── 0b. Giới thiệu thương hiệu ───────────────────────────────────────────
    if _is_brand_inquiry(query):
        return {
            "reply": _BRAND_REPLY,
            "intent": "brand_info",
            "action": "none",
            "entities": {},
            "confidence": 1.0,
            "sources": ["system:brand"],
        }

    # ── 1. Kiểm tra phạm vi vai trò ──────────────────────────────────────────
    scope, out_of_scope_reply = _check_scope(query)
    if scope == "out_of_scope":
        return {
            "reply": out_of_scope_reply,
            "intent": "out_of_scope",
            "action": "none",
            "entities": {},
            "confidence": 1.0,
            "sources": ["system:role_guard"],
        }

    # ── 2. Truy xuất chunks liên quan ────────────────────────────────────────
    hits = retrieve(query, top_k=top_k)

    if not hits:
        # Thử lấy dữ liệu sản phẩm trực tiếp từ DB làm fallback
        product_ctx = _get_live_product_context(query)
        if product_ctx:
            return {
                "reply": (
                    "Dưới đây là thông tin sản phẩm hiện tại của Nếp Thanh:\n\n"
                    + product_ctx
                    + "\n\nBạn cần tư vấn cụ thể về sản phẩm nào? Mình sẵn sàng hỗ trợ! 😊"
                ),
                "intent": "ask_catalog",
                "action": "none",
                "entities": {},
                "confidence": 0.7,
                "sources": ["db:products"],
            }
        return {
            "reply": (
                "Mình chưa tìm thấy thông tin liên quan đến câu hỏi của bạn trong hệ thống. "
                "Bạn có thể hỏi cụ thể hơn, hoặc liên hệ shop qua:\n"
                "📧 **nepthanh6886@gmail.com**\n"
                "📘 **Facebook/Instagram: @nepthanh**\n"
                "để được hỗ trợ trực tiếp nhé! 🙏"
            ),
            "intent": "other",
            "action": "handoff",
            "entities": {},
            "confidence": 0.0,
            "sources": [],
        }

    # ── 3. Lọc kết quả kém liên quan ─────────────────────────────────────────
    # Cosine distance: 0 = giống hệt, 1 = hoàn toàn khác biệt
    relevant = [h for h in hits if h["distance"] < 0.65]
    if not relevant:
        relevant = hits[:2]  # Giữ ít nhất 2 kết quả tốt nhất

    # ── 4. Xác định intent ───────────────────────────────────────────────────
    types = [h["type"] for h in relevant]
    sections = [h["section"] for h in relevant]

    if "product" in types:
        intent = "ask_price"
    elif "character" in types:
        intent = "recommend"
    elif "doi-tra" in sections or "ship" in sections:
        intent = "ask_policy"
    elif "thanh-toan" in sections:
        intent = "ask_payment"
    else:
        intent = "ask_policy"

    # ── 5. Tính confidence ───────────────────────────────────────────────────
    best_dist = relevant[0]["distance"]
    confidence = max(0.0, min(1.0, 1.0 - best_dist))

    # ── 6. Lấy bổ sung dữ liệu sản phẩm nếu câu hỏi về SP ──────────────────
    live_product_ctx = ""
    if intent in ("ask_price", "recommend") or "product" in types:
        live_product_ctx = _get_live_product_context(query)

    # ── 7. Tổng hợp câu trả lời ──────────────────────────────────────────────
    answer_parts = []
    sources = []

    for h in relevant:
        sources.append(h["source"])
        chunk_type = h["type"]
        chunk_text = h["text"].strip()

        if chunk_type == "product":
            # Format ngắn gọn cho sản phẩm
            lines = []
            for line in chunk_text.split("\n"):
                if line.strip():
                    lines.append(line)
            answer_parts.append("\n".join(lines))

        elif chunk_type == "character":
            # Format nhân vật với gợi ý QR
            formatted = _fmt_faq_chunk(chunk_text)
            if formatted:
                formatted += "\n\n🎭 *Quét QR trên mác áo để nghe câu chuyện, podcast và nhạc của nhân vật này!*"
                answer_parts.append(formatted)

        else:
            # FAQ / chính sách
            formatted = _fmt_faq_chunk(chunk_text)
            if formatted:
                answer_parts.append(formatted)

    # Thêm context sản phẩm live nếu có
    if live_product_ctx and intent == "ask_price":
        # Chèn vào đầu nếu là hỏi giá/sản phẩm
        answer_parts.insert(0, f"📦 **Sản phẩm hiện có tại Nếp Thanh:**\n{live_product_ctx}")

    # Deduplicate sources
    sources = list(dict.fromkeys(sources))

    # ── 8. Build final reply ──────────────────────────────────────────────────
    if not answer_parts:
        reply = (
            "Mình chưa có đủ thông tin để trả lời chính xác. "
            "Bạn liên hệ shop qua **nepthanh6886@gmail.com** để được hỗ trợ nhé! 🙏"
        )
    elif len(answer_parts) == 1:
        reply = answer_parts[0]
    else:
        reply = "\n\n---\n\n".join(answer_parts[:3])

    # ── 9. Thêm CTA phù hợp theo intent ─────────────────────────────────────
    cta = ""
    if intent == "ask_price" and confidence > 0.5:
        cta = "\n\n💬 Bạn muốn đặt hàng hoặc tư vấn thêm? Nhắn mình biết nhé!"
    elif intent == "recommend":
        cta = "\n\n👕 Bạn muốn xem giá hoặc đặt mẫu áo nhân vật này không?"
    elif intent == "ask_policy":
        cta = "\n\n📞 Nếu cần hỗ trợ thêm, liên hệ shop qua **nepthanh6886@gmail.com** nhé!"
    elif intent == "ask_payment":
        cta = "\n\n✅ Shop hỗ trợ COD, chuyển khoản và VNPay toàn quốc!"

    # Cảnh báo độ tin cậy thấp
    if confidence < 0.45:
        cta += (
            "\n\n⚠️ *Thông tin trên chỉ mang tính tham khảo. "
            "Bạn liên hệ shop để được xác nhận chính xác nhé!*"
        )

    reply = reply + cta

    return {
        "reply": reply,
        "intent": intent,
        "action": "none" if confidence >= 0.4 else "handoff",
        "entities": {},
        "confidence": round(confidence, 2),
        "sources": sources,
    }
