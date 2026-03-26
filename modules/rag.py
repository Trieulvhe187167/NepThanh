"""Local RAG engine – ChromaDB + sentence-transformers.

Ingests FAQ.md + product/character data from the DB into a ChromaDB
collection.  At query time, retrieves top-k relevant chunks and
assembles a grounded answer directly (no external LLM required).
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

        text = (
            f"Sản phẩm: {p['name']}\n"
            f"Nhân vật: {p['character_name'] or 'Không'}\n"
            f"Giá niêm yết: {int(p['base_price']):,} VND\n" if p['base_price'] else
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
# RAG Answer – assemble grounded response from retrieved chunks
# ---------------------------------------------------------------------------


def rag_answer(query, top_k=4):
    """
    Main entry: retrieve relevant chunks, assemble a grounded answer.
    Returns a dict compatible with the chatbot response format.
    """
    hits = retrieve(query, top_k=top_k)

    if not hits:
        return {
            "reply": (
                "Mình chưa tìm thấy thông tin liên quan đến câu hỏi của bạn. "
                "Bạn có thể hỏi cụ thể hơn, hoặc liên hệ shop qua email "
                "nepthanh6886@gmail.com để được hỗ trợ nhé! 🙏"
            ),
            "intent": "other",
            "action": "handoff",
            "entities": {},
            "confidence": 0.0,
            "sources": [],
        }

    # Filter out low-relevance hits (cosine distance > 0.7)
    relevant = [h for h in hits if h["distance"] < 0.7]
    if not relevant:
        relevant = hits[:1]  # Keep at least the best match

    # Determine primary type
    types = [h["type"] for h in relevant]
    if "product" in types:
        intent = "ask_price"
    elif "character" in types:
        intent = "recommend"
    else:
        intent = "ask_policy"

    # Assemble reply
    best_dist = relevant[0]["distance"]
    confidence = max(0.0, min(1.0, 1.0 - best_dist))

    # Format answer from chunks
    answer_parts = []
    sources = []

    for h in relevant:
        sources.append(h["source"])
        chunk_text = h["text"].strip()

        # Clean up markdown for display
        # Remove section headers for cleaner reading
        lines = []
        for line in chunk_text.split("\n"):
            if line.startswith("## ") or line.startswith("# "):
                continue  # skip top-level headers
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

        formatted = "\n".join(lines).strip()
        if formatted:
            answer_parts.append(formatted)

    # Deduplicate sources
    sources = list(dict.fromkeys(sources))

    if len(answer_parts) == 1:
        reply = answer_parts[0]
    else:
        # Merge, separate with dividers
        reply = "\n\n".join(answer_parts[:3])

    # Add confidence disclaimer if low
    if confidence < 0.5:
        reply += (
            "\n\n⚠️ *Mình chưa chắc lắm về thông tin này. "
            "Bạn có thể liên hệ shop để được tư vấn chính xác hơn nhé!*"
        )

    return {
        "reply": reply,
        "intent": intent,
        "action": "none" if confidence >= 0.4 else "handoff",
        "entities": {},
        "confidence": round(confidence, 2),
        "sources": sources,
    }
