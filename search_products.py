# search_products.py
import os
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMB_MODEL = os.getenv("EMB_MODEL", "text-embedding-3-small")
EMB_DIM = int(os.getenv("EMB_DIM", "1536"))

# Variáveis de banco separadas (em vez de DATABASE_URL)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

client = OpenAI(api_key=OPENAI_API_KEY)

def to_pgvector(vec):
    return "[" + ",".join(f"{float(x):.7f}" for x in vec) + "]"

def embed_query(q: str):
    e = client.embeddings.create(model=EMB_MODEL, input=q)
    v = e.data[0].embedding
    if len(v) != EMB_DIM:
        raise RuntimeError(f"Embedding dim {len(v)} != {EMB_DIM}")
    return v

def search_products(q: str, k: int = 8,
                    k_vec: int = 50, k_ft: int = 30, k_trgm: int = 15, k_kw: int = 50,
                    alpha: float = 0.50, beta: float = 0.30, gamma: float = 0.10, delta: float = 0.10,
                    require_kw_when_available: bool = True):
    assert OPENAI_API_KEY, "Configure OPENAI_API_KEY no .env"
    assert all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]), (
        "Configure DB_HOST, DB_PORT, DB_USER, DB_PASSWORD e DB_NAME no .env"
    )

    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
    ) as con, con.cursor(cursor_factory=RealDictCursor) as cur:
        # 1) determinístico por SKU/EAN
        cur.execute("SELECT * FROM rag.find_by_code(%s, %s);", (q, 5))
        det = cur.fetchall()
        if len(det) == 1:
            r = det[0]
            return {
                "method": "deterministic",
                "confidence": 1.0,
                "results": [{
                    "sku": r["sku"], "codigo_barras": r["codigo_barras"],
                    "name": r["name"], "reason": r["reason"], "score": 1.0
                }]
            }

        # 2) híbrido: vetorial + full-text (+ trigram)
        qvec = to_pgvector(embed_query(q))

        cur.execute("SELECT product_id, sku, name, codigo_barras, dist FROM rag.search_vec(%s::vector, %s);",
                    (qvec, k_vec))
        vec_rows = cur.fetchall()

        cur.execute("SELECT product_id, sku, name, codigo_barras, score_ft FROM rag.search_ft(%s, %s);",
                    (q, k_ft))
        ft_rows = cur.fetchall()

        # trigram opcional (pg_trgm); se não existir, ignora
        trgm_rows = []
        try:
            cur.execute("""
                SELECT id AS product_id, sku, name, codigo_barras,
                       similarity(name, %s) AS score_trgm
                FROM rag.products
                WHERE name % %s
                ORDER BY score_trgm DESC
                LIMIT %s;
            """, (q, q, k_trgm))
            trgm_rows = cur.fetchall()
        except Exception:
            trgm_rows = []

        # 2.1) canal extra: correspondência por palavra‑chave (ILIKE/unaccent) em name/description
        # Ajuda muito para termos curtos como "cimento". Nome tem peso maior que descrição.
        kw_rows = []
        try:
            # Monta padrões simples para múltiplas palavras (qualquer termo)
            tokens = [t for t in (q or "").strip().split() if t]
            if not tokens:
                tokens = [q]
            like_patterns = [f"%{t}%" for t in tokens]

            # Constrói cláusulas OR para name/description
            # Pontua 2 se nome casar, +1 se descrição casar
            where_clauses = []
            params = []
            for pat in like_patterns:
                where_clauses.append("(name ILIKE %s OR description ILIKE %s)")
                params.extend([pat, pat])
            where_sql = " OR ".join([f"({wc})" for wc in where_clauses]) or "TRUE"
            # Primeiro tenta com unaccent (se extensão existir); se falhar, cai no ILIKE simples
            try:
                sql_kw = f"""
                    SELECT id AS product_id, sku, name, codigo_barras,
                           ((CASE WHEN unaccent(name) ILIKE unaccent(%s) THEN 2 ELSE 0 END) +
                            (CASE WHEN unaccent(description) ILIKE unaccent(%s) THEN 1 ELSE 0 END))::float AS score_kw
                    FROM rag.products
                    WHERE {where_sql}
                    ORDER BY score_kw DESC, name ASC
                    LIMIT %s;
                """
                base_pat = f"%{q}%"
                exec_params = [base_pat, base_pat, *params, k_kw]
                cur.execute(sql_kw, exec_params)
                kw_rows = cur.fetchall()
            except Exception:
                sql_kw = f"""
                    SELECT id AS product_id, sku, name, codigo_barras,
                           ((CASE WHEN name ILIKE %s THEN 2 ELSE 0 END) +
                            (CASE WHEN description ILIKE %s THEN 1 ELSE 0 END))::float AS score_kw
                    FROM rag.products
                    WHERE {where_sql}
                    ORDER BY score_kw DESC, name ASC
                    LIMIT %s;
                """
                base_pat = f"%{q}%"
                exec_params = [base_pat, base_pat, *params, k_kw]
                cur.execute(sql_kw, exec_params)
                kw_rows = cur.fetchall()
        except Exception:
            kw_rows = []

    # 3) fusão + normalização
    items = {}
    def put(rows, key, val_fn):
        for r in rows:
            sku = r["sku"]
            it = items.setdefault(sku, {
                "sku": sku, "name": r["name"], "codigo_barras": r["codigo_barras"], "scores": {}
            })
            it["scores"][key] = float(val_fn(r))

    put(vec_rows, "vec", lambda r: max(0.0, 1.0 - float(r["dist"])))   # cosine -> similaridade
    put(ft_rows, "ft", lambda r: float(r["score_ft"]))
    put(trgm_rows, "trgm", lambda r: float(r["score_trgm"] or 0.0))
    put(kw_rows, "kw", lambda r: float(r.get("score_kw", 0.0)))

    max_vec = max((it["scores"].get("vec", 0.0) for it in items.values()), default=0.0)
    max_ft = max((it["scores"].get("ft", 0.0) for it in items.values()), default=0.0)
    max_tr = max((it["scores"].get("trgm", 0.0) for it in items.values()), default=0.0)
    max_kw = max((it["scores"].get("kw", 0.0) for it in items.values()), default=0.0)

    # re-normaliza pesos se algum canal não trouxe nada
    w_vec, w_ft, w_tr, w_kw = alpha, beta, gamma, delta
    total = 0.0
    if max_vec > 0: total += w_vec
    else: w_vec = 0.0
    if max_ft > 0: total += w_ft
    else: w_ft = 0.0
    if max_tr > 0: total += w_tr
    else: w_tr = 0.0
    if max_kw > 0: total += w_kw
    else: w_kw = 0.0
    if total > 0:
        w_vec /= total; w_ft /= total; w_tr /= total; w_kw /= total

    results = []
    for sku, it in items.items():
        vn = (it["scores"].get("vec", 0.0) / max_vec) if max_vec > 0 else 0.0
        fn = (it["scores"].get("ft", 0.0) / max_ft) if max_ft > 0 else 0.0
        tn = (it["scores"].get("trgm", 0.0) / max_tr) if max_tr > 0 else 0.0
        kn = (it["scores"].get("kw", 0.0) / max_kw) if max_kw > 0 else 0.0
        score = w_vec * vn + w_ft * fn + w_tr * tn + w_kw * kn
        results.append({
            "sku": sku, "name": it["name"], "codigo_barras": it["codigo_barras"],
            "score": round(score, 4), "vec": round(vn, 4), "ft": round(fn, 4), "trgm": round(tn, 4), "kw": round(kn, 4)
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    # 3.1) Se houver quaisquer itens com match por palavra‑chave, prioriza apenas esses no top
    if require_kw_when_available and any(r.get("kw", 0.0) > 0.0 for r in results):
        results = [r for r in results if r.get("kw", 0.0) > 0.0] or results
    confidence = results[0]["score"] if results else 0.0
    return {
        "method": "hybrid" if det == [] else "hybrid_with_deterministic_candidates",
        "confidence": round(confidence, 4),
        "weights": {"vec": round(w_vec, 2), "ft": round(w_ft, 2), "trgm": round(w_tr, 2), "kw": round(w_kw, 2)},
        "results": results[:k]
    }

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--q", required=True, help="consulta do usuário")
    ap.add_argument("--k", type=int, default=8)
    args = ap.parse_args()
    out = search_products(args.q, k=args.k)
    import json
    print(json.dumps(out, ensure_ascii=False, indent=2))
