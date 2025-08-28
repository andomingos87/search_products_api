import os, json, math, argparse
import csv
from decimal import Decimal, InvalidOperation
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import tiktoken
from openai import OpenAI

# ---------- Config ----------
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMB_MODEL = os.getenv("EMB_MODEL", "text-embedding-3-small")
EMB_DIM = int(os.getenv("EMB_DIM", "1536"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))
MAX_TOKENS_PER_CHUNK = 800  # seguro p/ embedding-3

client = OpenAI(api_key=OPENAI_API_KEY)
enc = tiktoken.get_encoding("cl100k_base")

EXPECTED_COLS = [
    "codigo_produto", "descricao", "descricao_tecnica",
    "codigo_barras", "tipo", "um", "qtde_cx",
    "estoque"
]

# ---------- Constantes internas de execução ----------
# Ajuste aqui para rodar sem precisar passar parâmetros no terminal
CSV_PATH = "resumido_200.csv"
LIMIT = None
SEP = None
ENCODING = None

 # ---------- Leitura robusta de CSV ----------
def diagnose_csv(csv_path: str, sep: str, encoding: str) -> list[tuple[int, int, int, str]]:
     """Analisa o CSV com csv.reader e retorna linhas problemáticas.
     Cada item: (line_no, cols_encontradas, cols_esperadas, trecho_da_linha)
     """
     problems: list[tuple[int, int, int, str]] = []
     try:
         with open(csv_path, "r", encoding=encoding, errors="replace") as f:
             raw_lines = f.readlines()
         reader = csv.reader(raw_lines, delimiter=sep, quotechar='"', doublequote=True)
         try:
             header = next(reader)
         except StopIteration:
             return [(1, 0, 0, "<arquivo vazio>")]
         expected = len(header)
         line_no = 2  # após cabeçalho
         for row in reader:
             if len(row) != expected:
                 snippet = raw_lines[line_no - 1].rstrip()[:160]
                 problems.append((line_no, len(row), expected, snippet))
             line_no += 1
     except Exception as e:
         problems.append((0, 0, 0, f"Falha ao diagnosticar: {e}"))
     return problems

def read_csv_safely(csv_path: str, sep_override: str | None = None, encoding_override: str | None = None,
                    report: bool = True) -> tuple[pd.DataFrame, dict]:
     """Tenta ler o CSV com diferentes combinações de sep/engine/encoding.
     Se 'sep_override'/'encoding_override' forem fornecidos, tenta primeiro com eles.
     Retorna (df, info) onde info traz 'sep', 'encoding', 'engine', 'tolerant' e 'problems'.
     """
     info = {"sep": None, "encoding": None, "engine": None, "tolerant": False, "problems": []}
     attempts = [
         {"sep": ",", "engine": None},
         {"sep": ";", "engine": None},
     ]
     encodings = ["utf-8", "utf-8-sig", "cp1252"]

     # Prioriza overrides, se fornecidos
     if sep_override:
         attempts = [{"sep": sep_override, "engine": None}] + [a for a in attempts if a["sep"] != sep_override]
     if encoding_override:
         encodings = [encoding_override] + [e for e in encodings if e != encoding_override]

     # Tentativas com engine padrão
     for enc_name in encodings:
         for at in attempts:
             try:
                 df = pd.read_csv(
                     csv_path,
                     dtype=str,
                     keep_default_na=False,
                     sep=at["sep"],
                     encoding=enc_name,
                 )
                 info.update({"sep": at["sep"], "encoding": enc_name, "engine": "c", "tolerant": False})
                 print(f"CSV lido com sep='{at['sep']}', encoding='{enc_name}' (engine padrão)")
                 if report:
                     info["problems"] = diagnose_csv(csv_path, info["sep"], info["encoding"])
                 return df, info
             except Exception:
                 continue

     # Fallback tolerante: pula linhas ruins
     for enc_name in encodings:
         try:
             df = pd.read_csv(
                 csv_path,
                 dtype=str,
                 keep_default_na=False,
                 sep=sep_override or ";",
                 engine="python",
                 quotechar='"',
                 doublequote=True,
                 on_bad_lines="skip",
                 encoding=enc_name,
             )
             info.update({"sep": sep_override or ";", "encoding": enc_name, "engine": "python", "tolerant": True})
             print(f"CSV lido no modo tolerante: sep='{info['sep']}', engine='python', encoding='{enc_name}', on_bad_lines='skip'")
             if report:
                 info["problems"] = diagnose_csv(csv_path, info["sep"], info["encoding"])
             return df, info
         except Exception:
             continue

     raise SystemExit("Não foi possível ler o CSV com as estratégias de fallback. Verifique separadores, aspas e encoding.")

# ---------- Conexão DB (DB_* com fallback para DATABASE_URL) ----------
def connect_db():
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    if all([db_host, db_user, db_pass, db_name]):
        return psycopg2.connect(
            host=db_host,
            port=int(db_port),
            user=db_user,
            password=db_pass,
            dbname=db_name,
        )

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "Defina DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME no .env ou forneça DATABASE_URL."
        )
    return psycopg2.connect(database_url)

# ---------- Utils ----------
def parse_decimal_br(x: str | float | int | None) -> Decimal | None:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    # remove separador de milhar e converte vírgula decimal para ponto
    s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None

def norm_str(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    return str(x).strip()

def chunk_by_tokens(text: str, max_tokens: int = MAX_TOKENS_PER_CHUNK):
    toks = enc.encode(text)
    chunks = []
    for i in range(0, len(toks), max_tokens):
        sub = enc.decode(toks[i:i+max_tokens])
        chunks.append(sub)
    return chunks or [""]

def build_product_text(row):
    # Texto que será embedado: nome + tipo + descrição técnica
    name = norm_str(row.get("descricao"))
    tipo = norm_str(row.get("tipo"))
    desc = norm_str(row.get("descricao_tecnica"))
    parts = [p for p in [name, tipo, desc] if p]
    base = " | ".join(parts)
    # Metadata leve pode ajudar sem poluir demais
    sku = norm_str(row.get("codigo_produto"))
    ean = norm_str(row.get("codigo_barras"))
    meta = f"\nSKU: {sku}" + (f" | EAN: {ean}" if ean else "")
    return (base + meta).strip()

def to_pgvector(vec):
    # pgvector literal: [v1,v2,...]
    return "[" + ",".join(f"{float(x):.7f}" for x in vec) + "]"

def get_embeddings(texts: list[str]) -> list[list[float]]:
    # chama em lote
    resp = client.embeddings.create(model=EMB_MODEL, input=texts)
    embs = [d.embedding for d in resp.data]
    # sanity check
    for e in embs:
        if len(e) != EMB_DIM:
            raise RuntimeError(f"Embedding dim {len(e)} != {EMB_DIM}")
    return embs

# ---------- DB ----------
UPSERT_PRODUCT_SQL = """
INSERT INTO rag.products
(sku, name, description, codigo_barras, tipo, um, qtde_cx, estoque, raw)
VALUES
(%(sku)s, %(name)s, %(description)s, %(codigo_barras)s, %(tipo)s, %(um)s, %(qtde_cx)s, %(estoque)s, %(raw)s)
ON CONFLICT (sku) DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  codigo_barras = EXCLUDED.codigo_barras,
  tipo = EXCLUDED.tipo,
  um = EXCLUDED.um,
  qtde_cx = EXCLUDED.qtde_cx,
  estoque = EXCLUDED.estoque,
  raw = EXCLUDED.raw
RETURNING id;
"""

DELETE_CHUNKS_SQL = "DELETE FROM rag.product_chunks WHERE product_id = %s;"

INSERT_CHUNKS_SQL_TEMPLATE = """
INSERT INTO rag.product_chunks (product_id, chunk_no, content, embedding)
VALUES %s
"""

def upsert_product(cur, row_dict) -> int:
    cur.execute(UPSERT_PRODUCT_SQL, row_dict)
    return cur.fetchone()[0]

def insert_chunks(cur, product_id: int, chunk_texts: list[str]):
    # Apaga chunks antigos para este produto (idempotência)
    cur.execute(DELETE_CHUNKS_SQL, (product_id,))

    # Embeddings em lote
    # Quebra em sublotes por BATCH_SIZE
    embeddings = []
    for i in range(0, len(chunk_texts), BATCH_SIZE):
        batch = chunk_texts[i:i+BATCH_SIZE]
        embeddings.extend(get_embeddings(batch))

    # Monta registros e insere com execute_values
    records = []
    for idx, (ct, emb) in enumerate(zip(chunk_texts, embeddings), start=1):
        records.append((
            product_id,
            idx,
            ct,
            to_pgvector(emb)  # será convertido via ::vector no template
        ))
    execute_values(
        cur,
        INSERT_CHUNKS_SQL_TEMPLATE,
        records,
        template="(%s,%s,%s,%s::vector)"
    )

def main(csv_path: str, limit: int | None = None, sep: str | None = None, encoding: str | None = None):
    assert OPENAI_API_KEY, "Configure OPENAI_API_KEY no .env"

    # Lê CSV de forma robusta
    df, info = read_csv_safely(csv_path, sep_override=sep, encoding_override=encoding, report=True)

    # Relatório de linhas problemáticas
    problems = info.get("problems", [])
    bad_count = len([p for p in problems if p[0] != 0])
    if bad_count:
        report_path = f"{csv_path}.bad_lines.txt"
        with open(report_path, "w", encoding="utf-8") as rf:
            rf.write(f"CSV: {csv_path}\n")
            rf.write(f"sep='{info['sep']}', encoding='{info['encoding']}', engine='{info['engine']}', tolerant={info['tolerant']}\n\n")
            rf.write("Linhas problemáticas (linha, cols_encontradas, cols_esperadas, trecho):\n")
            for ln, found, expected, snippet in problems:
                if ln == 0:  # erro geral
                    rf.write(f"[diagnóstico] {snippet}\n")
                else:
                    rf.write(f"{ln}\t{found}\t{expected}\t{snippet}\n")
        # Mostra um resumo curto no console
        preview = ", ".join(str(p[0]) for p in problems[:10] if p[0] != 0)
        print(f"Linhas problemáticas detectadas: {bad_count}. Amostra: {preview or 'n/d'}. Relatório: {report_path}")

    missing = [c for c in ["codigo_produto","descricao"] if c not in df.columns]
    if missing:
        raise SystemExit(f"CSV faltando colunas obrigatórias: {missing}")

    # Garante todas as colunas esperadas (se não existir, cria vazia)
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    if limit is not None and limit > 0:
        df = df.head(limit)
    total = len(df)
    print(f"Linhas no CSV: {total}")

    with connect_db() as con:
        con.autocommit = False
        with con.cursor() as cur:
            upserted = 0
            chunks_ins = 0

            for _, r in tqdm(df.iterrows(), total=total, desc="Processando"):
                sku = norm_str(r["codigo_produto"])
                if not sku:
                    continue  # ignora linhas sem código

                name = norm_str(r["descricao"])
                desc = norm_str(r["descricao_tecnica"])
                # preço não é mais ingerido
                codigo_barras = norm_str(r["codigo_barras"])
                tipo = norm_str(r["tipo"])
                um = norm_str(r["um"])
                qtde_cx = norm_str(r["qtde_cx"])
                estoque = parse_decimal_br(r["estoque"])
                # preço promocional não é mais ingerido

                # remove campos de preço do dump bruto
                raw_filtered = {k: norm_str(r.get(k)) for k in df.columns if k not in ("preco", "preco_promocional")}

                row_dict = {
                    "sku": sku,
                    "name": name,
                    "description": desc,
                    "codigo_barras": codigo_barras,
                    "tipo": tipo,
                    "um": um,
                    "qtde_cx": qtde_cx,
                    "estoque": estoque,
                    "raw": json.dumps(raw_filtered, ensure_ascii=False),
                }

                product_id = upsert_product(cur, row_dict)
                upserted += 1

                # chunking
                text = build_product_text(r)
                chunks = chunk_by_tokens(text, MAX_TOKENS_PER_CHUNK)
                insert_chunks(cur, product_id, chunks)
                chunks_ins += len(chunks)

            # otimiza planos de busca
            cur.execute("ANALYZE rag.products;")
            cur.execute("ANALYZE rag.product_chunks;")
        con.commit()

    print(f"Upserts em products: {upserted}")
    print(f"Chunks inseridos: {chunks_ins}")

if __name__ == "__main__":
    # Execução por constantes internas (sem CLI)
    # Para usar CLI no futuro, reative o bloco argparse acima.
    main(CSV_PATH, limit=LIMIT, sep=SEP, encoding=ENCODING)
