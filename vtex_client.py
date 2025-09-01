# api/vtex_client.py
"""
Cliente VTEX em Python que lê credenciais do .env e consulta SKU por RefId.

Uso via CLI:
  - Com .venv ativada e .env preenchido em api/.env
  - python api/vtex_client.py 33375

Variáveis esperadas no .env:
  VTEX_APP_TOKEN=...
  VTEX_APP_KEY=...
  VTEX_ACCOUNT_HOST=copafer.myvtex.com  # opcional (padrão)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict

import requests
from dotenv import load_dotenv


def _load_env() -> None:
    """Carrega variáveis de ambiente do .env (tenta api/.env e raiz)."""
    # 1) Tenta no mesmo diretório do arquivo (api/.env)
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)
    # 2) Fallback: procura .env subindo diretórios
    load_dotenv(override=False)


def get_sku_by_ref_id(ref_id: str, *, host: str | None = None) -> Dict[str, Any]:
    """Faz GET no endpoint VTEX para buscar SKU por RefId.

    Args:
        ref_id: Código de referência do SKU
        host: Domínio da conta VTEX (ex: 'copafer.myvtex.com'). Se None, usa VTEX_ACCOUNT_HOST

    Returns:
        JSON (dict) da resposta

    Raises:
        ValueError: se parâmetros ou credenciais faltarem
        requests.HTTPError: se a resposta não for 2xx
    """
    if not ref_id:
        raise ValueError("Parâmetro ref_id é obrigatório")

    _load_env()

    token = os.getenv("VTEX_APP_TOKEN")
    app_key = os.getenv("VTEX_APP_KEY")
    host = host or os.getenv("VTEX_ACCOUNT_HOST", "copafer.myvtex.com")

    if not token or not app_key:
        raise ValueError(
            "Credenciais ausentes: defina VTEX_APP_TOKEN e VTEX_APP_KEY no .env"
        )

    url = f"https://{host}/api/catalog/pvt/stockkeepingunit"
    params = {"RefId": ref_id}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppToken": token,
        "X-VTEX-API-AppKey": app_key,
    }

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        # inclui corpo de erro para facilitar debug
        raise requests.HTTPError(
            f"Falha na requisição ({resp.status_code} {resp.reason}): {resp.text}"
        ) from e

    return resp.json()


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Uso: python api/vtex_client.py <RefId>")
        return 1

    ref_id = argv[1]
    try:
        data = get_sku_by_ref_id(ref_id)
    except Exception as e:
        print(f"Erro: {e}")
        return 2

    import json

    print(json.dumps(data, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
