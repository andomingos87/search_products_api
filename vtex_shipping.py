import os
from typing import List, Optional, Dict, Any

import requests
from pydantic import BaseModel
from dotenv import load_dotenv
from pathlib import Path

# Carrega variáveis de ambiente (.env) do diretório deste arquivo (api/.env)
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH, override=False)

# =====================
# Config VTEX
# =====================
VTEX_HOST = os.getenv("VTEX_ACCOUNT_HOST", "copafer.myvtex.com").strip()
VTEX_BASE_URL = f"https://{VTEX_HOST}"
VTEX_APP_TOKEN = os.getenv("VTEX_APP_TOKEN", "").strip()
VTEX_APP_KEY = os.getenv("VTEX_APP_KEY", "").strip()


def _vtex_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if VTEX_APP_KEY and VTEX_APP_TOKEN:
        headers["X-VTEX-API-AppKey"] = VTEX_APP_KEY
        headers["X-VTEX-API-AppToken"] = VTEX_APP_TOKEN
    return headers


class ItemInput(BaseModel):
    sku: str
    quantity: int
    seller: Optional[str] = "1"


class ShippingSimulateRequest(BaseModel):
    items: List[ItemInput]
    postalCode: str
    country: str = "BRA"
    sc: str = "1"  # Sales Channel


def get_product_id_by_sku(ref_id: str) -> Optional[int]:
    """Consulta a VTEX e retorna ProductId a partir do RefId (SKU)."""
    url = f"{VTEX_BASE_URL}/api/catalog/pvt/stockkeepingunit"
    try:
        resp = requests.get(url, params={"RefId": ref_id}, headers=_vtex_headers(), timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        return int(data.get("ProductId")) if data and data.get("ProductId") is not None else None
    except Exception:
        return None


def simulate_shipping_for_skus(
    items: List[ItemInput],
    postal_code: str,
    country: str = "BRA",
    sc: str = "1",
) -> Dict[str, Any]:
    """Converte SKUs em ProductIds e simula frete na VTEX.

    Retorna: ok, notFoundSkus, request, logisticsInfo, slas (simplificado)
    """
    items_payload: List[Dict[str, Any]] = []
    not_found: List[str] = []

    for item in items:
        pid = get_product_id_by_sku(item.sku)
        if pid is None:
            not_found.append(item.sku)
            continue
        items_payload.append({
            "id": str(pid),
            "quantity": int(item.quantity),
            "seller": item.seller or "1",
        })

    if not items_payload:
        return {
            "ok": False,
            "message": "Nenhum SKU pôde ser convertido em ProductId",
            "notFoundSkus": not_found,
        }

    url = f"{VTEX_BASE_URL}/api/checkout/pub/orderForms/simulation"
    params = {"sc": sc}
    payload = {
        "items": items_payload,
        "country": country,
        "postalCode": postal_code,
        "geoCoordinates": [],
    }

    try:
        resp = requests.post(url, params=params, json=payload, headers=_vtex_headers(), timeout=20)
        resp.raise_for_status()
        data = resp.json()

        logistics_info = data.get("logisticsInfo", [])

        simplified: List[Dict[str, Any]] = []
        for info in logistics_info:
            slas = info.get("slas", []) or []
            simplified.append({
                "itemIndex": info.get("itemIndex"),
                "slas": [
                    {"id": s.get("id"), "price": s.get("price")}
                    for s in slas if isinstance(s, dict)
                ],
            })

        return {
            "ok": True,
            "notFoundSkus": not_found,
            "request": payload,
            "logisticsInfo": logistics_info,
            "slas": simplified,
        }
    except requests.HTTPError as e:
        return {
            "ok": False,
            "message": f"Erro HTTP na simulação: {e}",
        }
    except Exception as e:
        return {
            "ok": False,
            "message": f"Falha ao simular frete: {e}",
        }


def extract_slas_id_price(logistics_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extrai e achata todos os SLAs como [{"id": str, "price": number}]."""
    result: List[Dict[str, Any]] = []
    for info in logistics_info or []:
        for sla in (info.get("slas") or []):
            if isinstance(sla, dict) and "id" in sla and "price" in sla:
                result.append({"id": sla.get("id"), "price": sla.get("price")})
    return result
