from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from search_products import search_products  # importa sua função já pronta
from typing import List, Optional, Dict, Any
import os
import requests
from dotenv import load_dotenv
from pathlib import Path
from paint_estimator import estimate_paint as estimate_paint_logic
from vtex_shipping import (
    ItemInput,
    ShippingSimulateRequest,
    get_product_id_by_sku,
    simulate_shipping_for_skus,
    extract_slas_id_price,
)

app = FastAPI()
# Carrega .env do diretório deste arquivo (api/.env)
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH, override=False)

class Query(BaseModel):
    query: str

@app.post("/search")
def search(q: Query):
    result = search_products(q.query, k=8)
    return result

class PaintEstimateRequest(BaseModel):
    """Schema de entrada para o cálculo de tinta.

    - total_area_m2: área total em m² a ser pintada antes de descontos.
    - coverage_m2_per_liter: rendimento da tinta em m² por litro por demão.
    - coats: número de demãos a aplicar.
    - exclude_area_m2: área (m²) de portas/janelas a ser descontada da área total.
    - can_sizes_liters: lista de tamanhos de latas (em litros) disponíveis para compra.
    """
    total_area_m2: float  # área total a ser pintada em m² (antes de descontos)
    coverage_m2_per_liter: float  # rendimento da tinta (m² por litro por demão)
    coats: int = 1  # número de demãos
    exclude_area_m2: float = 0.0  # área de portas/janelas a descontar
    can_sizes_liters: List[float] = [18.0, 3.6, 2.5, 0.9, 0.5]  # tamanhos disponíveis

@app.post("/paint/estimate")
def estimate_paint(req: PaintEstimateRequest):
    """Endpoint para estimar a quantidade de tinta e latas necessárias.

    Lógica:
      1. Calcula a área pintável e litros necessários.
      2. Delega a composição de latas e métricas para `paint_estimator.estimate_paint`.

    Retorna um JSON com métricas e a composição de latas.
    """
    return estimate_paint_logic(
        total_area_m2=req.total_area_m2,
        coverage_m2_per_liter=req.coverage_m2_per_liter,
        coats=req.coats,
        exclude_area_m2=req.exclude_area_m2,
        can_sizes_liters=req.can_sizes_liters,
    )

"""
Integração VTEX desacoplada em `vtex_shipping.py`.
Este arquivo importa `ItemInput`, `ShippingSimulateRequest`, `get_product_id_by_sku`,
`simulate_shipping_for_skus` e `extract_slas_id_price`.
"""


@app.get("/vtex/sku/{sku}/productId")
def sku_to_product_id(sku: str):
    product_id = get_product_id_by_sku(sku)
    if product_id is None:
        return {"sku": sku, "found": False}
    return {"sku": sku, "found": True, "productId": product_id}


@app.post("/shipping/simulate")
def shipping_simulate(req: ShippingSimulateRequest):
    return simulate_shipping_for_skus(
        items=req.items,
        postal_code=req.postalCode,
        country=req.country,
        sc=req.sc,
    )


@app.post("/shipping/simulate/slas")
def shipping_simulate_slas(req: ShippingSimulateRequest):
    """Retorna apenas a lista flat de {id, price} dos SLAs em logisticsInfo."""
    res = simulate_shipping_for_skus(
        items=req.items,
        postal_code=req.postalCode,
        country=req.country,
        sc=req.sc,
    )

    if not res.get("ok"):
        return res

    slas_flat = extract_slas_id_price(res.get("logisticsInfo", []))
    return slas_flat

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
