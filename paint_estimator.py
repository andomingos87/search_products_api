"""Módulo de lógica para estimativa de tinta e composição de latas.

Fornece funções puras para serem usadas pelo endpoint FastAPI, mantendo a
`api/api.py` focada em roteamento e validação de entrada.
"""
from __future__ import annotations

from typing import Dict, List, Tuple
import math


def compute_cans(liters_needed: float, can_sizes: List[float]) -> Tuple[Dict[float, int], float, float]:
    """Calcula a decomposição de latas para atender a um volume em litros.

    Algoritmo: abordagem gulosa (greedy)
    1. Ordena os tamanhos das latas em ordem decrescente.
    2. Usa o máximo possível das latas maiores para reduzir o restante.
    3. Se ainda restar volume, completa com a menor lata até cobrir o restante.

    Observações:
    - Minimiza o número de latas e tende a reduzir desperdício, mas não garante
      ótimo absoluto para conjuntos de tamanhos arbitrários.
    - Remove do resultado final tamanhos com quantidade zero.

    Retorna:
      - dicionário {tamanho_em_litros: quantidade}
      - litros_totais comprados
      - desperdício em litros (litros_totais - liters_needed, truncado em >= 0)
    """
    sizes = sorted([s for s in can_sizes if s > 0], reverse=True)
    remaining = max(liters_needed, 0.0)
    cans: Dict[float, int] = {s: 0 for s in sizes}

    for s in sizes:
        if remaining <= 0:
            break
        qty = int(remaining // s)
        if qty > 0:
            cans[s] += qty
            remaining -= qty * s

    if remaining > 1e-9 and sizes:
        smallest = sizes[-1]
        extra_qty = math.ceil(remaining / smallest)
        cans[smallest] += extra_qty
        remaining -= extra_qty * smallest

    total_liters = sum(size * qty for size, qty in cans.items())
    waste = max(total_liters - liters_needed, 0.0)
    # remove tamanhos com 0
    cans = {size: qty for size, qty in cans.items() if qty > 0}
    return cans, total_liters, waste


def estimate_paint(
    *,
    total_area_m2: float,
    coverage_m2_per_liter: float,
    coats: int = 1,
    exclude_area_m2: float = 0.0,
    can_sizes_liters: List[float] | None = None,
) -> dict:
    """Calcula métricas de pintura e a composição de latas.

    Parâmetros:
      - total_area_m2: área total antes de descontos.
      - coverage_m2_per_liter: rendimento m²/L por demão (> 0).
      - coats: número de demãos (>= 1).
      - exclude_area_m2: área a descontar.
      - can_sizes_liters: tamanhos de latas disponíveis.

    Retorna um dicionário serializável com métricas e composição de latas.
    """
    sizes = can_sizes_liters or [18.0, 3.6, 2.5, 0.9, 0.5]
    paintable_area = max(total_area_m2 - exclude_area_m2, 0.0)
    liters_needed = (
        (paintable_area * coats) / coverage_m2_per_liter if coverage_m2_per_liter > 0 else 0.0
    )
    cans, total_liters, waste = compute_cans(liters_needed, sizes)
    total_cans = sum(cans.values())

    return {
        "paintable_area_m2": paintable_area,
        "coats": coats,
        "coverage_m2_per_liter": coverage_m2_per_liter,
        "liters_needed": round(liters_needed, 3),
        "cans": {str(size): qty for size, qty in cans.items()},
        "total_cans": total_cans,
        "total_liters": round(total_liters, 3),
        "waste_liters": round(waste, 3),
    }
