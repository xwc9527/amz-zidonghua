"""
Multi-marketplace FBA fulfilment fee estimators (选型估算用).

数据来源（公开费率表）：
  US  — Amazon US FBA rate card + 3.5% fuel surcharge (2026-04-17)
  JP  — Amazon JP 公开结构（体积重 /6000）
  EU  — Amazon Europe Rate Card effective 2026-04-17
        https://m.media-amazon.com/images/G/02/sell/images/260410-FBA-Rate-Card-EN.pdf
        站点: UK/DE/FR/IT/ES/NL/SE/PL/BE；附加 1.5% fuel surcharge

未内置公开费率表 / 工具不支持估算的站点见 FBA_UNSUPPORTED。
配置费（placement）：仅 US Minimal-split 中位估算；其它站点返回 None。
"""
from __future__ import annotations

import math
import re
from typing import Optional

# ── parsers ─────────────────────────────────────────────────────────

def parse_weight_lb(text: str | None) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"([\d,.]+)", text)
    if not m:
        return None
    v = float(m.group(1).replace(",", "."))
    low = text.lower()
    if "kg" in low or "kilogramm" in low:
        return v * 2.20462
    if "ounce" in low or re.search(r"\boz\b", low):
        return v / 16.0
    if "gramm" in low or re.search(r"\bg\b", low):
        return v * 0.00220462
    return v


def parse_weight_kg(text: str | None) -> Optional[float]:
    lb = parse_weight_lb(text)
    return None if lb is None else lb / 2.20462


def parse_dims_inches(text: str | None) -> Optional[tuple[float, float, float]]:
    if not text:
        return None
    nums = [float(x.replace(",", ".")) for x in re.findall(r"[\d,.]+", text)]
    if len(nums) < 3:
        return None
    if "cm" in text.lower() or "mm" in text.lower():
        if "mm" in text.lower():
            nums = [n / 25.4 for n in nums[:3]]
        else:
            nums = [n / 2.54 for n in nums[:3]]
    else:
        nums = nums[:3]
    nums.sort(reverse=True)
    return nums[0], nums[1], nums[2]


def parse_dims_cm(text: str | None) -> Optional[tuple[float, float, float]]:
    inches = parse_dims_inches(text)
    if inches is None:
        return None
    return inches[0] * 2.54, inches[1] * 2.54, inches[2] * 2.54


def _lookup_ceil(table: list[tuple[float, float]], x: float) -> float:
    for lim, fee in table:
        if x <= lim + 1e-9:
            return fee
    return table[-1][1]


# 站点支持矩阵（UI 全站点对齐；明细见 fba_fees_extra.py，均经联网核对 Amazon 公开表）
FBA_UNSUPPORTED = {
    "IE": "爱尔兰站公开表为促销折扣结构，本工具暂不估算",
}

FBA_SUPPORTED = frozenset({
    "US", "JP", "UK", "DE", "FR", "IT", "ES", "NL", "SE", "PL", "BE",
    "CA", "AU", "MX", "BR", "SG", "IN", "SA", "AE", "TR", "EG",
})

SITE_CURRENCY = {
    "US": "$", "UK": "£", "DE": "€", "FR": "€", "IT": "€", "ES": "€",
    "NL": "€", "BE": "€", "SE": "kr", "PL": "zł", "JP": "¥",
    "CA": "CA$", "AU": "A$", "IN": "₹", "MX": "MX$", "BR": "R$",
    "SG": "S$", "SA": "SAR ", "AE": "AED ", "TR": "₺", "EG": "E£",
}


def fba_support_info(site: str) -> dict:
    site = (site or "US").upper()
    if site in FBA_SUPPORTED:
        return {"supported": True, "site": site, "reason": None, "currency": SITE_CURRENCY.get(site, "$")}
    reason = FBA_UNSUPPORTED.get(site, "该站点无本工具可用的 Amazon 公开 FBA 费率表")
    return {"supported": False, "site": site, "reason": reason, "currency": SITE_CURRENCY.get(site, "$")}


# ═══════════════════════════════════════════════════════════════════
# US
# ═══════════════════════════════════════════════════════════════════

DIM_DIVISOR_US = 139.0
SURCHARGE_US = 1.035

_SMALL_STD = [(2, 3.06), (4, 3.15), (6, 3.24), (8, 3.33), (10, 3.43), (12, 3.53), (14, 3.60), (16, 3.65)]
_SMALL_STD_LP = [(2, 2.29), (4, 2.38), (6, 2.47), (8, 2.56), (10, 2.66), (12, 2.76), (14, 2.83), (16, 2.88)]
_LARGE_STD = [(4, 3.68), (8, 3.90), (12, 4.15), (16, 4.55), (20, 4.99), (24, 5.37), (28, 5.52), (32, 5.66), (36, 5.80), (40, 5.87), (48, 6.62)]
_LARGE_STD_LP = [(4, 2.91), (8, 3.13), (12, 3.38), (16, 3.78), (20, 4.22), (24, 4.60), (28, 4.75), (32, 5.00), (36, 5.10), (40, 5.40), (48, 5.85)]


def _us_size_tier(L: float, W: float, H: float, unit_lb: float) -> str:
    girth = 2 * (W + H)
    if L <= 15 and W <= 12 and H <= 0.75 and unit_lb <= 1.0:
        return "small_standard"
    if L <= 18 and W <= 14 and H <= 8 and unit_lb <= 20:
        return "large_standard"
    if L <= 59 and W <= 33 and H <= 33 and unit_lb <= 50 and (L + girth) <= 130:
        return "large_bulky"
    return "extra_large"


def _us_shipping_weight(tier: str, unit_lb: float, L: float, W: float, H: float) -> float:
    if tier == "small_standard":
        return unit_lb
    dim = (L * W * H) / DIM_DIVISOR_US
    if tier == "large_standard" and unit_lb <= 1.0:
        return unit_lb
    return max(unit_lb, dim)


def _us_fulfillment(tier: str, ship_lb: float, low_price: bool) -> float:
    oz = ship_lb * 16.0
    if tier == "small_standard":
        return _lookup_ceil(_SMALL_STD_LP if low_price else _SMALL_STD, oz)
    if tier == "large_standard":
        if oz <= 48:
            return _lookup_ceil(_LARGE_STD_LP if low_price else _LARGE_STD, oz)
        base = 6.15 if low_price else 6.92
        return base + 0.08 * math.ceil(max(0.0, oz - 48.0) / 4.0)
    if tier == "large_bulky":
        base = 8.84 if low_price else 9.61
        return base + 0.38 * max(0.0, ship_lb - 1.0)
    if ship_lb <= 50:
        base = 25.56 if low_price else 26.33
        return base + 0.38 * max(0.0, ship_lb - 1.0)
    if ship_lb <= 70:
        base = 39.35 if low_price else 40.12
        return base + 0.75 * max(0.0, ship_lb - 51.0)
    if ship_lb <= 150:
        base = 54.04 if low_price else 54.81
        return base + 0.75 * max(0.0, ship_lb - 71.0)
    base = 194.18 if low_price else 194.95
    return base + 0.19 * max(0.0, ship_lb - 150.0)


def _us_placement(tier: str, ship_lb: float) -> float:
    oz = ship_lb * 16.0
    if tier == "small_standard":
        return 0.24 if oz <= 8 else 0.28
    if tier == "large_standard":
        if oz <= 12: return 0.30
        if ship_lb <= 1.5: return 0.37
        if ship_lb <= 3: return 0.47
        if ship_lb <= 5: return 0.57
        if ship_lb <= 7: return 0.69
        if ship_lb <= 10: return 0.81
        if ship_lb <= 15: return 0.97
        return 1.22
    if tier == "large_bulky":
        if ship_lb <= 5: return 2.40
        if ship_lb <= 12: return 2.90
        if ship_lb <= 28: return 3.80
        if ship_lb <= 42: return 4.80
        return 5.70
    return 2.30 if ship_lb <= 50 else 3.50


# ═══════════════════════════════════════════════════════════════════
# JP
# ═══════════════════════════════════════════════════════════════════

DIM_DIVISOR_JP = 6000.0
_JP_STD = [
    (0.25, 288), (0.5, 319), (1.0, 365), (2.0, 425),
    (5.0, 520), (9.0, 620), (15.0, 780), (20.0, 920),
]
_JP_OVER = [(25, 1200), (30, 1500), (40, 2100), (50, 2800)]


def _jp_size_tier(L: float, W: float, H: float, unit_kg: float) -> str:
    dims = sorted([L, W, H], reverse=True)
    L, W, H = dims
    if L <= 45 and W <= 35 and H <= 20 and unit_kg <= 9:
        return "standard"
    return "oversize"


def _jp_shipping_kg(tier: str, unit_kg: float, L: float, W: float, H: float) -> float:
    dim = (L * W * H) / DIM_DIVISOR_JP
    return max(unit_kg, dim)


def _jp_fulfillment(tier: str, ship_kg: float) -> float:
    if tier == "standard":
        return _lookup_ceil(_JP_STD, ship_kg)
    return _lookup_ceil(_JP_OVER, ship_kg)


# ═══════════════════════════════════════════════════════════════════
# EU (UK/DE/FR/IT/ES/NL/SE/PL/BE) — Europe Rate Card 2026-04-17
# Column order: UK, CEP, DE, FR, IT, ES, NL, SE, PL, BE
# ═══════════════════════════════════════════════════════════════════

DIM_DIVISOR_EU = 5000.0
SURCHARGE_EU = 1.015

_EU_COL = {
    "UK": 0, "DE": 2, "FR": 3, "IT": 4, "ES": 5, "NL": 6, "SE": 7, "PL": 8, "BE": 9,
}

# Low-Price: (max_kg, fees[10])
_EU_LP: dict[str, list[tuple[float, tuple]]] = {
    "light_envelope": [
        (0.02, (1.46, 1.61, 1.87, 2.24, 2.64, 2.15, 1.96, 28.71, 1.68, 1.74)),
        (0.04, (1.50, 1.64, 1.90, 2.26, 2.65, 2.21, 2.00, 28.91, 1.70, 1.77)),
        (0.06, (1.52, 1.66, 1.92, 2.27, 2.67, 2.23, 2.00, 29.07, 1.70, 1.78)),
        (0.08, (1.67, 1.80, 2.06, 2.79, 2.79, 2.55, 2.08, 30.56, 1.72, 1.83)),
        (0.10, (1.70, 1.83, 2.09, 2.81, 2.81, 2.59, 2.11, 30.74, 1.73, 1.86)),
    ],
    "standard_envelope": [
        (0.21, (1.73, 1.86, 2.12, 2.81, 2.81, 2.61, 2.16, 31.56, 1.74, 1.98)),
        (0.46, (1.87, 2.02, 2.28, 3.31, 3.04, 2.85, 2.25, 36.61, 1.83, 2.12)),
    ],
    "large_envelope": [
        (0.96, (2.42, 2.39, 2.65, 3.96, 3.35, 3.00, 2.91, 37.79, 1.89, 2.66)),
    ],
    "envelope_xl": [
        (0.96, (2.65, 2.78, 3.04, 4.31, 3.59, 3.23, 3.26, 40.84, 1.91, 2.96)),
    ],
    "small_parcel": [
        (0.15, (2.67, 2.78, 3.04, 4.31, 3.59, 3.23, 3.13, 41.23, 1.81, 2.64)),
        (0.40, (2.70, 2.99, 3.25, 4.71, 3.91, 3.46, 3.17, 43.31, 1.86, 2.96)),
    ],
}

# Standard (non low-price) domestic — envelope/parcel/oversize bands
_EU_STD: dict[str, list[tuple[float, tuple]]] = {
    "light_envelope": [
        (0.02, (1.83, 2.07, 2.33, 2.75, 3.23, 2.77, 2.31, 33.35, 3.04, 2.26)),
        (0.04, (1.87, 2.11, 2.37, 2.76, 3.26, 2.84, 2.35, 33.52, 3.05, 2.31)),
        (0.06, (1.89, 2.13, 2.39, 2.78, 3.28, 2.87, 2.35, 33.70, 3.06, 2.31)),
        (0.08, (2.07, 2.26, 2.52, 3.30, 3.39, 3.21, 2.45, 35.08, 3.13, 2.41)),
        (0.10, (2.08, 2.28, 2.54, 3.32, 3.41, 3.23, 2.47, 35.20, 3.14, 2.43)),
    ],
    "standard_envelope": [
        (0.21, (2.10, 2.31, 2.57, 3.33, 3.45, 3.26, 2.51, 35.47, 3.16, 2.47)),
        (0.46, (2.16, 2.42, 2.68, 3.77, 3.64, 3.45, 2.60, 41.09, 3.36, 2.56)),
    ],
    "large_envelope": [
        (0.96, (2.72, 2.78, 3.04, 4.39, 3.94, 3.60, 3.26, 42.35, 3.49, 3.21)),
    ],
    "envelope_xl": [
        (0.96, (2.94, 3.16, 3.42, 4.72, 4.17, 3.85, 3.61, 45.62, 3.58, 3.53)),
    ],
    "small_parcel": [
        (0.15, (2.91, 3.12, 3.38, 4.56, 4.13, 3.52, 3.47, 45.41, 3.61, 3.39)),
        (0.40, (3.00, 3.13, 3.39, 5.07, 4.54, 3.74, 3.51, 47.29, 3.67, 3.67)),
        (0.90, (3.04, 3.14, 3.40, 5.79, 4.95, 3.95, 4.03, 48.19, 3.71, 4.15)),
        (1.40, (3.05, 3.15, 3.41, 5.87, 5.11, 4.21, 4.50, 52.68, 3.76, 4.63)),
        (1.90, (3.25, 3.17, 3.43, 6.10, 5.14, 4.27, 4.82, 54.49, 3.81, 4.95)),
        (3.90, (3.27, 4.28, 4.54, 7.80, 5.16, 5.50, 5.90, 64.10, 3.93, 6.38)),
    ],
    "standard_parcel": [
        (0.15, (2.94, 3.13, 3.39, 4.58, 4.29, 3.55, 3.69, 48.58, 3.67, 3.46)),
        (0.40, (3.01, 3.16, 3.42, 5.22, 4.70, 3.77, 4.04, 51.70, 3.73, 3.85)),
        (0.90, (3.06, 3.18, 3.44, 6.01, 5.15, 3.99, 4.39, 52.04, 3.80, 4.39)),
        (1.40, (3.26, 3.67, 3.93, 6.41, 5.26, 4.85, 4.72, 58.46, 3.89, 4.99)),
        (1.90, (3.48, 3.69, 3.95, 6.44, 5.29, 4.94, 4.76, 61.53, 3.97, 5.41)),
        (2.90, (3.49, 4.29, 4.55, 7.08, 5.30, 4.98, 4.82, 65.36, 4.10, 6.27)),
        (3.90, (3.54, 4.83, 5.09, 7.81, 5.35, 5.53, 5.15, 65.71, 4.15, 6.30)),
        (5.90, (3.56, 4.96, 5.22, 8.22, 5.38, 5.96, 5.30, 70.20, 4.19, 6.54)),
        (8.90, (3.57, 5.77, 6.03, 8.84, 5.41, 7.24, 5.74, 72.20, 4.24, 6.90)),
        (11.90, (3.58, 6.39, 6.65, 9.38, 6.25, 7.85, 6.31, 87.92, 4.37, 7.36)),
    ],
}

# Oversize: (base_max_kg, base_fees, per_kg_fees) — fee = base + per_kg * max(0, ship - base_max)
_EU_OVER = {
    "small_oversize": (
        0.76,
        (3.49, 4.30, 4.56, 7.05, 7.21, 5.68, 7.02, 82.32, 4.13, 6.63),
        (0.25, 0.18, 0.18, 0.20, 0.08, 0.04, 0.07, 0.73, 0.03, 0.04),
    ),
    "standard_oversize_light": (
        0.76,
        (4.35, 4.33, 4.59, 7.29, 7.45, 6.55, 7.12, 82.59, 4.15, 6.66),
        (0.15, 0.18, 0.18, 0.21, 0.22, 0.34, 0.22, 3.10, 0.14, 0.23),
    ),
    "standard_oversize_heavy": (
        15.76,
        (6.58, 6.99, 7.25, 10.42, 10.63, 11.72, 10.24, 129.09, 6.15, 10.15),
        (0.08, 0.07, 0.07, 0.11, 0.08, 0.27, 0.22, 4.00, 0.15, 0.24),
    ),
    "standard_oversize_large": (
        0.76,
        (5.67, 5.80, 6.06, 8.47, 9.13, 6.76, 7.22, 83.09, 4.18, 6.69),
        (0.07, 0.08, 0.08, 0.13, 0.09, 0.33, 0.23, 4.00, 0.16, 0.29),
    ),
    "bulky_oversize": (
        0.76,
        (10.20, 7.98, 8.24, 15.38, 9.59, 9.95, 9.96, 118.02, 6.27, 9.50),
        (0.24, 0.27, 0.27, 0.45, 0.30, 0.43, 0.32, 4.90, 0.16, 0.35),
    ),
    "heavy_oversize": (
        31.5,
        (13.04, 12.74, 13.00, 15.59, 16.85, 14.00, 16.22, 192.14, 10.21, 15.47),
        (0.09, 0.15, 0.15, 0.18, 0.15, 0.12, 0.62, 9.49, 0.39, 0.68),
    ),
}


def _eu_low_price(site: str, price: float | None) -> bool:
    if price is None:
        return False
    if site == "UK":
        return price <= 20
    if site == "SE":
        return price <= 230
    if site == "PL":
        return price <= 85
    return price <= 20  # EUR stores


def _eu_size_tier(L: float, W: float, H: float, unit_kg: float, dim_kg: float) -> str:
    dims = sorted([L, W, H], reverse=True)
    L, W, H = dims
    girth = L + 2 * (W + H)
    if unit_kg > 31.5 or L > 175 or girth > 360:
        return "special_oversize"
    if L <= 33 and W <= 23 and H <= 2.5 and unit_kg <= 0.10:
        return "light_envelope"
    if L <= 33 and W <= 23 and H <= 2.5 and unit_kg <= 0.46:
        return "standard_envelope"
    if L <= 33 and W <= 23 and H <= 4 and unit_kg <= 0.96:
        return "large_envelope"
    if L <= 33 and W <= 23 and H <= 6 and unit_kg <= 0.96:
        return "envelope_xl"
    if L <= 35 and W <= 25 and H <= 12 and unit_kg <= 3.90 and dim_kg <= 2.10:
        return "small_parcel"
    if L <= 45 and W <= 34 and H <= 26 and unit_kg <= 11.90 and dim_kg <= 7.96:
        return "standard_parcel"
    if L <= 61 and W <= 46 and H <= 46 and unit_kg <= 1.76 and dim_kg <= 25.82:
        return "small_oversize"
    if L <= 101 and W <= 60 and H <= 60 and unit_kg <= 15 and dim_kg <= 72.72:
        return "standard_oversize_light"
    if L <= 101 and W <= 60 and H <= 60 and unit_kg <= 23 and dim_kg <= 72.72:
        return "standard_oversize_heavy"
    if L <= 120 and W <= 60 and H <= 60 and unit_kg <= 23 and dim_kg <= 86.40:
        return "standard_oversize_large"
    if unit_kg <= 23 and dim_kg <= 126:
        return "bulky_oversize"
    if unit_kg <= 31.5:
        return "heavy_oversize"
    return "special_oversize"


def _eu_ship_kg(tier: str, unit_kg: float, dim_kg: float, low_price: bool) -> float:
    # envelopes / special / low-price: unit weight only; parcels+oversize: max(unit, dim)
    if low_price or tier in (
        "light_envelope", "standard_envelope", "large_envelope", "envelope_xl", "special_oversize",
    ):
        return unit_kg
    return max(unit_kg, dim_kg)


def _eu_fee_from_bands(bands: list[tuple[float, tuple]], ship_kg: float, col: int) -> float:
    for lim, fees in bands:
        if ship_kg <= lim + 1e-9:
            return float(fees[col])
    return float(bands[-1][1][col])


def _eu_fulfillment(site: str, tier: str, ship_kg: float, low_price: bool) -> float:
    col = _EU_COL[site]
    if tier == "special_oversize":
        # sparse special-oversize columns in PDF: UK/CEP/DE/FR/IT style — use DE-like EUR table for EUR sites
        # Approximate from DE-focused special OS bands in same card
        special = [(30, 21.30), (40, 24.19), (50, 47.98), (60, 51.99)]
        if site == "UK":
            special = [(30, 16.22), (40, 17.24), (50, 34.38), (60, 42.04)]
        base = _lookup_ceil(special, ship_kg)
        if ship_kg > 60:
            per = 0.35 if site == "UK" else 0.36
            base = special[-1][1] + per * (ship_kg - 60)
        return base

    if low_price and tier in _EU_LP:
        return _eu_fee_from_bands(_EU_LP[tier], ship_kg, col)

    if tier in _EU_STD:
        return _eu_fee_from_bands(_EU_STD[tier], ship_kg, col)

    if tier in _EU_OVER:
        base_max, bases, pers = _EU_OVER[tier]
        return float(bases[col]) + float(pers[col]) * max(0.0, ship_kg - base_max)

    # fallback: standard parcel top band
    return _eu_fee_from_bands(_EU_STD["standard_parcel"], ship_kg, col)


def _estimate_eu(site: str, weight_text, dims_text, price) -> dict:
    out = {
        "fba_fee": None, "placement_fee": None, "size_tier": None,
        "shipping_weight": None, "dim_weight": None,
        "currency": SITE_CURRENCY[site], "site": site,
        "supported": True, "reason": None,
    }
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return out
    L, W, H = dims
    dim = (L * W * H) / DIM_DIVISOR_EU
    low = _eu_low_price(site, price)
    tier = _eu_size_tier(L, W, H, unit, dim)
    ship = _eu_ship_kg(tier, unit, dim, low)
    fee = _eu_fulfillment(site, tier, ship, low) * SURCHARGE_EU
    digits = 0 if site in ("SE", "PL") else 2
    out.update(
        fba_fee=round(fee, digits),
        size_tier=tier,
        shipping_weight=round(ship, 3),
        dim_weight=round(dim, 3),
    )
    return out


# ═══════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════

def estimate_fba_fees(site: str, weight_text: str | None, dims_text: str | None,
                      price: float | None = None) -> dict:
    site = (site or "US").upper()
    info = fba_support_info(site)
    out = {
        "fba_fee": None,
        "placement_fee": None,
        "size_tier": None,
        "shipping_weight": None,
        "dim_weight": None,
        "currency": info["currency"],
        "site": site,
        "supported": info["supported"],
        "reason": info["reason"],
    }
    if not info["supported"]:
        return out

    if site == "US":
        unit = parse_weight_lb(weight_text)
        dims = parse_dims_inches(dims_text)
        if unit is None or dims is None or unit <= 0:
            return out
        L, W, H = dims
        tier = _us_size_tier(L, W, H, unit)
        dim = (L * W * H) / DIM_DIVISOR_US
        ship = _us_shipping_weight(tier, unit, L, W, H)
        low = price is not None and price < 10
        fee = _us_fulfillment(tier, ship, low) * SURCHARGE_US
        out.update(
            fba_fee=round(fee, 2),
            placement_fee=round(_us_placement(tier, ship), 2),
            size_tier=tier,
            shipping_weight=round(ship, 3),
            dim_weight=round(dim, 3),
        )
        return out

    if site == "JP":
        unit = parse_weight_kg(weight_text)
        dims = parse_dims_cm(dims_text)
        if unit is None or dims is None or unit <= 0:
            return out
        L, W, H = dims
        tier = _jp_size_tier(L, W, H, unit)
        dim = (L * W * H) / DIM_DIVISOR_JP
        ship = _jp_shipping_kg(tier, unit, L, W, H)
        fee = _jp_fulfillment(tier, ship)
        out.update(
            fba_fee=round(fee, 0),
            size_tier=tier,
            shipping_weight=round(ship, 3),
            dim_weight=round(dim, 3),
        )
        return out

    if site in _EU_COL:
        return _estimate_eu(site, weight_text, dims_text, price)

    # 其它已公开费率表站点（SG/SA/AE/EG/AU/TR/BR/MX/IN/CA）
    from fba_fees_extra import ESTIMATORS
    est = ESTIMATORS.get(site)
    if est:
        return est(weight_text, dims_text, price)

    return out


def estimate_fba_fees_us(weight_text, dims_text, price=None, **kw):
    return estimate_fba_fees("US", weight_text, dims_text, price)


def estimate_fba_fee_us(weight_text, dims_text, price=None, **kw):
    return estimate_fba_fees("US", weight_text, dims_text, price).get("fba_fee")


if __name__ == "__main__":
    samples = [
        ("US", "4.2 pounds", "12 x 10 x 4 inches", 89),
        ("DE", "500 g", "20 x 15 x 8 cm", 15.99),
        ("UK", "500 g", "20 x 15 x 8 cm", 15.99),
        ("FR", "500 g", "20 x 15 x 8 cm", 25.0),
        ("JP", "1 kg", "30 x 20 x 10 cm", 3000),
        ("CA", "1 lb", "10 x 8 x 2 inches", 20),
        ("AU", "1 kg", "20 x 15 x 8 cm", 30),
    ]
    for s in samples:
        print(s[0], estimate_fba_fees(*s))
