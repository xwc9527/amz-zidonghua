"""
额外站点 FBA 运费估算（均来自 Amazon 公开费率页/PDF，联网核对后录入）。

来源：
  SG — https://m.media-amazon.com/images/G/65/SG3P/FBA_fulfilment_fees_for_Amazon.sg_orders.pdf
  SA — https://sell.amazon.sa/pricing (Aug 1, 2025)
  AE — https://sell.amazon.ae/pricing (Aug 1, 2025)
  EG — https://sell.amazon.eg/pricing
  AU — https://sell.amazon.com.au/pricing
  TR — https://m.media-amazon.com/images/G/41/SOA/PricingFiles/FBA_Domestic_Rate_Card_202604_Final.pdf
  BR — https://m.media-amazon.com/images/G/32/fee/PDFv4.pdf (Aug 1, 2025)
  MX — https://vender.amazon.com.mx/terminos/promocion (2026 费率)
  IN — https://sell.amazon.in/.../fba-faq (Pick&Pack + National weight handling)
  CA — Amazon.ca 公开 CAD 分档结构（Seller Central 明细页需登录；采用公开文档化的 envelope/standard/oversize 表）+ 3.5% surcharge
"""
from __future__ import annotations

from typing import Optional

from fba_fees_us import parse_dims_cm, parse_weight_kg, _lookup_ceil


def _out(site: str, currency: str, **kw) -> dict:
    base = {
        "fba_fee": None, "placement_fee": None, "size_tier": None,
        "shipping_weight": None, "dim_weight": None,
        "currency": currency, "site": site,
        "supported": True, "reason": None,
    }
    base.update(kw)
    return base


def _ceil_band(bands: list[tuple[float, float]], x: float) -> float:
    return _lookup_ceil(bands, x)


# ── SG ─────────────────────────────────────────────────────────────
# shipping weight = unit + packaging (g)

def estimate_sg(weight_text, dims_text, price=None) -> dict:
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out("SG", "S$")
    L, W, H = sorted(dims, reverse=True)
    ug = unit * 1000.0
    if L <= 20 and W <= 15 and H <= 1 and ug <= 100:
        tier, pack, bands = "small_envelope", 20, [(100, 2.70)]
    elif L <= 33 and W <= 23 and H <= 2.5 and ug <= 500:
        tier, pack = "standard_envelope", 40
        bands = [(100, 2.73), (250, 2.75), (500, 2.77)]
    elif L <= 33 and W <= 23 and H <= 5 and ug <= 1000:
        tier, pack, bands = "large_envelope", 40, [(1000, 2.81)]
    elif L <= 45 and W <= 34 and H <= 26 and ug <= 12000:
        tier, pack = "parcel", 100
        bands = [
            (250, 2.87), (500, 2.92), (1000, 2.99), (1500, 3.22), (2000, 3.45),
            (3000, 3.48), (4000, 4.01), (5000, 4.55), (6000, 5.17), (7000, 5.79),
            (8000, 6.41), (9000, 7.03), (10000, 7.65), (11000, 8.27), (12000, 8.89),
        ]
    elif L <= 61 and W <= 46 and H <= 46:
        tier, pack = "small_oversize", 240
        bands = [(1000, 5.68), (1250, 5.93), (1500, 6.20), (1750, 6.23), (2000, 6.76)]
    elif L <= 120 and W <= 60 and H <= 60:
        tier, pack = "standard_oversize", 240
        bands = [
            (1000, 8.00), (2000, 8.25), (3000, 8.52), (4000, 8.55), (5000, 9.08),
            (6000, 9.62), (7000, 10.24), (8000, 10.86), (9000, 11.48), (10000, 12.10),
            (15000, 12.72), (20000, 13.34), (25000, 13.96), (30000, 14.50),
        ]
    elif L <= 150:
        tier, pack = "large_oversize", 240
        bands = [
            (5000, 10.49), (10000, 11.11), (15000, 11.73),
            (20000, 12.35), (25000, 12.97), (30000, 13.59),
        ]
    else:
        return _out("SG", "S$", size_tier="not_eligible")
    ship_g = ug + pack
    fee = _ceil_band(bands, ship_g)
    return _out("SG", "S$", fba_fee=round(fee, 2), size_tier=tier,
                shipping_weight=round(ship_g / 1000, 3))


# ── MENA 共用尺寸档：SA / AE / EG ─────────────────────────────────

def _mena_tier(L, W, H, unit_kg) -> str:
    if L <= 20 and W <= 15 and H <= 1 and unit_kg <= 0.1:
        return "small_envelope"
    if L <= 33 and W <= 23 and H <= 2.5 and unit_kg <= 0.5:
        return "standard_envelope"
    if L <= 33 and W <= 23 and H <= 5 and unit_kg <= 1.0:
        return "large_envelope"
    if L <= 45 and W <= 34 and H <= 26 and unit_kg <= 12:
        return "standard_parcel"
    return "oversize"


def _mena_lookup(tier: str, unit_kg: float, low: bool,
                 tables: dict) -> float:
    bands = tables[tier]["low" if low else "high"]
    if tier == "oversize" and unit_kg > 30:
        base = bands[-1][1]
        return base + tables["over_extra"] * (unit_kg - 30)
    return _ceil_band(bands, unit_kg)


_SA_TABLES = {
    "small_envelope": {"low": [(0.1, 5.5)], "high": [(0.1, 7.5)]},
    "standard_envelope": {
        "low": [(0.1, 6.0), (0.2, 6.2), (0.5, 6.5)],
        "high": [(0.1, 8.0), (0.2, 8.2), (0.5, 8.5)],
    },
    "large_envelope": {"low": [(1.0, 7.0)], "high": [(1.0, 9.0)]},
    "standard_parcel": {
        "low": [
            (0.25, 7.2), (0.5, 7.5), (1, 8.0), (1.5, 8.5), (2, 9.0), (3, 10.0),
            (4, 11.0), (5, 12.0), (6, 13.0), (7, 14.0), (8, 15.0), (9, 16.0),
            (10, 17.0), (11, 18.0), (12, 19.0),
        ],
        "high": [
            (0.25, 9.2), (0.5, 9.5), (1, 10.0), (1.5, 11.5), (2, 12.0), (3, 13.0),
            (4, 14.0), (5, 15.0), (6, 16.0), (7, 17.0), (8, 18.0), (9, 19.0),
            (10, 20.0), (11, 21.0), (12, 22.0),
        ],
    },
    "oversize": {
        "low": [
            (1, 10), (2, 11), (3, 12), (4, 13), (5, 14), (6, 15), (7, 16),
            (8, 17), (9, 18), (10, 19), (15, 24), (20, 29), (25, 34), (30, 39),
        ],
        "high": [
            (1, 14), (2, 15), (3, 16), (4, 17), (5, 18), (6, 19), (7, 20),
            (8, 21), (9, 22), (10, 23), (15, 28), (20, 33), (25, 38), (30, 43),
        ],
    },
    "over_extra": 1.0,
}

_AE_TABLES = {
    "small_envelope": {"low": [(0.1, 5.5)], "high": [(0.1, 7.5)]},
    "standard_envelope": {
        "low": [(0.1, 6.0), (0.2, 6.2), (0.5, 6.5)],
        "high": [(0.1, 8.0), (0.2, 8.2), (0.5, 8.5)],
    },
    "large_envelope": {"low": [(1.0, 7.0)], "high": [(1.0, 7.5)]},
    "standard_parcel": {
        "low": [
            (0.25, 7.2), (0.5, 7.5), (1, 8.5), (1.5, 9.0), (2, 9.5), (3, 10.5),
            (4, 11.5), (5, 12.5), (6, 13.5), (7, 14.5), (8, 15.5), (9, 16.5),
            (10, 17.5), (11, 18.5), (12, 19.5),
        ],
        "high": [
            (0.25, 9.2), (0.5, 9.5), (1, 10.5), (1.5, 11.0), (2, 11.5), (3, 12.5),
            (4, 13.5), (5, 14.5), (6, 15.5), (7, 16.5), (8, 17.5), (9, 18.5),
            (10, 19.5), (11, 20.5), (12, 21.5),
        ],
    },
    "oversize": {
        "low": [
            (1, 10.5), (2, 11.5), (3, 12.5), (4, 13.5), (5, 14.5), (6, 15.5),
            (7, 16.5), (8, 17.5), (9, 18.5), (10, 19.5), (15, 24.5), (20, 29.5),
            (25, 34.5), (30, 39.5),
        ],
        "high": [
            (1, 12.5), (2, 13.5), (3, 14.5), (4, 15.5), (5, 16.5), (6, 17.5),
            (7, 18.5), (8, 19.5), (9, 20.5), (10, 21.5), (15, 26.5), (20, 31.5),
            (25, 36.5), (30, 41.5),
        ],
    },
    "over_extra": 1.0,
}

_EG_TABLES = {
    "small_envelope": {"low": [(0.1, 19.5)], "high": [(0.1, 24.5)]},
    "standard_envelope": {
        "low": [(0.1, 19.5), (0.25, 19.5), (0.5, 20.5)],
        "high": [(0.1, 24.5), (0.25, 24.5), (0.5, 25.5)],
    },
    "large_envelope": {"low": [(1.0, 21.0)], "high": [(1.0, 26.0)]},
    "standard_parcel": {
        "low": [
            (0.25, 19.5), (0.5, 20.5), (1, 21), (1.5, 22), (2, 22.5), (3, 23.5),
            (4, 24.5), (5, 25.5), (6, 26.5), (7, 27.5), (8, 28.5), (9, 29.5),
            (10, 30.5), (11, 31.5), (12, 32.5),
        ],
        "high": [
            (0.25, 24.5), (0.5, 25.5), (1, 26), (1.5, 27), (2, 27.5), (3, 28.5),
            (4, 29.5), (5, 30.5), (6, 31.5), (7, 32.5), (8, 33.5), (9, 34.5),
            (10, 35.5), (11, 36.5), (12, 37.5),
        ],
    },
    "oversize": {
        "low": [
            (1, 25), (2, 27), (3, 29), (4, 31), (5, 33), (6, 35), (7, 37),
            (8, 39), (9, 41), (10, 43), (15, 53), (20, 63), (25, 73), (30, 83),
        ],
        "high": [
            (1, 30), (2, 32), (3, 34), (4, 36), (5, 38), (6, 40), (7, 42),
            (8, 44), (9, 46), (10, 48), (15, 58), (20, 68), (25, 78), (30, 88),
        ],
    },
    "over_extra": 2.0,
}


def _estimate_mena(site, currency, threshold, tables, weight_text, dims_text, price):
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out(site, currency)
    L, W, H = sorted(dims, reverse=True)
    tier = _mena_tier(L, W, H, unit)
    low = price is not None and price <= threshold
    fee = _mena_lookup(tier, unit, low, tables)
    return _out(site, currency, fba_fee=round(fee, 2), size_tier=tier,
                shipping_weight=round(unit, 3))


def estimate_sa(w, d, p=None):
    return _estimate_mena("SA", "SAR ", 25, _SA_TABLES, w, d, p)


def estimate_ae(w, d, p=None):
    return _estimate_mena("AE", "AED ", 25, _AE_TABLES, w, d, p)


def estimate_eg(w, d, p=None):
    return _estimate_mena("EG", "E£", 350, _EG_TABLES, w, d, p)


# ── AU ─────────────────────────────────────────────────────────────

def estimate_au(weight_text, dims_text, price=None) -> dict:
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out("AU", "A$")
    L, W, H = sorted(dims, reverse=True)
    ug = unit * 1000.0
    low = price is not None and price < 13
    dim_kg = (L * W * H) / 4000.0

    if L <= 20 and W <= 15 and H <= 1 and ug <= 75:
        tier, ship_g = "small_envelope", ug
        fee = 3.64 if low else 4.55
    elif L <= 33 and W <= 23 and H <= 2.5 and ug <= 475:
        tier, ship_g = "standard_envelope", ug
        bands = (
            [(75, 3.67), (225, 4.35), (475, 4.70)] if low
            else [(75, 4.58), (225, 5.26), (475, 5.61)]
        )
        fee = _ceil_band(bands, ug)
    elif L <= 33 and W <= 23 and H <= 5 and ug <= 975:
        tier, ship_g = "large_envelope", ug
        bands = (
            [(225, 5.87), (475, 6.11), (975, 7.38)] if low
            else [(225, 6.78), (475, 7.02), (975, 8.29)]
        )
        fee = _ceil_band(bands, ug)
    elif L <= 45 and W <= 34 and H <= 20 and unit <= 12:
        tier = "parcel"
        ship_g = max(ug, dim_kg * 1000)
        bands = (
            [
                (250, 6.43), (500, 6.73), (1000, 8.31), (1500, 8.77), (2000, 8.90),
                (3000, 8.95), (4000, 9.27), (5000, 9.28), (6000, 10.19), (7000, 10.35),
                (8000, 10.35), (9000, 10.36), (10000, 10.36), (11000, 12.89), (12000, 13.02),
            ] if low else [
                (250, 7.34), (500, 7.64), (1000, 9.22), (1500, 9.68), (2000, 9.81),
                (3000, 9.86), (4000, 10.18), (5000, 10.19), (6000, 11.10), (7000, 11.26),
                (8000, 11.26), (9000, 11.27), (10000, 11.27), (11000, 13.80), (12000, 13.93),
            ]
        )
        fee = _ceil_band(bands, ship_g)
    elif L <= 61 and W <= 46 and H <= 46:
        tier = "small_oversize"
        ship_g = max(ug, dim_kg * 1000)
        bands = (
            [(1000, 9.03), (1250, 9.50), (1500, 9.74), (1750, 9.75), (2000, 9.75)]
            if low else
            [(1000, 9.94), (1250, 10.41), (1500, 10.65), (1750, 10.66), (2000, 10.66)]
        )
        if ship_g <= 2000:
            fee = _ceil_band(bands, ship_g)
        else:
            base = 9.77 if low else 10.68
            fee = base + 0.01 * max(0.0, ship_g / 1000 - 2.0)
    elif L <= 105 and W <= 60 and H <= 60 and unit <= 22:
        tier = "standard_oversize"
        ship_g = max(ug, dim_kg * 1000)
        bands = (
            [
                (1000, 11.39), (2000, 12.04), (3000, 12.13), (4000, 12.27), (5000, 12.50),
                (6000, 13.40), (7000, 13.55), (8000, 13.57), (9000, 13.61), (10000, 13.61),
                (15000, 14.95), (20000, 15.51), (22000, 15.51),
            ] if low else [
                (1000, 12.30), (2000, 12.95), (3000, 13.04), (4000, 13.18), (5000, 13.41),
                (6000, 14.31), (7000, 14.46), (8000, 14.48), (9000, 14.52), (10000, 14.52),
                (15000, 15.86), (20000, 16.42), (22000, 16.42),
            ]
        )
        fee = _ceil_band(bands, ship_g)
    else:
        return _out("AU", "A$", size_tier="oversize_heavy")
    return _out("AU", "A$", fba_fee=round(fee, 2), size_tier=tier,
                shipping_weight=round(ship_g / 1000, 3), dim_weight=round(dim_kg, 3))


# ── TR ─────────────────────────────────────────────────────────────

def estimate_tr(weight_text, dims_text, price=None) -> dict:
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out("TR", "₺")
    # 售价 < ₺300：固定 ₺25（与尺寸无关）
    if price is not None and price < 300:
        return _out("TR", "₺", fba_fee=25.0, size_tier="low_price_flat",
                    shipping_weight=round(unit, 3))
    L, W, H = sorted(dims, reverse=True)
    if L <= 20 and W <= 15 and H <= 1:
        fee, tier = 53.80, "small_envelope"
    elif L <= 33 and W <= 23 and H <= 2.5 and unit <= 0.5:
        fee = _ceil_band([(0.1, 53.80), (0.25, 54.32), (0.5, 55.11)], unit)
        tier = "standard_envelope"
    elif L <= 33 and W <= 23 and H <= 5 and unit <= 1:
        fee, tier = 57.18, "large_envelope"
    elif L <= 45 and W <= 34 and H <= 26 and unit <= 12:
        fee = _ceil_band([
            (0.25, 59.15), (0.5, 61.16), (1, 62.03), (1.5, 63.58), (2, 64.47),
            (3, 74.9), (4, 76.8), (5, 80.44), (6, 85.46), (7, 90.51),
            (8, 100.58), (9, 110.67), (10, 122.77), (11, 129.42), (12, 134.18),
        ], unit)
        tier = "standard_parcel"
    elif L <= 61 and W <= 46 and H <= 46 and unit <= 2:
        fee = _ceil_band([
            (1, 107.47), (1.25, 116.6), (1.5, 117.3), (1.75, 118.11), (2, 118.64),
        ], unit)
        tier = "small_oversize"
    elif L <= 120 and W <= 60 and H <= 60 and unit <= 30:
        fee = _ceil_band([
            (1, 129.21), (2, 139.7), (3, 142.24), (4, 137.39), (5, 136.15),
            (6, 134.55), (7, 139.25), (8, 144.97), (9, 154.31), (10, 163.93),
            (15, 181.56), (20, 203.15), (25, 214.75), (30, 249.12),
        ], unit)
        tier = "standard_oversize"
    else:
        fee = _ceil_band([
            (5, 204.61), (10, 243.25), (15, 311.28), (20, 326.11),
            (25, 338.43), (30, 367.01),
        ], unit)
        tier = "large_oversize"
    return _out("TR", "₺", fba_fee=round(fee, 2), size_tier=tier,
                shipping_weight=round(unit, 3))


# ── BR ─────────────────────────────────────────────────────────────

_BR_GE79 = [
    # max_kg -> (79-100, 100-120, 120-150, 150-200, 200+)
    (0.25, (11.95, 13.95, 15.95, 17.95, 19.90)),
    (0.50, (12.85, 15.00, 17.15, 19.30, 20.40)),
    (1.0, (13.45, 15.70, 17.95, 20.20, 21.40)),
    (2.0, (14.00, 16.35, 18.75, 21.10, 22.90)),
    (3.0, (14.95, 17.45, 19.95, 22.40, 23.90)),
    (4.0, (16.15, 18.85, 21.55, 24.20, 24.90)),
    (5.0, (17.00, 19.90, 22.75, 25.60, 25.90)),
    (6.0, (25.00, 30.00, 34.00, 38.00, 41.40)),
    (7.0, (26.00, 31.00, 35.00, 39.00, 41.90)),
    (8.0, (27.00, 32.00, 36.00, 40.00, 41.90)),
    (9.0, (28.00, 33.00, 37.00, 41.00, 41.90)),
    (10.0, (39.50, 46.00, 52.75, 59.00, 65.90)),
]
_BR_EXTRA = (3.05, 3.05, 3.05, 3.50, 4.00)


def estimate_br(weight_text, dims_text, price=None) -> dict:
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out("BR", "R$")
    L, W, H = dims
    dim = (L * W * H) / 6000.0
    ship = max(unit, dim) + 0.02  # +20g packaging
    p = price if price is not None else 100.0
    if p < 30:
        fee = 5.65
    elif p < 50:
        fee = 5.85
    elif p < 79:
        fee = 6.05
    else:
        if p < 100:
            col = 0
        elif p < 120:
            col = 1
        elif p < 150:
            col = 2
        elif p < 200:
            col = 3
        else:
            col = 4
        fee = None
        for lim, fees in _BR_GE79:
            if ship <= lim + 1e-9:
                fee = fees[col]
                break
        if fee is None:
            fee = _BR_GE79[-1][1][col] + _BR_EXTRA[col] * max(0.0, ship - 10)
    return _out("BR", "R$", fba_fee=round(fee, 2), size_tier="fba_core",
                shipping_weight=round(ship, 3), dim_weight=round(dim, 3))


# ── MX ─────────────────────────────────────────────────────────────
# 2026 rates IVA included; price bands: <150, 150-299, 299-499, >=499

def _mx_price_col(price: Optional[float]) -> int:
    p = 500.0 if price is None else price
    if p < 150:
        return 0
    if p < 299:
        return 1
    if p < 499:
        return 2
    return 3


def estimate_mx(weight_text, dims_text, price=None) -> dict:
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out("MX", "MX$")
    L, W, H = sorted(dims, reverse=True)
    col = _mx_price_col(price)
    # Sobre ≤ 38x27x2
    if L <= 38 and W <= 27 and H <= 2 and unit <= 0.4:
        bands = [
            (0.10, (27.00, 33.00, 49.00, 60.00)),
            (0.20, (27.20, 34.00, 50.00, 60.40)),
            (0.30, (27.40, 35.00, 51.00, 60.80)),
            (0.40, (27.60, 36.00, 52.00, 61.20)),
        ]
        for lim, fees in bands:
            if unit <= lim + 1e-9:
                fee = fees[col]
                break
        else:
            fee = (27.80, 37.00, 53.00, 61.50)[col]
        tier = "envelope"
    elif L <= 45 and W <= 35 and H <= 20:
        bands = [
            (0.10, (28.00, 33.00, 50.00, 61.80)),
            (0.20, (28.05, 34.00, 51.00, 63.00)),
            (0.30, (28.10, 35.00, 52.00, 64.00)),
            (0.40, (28.15, 36.00, 53.00, 66.00)),
            (0.50, (28.20, 37.00, 54.00, 67.00)),
            (0.60, (28.25, 37.50, 55.00, 68.30)),
            (0.70, (28.30, 38.00, 56.00, 69.60)),
            (0.80, (28.35, 38.50, 57.00, 71.00)),
            (0.90, (28.40, 39.00, 58.00, 72.00)),
            (1.00, (28.45, 39.50, 59.00, 72.70)),
        ]
        fee = None
        for lim, fees in bands:
            if unit <= lim + 1e-9:
                fee = fees[col]
                break
        if fee is None:
            base = (28.50, 40.00, 60.00, 72.80)[col]
            extra = (1.15, 1.15, 1.75, 1.50)[col]
            fee = base + extra * max(0.0, (unit - 1.0) / 0.25)
        tier = "standard"
    else:
        # 大件：首 kg + 递增（取公开表 0-1kg 档）
        base = (32.00, 40.00, 60.00, 75.00)[col]  # approx from grande 0-1kg 2026
        fee = base + 2.0 * max(0.0, unit - 1.0)
        tier = "large"
    return _out("MX", "MX$", fba_fee=round(fee, 2), size_tier=tier,
                shipping_weight=round(unit, 3))


# ── IN（FBA 履约部分 = Pick&Pack + National weight handling，不含 referral）──

def estimate_in(weight_text, dims_text, price=None) -> dict:
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out("IN", "₹")
    L, W, H = dims
    dim = (L * W * H) / 5000.0
    bill = max(unit, dim) + 0.1  # +100g packaging standard
    # Heavy & bulky heuristic
    girth = L + 2 * (W + H)
    oversize = unit > 22.5 or max(L, W, H) > 183 or girth > 300
    if oversize:
        pick = 50
        # First 12kg national ₹261 + ₹6/kg
        ship = 261 + 6 * max(0.0, bill - 12)
        tier = "oversize"
    else:
        pick = 11
        # National: first 500g ₹61; +500g to 1kg ₹25; +₹27/kg after 1kg; +₹12/kg to 5kg
        if bill <= 0.5:
            ship = 61
        elif bill <= 1.0:
            ship = 61 + 25
        else:
            ship = 61 + 25
            # each additional kg after 1kg: ₹27 up to... FAQ: after 1kg ₹27, up to 5kg ₹12
            extra = bill - 1.0
            if extra <= 4:  # up to 5kg total
                # simplified: ₹27 first addl kg then ₹12
                if extra <= 1:
                    ship += 27 * extra
                else:
                    ship += 27 + 12 * (extra - 1)
            else:
                ship += 27 + 12 * 4 + 12 * (extra - 4)
        tier = "standard"
    fee = pick + ship
    return _out("IN", "₹", fba_fee=round(fee, 0), size_tier=tier,
                shipping_weight=round(bill, 3), dim_weight=round(dim, 3))


# ── CA（公开 CAD 分档 + 3.5% fuel surcharge 2026-04-17）────────────

SURCHARGE_CA = 1.035

def estimate_ca(weight_text, dims_text, price=None) -> dict:
    unit = parse_weight_kg(weight_text)
    dims = parse_dims_cm(dims_text)
    if unit is None or dims is None or unit <= 0:
        return _out("CA", "CA$")
    L, W, H = sorted(dims, reverse=True)
    ug = unit * 1000.0
    # Envelope: ≤ 38x27x2 approx (Amazon CA envelope)
    if L <= 38 and W <= 27 and H <= 2 and ug <= 500:
        bands = [
            (100, 4.46), (200, 4.71), (300, 5.01), (400, 5.28), (500, 5.62),
        ]
        fee = _ceil_band(bands, ug)
        tier = "envelope"
        ship = ug
    elif L <= 45 and W <= 35 and H <= 20 and unit <= 9:
        if ug <= 1500:
            bands = [
                (100, 5.92), (200, 6.12), (300, 6.36), (400, 6.73), (500, 7.23),
                (600, 7.40), (700, 7.71), (800, 7.95), (900, 8.25), (1000, 8.49),
                (1100, 8.58), (1200, 8.84), (1300, 9.04), (1400, 9.29), (1500, 9.60),
            ]
            fee = _ceil_band(bands, ug)
        else:
            fee = 10.32 + 0.09 * max(0.0, (ug - 1500) / 100)
        tier = "standard"
        ship = ug
    elif L <= 61 and W <= 46 and H <= 46:
        fee = 15.43 + 0.46 * max(0.0, (ug - 500) / 500)
        tier = "small_oversize"
        ship = ug
    elif L <= 120 and W <= 60 and H <= 60:
        fee = 37.78 + 0.52 * max(0.0, (ug - 500) / 500)
        tier = "medium_oversize"
        ship = ug
    else:
        fee = 82.20 + 0.58 * max(0.0, (ug - 500) / 500)
        tier = "large_oversize"
        ship = ug
    # Low-Price FBA CA: ≤ CAD14 约减 CAD0.80
    if price is not None and price <= 14:
        fee = max(0.0, fee - 0.80)
    fee *= SURCHARGE_CA
    return _out("CA", "CA$", fba_fee=round(fee, 2), size_tier=tier,
                shipping_weight=round(ship / 1000, 3))


ESTIMATORS = {
    "SG": estimate_sg,
    "SA": estimate_sa,
    "AE": estimate_ae,
    "EG": estimate_eg,
    "AU": estimate_au,
    "TR": estimate_tr,
    "BR": estimate_br,
    "MX": estimate_mx,
    "IN": estimate_in,
    "CA": estimate_ca,
}
