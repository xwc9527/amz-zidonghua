"""
detail_parser.py — 详情页字段解析与筛选（fetch_products / fetch_new_arrivals 共用）

从 fetch_products.py 抽取，签名显式传入 site，不依赖模块级全局变量。
"""
from __future__ import annotations

import json
import re
from datetime import datetime

from bs4 import BeautifulSoup

from fba_fees_us import estimate_fba_fees, parse_weight_lb, parse_dims_inches


_ORIGIN_LABEL_RE = re.compile(r"(?:country\s+of\s+origin|herkunftsland|原産国)\s*[:：]?\s*", re.I)
_DIRECTIONAL_CONTROLS_RE = re.compile(r"[\u200e\u200f\u202a-\u202e\u2066-\u2069]")
_SOCIAL_PROOF_SELECTORS = (
    "#socialProofingAsinFaceout_feature_div",
    "#social-proofing-faceout-title-tk_bought",
    "[data-csa-c-content-id='social-proofing-faceout-title-tk_bought']",
)


def _country_of_origin_from_row(row) -> str | None:
    cells = row.select("th, td")
    for index, cell in enumerate(cells):
        cell_text = _DIRECTIONAL_CONTROLS_RE.sub("", cell.get_text(" ", strip=True)).strip()
        match = _ORIGIN_LABEL_RE.search(cell_text)
        if not match:
            continue
        inline_value = (cell_text[:match.start()] + cell_text[match.end():]).strip(" :：")
        if inline_value:
            return inline_value
        for following in cells[index + 1:]:
            value = _DIRECTIONAL_CONTROLS_RE.sub("", following.get_text(" ", strip=True)).strip()
            if value and not _ORIGIN_LABEL_RE.fullmatch(value):
                return value

    text = _DIRECTIONAL_CONTROLS_RE.sub("", row.get_text(" ", strip=True)).strip()
    match = _ORIGIN_LABEL_RE.search(text)
    if match:
        value = (text[:match.start()] + text[match.end():]).strip(" :：")
        if value:
            return value
    return None


def _dynamic_image_best_url(raw: str) -> str | None:
    """data-a-dynamic-image 是 JSON: {url: [w, h], ...}；取宽度最大的一项。"""
    try:
        mapping = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(mapping, dict) or not mapping:
        return None
    best_url, best_w = None, -1
    for url, size in mapping.items():
        w = 0
        if isinstance(size, (list, tuple)) and size:
            try:
                w = int(size[0])
            except (TypeError, ValueError):
                w = 0
        if w > best_w:
            best_w, best_url = w, url
    return best_url


def _srcset_best_url(raw: str) -> str | None:
    """srcset: 'url1 300w, url2 600w' 或 'url1 1x, url2 2x'。
    解析每项的宽度(w)/像素密度(x)描述符取数值最大的一项，不假设书写顺序。"""
    if not raw:
        return None
    best_url, best_score = None, -1.0
    for part in raw.split(","):
        tokens = part.strip().split()
        if not tokens:
            continue
        url = tokens[0]
        score = 0.0
        if len(tokens) > 1:
            m = re.match(r"([\d.]+)\s*([wx])$", tokens[1], re.I)
            if m:
                try:
                    score = float(m.group(1))
                except ValueError:
                    score = 0.0
        if best_url is None or score > best_score:
            best_url, best_score = url, score
    return best_url


def extract_image_url(img_tag) -> str:
    """从榜单/搜索卡片的 <img> 标签按优先级提取最佳图片链接：
    data-a-dynamic-image（取最大分辨率）→ srcset（取最高密度）
    → data-src（懒加载真实图，需先于 src 避免拿到占位图）
    → src（过滤 data: 开头的占位图）。都没有则返回空串。"""
    if img_tag is None:
        return ""
    dyn = img_tag.get("data-a-dynamic-image")
    if dyn:
        url = _dynamic_image_best_url(dyn)
        if url:
            return url
    srcset = img_tag.get("srcset")
    if srcset:
        url = _srcset_best_url(srcset)
        if url:
            return url
    data_src = img_tag.get("data-src")
    if data_src and not data_src.startswith("data:"):
        return data_src
    src = img_tag.get("src") or ""
    if src and not src.startswith("data:"):
        return src
    return ""


def extract_detail_image_url(soup: BeautifulSoup) -> str:
    """详情页主图：优先 data-old-hires（Amazon 详情页专属的原图最高分辨率属性），
    其次走通用兜底链。找不到返回空串（调用方不应用空串覆盖已有的列表阶段图片）。"""
    img_el = soup.select_one("#landingImage, #imgBlkFront, #main-image")
    if img_el is None:
        return ""
    hires = img_el.get("data-old-hires")
    if hires:
        return hires
    return extract_image_url(img_el)


def parse_social_proof_count(raw: str | None) -> int | None:
    """把 Amazon 的月销量展示下限归一化为整数。

    示例：``50+ bought`` → 50，``1K+`` → 1000，``2.5K+`` → 2500。
    Amazon 给的是区间下限而非精确销量，因此字段语义是“至少售出”。
    """
    if not raw:
        return None
    text = _DIRECTIONAL_CONTROLS_RE.sub("", str(raw)).replace("\xa0", " ").strip()
    matches = list(re.finditer(
        r"(?<!\d)(\d+(?:[.,]\d+)?)\s*([KMB]|千|万)?\s*\+?",
        text,
        re.I,
    ))
    if not matches:
        return None
    # 日语/中文常写成“过去1个月购买500件”，销量数字位于月份数字之后。
    match = matches[-1]

    number_text = match.group(1)
    suffix = (match.group(2) or "").upper()
    if suffix:
        # 带 K/M/B 时逗号可能是本地化小数分隔符（例如 1,5K）。
        normalized = number_text.replace(",", ".")
        try:
            number = float(normalized)
        except ValueError:
            return None
        multiplier = {
            "K": 1_000,
            "M": 1_000_000,
            "B": 1_000_000_000,
            "千": 1_000,
            "万": 10_000,
        }.get(suffix, 1)
        return int(number * multiplier)

    # 无缩写时标点是千位分隔符（1,000 / 1.000）。
    digits = re.sub(r"[.,\s]", "", number_text)
    try:
        return int(digits)
    except ValueError:
        return None

DE_MONTHS = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4,
    "Mai": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}


def attach_normalized_dims(d: dict) -> dict:
    """从文本重量/尺寸写入可 SQL 筛选的数值字段（lb / inch，长≥宽≥高）。"""
    w = parse_weight_lb(d.get("item_weight"))
    if w is not None:
        d["weight_lb"] = round(w, 4)
    dims = parse_dims_inches(d.get("item_dimensions"))
    if dims is not None:
        d["dim_l_in"], d["dim_w_in"], d["dim_h_in"] = (
            round(dims[0], 4), round(dims[1], 4), round(dims[2], 4)
        )
    return d


def parse_detail_fields(html: str, site: str = "US") -> dict:
    """从详情页 HTML 提取补全字段。site 用于 FBA 费率估算。"""
    soup = BeautifulSoup(html, "lxml")
    d = {}
    site = (site or "US").upper()

    badge_blob = " ".join(
        el.get_text(" ", strip=True)
        for el in soup.select(
            ".a-badge, .a-badge-label, .a-badge-label-inner, "
            "[data-a-badge-type], #acBadge_feature_div, #zeitgeistBadge_feature_div, "
            "#badge_feature_div"
        )
    )
    badge_blob = f"{badge_blob} " + " ".join(
        el.get("data-a-badge-type", "")
        for el in soup.select("[data-a-badge-type]")
    )
    badge_blob_l = badge_blob.lower()
    d["is_amazon_choice"] = 1 if (
        "amazons-choice" in badge_blob_l
        or "amazon's choice" in badge_blob_l
        or "amazon choice" in badge_blob_l
        or "amazon\u304a\u3059\u3059\u3081" in badge_blob_l
    ) else 0
    d["is_bestseller"] = 1 if re.search(
        r"#\s*1\s+best\s+seller|best\s+seller\s+in|\u30d9\u30b9\u30c8\u30bb\u30e9\u30fc|\u58f2\u308c\u7b4b",
        badge_blob,
        re.I,
    ) else 0

    social_el = None
    for selector in _SOCIAL_PROOF_SELECTORS:
        social_el = soup.select_one(selector)
        if social_el:
            break
    if social_el:
        social_text = social_el.get_text(" ", strip=True)
        social_count = parse_social_proof_count(social_text)
        if social_text:
            d["social_proof"] = social_text
        if social_count is not None:
            d["social_proof_count"] = social_count

    # BSR
    bsr_section = (
        soup.select_one("#prodDetails")
        or soup.select_one("#detailBulletsWrapper_feature_div")
        or soup.select_one("#productDetails_db_sections")
    )
    if bsr_section:
        bsr_text = bsr_section.get_text(" ")
        bsr_matches = []
        for pat in [
            r"Nr\.\s*([\d\.]+)\s+in\s+(.+?)(?=\s*\(|\s{2,}|\s*#|\s*$)",
            r"#([\d,]+)\s+in\s+(.+?)(?=\s*\(|\s{2,}|\s*#|\s*$)",
        ]:
            for m in re.finditer(pat, bsr_text):
                rank_str = m.group(1).replace(".", "").replace(",", "")
                cat = m.group(2).strip().rstrip("( ,")
                if not cat or len(cat) < 2:
                    continue
                try:
                    bsr_matches.append((int(rank_str), cat))
                except ValueError:
                    pass
        seen = set()
        uniq = []
        for item in bsr_matches:
            if item in seen:
                continue
            seen.add(item)
            uniq.append(item)
        bsr_matches = uniq
        if bsr_matches:
            d["bsr_main_rank"] = bsr_matches[0][0]
            d["bsr_main_category"] = bsr_matches[0][1]
        if len(bsr_matches) > 1:
            d["bsr_sub_rank"] = bsr_matches[1][0]
            d["bsr_sub_category"] = bsr_matches[1][1]

    detail_rows = soup.select(
        "#detailBullets_feature_div li, "
        "#productDetails_techSpec_section_1 tr, "
        "#productDetails_detailBullets_sections1 tr, "
        "#prodDetails tr"
    )
    for row in detail_rows:
        text = row.get_text(" ", strip=True)

        if any(k in text for k in ["Date First Available", "Datum der Ersten",
                                     "Erstmals verfügbar", "発売日"]):
            dm = re.search(
                r"(\d{1,2})\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|"
                r"September|Oktober|November|Dezember)\s*(\d{4})",
                text,
            )
            if dm:
                try:
                    dt = datetime(int(dm.group(3)), DE_MONTHS[dm.group(2)], int(dm.group(1)))
                    d["date_first_available"] = dt.strftime("%Y-%m-%d")
                except (ValueError, KeyError):
                    pass
            if "date_first_available" not in d:
                em = re.search(
                    r"(January|February|March|April|May|June|July|August|September|"
                    r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
                    text,
                )
                if em:
                    try:
                        dt = datetime.strptime(
                            f"{em.group(1)} {em.group(2)} {em.group(3)}", "%B %d %Y"
                        )
                        d["date_first_available"] = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
            if "date_first_available" not in d:
                uk = re.search(
                    r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\s+(\d{4})",
                    text,
                )
                if uk:
                    try:
                        dt = datetime.strptime(
                            f"{uk.group(2)} {uk.group(1)} {uk.group(3)}", "%B %d %Y"
                        )
                        d["date_first_available"] = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
            if "date_first_available" not in d:
                jm = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", text)
                if jm:
                    try:
                        dt = datetime(int(jm.group(1)), int(jm.group(2)), int(jm.group(3)))
                        d["date_first_available"] = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

        if any(k in text for k in ["Item Weight", "Artikelgewicht", "商品の重量"]):
            # 含 Kilograms/Pounds 等英文复数；勿只用 kg（会被 Kilograms 内嵌字符干扰）
            wm = re.search(
                r"([\d,.]+)\s*(pounds?|ounces?|kilograms?|kilogramms?|kg|grams?|gramms?|g|lbs?|oz)\b",
                text, re.I,
            )
            if wm:
                d["item_weight"] = wm.group(0).strip()

        if any(k in text for k in ["Item Dimensions", "Produktabmessungen",
                                     "Artikelabmessungen", "Package Dimensions"]):
            # 兼容 "10 x 5 x 2 inches" 与 "23.62\"D x 11.61\"W x 32.28\"H"
            dim_m = re.search(
                r"([\d,.]+)\s*(?:\"\s*[DLWHdlwh])?\s*x\s*"
                r"([\d,.]+)\s*(?:\"\s*[DLWHdlwh])?\s*"
                r"(?:x\s*([\d,.]+)\s*(?:\"\s*[DLWHdlwh])?)?"
                r"(?:\s*(?:inches|cm|mm|Zoll|zoll|\"))?",
                text, re.I,
            )
            if dim_m:
                parts = [dim_m.group(1), dim_m.group(2)]
                if dim_m.group(3):
                    parts.append(dim_m.group(3))
                unit = " inches" if ("\"" in text or "inch" in text.lower()) else (
                    " cm" if "cm" in text.lower() else ""
                )
                d["item_dimensions"] = " x ".join(parts) + unit

        origin_value = _country_of_origin_from_row(row)
        if origin_value:
            if "country_of_origin" not in d:
                d["country_of_origin"] = origin_value
            continue

        if any(k in text for k in ["Country of Origin", "Herkunftsland", "原産国"]):
            val = None
            td = row.select_one("td")
            if td and td.get_text(strip=True):
                val = td.get_text(strip=True)
            if not val:
                spans = row.select("span.a-list-item, span.a-text-bold + span, span")
                texts = [s.get_text(strip=True) for s in spans if s.get_text(strip=True)]
                texts = [
                    t for t in texts
                    if t and not any(
                        k in t for k in ["Country of Origin", "Herkunftsland", "原産国", ":"]
                    )
                ]
                if texts:
                    val = texts[-1]
            if not val:
                parts = re.split(r"[:‏‎]+", text)
                if len(parts) >= 2:
                    val = parts[-1].strip()
            if val:
                d["country_of_origin"] = val

    variants = soup.select("#twister_feature_div li[data-defaultasin]")
    if variants:
        d["variant_option_count"] = len(variants)

    olp = soup.select_one("#olp_feature_div, #aod-offer-list")
    if olp:
        om = re.search(r"(\d+)\s+(?:new|neu|nouveau)", olp.get_text(), re.I)
        if om:
            d["other_sellers_count"] = int(om.group(1))

    delivery_el = soup.select_one("#mir-layout-DELIVERY_BLOCK, #deliveryBlockMessage")
    if delivery_el:
        dtxt = delivery_el.get_text(" ", strip=True)
        if re.search(r"\bFREE\b|Kostenlose|KOSTENLOS", dtxt, re.I):
            d["shipping_fee"] = "FREE"
            d["shipping_fee_value"] = 0.0
        else:
            fee_m = re.search(
                r"(?:für|for|:)\s*([\d,.]+)\s*(?:\xa0)?([€$£])|([€$£])\s*([\d,.]+)", dtxt
            )
            if fee_m:
                raw = (fee_m.group(1) or fee_m.group(4)).replace(",", ".")
                try:
                    d["shipping_fee_value"] = float(raw)
                    d["shipping_fee"] = fee_m.group(0).strip()
                except ValueError:
                    pass

    img_url = extract_detail_image_url(soup)
    if img_url:
        d["image_url"] = img_url

    attach_normalized_dims(d)

    price_for_fee = d.get("price")
    fees = estimate_fba_fees(site, d.get("item_weight"), d.get("item_dimensions"), price_for_fee)
    if fees.get("fba_fee") is not None:
        d["fba_fee"] = fees["fba_fee"]
    if fees.get("placement_fee") is not None:
        d["placement_fee"] = fees["placement_fee"]

    for sel in ("#merchant-info", "#merchantInfoFeature",
                ".offer-display-feature-text", "#tabular-buybox"):
        mel = soup.select_one(sel)
        if mel:
            mtxt = mel.get_text(" ", strip=True)
            if re.search(
                r"Fulfilled by Amazon|Versand durch Amazon|Expédié par Amazon|Amazonが発送|"
                r"Ships from Amazon\.com|Ships from and sold by Amazon|"
                r"Verkauf und Versand durch Amazon|Amazon\.de|"
                r"Amazon\.co\.jpが発送|Amazon\.co\.uk",
                mtxt, re.I,
            ):
                d["fulfillment_type"] = "FBA"
            else:
                d["fulfillment_type"] = "FBM"
            break

    return d


def check_detail_filters(detail: dict, filters: dict) -> bool:
    """检查详情页字段是否满足筛选条件。返回 True=通过，False=不符合。"""
    if not filters:
        return True

    # 重量/尺寸筛选依赖标准化字段；调用方若未 attach 则在此补齐
    need_norm = (
        filters.get("weight_min") or filters.get("weight_max")
        or filters.get("dim_l") or filters.get("dim_w") or filters.get("dim_h")
    )
    if need_norm and (
        ("weight_lb" not in detail and detail.get("item_weight"))
        or ("dim_l_in" not in detail and detail.get("item_dimensions"))
    ):
        attach_normalized_dims(detail)

    def _range_check(val, fmin_key, fmax_key):
        fmin = filters.get(fmin_key, 0) or 0
        fmax = filters.get(fmax_key, 0) or 0
        if not fmin and not fmax:
            return True
        if val is None:
            return False
        if fmin and val < fmin:
            return False
        if fmax and val > fmax:
            return False
        return True

    if not _range_check(detail.get("bsr_main_rank"), "bsr_main_min", "bsr_main_max"):
        return False
    if not _range_check(detail.get("bsr_sub_rank"), "bsr_sub_min", "bsr_sub_max"):
        return False
    if not _range_check(detail.get("variant_option_count"), "variant_min", "variant_max"):
        return False
    if not _range_check(detail.get("other_sellers_count"), "sellers_min", "sellers_max"):
        return False
    if not _range_check(detail.get("social_proof_count"), "social_proof_min", "social_proof_max"):
        return False

    # 使用 attach_normalized_dims 已写入的标准化数值，避免重复解析且与 parse_weight_lb/parse_dims_inches 不一致
    if not _range_check(detail.get("weight_lb"), "weight_min", "weight_max"):
        return False

    dl = filters.get("dim_l", 0) or 0
    dw = filters.get("dim_w", 0) or 0
    dh = filters.get("dim_h", 0) or 0
    if dl or dw or dh:
        l_in, w_in, h_in = detail.get("dim_l_in"), detail.get("dim_w_in"), detail.get("dim_h_in")
        if l_in is None or w_in is None or h_in is None:
            return False
        if dl and l_in > dl:
            return False
        if dw and w_in > dw:
            return False
        if dh and h_in > dh:
            return False

    if not _range_check(detail.get("fba_fee"), "fba_fee_min", "fba_fee_max"):
        return False

    ft = filters.get("fulfillment_type", "") or ""
    if ft:
        if not detail.get("fulfillment_type"):
            return False
        if detail["fulfillment_type"] != ft:
            return False

    country = filters.get("country", "") or ""
    if country:
        if not detail.get("country_of_origin"):
            return False
        if country.lower() not in detail["country_of_origin"].lower():
            return False

    if filters.get("amazons_choice") and detail.get("is_amazon_choice") != 1:
        return False
    if filters.get("bestseller") and detail.get("is_bestseller") != 1:
        return False

    date_range = filters.get("date_range", "") or ""
    if date_range:
        if not detail.get("date_first_available"):
            return False
        try:
            dfa = datetime.strptime(detail["date_first_available"], "%Y-%m-%d")
            if date_range == "custom":
                df = filters.get("date_from", "") or ""
                dt = filters.get("date_to", "") or ""
                if df and dfa < datetime.strptime(df, "%Y-%m-%d"):
                    return False
                if dt and dfa > datetime.strptime(dt, "%Y-%m-%d"):
                    return False
            else:
                days = int(date_range)
                if (datetime.now() - dfa).days > days:
                    return False
        except (ValueError, TypeError):
            return False

    return True
