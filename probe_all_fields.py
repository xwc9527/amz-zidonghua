"""
probe_all_fields.py — 全量探测 Amazon 详情页所有可提取字段
扫描所有样本HTML，输出完整字段清单（不遗漏任何数据块）
用法: python probe_all_fields.py [--file data/sample_xxx.html]
"""
import re, sys, json, os, glob
from collections import OrderedDict
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from bs4 import BeautifulSoup, NavigableString

SAMPLE_DIR = "data"
DEFAULT_FILES = [
    "data/sample_product.html",
    "data/sample_product2.html",
    "data/sample_de_detail.html",
    "data/sample_de_detail3.html",
]


def clean(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()[:300]


def extract_all_fields(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    fields = OrderedDict()

    # ── 1. 核心商品信息 ──
    g = OrderedDict()
    el = soup.select_one("#productTitle")
    g["title"] = clean(el.get_text()) if el else None

    el = soup.select_one("#bylineInfo")
    g["brand_text"] = clean(el.get_text()) if el else None
    g["brand_url"] = el.get("href", "")[:150] if el else None
    g["has_brand_store"] = bool(el and "/stores/" in el.get("href", ""))

    el = soup.select_one("input#ASIN, input[name='ASIN']")
    g["asin"] = el.get("value") if el else None

    fields["01_核心商品信息"] = g

    # ── 2. 价格 ──
    g = OrderedDict()
    el = soup.select_one("#corePriceDisplay_desktop_feature_div .a-price .a-offscreen")
    if not el:
        el = soup.select_one(".a-price .a-offscreen")
    g["price"] = clean(el.get_text()) if el else None

    el = soup.select_one(".a-text-price .a-offscreen, #listPrice .a-offscreen")
    g["original_price"] = clean(el.get_text()) if el else None

    el = soup.select_one(".savingsPercentage, #dealprice_savings .a-text-bold")
    g["discount_pct"] = clean(el.get_text()) if el else None

    el = soup.select_one("#couponBadgeRegularVpc, #vpcButton")
    g["coupon_text"] = clean(el.get_text()) if el else None
    g["has_coupon"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#vatMessage_feature_div")
    g["vat_message"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#tp_price_block_total_price_ww")
    g["twister_plus_price"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#priceblock_dealprice, #dealprice_feature_div")
    g["deal_price"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#dealBadge_feature_div")
    g["has_deal_badge"] = el is not None and bool(el.get_text(strip=True))

    fields["02_价格"] = g

    # ── 3. 评价与排名 ──
    g = OrderedDict()
    el = soup.select_one("#acrPopover .a-icon-alt, #acrPopover span.a-icon-alt")
    g["rating"] = clean(el.get_text()) if el else None

    el = soup.select_one("#acrCustomerReviewText")
    g["review_count"] = clean(el.get_text()) if el else None

    el = soup.select_one("#askATFLink span, #askATFLink")
    g["qa_count"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    # BSR - from prodDetails table
    bsr_raw = []
    for table in soup.select("#productDetails_feature_div table"):
        for row in table.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if th and td:
                label = th.get_text(strip=True)
                if "Best Sellers Rank" in label or "Bestseller-Rang" in label:
                    bsr_raw.append(clean(td.get_text()))

    # BSR - from detailBullets
    for li in soup.select("#detailBulletsWrapper_feature_div .a-list-item, #detailBullets_feature_div .a-list-item"):
        text = li.get_text(strip=True)
        if "Bestseller-Rang" in text or "Best Sellers Rank" in text:
            bsr_raw.append(clean(text))

    g["bsr_raw"] = bsr_raw[0] if bsr_raw else None

    # Badges
    el = soup.select_one(".ac-badge-wrapper, #acBadge_feature_div")
    g["is_amazons_choice"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#zeitgeistBadge_feature_div")
    g["bestseller_badge"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#socialProofingAsinFaceout_feature_div")
    g["social_proof"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    fields["03_评价与排名"] = g

    # ── 4. 媒体 ──
    g = OrderedDict()
    el = soup.select_one("#landingImage, #imgBlkFront")
    g["main_image_url"] = (el.get("data-old-hires") or el.get("src", ""))[:150] if el else None

    imgs = soup.select("#altImages .a-spacing-small img, #altImages li.item img")
    g["image_count"] = len(imgs)

    video_markers = soup.select('.vse-player-container, #vse-vw-dp-container, [class*="videoCount"]')
    g["has_video"] = len(video_markers) > 0

    el = soup.select_one("#ive-videos-for-this-product-widget_feature_div")
    g["has_product_videos_widget"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#rhapsodyARIngress_feature_div")
    g["has_3d_view"] = el is not None and bool(el.get_text(strip=True))

    fields["04_媒体"] = g

    # ── 5. 类目归属 ──
    g = OrderedDict()
    breadcrumbs = [a.get_text(strip=True) for a in soup.select("#wayfinding-breadcrumbs_feature_div a")]
    g["breadcrumbs"] = breadcrumbs if breadcrumbs else None

    fields["05_类目归属"] = g

    # ── 6. 卖点描述 ──
    g = OrderedDict()
    bullets = [li.get_text(strip=True) for li in soup.select("#feature-bullets .a-list-item") if li.get_text(strip=True)]
    g["bullet_count"] = len(bullets)
    g["bullets_sample"] = bullets[0][:100] if bullets else None

    el = soup.select_one("#aplus_feature_div")
    g["has_aplus"] = el is not None and len(el.get_text(strip=True)) > 20

    el = soup.select_one("#aplusBrandStory_feature_div")
    g["has_brand_story"] = el is not None and len(el.get_text(strip=True)) > 20

    el = soup.select_one("#productDescription_feature_div, #productDescription")
    g["has_description"] = el is not None and len(el.get_text(strip=True)) > 20

    fields["06_卖点描述"] = g

    # ── 7. 商品属性 (prodDetails 表格 / detailBullets) ──
    g = OrderedDict()

    # Format A: #productDetails_feature_div tables
    for table in soup.select("#productDetails_feature_div table"):
        for row in table.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if th and td:
                key = th.get_text(strip=True)
                val = td.get_text(strip=True)[:200]
                if key and val and "Bestseller" not in key and "Kundenbewertung" not in key and "Customer Reviews" not in key:
                    g[key] = val

    # Format B: #detailBullets (some DE products use this)
    for li in soup.select("#detailBulletsWrapper_feature_div .a-list-item, #detailBullets_feature_div .a-list-item"):
        text = li.get_text(strip=True)
        if ":" in text and "Bestseller" not in text and "Kundenbewertung" not in text:
            parts = text.split(":", 1)
            if len(parts) == 2:
                key = re.sub(r'[‎‏‎‏‪-‮\s]+', ' ', parts[0]).strip()
                val = re.sub(r'[‎‏‎‏‪-‮\s]+', ' ', parts[1]).strip()[:200]
                if key and val:
                    g[key] = val

    fields["07_商品属性"] = g

    # ── 8. 快速参数概览 (productOverview) ──
    g = OrderedDict()
    el = soup.select_one("#productOverview_feature_div")
    if el:
        for row in el.select("tr"):
            tds = row.select("td")
            if len(tds) >= 2:
                key = tds[0].get_text(strip=True)
                val = tds[1].get_text(strip=True)
                if key and val:
                    g[key] = val

    # voyagerAccordian (some DE products have important highlights here)
    el = soup.select_one("#voyagerAccordian_feature_div")
    if el:
        g["_voyager_highlights"] = clean(el.get_text())

    fields["08_快速参数概览"] = g

    # ── 9. 变体 ──
    g = OrderedDict()
    el = soup.select_one("#twister_feature_div")
    if el:
        labels = [l.get_text(strip=True) for l in el.select(".a-form-label, .inline-twister-dim-title-value-truncate")]
        g["variant_dimensions"] = [l for l in labels if l] or None
        opts = el.select("li[data-defaultasin], li[id*='color_name'], li[id*='size_name']")
        g["variant_option_count"] = len(opts)
    else:
        g["variant_dimensions"] = None
        g["variant_option_count"] = 0

    el = soup.select_one("#sizeChartV2Data_feature_div")
    g["has_size_chart"] = el is not None and bool(el.get_text(strip=True))

    fields["09_变体"] = g

    # ── 10. 配送与购买 ──
    g = OrderedDict()

    # 库存状态
    el = soup.select_one("#availability span, #availabilityInsideBuyBox_feature_div")
    g["availability"] = clean(el.get_text()) if el else None

    # 配送信息
    el = soup.select_one("#deliveryBlockMessage, #deliveryBlock_feature_div")
    g["delivery_message"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    # 运费 (从 delivery block 提取)
    shipping_fee = None
    el = soup.select_one("#deliveryBlockMessage")
    if el:
        text = el.get_text(strip=True)
        m = re.search(r'(?:Lieferung für|delivery|shipping)\s*([\d,.]+\s*[€$£]|[€$£]\s*[\d,.]+)', text, re.I)
        if m:
            shipping_fee = m.group(1).strip()
        elif re.search(r'(?:FREE|KOSTENLOS|Gratis|kostenlose)', text, re.I):
            shipping_fee = "FREE"
    g["shipping_fee"] = shipping_fee

    # 卖家/配送方 (核心: FBA vs FBM)
    merchant_text = None
    fulfillment_type = None

    # Method 1: #merchant-info
    el = soup.select_one("#merchant-info")
    if el and el.get_text(strip=True):
        merchant_text = clean(el.get_text())

    # Method 2: #merchantInfoFeature_feature_div
    if not merchant_text:
        el = soup.select_one("#merchantInfoFeature_feature_div")
        if el and el.get_text(strip=True):
            merchant_text = clean(el.get_text())

    # Method 3: .offer-display-feature-text (first one = merchant)
    if not merchant_text:
        els = soup.select(".offer-display-feature-text")
        if els:
            merchant_text = clean(els[0].get_text())

    # Method 4: #tabular-buybox
    if not merchant_text:
        el = soup.select_one("#tabular-buybox")
        if el:
            for row in el.select(".tabular-buybox-text"):
                t = row.get_text(strip=True)
                if t:
                    merchant_text = (merchant_text or "") + " | " + t

    g["merchant_info"] = merchant_text

    # 判定 FBA / FBM
    if merchant_text:
        mt = merchant_text.lower()
        if "fulfilled by amazon" in mt or "versand durch amazon" in mt:
            fulfillment_type = "FBA"
        elif "ships from amazon" in mt or "versand und verkauf" in mt:
            fulfillment_type = "FBA"
        elif "amazon" in mt and ("ships from" in mt or "versender" in mt or "versand" in mt):
            fulfillment_type = "FBA"
        else:
            fulfillment_type = "FBM"

    # 补充: offer-display 区域的 "Versender / Verkäufer" 信息
    offer_el = soup.select_one("#offerDisplayFeatures_desktop, #offer-display-features")
    if offer_el:
        offer_text = offer_el.get_text(strip=True)
        if "Amazon" in offer_text and ("Versender" in offer_text or "Ships" in offer_text):
            if fulfillment_type is None:
                fulfillment_type = "FBA"
        g["offer_display_raw"] = clean(offer_text)

    g["fulfillment_type"] = fulfillment_type

    # 卖家名称
    el = soup.select_one("#sellerProfileTriggerId")
    g["seller_name"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    # 数量选择
    el = soup.select_one("#quantityRelocate_feature_div, #quantity")
    g["has_quantity_selector"] = el is not None

    # 加购 / 立即购买按钮
    g["has_add_to_cart"] = soup.select_one("#add-to-cart-button") is not None
    g["has_buy_now"] = soup.select_one("#buy-now-button") is not None

    fields["10_配送与购买"] = g

    # ── 11. 退货与保障 ──
    g = OrderedDict()

    el = soup.select_one("#returnsInfoFeature_feature_div")
    g["return_policy"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#dynamicLegalWarrantyInfoFeature_feature_div")
    g["warranty_info"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#dynamicSecureTransactionFeature_feature_div")
    g["secure_transaction"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#dynamicPackageInfoFeature_feature_div")
    g["package_info"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    fields["11_退货与保障"] = g

    # ── 12. 营销标记 ──
    g = OrderedDict()

    el = soup.select_one("#promoPriceBlockMessage_feature_div")
    g["promotion_text"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#sims-fbt, #sims-fbt-form")
    g["has_fbt"] = el is not None and len(el.get_text(strip=True)) > 20

    el = soup.select_one("#HLCXComparisonTable, [id*='ComparisonTable']")
    g["has_comparison_table"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#dynamicGiftWrapInfoFeature_feature_div, #giftwrap_feature_div")
    g["has_gift_option"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#climatePledgeFriendlyATF_feature_div, #climatePledgeFriendlyBTF_feature_div")
    g["has_climate_pledge"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#sp_detail, #sponsoredProducts2-2_feature_div")
    g["has_sponsored_section"] = el is not None

    el = soup.select_one("#snsDetailPageFeature, #sns-feature, #snsAccordion")
    g["has_subscribe_save"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#olp_feature_div")
    g["other_sellers_text"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#dpFrequentlyReturnedMessage")
    g["frequently_returned_warning"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    fields["12_营销标记"] = g

    # ── 13. 品牌数据 ──
    g = OrderedDict()

    el = soup.select_one("#brandSnapshot_feature_div")
    g["brand_snapshot"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    fields["13_品牌数据"] = g

    # ── 14. EU/法规相关 ──
    g = OrderedDict()

    el = soup.select_one("#legalEUBtf_feature_div, #legalEUAtf_feature_div")
    g["eu_product_safety"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#energyEfficiency_feature_div")
    g["energy_efficiency"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#buffetServiceCard_feature_div")
    g["safety_resources"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#importantInformation_feature_div")
    g["important_info"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#cpsiaProductSafetyWarning-2_feature_div")
    g["safety_warning"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#productDocuments_feature_div")
    g["product_documents"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    fields["14_EU法规与安全"] = g

    # ── 15. 推荐与关联 ──
    g = OrderedDict()

    for el in soup.select('[id*="sims-simsContainer_feature_div"]'):
        if el.get_text(strip=True):
            g["has_similar_products"] = True
            break
    else:
        g["has_similar_products"] = False

    el = soup.select_one('[id*="sims-discoveryAndInspiration"]')
    g["has_discovery_inspiration"] = el is not None and bool(el.get_text(strip=True))

    el = soup.select_one("#postPurchaseWhatsInTheBox_MP_feature_div")
    g["whats_in_box"] = clean(el.get_text()) if el and el.get_text(strip=True) else None

    el = soup.select_one("#newerVersion_feature_div")
    g["has_newer_version"] = el is not None and bool(el.get_text(strip=True))

    fields["15_推荐与关联"] = g

    # ── 16. 评论区 ──
    g = OrderedDict()

    el = soup.select_one("#customer-reviews_feature_div")
    if el:
        g["has_reviews_section"] = True
        stars = {}
        for row in el.select(".cr-widget-Histogram tr, [data-hook='rating-filter-row']"):
            text = row.get_text(strip=True)
            m = re.search(r'(\d)\s*(?:Stern|Star).*?(\d+)%', text)
            if m:
                stars[f"{m.group(1)}_star"] = f"{m.group(2)}%"
        g["rating_distribution"] = stars if stars else None
    else:
        g["has_reviews_section"] = False
        g["rating_distribution"] = None

    fields["16_评论区"] = g

    # ── 17. 上架日期 ──
    g = OrderedDict()
    date_first = None

    # From prodDetails table
    for table in soup.select("#productDetails_feature_div table"):
        for row in table.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if th and td:
                label = th.get_text(strip=True)
                if "Date First" in label or "Verfügbarkeit" in label or "Im Angebot" in label:
                    date_first = td.get_text(strip=True)

    # From detailBullets
    if not date_first:
        for li in soup.select("#detailBulletsWrapper_feature_div .a-list-item, #detailBullets_feature_div .a-list-item"):
            text = li.get_text(strip=True)
            if "Im Angebot" in text or "Date First" in text or "Verfügbarkeit" in text:
                m = re.search(r':\s*(.+)', text)
                if m:
                    date_first = re.sub(r'[‎‏‪-‮]', '', m.group(1)).strip()

    g["date_first_available"] = date_first

    fields["17_上架日期"] = g

    # ── 18. 页面隐藏数据 ──
    g = OrderedDict()

    # JSON-LD
    scripts = soup.select('script[type="application/ld+json"]')
    g["json_ld_count"] = len(scripts)
    if scripts:
        try:
            d = json.loads(scripts[0].string)
            g["json_ld_type"] = d.get("@type") if isinstance(d, dict) else str(type(d))
        except:
            pass

    # twister data (variant ASIN mapping)
    for script in soup.select("script"):
        if script.string and "dimensionValuesDisplayData" in (script.string or ""):
            g["has_twister_data"] = True
            break
    else:
        g["has_twister_data"] = False

    # ASIN from hidden inputs
    parent_asin_el = soup.select_one("input[name='parentASIN'], input#parentAsin")
    g["parent_asin"] = parent_asin_el.get("value") if parent_asin_el else None

    fields["18_隐藏数据"] = g

    # ── 19. FOD (Featured Offer Display) ──
    g = OrderedDict()
    el = soup.select_one("#fod-cx-box, #fodcx_feature_div")
    if el and el.get_text(strip=True):
        text = el.get_text(strip=True)
        g["fod_message"] = clean(text)
        g["no_featured_offer"] = "Keine hervorgehobenen" in text or "No featured offer" in text.lower()
    else:
        g["fod_message"] = None
        g["no_featured_offer"] = False

    el = soup.select_one("#outOfStockBuyBox_feature_div")
    if el and el.get_text(strip=True):
        g["out_of_stock_message"] = clean(el.get_text())

    fields["19_FOD与库存"] = g

    return fields


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, default="", help="单个文件")
    parser.add_argument("--all", action="store_true", help="扫描所有默认样本")
    args = parser.parse_args()

    if args.file:
        files = [args.file]
    elif args.all:
        files = DEFAULT_FILES
    else:
        files = DEFAULT_FILES

    all_results = {}
    all_keys_by_group = {}

    for fp in files:
        if not os.path.exists(fp):
            print(f"[skip] {fp} 不存在")
            continue

        print(f"\n{'='*70}")
        print(f"  文件: {fp}")
        print(f"{'='*70}")

        result = extract_all_fields(fp)
        fname = os.path.basename(fp)
        all_results[fname] = result

        for group, fields in result.items():
            if group not in all_keys_by_group:
                all_keys_by_group[group] = set()
            for key, val in fields.items():
                if val is not None and val != False and val != 0 and val != [] and val != "":
                    all_keys_by_group[group].add(key)

            print(f"\n  {group}:")
            for key, val in fields.items():
                if val is None or val == False or val == 0 or val == [] or val == "":
                    indicator = "  ·"
                else:
                    indicator = "  ✓"
                    if isinstance(val, str) and len(val) > 80:
                        val = val[:80] + "..."
                print(f"  {indicator} {key}: {val}")

    # 汇总报告
    print(f"\n\n{'='*70}")
    print(f"  ═══ 跨样本字段汇总 ═══")
    print(f"{'='*70}")

    for group in sorted(all_keys_by_group.keys()):
        keys = sorted(all_keys_by_group[group])
        print(f"\n  {group} ({len(keys)} 个有效字段):")
        for key in keys:
            present_in = []
            for fname, result in all_results.items():
                if group in result:
                    val = result[group].get(key)
                    if val is not None and val != False and val != 0 and val != [] and val != "":
                        present_in.append(fname.replace("sample_", "").replace(".html", ""))
            print(f"    {key:<40} 出现于: {', '.join(present_in)}")

    # 输出 JSON
    out_file = "data/all_fields_inventory.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[输出] 完整字段清单: {out_file}")


if __name__ == "__main__":
    main()
