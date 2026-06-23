"""探测 Amazon 商品详情页所有可提取字段"""
import re, sys, json
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from bs4 import BeautifulSoup

with open("data/sample_product.html", "r", encoding="utf-8") as f:
    html = f.read()
soup = BeautifulSoup(html, "html.parser")
data = {}

# 1. 标题
el = soup.select_one("#productTitle")
data["title"] = el.get_text(strip=True) if el else None

# 2. 品牌
el = soup.select_one("#bylineInfo")
data["brand_raw"] = el.get_text(strip=True) if el else None
if el:
    data["brand_url"] = el.get("href", "")[:120]

# 3. 价格
el = soup.select_one("#corePriceDisplay_desktop_feature_div .a-price .a-offscreen")
if not el:
    el = soup.select_one(".a-price .a-offscreen")
data["price"] = el.get_text(strip=True) if el else None

el = soup.select_one(".a-text-price .a-offscreen")
data["original_price"] = el.get_text(strip=True) if el else None

el = soup.select_one(".savingsPercentage")
data["discount_pct"] = el.get_text(strip=True) if el else None

# 4. 评分
el = soup.select_one("#acrPopover .a-icon-alt")
data["rating"] = el.get_text(strip=True) if el else None

el = soup.select_one("#acrCustomerReviewText")
data["review_count"] = el.get_text(strip=True) if el else None

# 5. 图片
el = soup.select_one("#landingImage") or soup.select_one("#imgBlkFront")
data["main_image"] = (el.get("data-old-hires") or el.get("src", ""))[:120] if el else None
imgs = soup.select("#altImages .a-spacing-small img, #altImages li.item img")
data["image_count"] = len(imgs)

# 6. 视频
video_markers = soup.select('.vse-player-container, #vse-vw-dp-container, [class*="videoCount"], .a-icon-vse')
data["has_video"] = len(video_markers) > 0

# 7. 面包屑
crumb_el = soup.select_one("#wayfinding-breadcrumbs_feature_div")
if crumb_el:
    crumbs = [a.get_text(strip=True) for a in crumb_el.select("a")]
    data["breadcrumbs"] = crumbs
else:
    data["breadcrumbs"] = None

# 8. Bullet Points
bullets = soup.select("#feature-bullets .a-list-item")
bp = [b.get_text(strip=True) for b in bullets if b.get_text(strip=True) and "Make sure this" not in b.get_text()]
data["bullet_point_count"] = len(bp)
data["bullet_points_sample"] = bp[:2] if bp else []

# 9. Product Details (detailBullets)
details = {}
for li in soup.select("#detailBulletsWrapper_feature_div li, #detailBullets_feature_div li"):
    spans = li.select(".a-text-bold")
    if spans:
        key = spans[0].get_text(strip=True).strip("‎‏ :：")
        val_parts = [s.get_text(strip=True) for s in li.select("span") if s not in spans]
        val = " ".join(val_parts).strip()
        if key and val and key != val:
            details[key] = val[:200]
data["detail_bullets"] = details

# 10. Product Details table (prodDetTable + productDetails sections)
table_details = {}
for tr in soup.select("table.prodDetTable tr, #productDetails_detailBullets_sections1 tr, #productDetails_techSpec_section_1 tr"):
    th = tr.select_one("th")
    td = tr.select_one("td")
    if th and td:
        key = th.get_text(strip=True)
        val = td.get_text(" ", strip=True)
        if key and val and "Customer Reviews" not in key:
            table_details[key] = val[:200]
data["product_details_table"] = table_details

# 11. BSR - from prodDetTable or detailBullets
bsr_list = []
for tr in soup.select("table.prodDetTable tr, #productDetails_detailBullets_sections1 tr"):
    th = tr.select_one("th")
    td = tr.select_one("td")
    if th and td and "Best Sellers Rank" in th.get_text():
        txt = td.get_text(" ", strip=True)
        ranks = re.findall(r"#([\d,]+)\s+in\s+([^\(#\n]+)", txt)
        for rank_num, cat_name in ranks:
            bsr_list.append({"rank": int(rank_num.replace(",", "")), "category": cat_name.strip()})
for li in soup.select("#detailBulletsWrapper_feature_div li"):
    txt = li.get_text(" ", strip=True)
    if "Best Sellers Rank" in txt:
        ranks = re.findall(r"#([\d,]+)\s+in\s+([^\(#\n]+)", txt)
        for rank_num, cat_name in ranks:
            bsr_list.append({"rank": int(rank_num.replace(",", "")), "category": cat_name.strip()})
data["bsr"] = bsr_list

# 12. ASIN (hidden input fallback)
asin_input = soup.select_one("input#ASIN")
data["asin"] = asin_input["value"] if asin_input else None

# 13. 关键日期
data["date_first_available"] = details.get("Date First Available") or table_details.get("Date First Available")

# 13. 变体
variant_el = soup.select_one("#twisterContainer")
if variant_el:
    labels = [l.get_text(strip=True).rstrip(":") for l in variant_el.select(".a-form-label")]
    data["variant_types"] = labels
    options = variant_el.select("li[data-defaultasin], li[id^='color_name_'], li[id^='size_name_']")
    data["variant_option_count"] = len(options)
else:
    data["variant_types"] = []
    data["variant_option_count"] = 0

# 14. A+ Content
aplus = soup.select_one("#aplus, #aplus_feature_div, #aplusProductDescription_feature_div")
data["has_aplus"] = aplus is not None

# 15. Coupon
coupon = soup.select_one("#couponBadgeRegularVpc, #vpcButton")
data["has_coupon"] = coupon is not None
if coupon:
    data["coupon_text"] = coupon.get_text(strip=True)[:100]

# 16. Badges
data["is_amazons_choice"] = soup.select_one('.ac-badge-wrapper, [data-a-badge-type="amazons-choice"]') is not None
data["is_bestseller_badge"] = soup.select_one("#zeitgeistBadge_feature_div, .p13n-best-seller-badge") is not None

# 17. 卖家
seller = soup.select_one("#sellerProfileTriggerId")
data["seller_name"] = seller.get_text(strip=True) if seller else None

buybox_rows = soup.select("#tabular-buybox .tabular-buybox-text")
data["buybox_info"] = [r.get_text(strip=True) for r in buybox_rows]

# 18. 库存
avail = soup.select_one("#availability span")
data["availability"] = avail.get_text(strip=True) if avail else None

# 19. Subscribe & Save
data["has_subscribe_save"] = soup.select_one("#snsDetailPageFeature, #sns-base-price, #sns-price-block") is not None

# 20. Climate Pledge
data["climate_pledge"] = soup.select_one("#climatePledgeFriendly") is not None

# 21. FBT
data["has_fbt"] = soup.select_one("#sims-fbt") is not None

# 22. Q&A
qa = soup.select_one("#askATFLink span")
data["qa_count"] = qa.get_text(strip=True) if qa else None

# 23. 商品描述
desc = soup.select_one("#productDescription")
data["has_description"] = desc is not None
if desc:
    data["description_len"] = len(desc.get_text(strip=True))

# 24. 尺寸重量
dim_keys = ["Package Dimensions", "Product Dimensions", "Item Dimensions L x W", "Item Dimensions  LxWxH", "Item Dimensions LxWxH"]
data["dimensions"] = next((details.get(k) or table_details.get(k) for k in dim_keys if details.get(k) or table_details.get(k)), None)
data["weight"] = details.get("Item Weight") or table_details.get("Item Weight")

# 25. 制造商/产地
data["manufacturer"] = details.get("Manufacturer") or table_details.get("Manufacturer")
data["country_of_origin"] = details.get("Country of Origin") or table_details.get("Country of Origin")

# 27. Brand Store
store_link = soup.select_one('#bylineInfo[href*="/stores/"]')
data["has_brand_store"] = store_link is not None

# 28. 配送
delivery = soup.select_one("#mir-layout-DELIVERY_BLOCK-block")
data["delivery_info"] = delivery.get_text(" ", strip=True)[:150] if delivery else None

# 29. 比较表
data["has_comparison_table"] = soup.select_one("#HLCXComparisonTable") is not None

# 30. 赞助
data["has_sponsored_section"] = soup.select_one("#sp_detail, #sp_detail2") is not None

# 31. 购买选项数量 (Other Sellers)
other_sellers = soup.select("#aod-offer, #olp-new .a-row")
data["other_sellers_count"] = len(other_sellers)
new_used = soup.select_one("#olp_feature_div, #usedAndNew")
data["new_used_offers"] = new_used.get_text(strip=True)[:100] if new_used else None

# 32. 促销信息
promo = soup.select_one("#promoPriceBlockMessage_feature_div, #poPromo498_feature_div")
data["has_promotion"] = promo is not None

# 33. gift wrap
gift = soup.select_one("#gift-wrap, #giftwrap_feature_div")
data["has_gift_option"] = gift is not None

# 34. returns policy
returns = soup.select_one("#productSupportAndReturnPolicy-return_policy_feature_div")
data["has_return_policy"] = returns is not None

# ── Output ──
print("=" * 60)
print(f"页面大小: {len(html):,} bytes")
print("=" * 60)

for k, v in data.items():
    if isinstance(v, list) and len(v) > 4:
        print(f"\n{k}: [{len(v)} items]")
        for item in v[:3]:
            print(f"  - {item}")
    elif isinstance(v, dict) and len(v) > 3:
        print(f"\n{k}: {{{len(v)} keys}}")
        for dk, dv in list(v.items())[:8]:
            print(f"  {dk}: {dv}")
    else:
        print(f"{k}: {v}")
