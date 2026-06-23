"""生成 Amazon 商品详情页字段映射表 Excel"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

wb = openpyxl.Workbook()
ws = wb.active
ws.title = "商品详情页字段映射"

header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
header_font = Font(name="微软雅黑", size=11, bold=True, color="FFFFFF")
group_fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
group_font = Font(name="微软雅黑", size=11, bold=True, color="1F4E79")
normal_font = Font(name="微软雅黑", size=10)
thin_border = Border(
    left=Side(style="thin", color="D0D0D0"),
    right=Side(style="thin", color="D0D0D0"),
    top=Side(style="thin", color="D0D0D0"),
    bottom=Side(style="thin", color="D0D0D0"),
)

headers = ["分类", "英文字段名", "中文字段名", "CSS选择器 / 提取方式", "示例值", "出现频率", "存储建议"]
ws.append(headers)
for col in range(1, 8):
    cell = ws.cell(row=1, column=col)
    cell.fill = header_fill
    cell.font = header_font
    cell.alignment = Alignment(horizontal="center", vertical="center")

rows = [
    # ── 核心商品信息 ──
    ["核心商品信息", "asin", "商品编号", "input#ASIN", "B0GWFN7Y8T", "6/6", "TEXT PK"],
    ["", "title", "商品标题", "#productTitle", "Pocket Hose Ballistic 50 FT...", "6/6", "TEXT"],
    ["", "brand", "品牌", "#bylineInfo", "Visit the Pocket Hose Store", "6/6", "TEXT"],
    ["", "brand_url", "品牌店铺链接", "#bylineInfo[href]", "/stores/PocketHose/page/...", "6/6", "TEXT"],
    ["", "has_brand_store", "有品牌旗舰店", '#bylineInfo[href*="/stores/"]', "True", "3/6", "INT 0/1"],

    # ── 价格 ──
    ["价格", "price", "现价", ".a-price .a-offscreen", "$59.99", "5/6", "REAL"],
    ["", "original_price", "原价", ".a-text-price .a-offscreen", "$69.99", "4/6", "TEXT"],
    ["", "discount_pct", "折扣比例", ".savingsPercentage", "-14%", "4/6", "TEXT"],
    ["", "has_coupon", "有优惠券", "#couponBadgeRegularVpc", "False", "0/6", "INT 0/1"],
    ["", "coupon_text", "优惠券内容", "同上 .get_text()", "Save 5% with coupon", "0/6", "TEXT"],

    # ── 评价与排名 ──
    ["评价与排名", "rating", "评分", "#acrPopover .a-icon-alt", "4.2 out of 5 stars", "6/6", "REAL"],
    ["", "review_count", "评论数", "#acrCustomerReviewText", "15,195", "6/6", "INTEGER"],
    ["", "bsr_main_rank", "BSR大类排名", "prodDetTable #N in Category", "#19", "6/6", "INTEGER"],
    ["", "bsr_main_category", "BSR大类名称", "同上", "Patio, Lawn & Garden", "6/6", "TEXT"],
    ["", "bsr_sub_rank", "BSR子类排名", "同上（第二条）", "#2", "6/6", "INTEGER"],
    ["", "bsr_sub_category", "BSR子类名称", "同上", "Garden Hoses", "6/6", "TEXT"],
    ["", "is_amazons_choice", "Amazon精选", ".ac-badge-wrapper", "False", "0/6", "INT 0/1"],
    ["", "is_bestseller", "畅销标记", "#zeitgeistBadge_feature_div", "True", "2/6", "INT 0/1"],
    ["", "qa_count", "问答数", "#askATFLink span", "42 answered questions", "0/6", "TEXT"],

    # ── 媒体 ──
    ["媒体", "main_image", "主图URL", "#landingImage data-old-hires", "https://m.media-amazon.com/...", "6/6", "TEXT"],
    ["", "image_count", "图片数量", "#altImages img count", "7", "6/6", "INTEGER"],
    ["", "has_video", "有视频", ".vse-player-container", "True", "1/6", "INT 0/1"],

    # ── 类目与内容 ──
    ["类目与内容", "breadcrumbs", "面包屑路径", "#wayfinding-breadcrumbs a", "Patio > Gardening > Watering", "6/6", "TEXT JSON"],
    ["", "bullet_points", "五点描述", "#feature-bullets .a-list-item", "5条完整文本", "6/6", "TEXT JSON"],
    ["", "bullet_point_count", "卖点条数", "同上 count", "5", "6/6", "INTEGER"],
    ["", "has_aplus", "有A+页面", "#aplus_feature_div", "True", "6/6", "INT 0/1"],
    ["", "has_description", "有商品描述", "#productDescription", "True", "2/6", "INT 0/1"],

    # ── 变体 ──
    ["变体", "variant_types", "变体维度", "#twisterContainer .a-form-label", '["Color", "Size"]', "0/6", "TEXT JSON"],
    ["", "variant_option_count", "变体选项数", "#twisterContainer li[data-defaultasin]", "5", "0/6", "INTEGER"],

    # ── 购买与配送 ──
    ["购买与配送", "seller_name", "卖家名称", "#sellerProfileTriggerId", "BulbHead", "1/6", "TEXT"],
    ["", "buybox_info", "购买框信息", "#tabular-buybox", "Ships from Amazon / Sold by X", "0/6", "TEXT JSON"],
    ["", "availability", "库存状态", "#availability span", "In Stock", "6/6", "TEXT"],
    ["", "delivery_info", "配送信息", "#mir-layout-DELIVERY_BLOCK-block", "FREE delivery Monday", "0/6", "TEXT"],
    ["", "has_subscribe_save", "有订阅省", "#snsDetailPageFeature", "False", "0/6", "INT 0/1"],
    ["", "other_sellers_count", "其他卖家数", "#aod-offer count", "0", "0/6", "INTEGER"],
    ["", "new_used_offers", "新旧报价", "#olp_feature_div", "3 new from $55.99", "0/6", "TEXT"],

    # ── 营销标记 ──
    ["营销标记", "has_promotion", "有促销", "#promoPriceBlockMessage_feature_div", "True", "2/6", "INT 0/1"],
    ["", "has_fbt", "有经常一起买", "#sims-fbt", "False", "0/6", "INT 0/1"],
    ["", "has_comparison_table", "有对比表", "#HLCXComparisonTable", "False", "0/6", "INT 0/1"],
    ["", "has_gift_option", "有礼品选项", "#giftwrap_feature_div", "True", "2/6", "INT 0/1"],
    ["", "climate_pledge", "气候友好认证", "#climatePledgeFriendly", "False", "0/6", "INT 0/1"],
    ["", "has_sponsored_section", "有赞助推荐", "#sp_detail", "False", "0/6", "INT 0/1"],

    # ── 高频商品属性 ──
    ["高频商品属性", "color", "颜色", "prodDetTable → Color", "Red", "6/6", "TEXT"],
    ["", "material_type", "材质", "prodDetTable → Material Type", "Stainless Steel", "4/6", "TEXT"],
    ["", "item_weight", "商品重量", "prodDetTable → Item Weight", "0.81 Pounds", "3/6", "TEXT"],
    ["", "item_dimensions", "商品尺寸", "prodDetTable → Item Dimensions *", '300"L x 0.75"W', "5/6", "TEXT"],
    ["", "capacity", "容量", "prodDetTable → Capacity", "20 Fluid Ounces", "4/6", "TEXT"],
    ["", "size", "尺寸规格", "prodDetTable → Size", "20 Ounces", "3/6", "TEXT"],
    ["", "wattage", "功率", "prodDetTable → Wattage", "1800 Watts", "3/6", "TEXT"],
    ["", "power_source", "电源类型", "prodDetTable → Power Source", "Corded Electric", "2/6", "TEXT"],
    ["", "manufacturer", "制造商", "prodDetTable → Manufacturer", "Bulbhead", "5/6", "TEXT"],
    ["", "model_number", "型号", "prodDetTable → Model Number", "C09125", "5/6", "TEXT"],
    ["", "upc", "通用产品代码", "prodDetTable → UPC", "097298700637", "4/6", "TEXT"],
    ["", "unit_count", "件数", "prodDetTable → Unit Count", "1.0 Count", "6/6", "TEXT"],
    ["", "included_components", "包含组件", "prodDetTable → Included Components", "Lid", "6/6", "TEXT"],
    ["", "country_of_origin", "产地", "prodDetTable → Country of Origin", "China", "0/6", "TEXT"],
    ["", "date_first_available", "上架日期", "prodDetTable → Date First Available", "March 15, 2025", "0/6", "TEXT"],
    ["", "warranty_description", "保修说明", "prodDetTable → Warranty Description", "1 Year Limited", "1/6", "TEXT"],
    ["", "voltage", "电压", "prodDetTable → Voltage", "120 Volts", "1/6", "TEXT"],
    ["", "noise_level", "噪音", "prodDetTable → Noise", "48 dB", "1/6", "TEXT"],
    ["", "required_assembly", "需要组装", "prodDetTable → Required Assembly", "Yes", "1/6", "TEXT"],
    ["", "product_care", "护理说明", "prodDetTable → Product Care Instructions", "Hand Wash Only", "2/6", "TEXT"],
    ["", "floor_area", "适用面积", "prodDetTable → Floor Area", "450 Square Feet", "2/6", "TEXT"],

    # ── 兜底 ──
    ["动态属性兜底", "attributes_json", "其余属性(JSON)", "prodDetTable 全量 - 已提取字段", '{"Door Style":"French",...}', "6/6", "TEXT JSON"],
]

r = 2
for row in rows:
    ws.append(row)
    for col in range(1, 8):
        cell = ws.cell(row=r, column=col)
        cell.font = normal_font
        cell.border = thin_border
        cell.alignment = Alignment(vertical="center", wrap_text=(col in (4, 5)))
    if row[0]:
        for col in range(1, 8):
            cell = ws.cell(row=r, column=col)
            cell.fill = group_fill
            if col == 1:
                cell.font = group_font
            else:
                cell.font = Font(name="微软雅黑", size=10, bold=True)
    r += 1

ws.column_dimensions["A"].width = 16
ws.column_dimensions["B"].width = 26
ws.column_dimensions["C"].width = 16
ws.column_dimensions["D"].width = 40
ws.column_dimensions["E"].width = 35
ws.column_dimensions["F"].width = 10
ws.column_dimensions["G"].width = 18
ws.freeze_panes = "A2"

ws2 = wb.create_sheet("说明")
ws2.column_dimensions["A"].width = 18
ws2.column_dimensions["B"].width = 60
info = [
    ["Amazon 商品详情页字段映射表", ""],
    ["", ""],
    ["数据来源", "实际抓取6个不同品类商品详情页（水管/水杯/烤箱/风扇/电视柜/便携空调）"],
    ["出现频率", "N/6 表示在6个样品中有N个包含该字段"],
    ["存储建议", "推荐的SQLite列类型"],
    ["", ""],
    ["字段总数", "63个固定列 + 1个JSON兜底列"],
    ["高频属性", "从prodDetTable提取的出现2次以上的通用属性，独立建列"],
    ["动态属性", "prodDetTable中品类专属字段统一存入 attributes_json"],
    ["", ""],
    ["生成时间", "2026-06-21"],
]
for row in info:
    ws2.append(row)
ws2["A1"].font = Font(name="微软雅黑", size=14, bold=True)

out = r"C:\Users\47763\Desktop\Amazon商品字段映射表.xlsx"
wb.save(out)
print(f"已保存: {out}")
