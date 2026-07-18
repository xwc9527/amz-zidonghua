"""最小可用列表/详情 HTML 构造器，覆盖 US / DE / JP 关键字段。"""
from __future__ import annotations


def build_list_card_html(
    asin: str,
    title: str,
    price_offscreen: str,
    rating_alt: str,
    review_text: str,
) -> str:
    return f"""
    <div data-component-type="s-search-result" data-asin="{asin}">
      <h2><a href="/dp/{asin}"><span>{title}</span></a></h2>
      <span class="a-price"><span class="a-offscreen">{price_offscreen}</span></span>
      <i class="a-icon a-icon-star-small"><span class="a-icon-alt">{rating_alt}</span></i>
      <a href="/product-reviews/{asin}/#customerReviews">{review_text}</a>
      <img class="s-image" src="https://m.media-amazon.com/images/I/{asin}.jpg"/>
    </div>
    """


def build_list_page(cards_html: list[str], captcha: bool = False) -> str:
    body = "\n".join(cards_html)
    if captcha:
        body = "<form>captcha</form>" + body
    return f"<!doctype html><html><body>{body}</body></html>"


def build_us_detail_html(
    *,
    bsr_main: tuple[int, str] | None = (5000, "Home & Kitchen"),
    bsr_sub: tuple[int, str] | None = (200, "Kitchen Tools"),
    date_text: str = "Date First Available : June 20, 2026",
    weight_text: str = "Item Weight : 1.5 pounds",
    dims_text: str = "Item Dimensions : 10 x 5 x 2 inches",
    country_text: str = "Country of Origin : China",
    variant_asins: list[str] | None = None,
    sellers_text: str | None = "3 new from $19.99",
    merchant_text: str = "Ships from and sold by Amazon.com",
    amazon_choice: bool = False,
    bestseller: bool = False,
) -> str:
    badges = []
    if amazon_choice:
        badges.append('<div class="a-badge" data-a-badge-type="amazons-choice">Amazon\'s Choice</div>')
    if bestseller:
        badges.append('<div id="zeitgeistBadge_feature_div" class="a-badge">#1 Best Seller</div>')
    badge_html = "\n".join(badges)

    bsr_parts = []
    if bsr_main:
        bsr_parts.append(f"#{bsr_main[0]:,} in {bsr_main[1]}")
    if bsr_sub:
        bsr_parts.append(f"#{bsr_sub[0]:,} in {bsr_sub[1]}")
    bsr_html = "  ".join(bsr_parts)

    variants = variant_asins or []
    twister = "".join(
        f'<li data-defaultasin="{a}"></li>' for a in variants
    )
    olp = f'<div id="olp_feature_div">{sellers_text}</div>' if sellers_text else ""

    # 真实 Amazon 多为 th/td 分列；同时保留「标签: 值」单格形态供鲁棒性覆盖
    if ":" in country_text:
        co_label, _, co_val = country_text.partition(":")
        co_row = f"<tr><th>{co_label.strip()}</th><td>{co_val.strip()}</td></tr>"
    else:
        co_row = f"<tr><td>{country_text}</td></tr>"

    def _kv_row(line: str) -> str:
        if ":" in line:
            lab, _, val = line.partition(":")
            return f"<tr><th>{lab.strip()}</th><td>{val.strip()}</td></tr>"
        return f"<tr><td>{line}</td></tr>"

    return f"""<!doctype html><html><body>
{badge_html}
<div id="prodDetails">
  <table>
    <tr><th>Best Sellers Rank</th><td>{bsr_html}</td></tr>
    {_kv_row(date_text)}
    {_kv_row(weight_text)}
    {_kv_row(dims_text)}
    {co_row}
  </table>
</div>
<div id="twister_feature_div"><ul>{twister}</ul></div>
{olp}
<div id="merchant-info">{merchant_text}</div>
</body></html>"""


def build_de_detail_html() -> str:
    return """<!doctype html><html lang="de"><body>
<div id="prodDetails">
  <table>
    <tr><td>Amazon Bestseller-Rang</td><td>Nr. 1.234 in Küche, Haushalt & Wohnen  Nr. 56 in Küchenhelfer</td></tr>
    <tr><td>Datum der Ersten Verfügbarkeit : 15. März 2026</td></tr>
    <tr><td>Artikelgewicht : 500 Gramm</td></tr>
    <tr><td>Produktabmessungen : 20 x 10 x 5 cm</td></tr>
    <tr><td>Herkunftsland : China</td></tr>
  </table>
</div>
<div id="merchant-info">Verkauf und Versand durch Amazon</div>
</body></html>"""


def build_jp_detail_html() -> str:
    return """<!doctype html><html lang="ja"><body>
<div class="a-badge" data-a-badge-type="amazons-choice">Amazonおすすめ</div>
<div id="prodDetails">
  <table>
    <tr><td>売れ筋ランキング</td><td>#2,500 in ホーム＆キッチン  #88 in キッチン用品</td></tr>
    <tr><td>発売日 : 2026/04/10</td></tr>
    <tr><td>商品の重量 : 1.2 kg</td></tr>
    <tr><td>Product Dimensions : 25 x 12 x 8 cm</td></tr>
    <tr><td>原産国 : 中国</td></tr>
  </table>
</div>
<div id="merchant-info">Amazon.co.jpが発送</div>
</body></html>"""


def build_dims_dw_h_html(dims_line: str) -> str:
    return f"""<!doctype html><html><body>
<div id="prodDetails">
  <table><tr><td>Item Dimensions LxWxH</td><td>{dims_line}</td></tr></table>
</div>
</body></html>"""
