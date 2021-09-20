import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


def _url(code, period, year, month):
    return f"https://mops.twse.com.tw/mops/web/t120sg01?TYPEK=&bond_id={code}{period}&bond_kind=5&bond_subn=%24M00000001&bond_yrn={period}&come=2&encodeURIComponent=1&firstin=ture&issuer_stock_code={code}&monyr_reg={year}{month}&pg=&step=0&tg="


# 最近上櫃可轉債
def new():
    data = []

    r = requests.get("https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo.php?l=zh-tw",
                     headers=HEADERS)
    r.encoding = 'utf-8'
    html = BeautifulSoup(r.text, 'html.parser')
    for tr in html.find('tbody').find_all('tr'):
        tds = tr.find_all('td')
        data.append([
            tds[0].text,
            tds[1].text,
            tds[3].text,
            tds[4].text,
            tds[5].text.strip(),
            tds[6].text.replace(",", ""),
            tds[7].find('a').attrs['href']
        ])

    return data


# 根據年月查詢可轉債
def select(year, month):
    data = {}

    r = requests.post("https://sii.twse.com.tw/server-java/t120sc11", {
        'id': '',
        'key': '',
        'TYPEK': '',
        'step': 1,
        'excel': 0,
        'typek': '',
        'type': 2,
        'year': year - 1911,
        'month': month,
    }, headers=HEADERS)
    r.encoding = 'big5-hkscs'

    for index, value in pd.read_html(StringIO(r.text))[1].iterrows():
        if value['發行種類'] != '轉(交)換公司債' or value['證券代號'] == '合計' or value['債券掛牌情形'] == '未掛牌交易':
            continue

        data[value['債券代碼']] = _url(value['證券代號'], value['債券期別'], year, "%02d" % month)

    return data


def findByUrl(url):
    url = url.replace("http://", "https://")

    try:
        r = requests.get(url, headers=HEADERS)
    except Exception as e:
        return None

    r.encoding = 'utf-8'
    html = BeautifulSoup(r.text, 'html.parser')
    table = html.find('table', class_='hasBorder')

    if table is None:
        return None

    trs = table.find_all('tr')

    startDate = trs[2].contents[0].text.replace('發行日期：', '').split('/')
    startDate[0] = str(int(startDate[0]) + 1911)

    endDate = trs[3].contents[0].text.replace('到期日期：', '').split('/')
    endDate[0] = str(int(endDate[0]) + 1911)

    conversionDate = trs[27].contents[0].text.replace('轉(交)換期間：', '').split('～')
    startConversionDate = conversionDate[0].split('/')
    startConversionDate[0] = str(int(startConversionDate[0]) + 1911)

    endConversionDate = conversionDate[1].split('/')
    endConversionDate[0] = str(int(endConversionDate[0]) + 1911)

    market = {
        '上市': 1,
        '上櫃': 2,
        '興櫃': 3,
    }

    return {
        'code': trs[7].contents[0].text.split("：")[1],
        'name': trs[7].contents[2].text.split("：")[1].strip(),
        'period': trs[7].contents[0].text.split("：")[1][-1],
        'start_date': "-".join(startDate),
        'end_date': "-".join(endDate),
        'active_year': trs[2].contents[2].text.replace("發行期限：", '').split('年')[0],
        'apply_total_amount': int(trs[12].contents[0].text.replace('申請發行總額：', '').replace(',', '').replace('元', '')),
        'publish_total_amount': int(trs[13].contents[0].text.replace('實際發行總額：', '').replace(',', '').replace('元', '')),
        'publish_price': float(trs[24].contents[0].text.replace('發行價格：', '').split('元')[0]),
        'start_conversion_date': "-".join(startConversionDate),
        'end_conversion_date': "-".join(endConversionDate),
        'conversion_premium_rate': float(trs[26].contents[2].text.replace('轉換溢價率：', '').replace('%', '')),
        'coupon_rate': float(trs[25].contents[0].text.replace('票面利率：', '').replace('%', '')),
        'market': market[trs[5].contents[0].text.replace('債券掛牌情形：', '')],
        'is_collateral': trs[30].contents[0].text.replace('擔保情形：', '').split('，')[0] == '有',
        'url': url,
    }
