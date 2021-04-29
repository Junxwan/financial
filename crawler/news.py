import requests
import time
import feedparser
import pytz
from datetime import datetime as dt
from dateutil import parser
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}

LIMIT = 15


# 聯合報
# cate_id 6644(產經) 6645(股市)
# https://udn.com/news/cate/2/6644
# https://udn.com/news/cate/2/6645
def udn(cate_id, end_date):
    news = []
    isRun = True
    page = 0

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://udn.com/api/more?channelId=2&cate_id={cate_id}&page={page}&type=cate_latest_news",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        data = r.json()

        if data['state'] == False or len(data['lists']) == 0:
            break

        for v in data['lists']:
            if v['time']['date'] <= end_date:
                isRun = False
                break

            news.append({
                'title': v['title'],
                'url': f"https://udn.com{v['titleLink']}",
                'date': v['time']['date'],
            })

        page = page + 1

        time.sleep(1)

    return news


# 蘋果 https://tw.appledaily.com/realtime/property/
def appledaily(end_date, timezone='Asia/Taipei'):
    news = []
    next = 8
    isRun = True
    tz = pytz.timezone(timezone)

    while isRun:
        if (next / 8) >= LIMIT:
            break

        r = requests.get(
            f"https://tw.appledaily.com/pf/api/v3/content/fetch/query-feed?query=%7B%22feedOffset%22%3A0%2C%22feedQuery%22%3A%22(taxonomy.primary_section._id%3A%5C%22%2Frealtime%2Fproperty%5C%22)%2BAND%2Btype%3Astory%2BAND%2Bdisplay_date%3A%5Bnow-200h%2Fh%2BTO%2Bnow%5D%2BAND%2BNOT%2Btaxonomy.tags.text.raw%3A_no_show_for_web%2BAND%2BNOT%2Btaxonomy.tags.text.raw%3A_nohkad%22%2C%22feedSize%22%3A{next}%2C%22sort%22%3A%22display_date%3Adesc%22%7D&d=203&_website=tw-appledaily",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        data = r.json()

        if len(data['content_elements']) == 0:
            break

        for v in data['content_elements'][data['next'] - 8:data['next']]:
            date = dt.fromtimestamp(parser.parse(v['display_date']).timestamp(), tz=tz).strftime(
                '%Y-%m-%d %H:%M:%S')

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': v['headlines']['basic'],
                'url': f"https://tw.appledaily.com/{v['website_url']}",
                'date': date,
            })

        if data['next'] >= data['count']:
            break

        next = data['next'] + 8

        time.sleep(1)

    return news


# 經濟日報
# https://money.udn.com/money/cate/5591 產業熱點(5612) 生技醫藥(10161) 企業CEO(5649)
# https://money.udn.com/money/cate/10846 總經趨勢(10869) 2021投資前瞻(121887)
# https://money.udn.com/money/cate/5588 國際焦點(5599) 美中貿易戰(10511)
# https://money.udn.com/money/cate/12017 金融脈動(5613)
# https://money.udn.com/money/cate/5590 市場焦點(5607) 集中市場(5710) 櫃買市場(11074)
# https://money.udn.com/money/cate/11111 國際期貨(11114)
# https://money.udn.com/money/cate/12925 國際綜合(121854) 外媒解析(12937) 產業動態(121852) 產業分析(12989)
def money_udn(cate_id, sub_id, end_date, timezone='Asia/Taipei'):
    news = []
    data = feedparser.parse(f"https://money.udn.com/rssfeed/news/1001/{cate_id}/{sub_id}?ch=money")
    tz = pytz.timezone(timezone)

    for v in data['entries']:
        date = dt.fromtimestamp(parser.parse(v['published']).timestamp(), tz=tz).strftime(
            '%Y-%m-%d %H:%M:%S')

        if date <= end_date:
            break

        news.append({
            'title': v['title'],
            'url': v['link'],
            'date': date,
        })

    time.sleep(1)

    return news


# 中時 https://www.chinatimes.com/money/total?page=1&chdtv
def chinatimes(end_date):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://www.chinatimes.com/money/total?page={page}&chdtv",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')

        for v in soup.select('h3', class_='articlebox-compact'):
            date = f"{v.parent.contents[3].contents[1].attrs['datetime']}:00"

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': v.text,
                'url': f"https://www.chinatimes.com{v.parent.contents[1].contents[0].attrs['href']}?chdtv",
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# 科技新報 https://technews.tw/
def technews(end_date, timezone='Asia/Taipei'):
    news = []
    data = feedparser.parse(f"https://technews.tw/feed/")
    tz = pytz.timezone(timezone)

    for v in data['entries']:
        date = dt.fromtimestamp(parser.parse(v['published']).timestamp(), tz=tz).strftime(
            '%Y-%m-%d %H:%M:%S')

        if date <= end_date:
            break

        news.append({
            'title': v['title'],
            'url': v['link'],
            'date': date,
        })

    return news


# 工商時報
# https://ctee.com.tw/category/news/industry (產業)
# https://ctee.com.tw/category/news/tech (科技)
# https://ctee.com.tw/category/news/global (國際)
# https://ctee.com.tw/category/news/china (兩岸)
def ctee(end_date, type, timezone='Asia/Taipei'):
    news = []
    isRun = True
    page = 1
    tz = pytz.timezone(timezone)

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://ctee.com.tw/category/news/{type}/page/{page}",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')

        for v in soup.select('article'):
            date = dt.fromtimestamp(parser.parse(v.find('time').attrs['datetime']).timestamp(), tz=tz).strftime(
                '%Y-%m-%d %H:%M:%S')

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': v.find('h2', class_='title').text.strip(),
                'url': v.find('h2', class_='title').a.attrs['href'],
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news
