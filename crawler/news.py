import json
import requests
import metadata_parser
import time
import feedparser
import pytz
import urllib.parse
from datetime import datetime as dt
from dateutil import parser
from bs4 import BeautifulSoup
from pyppeteer import launch

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}

LIMIT = 15

TIMEZONE = pytz.timezone('Asia/Taipei')


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
                'date': f"{v['time']['date']}:00",
            })

        page = page + 1

        time.sleep(1)

    return news


# 聯合報文章內容
def udn_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    body = soup.find('article', class_='article-content')
    if body is None:
        body = soup.find('meta', property='og:description')

        if body is not None:
            return body.attrs['content']
        return None

    return body.prettify()


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


# 經濟日報章內容
def money_udn_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    if soup is None:
        return None

    return {
        'title': soup.find(id='story_art_title').text,
        'date': dt.fromtimestamp(parser.parse(soup.find('time').text.strip()).timestamp()).strftime(
            '%Y-%m-%d %H:%M:%S'),
        'body': soup.find(class_='article-body').text,
    }


def money_udn(cate_id, sub_id, end_date):
    news = []
    page = 1
    isRun = True

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://money.udn.com/money/get_article/{page}/1001/{cate_id}/{sub_id}?_={time.time()}",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')
        for v in soup.find_all('li'):
            url = f"https://money.udn.com{v.find('a').attrs['href']}"
            context = money_udn_context(url)

            if context['date'] <= end_date:
                isRun = False
                break

            news.append({
                'title': context['title'],
                'url': url,
                'date': context['date'],
            })

            time.sleep(3)

        page = page + 1

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
def money_udn_rss(cate_id, sub_id, end_date, timezone='Asia/Taipei'):
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


# 經濟日報-證卷
# https://ctee.com.tw/category/news/stocks
def money_udn_stock(end_date, timezone='Asia/Taipei'):
    news = []
    page = 1
    isRun = True
    tz = pytz.timezone(timezone)

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://ctee.com.tw/category/news/stocks/page/{page}",
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
                'title': v.find('h2').text,
                'url': v.find('a').attrs['href'],
                'date': date,
            })

        page = page + 1

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


# 中時-財經要聞 https://www.chinatimes.com/newspapers/260202?page=1&chdtv
def chinatimes_newspapers(end_date):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://www.chinatimes.com/newspapers/260202?page={page}&chdtv",
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


# 中時文章內容
def chinatimes_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    for child in soup.find_all('div', class_='promote-word'):
        child.decompose()
    for child in soup.find_all('div', class_='ad'):
        child.decompose()

    soup = soup.find('div', class_='article-body')
    if soup is None:
        return None

    return soup.prettify()


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


# 科技新報文章內容
def technews_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser').find('div', class_='indent')
    if soup is None:
        return None

    return soup.prettify()


# 工商時報
# https://ctee.com.tw/livenews/aj (財經)
# https://ctee.com.tw/livenews/gj (國際)
# https://ctee.com.tw/livenews/kj (科技)
# https://ctee.com.tw/livenews/lm (兩岸)
def ctee(end_date, type):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://ctee.com.tw/livenews/{type}/page/{page}",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')
        year = soup.find(class_='topbar-date').text.split('.')[0]

        for v in soup.find_all(class_='item-content'):
            d = f"{year}/{v.find_all('a')[1].contents[1].text.replace('|', '').strip()}"
            date = dt.fromtimestamp(parser.parse(d).timestamp()).strftime('%Y-%m-%d %H:%M:%S')

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': str(v.find_all('a')[1].contents[0]).strip(),
                'url': v.find_all('a')[1].attrs['href'],
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# 工商時報
# https://ctee.com.tw/category/news/stocks (證卷)
def cteeCategory(end_date, type):
    news = []
    isRun = True
    page = 1

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

        for v in soup.find_all('article'):
            date = dt.fromtimestamp(parser.parse(v.find('time').attrs['datetime']).timestamp()).strftime(
                '%Y-%m-%d %H:%M:%S')

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': str(v.find(class_='title').text).strip(),
                'url': v.find('a').attrs['href'],
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# 工商時報文章內容
def ctee_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    for child in soup.find_all('div', class_='mbwsrzpaxh-hide-on-desktop'):
        child.decompose()

    soup = soup.find('div', class_='entry-content')
    if soup is None:
        return None

    return soup.prettify()


# 鉅亨網
# https://news.cnyes.com/news/cat/tw_stock (台股)
# https://news.cnyes.com/news/cat/wd_stock (國際股)
# https://news.cnyes.com/news/cat/future (期貨)
def cnyes(end_date, type, timezone='Asia/Taipei'):
    r = requests.get(
        f"https://news.cnyes.com/news/cat/{type}",
        headers=HEADERS
    )

    if r.status_code != 200:
        return []

    news = []
    tz = pytz.timezone(timezone)
    soup = BeautifulSoup(r.text, 'html.parser')
    soup = soup.find('div', class_='theme-list')

    for v in soup.find_all('a', class_='_1Zdp'):
        date = dt.fromtimestamp(parser.parse(v.find('time').attrs['datetime']).timestamp(), tz=tz).strftime(
            '%Y-%m-%d %H:%M:%S')

        if date <= end_date:
            break

        news.append({
            'title': v.attrs['title'].strip(),
            'url': f"https://news.cnyes.com{v.attrs['href']}",
            'date': date,
        })

    return news


# 鉅亨網文章內容
def cnyes_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    for child in soup.find_all('div', class_='cnyes-dfp-banner'):
        child.decompose()

    soup = soup.find('div', itemprop='articleBody')
    if soup is None:
        return None

    return soup.prettify()


# 自由時報
# https://ec.ltn.com.tw/list/international (國際財經)
# https://ec.ltn.com.tw/list/securities (證券產業)
def ltn(end_date, type):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://ec.ltn.com.tw/list_ajax/{type}/{page}",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        for v in r.json():
            date = f"{v['A_ViewTime'][:4]}-{v['A_ViewTime'][5:7]}-{v['A_ViewTime'][8:10]} {v['A_ViewTime'][11:]}:00"

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': v['LTNA_Title'],
                'url': v['url'],
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# 自由時報文章內容
def ltn_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser').find('div', class_='text')
    if soup is None:
        return None

    for child in soup.find_all('p', class_='appE1121'):
        child.decompose()

    for child in soup.find_all('script'):
        child.decompose()

    return soup.prettify()


# moneydj
# https://www.moneydj.com/kmdj/news/newsreallist.aspx?a=mb010000 (頭條新聞)
# https://www.moneydj.com/kmdj/news/newsreallist.aspx?a=mb020000 (總體經濟)
# https://www.moneydj.com/kmdj/news/newsreallist.aspx?a=mb040200 (債券市場)
# https://www.moneydj.com/kmdj/news/newsreallist.aspx?a=mb07 (產業情報)
# https://www.moneydj.com/kmdj/news/newsreallist.aspx?a=mb070100 (科技脈動)
def moneydj(end_date, type):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://www.moneydj.com/kmdj/news/newsreallist.aspx?index1={page}&a={type}",
            headers=HEADERS,verify=False
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')
        y = end_date[:4]

        for v in soup.find('table', class_='forumgrid').find_all('tr'):
            if v.text.strip() == '時間標題人氣':
                continue
            v.find('a')
            date = f"{y}-{v.contents[1].text.strip()[:2]}-{v.contents[1].text.strip()[3:5]} {v.contents[1].text.strip()[6:11]}:00"

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': v.contents[2].text.strip(),
                'url': f"https://www.moneydj.com{v.find('a').attrs['href']}",
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# moneydj 文章內容
def moneydj_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser').find('div', id='viewer_body')
    if soup is None:
        return None

    soup = soup.find('div', class_='wikilink')
    if soup is None:
        return None

    return soup.prettify()


# 東森新聞
# https://fnc.ebc.net.tw/fncnews/stock (財經新聞台股)
def ebc(end_date, type):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://fnc.ebc.net.tw/fncnews/{type}?page={page}",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')

        for v in soup.find('div', class_='fncnews-list-box').find_all('a'):
            date = dt.fromtimestamp(
                parser.parse(f"{v.find('span', class_='small-gray-text').text[1:-1]}:00").timestamp()).strftime(
                '%Y-%m-%d %H:%M:%S')

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': v.find('p').text.strip(),
                'url': f"https://fnc.ebc.net.tw{v.attrs['href']}",
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# 東森新聞文章內容
def ebc_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser').find_all('script')
    if len(soup) < 19 or len(soup[19].contents) < 1:
        return None

    body = soup[19].contents[0]

    return json.loads(body[body.find("{"):body.find("}") + 1])['content']


# trendforce
# https://www.trendforce.cn/presscenter/news?page=1 新聞中心
def trendforce(end_date):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= LIMIT:
            break

        r = requests.get(
            f"https://www.trendforce.cn/presscenter/news?page={page}",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')
        for v in soup.find_all('div', class_='list-item'):
            time.sleep(1)

            url = f"https://www.trendforce.cn{v.find('a').attrs['href']}"

            r = requests.get(url, headers=HEADERS)

            if r.status_code != 200:
                break

            soup = BeautifulSoup(r.text, 'html.parser')
            image = soup.find("meta", property="og:image")
            image = image.attrs['content'].split('/')[-1]
            date = f"{image[:4]}-{image[4:6]}-{image[6:8]} {image[9:11]}:{image[11:13]}:{image[13:15]}"

            try:
                parser.parse(date)
            except Exception as a:
                date = parser.parse(soup.find('div', class_='tag-row').contents[1].text).strftime('%Y-%m-%d %H:%M:%S')

            if date < end_date:
                isRun = False
                break

            news.append({
                'title': v.find('a').text,
                'url': url,
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# trendforce 文章內容
def trendforce_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    return BeautifulSoup(r.text, 'html.parser').find('div', class_='presscenter').prettify()


# dramx
# https://www.dramx.com/Info/1.html#articlelist
def dramx(end_date):
    news = []
    isRun = True
    page = 1

    while isRun:
        if page >= 300:
            break

        r = requests.get(
            f"https://www.dramx.com/Info/{page}.html#articlelist",
            headers=HEADERS
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')

        for v in soup.find_all('div', class_='Article-box-cont'):
            t = v.find('img').attrs['src'].split('/')[-1][:14]
            url = f"https://www.dramx.com{v.find('a').attrs['href']}"

            if t.find('Default') == 0:
                r = requests.get(url, headers=HEADERS)
                date = BeautifulSoup(r.text, 'html.parser').find('time').text
            else:
                date = f"{t[:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:14]}"

            if date <= end_date:
                isRun = False
                break

            news.append({
                'title': v.find('h3').text.strip(),
                'url': url,
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# dramx 文章內容
def dramx_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    return BeautifulSoup(r.text, 'html.parser').find('div', class_='newspage-cont').prettify()


# digitimes-報導總欄
# https://www.digitimes.com.tw/tech/dt/allnewslist.asp?CnlID=99
def digitimes(end_date, timezone='Asia/Taipei'):
    news = []
    isRun = True
    page = 1
    tz = pytz.timezone(timezone)

    while isRun:
        if page >= 300:
            break

        r = requests.post(
            "https://www.digitimes.com.tw/tech/dt/newslist_ajax.asp", {
                'sdate': 0,
                'cat1': 0,
                'sort': 0,
                'sorttype': 'desc',
                'page': page,
            },
            headers={
                'User-Agent': USER_AGENT,
                'Host': 'www.digitimes.com.tw',
                'Referer': 'https://www.digitimes.com.tw/tech/dt/allnewslist.asp?CnlID=99',
            }
        )

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser')
        year = dt.now().year

        for v in soup.find_all('div', class_='col-md-12 col-sm-12 col-xs-12 trbox'):
            span = v.find_all('span')
            date = dt.fromtimestamp(parser.parse(f"{year} {span[3].text}").timestamp(), tz=tz).strftime(
                '%Y-%m-%d %H:%M:%S')

            if date <= end_date:
                isRun = False
                break

            url = f"https://www.digitimes.com.tw{v.find('a', target='_blank').attrs['href']}"

            r = requests.get(url, headers=HEADERS)
            soup = BeautifulSoup(r.text, 'html.parser')

            time.sleep(1)

            if (soup.find('button') is not None):
                continue

            news.append({
                'title': span[1].text,
                'url': url,
                'date': date,
            })

        page = page + 1

        time.sleep(1)

    return news


# digitimes-內容文章
def digitimes_context(url):
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return None

    return BeautifulSoup(r.text, 'html.parser').find('div', id='newsText').prettify()


# google news
async def google_news(keyWord, url, num=30):
    news = []
    browser = await launch(timeout=300000)
    u = ''

    try:
        for i in range(num):
            page = await browser.newPage()

            if url is None:
                print('google new page end')
                break

            await page.goto(url)
            soup = BeautifulSoup(await page.content(), 'html.parser')
            for html in soup.find_all(class_='ftSUBd'):
                title = str(html.find(class_='iRPxbe').contents[2].text)
                if title.find(keyWord) < 0:
                    continue

                u = html.find('a').attrs['href']

                try:
                    meta = metadata_parser.MetadataParser(url=u, url_headers=HEADERS)
                except Exception as e:
                    print(f'error google new page {i + 1} url: {u} error: {e.__str__()}')
                    continue

                if len(meta.metadata['meta']) == 0:
                    continue

                hostName = urllib.parse.urlparse(u).hostname
                if hostName in ['tw.stock.yahoo.com', 'tw.yahoo.com', 'tw.news.yahoo.com']:
                    t = meta.soup.find('time').attrs['datetime']
                elif hostName == 'money.udn.com':
                    t = meta.metadata['meta']['date']
                elif hostName in ['finance.ettoday.net', 'www.chinatimes.com', 'wantrich.chinatimes.com',
                                  'www.ettoday.net']:
                    t = meta.metadata['meta']['pubdate']
                elif hostName == 'www.bnext.com.tw':
                    t = meta.metadata['meta']['my:date']
                elif hostName == 'www.moneydj.com':
                    t = f"{meta.soup.find(id='MainContent_Contents_lbDate').text}:00"
                elif hostName == 'technews.tw':
                    t = meta.soup.find_all(class_='body')[1].text.replace(' 年 ', '-').replace(' 月 ', '-').replace(' 日 ',
                                                                                                                  ' ')
                elif hostName == 'www.wealth.com.tw':
                    p = await browser.newPage()
                    await p.goto(u)
                    t = BeautifulSoup(await p.content(), 'html.parser').find(class_='_1G3Lb').contents[0].text
                    await p.close()
                elif hostName == 'www.gvm.com.tw':
                    t = meta.soup.find(class_='article-time').text
                elif hostName == 'fnc.ebc.net.tw':
                    ts = meta.soup.find(class_='small-gray-text').text.split(' ')
                    t = f"{ts[0]} {ts[1]}:00"
                elif hostName == 'finance.technews.tw':
                    t = meta.soup.find(class_='entry-header').find_all(class_='body')[1].text.replace(' 年 ',
                                                                                                      '-').replace(
                        ' 月 ', '-').replace(' 日 ', ' ')
                elif hostName == 'www.ithome.com.tw':
                    t = meta.soup.find(class_='created').text
                elif hostName == 'www.businessweekly.com.tw':
                    t = meta.metadata['meta']['lastmod']
                elif hostName == 'ubrand.udn.com':
                    t = meta.metadata['meta']['date.available']
                elif hostName in ['hk.jrj.com.cn', 'inews.hket.com', 'www.aastocks.com', 'www.quamnet.com',
                                  'www.businessweekly.com.tw', 'style.udn.com', 'health.udn.com', 'gnn.gamer.com.tw',
                                  'news.tvbs.com.tw', 'www.finet.hk', 'www.walkerland.com.tw', 'health.ltn.com.tw',
                                  'video.udn.com', 'www.1111.com.tw', 'invest.hket.com', 'www.cna.com.tw',
                                  'www.businesswirechina.com', 'www.4gamers.com.tw', 'www.digitimes.com.tw',
                                  'www.ctwant.com', 'www.youtube.com', 'turnnewsapp.com', 'blog.housetube.tw',
                                  'wealth.businessweekly.com.tw', 'times.hinet.net', 'www.cw.com.tw',
                                  'www.foodnext.net', 'estate.ltn.com.tw', 'house.udn.com', 'finance.ce.cn']:
                    continue
                else:
                    t = meta.metadata['meta']['article:published_time']

                news.append({
                    'url': u,
                    'title': title,
                    'date': parser.parse(t).strftime('%Y-%m-%d %H:%M:%S'),
                })

                time.sleep(2)

            for p in soup.find(class_='AaVjTc').find_all('a'):
                if p.text.isnumeric() and int(p.text) == i + 2:
                    url = "https://www.google.com" + p.attrs['href']
                    time.sleep(5)
                    break
                else:
                    url = None

            print(f'google new page {i + 1} news {len(news)}')

            await page.close()

    except Exception as e:
        print(f'google new page error: {e.__str__()} url: {u}')

    await browser.close()
    return news
