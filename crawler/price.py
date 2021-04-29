import requests
import pandas as pd
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


# 面板報價
def wits_view():
    r = requests.get("https://www.witsview.cn", headers=HEADERS)
    r.encoding = 'utf8'

    table = pd.read_html(r.text)
    soup = BeautifulSoup(r.text, 'html.parser')
    name = soup.find_all('div', class_="table-btn")
    p = soup.select('p')

    name = [
        name[0].text,
        name[1].text,
        name[2].text,
        name[3].text,
    ]

    p = [
        f"{p[0].text[7:12]}-{p[0].text[13:15]}-{p[0].text[16:18]}",
        f"{p[2].text[7:12]}-{p[2].text[13:15]}-{p[2].text[16:18]}",
        f"{p[3].text[7:12]}-{p[3].text[13:15]}-{p[3].text[16:18]}",
        f"{p[4].text[7:12]}-{p[4].text[13:15]}-{p[4].text[16:18]}",
    ]

    return {
        name[0]: {
            p[0]: table[0]
        },
        name[1]: {
            p[1]: table[1]
        },
        name[2]: {
            p[2]: table[2]
        },
        name[3]: {
            p[3]: table[3]
        },
    }
