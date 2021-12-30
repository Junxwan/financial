import requests
import pandas as pd
from datetime import datetime

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


# sp 500
def sp500(code):
    r = requests.get(f"https://money.moneydj.com/us/uslist/list0005/{code}", headers=HEADERS)
    r.encoding = 'utf8'
    table = pd.read_html(r.text)
    date = table[3].iloc[0].loc['日期']

    return {
        f"{datetime.now().year}-{date[:2]}-{date[3:5]}": table[3]
    }
