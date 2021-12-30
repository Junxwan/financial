import time
import requests
from datetime import datetime

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


def eastmoney(date, page=1, to=1):
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    endDate = f"{int(date[:4]) - 2}{date[4:]}"

    data = []

    for i in range(to):
        if i > 0:
            time.sleep(3)

        r = requests.get(
            f"http://reportapi.eastmoney.com/report/list?industryCode=*&pageSize=50&industry=*&rating=*&ratingChange=*&beginTime={endDate}&endTime={date}&pageNo={page}&fields=&qType=1&orgCode=&rcode=&_={time.time() * 1000}",
            headers=HEADERS
        )

        if r.status_code != 200:
            return

        repo = r.json()

        for v in repo['data']:
            data.append({
                'title': v['title'],
                'date': v['publishDate'][:19],
                'industry': v['industryName'],
                'url': f"http://data.eastmoney.com/report/zw_industry.jshtml?infocode={v['infoCode']}"
            })

        page = page + 1

    return data
