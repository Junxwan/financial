PATH=/var/www/data
SAVE=false
NOTIFY=false
RESTART=false
CLI=./venv/bin/python cli.py

# 爬月營收
month_revenue:
        $(CLI) month_revenue -o $(PATH) -s $(SAVE) -n $(NOTIFY)

# 爬財報
financial:
        $(CLI) financial -o $(PATH) -s $(SAVE) -n $(NOTIFY)

# 爬投信持股
fund:
        $(CLI) fund -o $(PATH)/fund -s $(SAVE) -n $(NOTIFY)

# 建立產業指數價格
tag_exponent:
        $(CLI) tag_exponent -r $(RESTART)

# 爬大盤價格
get_tse:
        $(CLI) price -t TSE

# 爬OTC價格
get_otc:
        $(CLI) price -t OTC

# 爬新聞內容
get_news_content:
        $(CLI) price news-context

# 爬股利
dividend:
        $(CLI) dividend -s true

# 匯入個股價格
import_stock_price:
        $(CLI) import-to-database -t price -d $(PATH)