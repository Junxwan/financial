PATH=/var/www/data
SAVE=false
NOTIFY=false
RESTART=false
CLI=./venv/bin/python cli.py

day: get_tse get_otc dividend tag_exponent stock cb_info cb_price

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
		$(CLI) tag_exponent -r $(RESTART) -n $(NOTIFY)

# 爬大盤價格
get_tse:
		$(CLI) price -t TSE -n $(NOTIFY)

# 爬OTC價格
get_otc:
		$(CLI) price -t OTC -n $(NOTIFY)

# 爬新聞內容
get_news_content:
		$(CLI) news-context

# 爬股利
dividend:
		$(CLI) dividend -s true

# 匯入個股價格
import_stock_price:
		$(CLI) import-to-database -t price -d $(PATH)

# 可轉債
cb_info:
		$(CLI) cb -t info -n $(NOTIFY)

# 可轉債轉換價格
cb_conversion_price:
		$(CLI) cb -t conversion_price -n $(NOTIFY)

# 可轉債餘額
cb_balance:
		$(CLI) cb -t balance -n $(NOTIFY)

# 可賺債價格
cb_price:
		$(CLI) cb -t price -n $(NOTIFY)

# 最近上市上櫃
stock:
		$(CLI) stock -n $(NOTIFY)

# Line新聞
line_new:
		$(CLI) line-news -n $(NOTIFY)

# 股權分散表
stock_dispersion:
		$(CLI) stock_dispersion -n $(NOTIFY)

# 籌碼
bargaining_chip:
		$(CLI) bargaining_chip -n $(NOTIFY)

# Line通知
line:
	$(CLI) line

news:
	$(CLI) news -h 2 -s true -n $(NOTIFY)