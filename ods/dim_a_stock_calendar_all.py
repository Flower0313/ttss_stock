# -*- coding: utf-8 -*-
# @Time : 2022/5/20 - 0:36
# @Author : Holden
# @File : get
# @Project : python
import datetime
import time
import requests
import duckdb
import pandas as pd

START = "1998-01"
END = "2024-12"
DELAY_SECONDS = 1.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10

months = pd.date_range(start=START + "-01", end=END + "-01", freq="MS").strftime("%Y-%m").tolist()

records = []

for m in months:
    time.sleep(DELAY_SECONDS)

    url = f"http://www.szse.cn/api/report/exchange/onepersistenthour/monthList?month={m}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            result_json = resp.json()
            break
        except Exception as e:
            print(f"[警告] 请求 {m} 出错 (尝试 {attempt}/{MAX_RETRIES})：{e}")
            if attempt == MAX_RETRIES:
                print(f"[跳过] 放弃 {m}，继续下一个月份。")
                result_json = {"data": []}
            else:
                time.sleep(2 ** attempt)
    data_list = result_json.get("data") or []
    for a in data_list:
        jyrq = a.get("jyrq")
        if not jyrq:
            continue
        try:
            ds = datetime.datetime.strptime(jyrq, "%Y-%m-%d").date()
        except Exception:
            continue

        jybz = a.get("jybz")
        is_trade_day = (str(jybz) == "1")

        records.append({
            "ds": ds,
            "is_trade_day": is_trade_day,
            "year": ds.year,
            "quarter": (ds.month - 1) // 3 + 1,
            "month": ds.month,
            "week_of_year": ds.isocalendar()[1],
            "day_of_week": ds.isoweekday(),
            "day_of_week_cn": ["周一","周二","周三","周四","周五","周六","周日"][ds.isoweekday()-1],
            "day_of_month": ds.day,
            "is_month_start": ds.day == 1,
            "is_month_end": (ds + datetime.timedelta(days=1)).month != ds.month
        })

if not records:
    print("未获取到任何数据，检查网络或接口是否变化。")
else:
    df = pd.DataFrame(records)

    df['ds'] = pd.to_datetime(df['ds']).dt.date
    df = df.drop_duplicates(subset=['ds']).sort_values('ds').reset_index(drop=True)

    trade_dates = sorted(df.loc[df['is_trade_day'], 'ds'].tolist())

    trade_date_to_index = {d: i+1 for i, d in enumerate(trade_dates)}
    df['trade_day_index'] = df['ds'].map(trade_date_to_index).astype('Int64')

    def prev_trade(idx):
        if pd.isna(idx):
            return None
        i = int(idx) - 1
        return trade_dates[i-1] if i - 1 >= 0 else None

    def next_trade(idx):
        if pd.isna(idx):
            return None
        i = int(idx) - 1
        return trade_dates[i+1] if i + 1 < len(trade_dates) else None

    df['prev_trade_date'] = df['trade_day_index'].apply(prev_trade)
    df['next_trade_date'] = df['trade_day_index'].apply(next_trade)

    con = duckdb.connect('stocks.duckdb')

    con.register('df_view', df)
    con.execute("""
    INSERT INTO dim_a_stock_calendar_all (
        ds, is_trade_day, year, quarter, month, week_of_year, day_of_week,
        day_of_week_cn, day_of_month, is_month_start, is_month_end,
        trade_day_index, prev_trade_date, next_trade_date
    )
    SELECT
        ds, is_trade_day, year, quarter, month, week_of_year, day_of_week,
        day_of_week_cn, day_of_month, is_month_start, is_month_end,
        trade_day_index, prev_trade_date, next_trade_date
    FROM df_view
    """)
    con.unregister('df_view')
    con.close()

    print("股票日期维度表数据插入完成！")