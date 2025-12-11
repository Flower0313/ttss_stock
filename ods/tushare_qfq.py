# 前复权
import random
import time
import akshare as ak
import pandas as pd
import tushare as ts
import duckdb

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)  # 显示所有行（注意太大可能卡）
pd.set_option('display.width', 1000)  # 设置显示宽度，防止自动换行
from datetime import datetime


# tushare 历史日线
def tushare_qfq_all(con, code, switch):
    # CREATE TABLE df_a_tu_qfq_stock_daily_df(code VARCHAR, date VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, "close" DOUBLE, preclose DOUBLE, change DOUBLE, pctchg DOUBLE, volume DOUBLE, amount DOUBLE);
    ts.set_token('0d46c0471fac9d57ace59a6c4b2b7498305a0eb52c6cd382c4fdbf7e')
    df = None

    if switch:
        # 历史
        df = ts.pro_bar(ts_code=code, adj='qfq', start_date='20200101', end_date='20251231')
    else:
        # 每日
        df = ts.pro_bar(ts_code=code, adj='qfq', start_date=datetime.today().strftime('%Y%m%d'),
                        end_date=datetime.today().strftime('%Y%m%d'))

    if df.empty:
        return df

    df = df.rename(columns={
        'ts_code': 'code',
        'trade_date': 'date',
        'pct_chg': 'pctchg',
        'vol': 'volume'
    })

    # 转换日期格式 YYYYMMDD → YYYY-MM-DD
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # 保留需要的列，顺序严格对应 DuckDB 表
    insert_cols = ['code', 'date', 'open', 'high', 'low', 'close', 'pre_close',
                   'change', 'pctchg', 'volume', 'amount']

    # 如果某列不存在，补空值
    for col in insert_cols:
        if col not in df.columns:
            df[col] = None

    df = df[insert_cols]

    # 类型转换
    numeric_cols = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pctchg', 'volume', 'amount']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    print("正在抓取>>>" + code)
    con.from_df(df).insert_into('df_a_tu_qfq_stock_daily_df')
    con.commit()


if __name__ == '__main__':
    con = duckdb.connect("../stocks.duckdb")
    codes_df = con.execute(
        "SELECT distinct ts_code FROM dim_a_tu_stock_info_df where market='主板'").fetchdf()
    codes_list = codes_df['ts_code'].tolist()  # 转为列表
    for stock_code in codes_list:
        time.sleep(random.uniform(0.5, 2.5))
        df = tushare_qfq_all(con, stock_code, True)

        # https://tushare.pro/document/2?doc_id=372 实时日线
