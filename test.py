import random
import time

import akshare as ak
import pandas as pd
import duckdb

con = duckdb.connect("stocks.duckdb")
data = con.execute("select distinct a.code from df_a_stock_detail_df a left join tmp_stock_metrics b on a.code=b.code where a.ds='2025-12-10' and a.current_price>0 and b.code is null").fetchdf()
codes = data['code'].tolist()


# insert into df_a_tu_stock_financial_df
# select b.ts_code,ds,close_price as close,0,0,0,pe_static,pe_ttm,pb,0 as ps,ps_ratio,0,0,total_shares/10000,circ_shares/10000,0,total_mv/10000,circ_mv/10000
# from tmp_stock_metrics a left join dim_a_tu_stock_info_df b on a.code=b.symbol
# where ds<='2025-10-30'

for i in codes:
    try:
        print(f"{i} 正在抓取中...")

        # AKShare 获取数据
        stock_value_em_df = ak.stock_value_em(symbol=i)

        # 如果 API 返回空表
        if stock_value_em_df is None or stock_value_em_df.empty:
            print(f"⚠ {i} 返回空数据，已跳过")
            continue

        # 字段重命名
        df = stock_value_em_df.rename(columns={
            '数据日期': 'ds',
            '当日收盘价': 'close_price',
            '当日涨跌幅': 'pct_change',
            '总市值': 'total_mv',
            '流通市值': 'circ_mv',
            '总股本': 'total_shares',
            '流通股本': 'circ_shares',
            'PE(TTM)': 'pe_ttm',
            'PE(静)': 'pe_static',
            '市净率': 'pb',
            'PEG值': 'peg',
            '市现率': 'ps_cashflow',
            '市销率': 'ps_ratio'
        })

        # 加 code 字段
        df['code'] = i

        # 排序字段
        df = df[['ds', 'code', 'close_price', 'pct_change', 'total_mv', 'circ_mv',
                 'total_shares', 'circ_shares', 'pe_ttm', 'pe_static', 'pb',
                 'peg', 'ps_cashflow', 'ps_ratio']]

        #time.sleep(random.uniform(0.5, 1.4))
        # 插入 DuckDB：需要注册临时视图
        con.register("tmp_df", df)
        con.execute("INSERT INTO tmp_stock_metrics SELECT * FROM tmp_df")
        con.unregister("tmp_df")

    except Exception as e:
        print(f"❌ {i} 抓取/插入失败：{e}")
        continue
