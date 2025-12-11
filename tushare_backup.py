# _*_ coding: utf-8 _*_
# @Author : Holden
# @File   : get_tushare_daily_insert_duckdb.py
# @Desc   : 从 Tushare 获取指定交易日 A 股日线行情并写入 DuckDB

import tushare as ts
import pandas as pd
import duckdb

# ===== 1. 初始化 Tushare API =====
ts.set_token("0d46c0471fac9d57ace59a6c4b2b7498305a0eb52c6cd382c4fdbf7e")
pro = ts.pro_api()

# ===== 2. 目标日期 =====
target_date = "20251030"

# ===== 3. 获取指定日期的日线行情 =====
print(f"📡 正在获取 {target_date} 的日线行情...")
df = pro.daily(trade_date=target_date)

if df.empty:
    print(f"⚠️ {target_date} 无数据返回，请检查是否为交易日。")
    exit()

print(f"✅ 获取成功，共 {len(df)} 条记录")

# ===== 4. 数据预处理 =====
# Tushare 返回 trade_date 格式为 'YYYYMMDD'，转为 DATE 类型便于 DuckDB 查询
df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

# ===== 5. 连接 DuckDB =====
con = duckdb.connect("stocks.duckdb")

# ===== 6. 创建表（如果不存在） =====
con.execute("""
CREATE TABLE IF NOT EXISTS df_a_stock_daily_tushare_df (
    ts_code       VARCHAR,      -- 股票代码，例如 600519.SH
    trade_date    DATE,         -- 交易日期
    open          DOUBLE,       -- 开盘价
    high          DOUBLE,       -- 最高价
    low           DOUBLE,       -- 最低价
    close         DOUBLE,       -- 收盘价
    pre_close     DOUBLE,       -- 昨收价
    change        DOUBLE,       -- 涨跌额
    pct_chg       DOUBLE,       -- 涨跌幅（百分比）
    vol           DOUBLE,       -- 成交量（手）
    amount        DOUBLE
);
""")

# ===== 7. 注册 DataFrame 并插入 =====
con.register("tmp_daily_df", df)

con.execute("""
INSERT INTO df_a_stock_daily_tushare_df (
    ts_code, trade_date, open, high, low, close, pre_close,
    change, pct_chg, vol, amount
)
SELECT 
    ts_code, trade_date, open, high, low, close, pre_close,
    change, pct_chg, vol, amount
FROM tmp_daily_df
""")

con.commit()
print(f"✅ 已成功插入 {len(df)} 条记录至 df_a_stock_daily_tushare_df")

# ===== 8. 可选：检查表数据 =====
check_df = con.execute("""
SELECT COUNT(*) AS cnt, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date 
FROM df_a_stock_daily_tushare_df
""").fetchdf()
print("\n📊 表中当前数据概览：")
print(check_df)

# ===== 9. 关闭连接 =====
con.close()
