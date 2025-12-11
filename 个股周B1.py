import duckdb
import pandas as pd
import numpy as np

# 连接 DuckDB
con = duckdb.connect("stocks.duckdb")

# 拉取数据
sql = """
select ts_code as code,trade_date as ds,name,open,high
,low,close,vol as volume,industry
,round((close-pre_close)*100/pre_close,2) as up_down_rate
,round((high - low)* 100 / pre_close,2) as amplitude
from (
	select a.ts_code,a.trade_date,b.name,a.open,a.high,a.low,a.close,a.vol,a.amount
	,lag(a.close,1,a.close) over(partition by a.ts_code order by a.trade_date) as pre_close
	,b.industry,b.act_ent_type
	from df_a_tu_stock_daily_df a
	join dim_a_tu_stock_info_df b on a.ts_code=b.ts_code
	join (select ts_code from df_a_tu_stock_financial_df where trade_date='2025-12-01' and circ_mv>=900000) c on a.ts_code=c.ts_code
	where a.trade_date>='2025-05-01' and industry<>'房产服务' -- and c.circ_mv>=800000 
) a
"""
df = con.execute(sql).df()

df["ds"] = pd.to_datetime(df["ds"])

# ----------- 周线汇总 -----------
def resample_weekly(group):
    weekly = group.resample("W-FRI", on="ds").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "industry": "first"
    }).dropna()
    return weekly

weekly_df = (
    df.groupby("code", group_keys=True)
      .apply(resample_weekly)
      .reset_index()
)

# ----------- 计算KDJ -----------
def calc_kdj(sub_df, n=9):
    low_min = sub_df["low"].rolling(n, min_periods=1).min()
    high_max = sub_df["high"].rolling(n, min_periods=1).max()
    rsv = (sub_df["close"] - low_min) / (high_max - low_min) * 100

    k = rsv.ewm(alpha=1/3, adjust=False).mean()
    d = k.ewm(alpha=1/3, adjust=False).mean()
    j = 3 * k - 2 * d

    sub_df["K"] = k
    sub_df["D"] = d
    sub_df["J"] = j
    return sub_df

weekly_df = weekly_df.groupby("code", group_keys=False).apply(calc_kdj)

# ----------- 在 weekly_df 中加入白线 & 黄线 -----------
def add_lines(sub_df):
    close = sub_df["close"]

    # ---------- 白线（双 EMA(10)） ----------
    sub_df["white_line"] = close.ewm(span=10, adjust=False).mean().ewm(span=10, adjust=False).mean()

    # ---------- 黄线（四条均线的平均） ----------
    ma1 = close.rolling(14, min_periods=1).mean()
    ma2 = close.rolling(28, min_periods=1).mean()
    ma3 = close.rolling(57, min_periods=1).mean()
    ma4 = close.rolling(114, min_periods=1).mean()
    sub_df["yellow_line"] = (ma1 + ma2 + ma3 + ma4) / 4.0

    return sub_df

weekly_df = weekly_df.groupby("code", group_keys=False).apply(add_lines)

# ----------- 在目标日期筛选：收盘价 > 黄线 且 J < 0 -----------
target_date = pd.Timestamp("2025-11-28")

result = weekly_df[
    (weekly_df["ds"] == target_date) &
    (weekly_df["close"] >= weekly_df["yellow_line"]) &
    (weekly_df["white_line"] >= weekly_df["yellow_line"]) &
    (weekly_df["J"] <= 13)
][["code", "ds", "close", "J", "white_line", "yellow_line","industry"]].dropna()

# 排序（例如按J升序）

result = result.sort_values(by=["industry", "J"])
print(result)

con.register("tmp_result", result)
con.execute(f"DELETE FROM df_a_stock_week_b1_df")
con.execute("""
INSERT INTO df_a_stock_week_b1_df
SELECT code, ds
FROM tmp_result
""")
con.commit()

# 保存到 Excel 文件
#output_file = "weekly_kdj_b1.xlsx"

# index=False 表示不写行索引
#result.to_excel(output_file, index=False)

#print(f"结果已保存到 {output_file}")



