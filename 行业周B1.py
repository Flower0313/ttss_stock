import duckdb
import pandas as pd
import numpy as np

# 连接 DuckDB
con = duckdb.connect("stocks.duckdb")

# 拉取数据
sql = """
SELECT
    a.ds,
    a.name,
    a.opening_price AS open,
    a.closing_price AS close,
    a.highest AS high,
    a.lowest AS low,
    a.deal_vol AS volume
FROM df_a_industry_sector_history_df a 
WHERE a.ds >= '2025-04-01' 
ORDER BY a.ds
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
        "volume": "sum"
    }).dropna()
    return weekly

weekly_df = (
    df.groupby("name", group_keys=True)
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

weekly_df = weekly_df.groupby("name", group_keys=False).apply(calc_kdj)

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

weekly_df = weekly_df.groupby("name", group_keys=False).apply(add_lines)


# ----------- 在目标日期筛选：收盘价 > 黄线 且 J < 0 -----------
target_date = pd.Timestamp("2025-11-28")

result = weekly_df[
    (weekly_df["ds"] == target_date) &
    # (weekly_df["close"] < weekly_df["white_line"]) &
    (weekly_df["close"] >= weekly_df["yellow_line"]) &
    (weekly_df["J"] < 13)
][["name", "ds", "close", "K", "D", "J", "white_line", "yellow_line"]].dropna()

# 排序（例如按J升序）

result = result.sort_values(by=["J"])
print(result)

# 保存到 Excel 文件
#output_file = "weekly_kdj_b1.xlsx"

# index=False 表示不写行索引
#result.to_excel(output_file, index=False)

#print(f"结果已保存到 {output_file}")



