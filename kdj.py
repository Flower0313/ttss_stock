import duckdb
import pandas as pd
import numpy as np

# 连接 DuckDB
con = duckdb.connect("stocks.duckdb")

# SQL取数据
query = """
SELECT
    a.ds,
    a.code,
    a.closing_price,
    a.highest,
    a.lowest
FROM df_a_stock_history_detail_df a 
JOIN df_a_stock_detail_df b 
    ON a.code=b.code 
   AND a.market=b.market 
   AND b.current_price>5 
   AND b.name NOT LIKE '%ST%' 
   AND b.ds='2025-09-24' 
   AND b.board in (2,6)
   AND b.industry NOT LIKE '%房地产%'
WHERE a.ds >= '2025-07-01' 
  AND a.code='000061'
ORDER BY a.ds
"""

df = con.execute(query).fetchdf()

# 转换日期
df["ds"] = pd.to_datetime(df["ds"])

# 计算 RSV
n = 9
df["low_n"] = df["lowest"].rolling(n, min_periods=1).min()
df["high_n"] = df["highest"].rolling(n, min_periods=1).max()
df["RSV"] = (df["closing_price"] - df["low_n"]) / (df["high_n"] - df["low_n"]) * 100

# 初始化 K 和 D
K = [50]
D = [50]

for rsv in df["RSV"].iloc[1:]:
    K.append(2/3 * K[-1] + 1/3 * rsv)
    D.append(2/3 * D[-1] + 1/3 * K[-1])

df["K"] = K
df["D"] = D
df["J"] = 3 * df["K"] - 2 * df["D"]

# 筛选 2025-09 的数据
df_sep = df[(df["ds"] >= "2025-09-01") & (df["ds"] <= "2025-09-30")]

# 打印结果
print(df_sep[["ds", "code", "closing_price", "K", "D", "J"]])
