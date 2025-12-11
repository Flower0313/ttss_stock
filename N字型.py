import duckdb
import pandas as pd
from scipy.signal import find_peaks

# 连接 DuckDB
con = duckdb.connect("stocks.duckdb")

# 取股票数据
df = con.execute("""
    SELECT ds, closing_price
    FROM df_a_stock_history_detail_df
    WHERE code = '603259'
      AND ds >= '2024-01-01'
    ORDER BY ds
""").fetchdf()

df['ds'] = pd.to_datetime(df['ds'])

# 找波峰和波谷
peaks, _ = find_peaks(df['closing_price'], distance=3)
troughs, _ = find_peaks(-df['closing_price'], distance=3)

results = []
for i in range(len(peaks)-1):
    peak1, peak2 = peaks[i], peaks[i+1]

    # 找两个波峰之间的波谷
    possible_troughs = [t for t in troughs if peak1 < t < peak2]
    if not possible_troughs:
        continue
    trough = min(possible_troughs, key=lambda x: df.loc[x, 'closing_price'])

    # 确认 N 型（后峰高于前峰）
    if df.loc[peak2, 'closing_price'] > df.loc[peak1, 'closing_price']:
        results.append({
            "trough_date": df.loc[trough, 'ds'].date(),
            "trough_price": df.loc[trough, 'closing_price'],
            "prev_peak_date": df.loc[peak1, 'ds'].date(),
            "next_peak_date": df.loc[peak2, 'ds'].date()
        })

trough_df = pd.DataFrame(results)
print(trough_df)
