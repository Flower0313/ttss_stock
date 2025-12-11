import duckdb
import pandas as pd
import numpy as np

# 连接 duckdb
con = duckdb.connect("stocks.duckdb")

# 取数据（这里按需加 WHERE code = '600519'，或者取全部股票）
query = """
SELECT code, ds, closing_price, highest, lowest
FROM df_a_stock_history_detail_df
WHERE ds >= '2025-01-01' and code='603259'
ORDER BY code, ds
"""
df = con.execute(query).df()

# 定义碗状检测函数
def is_bowl(prices, tolerance=0.02, min_drop=0.05, min_rebound=0.05):
    """
    判断 15 日股价是否碗状
    prices: 长度为15的收盘价序列
    tolerance: 二次拟合相对误差阈值
    min_drop: 从碗沿到碗底的最小跌幅
    min_rebound: 从碗底到右沿的最小涨幅
    """
    x = np.arange(len(prices))
    y = np.array(prices)

    # 二次拟合
    coeffs = np.polyfit(x, y, 2)
    a, b, c = coeffs

    # 必须开口向上
    if a <= 0:
        return False

    # 拟合误差
    y_fit = np.polyval(coeffs, x)
    error = np.mean(np.abs((y - y_fit) / y))
    if error >= tolerance:
        return False

    # 确认碗口深度：左沿 -> 底 -> 右沿
    left, mid, right = y[0], y[len(y)//2], y[-1]
    drop = (mid - left) / left  # 碗沿到碗底跌幅
    rebound = (right - mid) / mid  # 碗底到右沿涨幅

    if drop > -min_drop or rebound < min_rebound:
        return False

    return True

# 在每个股票上滑窗查找
window = 15
results = []

for code, group in df.groupby("code"):
    prices = group["closing_price"].values
    dates = group["ds"].values

    for i in range(len(group) - window + 1):
        segment = prices[i:i+window]
        if is_bowl(segment):
            results.append({
                "code": code,
                "start_date": dates[i],
                "end_date": dates[i+window-1],
                "bottom_date": dates[i + window//2],
                "left_price": segment[0],
                "bottom_price": segment[window//2],
                "right_price": segment[-1],
                "drop_pct": (segment[window//2] - segment[0]) / segment[0] * 100,
                "rebound_pct": (segment[-1] - segment[window//2]) / segment[window//2] * 100
            })

# 输出结果
results_df = pd.DataFrame(results)
print(results_df)
