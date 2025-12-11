import duckdb
import pandas as pd
import numpy as np
from scipy.signal import find_peaks


def find_wave_peaks_with_relative_gain(prices, left=3, right=3, distance=None):
    """
    查找局部波峰，并计算当前波峰与前一个波峰之间的最低点到当前波峰的涨幅
    prices: 收盘价序列
    left, right: 用于局部峰值判断的左右比较天数
    distance: 波峰之间最小间隔
    返回波峰索引列表和对应涨幅列表
    """
    prices = np.array(prices)
    if distance is None:
        distance = max(left, right) + 1

    # 初步找波峰
    peaks, _ = find_peaks(prices, distance=distance)

    # 局部左右比较
    final_peaks = []
    for idx in peaks:
        if idx < left or idx > len(prices) - right - 1:
            continue
        if prices[idx] > max(prices[idx - left:idx]) and prices[idx] > max(prices[idx + 1:idx + 1 + right]):
            final_peaks.append(idx)

    # 计算涨幅：当前波峰 vs 前一个波峰之间最低点
    gains = []
    for i, peak_idx in enumerate(final_peaks):
        if i == 0:
            # 第一个波峰可以取从起点到波峰的最低价
            low = prices[:peak_idx].min() if peak_idx > 0 else prices[peak_idx]
        else:
            # 前一个波峰索引到当前波峰索引之间的最低价
            low = prices[final_peaks[i - 1]:peak_idx].min()
        gain = (prices[peak_idx] - low) / low * 100
        gains.append(gain)

    return final_peaks, gains


# ---------------------------
# 示例：查询股票数据
# ---------------------------
con = duckdb.connect("stocks.duckdb")
stock_code = '600519'

df = con.execute(f"""
    SELECT ds, closing_price
    FROM df_a_stock_history_detail_df
    WHERE code = '{stock_code}'
      AND ds >= '2025-01-01'
    ORDER BY ds
""").df()

prices = df['closing_price'].values

# 查找波峰及相对涨幅
peaks_idx, gains = find_wave_peaks_with_relative_gain(prices, left=3, right=3)

# 输出波峰日期、价格和前波峰最低点到当前波峰的涨幅
wave_peaks = df.iloc[peaks_idx][['ds', 'closing_price']].copy()
wave_peaks['gain_from_prev_peak_min(%)'] = gains
print("每个波峰及与前一个波峰之间最低点的涨幅:")
print(wave_peaks)
