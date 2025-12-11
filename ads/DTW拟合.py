import duckdb
import pandas as pd
import numpy as np
import sys
from multiprocessing import Pool, cpu_count
from functools import partial
import time

DB_PATH = "../stocks.duckdb"
TABLE = "df_a_bao_stock_daily_df"

TARGET_CODE = "sh.603355"
TARGET_START = "2025-10-14"
TARGET_END   = "2025-11-07"

# 筛选阈值
MIN_CLOSE_CORR = 0.7
MIN_VOL_CORR   = 0.5

# 并行配置
NUM_WORKERS = max(1, cpu_count() - 1)

# DTW / 相似度权重（可调整）
W_CLOSE_CORR = 0.30
W_CLOSE_DTW  = 0.20
W_VOL_CORR   = 0.30
W_VOL_DTW    = 0.20

def dtw_distance(a, b):
    na = len(a)
    nb = len(b)
    # 使用较小内存的实现
    # 初始化
    INF = 1e18
    D = np.full((na + 1, nb + 1), INF, dtype=np.float64)
    D[0, 0] = 0.0
    for i in range(1, na + 1):
        ai = a[i - 1]
        # 行向量更新
        for j in range(1, nb + 1):
            cost = (ai - b[j - 1]) ** 2
            D[i, j] = cost + min(D[i - 1, j],    # insertion
                                 D[i, j - 1],    # deletion
                                 D[i - 1, j - 1])# match
    return float(np.sqrt(D[na, nb]))  # 返回平方和的根作为距离

def dtw_to_similarity(dtw_dist, length):
    if length <= 0:
        return 0.0
    return 1.0 / (1.0 + dtw_dist / float(length))

def process_one_code(code, g_df, base_close_arr, base_vol_arr, window):
    """
    处理单只股票：扫描其所有滑动窗口并返回满足筛选条件的结果列表。
    返回：list of dict
    """
    results_local = []
    g = g_df.sort_values("date").reset_index(drop=True)
    n = len(g)
    if n < window:
        return results_local

    closes = g["close"].values
    vols   = g["volume"].values

    # 逐窗口扫描
    for i in range(0, n - window + 1):
        w_close = closes[i:i + window]
        w_vol   = vols[i:i + window]

        # 排除方差为0或无效序列
        if np.std(w_close) == 0 or np.std(w_vol) == 0:
            continue

        # 标准化（z-score）
        w_close_norm = (w_close - w_close.mean()) / (w_close.std() if w_close.std() != 0 else 1.0)
        w_vol_norm   = (w_vol   - w_vol.mean())   / (w_vol.std()   if w_vol.std()   != 0 else 1.0)

        # 相关系数（pearson）
        try:
            r_close = np.corrcoef(base_close_arr, w_close_norm)[0, 1]
        except Exception:
            r_close = 0.0
        try:
            r_vol = np.corrcoef(base_vol_arr, w_vol_norm)[0, 1]
        except Exception:
            r_vol = 0.0

        # # 满足过滤阈值（相关性门槛）
        # if r_close < MIN_CLOSE_CORR or r_vol < MIN_VOL_CORR:
        #     continue

        # DTW 距离及相似度（为稳定性，我们在 DTW 前用未标准化序列或标准化序列都可；这里对标准化序列使用）
        dtw_close_dist = dtw_distance(base_close_arr, w_close_norm)
        dtw_vol_dist   = dtw_distance(base_vol_arr,   w_vol_norm)

        dtw_close_sim = dtw_to_similarity(dtw_close_dist, window)
        dtw_vol_sim   = dtw_to_similarity(dtw_vol_dist,   window)

        # 综合得分（权重可调）
        similarity = (W_CLOSE_CORR * r_close +
                      W_CLOSE_DTW  * dtw_close_sim +
                      W_VOL_CORR   * r_vol +
                      W_VOL_DTW    * dtw_vol_sim)

        results_local.append({
            "code": code,
            "start_date": g.loc[i, 'date'],
            "end_date":   g.loc[i + window - 1, 'date'],
            "similarity": similarity,
            "close_corr": float(r_close),
            "vol_corr": float(r_vol),
            "dtw_close_dist": float(dtw_close_dist),
            "dtw_vol_dist": float(dtw_vol_dist),
            "dtw_close_sim": float(dtw_close_sim),
            "dtw_vol_sim": float(dtw_vol_sim)
        })

    return results_local

# =====================================
# 3. 主流程
# =====================================
def main():
    t0 = time.time()
    con = duckdb.connect(DB_PATH)

    # 读取基准序列
    base = con.execute(f"""
        select date, close, volume
        from {TABLE}
        where code = '{TARGET_CODE}'
          and date between '{TARGET_START}' and '{TARGET_END}'
        order by date
    """).df()

    if len(base) == 0:
        raise ValueError("目标区间没有数据！")

    window = len(base)
    print(f"目标窗口长度: {window} 天")

    # 基准使用标准化序列（z-score）
    base_close_arr = (base['close'].values - base['close'].mean()) / (base['close'].std() if base['close'].std() != 0 else 1.0)
    base_vol_arr   = (base['volume'].values - base['volume'].mean()) / (base['volume'].std() if base['volume'].std() != 0 else 1.0)

    # 读取全表（按 code, date 排序）
    print("正在加载全市场数据...")
    df = con.execute(f"""
        select code, date, close, volume
        from {TABLE} where date>='2025-06-01'
        order by code, date
    """).df()

    all_codes = df['code'].unique().tolist()
    total_codes = len(all_codes)
    print(f"共发现 {total_codes} 只股票可扫描。 使用 {NUM_WORKERS} 个 worker 并行处理。\n")

    # 为并行准备每个 code 的子 DataFrame（避免在子进程中再去全表筛选）
    groups = {code: grp.copy() for code, grp in df.groupby("code")}

    # partial 函数：固化 base arr & window
    worker_func = partial(process_one_code,
                          base_close_arr=base_close_arr,
                          base_vol_arr=base_vol_arr,
                          window=window)

    results = []
    processed = 0

    with Pool(processes=NUM_WORKERS) as pool:
        tasks = ((code, groups[code]) for code in all_codes)
        # 将 process_one_code 直接作为顶层函数调用（因为 partial 固化了其他参数）
        for res in pool.starmap(worker_func, tasks):
            processed += 1
            if res:
                results.extend(res)

    print("\n\n并行扫描完成。用时 {:.1f} 秒。".format(time.time() - t0))

    if len(results) == 0:
        raise ValueError("没有符合筛选条件的相似序列！")

    # 合并成 DataFrame 并排序
    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values("similarity", ascending=False).reset_index(drop=True)

    # 输出 top20 到控制台
    print("\n===== 最相似 Top 20（含 DTW 信息） =====")
    print(result_df.head(20).to_string(index=False))

    # 保存 CSV
    out_csv = "similar_stocks_result_filtered_dtw.csv"
    result_df.to_csv(out_csv, index=False)
    print(f"\n结果已保存到 {out_csv}")

if __name__ == "__main__":
    main()
