# _*_ coding : utf-8 _*_
# @Time : 2022/5/28 - 13:02
# @Author : Holden
# @File : get__a_stock_kline
# @Project : python
# 首次执行即可
import datetime
import random
import time
import threading

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import duckdb
import pandas as pd

# 创建线程锁，确保数据库写入安全
db_lock = threading.Lock()


def get_all_kline(market_code_str):
    #print(f"线程 {threading.current_thread().name} 抓取 {market_code_str}...")

    # 每个线程创建自己的数据库连接
    con = duckdb.connect('../stocks.duckdb')

    # 必须放在线程里面
    params = {
        'secid': market_code_str,
        'ut': '7eea3edcaed734bea9cbfc24409ed989',
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': '101',  # 日k线
        'fqt': '1',  # 1前复权
        'beg': '20200101',  # 大于等于
        'end': '20251128',  # 小于等于
        '_': str(int(datetime.datetime.now().timestamp() * 1000)),
    }

    cookies = {
        'qgqp_b_id': 'cbaf91ab2a32ba754f9ff83b95ca890b',
        'st_nvi': 'egZQSMgtTClr1cqkCuwnXe6c6',
        'nid': '03758b4bcce6f57e45d38a2ba747596b',
        'nid_create_time': '1756130511737',
        'gvi': 'zUCjQOcq0sJpK-BBB5Cmr5255',
        'gvi_create_time': '1756130511737',
        'mtp': '1',
        'sid': '131578758',
        'vtpst': '|',
        'emshistory': '%5B%22xd%E8%B5%9B%E5%8A%9B%E6%96%AF%22%2C%22%E5%A4%A9%E5%92%8C%E7%A3%81%E6%9D%90%22%5D',
        'ct': 'FT3ki4qamnZ3ZBIYTkMjBtfGHflrYMBSxOzaJuay1ZmCuv0COdNwch5ksCpoYJYbdeEDuBLLJm-O_fn0jqgwh7W2U1WZZJ81TAFGixXeQT7mIWBrUs5NzFaOTMwfLHtVG6hBF1j2RDyTrDKXAjobDfZo0OajUQMz3NxN9XGUiLE',
        'ut': 'FobyicMgeV4n4cCT8MJKZOhRZpPnvGrbz_B_nVAvVyBqu1HKh8C_X-DZIbMu0e-69SQKPieUWG7iAtaVxvX2gHxvUsl0Ij-Dx93k5eQgcELPnYGC38fl2w37MaBNh7wkezhFuExwif1d0VqnJCI_ppvxQqVdpMja08rqvec05cwi8d9ITNGrl74x3SiqaZ-shoxBo22_C0p-qJqrNwPXofKOtdA6FsCIvMZ0euqvJBoAMdSX2-_YZvdXb1yjNE8AhdQ77Pabq_y9OrC-1CeWFe87Zts2Gt1f',
        'websitepoptg_api_time': '1763391719063',
        'fullscreengg': '1',
        'fullscreengg2': '1',
        'st_si': '41195943705914',
        'st_asi': 'delete',
        'st_pvi': '80850450855664',
        'st_sp': '2025-08-25%2022%3A01%3A51',
        'st_inirUrl': 'https%3A%2F%2Fwww.baidu.com%2Flink',
        'st_sn': '14',
        'st_psi': '20251118212432763-113200301201-9612812933',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6',
        'Connection': 'keep-alive',
        'Referer': 'https://quote.eastmoney.com/',
        'Sec-Fetch-Dest': 'script',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    # 日k线
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    try:
        response = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=15)
        response.raise_for_status()
        result_json = response.json().get('data')
        if not result_json or 'klines' not in result_json:
            return 0  # 无数据

        records = []
        market = result_json.get('market')
        code = result_json.get('code')
        name = result_json.get('name')

        for k in result_json['klines']:
            # k形如 "2023-08-10,15.24,15.40,15.11,15.37,309303,4725160,0.00,0.00,0.00,0.00"
            klines = k.split(',')
            record = (
                market,
                code,
                name,
                float(klines[8]),  # up_down_rate
                float(klines[9]),  # up_down_amount
                float(klines[10]),  # turnover_rate
                float(klines[7]),  # amplitude
                float(klines[3]),  # highest
                float(klines[4]),  # lowest
                float(klines[1]),  # opening_price
                float(klines[2]),  # closing_price
                float(klines[5]),  # deal_amount
                float(klines[6]),  # deal_vol
                klines[0]
            )
            records.append(record)

        # 转成DataFrame批量写入
        df = pd.DataFrame(records, columns=[
            'market', 'code', 'name', 'up_down_rate', 'up_down_amount', 'turnover_rate', 'amplitude',
            'highest', 'lowest', 'opening_price', 'closing_price', 'deal_vol','deal_amount',
            'ds'
        ])

        # 使用线程锁确保数据库写入安全
        with db_lock:
            # 批量插入
            con.register('tmp_kline_df', df)
            con.execute('''
                    INSERT INTO df_a_stock_history_detail_df 
                    SELECT market,
                            code,
                            name,
                            up_down_rate,
                            up_down_amount,
                            turnover_rate,
                            amplitude,
                            highest,
                            lowest,
                            opening_price,
                            closing_price,
                            deal_amount,
                            deal_vol,
                            ds FROM tmp_kline_df
                ''')
            con.commit()

        # 随机延时，避免请求过于频繁
        time.sleep(random.uniform(0.2, 0.5))
        return len(records)
    except Exception as e:
        print(f"Error fetching {market_code_str}: {e}")
        time.sleep(2)  # 错误时短暂等待
        return 0
    finally:
        con.close()


def main():
    con = duckdb.connect('../stocks.duckdb')

    # 先查询所有 market 和 code
    query_result = con.execute(
        "select a.market,a.code from df_a_stock_detail_df a left join df_a_stock_history_detail_df b on a.code=b.code where a.ds='2025-11-28' and a.board in (2,6) and current_price>0 and b.code is null;").fetchall()
    con.close()

    print(f"准备抓取 {len(query_result)} 只股票的历史K线数据...")

    # 构建任务列表
    tasks = []
    for market, code in query_result:
        key = f"{market}.{code}"
        tasks.append(key)

    success_count = 0
    # 使用线程池，最大线程数为20
    with ThreadPoolExecutor(max_workers=30, thread_name_prefix="StockWorker") as executor:
        # 提交所有任务
        future_to_stock = {executor.submit(get_all_kline, task): task for task in tasks}

        # 处理完成的任务
        for future in as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                count = future.result()
                success_count += count
                print(f"抓取 {stock} 完成，获取 {count} 条K线数据，总计成功写入 {success_count} 条")
            except Exception as exc:
                print(f"{stock} 抓取失败: {exc}")

    print("历史K线抓取写入完成")


if __name__ == '__main__':
    main()