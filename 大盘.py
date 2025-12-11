# _*_ coding : utf-8 _*_
# @Time : 2022/5/28 - 13:02
# @Author : Holden
# @File : get__a_stock_kline
# @Project : python
# 首次执行即可
import datetime
import time

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import duckdb
import pandas as pd


def get_all_kline(market_code_str, con):
    print(market_code_str, "抓取中...")

    # 必须放在线程里面
    params = {
        'secid': '1.000001',
        'ut': '7eea3edcaed734bea9cbfc24409ed989',
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': '101',  # 日k线
        'fqt': '1',  # 1才是真实的k线
        'beg': '20250923',  # 大于等于
        'end': '20251117',  # 小于等于
        '_': str(int(datetime.datetime.now().timestamp() * 1000)),
    }

    cookies = {
        'qgqp_b_id': '110276fdc7a31f5dee37aad9fb4e446a',
        'ct': 'EEIhWo9zu9jtBAmjdkl2YBRq82hgRuqso3qChECmv0WJfiFuRl8IOgcthlInlYMJdK2pkiqhGmhbVqhoDy02sHmRPgKov-Lk48OiVgIeJUzp6emLUbvnf3sHgkTWzT-rYFqHMON2RgEiToOW-O_57LMljhaOPEi0JsXGTcH44ks',
        'ut': 'FobyicMgeV6SKrOicruKo72Vo--axxKGABvmM2iTa7d3SGZEFeomi-JiqMabzBGSKGMZO-TLub02PbweePxPh04gi6Do7TremUC4fA38SLZd20Fkf-QpmUa_u5Y4Bl8J_14Il46DHC2klVCCOs86R7lHzLiP1_RUTrpafGwDKEv01WS_BfOWii0_37rE02Vlnco26NzSNOZJOmw4sbU8fmvEHu8Th0Xto71KwXxlKa4rXNnxXoGYW3V4hWGzd58AoH4BNuO8jfcyE8JZKvlEMIQ2MlJ16gkGketQ4R-6Ygk',
        'uidal': '4819115464066356Holdenxiao',
        'sid': '131578758',
        'vtpst': '%7c',
        'em_hq_fls': 'js',
        'qquestionnairebox': '1',
        'em-quote-version': 'topspeed',
        'st_si': '32426815590998',
        'st_asi': 'delete',
        'HAList': 'a-sz-300010-%u8C46%u795E%u6559%u80B2%2Ca-sh-600962-%u56FD%u6295%u4E2D%u9C81%2Cty-0-430510-%u4E30%u5149%u7CBE%u5BC6%2Cty-0-835640-%u5BCC%u58EB%u8FBE%2Cty-0-833266-%u751F%u7269%u8C37%2Ca-sh-600721-%u767E%u82B1%u6751%2Ca-sz-002213-%u5927%u4E3A%u80A1%u4EFD%2Ca-sz-003032-%u4F20%u667A%u6559%u80B2%2Ca-sz-002523-%u5929%u6865%u8D77%u91CD%2Ca-sz-300059-%u4E1C%u65B9%u8D22%u5BCC%2Ca-sz-002821-%u51EF%u83B1%u82F1',
        'st_pvi': '99298644841036',
        'st_sp': '2022-05-13%2009%3A41%3A33',
        'st_inirUrl': 'https%3A%2F%2Fwww.baidu.com%2Flink',
        'st_sn': '48',
        'st_psi': '20220528130858545-113200301201-6079724835',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Referer': 'http://quote.eastmoney.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
    }

    # 日k线
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
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
            'highest', 'lowest', 'opening_price', 'closing_price', 'deal_amount', 'deal_vol',
            'ds'
        ])

        # 批量插入
        con.register('tmp_kline_df', df)
        con.execute('''
                INSERT INTO df_a_stock_history_detail_df 
                SELECT * FROM tmp_kline_df
            ''')
        con.commit()
        time.sleep(2)
        return len(records)
    except Exception as e:
        print(f"Error fetching {market_code_str}: {e}")
        time.sleep(5)
        return 0


def main():
    con = duckdb.connect('stocks.duckdb')

    # 先查询所有 market 和 code
    query_result = con.execute("select distinct 1 as market,'000001' as code").fetchall()

    print(f"准备抓取 {len(query_result)} 只股票的历史K线数据...")

    success_count = 0
    for market, code in query_result:
        key = f"{market}.{code}"
        count = get_all_kline(key, con)
        success_count += count
        print(f"抓取 {count} 条K线数据，总计成功写入 {success_count} 条")

    print("历史K线抓取写入完成")


if __name__ == '__main__':
    main()
