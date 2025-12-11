# _*_ coding : utf-8 _*_
# @Time : 2022/5/28 - 13:02
# @Author : Holden
# @File : get__a_stock_kline
# @Project : python
# 首次执行即可
import time
from datetime import datetime
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import duckdb
import pandas as pd


def get_all_kline(market_code_str, con):
    print(market_code_str, "抓取中...")

    # 必须放在线程里面
    params = {
        'secid': market_code_str,
        'ut': '7eea3edcaed734bea9cbfc24409ed989',
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': '101',  # 日k线
        'fqt': '1',  # 1才是真实的k线
        'beg': '19900101',#datetime.today().strftime("%Y%m%d"),  # 大于等于
        'end': datetime.today().strftime("%Y%m%d"),  # 小于等于
        '_': str(int(datetime.now().timestamp() * 1000)),
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
                INSERT INTO df_a_concept_sector_history_df 
                SELECT * FROM tmp_kline_df
            ''')
        con.commit()
        time.sleep(2)
        return len(records)
    except Exception as e:
        print(f"Error fetching {market_code_str}: {e}")
        time.sleep(5)
        return 0


def get_all_sector():


    cookies = {
        'qgqp_b_id': 'cbaf91ab2a32ba754f9ff83b95ca890b',
        'st_nvi': 'egZQSMgtTClr1cqkCuwnXe6c6',
        'nid': '03758b4bcce6f57e45d38a2ba747596b',
        'nid_create_time': '1756130511737',
        'gvi': 'zUCjQOcq0sJpK-BBB5Cmr5255',
        'gvi_create_time': '1756130511737',
        'emshistory': '%5B%22%E5%A4%A9%E5%92%8C%E7%A3%81%E6%9D%90%22%5D',
        'mtp': '1',
        'ct': 'RkylImPwOtnhu7i2Asu8N6BtxYnQ3VK1EAQDPAwiEo5flTROdZJVRrydVCyVIPDRTwaYiul6irugzVYmBXIud_56dUvB7oE-ixbaT-6ATGK9PC2QRn6ZVayAvoGKwpjrfp5vSlKeQNsKqcBanZJIzvse5DOoyicw-DJZ37AquRw',
        'ut': 'FobyicMgeV48bu-ZsQ-sXrbEzg_UMHw1FoX8wQInZ8XGQ21IyJPfgYB-CO_h6ST4vb71VTgw2beGqo6IZA2kJyaLY_tuUHX-LstgZ6t3jSBMoBCuY2fKrTO-Giz_nyTSwZ9QZttQaEn0knDW8Ldy7rlnNglaxL2Su7oztpM4qrCYm3z6ut-Q9Xh2xQTG7K7tJfT50REKTo958Tayi59gao0D7cLz7KQvXWzcPJCWVwxkh5WO4gqJQcKVDjzftAsvxIjt1z6OyRZljhfgMK-5M0pa-YPJE1ZM0s5zihM7lYPsOd2YiXhEnShFPaMiDKgW5dM0ndeoKvstpfd1MOK7Jisdse0TbGbSuqR2rFsfwQZQoyZTzKdbpQm5KC_5PEmDP83NrCrHL7BQAC_0lS4yqQXDrKyZ36dsB2WFqW4ddCZ__gQrxKYzC0r7aPt8srx6vT0DylkTe3tx7FLqGbXf5_sjB4O0vkpviQyF9-YI0Tkb_ZjEVXFUKNjd6PXW3Oz7',
        'pi': '4819115464066356%3Bu4819115464066356%3BHoldenxiao%3BXOjFHhU95Al7QPafd0SksxHBzMgacuF30mnW2ddc2ylchsWfVFF5%2BwIqC8GnUXf85kGv5%2B3c48Y8gc00ZMee46%2B1IsrpYEVDar8gT53g8WF2OMwn6m9mIb35YB3RQbEtoOKQ87xp3Tl9PNLyKDdtYyPBnXDJrQMy8rozqcbCMFxhQ4jy90jUWyNSNqzaHfC1bNelSF8%2F%3BOO4KNUFIVKqhOK8%2FQysPSHUz0AICo2dglmgucFSbjucCFBKDiRasxg5uAC5VslVC6jv5CJpljqJaxX7gFx4%2FLLs1Sje%2BxbIViSKxlC5er6t6kHDhR%2F7E%2BsIGKf5DmjvImXttRmB53EK5easX8ZQZv5Su8ov9qw%3D%3D',
        'uidal': '4819115464066356Holdenxiao',
        'sid': '131578758',
        'vtpst': '|',
        'websitepoptg_api_time': '1758432536869',
        'st_si': '67385033325354',
        'st_asi': 'delete',
        'fullscreengg': '1',
        'fullscreengg2': '1',
        'st_pvi': '80850450855664',
        'st_sp': '2025-08-25%2022%3A01%3A51',
        'st_inirUrl': 'https%3A%2F%2Fwww.baidu.com%2Flink',
        'st_sn': '14',
        'st_psi': '20250921140026774-113200301353-4454559763',
    }

    BASE_URL = "https://push2.eastmoney.com/api/qt/clist/get"

    page = 1
    page_size = 100
    all_results = []
    # 请求参数
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    while True:
        params = {
            "np": 1,
            "fltt": 1,
            "invt": 2,
            "fs": "m:90+t:3+f:!50",
            "fields": "f12,f13,f14,f1,f2,f4,f3,f152,f20,f8,f104,f105,f128,f140,f141,f207,f208,f209,f136,f222",
            "fid": "f3",
            "pn": page,
            "pz": page_size,
            "po": 1,
            "dect": 1,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "wbp2u": "4819115464066356|0|1|0|web",
            "_": int(time.time() * 1000)
        }

        response = requests.get(BASE_URL, headers=headers, cookies=cookies, params=params, verify=False)
        data = response.json()

        diff_list = data["data"]["diff"]
        if not diff_list:
            break

        # 处理数据，比如 f13.f12
        all_results.extend([str(item["f13"]) + "." + str(item["f12"]) for item in diff_list])
        total_count = data["data"]["total"]
        # 如果已经抓完，退出循环
        if page * page_size >= total_count:
            break

        page += 1
        time.sleep(3)  # 防止请求太频繁

    return all_results

def main():
    con = duckdb.connect('../stocks.duckdb')
    # con.execute("DELETE FROM df_a_concept_sector_detail_df")

    # 先查询所有 market 和 code
    success_count = 0
    for item in get_all_sector():
       count = get_all_kline(item, con)
       success_count += count
       print(f"抓取 {count} 条K线数据，总计成功写入 {success_count} 条")
    print("历史K线抓取写入完成")


if __name__ == '__main__':
    main()
