# _*_ coding : utf-8 _*_
# @Time : 2022/5/20 - 0:36
# @Author : Holden
# @File : get
# @Project : python
import random
from datetime import datetime, UTC
import requests
import json
import datetime
import time
import duckdb
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
def get_a_stock_num():
    a_url = 'http://4.push2.eastmoney.com/api/qt/clist/get?pn=2&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=4819115464066356|0|0|0|web&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f1&_=' + str(
        int(time.mktime(time.localtime(time.time()))) * 1000)
    response = requests.get(url=a_url)
    content = response.text
    target_json = json.loads(content)
    return target_json.get('data').get('total')


def init_duckdb():
    con = duckdb.connect('../stocks.duckdb')
    con.execute('''
    CREATE TABLE IF NOT EXISTS df_a_stock_detail_df(
        market VARCHAR,
        code VARCHAR,
        name VARCHAR,
        current_price DOUBLE,
        up_down_rate DOUBLE,
        up_down_rate5 DOUBLE,
        up_down_rate10 DOUBLE,
        up_down_amount DOUBLE,
        turnover_rate DOUBLE,
        PE_ratio_d DOUBLE,
        amplitude DOUBLE,
        volume_ratio DOUBLE,
        highest DOUBLE,
        lowest DOUBLE,
        opening_price DOUBLE,
        t_1_price DOUBLE,
        total_market_v DOUBLE,
        circulation_market_v DOUBLE,
        price_to_b_ratio DOUBLE,
        increase_this_year DOUBLE,
        time_to_market BIGINT,
        outer_disk DOUBLE,
        inner_disk DOUBLE,
        roe DOUBLE,
        total_share_capital DOUBLE,
        tradable_shares DOUBLE,
        total_revenue DOUBLE,
        total_revenue_r DOUBLE,
        gross_profit_margin DOUBLE,
        total_assets DOUBLE,
        debt_ratio DOUBLE,
        industry VARCHAR,
        regional_plate VARCHAR,
        profit DOUBLE,
        PE_ratio_s DOUBLE,
        ttm DOUBLE,
        net_assets DOUBLE,
        deal_amount DOUBLE,
        deal_vol DOUBLE,
        dealTradeStae VARCHAR,
        commission DOUBLE,
        net_margin DOUBLE,
        total_profit DOUBLE,
        net_assets_per_share DOUBLE,
        net_profit DOUBLE,
        net_profit_r DOUBLE,
        unearnings_per_share DOUBLE,
        main_inflow DOUBLE,
        main_inflow_ratio DOUBLE,
        Slarge_inflow DOUBLE,
        Slarge_inflow_ratio DOUBLE,
        large_inflow DOUBLE,
        large_inflow_ratio DOUBLE,
        mid_inflow DOUBLE,
        mid_inflow_ratio DOUBLE,
        small_inflow DOUBLE,
        small_inflow_ratio DOUBLE,
        board VARCHAR,
        ds DATE
    )
    ''')
    return con


def fix_val(v):
    if v == '-' or v is None:
        return 0.0
    return v


def catch_stock_page(pn, pz):
    url = 'http://2.push2.eastmoney.com/api/qt/clist/get'
    cookies = {
        'qgqp_b_id': 'b324e92afc7fde4a0afbc2f8451e7597',
        'st_nvi': 'mTVjrdgwBPD_kB-PyxoD7fb02',
        'st_si': '50373445237036',
        'st_asi': 'delete',
        'nid18': '0c5414be2064203f83bda4c87fdd9ffc',
        'nid18_create_time': '1765034571831',
        'gviem': 'c2WYu_WX6vYQjkSCfwZIfc548',
        'gviem_create_time': '1765034571831',
        'fullscreengg': '1',
        'fullscreengg2': '1',
        'st_pvi': '21898710297471',
        'st_sp': '2025-12-06%2023%3A22%3A51',
        'st_inirUrl': 'https%3A%2F%2Fwww.eastmoney.com%2F',
        'st_sn': '4',
        'st_psi': '20251206232303224-113200301321-8455612159',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://quote.eastmoney.com/center/gridlist.html',
        'Sec-Fetch-Dest': 'script',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        # 'Cookie': 'qgqp_b_id=b324e92afc7fde4a0afbc2f8451e7597; st_nvi=mTVjrdgwBPD_kB-PyxoD7fb02; st_si=50373445237036; st_asi=delete; nid18=0c5414be2064203f83bda4c87fdd9ffc; nid18_create_time=1765034571831; gviem=c2WYu_WX6vYQjkSCfwZIfc548; gviem_create_time=1765034571831; fullscreengg=1; fullscreengg2=1; st_pvi=21898710297471; st_sp=2025-12-06%2023%3A22%3A51; st_inirUrl=https%3A%2F%2Fwww.eastmoney.com%2F; st_sn=4; st_psi=20251206232303224-113200301321-8455612159',
    }
    data = {
        'pn': pn,
        'pz': pz,
        'np': 1,
        'po': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2,
        'invt': 2,
        'wbp2u': '4819115464066356|0|0|0|web',
        'fid': 'f3',
        'fs': 'm:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2',#'m:0+t:6+f:!2,m:0+t:80+f:!2', #'m:1+t:2+f:!2,m:1+t:23+f:!2',
        'fields': 'f13,f12,f14,f2,f3,f109,f160,f4,f8,f9,f7,f10,f15,f16,f17,f18,f19,f20,f21,f23,f25,f26,f34,f35,f37,f38,f39,f40,f41,f49,f50,f57,f100,f102,f112,f114,f115,f135,f6,f5,f292,f33,f129,f44,f113,f45,f46,f48,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87',
        '_': int(time.time() * 1000)
    }
    response = requests.get(url, params=data, headers=headers, cookies=cookies, verify=False)
    data = response.json()
    diff = data.get('data', {}).get('diff', [])
    records = []
    ds_date = str(datetime.datetime.fromtimestamp(int(time.time()), UTC).strftime("%Y-%m-%d"))
    for j in diff:
        record = (
            j.get('f13'), j.get('f12'), j.get('f14'), fix_val(j.get('f2')), fix_val(j.get('f3')), fix_val(j.get('f109')),
            fix_val(j.get('f160')), fix_val(j.get('f4')), fix_val(j.get('f8')), fix_val(j.get('f9')), fix_val(j.get('f7')),
            fix_val(j.get('f10')), fix_val(j.get('f15')), fix_val(j.get('f16')), fix_val(j.get('f17')),
            fix_val(j.get('f18')), fix_val(j.get('f20')), fix_val(j.get('f21')), fix_val(j.get('f23')), fix_val(j.get('f25')),
            fix_val(j.get('f26')), fix_val(j.get('f34')), fix_val(j.get('f35')), fix_val(j.get('f37')), fix_val(j.get('f38')),
            fix_val(j.get('f39')), fix_val(j.get('f40')), fix_val(j.get('f41')), fix_val(j.get('f49')), fix_val(j.get('f50')),
            fix_val(j.get('f57')), fix_val(j.get('f100')), fix_val(j.get('f102')), fix_val(j.get('f112')),
            fix_val(j.get('f114')), fix_val(j.get('f115')), fix_val(j.get('f135')), fix_val(j.get('f6')), fix_val(j.get('f5')),
            fix_val(j.get('f292')), fix_val(j.get('f33')), fix_val(j.get('f129')), fix_val(j.get('f44')), fix_val(j.get('f113')),
            fix_val(j.get('f45')), fix_val(j.get('f46')), fix_val(j.get('f48')), fix_val(j.get('f62')), fix_val(j.get('f184')),
            fix_val(j.get('f66')), fix_val(j.get('f69')), fix_val(j.get('f72')), fix_val(j.get('f75')), fix_val(j.get('f78')),
            fix_val(j.get('f81')), fix_val(j.get('f84')), fix_val(j.get('f87')), j.get('f19'), ds_date
        )
        records.append(record)
    return records


def main():
    con = init_duckdb()
    total = 5451  # 可用 get_a_stock_num() 获取实时总量
    page_size = 100
    total_pages = (total + page_size - 1) // page_size

    print(f"📊 总股票数: {total}, 共 {total_pages} 页, 每页 {page_size} 条")

    all_records = []
    max_workers = 15

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(catch_stock_page, page, page_size): page for page in range(1, total_pages + 1)}
        for future in as_completed(futures):
            page = futures[future]
            try:
                records = future.result()
                print(f"✅ 第 {page} 页完成, 抓取 {len(records)} 条")
                all_records.extend(records)
            except Exception as e:
                print(f"⚠️ 第 {page} 页出错: {e}")

    print(f"📦 共抓取 {len(all_records)} 条数据，准备写入数据库...")

    if all_records:
        columns = ['market', 'code', 'name', 'current_price', 'up_down_rate', 'up_down_rate5', 'up_down_rate10',
                   'up_down_amount', 'turnover_rate', 'PE_ratio_d', 'amplitude', 'volume_ratio', 'highest', 'lowest',
                   'opening_price', 't_1_price', 'total_market_v', 'circulation_market_v', 'price_to_b_ratio',
                   'increase_this_year', 'time_to_market', 'outer_disk', 'inner_disk', 'roe', 'total_share_capital',
                   'tradable_shares', 'total_revenue', 'total_revenue_r', 'gross_profit_margin', 'total_assets',
                   'debt_ratio', 'industry', 'regional_plate', 'profit', 'PE_ratio_s', 'ttm', 'net_assets', 'deal_amount',
                   'deal_vol', 'dealTradeStae', 'commission', 'net_margin', 'total_profit', 'net_assets_per_share',
                   'net_profit', 'net_profit_r', 'unearnings_per_share', 'main_inflow', 'main_inflow_ratio',
                   'Slarge_inflow', 'Slarge_inflow_ratio', 'large_inflow', 'large_inflow_ratio', 'mid_inflow',
                   'mid_inflow_ratio', 'small_inflow', 'small_inflow_ratio', 'board', 'ds']
        df = pd.DataFrame(all_records, columns=columns)
        con.register('df_view', df)
        con.execute("INSERT INTO df_a_stock_detail_df SELECT * FROM df_view")
        con.commit()
        print("✅ 数据写入完成！")
    else:
        print("⚠️ 没有抓取到任何数据！")


if __name__ == '__main__':
    main()
