"""
tt
ebay转寄定价时，会获取FBA链接库存及预计可售天数，并计算转寄定价信息。
现需要创建自动任务，每天将业务所需数据上传至中台接口
"""
##
import datetime,time
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
import pandas as pd
import numpy as np
from pulic_func.base_api.adjust_price_function_amazon import fanou_fanmei
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json
import warnings
warnings.filterwarnings('ignore')

##
def get_fba_info():
    """
    Ebay “FBA正负利润加快动销最新库存”的数据 还需要出一份“美英x德净利建议定价”
    ebay举例：
    0.0747 销毁价综合系数
    10% 是平台佣金 （前三个月免费）后面10%收取
    0.16 是毛净利差值  新平台 （盲拍）
    0.05 定价综合系数
    """

    today_y = datetime.date.today() - datetime.timedelta(days=1)
    today_y = f'{today_y.year:04d}{today_y.month:02d}{today_y.day:02d}'
    sql = f"""
    SELECT a.*, b.`最大库龄天数` as `最大库龄天数`
    FROM (
        SELECT 
            a.account_id account_id, a.seller_sku seller_sku, sku, `站点`, `可售库存`, `日均销量`, `可售天数`,
            case 
                when adjustment_priority is Null then '正常'
                when adjustment_priority = '' then '正常'
                else adjustment_priority 
            end as adjustment_priority,
            case 
                when b.area = '泛欧' then concat(lower(d.shop_name), '欧洲')
                when c.site is not Null then concat(lower(d.shop_name), '北美')
                else concat(lower(d.shop_name), `站点`)
            end as `标识`
        FROM domestic_warehouse_clear.fba_available_days a
        LEFT JOIN tt_fba.fba_fanmei c
        ON a.account_id = c.account_id and a.seller_sku = c.seller_sku
        LEFT JOIN (
            SELECT account_id, seller_sku, adjustment_priority
            FROM domestic_warehouse_clear.yibai_fba_clear_new
            WHERE end_time is Null
        ) e ON e.account_id = a.account_id and e.seller_sku = a.seller_sku
        LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon b
        ON a.`站点` = b.site1
        LEFT JOIN (
            SELECT erp_id, shop_name
            FROM tt_sale_center_system_sync.tt_system_account
            WHERE platform_code = 'AMAZON'
        ) d
        ON a.account_id = d.erp_id
    ) a
    LEFT JOIN (
        SELECT `标识`, seller_sku, max(`库龄天数`) as `最大库龄天数`
        FROM tt_price_fba.tt_amazon_fba_inventory_age_self_calculated3_{today_y}
        GROUP BY `标识`, seller_sku
    ) b
    ON a.`标识` = b.`标识` and a.seller_sku = b.seller_sku
    WHERE a.`站点` IN ('美国', '英国', '德国', '加拿大')

    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据t')
    df = conn_ck.ck_select_to_df(sql)
    print(f"FBA链接获取完成，共{len(df)}条")
    # df['最大库龄天数'] = 0
    col = ['seller_sku', 'account_id', 'sku', '可售库存', '日均销量', '可售天数', 'adjustment_priority', '最大库龄天数']
    df = df[col]
    df.columns = ['seller_sku', 'account_id', 'sku', 'stock', 'avg_day_sales', 'stock_sellable_days', 'sale_status', 'max_stock_age']
    df = df.sort_values(by=['seller_sku', 'account_id', 'stock'], ascending=[True, True, True]).drop_duplicates(
        subset=['seller_sku', 'account_id'])
    df['account_id'] = df['account_id'].astype(str)
    df[['stock', 'max_stock_age']] = df[['stock', 'max_stock_age']].fillna(0).astype(int)
    df[['stock_sellable_days']] = df[['stock_sellable_days']].fillna(0).astype(float)
    df['sale_status'] = df['sale_status'].replace(
        {'清仓': '4', '正利润加快动销': '9', '负利润加快动销': '10', '正常': '11', '涨价缩销': '12'})
    df['sale_status'] = df['sale_status'].astype(str)
    df = df.reset_index(drop=True).reset_index()
    df['index'] = (df['index'] / 1000).astype(int)
    print(df.info())
    print('done!')

    # 存表
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'tt_fba_stock_to_center', if_exists='replace')
    return df
# df = get_fba_info()
##
##

def main():
    # 查表是否存在
    date_today = time.strftime('%Y%m%d')
    # table_now = f"tt_fba_clear_seller_sku_{date_today}"
    # # table_now = f"fba_clear_seller_sku_20240624"
    # sql_table_monitor = f"""
    #     select distinct table from system.parts
    #     where database = 'support_document'
    #     and table like 'tt_fba_clear_seller_sku%'
    #     and table <= \'{table_now}\'
    #     order by table desc
    #     limit 7
    # """
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据t')
    # df_table_monitor = conn_mx.ck_select_to_df(sql_table_monitor)
    # if table_now in df_table_monitor['table'].unique():
    print('今日调价数据表已生成.')
    # 获取fba链接信息
    df = get_fba_info()
    print(df.info())
    # df = df.sample(2)
    # df.to_excel('F://Desktop//tt_fba_to_center_sample.xlsx', index=0)
    if len(df) > 0:
        # 上传程序
        threadPool = ThreadPoolExecutor(max_workers=10)
        thread_list = []
        for key, group in tqdm(df.groupby(['index'])):
            group.drop('index', axis=1, inplace=True)
            group = group.reset_index(drop=True)
            # future = threadPool.submit(center_interface, group)
            # thread_list.append(future)
            center_interface(group)
        print('上传完成！')
    else:
        print('暂无数据.')

##
def center_interface(group):
    """
    接口调用函数
    """
    url = 'http://salescenter.yibainetwork.com:91/apis/ebay/ebay_syncstock_fba_amazon_api/push_fba_sku_max_sotck_age?org_code=org_00235'
    # url = 'http://192.168.86.142:100/apis/ebay/ebay_syncstock_fba_amazon_api/push_fba_sku_max_sotck_age?org_code=org_00001'
    # url = 'http://192.168.86.141:100/apis/ebay/ebay_syncstock_fba_amazon_api/push_fba_sku_max_sotck_age?org_code=org_00001'
    data_temp = []
    for i in range(len(group['sku'])):
        data_dict = {
            "seller_sku": str(group.iloc[i, 0]),
            "account_id": str(group.iloc[i, 1]),
            "sku": str(group.iloc[i, 2]),
            "stock": int(group.iloc[i, 3]),
            "avg_day_sales": float(group.iloc[i, 4]),
            "stock_sellable_days": int(group.iloc[i, 5]),
            "sale_status": str(group.iloc[i, 6]),
            "max_stock_age": int(group.iloc[i, 7])
        }
        # print(data_dict)
        data_temp.append(data_dict)
    data_post = json.dumps({"org_code":"org_00235", "data": data_temp})
    # print(data_post)
    # res = requests.post(url, data=data_post, headers={'Content-Type': 'application/json'}).json()
    # print(json.dumps(res, ensure_ascii=False))
    n = 0
    while n < 5:
        try:
            res = requests.post(url, data=data_post, headers={'Content-Type': 'application/json'}).json()
            # print(res)
            if res['status'] == 200:
                break
            else:
                print(f'上传失败，报错信息：{res}')
                # print(data_post0)
                n += 1
        except:
            print(f'接口失败，重新上传')
            time.sleep(10)
            n += 1

##

if __name__ == "__main__":
    main()
    # get_fba_info()
    # df = get_fba_info()
