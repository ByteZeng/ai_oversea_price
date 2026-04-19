"""
海外仓运费接口
"""
##
import pandas as pd
import numpy as np
import time, datetime
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea, get_trip_fee, get_trip_fee2
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, sku_and_num_split
import re
import os
import pymysql
import warnings
warnings.filterwarnings("ignore")

##
def get_interf_fee(df, platform='TEMU'):
    """
    输入：sku, country, (warehouse_id)
    输出：sku, country, 'warehouseName', 'shipName', 'totalCost','shippingCost', 'firstCarrierCost'
    """
    df = df[['sku', 'country','warehouse_id']].drop_duplicates()
    #
    df['数量'] = 1
    df = df.reset_index(drop=True).reset_index()
    # 控制country对应的子仓
    dic = {'US': '49,50', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
           'AU': '353,769','CZ': '325','PL': '325','HU':'325','PT':'325','NL':'325','MX':'956','BR':'1467'}
    # dic = {'US':'47,49,50,58,1700,1499,1695,1498,1694,1696,1733,1698,1699,1739,1098'}
    # df['warehouse_id'] = df['country'].replace(dic)
    # print(df.head(5))
    # df_bundle = df_bundle.sample(1000)
    df_bundle = sku_and_num_split(df)
    print(df_bundle.info())
    df_fee = load_interface(df_bundle, platform)
    df_fee = df_fee[['sku', 'shipCountry', 'warehouseName', 'shipName', 'totalCost','shippingCost', 'firstCarrierCost']]

    return df_fee

def get_interf_fee_2(df, platform='TEMU'):
    """
    指定warehouse_id
    输入：sku, country, warehouse_id
    输出：sku, country, 'warehouseName', 'shipName', 'totalCost','shippingCost', 'firstCarrierCost'
    """
    df = df[['sku', 'country', 'warehouse_id']].drop_duplicates()
    #
    df['数量'] = 1
    df = df.reset_index(drop=True).reset_index()
    # # 控制country对应的子仓
    # dic = {'US': '47,49,50,58', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
    #        'AU': '353,769','CZ': '325','PL': '325','HU':'325','PT':'325','NL':'325','MX':'956,957','BR':'1467'}
    # dic = {'US':'47,49,50,58,1700,1499,1695,1498,1694,1696,1733,1698,1699,1739,1098'}
    # df['warehouse_id'] = df['country'].replace(dic)

    print(df.head(5))
    # df_bundle = df_bundle.sample(1000)
    df_bundle = sku_and_num_split(df)
    print(df_bundle.info())
    df_fee = load_interface(df_bundle, platform)
    df_fee = df_fee[['sku', 'shipCountry', 'warehouseName', 'shipName', 'totalCost','shippingCost', 'firstCarrierCost']]

    return df_fee

def load_interface(df, platform='TEMU'):
    """
    调用运费接口获取sku海外仓运费
    """
    df_result = pd.DataFrame()
    # key2 = '49;769;325;88;50;353;47;680;58;1139'
    for (key1, key2), group in df.groupby(['country', 'warehouse_id']):
        # print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea(platform, key1, key2, '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30','')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost', 'shippingCost', 'firstCarrierCost','dutyCost']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost','dutyCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True).drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])

    return df_result

def main():
    """
    输入sku+country
    """
    df = pd.read_excel('F://Desktop//df_mx.xlsx', dtype={'sku':str})
    # df = pd.read_excel('F://Desktop//sku_us.xlsx', dtype={'sku': str})
    # df['country'] = 'MX'
    # df = df[['sku','country']]
    df = df[['sku', 'warehouse_id', 'country']]
    # df = df.sample(100)
    df['warehouse_id'] = df['warehouse_id'].astype(str)
    print(df.info())
    df_result = get_interf_fee(df, platform='AMAZON')
    print(df_result.info())
    df_result.to_excel('F://Desktop//df_mx_fee.xlsx', index=0)

def get_shopee_fee():
    """ 获取东南亚仓sku """
    sql = """
        SELECT sku, warehouse, best_warehouse_name warehouse_name, best_warehouse_id warehouse_id, 
        available_stock, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-05-01')
        and warehouse in ('泰国仓', '越南仓', '菲律宾仓', '印度尼西亚仓', '马来西亚仓')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    # df.to_excel('F://Desktop//df_sku.xlsx', index=0)

    print(f"东南亚仓sku数量共{len(df)}个.")
    df = df[['sku','warehouse','warehouse_id']]
    dic = {'泰国仓':'TH', '越南仓':'VN', '菲律宾仓':'PH', '印度尼西亚仓':'ID', '马来西亚仓':'MY'}
    df['country'] = df['warehouse'].replace(dic)

    df = df[['sku', 'country','warehouse_id']].drop_duplicates()
    df['数量'] = 1
    df = df.reset_index(drop=True).reset_index()
    print(df.head(5))
    # df_bundle = df_bundle.sample(1000)
    df_bundle = sku_and_num_split(df)
    print(df_bundle.info())
    df_fee_1 = load_interface(df_bundle, 'SHOPEE')
    df_fee_1['platform'] = 'SHOPEE'

    df_fee_2 = load_interface(df_bundle, 'LAZADA')
    df_fee_2['platform'] = 'LAZADA'

    df_fee = pd.concat([df_fee_1, df_fee_2])
    df_fee['date_id'] = time.strftime('%Y-%m-%d')
    print(df_fee.info())

    df_fee.drop(['数量'], axis=1, inplace=True)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_fee, 'oversea_shopee_fee_daily', if_exists='replace')
    # df.to_excel('F://Desktop//df_sku.xlsx', index=0)
    # df_fee.to_excel('F://Desktop//df_fee.xlsx', index=0)


#  shopee and lazada 平台运费数据
def get_shopee_fee_temp():
    """ 调用接口获取shopee和lazada平台的海外仓运费数据 """
    pass

def temp_main():
    df = pd.DataFrame([['1014230044211', 'AU', 1]], columns=['sku', 'site', '数量'])
    w_list = "2,3,6,5,4,16,17"
    # w_list = '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30'
    yunfei_jisuan = get_trip_fee(platform='AMAZON', shipCountry='AU', warehouse='478', shipType=w_list)
    temp = yunfei_jisuan.batch_df_order(df)
    print(temp.info())

def temp():
    """
    输入sku+country
    """
    df = pd.read_excel('F://Desktop//df_amazon_fee.xlsx', dtype={'sku':str})
    # df = df.sample(100)
    # df['warehouse_id'] = df['warehouse_id'].astype(str)
    print(df.info())
    df_result = get_interf_fee(df, platform='AMAZON')
    print(df_result.info())
    df_result.to_excel('F://Desktop//df_amazon_fee_result_2.xlsx', index=0)

def get_sku():

    df = pd.read_excel('F://Desktop//df_sku_price.xlsx', dtype={'sku':str})
    sql = """
    SELECT 
        sku, title_cn `产品名称`, develop_source, b.develop_source_name,
        CASE 
            when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
            when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
            else toFloat64(product_cost) 
        END as `new_price`
    FROM yibai_prod_base_sync.yibai_prod_sku a
    LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source b
    ON a.develop_source = b.id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    df_sku = df_sku.drop_duplicates(subset='sku')

    df = pd.merge(df, df_sku[['sku', 'new_price']], how='left', on='sku')
    df.to_excel('F://Desktop//df_sku_cost.xlsx', index=0)

    return df

if __name__ == '__main__':
    main()
    # temp_main()
    # get_shopee_fee()
    # temp()