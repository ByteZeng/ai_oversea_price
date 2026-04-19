"""
2025.12
1、海外仓各平台链接价格信息
2、需输出链接价格是否达标

达标标准：
1、链接在线价净利率 >= 平台目标净利率
"""

import pandas as pd
import numpy as np
import time, datetime
import warnings
import os
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
# from all_auto_task.oversea_price_adjust_2023 import  get_rate
# from all_auto_task.oversea_listing_detail_2023 import get_amazon_commission
from all_auto_task.oversea_sku_price import get_sku_price
# from all_auto_task.oversea_temu_price import get_temu_account, get_transport_fee, get_platform_fee
# from all_auto_task.oversea_price_adjust_tt import tt_get_platform_fee
warnings.filterwarnings("ignore")

# 1、获取各平台链接

def get_sales_status():
    """ 销售状态表 """
    sql = """
        SELECT sku, warehouse, sale_status sales_status 
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # df.to_excel('F://Desktop//df_sales_status.xlsx', index=0)
    return df

def get_dwm_info():
    """  dwm_sku表 """
    sql = """
        SELECT sku, warehouse, overage_level
        FROM over_sea.dwm_sku_temp_info 
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df

# 获取供应商sku
def get_sku_type(df):
    # 开发来源
    sql = f"""
            select distinct a.sku sku,  1 is_supplier
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
            INNER JOIN (
                SELECT distinct id as develop_source, develop_source_name
                FROM yibai_prod_base_sync.yibai_prod_develop_source
                WHERE develop_source_name = '供应商货盘'
            ) c on a.develop_source = c.develop_source

        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sup = conn_ck.ck_select_to_df(sql)
    df_sup = df_sup.drop_duplicates(subset='sku')

    sql = """
        SELECT distinct merchant_sku sku, 1 is_supplier
        FROM yibai_dcm_base_sync.dcm_product_map_relation_management
        WHERE is_del = 0
    """
    df_sup_2 = conn_ck.ck_select_to_df(sql)

    df_sup = pd.concat([df_sup, df_sup_2])

    df = pd.merge(df, df_sup, how='left', on=['sku'])
    df['is_supplier'] = df['is_supplier'].fillna(0).astype(int)

    return df

def yb_listing_info():
    """ 获取各平台海外仓链接 """
    col = ['amazon', 'ebay', 'cdiscount', 'walmart', 'allegro', 'ali', 'temu']

    yb_amazon_listing_info()
    yb_temu_listing_info()
    yb_ali_listing_info()
    yb_ebay_listing_info()
    yb_cd_listing_info()
    yb_walmart_listing_info()
    yb_allegro_listing_info()
    yb_shopee_listing_info()
    yb_lazada_listing_info()


def yb_amazon_listing_info():
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, short_name, status listing_status, 'amazon' platform, country, sku, seller_sku, asin,
        online_price, 1-ppve-(new_price+total_cost)/online_price/rate-platform_zero online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, is_white_listing, is_fba_asin is_platform_cdt, is_supplier, is_supplier_price
        FROM yibai_oversea.oversea_amazon_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_amazon_listing_all) and online_price > 0
    """
    df = conn_ck.ck_select_to_df(sql)
    df_dwm = get_dwm_info()
    df = pd.merge(df, df_dwm, how='left', on=['sku', 'warehouse'])
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)

    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['date_id'] = time.strftime('%Y-%m-%d')
    df = df.drop_duplicates(subset=['account_id', 'seller_sku', 'sku'])
    # print(df.info())
    write_to_ck(df, 'oversea_listing_profit', platform='amazon')
    print(f"amazon平台数据上传完成.")

def yb_temu_listing_info():

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT account_id, short_name, online_status listing_status, 'temu' platform, country, sku, 
        product_sku_id product_id, 
        supplier_price online_price, freight_subsidy, 0 online_profit,
        warehouse
        FROM yibai_oversea.yibai_ads_oversea_temu_listing
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_temu_listing)
    """
    df_temu = conn_ck.ck_select_to_df(sql)

    sql = """
        SELECT sku, warehouse, country, best_warehouse_name, new_price, total_cost, available_stock, overage_level,
        rate, ppve, platform_zero, platform_must_percent
        FROM over_sea.dwm_oversea_price_temu
        WHERE date_id = (SELECT max(date_id) FROM dwm_oversea_price_temu)
    """
    df_dwm = conn.read_sql(sql)
    us_rate = df_dwm.loc[df_dwm['country'] == 'US', 'rate'].iloc[0]
    # 补充捆绑sku价格信息
    sql = """
        SELECT sku, warehouse, country, best_warehouse_name, new_price, total_cost, available_stock, overage_level,
        1 rate, ppve, platform_zero, platform_must_percent
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl) 
        and platform = 'TEMU'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dwm_2 = conn_ck.ck_select_to_df(sql)
    df_dwm = pd.concat([df_dwm, df_dwm_2])
    df_dwm = df_dwm.drop_duplicates(subset=['sku','warehouse','country'])
    df_dwm['rate'] = us_rate
    df = pd.merge(df_temu, df_dwm, how='left', on=['sku', 'warehouse', 'country'])

    # 供应商sku信息替换

    # 销售状态
    df_status = get_sales_status()
    df = pd.merge(df, df_status, how='left', on=['sku', 'warehouse'])
    df['sales_status'] = df['sales_status'].fillna('正常')
    # print(df.info())
    # print(df[df['new_price'].isna()]['country'].value_counts())
    df = df[(~df['new_price'].isna()) & (~df['rate'].isna())]
    # print(df[df['rate'].isna()]['country'].value_counts())
    col = ['online_price', 'ppve', 'new_price', 'total_cost', 'rate', 'platform_zero', 'platform_must_percent']
    df[col] = df[col].astype(float)
    df['online_profit'] = 1 - df['ppve'] - (
            df['new_price'] + df['total_cost']) / (df['online_price'] * us_rate + df['freight_subsidy']) - df[
                              'platform_zero']
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)

    col = ['available_stock', 'overage_level']
    df[col] = df[col].fillna(0).astype(int)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())

    # 获取供应商sku
    df = get_sku_type(df)

    write_to_ck(df, 'oversea_listing_profit', platform='temu')
    print(f"temu平台数据上传完成.")

def yb_ali_listing_info():
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, short_name, product_status_type listing_status, 'ali' platform, country, sku, 
        product_id, currency_code, 0 online_profit, warehouse_country,
        case when current_effective_supply_price>0 then current_effective_supply_price else sku_price end as online_price
        FROM yibai_oversea.yibai_ads_oversea_ali_listing
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_ali_listing)
    """
    df = conn_ck.ck_select_to_df(sql)
    dic = {'US': '美国仓', 'UK': '英国仓', 'DE': '德国仓', 'AU': '澳洲仓', 'FR': '法国仓', 'MX': '墨西哥仓',
           'RU': '俄罗斯仓', 'JP': '日本仓', 'ES': '西班牙仓', 'IT': '意大利仓'}
    df['warehouse'] = df['warehouse_country'].replace(dic)

    # sku维度定价信息
    df_sku = get_sku_price(platform='ALI')
    df_sku['可用库存'] = df_sku['可用库存'].replace('5+', 6)
    # 国家为GLO时，使用运费最贵的国家数据
    df_sku_glo = df_sku[df_sku['warehouse'].isin(['德国仓', '英国仓', '法国仓'])]
    df_sku_glo = df_sku_glo.sort_values(by=['总运费'], ascending=False).drop_duplicates(
        subset=['sku', 'warehouse'])
    df_sku_glo['目的国'] = 'GLO'
    df_sku = pd.concat([df_sku, df_sku_glo])
    dic = {'new_price': '成本', 'type': '开发来源', 'gross': '毛重', 'product_package_size': '包装尺寸',
           'best_warehouse_id': '最优子仓ID', 'sales_status': '销售状态',
           'best_warehouse_name': '最优子仓', 'available_stock': '可用库存', 'on_way_stock': '在途库存',
           'overage_level': '超库龄等级', 'age_90_plus': '超90库龄库存', 'age_180_plus': '超180库龄库存',
           'ship_name': '尾程渠道', 'total_cost': '总运费', 'price': 'sku建议兜底价',
           'firstCarrierCost': '头程', 'ship_fee': '尾程', 'country': '目的国', 'ppve': '佣金+库损汇损+vat',
           'platform_zero': '差值', 'platform_must_percent': '平台要求净利率'}
    dic = {v: k for k, v in dic.items()}
    df_sku = df_sku.rename(columns=dic)
    col = ['sku', 'warehouse', 'available_stock', 'sales_status', 'overage_level', 'new_price', 'total_cost',
           'rate', 'country', 'ppve', 'platform_zero', 'platform_must_percent', 'price', 'best_warehouse_name']
    df = pd.merge(df, df_sku[col], how='inner', on=['sku', 'country', 'warehouse'])
    us_rate = df.loc[df['country'] == 'US', 'rate'].iloc[0]

    df['online_price'] = np.where(df['currency_code'] == 'USD', df['online_price'] * us_rate,
                                  df['online_price'])
    df['price'] = np.where(df['currency_code'] == 'USD', df['price'] / us_rate, df['price'])
    col = ['new_price', 'total_cost', 'online_price', 'platform_zero', 'platform_must_percent']
    df[col] = df[col].astype(float)
    df['online_profit'] = 1 - 0.05 - (df['new_price'] + df['total_cost']) / (df['online_price']) - df['platform_zero']
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)

    col = ['available_stock', 'overage_level']
    df[col] = df[col].fillna(0).astype(int)
    df['listing_status'] = df['listing_status'].astype(str)
    df.drop(['currency_code', 'warehouse_country'], axis=1, inplace=True)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    df = get_sku_type(df)
    write_to_ck(df, 'oversea_listing_profit', platform='ali')
    print(f"ali平台数据上传完成.")

def yb_ebay_listing_info():
    """ yb ebay海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, listing_status, 'ebay' platform, country, sku, item_id, 
        online_price, 1-ppve-(new_price+total_cost)/online_price/rate-platform_zero online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, is_supplier, is_supplier_price
        FROM yibai_oversea.oversea_ebay_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_ebay_listing_all)
    """
    df = conn_ck.ck_select_to_df(sql)
    df_dwm = get_dwm_info()
    df = pd.merge(df, df_dwm, how='left', on=['sku', 'warehouse'])
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)

    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())

    write_to_ck(df, 'oversea_listing_profit', platform='ebay')
    print(f"ebay平台数据上传完成.")

def yb_cd_listing_info():
    """ yb cd海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, account_name short_name, offer_state listing_status, 'cdiscount' platform, 
        country, sku, seller_sku, product_id,
        online_price, 1-ppve-(new_price+total_cost)/online_price/rate-platform_zero online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,is_platform_cdt,
        is_white_account, is_supplier, is_supplier_price
        FROM yibai_oversea.oversea_cdiscount_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_cdiscount_listing_all)
    """
    df = conn_ck.ck_select_to_df(sql)
    df_dwm = get_dwm_info()
    df = pd.merge(df, df_dwm, how='left', on=['sku', 'warehouse'])
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)

    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())

    write_to_ck(df, 'oversea_listing_profit', platform='cdiscount')
    print(f"cd平台数据上传完成.")

def yb_walmart_listing_info():
    """ yb walmart海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, short_name, 1 listing_status, 'walmart' platform, 
        country, sku, seller_sku, item_id,
        online_price, 1-ppve-(new_price+total_cost)/online_price/rate-platform_zero online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,is_platform_cdt,is_white_listing,
        is_white_account, is_supplier, is_supplier_price
        FROM yibai_oversea.oversea_walmart_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_walmart_listing_all)
        and is_ca = 0 and online_price > 0
    """
    df = conn_ck.ck_select_to_df(sql)
    df_dwm = get_dwm_info()
    df = pd.merge(df, df_dwm, how='left', on=['sku', 'warehouse'])
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['listing_status'] = df['listing_status'].astype(str)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    # df = get_sku_type(df)
    write_to_ck(df, 'oversea_listing_profit', platform='walmart')
    print(f"walmart平台数据上传完成.")

def yb_allegro_listing_info():
    """ yb allegro海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, account_name short_name, '1' listing_status, 'allegro' platform, 
        country, sku, offer_id item_id,
        online_price, online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,overage_level,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, is_supplier, is_supplier_price
        FROM yibai_oversea.oversea_allegro_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_allegro_listing_all)
    """
    df = conn_ck.ck_select_to_df(sql)
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    write_to_ck(df, 'oversea_listing_profit', platform='allegro')
    print(f"allegro平台数据上传完成.")

def yb_shopee_listing_info():
    """ yb shopee海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, account_name short_name, '1' listing_status, 'shopee' platform, 
        country, sku, item_id,
        online_price, online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,overage_level,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, is_supplier, is_supplier_price
        FROM yibai_oversea.oversea_shopee_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_shopee_listing_all)
    """
    df = conn_ck.ck_select_to_df(sql)
    col = ['available_stock','overage_level']
    df[col] = df[col].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    # df.to_excel('F://Desktop//df_shopee_listing.xlsx', index=0)

    write_to_ck(df, 'oversea_listing_profit', platform='shopee')
    print(f"shopee平台数据上传完成.")


def yb_lazada_listing_info():
    """ yb lazada海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, account_name short_name, '1' listing_status, 'lazada' platform, 
        country, sku, item_id, seller_sku,
        online_price, online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,overage_level,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, is_supplier, is_supplier_price
        FROM yibai_oversea.oversea_lazada_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_lazada_listing_all)
    """
    df = conn_ck.ck_select_to_df(sql)
    col = ['available_stock', 'overage_level']
    df[col] = df[col].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    # df.to_excel('F://Desktop//df_lazada_listing.xlsx', index=0)

    write_to_ck(df, 'oversea_listing_profit', platform='lazada')
    print(f"lazada平台数据上传完成.")


def write_to_ck(df, table_name, platform):
    """
    将中间表数据写入ck
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    sql = f"""
    ALTER TABLE yibai_oversea.{table_name} DELETE where date_id = '{date_id}' and platform = '{platform}'
    """
    conn_ck.ck_execute_sql(sql)
    # 确认当天日期数据已删除
    n = 1
    while n < 5:
        # print(f'删除当前表里的数据，第{n}次测试...')
        sql = f"""
            SELECT count()
            FROM yibai_oversea.{table_name}
            where date_id = '{date_id}' and platform = '{platform}'
        """
        df_cnt = conn_ck.ck_select_to_df(sql)
        if df_cnt.iloc[0,0] == 0:
            # print('结果表删除成功！')
            conn_ck.ck_insert(df, table_name, if_exist='append')
            break
        else:
            n += 1
            time.sleep(60)
    if n == 5:
        print('备份CK失败，当天数据未删除完成，CK未备份')

    # 删除30天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 60, '%Y-%m-%d') and platform = '{platform}'
    """
    conn_ck.ck_execute_sql(sql)


# 通拓平台
def tt_amazon_listing_info():
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT account_id, short_name, '1' listing_status, 'amazon' platform, country, sku, seller_sku, asin,
        online_price, 1-ppve-(new_price+total_cost)/online_price/rate-platform_zero online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,
        price,  rate, ppve, platform_zero, platform_must_percent, is_white_listing,  is_supplier, is_supplier_price
        FROM over_sea.tt_oversea_amazon_listing_all
        WHERE date_id = (SELECT max(date_id) FROM over_sea.tt_oversea_amazon_listing_all)
    """
    df = conn.read_sql(sql)
    df_dwm = get_dwm_info()
    df = pd.merge(df, df_dwm, how='left', on=['sku', 'warehouse'])
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)

    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    col = ['available_stock','is_supplier','is_supplier_price']
    df[col] = df[col].fillna(0).astype(int)
    df['date_id'] = time.strftime('%Y-%m-%d')
    df = df.drop_duplicates(subset=['account_id', 'seller_sku', 'sku'])
    # print(df.info())
    write_to_ck(df, 'tt_oversea_listing_profit', platform='amazon')
    print(f"amazon平台数据上传完成.")

def tt_ebay_listing_info():
    """ tt ebay海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT account_id, listing_status, 'ebay' platform, country, sku, item_id, 
        online_price, 1-ppve-(new_price+total_cost)/online_price/rate-platform_zero online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,
        price,  rate, ppve, platform_zero, platform_must_percent, 
        is_supplier, is_supplier_price
        FROM over_sea.tt_oversea_ebay_listing_all
        WHERE date_id = (SELECT max(date_id) FROM over_sea.tt_oversea_ebay_listing_all)
    """
    df = conn.read_sql(sql)
    df_dwm = get_dwm_info()
    df = pd.merge(df, df_dwm, how='left', on=['sku', 'warehouse'])
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    col = ['account_id','available_stock','is_supplier','is_supplier_price']
    df[col] = df[col].fillna(0).astype(int)
    col = ['listing_status']
    df[col] = df[col].fillna(0).astype(int).astype(str)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())

    write_to_ck(df, 'tt_oversea_listing_profit', platform='ebay')
    print(f"ebay平台数据上传完成.")

def tt_temu_listing_info():

    conn_ck = pd_to_ck(database='tt_oversea', data_sys='通拓-新')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT account_id, short_name, online_status listing_status, 'temu' platform, country, sku, 
        product_sku_id product_id, 
        supplier_price online_price, freight_subsidy, 0 online_profit,
        warehouse
        FROM tt_oversea.tt_ads_oversea_temu_listing
        WHERE date_id = (SELECT max(date_id) FROM tt_oversea.tt_ads_oversea_temu_listing)
    """
    df_temu = conn_ck.ck_select_to_df(sql)

    sql = """
        SELECT sku, warehouse, country, best_warehouse_name, new_price, total_cost, available_stock, overage_level,
        rate, ppve, platform_zero, platform_must_percent
        FROM over_sea.dwm_oversea_price_temu
        WHERE date_id = (SELECT max(date_id) FROM dwm_oversea_price_temu)
    """
    df_dwm = conn.read_sql(sql)
    us_rate = df_dwm.loc[df_dwm['country'] == 'US', 'rate'].iloc[0]
    # 补充供应商sku
    sql = """
        SELECT sku, warehouse, country, best_warehouse_name, new_price, total_cost, available_stock, overage_level,
        1 rate, ppve, platform_zero, platform_must_percent
        FROM yibai_oversea.tt_dwm_oversea_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_dwm_oversea_price)
        and platform = 'TEMU'
    """
    conn_ck_yb = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dwm_2 = conn_ck_yb.ck_select_to_df(sql)
    df_dwm = pd.concat([df_dwm, df_dwm_2])
    df_dwm['rate'] = us_rate

    df = pd.merge(df_temu, df_dwm, how='left', on=['sku', 'warehouse', 'country'])

    # 销售状态
    df_status = get_sales_status()
    df = pd.merge(df, df_status, how='left', on=['sku', 'warehouse'])
    df['sales_status'] = df['sales_status'].fillna('正常')
    # print(df.info())
    # print(df[df['new_price'].isna()]['country'].value_counts())
    df = df[(~df['new_price'].isna()) & (~df['rate'].isna())]
    # print(df[df['rate'].isna()]['country'].value_counts())
    df['platform_zero'] = 0.19
    df['platform_must_percent'] = 0.08
    col = ['online_price', 'ppve', 'new_price', 'total_cost', 'rate', 'platform_zero', 'platform_must_percent']
    df[col] = df[col].astype(float)
    df['online_profit'] = 1 - df['ppve'] - (
            df['new_price'] + df['total_cost']) / (df['online_price'] * us_rate + df['freight_subsidy']) - df[
                              'platform_zero']
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)

    col = ['available_stock', 'overage_level']
    df[col] = df[col].fillna(0).astype(int)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    df = get_sku_type(df)
    write_to_ck(df, 'tt_oversea_listing_profit', platform='temu')
    print(f"temu平台数据上传完成.")

def tt_allegro_listing_info():
    """ yb allegro海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT account_id, account_name short_name, '1' listing_status, 'allegro' platform, 
        country, sku, offer_id item_id,
        online_price, online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,overage_level,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, is_supplier, is_supplier_price
        FROM yibai_oversea.tt_oversea_allegro_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_oversea_allegro_listing_all)
    """
    df = conn_ck.ck_select_to_df(sql)
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    # df = get_sku_type(df)
    write_to_ck(df, 'tt_oversea_listing_profit', platform='allegro')
    print(f"allegro平台数据上传完成.")

def tt_ali_listing_info():
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck = pd_to_ck(database='tt_oversea', data_sys='通拓-新')
    sql = f"""
        SELECT account_id, short_name, product_status_type listing_status, 'ali' platform, country, sku, 
        product_id, currency_code, 0 online_profit, warehouse_country,
        case when current_effective_supply_price>0 then current_effective_supply_price else sku_price end as online_price
        FROM tt_oversea.tt_ads_oversea_ali_listing
        WHERE date_id = (SELECT max(date_id) FROM tt_oversea.tt_ads_oversea_ali_listing)
    """
    df = conn_ck.ck_select_to_df(sql)
    dic = {'US': '美国仓', 'UK': '英国仓', 'DE': '德国仓', 'AU': '澳洲仓', 'FR': '法国仓', 'MX': '墨西哥仓',
           'RU': '俄罗斯仓', 'JP': '日本仓', 'ES': '西班牙仓', 'IT': '意大利仓'}
    df['warehouse'] = df['warehouse_country'].replace(dic)

    # sku维度定价信息
    df_sku = get_sku_price(platform='ALI', org='TT')
    df_sku['可用库存'] = df_sku['可用库存'].replace('5+', 6)
    # 国家为GLO时，使用运费最贵的国家数据
    df_sku_glo = df_sku[df_sku['warehouse'].isin(['德国仓', '英国仓', '法国仓'])]
    df_sku_glo = df_sku_glo.sort_values(by=['总运费'], ascending=False).drop_duplicates(
        subset=['sku', 'warehouse'])
    df_sku_glo['目的国'] = 'GLO'
    df_sku = pd.concat([df_sku, df_sku_glo])
    dic = {'new_price': '成本', 'type': '开发来源', 'gross': '毛重', 'product_package_size': '包装尺寸',
           'best_warehouse_id': '最优子仓ID', 'sales_status': '销售状态',
           'best_warehouse_name': '最优子仓', 'available_stock': '可用库存', 'on_way_stock': '在途库存',
           'overage_level': '超库龄等级', 'age_90_plus': '超90库龄库存', 'age_180_plus': '超180库龄库存',
           'ship_name': '尾程渠道', 'total_cost': '总运费', 'price': 'sku建议兜底价',
           'firstCarrierCost': '头程', 'ship_fee': '尾程', 'country': '目的国', 'ppve': '佣金+库损汇损+vat',
           'platform_zero': '差值', 'platform_must_percent': '平台要求净利率'}
    dic = {v: k for k, v in dic.items()}
    df_sku = df_sku.rename(columns=dic)
    col = ['sku', 'warehouse', 'available_stock', 'sales_status', 'overage_level', 'new_price', 'total_cost',
           'rate', 'country', 'ppve', 'platform_zero', 'platform_must_percent', 'price', 'best_warehouse_name']
    df = pd.merge(df, df_sku[col], how='inner', on=['sku', 'country', 'warehouse'])
    us_rate = df.loc[df['country'] == 'US', 'rate'].iloc[0]

    df['online_price'] = np.where(df['currency_code'] == 'USD', df['online_price'] * us_rate,
                                  df['online_price'])
    df['price'] = np.where(df['currency_code'] == 'USD', df['price'] / us_rate, df['price'])
    col = ['new_price', 'total_cost', 'online_price', 'platform_zero', 'platform_must_percent']
    df[col] = df[col].astype(float)
    df['online_profit'] = 1 - 0.05 - (df['new_price'] + df['total_cost']) / (df['online_price']) - df['platform_zero']
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)

    col = ['available_stock', 'overage_level']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce').fillna(0)
    df[col] = df[col].fillna(0).astype(int)
    df['listing_status'] = df['listing_status'].astype(str)
    df.drop(['currency_code', 'warehouse_country'], axis=1, inplace=True)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    df = get_sku_type(df)
    write_to_ck(df, 'tt_oversea_listing_profit', platform='ali')
    print(f"ali平台数据上传完成.")

def tt_shopee_listing_info():
    """ yb shopee海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT account_id, account_name short_name, '1' listing_status, 'shopee' platform, 
        country, sku, item_id,
        online_price, online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,overage_level,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, 0 is_supplier, 0 is_supplier_price
        FROM over_sea.tt_oversea_shopee_listing_all
        WHERE date_id = (SELECT max(date_id) FROM over_sea.tt_oversea_shopee_listing_all)
    """
    df = conn.read_sql(sql)
    col = ['available_stock','overage_level']
    df[col] = df[col].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    # df.to_excel('F://Desktop//df_shopee_listing.xlsx', index=0)

    write_to_ck(df, 'tt_oversea_listing_profit', platform='shopee')
    print(f"shopee平台数据上传完成.")


def tt_lazada_listing_info():
    """ yb lazada海外仓链接利润率是否达标 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    sql = f"""
        SELECT account_id, account_name short_name, '1' listing_status, 'lazada' platform, 
        country, sku, item_id, seller_sku, 
        online_price, online_profit,
        warehouse, best_warehouse_name, new_price, total_cost, available_stock, sales_status,overage_level,
        price,  rate, ppve, platform_zero, platform_must_percent, is_normal_cdt,
        is_white_account, 0 is_supplier, 0 is_supplier_price
        FROM over_sea.tt_oversea_lazada_listing_all
        WHERE date_id = (SELECT max(date_id) FROM over_sea.tt_oversea_lazada_listing_all)
    """
    df = conn.read_sql(sql)
    col = ['available_stock', 'overage_level']
    df[col] = df[col].fillna(0).astype(int)
    c1 = (df['online_profit'] >= df['platform_must_percent'])
    df['is_target_profit'] = np.where(c1, 1, 0)
    col = ['online_price', 'online_profit', 'new_price', 'total_cost', 'price', 'rate', 'ppve', 'platform_zero',
           'platform_must_percent']
    df[col] = df[col].astype(float)
    df['date_id'] = time.strftime('%Y-%m-%d')
    # print(df.info())
    # df.to_excel('F://Desktop//df_lazada_listing.xlsx', index=0)

    write_to_ck(df, 'tt_oversea_listing_profit', platform='lazada')
    print(f"lazada平台数据上传完成.")


def tt_listing_info():
    """ 获取各平台海外仓链接 """
    col = ['amazon', 'ebay', 'cdiscount', 'walmart', 'allegro', 'ali', 'temu']

    tt_amazon_listing_info()
    tt_temu_listing_info()
    tt_ali_listing_info()
    tt_ebay_listing_info()
    tt_allegro_listing_info()
    tt_shopee_listing_info()
    tt_lazada_listing_info()

def get_sup_temp():

    sql = """
        SELECT YM_sku, YB_sku, warehouse, available_stock, warehouse_name
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup = conn_ck.ck_select_to_df(sql)

    df_sup.to_excel('F://Desktop//df_sup.xlsx', index=0)


if __name__ == '__main__':
    # yb_listing_info()

    yb_amazon_listing_info()
    yb_ebay_listing_info()
    # yb_cd_listing_info()
    # yb_walmart_listing_info()
    # yb_allegro_listing_info()
    yb_temu_listing_info()
    yb_ali_listing_info()
    # yb_shopee_listing_info()
    # yb_lazada_listing_info()

    # tt_amazon_listing_info()
    # tt_ebay_listing_info()
    # tt_temu_listing_info()
    # tt_allegro_listing_info()
    # tt_ali_listing_info()
    # tt_shopee_listing_info()
    # tt_lazada_listing_info()

    tt_listing_info()
    # get_sales_status()
    # get_sup_temp()
