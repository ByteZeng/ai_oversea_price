"""
统计FMA定价/FBM定价的定价比，与销量、库存金额等因素的关系、
1、先获取FBA链接，及对应的FBM链接。同asin同站点只去一条记录
2、获取FBA目标价和FBM目标价
"""
##
import pandas as pd
import numpy as np
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd, connect_to_sql
import datetime, time
import pulic_func.base_api.mysql_connect as mc
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.all_freight_interface import *
from pulic_func.base_api.adjust_price_function_amazon import shuilv,fanou_fanmei
##
# sql = """
#     SELECT
#         account_id, parent_seller_sku, seller_sku, account_name, product_id, `父体SKU`,sku, `子SKU`, country_code
#     FROM (
#         SELECT *
#         FROM yibai_oversea.yibai_wish_over_sea_listing
#         WHERE `是否修改运费` = '修改WE及直发运费' and `子SKU` Not In (
#             SELECT distinct
#                 case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then subString(a.sku, 4)
#                      when subString(a.sku, 1, 2) in ['DE', 'GB'] then subString(a.sku, 3)
#                      when subString(a.sku, -2) in ['DE', 'GB'] then subString(a.sku, 1, -2)
#                      else a.sku
#                 end AS son_sku
#             FROM yibai_oversea.dwm_sku_temp_info_temp a
#             WHERE available_stock >0 or on_way_stock > 0
#             )
#         )
#     settings max_memory_usage = 200000000000
#     """
# conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
# df_wish = conn_ck.ck_select_to_df(sql)
#
# ##
# sql = """
#         SELECT
#             case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then
#                  subString(a.sku, 4)
#             when subString(a.sku, 1, 2) in ['DE', 'GB'] then
#                  subString(a.sku, 3)
#             when subString(a.sku, -2) in ['DE', 'GB'] then
#                  subString(a.sku, 1, -2)
#             else
#                  a.sku
#             end AS son_sku, available_stock, on_way_stock,is_new
#         FROM yibai_oversea.dwm_sku_temp_info_temp a
#         WHERE available_stock <=0 and on_way_stock <= 0
#     """
# conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
# df_sku = conn_ck.ck_select_to_df(sql)
# ##
# df_sku = df_sku.sort_values(by='is_new', ascending=False).drop_duplicates(subset='son_sku',keep='first')
# ##
# df_wish_0= pd.merge(df_wish, df_sku, how='left', left_on=['子SKU'], right_on='son_sku')
# ##
# df_wish_0 = df_wish_0[~df_wish_0['is_new'].isna()]
# ##
# df_wish_2 = df_wish_0.groupby(['account_id', 'parent_seller_sku', 'seller_sku', 'account_name', 'product_id',
#                              '子SKU']).agg({'country_code':set,'is_new':'sum'}).reset_index()
# ##
# df_wish_0.to_excel('df_wish_0.xlsx', index=0)
# ##
# df_wish_2.to_excel('df_wish_2.xlsx', index=0)
# ##
# del df_wish
# ##
# sql = f"""
#     SELECT distinct account_id
#     FROM (
#         SELECT
#             distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site,
#             case
#                 when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
#                 else c.site1
#             end as site2
#         FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
#         LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
#         ON toInt32(a.account_id) = toInt32(b.id)
#         LEFT JOIN domestic_warehouse_clear.site_table c
#         ON b.site = c.site
#         WHERE a.fulfillment_channel = 'AMA'
#     )
#     WHERE site2 != '泛欧'
# """
# df_account_id = conn_ck.ck_select_to_df(sql)
##
# date_today = time.strftime('%Y%m%d')
# conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
# # 获取FBA全量链接信息
# # 由于数量较大，采用分批取数的方式
# sql = f"""
#     SELECT distinct account_id
#     FROM support_document.fba_clear_seller_sku_{date_today} f
# """
# df_account = conn_ck.ck_select_to_df(sql)
# print(len(df_account))
# step = 10
# account_list = [df_account[i:i + step] for i in range(0, int(len(df_account)/100), step)]
# # username, password, host, port = mc.get_ck_conf_data('数据部服务器')
#
# host = '121.37.248.212'
# port = '9003'
# username = 'songzhanzhao'
# password = 'songzHanzhao0103220490'
# sql = """
#     SELECT distinct sku as new_sku, site, total_cost as fbm_fees
#     FROM yibai_temp_hxx.freight_interface_amazon
#     LIMIT 1000
# """
# ck_client = CkClient(user='songzhanzhao', password='songzHanzhao0103220490', host='121.37.248.212', port='9003',
#                      db_name='yibai_temp_hxx')
# df_sku_stock = ck_client.ck_select_to_df(sql)
# ##
# df_listing = pd.DataFrame()
# for a in account_list:
#     account_tuple = tuple(a['account_id'].unique())
#     sql = f"""
#         SELECT
#             f.account_id, f.asin, f.seller_sku as fba_seller, a.seller_sku as fbm_seller, f.sku, f.site, f.site1, f.site2, f.fba_price_rmb,f.fba_fees,
#             f.FBM_difference,f.FBM_profit,f.tax_rate, cost, length, width, high, weight, first_trip_source, e.first_trip_fee_rmb, p.available,
#             p.inv_age_0_to_90_days, p.inv_age_91_to_180_days, p.inv_age_181_to_270_days,p.inv_age_271_to_365_days,p.inv_age_365_plus_days
#         FROM (
#             SELECT
#                 a.*,
#                 (toFloat32(product_cost)+toFloat32(fba_fees)*rate+toFloat32(first_trip_fee_rmb)+2)/(1-commission_rate-0.03-FBA_difference-FBA_profit-tax_rate) as fba_price_rmb,
#                 b.site as site2 ,b.FBM_difference, b.FBM_profit,
#                 case
#                     when a.site in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
#                     else a.site
#                 end as site1
#             FROM support_document.fba_clear_seller_sku_{date_today} a
#             LEFT JOIN domestic_warehouse_clear.site_table b
#             ON a.site = b.site1
#             WHERE account_id in {account_tuple}
#         ) f
#         LEFT JOIN (
#             SELECT account_id, seller_sku, asin, cost, length, width, high, weight, first_trip_source, first_trip_fee_rmb
#             FROM domestic_warehouse_clear.fba_fees
#         ) e
#         ON f.account_id = e.account_id and f.seller_sku = e.seller_sku and f.asin = e.asin
#         LEFT JOIN (
#             SELECT distinct account_id, asin1 as asin, seller_sku, price
#             FROM yibai_product_kd_sync.yibai_amazon_listing_alls
#             WHERE fulfillment_channel != 'AMA' and account_id in {account_tuple}
#         ) a
#         ON f.account_id = a.account_id and f.asin = a.asin
#         LEFT JOIN (
#             SELECT account_id, sku, available, inv_age_0_to_90_days, inv_age_91_to_180_days, inv_age_181_to_270_days,inv_age_271_to_365_days,inv_age_365_plus_days
#             FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_aged_planning_data
#             WHERE toInt32(account_id) in {account_tuple}
#         ) p
#         ON toInt32(f.account_id) = toInt32(p.account_id) and f.seller_sku = p.sku
#     """
#     # LEFT
#     # JOIN(
#     #     SELECT
#     # distinct
#     # sku as new_sku, site, total_cost as fbm_fees
#     # FROM
#     # remote('{host}:{port}', 'yibai_temp_hxx', 'freight_interface_amazon', '{username}', '{password}')
#     # ) f2
#     # on
#     # f.sku = f2.sku and f.site = f2.site
#     df_temp = conn_ck.ck_select_to_df(sql)
#     df_listing = pd.concat([df_temp, df_listing])
# df_listing.columns = [i.split('.')[-1] for i in df_listing.columns]
# ##
# df_listing = df_listing.rename(columns={'site':'站点'})
# df_jie_kou = freight_interface_fu(df_listing, table_name='freight_interface_amazon')
# df_listing = df_listing.merge(df_jie_kou, on=['站点', 'sku'], how='left')
##
##
# 价差分析取数
##
# sql1 = """
#
#     SELECT
#         `定价比值分段_2`, sum(inv_0_90_money) inv_0_90_money, sum(inv_90_180_money) inv_90_180_money, sum(inv_180_270_money) inv_180_270_money,
#         sum(inv_270_365_money) inv_270_365_money, sum(inv_365_money) inv_365_money, sum(stock_money) stock_money
#     FROM (
#         SELECT
#             asin, fba_seller, fbm_seller, sku, site1, fba_price_rmb, fbm_price_rmb,`定价价差`,`定价比值`,`定价价差分段`,`定价比值分段`,`定价比值分段_2`,first_trip_source,
#             cost, first_trip_fee_rmb, fbm_fees, available,inv_age_0_to_90_days,inv_age_91_to_180_days,inv_age_181_to_270_days,inv_age_271_to_365_days,inv_age_365_plus_days,
#             available*cost stock_money,inv_age_0_to_90_days*cost inv_0_90_money,inv_age_91_to_180_days*cost inv_90_180_money,inv_age_181_to_270_days*cost inv_180_270_money
#             ,inv_age_271_to_365_days*cost inv_270_365_money,inv_age_365_plus_days*cost inv_365_money,
#             `销售额`,`净利润`,`日均销量分段`,`fba头程分段`,`fbm运费分段`
#         FROM over_sea.price_ratio_analysis
#         WHERE site1 = '日本'
#     ) a
#     GROUP BY `定价比值分段_2`
#
# """
# sql2 = """
#     SELECT
#         `ky定价比值分段`, sum(inv_0_90_money) inv_0_90_money, sum(inv_90_180_money) inv_90_180_money, sum(inv_180_270_money) inv_180_270_money,
#         sum(inv_270_365_money) inv_270_365_money, sum(inv_365_money) inv_365_money, sum(stock_money) stock_money
#     FROM (
#         SELECT
#             asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
#             ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
#             `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
#             `日均销量`,'日均销量分段'
#         FROM over_sea.price_ratio_analysis_20231212_泛欧
#     ) a
#     GROUP BY `ky定价比值分段`
# """
# sql3 = """
#
#     SELECT
#         `定价价差分段`, sum(inv_0_90_money) inv_0_90_money, sum(inv_90_180_money) inv_90_180_money, sum(inv_180_270_money) inv_180_270_money,
#         sum(inv_270_365_money) inv_270_365_money, sum(inv_365_money) inv_365_money, sum(stock_money) stock_money
#     FROM (
#         SELECT
#             asin, fba_seller, fbm_seller, sku, site1, fba_price_rmb, fbm_price_rmb,`定价价差`,`定价比值`,`定价价差分段`,`定价比值分段`,`定价比值分段_2`,first_trip_source,
#             cost, first_trip_fee_rmb, fbm_fees, available,inv_age_0_to_90_days,inv_age_91_to_180_days,inv_age_181_to_270_days,inv_age_271_to_365_days,inv_age_365_plus_days,
#             available*cost stock_money,inv_age_0_to_90_days*cost inv_0_90_money,inv_age_91_to_180_days*cost inv_90_180_money,inv_age_181_to_270_days*cost inv_180_270_money
#             ,inv_age_271_to_365_days*cost inv_270_365_money,inv_age_365_plus_days*cost inv_365_money,
#             `销售额`,`净利润`,`日均销量分段`,`fba头程分段`,`fbm运费分段`
#         FROM over_sea.price_ratio_analysis
#         WHERE site1 = '日本'
#     ) a
#     GROUP BY `定价价差分段`
#
# """
# sql4 = """
#
#     SELECT
#         `kh定价比值分段`, sum(`净利润`) `净利润`, sum(`销售额`) `销售额`, sum(`净利润`)/sum(`销售额`) as `净利润率`
#     FROM (
#         SELECT
#             asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
#             ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
#             `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
#             `日均销量`,'日均销量分段'
#         FROM over_sea.price_ratio_analysis_20231211_泛欧
#     ) a
#     GROUP BY `kh定价比值分段`
#
# """

conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
# df = conn.read_sql(sql2)
##
def pr_and_stock(site='泛欧'):
    cols = ['ky定价比值分段','hy定价比值分段','tl定价比值分段','kh定价比值分段']
    df = pd.DataFrame()
    for i in cols:
        sql = f"""
        SELECT
            `{i}`, sum(inv_0_90_money) inv_0_90_money, sum(inv_90_180_money) inv_90_180_money, sum(inv_180_270_money) inv_180_270_money,
            sum(inv_270_365_money) inv_270_365_money, sum(inv_365_money) inv_365_money, sum(stock_money) stock_money
        FROM (
            SELECT
                asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
                `日均销量`,`日均销量分段`
            FROM over_sea.price_ratio_analysis_20231213_{site}
        ) a
        GROUP BY `{i}` 
        """
        df_temp = conn.read_sql(sql)
        df_temp = df_temp.sort_values(by=i, ascending=True)
        df = pd.concat([df_temp, df])
    df.to_excel('df.xlsx', index=0)
    return df
#
def pr_and_m3(site='泛欧'):
    cols = ['ky定价比值分段','hy定价比值分段','tl定价比值分段','kh定价比值分段']
    df = pd.DataFrame()
    for i in cols:
        sql = f"""
            SELECT
                `{i}`, sum(`净利润`)/sum(`销售额`) as `净利润率`, sum(`净利润`) `净利润`, sum(`销售额`) `销售额` 
            FROM (
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
                    `日均销量`,`日均销量分段`
                FROM over_sea.price_ratio_analysis_20231213_{site}
            ) a
            GROUP BY `{i}`
        """
        df_temp = conn.read_sql(sql)
        df_temp = df_temp.sort_values(by=i, ascending=True)
        df = pd.concat([df_temp, df])
    df.to_excel('df.xlsx', index=0)
    return df
#
def pr_and_sales(site='泛欧'):
    cols = ['ky定价比值分段','hy定价比值分段','tl定价比值分段','kh定价比值分段']
    df = pd.DataFrame()
    writer = pd.ExcelWriter('df0.xlsx', engine='openpyxl')
    for i in cols:
        sql = f"""
            SELECT
            `{i}`, `日均销量分段`, count(1) as cnt
        FROM (
            SELECT
                asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
                `日均销量`,`日均销量分段`
            FROM over_sea.price_ratio_analysis_20231213_{site}
        ) a
        GROUP BY `{i}`,`日均销量分段`
        """
        df_temp = conn.read_sql(sql)
        # df_temp = df_temp.sort_values(by=i, ascending=True)
        # df_temp = df_temp.set_index([i,'日均销量分段']).unstack().reset_index()
        df_temp.to_excel(writer, sheet_name=i, index=0)
    writer.save()
    # df = pd.concat([df_temp, df])
    # df.to_excel('df.xlsx', index=0)
    return df
#
def fr_and_sales(site='泛欧'):
    cols = ['运费比分段','总运费比分段','空运总运费比分段','海运总运费比分段','铁路总运费比分段','卡航总运费比分段']
    df = pd.DataFrame()
    for i in cols:
        sql = f"""
            SELECT
                `{i}`, sum(`净利润`)/sum(`销售额`) as `净利润率`, sum(`净利润`) `净利润`, sum(`销售额`) `销售额` 
            FROM (
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`空运总运费比分段`,`海运总运费比分段`,`铁路总运费比分段`,`卡航总运费比分段`,
                    `日均销量`,'日均销量分段'
                FROM over_sea.price_ratio_analysis_20231213_{site}
            ) a
            GROUP BY `{i}`
        """
        df_temp = conn.read_sql(sql)
        df_temp = df_temp.sort_values(by=i, ascending=True)
        df = pd.concat([df_temp, df])
    df.to_excel('df.xlsx', index=0)
    return df
#
def pr_and_stock_all():
    cols = ['ky定价比值分段','hy定价比值分段','tl定价比值分段','kh定价比值分段']
    site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','新加坡','日本','沙特','澳大利亚','美国','英国']
    df = pd.DataFrame()
    for i in cols:
        df_temp = pd.DataFrame()
        for j in site_cols:
            sql = f"""
            SELECT
                `{i}`, sum(inv_0_90_money) inv_0_90_money, sum(inv_90_180_money) inv_90_180_money, sum(inv_180_270_money) inv_180_270_money,
                sum(inv_270_365_money) inv_270_365_money, sum(inv_365_money) inv_365_money, sum(stock_money) stock_money
            FROM (
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
                    `日均销量`,`日均销量分段`
                FROM over_sea.price_ratio_analysis_20231213_{j}
            ) a
            GROUP BY `{i}` 
            """
            df_temp_0 = conn.read_sql(sql)
            df_temp = pd.concat([df_temp, df_temp_0])
            df_temp = df_temp.groupby(i).sum().reset_index()
        df_temp = df_temp.sort_values(by=i, ascending=True)
        df = pd.concat([df_temp, df])
    df.to_excel('df.xlsx', index=0)
    return df
#
def pr_and_m3_all():
    cols = ['ky定价比值分段','hy定价比值分段','tl定价比值分段','kh定价比值分段']
    site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','新加坡','日本','沙特','澳大利亚','美国','英国']
    df = pd.DataFrame()
    for i in cols:
        df_temp = pd.DataFrame()
        for j in site_cols:
            sql = f"""
            SELECT
                `{i}`, sum(`净利润`) `净利润`, sum(`销售额`) `销售额` 
            FROM (
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
                    `日均销量`,`日均销量分段`
                FROM over_sea.price_ratio_analysis_20231213_{j}
            ) a
            GROUP BY `{i}` 
            """
            df_temp_0 = conn.read_sql(sql)
            df_temp = pd.concat([df_temp, df_temp_0])
            df_temp = df_temp.groupby(i).sum().reset_index()
        df_temp = df_temp.sort_values(by=i, ascending=True)
        df_temp['净利润率'] = df_temp['净利润']/df_temp['销售额']
        df = pd.concat([df_temp, df])
    df.to_excel('df.xlsx', index=0)
    return df
#
def pr_and_sales_all():
    cols = ['ky定价比值分段','hy定价比值分段','tl定价比值分段','kh定价比值分段']
    site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','新加坡','日本','沙特','澳大利亚','美国','英国']
    df = pd.DataFrame()
    writer = pd.ExcelWriter('df0.xlsx', engine='openpyxl')
    for i in cols:
        df_temp = pd.DataFrame()
        for j in site_cols:
            sql = f"""
            SELECT
                `{i}`,`日均销量分段`,count(1) as cnt 
            FROM (
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`,`ky价差分段`,`ky定价比值分段`,`hy定价比值分段`,`tl定价比值分段`,`kh定价比值分段`,
                    `日均销量`,`日均销量分段`
                FROM over_sea.price_ratio_analysis_20231213_{j}
            ) a
            GROUP BY `{i}`,`日均销量分段`
            """
            df_temp_0 = conn.read_sql(sql)
            df_temp = pd.concat([df_temp, df_temp_0])
            df_temp = df_temp.groupby([i,'日均销量分段']).sum().reset_index()
        df_temp = df_temp.sort_values(by=i, ascending=True)
        df_temp.to_excel(writer, sheet_name=i, index=0)
        # df = pd.concat([df_temp, df])
    # df.to_excel('df.xlsx', index=0)
    writer.save()
    return df
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
# df = pr_and_m3()
# df = pr_and_stock()
# df = pr_and_sales(site='泛欧')
# df = fr_and_sales(site='泛欧')
# df = pr_and_stock_all()
# df = pr_and_stock_all()
df = pr_and_sales_all()
##
# 全量数据重加工
def fr_and_m3_all():
    # site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','新加坡','日本','沙特','澳大利亚','美国','英国']
    # col_list = ['空运总运费比', '海运总运费比', '铁路总运费比', '卡航总运费比']
    col_list = ['卡航总运费比']
    site_cols = ['泛欧','英国']
    df = pd.DataFrame()
    for i in col_list:
        df_temp = pd.DataFrame()
        for j in site_cols:
            sql = f"""
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`, toucheng_kongyun/fbm_fees `空运总运费比`, toucheng_haiyun/fbm_fees `海运总运费比`,
                    toucheng_tl/fbm_fees `铁路总运费比`, toucheng_kh/fbm_fees `卡航总运费比`, `日均销量`,`日均销量分段`
                FROM over_sea.price_ratio_analysis_20231213_{j}
            """
            df_info = conn.read_sql(sql)
            if i == '空运总运费比':
                df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                             bins=[-1, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6,
                                                   0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1,1.05,1.1,1.15,1.2,1.3,1.4,1.5,1.6,2,3, 9],
                                             labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                                     'e:(0.2, 0.25]', 'f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]',
                                                     'i:(0.4, 0.45]',
                                                     'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]',
                                                     'n:(0.65, 0.7]', 'o:(0.7, 0.75]', 'p:(0.75, 0.8)', 'q:(0.8, 0.85)', 'r:(0.85, 0.9)',
                                                     's:(0.9, 0.95)','t:(0.95,1)','u:(1,1.05)','v:(1.05,1.1)','w:(1.1,1.15)','x:(1.15,1.2)',
                                                     'y:(1.2,1.3)','z1:(1.3,1.4)','z2:(1.4,1.5)','z3:(1.5,1.6)','z4:(1.6,2)','z5:(2,3)','z6:(3,+)'])
                df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 3, 'z6:(3,+)', df_info[f'{i}分段'])
            else:
                df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                             bins=[-1, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1,
                                                   0.12, 0.15, 0.2, 0.25, 0.4, 0.5, 0.6, 9],
                                             labels=['a:(0, 0.005]', 'b:(0.005, 0.01]', 'c:(0.01, 0.02]', 'd:(0.02, 0.03]',
                                                     'e:(0.03, 0.04]', 'f:(0.04, 0.05]', 'g:(0.05, 0.06', 'h:(0.06, 0.07]',
                                                     'i:(0.07, 0.08]',
                                                     'j:(0.08, 0.09]', 'k:(0.09, 0.1]', 'l:(0.1, 0.12]', 'm:(0.12, 0.15]',
                                                     'n:(0.15, 0.2]',
                                                     'o:(0.2, 0.25]', 'p:(0.25, 0.4)', 'q:(0.4, 0.5)', 'r:(0.5, 0.6)',
                                                     's:(0.6, +)'])
                df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 0.6, 's:(0.6, +)', df_info[f'{i}分段'])
            df_temp_0 = df_info.groupby(f'{i}分段').agg({'净利润': 'sum', '销售额': 'sum'}).reset_index()
            df_temp = pd.concat([df_temp, df_temp_0])
            df_temp = df_temp.groupby(f'{i}分段').sum().reset_index()
        df_temp = df_temp.sort_values(by=f'{i}分段', ascending=True)
        df_temp['净利润率'] = df_temp['净利润'] / df_temp['销售额']
        df = pd.concat([df_temp, df])

    return df

def fr_and_stock_all():
    # site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','新加坡','日本','沙特','澳大利亚','美国','英国']
    # site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','日本','澳大利亚','美国','英国']
    # site_cols = ['泛欧']
    site_cols = ['泛欧','英国']
    # col_list = ['空运总运费比', '海运总运费比', '铁路总运费比', '卡航总运费比']
    col_list = ['卡航总运费比']
    df = pd.DataFrame()
    for i in col_list:
        df_temp = pd.DataFrame()
        for j in site_cols:
            sql = f"""
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`, toucheng_kongyun/fbm_fees `空运总运费比`, toucheng_haiyun/fbm_fees `海运总运费比`,
                    toucheng_tl/fbm_fees `铁路总运费比`, toucheng_kh/fbm_fees `卡航总运费比`, `日均销量`,`日均销量分段`
                FROM over_sea.price_ratio_analysis_20231214_{j}
            """
            df_info = conn.read_sql(sql)
            if i == '空运总运费比':
                df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                             bins=[-1, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6,
                                                   0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1,1.05,1.1,1.15,1.2,1.3,1.4,1.5,1.6,2,3, 9],
                                             labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                                     'e:(0.2, 0.25]', 'f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]',
                                                     'i:(0.4, 0.45]',
                                                     'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]',
                                                     'n:(0.65, 0.7]', 'o:(0.7, 0.75]', 'p:(0.75, 0.8)', 'q:(0.8, 0.85)', 'r:(0.85, 0.9)',
                                                     's:(0.9, 0.95)','t:(0.95,1)','u:(1,1.05)','v:(1.05,1.1)','w:(1.1,1.15)','x:(1.15,1.2)',
                                                     'y:(1.2,1.3)','z1:(1.3,1.4)','z2:(1.4,1.5)','z3:(1.5,1.6)','z4:(1.6,2)','z5:(2,3)','z6:(3,+)'])
                df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 3, 'z6:(3,+)', df_info[f'{i}分段'])
            else:
                df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                             bins=[-1, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1,
                                                   0.12, 0.15, 0.2, 0.25, 0.4, 0.5, 0.6, 9],
                                             labels=['a:(0, 0.005]', 'b:(0.005, 0.01]', 'c:(0.01, 0.02]', 'd:(0.02, 0.03]',
                                                     'e:(0.03, 0.04]', 'f:(0.04, 0.05]', 'g:(0.05, 0.06', 'h:(0.06, 0.07]',
                                                     'i:(0.07, 0.08]',
                                                     'j:(0.08, 0.09]', 'k:(0.09, 0.1]', 'l:(0.1, 0.12]', 'm:(0.12, 0.15]',
                                                     'n:(0.15, 0.2]',
                                                     'o:(0.2, 0.25]', 'p:(0.25, 0.4)', 'q:(0.4, 0.5)', 'r:(0.5, 0.6)',
                                                     's:(0.6, +)'])
                df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 0.6, 's:(0.6, +)', df_info[f'{i}分段'])
            df_temp_0 = df_info.groupby(f'{i}分段').agg({'inv_0_90_money': 'sum', 'inv_90_180_money': 'sum', 'inv_180_270_money': 'sum', 'inv_270_365_money': 'sum',
                                                         'inv_365_money': 'sum', 'stock_money': 'sum'}).reset_index()
            df_temp = pd.concat([df_temp, df_temp_0])
            df_temp = df_temp.groupby(f'{i}分段').sum().reset_index()
        df_temp = df_temp.sort_values(by=f'{i}分段', ascending=True)
        df = pd.concat([df_temp, df])

    return df

def fr_and_sales_all():
    # site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','新加坡','日本','沙特','澳大利亚','美国','英国']
    # site_cols = ['泛欧','中东','加拿大','墨西哥','巴西','日本','澳大利亚','美国','英国']
    # site_cols = ['泛欧']
    site_cols = ['泛欧','英国']
    # col_list = ['空运总运费比', '海运总运费比', '铁路总运费比', '卡航总运费比']
    col_list = ['卡航总运费比']
    df = pd.DataFrame()
    for i in col_list:
        df_temp = pd.DataFrame()
        for j in site_cols:
            sql = f"""
                SELECT
                    asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
                    ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
                    `销售额`,`净利润`,`运费比分段`,`总运费比分段`, toucheng_kongyun/fbm_fees `空运总运费比`, toucheng_haiyun/fbm_fees `海运总运费比`,
                    toucheng_tl/fbm_fees `铁路总运费比`, toucheng_kh/fbm_fees `卡航总运费比`, `日均销量`,`日均销量分段`
                FROM over_sea.price_ratio_analysis_20231214_{j}
            """
            df_info = conn.read_sql(sql)
            if i == '空运总运费比':
                df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                             bins=[-1, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6,
                                                   0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1,1.05,1.1,1.15,1.2,1.3,1.4,1.5,1.6,2,3, 9],
                                             labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                                     'e:(0.2, 0.25]', 'f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]',
                                                     'i:(0.4, 0.45]',
                                                     'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]',
                                                     'n:(0.65, 0.7]', 'o:(0.7, 0.75]', 'p:(0.75, 0.8)', 'q:(0.8, 0.85)', 'r:(0.85, 0.9)',
                                                     's:(0.9, 0.95)','t:(0.95,1)','u:(1,1.05)','v:(1.05,1.1)','w:(1.1,1.15)','x:(1.15,1.2)',
                                                     'y:(1.2,1.3)','z1:(1.3,1.4)','z2:(1.4,1.5)','z3:(1.5,1.6)','z4:(1.6,2)','z5:(2,3)','z6:(3,+)'])
                df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 3, 'z6:(3,+)', df_info[f'{i}分段'])
            else:
                df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                             bins=[-1, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1,
                                                   0.12, 0.15, 0.2, 0.25, 0.4, 0.5, 0.6, 9],
                                             labels=['a:(0, 0.005]', 'b:(0.005, 0.01]', 'c:(0.01, 0.02]', 'd:(0.02, 0.03]',
                                                     'e:(0.03, 0.04]', 'f:(0.04, 0.05]', 'g:(0.05, 0.06', 'h:(0.06, 0.07]',
                                                     'i:(0.07, 0.08]',
                                                     'j:(0.08, 0.09]', 'k:(0.09, 0.1]', 'l:(0.1, 0.12]', 'm:(0.12, 0.15]',
                                                     'n:(0.15, 0.2]',
                                                     'o:(0.2, 0.25]', 'p:(0.25, 0.4)', 'q:(0.4, 0.5)', 'r:(0.5, 0.6)',
                                                     's:(0.6, +)'])
                df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 0.6, 's:(0.6, +)', df_info[f'{i}分段'])
            df_temp_0 = df_info.groupby([f'{i}分段','日均销量分段']).agg({'asin': 'count'}).reset_index()
            df_temp = pd.concat([df_temp, df_temp_0])
            df_temp = df_temp.groupby([f'{i}分段','日均销量分段']).sum().reset_index()
        df_temp = df_temp.sort_values(by=f'{i}分段', ascending=True)
        df = pd.concat([df_temp, df])

    return df
##
# df = fr_and_stock_all()
df = fr_and_sales_all()
##
df.to_excel('df.xlsx', index=0)
##
# 相关性分析
sql = f"""
    SELECT
        asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
        ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
        `销量`,`销售额`,`净利润`,`净利润`/`销售额` as `净利润率`, `ky定价比值`,`hy定价比值`,`tl定价比值`,`kh定价比值`,`运费比`,`总运费比`, toucheng_kongyun/fbm_fees `空运总运费比`, toucheng_haiyun/fbm_fees `海运总运费比`,
        toucheng_tl/fbm_fees `铁路总运费比`, toucheng_kh/fbm_fees `卡航总运费比`
    FROM over_sea.price_ratio_analysis_20231213_泛欧
    WHERE `销售额` > 0
"""
df_cor_data = conn.read_sql(sql)
##
# df_cor_data['净利润率'] = df_cor_data['净利润'] / df_cor_data['销售额']
##
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
fig, ax = plt.subplots(figsize=(12, 7))
df_data_temp = df_cor_data[(df_cor_data['空运总运费比']<3) & (df_cor_data['净利润率']>-1)]
ax.plot(df_data_temp['空运总运费比'], df_data_temp['净利润率'], "o")
##
a = df_cor_data[df_cor_data['销量'] > 60].corr(method="pearson")
##
a = a.round(2)
##
import seaborn as sea
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.subplots(figsize = (16,12))
sea.heatmap(a,annot = True,vmax = 1,square = True,cmap = "Reds")
plt.show()
##
#重加工
sql = f"""
    SELECT
        asin, seller_sku, available*`成本` stock_money,inv_age_0_to_90_days*`成本` inv_0_90_money,inv_age_91_to_180_days*`成本` inv_90_180_money,inv_age_181_to_270_days*`成本` inv_180_270_money
        ,inv_age_271_to_365_days*`成本` inv_270_365_money,inv_age_365_plus_days*`成本` inv_365_money,
        `销售额`,`净利润`,`运费比分段`,`总运费比分段`, toucheng_kongyun/fbm_fees `空运总运费比`, toucheng_haiyun/fbm_fees `海运总运费比`,
        toucheng_tl/fbm_fees `铁路总运费比`, toucheng_kh/fbm_fees `卡航总运费比`, `日均销量`,`日均销量分段`
    FROM over_sea.price_ratio_analysis_20231213_泛欧
"""
df_info = conn.read_sql(sql)
##
col_list = ['空运总运费比', '海运总运费比', '铁路总运费比', '卡航总运费比']
for i in col_list:
    if i == '空运总运费比':
        df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                bins=[-1, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65,
                                      0.7, 0.75, 0.8, 1, 2, 9],
                                labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                        'e:(0.2, 0.25]', 'f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]',
                                        'i:(0.4, 0.45]',
                                        'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]',
                                        'n:(0.65, 0.7]',
                                        'o:(0.7, 0.75]', 'p:(0.75, 0.8)', 'q:(0.8, 1)', 'r:(1, 2)', 's:(2, +)'])
        df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 2, 's:(2, +)', df_info[f'{i}分段'])
    else:
        df_info[f'{i}分段'] = pd.cut(df_info[f'{i}'],
                                bins=[-1, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.12, 0.15, 0.2, 0.25, 0.4, 0.5, 0.6, 9],
                                labels=['a:(0, 0.005]', 'b:(0.005, 0.01]', 'c:(0.01, 0.02]', 'd:(0.02, 0.03]',
                                        'e:(0.03, 0.04]', 'f:(0.04, 0.05]', 'g:(0.05, 0.06', 'h:(0.06, 0.07]',
                                        'i:(0.07, 0.08]',
                                        'j:(0.08, 0.09]', 'k:(0.09, 0.1]', 'l:(0.1, 0.12]', 'm:(0.12, 0.15]',
                                        'n:(0.15, 0.2]',
                                        'o:(0.2, 0.25]', 'p:(0.25, 0.4)', 'q:(0.4, 0.5)', 'r:(0.5, 0.6)', 's:(0.6, +)'])
        df_info[f'{i}分段'] = np.where(df_info[f'{i}'] >= 0.6, 's:(0.6, +)', df_info[f'{i}分段'])
##
# 运费比与净利率
df = pd.DataFrame()
for i in col_list:
    df_temp = df_info.groupby(f'{i}分段').agg({'净利润':'sum','销售额':'sum'}).reset_index()
    df_temp['净利润率'] = df_temp['净利润']/df_temp['销售额']
    df_temp = df_temp.sort_values(by=f'{i}分段', ascending=True)
    df = pd.concat([df_temp, df])
df.to_excel('df.xlsx',index=0)
##
# 运费比与库存金额

df = pd.DataFrame()
for i in col_list:
    df_temp = df_info.groupby(f'{i}分段').agg({'inv_0_90_money':'sum','inv_90_180_money':'sum','inv_180_270_money':'sum','inv_270_365_money':'sum',
                                               'inv_365_money':'sum','stock_money':'sum'}).reset_index()
    df_temp = df_temp.sort_values(by=f'{i}分段', ascending=True)
    df = pd.concat([df_temp, df])
df.to_excel('df.xlsx',index=0)
##
# 运费比与日销
df = pd.DataFrame()
writer = pd.ExcelWriter('df0.xlsx', engine='openpyxl')
for i in col_list:
    df_temp = df_info.groupby([f'{i}分段','日均销量分段']).agg({'asin':'count'}).reset_index()
    df_temp = df_temp.sort_values(by=f'{i}分段', ascending=True)
    df = pd.concat([df_temp, df])
    df_temp.to_excel(writer, sheet_name=i, index=0)
writer.save()
##
sql = """
    SELECT
        asin, fba_seller, fbm_seller, sku, site1, fba_price_rmb, fbm_price_rmb,`定价价差`,`定价比值`,`定价价差分段`,`定价比值分段`,`定价比值分段_2`,first_trip_source,
        cost, first_trip_fee_rmb, fbm_fees, first_trip_fee_rmb/fbm_fees as `运费比`,
        `销售额`,`净利润`,`日均销量分段`,`fba头程分段`,`fbm运费分段`
    FROM over_sea.price_ratio_analysis
    WHERE site1 = '泛欧'
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_all = conn.read_sql(sql)
##
df_all['运费比分段'] = pd.cut(df_all[f'运费比'], bins=[0, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7,0.75, 0.8, 1, 2, 9],
                                            labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                                    'e:(0.2, 0.25]','f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]', 'i:(0.4, 0.45]',
                                                    'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]','n:(0.65, 0.7]',
                                                    'o:(0.7, 0.75]', 'p:(0.75, 0.8)','q:(0.8, 1)','r:(1, 2)','s:(2, +)'])
df_all['运费比分段'] = np.where(df_all[f'运费比'] >= 2, 's:(2, +)', df_all['运费比分段'])
##
df_all['运费比分段'].value_counts()
##
df_all.to_excel('df.xlsx', index=0)
##
def get_listing(site='泛欧'):
    # date_today = time.strftime('%Y%m%d')
    date_today = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    # 获取FBA全量链接信息
    # 由于数量较大，采用分批取数的方式
    if site == '泛欧':
        sql = f"""
            SELECT distinct account_id
            FROM (
                SELECT 
                    distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site,
                    case 
                        when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
                        else c.site1
                    end as site2
                FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
                LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
                ON toInt32(a.account_id) = toInt32(b.id)
                LEFT JOIN domestic_warehouse_clear.site_table c
                ON b.site = c.site
                WHERE a.fulfillment_channel = 'AMA'
            )
            WHERE site2 = '泛欧'
        """
    if site == '非泛欧':
        sql = f"""
            SELECT distinct account_id
            FROM (
                SELECT 
                    distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site,
                    case 
                        when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
                        else c.site1
                    end as site2
                FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
                LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
                ON toInt32(a.account_id) = toInt32(b.id)
                LEFT JOIN domestic_warehouse_clear.site_table c
                ON b.site = c.site
                WHERE a.fulfillment_channel = 'AMA'
            )
            WHERE site2 != '泛欧'
        """
    df_account = conn_ck.ck_select_to_df(sql)
    print(f'店铺总数量：{len(df_account)}')
    # sql = f"""
    #     SELECT count()
    #     FROM yibai_product_kd_sync.yibai_amazon_listing_alls
    #     WHERE fulfillment_channel = 'AMA'
    # """
    # df_cnt = conn_ck.ck_select_to_df(sql)
    # print(df_cnt)
    #
    step = 100
    account_list = [df_account[i:i + step] for i in range(0, int(len(df_account)/10), step)]
    #
    df_listing = pd.DataFrame()
    time1 = time.time()
    for a in account_list:
        account_tuple = tuple(a['account_id'].unique())
        sql = f"""
            SELECT 
                f.account_id, f.asin, f.seller_sku as seller_sku, a.seller_sku as fbm_seller, e.sku, f.site, f.site2, 
                case when r.commission_rate is Null then 0.15 else r.commission_rate end commission_rate, f.FBM_difference,f.FBM_profit, 
                cost, first_trip_source, first_trip_fee_rmb, toucheng_kongyun,toucheng_haiyun,toucheng_tl,
                toucheng_kh,e.fba_fees_rmb, m.available, 
                p.inv_age_0_to_90_days, p.inv_age_91_to_180_days, p.inv_age_181_to_270_days,p.inv_age_271_to_365_days,p.inv_age_365_plus_days
            FROM (
                SELECT 
                    distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site,
                    case 
                        when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
                        else c.site1
                    end as site2, FBM_difference , FBM_profit
                FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
                LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
                ON toInt32(a.account_id) = toInt32(b.id)
                LEFT JOIN domestic_warehouse_clear.site_table c
                ON b.site = c.site
                WHERE a.fulfillment_channel = 'AMA' and a.account_id in {account_tuple} 
            ) f
            INNER JOIN (
                SELECT 
                    account_id, seller_sku, asin, sku, cost, length, width, high, weight, first_trip_source, fba_fees*rate as fba_fees_rmb,
                    rate,first_trip_fee_rmb,toucheng_kongyun,toucheng_haiyun,toucheng_tl,toucheng_kh 
                FROM domestic_warehouse_clear.fba_fees
                WHERE account_id in {account_tuple}
            ) e
            ON f.account_id = e.account_id and f.seller_sku = e.seller_sku and f.asin = e.asin
            LEFT JOIN (
                SELECT distinct account_id, asin1 as asin, seller_sku
                FROM yibai_product_kd_sync.yibai_amazon_listing_alls
                WHERE fulfillment_channel != 'AMA' and account_id in {account_tuple}
            ) a 
            ON f.account_id = a.account_id and f.asin = a.asin
            LEFT JOIN (
                SELECT account_id, seller_sku, `佣金率` commission_rate
                FROM domestic_warehouse_clear.yibai_amazon_referral_fee
                WHERE account_id in {account_tuple} 
            ) r
            ON f.account_id = r.account_id and f.seller_sku = r.seller_sku
            LEFT JOIN (
                SELECT account_id, sku, afn_warehouse_quantity as available,afn_fulfillable_quantity as available0
                FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end 
                WHERE toInt32(account_id) in {account_tuple}
            ) m
            ON toInt32(f.account_id) = toInt32(m.account_id) and f.seller_sku = m.sku
            LEFT JOIN (
                SELECT account_id, sku, inv_age_0_to_90_days, inv_age_91_to_180_days, inv_age_181_to_270_days,inv_age_271_to_365_days,inv_age_365_plus_days
                FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_aged_planning_data 
                WHERE toInt32(account_id) in {account_tuple}
            ) p
            ON toInt32(f.account_id) = toInt32(p.account_id) and f.seller_sku = p.sku
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        df_listing = pd.concat([df_temp, df_listing])
    df_listing.columns = [i.split('.')[-1] for i in df_listing.columns]
    time2 = time.time()
    t = time2 - time1
    print(f"FBA链接信息获取完成. 共耗时{'%.0f' % t}s")
    return df_listing

def get_linelist(df):
    sql = """select distinct a.sku as sku,c.category_name as `一级产品线`
                from yibai_prod_base_sync.yibai_prod_sku a 
                left join yibai_prod_base_sync.yibai_prod_category b 
                on a.product_category_id=b.id
                left join yibai_prod_base_sync.yibai_prod_category c 
                on b.category_id_level_1=c.id"""
    conn = pd_to_ck(database='fba_ag_over_180_days_price_adjustment', data_sys='调价明细历史数据')
    linlist = conn.ck_select_to_df(sql)
    df['sku'] = df['sku'].astype(str)
    linlist['sku'] = linlist['sku'].astype(str)

    df = df.merge(linlist, on=['sku'], how='left')
    df = df.fillna({'一级产品线': 'other'})
    return df

def get_4_difference(df, type1='FBA'):
    df.rename(columns={'cost': '成本'}, inplace=True)
    df.rename(columns={'site': '站点'}, inplace=True)
    # type: 'FBA','FBM','ALL'
    sql = """
    SELECT case when a.shipping_type =1 THEN 'FBM' WHEN 3 THEN 'FBA' END AS `渠道`,
    case when a.site = 'other' then 'other' else c.site1  end as `站点`,
    case when b.category_name ='' then 'other' else b.category_name end as `一级产品线`,a.cost_range `成本段`,
    toFloat64(net_profit2) `毛净利差值`,toFloat64(true_refund_profit) `真实退款率`
    FROM yibai_sale_center_listing_sync.yibai_listing_profit_config a
    left join yibai_prod_base_sync.yibai_prod_category b on a.first_product_line = b.id
    left join domestic_warehouse_clear.site_table c on a.site = UPPER(c.site) 
    where a.platform_code ='AMAZON' AND a.shipping_type in (1,3)  and a.is_del = 0 and a.status = 1
    HAVING `站点` <> ''
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    cha = conn_mx.ck_select_to_df(sql)
    cha['毛净利差值'] = cha['毛净利差值'] / 100
    cha['真实退款率'] = cha['真实退款率'] / 100
    cha = cha[cha['渠道'] == type1]
    cha = cha.drop_duplicates(['站点', '一级产品线', '成本段'])
    cha.drop(['渠道'], axis=1, inplace=True)

    cha1 = cha.loc[(cha['成本段'] == 'other'), ['站点', '一级产品线', '毛净利差值', '真实退款率']].drop_duplicates(
        ['站点', '一级产品线'])
    cha1.rename(columns={'毛净利差值': '毛净利差值1', '真实退款率': '真实退款率1'}, inplace=True)

    cha2 = cha.loc[(cha['一级产品线'] == 'other') & (cha['成本段'] == 'other'), ['站点', '毛净利差值',
                                                                                 '真实退款率']].drop_duplicates(
        ['站点'])
    cha2.rename(columns={'毛净利差值': '毛净利差值2', '真实退款率': '真实退款率2'}, inplace=True)
    if type1 == 'FBA':
        df['成本段'] = 'other'
        df.loc[(df['成本'] <= 20) & (df['成本'] > 0), '成本段'] = '0,20'
        df.loc[(df['成本'] <= 100) & (df['成本'] > 20), '成本段'] = '20,100'
        df.loc[df['成本'] > 100, '成本段'] = '100,100000'
    elif type1 == 'FBM':
        df['成本段'] = 'other'
        df.loc[(df['成本'] <= 100) & (df['成本'] > 0), '成本段'] = '0,100'
        df.loc[(df['成本'] <= 300) & (df['成本'] > 100), '成本段'] = '100,300'
        df.loc[df['成本'] > 300, '成本段'] = '300,100000'
    df = df.merge(cha, on=['站点', '一级产品线', '成本段'], how='left')
    df = df.merge(cha1, on=['一级产品线', '站点'], how='left')
    df.loc[df['毛净利差值'].isnull(), '毛净利差值'] = df['毛净利差值1']
    df.loc[df['真实退款率'].isnull(), '真实退款率'] = df['真实退款率1']
    df = df.merge(cha2, on=['站点'], how='left')
    df.loc[df['毛净利差值'].isnull(), '毛净利差值'] = df['毛净利差值2']
    df.loc[df['真实退款率'].isnull(), '真实退款率'] = df['真实退款率2']
    df.drop(['毛净利差值1', '毛净利差值2', '真实退款率1', '真实退款率2'], axis=1, inplace=True)
    return df

def get_price(df):
    df['fba_ky_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_kongyun']
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBM_profit'] - df[
        '税率'] - 0.03)
    df['fba_hy_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_haiyun']
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBM_profit'] - df[
        '税率'] - 0.03)
    df['fba_tl_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_tl']
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBM_profit'] - df[
        '税率'] - 0.03)
    df['fba_kh_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_kh']
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBM_profit'] - df[
        '税率'] - 0.03)
    df['fbm_price_rmb'] = (df['成本'] + df['fbm_fees']) / (
                1 - 0.15 - 0.03 - df['FBM_difference'] - df['FBM_profit'] - df['税率'])
    df[['fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb', 'fba_kh_price_rmb','fbm_price_rmb']] = df[
        ['fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb','fba_kh_price_rmb', 'fbm_price_rmb']].astype(float).round(2)
    return df

def cut_bins(df):
    df['ky价差分段'] = pd.cut(df['ky定价价差'], bins=[-3000,-1000, -10, 0, 5, 10, 20,30,40,50,60,100,1000],
                                labels=['A:(,-100]', 'B:(-100,-10]', 'C:(-10,0]', 'D:(0,5]', 'E:(5,10]','F:(10,20]','G:(20,30]','H:(30,40]','I:(40,50]','J:(50,60]','K:(60,100]','L:(100,+]'])
    df['ky价差分段'] = np.where(df['ky定价价差'] > 100, 'L:(100,+]', df['ky价差分段'])
    df['ky价差分段'] = np.where(df['ky定价价差'] < -100, 'A:(,-100]', df['ky价差分段'])

    df['ky定价比值分段'] = pd.cut(df['ky定价比值'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8,3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]','e:(0.8, 1]','f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]','j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]', 'm:(2.4, 2.6]','n:(2.6, 2.8]','o:(2.8, 3]', 'p:(3, +)'])
    df['ky定价比值分段'] = np.where(df['ky定价比值'] > 5, 'p:(3, +)', df['ky定价比值分段'])
    df['hy定价比值分段'] = pd.cut(df['hy定价比值'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8,3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]','e:(0.8, 1]','f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]','j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]', 'm:(2.4, 2.6]','n:(2.6, 2.8]','o:(2.8, 3]', 'p:(3, +)'])
    df['hy定价比值分段'] = np.where(df['hy定价比值'] > 5, 'p:(3, +)', df['hy定价比值分段'])
    df['hy定价比值分段'] = np.where(df['hy定价比值'].isna(), 'Others', df['hy定价比值分段'])
    df['tl定价比值分段'] = pd.cut(df['tl定价比值'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8,3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]','e:(0.8, 1]','f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]','j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]', 'm:(2.4, 2.6]','n:(2.6, 2.8]','o:(2.8, 3]', 'p:(3, +)'])
    df['tl定价比值分段'] = np.where(df['tl定价比值'] > 5, 'p:(3, +)', df['tl定价比值分段'])
    df['tl定价比值分段'] = np.where(df['tl定价比值'].isna(), 'Others', df['tl定价比值分段'])
    # 修改
    df['kh定价比值分段'] = pd.cut(df['kh定价比值'],
                                  bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8, 3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]', 'e:(0.8, 1]',
                                          'f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]', 'j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]',
                                          'm:(2.4, 2.6]', 'n:(2.6, 2.8]', 'o:(2.8, 3]', 'p:(3, +)'])
    df['kh定价比值分段'] = np.where(df['kh定价比值'] > 5, 'p:(3, +)', df['kh定价比值分段'])
    df['kh定价比值分段'] = np.where(df['kh定价比值'].isna(), 'Others', df['kh定价比值分段'])
    df['运费比分段'] = pd.cut(df[f'运费比'],
                                  bins=[0, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65,
                                        0.7, 0.75, 0.8, 1, 2, 9],
                                  labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                          'e:(0.2, 0.25]', 'f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]',
                                          'i:(0.4, 0.45]',
                                          'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]',
                                          'n:(0.65, 0.7]',
                                          'o:(0.7, 0.75]', 'p:(0.75, 0.8)', 'q:(0.8, 1)', 'r:(1, 2)', 's:(2, +)'])
    df['运费比分段'] = np.where(df[f'运费比'] >= 2, 's:(2, +)', df['运费比分段'])
    col_list = ['总运费比', '空运总运费比', '海运总运费比', '铁路总运费比', '卡航总运费比']
    for i in col_list:
        df[f'{i}分段'] = pd.cut(df[f'{i}'],
                                      bins=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2, 1.3,
                                            1.4, 1.5, 1.6, 2, 4, 20],
                                      labels=['a:(0, 0.1]', 'b:(0.1, 0.2]', 'c:(0.2, 0.3]', 'd:(0.3, 0.4]',
                                              'e:(0.4, 0.5]', 'f:(0.5, 0.6]', 'g:(0.6, 0.7]', 'h:(0.7, 0.8]',
                                              'i:(0.8, 0.9]',
                                              'j:(0.9, 1]', 'k:(1, 1.1]', 'l:(1.1, 1.2]', 'm:(1.2, 1.3]',
                                              'n:(1.3, 1.4]',
                                              'o:(1.4, 1.5]', 'p:(1.5, 1.6)', 'q:(1.6, 2)', 'r:(2, 4)', 's:(4, +)'])
    df[f'{i}分段'] = np.where(df[f'{i}'] >= 4, 's:(4, +)', df[f'{i}分段'])
    return df
##
def dwm_data(df_listing):
    """
    计算定价比
    """
    #
    # (product_cost)+(fba_fees)*rate+(first_trip_fee_rmb)+2)/(1-commission_rate-0.03-FBA_difference-FBA_profit-tax_rate)
    print(df_listing.info())
    df_listing = get_linelist(df_listing)
    df_listing = get_4_difference(df_listing, type1='FBA')
    df_listing = shuilv(df_listing, fb_type='FBA')
    df_jie_kou = freight_interface_fu(df_listing, table_name='freight_interface_amazon')
    print('FBM运费数据获取完成.')
    df_listing = df_listing.merge(df_jie_kou, on=['站点', 'sku'], how='left')
    df_listing = df_listing.rename(columns={'运费': 'fbm_fees'})

    df_listing = get_price(df_listing)
    print('价格计算完成.')
    # 库存数据处理
    df_stock = df_listing[['account_id', 'asin', 'seller_sku', '站点', 'site2', 'available', 'inv_age_0_to_90_days',
                           'inv_age_91_to_180_days',
                           'inv_age_181_to_270_days', 'inv_age_271_to_365_days', 'inv_age_365_plus_days']]
    df_stock = fanou_fanmei(df_stock)
    df_stock['排序'] = 2
    df_stock.loc[df_stock['站点'] == '美国', '排序'] = 1
    df_stock = df_stock.sort_values(['排序', 'available'], ascending=[True, False])
    df_stock = df_stock.drop_duplicates(['标识', 'seller_sku'], 'first')
    df_stock.drop(['排序', '站点'], axis=1, inplace=True)
    df_stock_asin = df_stock.groupby(['asin', 'site2'])['available', 'inv_age_0_to_90_days', 'inv_age_91_to_180_days',
    'inv_age_181_to_270_days', 'inv_age_271_to_365_days', 'inv_age_365_plus_days'].sum().reset_index()
    # 取订单数据
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    # 订单数据统计
    date_today = datetime.datetime.today().strftime('%Y-%m-%d')
    date_start = (datetime.datetime.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    sql = f"""
        SELECT account_id, seller_sku, sum(`sku数量`) as `销量`, sum(`净利润`) as `净利润`, sum(`销售额`) as `销售额`
        FROM domestic_warehouse_clear.monitor_fba_order
        WHERE created_time >= '{date_start}' and created_time < '{date_today}'
        GROUP BY account_id, seller_sku
    """
    df_sales = conn_ck.ck_select_to_df(sql)
    #
    # df_listing.drop(['销量','销售额'], axis=1, inplace=True)
    df_sales_final = pd.merge(df_listing, df_sales, how='left', on=['account_id', 'seller_sku'])

    # 销量处理
    print('按asin+站点聚合信息...')
    column_list = ['account_id', 'seller_sku', 'asin', 'site2', '销量', '销售额', '净利润']
    df_sales_final = df_sales_final[column_list].drop_duplicates(subset=['account_id', 'seller_sku'])
    column_list_2 = ['销量', '销售额', '净利润']
    df_sales_final[column_list_2] = df_sales_final[column_list_2].fillna(0)
    df_sales_final = df_sales_final.groupby(['asin', 'site2']).agg(
        {'销量': 'sum', '销售额': 'sum', '净利润': 'sum'}).reset_index()
    df_listing = df_listing[(~df_listing['fbm_price_rmb'].isna())]
    df_listing['ky定价价差'] = (df_listing['fba_ky_price_rmb'] - df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['ky定价比值'] = (df_listing['fba_ky_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['hy定价比值'] = (df_listing['fba_hy_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['tl定价比值'] = (df_listing['fba_tl_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['kh定价比值'] = (df_listing['fba_kh_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)

    # 链接处理
    col_list = ['asin', 'seller_sku', 'fbm_seller', 'sku', 'site2', '成本',
                'first_trip_source', 'fba_fees_rmb', 'first_trip_fee_rmb', 'toucheng_kongyun', 'toucheng_haiyun',
                'toucheng_tl', 'toucheng_kh', 'fbm_fees',
                'fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb', 'fba_kh_price_rmb', 'fbm_price_rmb']
    df_listing_final = df_listing[col_list]
    df_listing_final = df_listing_final[~df_listing_final['fbm_price_rmb'].isna()].drop_duplicates(
        subset=['asin', 'site2'])
    df_listing_price = df_listing.groupby(['asin', 'site2']).agg(
        {'ky定价价差': 'mean', 'ky定价比值': 'mean', 'hy定价比值': 'mean', 'tl定价比值': 'mean', 'kh定价比值': 'mean'})
    # 按asin + site维度聚合销售数据和库存数据
    df_listing_final = pd.merge(df_listing_final, df_listing_price, how='left', on=['asin', 'site2'])
    df_listing_final = pd.merge(df_listing_final, df_stock_asin, how='left', on=['asin', 'site2'])
    df_listing_final = pd.merge(df_listing_final, df_sales_final, how='left', on=['asin', 'site2'])
    column_list_3 = ['available', 'inv_age_0_to_90_days', 'inv_age_91_to_180_days', 'inv_age_181_to_270_days',
                     'inv_age_271_to_365_days', 'inv_age_365_plus_days']
    #
    df_listing_final[column_list_3] = df_listing_final[column_list_3].apply(pd.to_numeric, errors='coerce').fillna(
        0).astype(int)
    # 分段处理
    df_listing_final['运费比'] = df_listing_final['first_trip_fee_rmb'] / df_listing_final['fbm_fees']
    df_listing_final['总运费比'] = (df_listing_final['first_trip_fee_rmb'] + df_listing_final['fba_fees_rmb']) / \
                                   df_listing_final['fbm_fees']
    df_listing_final['空运总运费比'] = (df_listing_final['toucheng_kongyun'] + df_listing_final['fba_fees_rmb']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final['海运总运费比'] = (df_listing_final['toucheng_haiyun'] + df_listing_final['fba_fees_rmb']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final['铁路总运费比'] = (df_listing_final['toucheng_tl'] + df_listing_final['fba_fees_rmb']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final['卡航总运费比'] = (df_listing_final['toucheng_kh'] + df_listing_final['fba_fees_rmb']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final = cut_bins(df_listing_final)
    #
    df_listing_final['销量'] = df_listing_final['销量'].fillna(0)
    df_listing_final['日均销量'] = (df_listing_final['销量'] / 30).astype(float)

    df_listing_final['日均销量分段'] = pd.cut(df_listing_final['日均销量'],
                                              bins=[-1, 0, 0.1, 0.3, 0.6, 1, 3, 5, 10, 20, 50],
                                              labels=['J:0', 'I:(0,0.1]', 'H:(0.1,0.3]', 'G:(0.3,0.6]', 'F:(0.6,1]',
                                                      'E:(1,3]', 'D:(3,5]', 'C:(5,10]', 'B:(10,20]', 'A:(20,+]'])
    df_listing_final['日均销量分段'] = np.where(df_listing_final['日均销量'] >= 50, 'A:(20,+]',
                                                df_listing_final['日均销量分段'])

    # df_listing_final.to_excel('df_listing_final.xlsx',index=0)

    return df_listing_final
##
# 测试
sql = f"""
    SELECT 
        f.account_id, f.asin, f.seller_sku as seller_sku, a.seller_sku as fbm_seller, e.sku, f.site, f.site2, 
        m.available, 
        p.inv_age_0_to_90_days, p.inv_age_91_to_180_days, p.inv_age_181_to_270_days,p.inv_age_271_to_365_days,p.inv_age_365_plus_days
    FROM (
        SELECT 
            distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site,
            case 
                when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
                else c.site1
            end as site2, FBM_difference , FBM_profit
        FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
        LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
        ON toInt32(a.account_id) = toInt32(b.id)
        LEFT JOIN domestic_warehouse_clear.site_table c
        ON b.site = c.site
        WHERE a.fulfillment_channel = 'AMA' and a.asin1 = 'B07Z56VZ82'
    ) f
    INNER JOIN (
        SELECT 
            account_id, seller_sku, asin, sku, cost, length, width, high, weight, first_trip_source, fba_fees*rate as fba_fees_rmb,
            rate,first_trip_fee_rmb,toucheng_kongyun,toucheng_haiyun,toucheng_tl,toucheng_kh 
        FROM domestic_warehouse_clear.fba_fees
        WHERE asin = 'B07Z56VZ82'
    ) e
    ON f.account_id = e.account_id and f.seller_sku = e.seller_sku and f.asin = e.asin
    LEFT JOIN (
        SELECT distinct account_id, asin1 as asin, seller_sku
        FROM yibai_product_kd_sync.yibai_amazon_listing_alls
        WHERE fulfillment_channel != 'AMA' and asin1 = 'B07Z56VZ82'
    ) a 
    ON f.account_id = a.account_id and f.asin = a.asin
    LEFT JOIN (
        SELECT account_id, sku, afn_warehouse_quantity as available,afn_fulfillable_quantity as available0
        FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end 
        WHERE asin = 'B07Z56VZ82'
    ) m
    ON toInt32(f.account_id) = toInt32(m.account_id) and f.seller_sku = m.sku
    LEFT JOIN (
        SELECT account_id, sku, inv_age_0_to_90_days, inv_age_91_to_180_days, inv_age_181_to_270_days,inv_age_271_to_365_days,inv_age_365_plus_days
        FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_aged_planning_data 
        WHERE asin = 'B07Z56VZ82'
    ) p
    ON toInt32(f.account_id) = toInt32(p.account_id) and f.seller_sku = p.sku
"""
conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
df_temp = conn_ck.ck_select_to_df(sql)
##
df_temp.columns = [i.split('.')[-1] for i in df_temp.columns]
##
df_stock = fanou_fanmei(df_temp)
df_stock['排序'] = 2
df_stock.loc[df_stock['site'] == '美国', '排序'] = 1
df_stock['a_col'] = df_stock['inv_age_0_to_90_days']+df_stock['inv_age_91_to_180_days']+df_stock['inv_age_181_to_270_days']+df_stock['inv_age_271_to_365_days']+df_stock['inv_age_365_plus_days']
df_stock = df_stock.sort_values(['排序', 'available','a_col'], ascending=[True, False,False])
##
# 临时
df_listing = get_listing(site='非泛欧')
##
cols = ['available', 'available0','inv_age_0_to_90_days', 'inv_age_91_to_180_days', 'inv_age_181_to_270_days', 'inv_age_271_to_365_days', 'inv_age_365_plus_days']
df_listing[cols] = df_listing[cols].apply(pd.to_numeric, errors='coerce').fillna(0)
df_listing['age_stock'] = df_listing['inv_age_0_to_90_days']+df_listing['inv_age_91_to_180_days']+df_listing['inv_age_181_to_270_days']+df_listing['inv_age_271_to_365_days']+df_listing['inv_age_365_plus_days']
print(df_listing['available'].sum(),df_listing['available0'].sum(),df_listing['age_stock'].sum())
##
df_listing = get_linelist(df_listing)
df_listing = get_4_difference(df_listing, type1='FBA')
df_listing = shuilv(df_listing, fb_type='FBA')
##
df_jie_kou = freight_interface_fu(df_listing, table_name='freight_interface_amazon')
print('FBM运费数据获取完成.')
df_listing = df_listing.merge(df_jie_kou, on=['站点', 'sku'], how='left')
df_listing = df_listing.rename(columns={'运费': 'fbm_fees'})

df_listing = get_price(df_listing)
print('价格计算完成.')
##
# 库存数据处理
df_stock = df_listing[['account_id', 'asin', 'seller_sku', '站点', 'site2', 'available', 'inv_age_0_to_90_days',
                       'inv_age_91_to_180_days',
                       'inv_age_181_to_270_days', 'inv_age_271_to_365_days', 'inv_age_365_plus_days']]
df_stock = fanou_fanmei(df_stock)
df_stock['排序'] = 2
df_stock['a_col'] = df_stock['inv_age_0_to_90_days']+df_stock['inv_age_91_to_180_days']+df_stock['inv_age_181_to_270_days']+df_stock['inv_age_271_to_365_days']+df_stock['inv_age_365_plus_days']
df_stock = df_stock.sort_values(['排序', 'available','a_col'], ascending=[True, False,False])
df_stock = df_stock.drop_duplicates(['标识', 'seller_sku'], 'first')
df_stock.drop(['排序', '站点','a_col'], axis=1, inplace=True)
cols = ['available', 'inv_age_0_to_90_days', 'inv_age_91_to_180_days', 'inv_age_181_to_270_days', 'inv_age_271_to_365_days', 'inv_age_365_plus_days']
df_stock[cols] = df_stock[cols].apply(pd.to_numeric, errors='coerce').fillna(0)
df_stock_asin = df_stock.groupby(['asin', 'site2']).agg({'available':'sum', 'inv_age_0_to_90_days':'sum', 'inv_age_91_to_180_days':'sum',
'inv_age_181_to_270_days':'sum', 'inv_age_271_to_365_days':'sum', 'inv_age_365_plus_days':'sum'}).reset_index()
##
df_stock_asin.to_excel('df_stock_asin_2.xlsx',index=0)
##
def write_to_sql(df):
    """
    将中间表数据写入mysql
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = datetime.datetime.now().strftime('%Y%m%d')
    df = df.reset_index(drop=True)
    site_list = df['site2'].unique()
    for i in site_list:
        df_temp = df[df['site2'] == i]
        table_name = f'price_ratio_analysis_{date_today}_{i}'
        conn.to_sql(df_temp, table_name, if_exists='append')
        print(f'{i}价差分析数据存表完成, 表名{table_name}.')
    # sql = f"""
    # delete from {table_name}
    # """
    # conn.execute(sql)
    conn.close()
##
if __name__ == "__main__":
    df_listing_final = dwm_data(get_listing())
    print(df_listing_final.info())
    print('存表...')
    date_today = datetime.datetime.today().strftime('%Y%m%d')
    table_name = f'price_ratio_analysis_{date_today}'
    write_to_sql(df_listing_final, table_name)
    # df_listing_final.to_csv( 'df_listing_final.csv', index=0)

