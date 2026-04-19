"""
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
    # date_today = datetime.date.today()
    # # date_today = (datetime.date.today() - datetime.timedelta(days=1))
    # today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    # # today = '20240607'
    # is_high_season = int((today >= '20231015') & (today <= '20240114'))
    # # is_high_season = 1
    #
    # # 一级产品线
    # sql_line = f"""
    #         select a.sku sku, b.path_name as `产品线路线` from yb_datacenter.yb_product a
    #         left join yb_datacenter.yb_product_linelist b
    #         on toInt32(a.product_linelist_id) = toInt32(b.id)
    #     """
    # conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df_line = conn_ck.ck_select_to_df(sql_line)
    # # df_line = read_sql_ck(sql_line, client)
    # df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    # print('一级产品线提取完成')
    # today_y = datetime.date.today()- datetime.timedelta(days=1)
    # today_y = f'{today_y.year:04d}{today_y.month:02d}{today_y.day:02d}'
    # # # 获取fba库龄表信息
    # sql = f"""
    #     SELECT `标识`, seller_sku, max(`库龄天数`) as `最大库龄天数`
    #     FROM fba_inventory.amazon_fba_inventory_age_self_calculated_{today_y}3
    #     GROUP BY `标识`, seller_sku
    # """
    # # df_age = ck_client.ck_select_to_df(sql)
    # df_age = conn_ck.ck_select_to_df(sql)
    # # 美国
    # sql1 = f"""
    # select a.*,
    #     ceil(fba_to_other_fees/(1-0.15-0.04-0.0747),1)-0.01 as `lowest_price`,
    #     ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.04-0.013)/rate,1)-0.01 as  `ebay_price_sku`,
    #     GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.04-0.013)/rate,1)-0.01,toFloat64(lowest_price)) as  `ebay_price_seller_sku`
    # from
    #     (
    #         select  A.account_id account_id,
    #                 A.site site,
    #                 B.short_name short_name,
    #                 A.seller_sku seller_sku,
    #                 A.Current_price as `当前售价`,
    #                 A.sku sku,
    #                 A.group_name group_name,
    #                 A.area area,
    #                 A.product_cost as `product_cost`,
    #                 A.Current_gross_rate as `当前售价毛利润率`,
    #                 A.Current_net_rate as  `当前售价净利润率`,
    #                 m.afn_fulfillable_quantity as `当前库存数量`,
    #                 m.afn_fulfillable_quantity*product_cost as `当前库存金额`,
    #                 A.Average_daily_sales as `日均销量`,
    #                 A.available_days_in_stock as `在库预计可售天数`,
    #                 A.adjustment_priority as `销售状态`,
    #                 A.Destruction_price as `销毁价格本币`,
    #                 A.fba_fees as `fba_fees`,
    #                 A.first_trip_fee_rmb as `first_trip_fee_rmb`,
    #                 A.rate as `rate`,
    #                 A.FBA_difference as `差值`,
    #                 A.Current_net_rate*A.Current_price*A.rate AS `relese_monry_rmb`,
    #                 C.weight,
    #                 C.weight/28.34 AS `盎司`,
    #                 case
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then '小号标准尺寸： 不超过 6 盎司'
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then '小号标准尺寸： 6 至 12 盎司（不含 6 盎司）'
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then '小号标准尺寸： 12 至 16 盎司（不含 12 盎司）'
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then '大号标准尺寸： 不超过 6 盎司'
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then '大号标准尺寸： 6 至 12 盎司（不含 6 盎司）'
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then '大号标准尺寸： 12 至 16 盎司（不含 12 盎司）'
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then '大号标准尺寸： 1 至 2 磅（不含 1 磅）'
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then '大号标准尺寸： 2 至 20 磅（不含 2 磅）'
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then '小号大件： 不超过 2 磅'
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then '小号大件： 2 至 30 磅（不含 2 磅）'
    #                     when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then '小号大件： 超过 30 磅'
    #                     when  C.size_segment like '%中号大件%' then '中号大件'
    #                     when  C.size_segment like '%大号大件%' then '大号大件'
    #                     when  C.size_segment like '%特殊大件%' then '特殊大件'
    #                 else null end as `尺寸`,
    #                 case
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.15
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 7.8
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.35
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 8.2
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.5
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.5
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 9.5
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 16
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 16
    #                     when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then 32.88
    #                     when  C.size_segment like '%中号大件%' then 25.25
    #                     when  C.size_segment like '%大号大件%' then 118.8
    #                     when  C.size_segment like '%特殊大件%' then 189.19
    #                 else null end as `初始收费`,
    #                 case
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 0
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 0
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 0
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 0
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 0
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 0
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 0
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 0.62
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 0
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 0.62
    #                     when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then 0.62
    #                     when  C.size_segment like '%中号大件%' then 0.62
    #                     when  C.size_segment like '%大号大件%' then 1.16
    #                     when  C.size_segment like '%特殊大件%' then 1.21
    #                 else null end as `初续重收费`,
    #                 case
    #                     when C.size_segment like '%小号标准尺寸%' then 0.2
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  0.00001 and 2 then 0.3
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 0.5
    #                     when C.size_segment  like '%小号大件%' then 1
    #                     when  C.size_segment like '%中号大件%' then 2.5
    #                     when  C.size_segment like '%大号大件%' then 2.5
    #                     when  C.size_segment like '%特殊大件%' then 2.5
    #                 else null end as `旺季增长费用`,
    #                 case
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.15 + 0.2 * {is_high_season}
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 7.8 + 0.2 * {is_high_season}
    #                     when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25 + 0.2 * {is_high_season}
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.35 + 0.3 * {is_high_season}
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 8.2 + 0.3 * {is_high_season}
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.5 + 0.3 * {is_high_season}
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.5 + 0.3 * {is_high_season}
    #                     when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 9.5+(C.weight/453.59-2)*0.62 + 0.5 * {is_high_season}
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 16 + 1 * {is_high_season}
    #                     when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 16+(C.weight/453.59-2)*0.62 + 1 * {is_high_season}
    #                     when  C.size_segment like '%小号大件%' and C.weight/453.59 > 30 then 32.88+(C.weight/453.59-2)*0.62 + 1 * {is_high_season}
    #                     when  C.size_segment like '%中号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 25.25 + 2.5 * {is_high_season}
    #                     when  C.size_segment like '%中号大件%' and C.weight/453.59 > 2 then 25.25+(C.weight/453.59-2)*0.62 + 2.5 * {is_high_season}
    #                     when  C.size_segment like '%大号大件%' and C.weight/453.59 BETWEEN 0 and 90 then 118.8 + 2.5 * {is_high_season}
    #                     when  C.size_segment like '%大号大件%' and C.weight/453.59 > 90 then 118.8+(C.weight/453.59-90)*1.16 + 2.5 * {is_high_season}
    #                     when  C.size_segment like '%特殊大件%' and C.weight/453.59 BETWEEN 0 and 90 then 189.19 + 2.5 * {is_high_season}
    #                     when  C.size_segment like '%特殊大件%' and C.weight/453.59 > 90 then 189.19+(C.weight/453.59-90)*1.21 + 2.5 * {is_high_season}
    #                 else null end as `fba_to_other_fees`,
    #                 C.size_segment
    #                 from support_document.fba_clear_seller_sku_{today} as A
    #                 LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
    #                 LEFT JOIN  yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
    #                 left join yibai_fba.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
    #                 where available_stock>0  and A.site='美国'
    #     ) a
    # """
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df1 = conn_mx.ck_select_to_df(sql1)
    # # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    # #                      db_name='support_document')
    # # df1 = ck_client.ck_select_to_df(sql1)
    #
    # # 匹配fba库龄信息
    # df1 = df1.merge(df_line, on='sku', how='left')
    # df1 = df1.rename(columns={'site': '站点'})
    # df1 = fanou_fanmei(df1)
    # df1 = pd.merge(df1, df_age, how='left', on=['标识', 'seller_sku'])
    # df1.drop('标识', axis=1, inplace=True)
    # print(df1.info())
    # print('美国计算完成！')
    #
    # # 英国德国
    # sql2 = f"""
    # select
    #     a.*,
    #     ceil(fba_to_other_fees/(1-0.15-0.04-0.0747-0.167),1)-0.01 as "lowest_price",
    #     ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.04-0.013-0.167)/rate,1)-0.01 as  "ebay_price_sku",
    #     GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.04-0.013-0.167)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"
    # from (
    #     select
    #         A.account_id account_id,
    #         A.site site,
    #         B.short_name short_name,
    #         A.seller_sku seller_sku,
    #         A.Current_price as "当前售价",
    #         A.sku sku,
    #         A.group_name group_name,
    #         A.area area,
    #         A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
    #         m.afn_fulfillable_quantity as "当前库存数量",   m.afn_fulfillable_quantity*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
    #         A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",
    #         A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",
    #         A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司",
    #         case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'
    #                 when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'
    #                 when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
    #                 when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
    #                 when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                 when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克'
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
    #                 when C.size_segment like '%标准大件%' and C.weight > 29760 then '标准大件 > 29.76 千克'
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
    #                 when C.size_segment like '%大号大件%' and C.weight > 31500 then '大号大件：> 31.5 千克'
    #                 else null end as "尺寸",
    #         case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then 3.35
    #                 when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then 3.37
    #                 when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then 3.39
    #                 when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then 3.41
    #                 when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then 3.43
    #                 when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then 3.43
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then 3.71
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then 4.18
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then 4.91
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then 5.74
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then 6.56
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then 7.9
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then 9.39
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then 9.8
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then 10.2
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then 10.81
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then 11.21
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then 11.61
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then 12.02
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then 12.42
    #                 when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then 12.83
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then 9.47
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then 11.18
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then 11.7
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then 12.2
    #                 when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then 12.4
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then 11
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then 12.5
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then 13.12
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then 14.87
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then 14.99
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then 16.48
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then 16.88
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then 17.2
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then 18.12
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then 19.02
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then 20.22
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then 21.84
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then 25.71
    #                 when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then 29.75
    #                 when C.size_segment like '%标准大件%' and C.weight > 29760 then 0.99*(C.weight/1000)
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then 16.31
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then 20.72
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then 23
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then 24.2
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then 28
    #                 when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then 33
    #                 when C.size_segment like '%大号大件%' and C.weight > 31500 then 0.99*(C.weight/1000)
    #                 else null end as "初始收费",
    #         0 as "初续重收费",
    #         `初始收费`+`初续重收费` as "fba_to_other_fees",
    #         'uk' as "price_country",
    #         C.size_segment
    #     from support_document.fba_clear_seller_sku_{today}  as A
    #     LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
    #     LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
    #     left join yibai_fba.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
    #     where available_stock>0  and A.site='英国'
    # ) a
    #
    # """
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df2 = conn_mx.ck_select_to_df(sql2)
    # df2 = df2.merge(df_line, on='sku', how='left')
    # df2 = df2.rename(columns={'site': '站点'})
    # df2 = fanou_fanmei(df2)
    # df2 = pd.merge(df2, df_age, how='left', on=['标识', 'seller_sku'])
    # df2.drop(['标识','price_country'], axis=1, inplace=True)
    # print(df2.info())
    # print('英国计算完成')
    #
    # sql3 = f"""
    # select
    #     a.*,   ceil(fba_to_other_fees/(1-0.15-0.04-0.0747-0.1597),1)-0.01 as "lowest_price",
    #     ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.04-0.013-0.1597)/rate,1)-0.01 as  "ebay_price_sku",
    #     GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.04-0.013-0.1597)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"
    # from (
    #       select
    #           A.account_id account_id,
    #           A.site site,
    #           B.short_name short_name,
    #           A.seller_sku seller_sku,
    #           A.Current_price as "当前售价",
    #           A.sku sku,
    #           A.group_name group_name,
    #           A.area area,
    #           A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
    #           m.afn_fulfillable_quantity as "当前库存数量",      m.afn_fulfillable_quantity*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
    #           A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",
    #           A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",
    #           A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司",
    #                 case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'
    #                         when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'
    #                         when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
    #                         when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
    #                         when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克'
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
    #                         when C.size_segment like '%标准大件%' and C.weight > 29760 then '标准大件 > 29.76 千克'
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
    #                         when C.size_segment like '%大号大件%' and C.weight > 31500 then '大号大件：> 31.5 千克'
    #                         else null end as "尺寸",
    #                 case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then 5.09
    #                         when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then 5.29
    #                         when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then 5.45
    #                         when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then 5.55
    #                         when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then 5.87
    #                         when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then 5.87
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then 6.19
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then 6.76
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then 7.58
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then 8.45
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then 9.53
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then 10.55
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then 11.59
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then 12.02
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then 12.86
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then 13.6
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then 14.63
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then 15.37
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then 16.4
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then 17.44
    #                         when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then 18.98
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then 12.74
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then 13.1
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then 13.47
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then 13.83
    #                         when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then 14.49
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then 15.15
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then 15.76
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then 16.37
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then 16.98
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then 17.62
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then 18.24
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then 18.85
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then 19.46
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then 20.07
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then 20.68
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then 23.74
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then 26.81
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then 29.87
    #                         when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then 33.33
    #                         when C.size_segment like '%标准大件%' and C.weight > 29760 then 1.11*(C.weight/1000)
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then 30.39
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then 33.17
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then 35.95
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then 38.72
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then 41.5
    #                         when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then 45.78
    #                         when C.size_segment like '%大号大件%' and C.weight > 31500 then 1.45*(C.weight/1000)
    #                         else null end as "初始收费",
    #                 0 as "初续重收费",
    #                 `初始收费`+`初续重收费` as "fba_to_other_fees",
    #                 'DE' as "price_country",
    #                 C.size_segment
    #         from support_document.fba_clear_seller_sku_{today} A
    #         LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
    #         LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
    #         left join yibai_fba.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
    #         where available_stock>0  and A.site='德国'
    #     ) a
    #
    # """
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df3 = conn_mx.ck_select_to_df(sql3)
    # df3 = df3.merge(df_line, on='sku', how='left')
    # # 匹配fba库龄信息
    # df3 = df3.rename(columns={'site': '站点'})
    # df3 = fanou_fanmei(df3)
    # df3 = pd.merge(df3, df_age, how='left', on=['标识', 'seller_sku'])
    # df3.drop(['标识','price_country'], axis=1, inplace=True)
    # print(df3.info())
    # print('德国计算完成')
    # df = pd.concat([df1, df2, df3])

    today_y = datetime.date.today() - datetime.timedelta(days=1)
    today_y = f'{today_y.year:04d}{today_y.month:02d}{today_y.day:02d}'
    sql = f"""
    SELECT a.*, b.`最大库龄天数` `最大库龄天数`
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
        LEFT JOIN yibai_fba.fba_fanmei c
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
            FROM yibai_sale_center_system_sync.yibai_system_account
            WHERE platform_code = 'AMAZON'
        ) d
        ON a.account_id = d.erp_id
    ) a
    LEFT JOIN (
        SELECT `标识`, seller_sku, max(`库龄天数`) as `最大库龄天数`
        FROM yibai_price_fba.yibai_amazon_fba_inventory_age_self_calculated_{today_y}3
        GROUP BY `标识`, seller_sku
    ) b
    ON a.`标识` = b.`标识` and a.seller_sku = b.seller_sku
    WHERE a.`站点` IN ('美国', '英国', '德国', '加拿大')
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(f"FBA链接获取完成，共{len(df)}条")

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
    conn.to_sql(df, 'fba_stock_to_center', if_exists='replace')
    return df
##

def main():
    # 查表是否存在
    date_today = time.strftime('%Y%m%d')
    table_now = f"fba_clear_seller_sku_{date_today}"
    # table_now = f"fba_clear_seller_sku_20240624"
    sql_table_monitor = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'fba_clear_seller_sku%'
        and table <= \'{table_now}\'
        order by table desc
        limit 7
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_table_monitor = conn_mx.ck_select_to_df(sql_table_monitor)
    if table_now in df_table_monitor['table'].unique():
        print('今日调价数据表已生成.')
        # 获取fba链接信息
        df = get_fba_info()
        # df = df.sample(10)

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
        print('今日调价数据表还未生成，退出程序！')
##
def center_interface(group):
    """
    接口调用函数
    """
    url = 'http://salescenter.yibainetwork.com:91/apis/ebay/ebay_syncstock_fba_amazon_api/push_fba_sku_max_sotck_age?org_code=org_00001'
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
    data_post = json.dumps({"org_code":"org_00001", "data": data_temp})

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
# df = get_fba_info()
# ##
# df = df.sort_values(by=['seller_sku', 'account_id', 'stock'], ascending=[True, True, True]).drop_duplicates(subset=['seller_sku','account_id'])
# ##
# df_temp = df.sample(10000)
# # threadPool = ThreadPoolExecutor(max_workers=2)
# # thread_list = []
#
# ##
# for key, group in tqdm(df_temp.groupby(['index'])):
#     group.drop('index', axis=1, inplace=True)
#     group = group.reset_index(drop=True)
#     # future = threadPool.submit(center_interface, group, key)
#     # thread_list.append(future)
#     center_interface(group)
# ##
# temp = df_temp[df_temp.duplicated(subset=['seller_sku','account_id'], keep=False)].sort_values(by='seller_sku')
##

# # url = 'http://192.168.86.142:100/apis/ebay/ebay_syncstock_fba_amazon_api/push_fba_sku_max_sotck_age?org_code=org_00001'
# # url = 'http://192.168.86.142:100/apis/ebay/ebay_listing_info_api/push_fba_sku_max_sotck_age?org_code=org_00001'
# url = 'http://192.168.86.141:100/apis/ebay/ebay_syncstock_fba_amazon_api/push_fba_sku_max_sotck_age?org_code=org_00001'
# # url = 'http://salescenter.yibainetwork.com:91/apis/ebay/ebay_syncstock_fba_amazon_api/push_fba_sku_max_sotck_age?org_code=org_00001'
#
# data_temp = [{"seller_sku":"ceyo8b13efHY-QT00061-FBA","account_id":"4","sku":"QT00061","stock":1,"stock_sellable_days":1,"sale_status":"0","max_stock_age":1},
#              {"seller_sku":"UK-YB01L21000001-O Filter-FBA","account_id":"7","sku":"CW00699","stock":10,"stock_sellable_days":2,"sale_status":1,"max_stock_age":12}]
#
# # data_post = {'org_token':'f2010d0a24910077a388ce64c717da6c','org_code':'org_00001', 'platform':'EB', 'test':'1', 'data': data_temp}
#
# data_post = json.dumps({"org_code":"org_00001", "data": data_temp})
# # data_post = {"platform_code":"EB", "data": data_temp, "debug":"1"}
#
# res = requests.post(url, data=data_post, headers={'Content-Type':'application/json'}).json()
#
# print(res)
##


if __name__ == "__main__":
    # main()
    # get_fba_info()
    df = get_fba_info()
    # print(df.info())
    # temp()