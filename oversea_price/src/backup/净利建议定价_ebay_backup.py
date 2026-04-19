"""
1、20231110，新增fba库龄数据信息
"""
##
import warnings
from dateutil.relativedelta import relativedelta
from utils import utils
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, \
    is_df_exist, read_df, make_path, get_path
import datetime,time
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
import pandas as pd
import numpy as np
from pulic_func.base_api.adjust_price_function_amazon import fanou_fanmei
import requests
warnings.filterwarnings('ignore')

def main():
    """
    Ebay “FBA正负利润加快动销最新库存”的数据 还需要出一份“美英x德净利建议定价”
    ebay举例：
    0.0747 销毁价综合系数
    10% 是平台佣金 （前三个月免费）后面10%收取
    0.16 是毛净利差值  新平台 （盲拍）
    0.05 定价综合系数
    """
    utils.program_name = '净利建议定价_ebay'
    make_path()
    cur_path, root_path = get_path()
    # date_today = datetime.date.today()
    date_today = (datetime.date.today() - datetime.timedelta(days=3))
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    is_high_season = int((today >= '20231015') & (today <= '20240114'))
    # is_high_season = 1

    # 一级产品线
    sql_line = f"""
            select a.sku sku, b.path_name as `产品线路线` 
            from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    print('一级产品线提取完成')
    today_y = datetime.date.today()- datetime.timedelta(days=1)
    today_y = f'{today_y.year:04d}{today_y.month:02d}{today_y.day:02d}'
    # # 获取fba库龄表信息
    sql = f"""
        SELECT `标识`, seller_sku, max(`库龄天数`) as `最大库龄天数`
        FROM fba_inventory.amazon_fba_inventory_age_self_calculated_{today_y}3
        GROUP BY `标识`, seller_sku
    """
    df_age = conn_ck.ck_select_to_df(sql)


    # 加拿大
    sql1 = f"""
        select a.*,
            ceil(fba_to_other_fees/(1-0.15-0.03-0.0747),1)-0.01 as `lowest_price`,
            ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013)/rate,1)-0.01 as  `ebay_price_sku`,
            GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013)/rate,1)-0.01,toFloat64(lowest_price)) as  `ebay_price_seller_sku`
        from
            (
                select  A.account_id account_id,
                        A.site site,
                        B.short_name short_name,
                        A.seller_sku seller_sku,
                        A.Current_price as `当前售价`,
                        A.sku sku,
                        A.asin asin,
                        A.group_name group_name,
                        A.area area,
                        A.product_cost as `product_cost`,
                        A.Current_gross_rate as `当前售价毛利润率`,
                        A.Current_net_rate as  `当前售价净利润率`,
                        m.afn_fulfillable_quantity as `当前库存数量`,
                        m.afn_fulfillable_quantity*product_cost as `当前库存金额`,
                        A.Average_daily_sales as `日均销量`,
                        A.available_days_in_stock as `在库预计可售天数`,
                        A.adjustment_priority as `销售状态`,
                        A.Destruction_price as `销毁价格本币`,
                        A.fba_fees as `fba_fees`,
                        A.first_trip_fee_rmb as `first_trip_fee_rmb`,
                        A.rate as `rate`,
                        A.FBA_difference as `差值`,
                        A.Current_net_rate*A.Current_price*A.rate AS `relese_monry_rmb`,
                        C.weight,
                        C.weight/28.34 AS `盎司`,
                        case
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  0 and 100 then '标准尺寸：0~100g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  100 and 200 then '标准尺寸：101~200g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  200 and 300 then '标准尺寸：201~300g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  300 and 400 then '标准尺寸：301~400g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  400 and 500 then '标准尺寸：401~500g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  500 and 600 then '标准尺寸：501~600g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  600 and 700 then '标准尺寸：601~700g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  700 and 800 then '标准尺寸：701~800g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  800 and 900 then '标准尺寸：801~900g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  900 and 1000 then '标准尺寸：901~1000g'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  1000 and 10000 then '标准尺寸：不超过10kg'
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight > 10000 then '标准尺寸：超过10kg'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  0 and 100 then '大件：0~100g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  100 and 200 then '大件：101~200g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  200 and 300 then '大件：201~300g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  300 and 400 then '大件：301~400g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  400 and 500 then '大件：401~500g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  500 and 600 then '大件：501~600g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  600 and 700 then '大件：601~700g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  700 and 800 then '大件：701~800g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  800 and 900 then '大件：801~900g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  900 and 1000 then '大件：901~1000g'
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  1000 and 10000 then '大件：不超过10kg'
                            when C.size_segment like '%%大件%%' and C.weight > 10000 then '大件：超过10kg'     
                        else null end as "尺寸",       
                        case  
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  0 and 100 then 4.93
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  100 and 200 then 5.33
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  200 and 300 then 5.83
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  300 and 400 then 6.23
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  400 and 500 then 6.68
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  500 and 600 then 6.98
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  600 and 700 then 7.38
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  700 and 800 then 7.58
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  800 and 900 then 7.73
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  900 and 1000 then 7.88
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  1000 and 10000 then 7.88
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight > 10000 then 7.88
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  0 and 100 then 4.93
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  100 and 200 then 5.33
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  200 and 300 then 5.83
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  300 and 400 then 6.23
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  400 and 500 then 6.68
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  500 and 600 then 6.98
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  600 and 700 then 7.38
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  700 and 800 then 7.58
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  800 and 900 then 7.73
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  900 and 1000 then 7.88
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  1000 and 10000 then 7.88
                            when C.size_segment like '%%大件%%' and C.weight > 10000 then 7.88
                        else 0 end as "初始收费",        
                        case  
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight BETWEEN  1000 and 10000 then 1.05*(C.weight-1000)/500
                            when C.size_segment in ('信封装商品', '标准尺寸商品') and C.weight > 10000 then 0.48*(C.weight-10000)/500
                            when C.size_segment like '%%大件%%' and C.weight BETWEEN  1000 and 10000 then 1.05*(C.weight-1000)/500
                            when C.size_segment like '%%大件%%' and C.weight > 10000 then 0.48*(C.weight-10000)/500
                        else 0 end as "初续重收费", 
                        case  
                            when C.size_segment like '%%小号标准尺寸%%' then 0
                            when C.size_segment  like '%%大号标准尺寸%%' then 0
                            when  C.size_segment like '%%大号大件%%' then 0
                            when  C.size_segment like '%%超大件%%' then 0
                        else 0 end as "旺季增长费用", 
                        case  
                            when C.size_segment in ('信封装商品', '标准尺寸商品') then 3.64 + `初始收费` + `初续重收费`
                            when C.size_segment like '%%大件%%' then 12 + `初始收费` + `初续重收费`
                        else 0 end as `fba_to_other_fees`,
                        C.size_segment
                        from support_document.fba_clear_seller_sku_{today} as A
                        LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
                        LEFT JOIN  yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                        left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                        where available_stock>0  and A.site='加拿大' and A.biaoshi not like '%北美%'
            ) a
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    df0 = conn_ck.ck_select_to_df(sql1)
    # 匹配fba库龄信息
    df0 = df0.merge(df_line, on='sku', how='left')
    df0 = df0.rename(columns={'site': '站点'})
    df0 = fanou_fanmei(df0)
    df0 = pd.merge(df0, df_age, how='left', on=['标识', 'seller_sku'])
    df0.drop(['旺季增长费用'], axis=1, inplace=True)
    df0 = df0.rename(columns={'C.weight':'重量(g)', 'fba_fees':'fba尾程','first_trip_fee_rmb':'头程','fba_to_other_fees':'多渠道尾程运费'})
    save_df(df0, '加拿大', file_type='xlsx')
    print('加拿大计算完成！')

    # 美国
    sql1 = f"""
        select a.*,
            ceil(fba_to_other_fees/(1-0.15-0.03-0.0747),1)-0.01 as `lowest_price`,
            ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013)/rate,1)-0.01 as  `ebay_price_sku`,
            GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013)/rate,1)-0.01,toFloat64(lowest_price)) as  `ebay_price_seller_sku`
        from
            (
                select  A.account_id account_id,
                        A.site site,
                        B.short_name short_name,
                        A.seller_sku seller_sku,
                        A.Current_price as `当前售价`,
                        A.sku sku,
                        A.asin asin,
                        A.group_name group_name,
                        A.area area,
                        A.product_cost as `product_cost`,
                        A.Current_gross_rate as `当前售价毛利润率`,
                        A.Current_net_rate as  `当前售价净利润率`,
                        m.afn_fulfillable_quantity as `当前库存数量`,
                        m.afn_fulfillable_quantity*product_cost as `当前库存金额`,
                        A.Average_daily_sales as `日均销量`,
                        A.available_days_in_stock as `在库预计可售天数`,
                        A.adjustment_priority as `销售状态`,
                        A.Destruction_price as `销毁价格本币`,
                        A.fba_fees as `fba_fees`,
                        A.first_trip_fee_rmb as `first_trip_fee_rmb`,
                        A.rate as `rate`,
                        A.FBA_difference as `差值`,
                        A.Current_net_rate*A.Current_price*A.rate AS `relese_monry_rmb`,
                        C.weight,
                        C.weight/28.34 AS `盎司`,
                        case
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then '小号标准尺寸： 不超过 4 盎司'
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then '小号标准尺寸： 4 至 8 盎司（不含 4 盎司）'      
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then '小号标准尺寸： 8 至 12 盎司（不含 8 盎司）'      
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16 then '小号标准尺寸： 12 至 16 盎司（不含 12 盎司）'      
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then '大号标准尺寸： 不超过 4 盎司'  
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then '大号标准尺寸： 4 至 8 盎司（不含 4 盎司）'     
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then '大号标准尺寸： 8 至 12 盎司（不含 8 盎司）'      
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16.005 then '大号标准尺寸： 12 至 16 盎司（不含 12 盎司）'      
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  1.00001 and 2 then '大号标准尺寸： 1 至 2 磅（不含 1 磅）' 
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  2.00001 and 3 then '大号标准尺寸： 2 至 3 磅（不含 2 磅）'      
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  3.00001 and 20 then '大号标准尺寸： 3 至 20 磅（不含 3 磅）'
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 BETWEEN  0 and 30 then '大号大件： 不超过 30 磅'
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 > 30.00001 then '大号大件：超过 30 磅'      
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 0 and 50 then '超大号：不超过 50 磅'      
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 50 and 70 then '超大号： 50 至 70 磅（不含 50 磅）'
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 70 and 150 then '超大号： 70 至 150 磅（不含 70 磅）'      
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 > 150.00001 then '超大号：150 磅以上（不含 150 磅）'      
                        else null end as "尺寸",       
                        case  
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then 6.99
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then 7.15
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then 7.8
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then 7.2
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then 7.35
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then 8.2
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16.005 then 8.5
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.78
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  2.00001 and 3 then 10.81
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  3.00001 and 20 then 10.81
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 BETWEEN  0 and 30 then 16.32
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 > 30.00001 then 35.36 
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 0 and 50 then 28.5      
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 50 and 70 then 55.5
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 70 and 150 then 75.00      
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 > 150.00001 then 225.00    
                        else null end as "初始收费",        
                        case  
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then 0
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then 0
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then 0
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 0
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then 0
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then 0
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then 0
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16.005 then 0
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 0
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  2.00001 and 3 then 0
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  3.00001 and 20 then 0.65
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 BETWEEN  0 and 30 then 0.68
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 > 30.00001 then 0.68
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 0 and 50 then 0.55      
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 50 and 70 then 0.90
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 70 and 150 then 1.25     
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 > 150.00001 then 1.50  
                        else null end as "初续重收费", 
                        case  
                            when C.size_segment like '%%小号标准尺寸%%' then 0
                            when C.size_segment  like '%%大号标准尺寸%%' then 0
                            when  C.size_segment like '%%大号大件%%' then 0
                            when  C.size_segment like '%%超大件%%' then 0
                        else null end as "旺季增长费用", 
                        case  
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then 6.99
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then 7.15
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then 7.8
                            when C.size_segment like '%%小号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  0 and 4 then 7.20
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  4 and 8 then 7.35
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  8.00001 and 12 then 8.2
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/28.34 BETWEEN  12.00001 and 16.005 then 8.5
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.78
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  2.00001 and 3 then 10.81
                            when C.size_segment  like '%%大号标准尺寸%%' and C.weight/453.59 BETWEEN  3.00001 and 20 then 10.81+(C.weight/453.59-3)*0.65
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 BETWEEN  0 and 30 then 16.32 + (C.weight/453.59-2)*0.68
                            when C.size_segment  like '%%大号大件%%' and C.weight/453.59 > 30.00001 then 35.36 + (C.weight/453.59-30)*0.68
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 0 and 50 then 28.5 + (C.weight/453.59-1)*0.55      
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 50 and 70 then 55.5 + (C.weight/453.59-51)*0.90
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 BETWEEN 70 and 150 then 75.00 + (C.weight/453.59-71)*1.25     
                            when  C.size_segment like '%%超大件%%' and C.weight/453.59 > 150.00001 then 225.00 + (C.weight/453.59-151)*1.5
                        else null end as `fba_to_other_fees`,
                        C.size_segment
                        from support_document.fba_clear_seller_sku_{today} as A
                        LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
                        LEFT JOIN  yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                        left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                        where available_stock>0  and A.site='美国'
            ) a
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    df1 = conn_ck.ck_select_to_df(sql1)
    # 匹配fba库龄信息
    df1 = df1.merge(df_line, on='sku', how='left')
    df1 = df1.rename(columns={'site': '站点'})
    df1 = fanou_fanmei(df1)
    df1 = pd.merge(df1, df_age, how='left', on=['标识', 'seller_sku'])
    df1.drop(['旺季增长费用'], axis=1, inplace=True)
    df1 = df1.rename(columns={'C.weight': '重量(g)', 'fba_fees': 'fba尾程', 'first_trip_fee_rmb': '头程',
                              'fba_to_other_fees': '多渠道尾程运费'})
    save_df(df1, '美国', file_type='xlsx')
    print('美国计算完成！')

    # 英国德国
    sql2 = f"""
        select 
            a.*, 
            ceil(fba_to_other_fees/(1-0.15-0.03-0.0747-0.167),1)-0.01 as "lowest_price",           
            ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.167)/rate,1)-0.01 as  "ebay_price_sku",      
            GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.167)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"        
            from 
            (      
                select  	
                    A.account_id account_id,
                    A.site site,
                    B.short_name short_name,
                    A.seller_sku seller_sku,
                    A.Current_price as "当前售价",
                    A.sku sku,
                    A.asin asin,
                    A.group_name group_name,
                    A.area area,      
                    A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",      
                    m.afn_fulfillable_quantity as "当前库存数量",   m.afn_fulfillable_quantity*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",      
                    A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",      
                    A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",      
                    A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司",      
                    case    
                        when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then '加大号信封：不超过 960 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then '小包裹：不超过 150 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then '小包裹：不超过 400 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then '小包裹：不超过 900 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then '小包裹：不超过 1.4 千克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then '小包裹：不超过 1.9 千克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then '小包裹：不超过 3.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then '标准大件 > 29.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then '大号大件：> 31.5 千克'
                    else null end as "尺寸",
                    case
                        when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 3.35
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 3.37
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 3.39
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 3.41
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 3.60
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 3.64
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 3.89
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 4.39
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 5.06
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 5.74
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 6.56
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 9.39
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 4.58
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 4.78
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 5.38
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 6.15
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 6.96
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 8.36
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 9.59
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 9.8
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 10.2
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 10.81
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 11.21
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 11.61
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 12.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 12.42
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 12.83
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 10.32
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 11.33
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 11.7
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 12.2
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 12.4
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 12.68
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 13.84
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 14.43
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 15.32
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 16.23
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 16.90
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 17.6
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 18.83
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 19.57
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 21.40
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 22.52
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 24.70
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 30.95
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 36.20
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.21*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 19.10
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 23.8
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 26.3
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 31.0
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 36.7
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 42.97
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.36*(C.weight/1000)
                    else null end as "初始收费",            
                    0 as "初续重收费",
                    `初始收费`+`初续重收费` as "fba_to_other_fees",      
                    'uk' as "price_country",  
                    C.size_segment       
                    from support_document.fba_clear_seller_sku_{today}  as A
                    LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku      
                    LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)      
                    left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku      
                    where available_stock>0  and A.site='英国'
            ) a                              
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df2 = conn_ck.ck_select_to_df(sql2)
    df2 = df2.merge(df_line, on='sku', how='left')
    df2 = df2.rename(columns={'site': '站点'})
    df2 = fanou_fanmei(df2)
    df2 = pd.merge(df2, df_age, how='left', on=['标识', 'seller_sku'])
    df2.drop(['price_country'], axis=1, inplace=True)
    df2 = df2.rename(columns={'C.weight': '重量(g)', 'fba_fees': 'fba尾程', 'first_trip_fee_rmb': '头程',
                              'fba_to_other_fees': '多渠道尾程运费'})
    # 剔除英欧互发的链接
    df2 = df2[~df2['标识'].str.contains('欧洲')]
    save_df(df2, '英国', file_type='xlsx')
    print('英国计算完成')

    sql3 = f"""
    select
        a.*,   ceil(fba_to_other_fees/(1-0.15-0.03-0.0747-0.1597),1)-0.01 as "lowest_price",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.1597)/rate,1)-0.01 as  "ebay_price_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.1597)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"    from
        (
          select
              A.account_id account_id,
              A.site site,
              B.short_name short_name,
              A.seller_sku seller_sku,
              A.Current_price as "当前售价",
              A.sku sku,
              A.asin asin,
              A.group_name group_name,
              A.area area,
              A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
              m.afn_fulfillable_quantity as "当前库存数量",      m.afn_fulfillable_quantity*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
              A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",
              A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",
              A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司",
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then '小包裹：不超过 150 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then '小包裹：不超过 400 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then '小包裹：不超过 900 克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then '小包裹：不超过 1.4 千克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then '小包裹：不超过 1.9 千克'
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then '小包裹：不超过 3.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克'
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then '标准大件 > 29.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then '大号大件：> 31.5 千克'
                        else null end as "尺寸",
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 5.21
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 5.29
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 5.45
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 5.55
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 5.87
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 6.07
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 6.19
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 6.76
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.58
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.45
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.53
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 11.59
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 7.03
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 7.30
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.92
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.79
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.87
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 10.89
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 11.93
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 12.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 12.86
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 13.60
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 14.63
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 15.37
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 16.40
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 17.44
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 18.98
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 12.74
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 13.10
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 13.47
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 13.83
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 14.49
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 15.15
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 15.76
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 16.37
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 16.98
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 17.62
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 18.24
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 18.85
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 19.46
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 20.07
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 20.68
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 23.74
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 26.81
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 32.56
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 40.13
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.34*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 30.39
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 33.17
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 35.95
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 44.98
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 52.08
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 61.13
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.94*(C.weight/1000)
                    else null end as "初始收费",
                    0 as "初续重收费",
                    `初始收费`+`初续重收费` as "fba_to_other_fees",
                    'DE' as "price_country",
                    C.size_segment
                    from support_document.fba_clear_seller_sku_{today} A
                    LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
                    LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                    left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                    where available_stock>0  and A.site='德国'
        ) a
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df3 = conn_ck.ck_select_to_df(sql3)
    df3 = df3.merge(df_line, on='sku', how='left')
    # 匹配fba库龄信息
    df3 = df3.rename(columns={'site': '站点'})
    df3 = fanou_fanmei(df3)
    df3 = pd.merge(df3, df_age, how='left', on=['标识', 'seller_sku'])
    df3.drop(['price_country'], axis=1, inplace=True)
    df3 = df3.rename(columns={'C.weight': '重量(g)', 'fba_fees': 'fba尾程', 'first_trip_fee_rmb': '头程',
                              'fba_to_other_fees': '多渠道尾程运费'})
    # 剔除英欧互发的链接
    df3 = df3[~df3['标识'].str.contains('英国')]
    save_df(df3, '德国', file_type='xlsx')

    print('done!')

##
def temp_data():
    # FBA低销淘汰链接，转寄定价+补充账号简称
    df_fba_listing = pd.read_excel('F:\yibai-price-strategy\data\FBA低销淘汰链接-2023-11.xlsx')
    #
    df_fba_listing = df_fba_listing[df_fba_listing['site'].isin(['us','de','uk'])]
    #
    # 获取产品成本、头程运费、多渠道转寄运费（需加工）、账号简称
    account_tuple = tuple(df_fba_listing['account_id'].unique())
    # 美国
    date_today = (datetime.date.today() - datetime.timedelta(days=1))
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    is_high_season = int((today >= '20231015') & (today <= '20240114'))
    sql = f"""
        select 
            a.*   
        from (      
                select  
                    C.account_id account_id,
                    C.site site,
                    B.short_name short_name,
                    C.seller_sku seller_sku,     
                    C.cost as `product_cost`,          
                    m.afn_fulfillable_quantity as `当前库存数量`,      
                    m.afn_fulfillable_quantity*product_cost as `当前库存金额`,          
                    C.first_trip_fee_rmb as `first_trip_fee_rmb`,      
                    C.rate as `rate`,           
                    C.weight,      
                    C.weight/28.34 AS `盎司`,      
                    case  
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then '小号标准尺寸： 不超过 6 盎司'      
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then '小号标准尺寸： 6 至 12 盎司（不含 6 盎司）'      
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then '小号标准尺寸： 12 至 16 盎司（不含 12 盎司）'      
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then '大号标准尺寸： 不超过 6 盎司'      
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then '大号标准尺寸： 6 至 12 盎司（不含 6 盎司）'      
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then '大号标准尺寸： 12 至 16 盎司（不含 12 盎司）'      
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then '大号标准尺寸： 1 至 2 磅（不含 1 磅）'      
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then '大号标准尺寸： 2 至 20 磅（不含 2 磅）'
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then '小号大件： 不超过 2 磅'
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then '小号大件： 2 至 30 磅（不含 2 磅）'      
                        when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then '小号大件： 超过 30 磅'      
                        when  C.size_segment like '%中号大件%' then '中号大件' 
                        when  C.size_segment like '%大号大件%' then '大号大件'      
                        when  C.size_segment like '%特殊大件%' then '特殊大件'      
                    else null end as `尺寸`,       
                    case  
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.15
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 7.8
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.35
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 8.2
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.5
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.5
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 9.5
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 16
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 16
                        when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then 32.88
                        when  C.size_segment like '%中号大件%' then 25.25
                        when  C.size_segment like '%大号大件%' then 118.8
                        when  C.size_segment like '%特殊大件%' then 189.19   
                    else null end as `初始收费`,        
                    case  
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 0
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 0
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 0
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 0
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 0
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 0
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 0
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 0.62
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 0
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 0.62
                        when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then 0.62
                        when  C.size_segment like '%中号大件%' then 0.62
                        when  C.size_segment like '%大号大件%' then 1.16
                        when  C.size_segment like '%特殊大件%' then 1.21
                    else null end as `初续重收费`, 
                    case  
                        when C.size_segment like '%小号标准尺寸%' then 0.2
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  0.00001 and 2 then 0.3
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 0.5
                        when C.size_segment  like '%小号大件%' then 1
                        when  C.size_segment like '%中号大件%' then 2.5
                        when  C.size_segment like '%大号大件%' then 2.5
                        when  C.size_segment like '%特殊大件%' then 2.5
                    else null end as `旺季增长费用`, 
                    case  
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.15 + 0.2 * {is_high_season}
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 7.8 + 0.2 * {is_high_season}
                        when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25 + 0.2 * {is_high_season}
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.35 + 0.3 * {is_high_season}
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 8.2 + 0.3 * {is_high_season}
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.5 + 0.3 * {is_high_season}
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.5 + 0.3 * {is_high_season}
                        when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 9.5+(C.weight/453.59-2)*0.62 + 0.5 * {is_high_season}
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 16 + 1 * {is_high_season}
                        when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 16+(C.weight/453.59-2)*0.62 + 1 * {is_high_season}
                        when  C.size_segment like '%小号大件%' and C.weight/453.59 > 30 then 32.88+(C.weight/453.59-2)*0.62 + 1 * {is_high_season}
                        when  C.size_segment like '%中号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 25.25 + 2.5 * {is_high_season}
                        when  C.size_segment like '%中号大件%' and C.weight/453.59 > 2 then 25.25+(C.weight/453.59-2)*0.62 + 2.5 * {is_high_season}
                        when  C.size_segment like '%大号大件%' and C.weight/453.59 BETWEEN 0 and 90 then 118.8 + 2.5 * {is_high_season}
                        when  C.size_segment like '%大号大件%' and C.weight/453.59 > 90 then 118.8+(C.weight/453.59-90)*1.16 + 2.5 * {is_high_season}
                        when  C.size_segment like '%特殊大件%' and C.weight/453.59 BETWEEN 0 and 90 then 189.19 + 2.5 * {is_high_season}
                        when  C.size_segment like '%特殊大件%' and C.weight/453.59 > 90 then 189.19+(C.weight/453.59-90)*1.21 + 2.5 * {is_high_season}
                    else null end as `fba_to_other_fees`,     
                    C.size_segment       
                from domestic_warehouse_clear.fba_fees as C
                LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(C.account_id)=toUInt64(m.account_id) and C.seller_sku = m.sku      
                LEFT JOIN  yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(C.account_id)=toUInt64(B.id)           
                where C.site='美国' and C.account_id in {account_tuple}
            ) a    
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='domestic_warehouse_clear')
    df_fba_fees_0 = ck_client.ck_select_to_df(sql)
    #
    # 德国、英国
    sql = f"""
        select 
            a.*
        from (      
            select  	
                C.account_id account_id,
                C.site site,
                B.short_name short_name,
                C.seller_sku seller_sku,     
                C.cost as `product_cost`,          
                m.afn_fulfillable_quantity as `当前库存数量`,      
                m.afn_fulfillable_quantity*product_cost as `当前库存金额`,          
                C.first_trip_fee_rmb as `first_trip_fee_rmb`,      
                C.rate as `rate`,           
                C.weight,      
                C.weight/28.34 AS `盎司`,       
                case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'      
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'       
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
                        when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
                        when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'     
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克' 
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'         
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'  
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'      
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'     
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'              
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'  
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克' 
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克' 
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight > 29760 then '标准大件 > 29.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
                        when C.size_segment like '%大号大件%' and C.weight > 31500 then '大号大件：> 31.5 千克'
                        else null end as "尺寸",            
                case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then 3.35     
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then 3.37      
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then 3.39       
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then 3.41       
                        when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then 3.43
                        when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then 3.43   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then 3.71       
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then 4.18      
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then 4.91          
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then 5.74           
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then 6.56            
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then 7.9             
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then 9.39              
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then 9.8               
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then 10.2               
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then 10.81                 
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then 11.21                  
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then 11.61                   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then 12.02                   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then 12.42                     
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then 12.83
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then 9.47     
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then 11.18
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then 11.7
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then 12.2      
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then 12.4                 
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then 11
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then 12.5      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then 13.12      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then 14.87      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then 14.99     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then 16.48     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then 16.88   
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then 17.2     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then 18.12  
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then 19.02     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then 20.22      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then 21.84      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then 25.71     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then 29.75
                        when C.size_segment like '%标准大件%' and C.weight > 29760 then 0.99*(C.weight/1000)
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then 16.31    
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then 20.72      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then 23      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then 24.2      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then 28      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then 33
                        when C.size_segment like '%大号大件%' and C.weight > 31500 then 0.99*(C.weight/1000)
                        else null end as "初始收费",            
                0 as "初续重收费",
                `初始收费`+`初续重收费` as "fba_to_other_fees",        
                C.size_segment       
            from domestic_warehouse_clear.fba_fees  as C
            LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(C.account_id)=toUInt64(m.account_id) and C.seller_sku = m.sku      
            LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(C.account_id)=toUInt64(B.id)           
            where C.site='英国' and C.account_id in {account_tuple}
        ) a                              
    union all           
        select 
            a.*        
        FROM (      
            select       
                C.account_id account_id,
                C.site site,
                B.short_name short_name,
                C.seller_sku seller_sku,     
                C.cost as `product_cost`,          
                m.afn_fulfillable_quantity as `当前库存数量`,      
                m.afn_fulfillable_quantity*product_cost as `当前库存金额`,          
                C.first_trip_fee_rmb as `first_trip_fee_rmb`,      
                C.rate as `rate`,           
                C.weight,      
                C.weight/28.34 AS `盎司`,     
                case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'      
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'       
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
                        when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
                        when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'     
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克' 
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'         
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'  
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'      
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'     
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'              
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'  
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克' 
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克' 
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
                        when C.size_segment like '%标准大件%' and C.weight > 29760 then '标准大件 > 29.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
                        when C.size_segment like '%大号大件%' and C.weight > 31500 then '大号大件：> 31.5 千克'
                        else null end as "尺寸",            
                case    when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then 5.09
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then 5.29      
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then 5.45       
                        when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then 5.55     
                        when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then 5.87
                        when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then 5.87       
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then 6.19        
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then 6.76         
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then 7.58          
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then 8.45
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then 9.53            
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then 10.55             
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then 11.59              
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then 12.02               
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then 12.86               
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then 13.6                 
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then 14.63                  
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then 15.37                
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then 16.4                   
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then 17.44                     
                        when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then 18.98
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then 12.74
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then 13.1
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then 13.47
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then 13.83      
                        when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then 14.49                 
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then 15.15
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then 15.76      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then 16.37      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then 16.98      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then 17.62     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then 18.24     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then 18.85   
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then 19.46     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then 20.07     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then 20.68     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then 23.74      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then 26.81      
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then 29.87     
                        when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then 33.33
                        when C.size_segment like '%标准大件%' and C.weight > 29760 then 1.11*(C.weight/1000)
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then 30.39
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then 33.17      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then 35.95      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then 38.72      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then 41.5      
                        when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then 45.78
                        when C.size_segment like '%大号大件%' and C.weight > 31500 then 1.45*(C.weight/1000)
                        else null end as "初始收费",            
                0 as "初续重收费",            
                `初始收费`+`初续重收费` as "fba_to_other_fees",         
                C.size_segment       
            from domestic_warehouse_clear.fba_fees C
            LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(C.account_id)=toUInt64(m.account_id) and C.seller_sku = m.sku      
            LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(C.account_id)=toUInt64(B.id)            
            where C.site='德国' and C.account_id in {account_tuple}
            ) a 
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='domestic_warehouse_clear')
    df_fba_fees_1 = ck_client.ck_select_to_df(sql)
    #
    df_fba_fees = pd.concat([df_fba_fees_0, df_fba_fees_1])
    df_fba_fees.columns = [i.split('.')[-1] for i in df_fba_fees.columns.to_list()]
    #
    df = pd.merge(df_fba_listing, df_fba_fees, how='left', on=['account_id', 'seller_sku'])
    #
    # 账号简称
    sql = """
        SELECT id as account_id, short_name
        FROM yibai_system_kd_sync.yibai_amazon_account
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yibai_system_kd_sync')
    df_account = ck_client.ck_select_to_df(sql)
    #
    df = pd.merge(df, df_account, how='left', on=['account_id'])
    #

    c1 = df['site_x'] == 'us'
    c2 = df['site_x'] == 'uk'
    c3 = df['site_x'] == 'de'
    df['tax_rate'] = np.select([c1, c2, c3], [0, 0.167, 0.1597])
    df['lowest_price'] = df['fba_to_other_fees']/(1-0.15-0.03-0.0747-df['tax_rate'])
    df['ebay_price_sku'] = (df['product_cost'] + df['fba_to_other_fees']*df['rate'] + df['first_trip_fee_rmb']) / (
                            1-0.15-0.16-0.05-0.03-0.013-df['tax_rate'])
    #
    df['ebay_price_sku'] = np.where(df['ebay_price_sku']<df['lowest_price'], df['lowest_price'], df['ebay_price_sku'])
    #
    df.to_excel('df_ebay.xlsx',index=0)
##
# 临时
def data_to_center():
    sql = """
        SELECT 
            sku, `站点`,`税号`,`FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,
            `FBA空运定价_修正`,`FBA海运定价_修正`,`FBA铁路定价_修正`,`FBA卡航定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`建议物流方式`
        FROM support_document.pricing_ratio_to_center
        LIMIT 10
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='support_document')
    df_temp = ck_client.ck_select_to_df(sql)
    #
    # 转化为字典格式
    df_temp = df_temp.reset_index(drop=True)
    df_temp['index'] = df_temp.index
    df_temp['index'] = df_temp['index'].apply(lambda m_data: int(m_data / 5))
    #
    for key, group in df_temp.groupby(['index']):
        group = group.reset_index(drop=True)
        data_post0 = []
        for i in range(len(group['sku'])):
            data_dict = {
                "sku": str(group['sku'][i]),
                "site": str(group['站点'][i]),
                "tax_number": str(group['税号'][i]),
                "fba_ari_price": float(group['FBA空运定价/FBM定价_修正'][i]),
                "fba_qatar_ari_price": float(group['FBA卡航定价/FBM定价_修正'][i]),
                "fba_shipping_price": float(group['FBA海运定价/FBM定价_修正'][i]),
                "fba_railway_price": float(group['FBA铁路定价/FBM定价_修正'][i]),
                "fba_ari_amend": float(group['FBA空运定价_修正'][i]),
                "fba_qatar_ari_amend": float(group['FBA卡航定价_修正'][i]),
                "fba_shipping_amend": float(group['FBA海运定价_修正'][i]),
                "fba_railway_amend": float(group['FBA铁路定价_修正'][i]),
                "ari_train_same_amend": float(group['空铁同价利率差_修正'][i]),
                "ari_shipping_same_amend": float(group['空海同价利率差_修正'][i]),
                "proposed_logistics_mode": str(group['建议物流方式'][i])
            }
            # print(data_dict)
            data_post0.append(data_dict)
        data_post = {'data_list': data_post0}
    #
        url = 'http://salescenter.yibainetwork.com:91/apis/open/amazon_open_api/set_sku_price_data?org_code=org_00001'
        # headers = {'Content-Type': 'application/json'}
        # while True:
        try:
            res = requests.post(url, json=data_post).json()
            print(res)
            if res['status'] == 1:
                print('上传成功')
            else:
                print(f'定价比{key}:', res['error_mess'])
        except:
            print(f'定价比{key}:接口失败，重新上传')
            time.sleep(5)
    return df_temp
##
# df_temp = data_to_center()
##

def ebay_Importprice(group, item):
    url = 'http://ebayapi.yibainetwork.com/services/ebay/ebayapi/modifypriceapproval'
    headers = {'Content-Type': 'application/json', }

    data_post0 = []
    # group.to_excel('rslj.xlsx')
    for i in range(len(group['sku'])):
        data_dict = {
            'sku': str(group['sku'][i]),
            'itemid': int(group['item_id'][i]),
            'start_price_target': float(group['销售价'][i]),
            'target_profit': float(group['目标利润率'][i]),
            'shipping_service_cost': str(group['运费(国内第一运费)'][i]),
            'shipping_service_additional_cost': str(group['额外每件加收运费'][i]),
            'work_number': '209313',
            'sale_status': str(group['sale_status'][i]),
            'warehouse_ids': str(group['warehouse_id'][i])

        }

        # print(data_dict)
        data_post0.append(data_dict)
    data_post = {'data': data_post0}
    # print(data_post)

    while True:
        try:
            res = requests.post(url, json=data_post, headers=headers, timeout=(180, 120)).json()
            print(res)
            if res['code'] == 200:
                break
            else:
                print(f'eaby{item}:', res['message'])
        except:
            print(f'eaby{item}:接口失败，重新上传')
            time.sleep(30)

##
def temp_fun(df):
    # 多渠道运费计算
    c1 = (df['size_segment'].str.contains('小号标准尺寸')) & (df['weight'].between(0, 6*28.34, inclusive='both'))
    c2 = (df['size_segment'].str.contains('小号标准尺寸')) & (df['weight'].between(6*28.34, 12*28.34, inclusive='both'))
    c3 = (df['size_segment'].str.contains('小号标准尺寸')) & (df['weight'].between(12*28.34, 453.59, inclusive='both'))
    c4 = (df['size_segment'].str.contains('大号标准尺寸')) & (df['weight'].between(0, 6*28.34, inclusive='both'))
    c5 = (df['size_segment'].str.contains('大号标准尺寸')) & (df['weight'].between(6*28.34, 12*28.34, inclusive='both'))
    c6 = (df['size_segment'].str.contains('大号标准尺寸')) & (df['weight'].between(12*28.34, 453.59, inclusive='both'))
    c7 = (df['size_segment'].str.contains('大号标准尺寸')) & (df['weight'].between(453.59, 2*453.59, inclusive='both'))
    c8 = (df['size_segment'].str.contains('大号标准尺寸')) & (df['weight'].between(2*453.59, 20*453.59, inclusive='both'))
    c9 = (df['size_segment'].str.contains('小号大件')) & (df['weight'].between(0, 2*453.59, inclusive='both'))
    c10 = (df['size_segment'].str.contains('小号大件')) & (df['weight'].between(2*453.59, 30*453.59, inclusive='both'))
    c11 = (df['size_segment'].str.contains('小号大件')) & (df['weight']>30*453.59)
    c12 = (df['size_segment'].str.contains('中号大件')) & (df['weight'].between(0*453.59, 2*453.59, inclusive='both'))
    c13 = (df['size_segment'].str.contains('中号大件')) & (df['weight']>2*453.59)
    c14 = (df['size_segment'].str.contains('大号大件')) & (df['weight'].between(0*453.59, 90*453.59, inclusive='both'))
    c15 = (df['size_segment'].str.contains('大号大件')) & (df['weight']>90*453.59)
    c16 = (df['size_segment'].str.contains('特殊大件')) & (df['weight'].between(0*453.59, 90*453.59, inclusive='both'))
    c17 = (df['size_segment'].str.contains('特殊大件')) & (df['weight']>90*453.59)
    df['fba_to_other_fees'] = np.select([c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17],
                                        [7.15+0.2, 7.8+0.2, 8.25+0.2, 7.35+0.3,8.2 + 0.3,8.5 + 0.3,9.5 + 0.3,
                                         9.5+(df['weight']/453.59-2)*0.62 + 0.5,16 + 1,16+(df['weight']/453.59-2)*0.62 + 1,
                                         32.88+(df['weight']/453.59-2)*0.62 + 1,25.25 + 2.5,25.25+(df['weight']/453.59-2)*0.62 + 2.5,
                                         118.8 + 2.5,118.8+(df['weight']/453.59-90)*1.16 + 2.5,189.19 + 2.5,189.19+(df['weight']/453.59-90)*1.21 + 2.5])
##
# 临时
# 国内仓清仓sku筛选
def temp_sku():
    sql = """
        SELECT s.*, t.title_cn,d.Total_inventory_quantity
        FROM (
            SELECT sku, start_time, end_time, status1
            FROM domestic_warehouse_clear.domestic_warehouse_clear_status 
            WHERE end_time is Null
            ) s
        LEFT JOIN (
            SELECT distinct sku, title_cn
            FROM yibai_prod_base_sync.yibai_prod_sku
        ) t
        ON s.sku = t.sku
        LEFT JOIN support_document.domestic_warehouse_clear_sku_20231219 d
        ON s.sku = d.sku
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='domestic_warehouse_clear')
    df_sku = ck_client.ck_select_to_df(sql)
    ##
    df_sku.columns = [i.split('.')[-1] for i in df_sku.columns.to_list()]
    ##
    sql = f"""
            select a.sku sku, b.path_name as `产品线路线` 
            from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_line = ck_client.ck_select_to_df(sql)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    ##
    df_sku_info = pd.merge(df_sku, df_line, how='left', on=['sku'])
    ##
    c1 = df_sku_info['title_cn'].str.contains("棉衣|棉鞋|帐篷|电热毯|折叠床|火炉")
    df_sku_info['donable_sku'] = np.where(c1, 1, 0)
    ##
    # df_sku_info.to_excel('df_sku_info.xlsx', index=0)
    return None
##
if __name__ == '__main__':
    main()
