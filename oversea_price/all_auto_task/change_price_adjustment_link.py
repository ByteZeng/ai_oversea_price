import datetime
import time
import traceback
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import pandas as pd
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from pulic_func.adjust_price_base_api import public_function as pf
from requests.auth import HTTPBasicAuth
import requests
from all_auto_task.scripts_ck_client import CkClient
import numpy as np
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd
from pulic_func.base_api.adjust_price_function import adjust_post
from pulic_func.base_api.mysql_connect import pd_to_ck
import json

def get_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    return out_date


def get_now_weekday():
    day_of_week = datetime.datetime.now().isoweekday()
    return day_of_week


def get_cd_log_count():
    sql_cd_11 = f"""select count(1) from yibai_oversea_cd_adjust_log"""
    df = sql_to_pd(sql=sql_cd_11, database='over_sea', data_sys='数据部服务器')
    adjust_number = df.loc[0][0]
    print(adjust_number)
    return adjust_number


def get_cd_temp_count():
    sql_cd_11 = f"""select count(1) from yibai_cdiscount_adjust_temp"""
    df = sql_to_pd(sql=sql_cd_11, database='over_sea', data_sys='数据部服务器')
    adjust_number = df.loc[0][0]
    print(adjust_number)
    return adjust_number


def get_newest_time():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    table_list = conn.show_table_list()
    r_list = [i for i in table_list if 'oversea_age_' in i and 'dtl' in i]
    conn.close()
    #
    res_list = [int(i[i.rindex('_') - 8:i.rindex('_')]) for i in r_list if
                '_' not in i[i.rindex('_') - 8:i.rindex('_')]]
    max_number = max(res_list)
    return max_number


def judge_dtl_temp_table_count():
    sql_dtl_temp = f"""select count(1) from oversea_adjust_platform_dtl_temp"""
    df = sql_to_pd(sql=sql_dtl_temp, database='over_sea', data_sys='数据部服务器')
    adjust_number = df.loc[0][0]
    print(adjust_number)
    if not adjust_number:
        send_msg('动销组定时任务推送', 'oversea_adjust_platform_dtl_temp表数据量检查',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} oversea_adjust_platform_dtl_temp表没有数据，请检查",
                 is_all=False)
        raise Exception('oversea_adjust_platform_dtl_temp表没有数据')


def insert_oversea_adjust_platform_dtl():
    try:
        now_time = time.strftime('%Y-%m-%d')
        print(now_time)
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        sql_delete = f"""delete from oversea_adjust_platform_dtl where date='{now_time}'"""
        conn.execute(sql_delete)
        print(1)
        # 2023-06-20 万邑通库存加入清仓状态中
        # 2023-07-19：小海外仓SKU需要清仓
        # 2023-08-22 万邑通有三个仓库解除清仓状态
        sql = """
        SELECT  
            sku, warehouse_code, warehouse_id, best_warehouse, available_stock, warehouse
        FROM yb_datacenter.yb_stock ys
        LEFT JOIN (
            SELECT 
                warehouse_code, warehouse_id, warehouse_name as best_warehouse, 
                case when warehouse_name like '%墨西哥%' then '墨西哥仓' else warehouse end as warehouse
            FROM yb_datacenter.v_warehouse_erp
            ) ve
        ON ys.warehouse_id = ve.warehouse_id
        WHERE 
            date_id = toYYYYMMDD(now()) AND available_stock > 0 and 
            (warehouse_code like '%WYT%' and warehouse_code NOT like '%Kevin%' or warehouse_id in (646,339,796,795,416,421))
            and warehouse_id not in (353, 35, 47)
        """
        ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                             db_name='yibai_oversea')
        df_wyt = ck_client.ck_select_to_df(sql)
        df_wyt['temp'] = df_wyt['sku'] + df_wyt['warehouse']
        # 获取dtl表，修改万邑通SKU的销售状态
        sql = f"""
        SELECT * FROM oversea_age_{get_newest_time()}_dtl
        """
        df_dtl = conn.read_sql(sql)
        df_dtl['temp'] = df_dtl['SKU'] + df_dtl['warehouse']

        df_dtl['adjust_recent_clean'] = np.where(df_dtl['temp'].isin(df_wyt['temp'].unique()), '清仓',
                                                 df_dtl['adjust_recent_clean'])
        df_dtl.drop('temp', axis=1, inplace=True)
        conn.to_sql(df_dtl, table=f"oversea_age_{get_newest_time()}_dtl", if_exists="replace")

        print("价格计算开始...")

        sql_insert = f"""insert into oversea_adjust_platform_dtl
        (
         SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, 
         linest, available_stock, available_stock_money, on_way_stock, create_time, product_size, 
         product_package_size, best_warehouse, warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, 
         inv_age_60_to_90_days, inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, 
         inv_age_180_to_210_days, inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, 
         inv_age_70_plus_days, `SUM(7days_sales)`, `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, 
         day_sales, reduce_day_sales, estimated_sales_days, estimated_reduce_sales_days, a_value_section, 
         day_sales_value_section, `DATE`, over_age150_stock, over_age120_stock, over_age90_stock, 
         over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock, over_sale_section, 
         gradient, is_chargo, acost, begin_profit, `section`, lowest_profit,  first_come_profit, after_profit, 
         adjust_recent_clean, is_adjust, totalCost, lowest_price, platform, country, shipName, price_rmb, 
         AMAZON_five_persent_rmb, AMAZON_ten_persent_rmb, profit_rate, real_profit_rate, price, 
         AMAZON_five_persent, AMAZON_ten_persent, is_distory
        )
        select A.*,
        case
        when  A.platform ='EB'  and A.country in ('UK','DE','FR','ES','IT')  then
        round(((1-0.15-0.015-0.167-0.04)*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='EB'  and A.country not in ('UK','DE','FR','ES','IT') then
        round((0.805*RMB定价-totalCost-new_price)/RMB定价,2)
        ELSE
        round((0.82*RMB定价-totalCost-new_price)/RMB定价,2)
        END AS '实际毛利润率',
        case
        when A.platform ='AMAZON' then
        round((0.62*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='ALI' then
        round((0.79*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='WISH' then
        round((0.62*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform in ('CDISCOUNT','WALMART') then
        round((0.67*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='EB' and A.country in ('UK','DE','FR','ES','IT') then
        round((0.482*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='EB' then
        round((0.652*RMB定价-totalCost-new_price)/RMB定价,2)
        ELSE NULL END AS '实际净利润率',

        CASE
        WHEN  A.platform='EB' THEN  round((A.RMB定价+0.8)/B.rate+0.35,1)-0.01
        WHEN A.platform IN ('AMAZON','WALMART','CDISCOUNT')
        THEN round(A.RMB定价/B.rate,1)-0.01
        WHEN A.platform ='ALI' THEN round(A.RMB定价/6,1)-0.01
        WHEN A.platform ='WISH' THEN A.RMB定价-0.01
        ELSE NULL END AS '本币定价',
        CASE
        WHEN A.platform IN ('AMAZON','EB','WALMART','CDISCOUNT') THEN round(A.净利润百分之五RMB定价_AMAZON/B.rate,1)-0.01
        ELSE NULL END AS 'Amazon5%%净利润本币定价',
        CASE
        WHEN A.platform IN ('AMAZON','EB','WALMART','CDISCOUNT') THEN round(A.净利润百分之十RMB定价_AMAZON/B.rate,1)-0.01
        ELSE NULL END AS 'Amazon10%%净利润本币定价',
        case when A.RMB定价-2<=ROUND(lowest_price,1)-0.01 then '已达到销毁价格'
        else '未达到销毁价格' end as '是否达到销毁价格'
        from
        (
        SELECT
        A.*,
        B.totalCost,
        B.lowest_price,
        B.platform,
        B.country,
        B.shipName,
        case 
        -- 4月30日前可以卖完的不用进行第二阶段降价
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))>=B.lowest_price
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0)),1)-0.01,B.limit_price_rmb)
        
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))<B.lowest_price 
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02),1)-0.01,B.limit_price_rmb)

        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent),1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)

        -- 以下四种情况于2023-04-26添加，小包仓和海外虚拟仓历史累积入库数量低的 sku，当前售价以低于目标净利 * 0.5 维持，即 platform_must_percent * 0.5
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02),1)-0.01,B.limit_price_rmb)

        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5),1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        -- 4月30日前卖不完的需要清仓的进行第二阶段降价
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0)) >= B.lowest_price
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))*power(0.95,IFNULL(D.times_95_off,0))*power(0.9,IFNULL(D.times_90_off,0)),1)-0.01,B.lowest_price) 
        
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))<B.lowest_price 
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
       
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02),1)-0.01,B.limit_price_rmb) 
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
    
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent),1)-0.01,B.limit_price_rmb)
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)

        -- 2023-04-26添加，小包仓和海外虚拟仓历史累积入库数量低的 sku，当前售价以低于目标净利 * 0.5 维持，即 platform_must_percent * 0.5
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02),1)-0.01,B.limit_price_rmb) 
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
    
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5),1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)

        ELSE NULL 
        END AS RMB定价,

        case
        when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05)<B.lowest_price THEN ROUND(B.lowest_price,1)-0.01
         when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05)>=B.lowest_price THEN ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05),1)-0.01
        ELSE NULL
        END AS '净利润百分之五RMB定价_AMAZON',
        case
        when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1)<B.lowest_price THEN ROUND(B.lowest_price,1)-0.01
         when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1)>=B.lowest_price THEN ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1),1)-0.01

        ELSE NULL
        END AS '净利润百分之十RMB定价_AMAZON'
         FROM  oversea_age_{get_newest_time()}_dtl A
        INNER JOIN oversea_transport_fee_useful B ON A.SKU=B.SKU AND A.warehouse_id=B.warehouseId  and platform<>'CDISCOUNT'
        INNER join yibai_platform_fee C on B.platform=C.platform AND B.country=C.site
        left join (
            SELECT sku, warehouse_id, times_95_off, times_90_off, date
            FROM circular_reduction_date 
            where date = (SELECT max(date) FROM circular_reduction_date)
        ) D on A.SKU = D.sku AND A.warehouse_id = D.warehouse_id
        left join virtual_oversea_sku E ON A.SKU = E.sku
        )A
        left join domestic_warehouse_clear.erp_rate B on A.country=B.country COLLATE utf8mb4_general_ci
        
    """
        print(1.5)
        conn.execute(sql_insert)
        print(2)
        sql_insert_2 = f"""
            insert into oversea_adjust_platform_dtl
            (
             SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, 
             linest, available_stock, available_stock_money, on_way_stock, create_time, product_size, 
             product_package_size, best_warehouse, warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, 
             inv_age_60_to_90_days, inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, 
             inv_age_180_to_210_days, inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, 
             inv_age_70_plus_days, `SUM(7days_sales)`, `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, 
             day_sales, reduce_day_sales, estimated_sales_days, estimated_reduce_sales_days, a_value_section, 
             day_sales_value_section, `DATE`, over_age150_stock, over_age120_stock, over_age90_stock, 
             over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock, over_sale_section, 
             gradient, is_chargo, acost, begin_profit, `section`, lowest_profit,  first_come_profit, after_profit, 
             adjust_recent_clean, is_adjust, totalCost, lowest_price, platform, country, shipName, price_rmb, 
             AMAZON_five_persent_rmb, AMAZON_ten_persent_rmb, profit_rate, real_profit_rate, price, 
             AMAZON_five_persent, AMAZON_ten_persent, is_distory
            )
        select A.*,
        case
        when  A.platform ='EB'  and A.country in ('UK','DE','FR','ES','IT')  then
        round(((1-0.15-0.015-0.167-0.04)*RMB定价-totalCost-new_price)/RMB定价,2)


        when A.platform ='EB'  and A.country not in ('UK','DE','FR','ES','IT') then
        round((0.805*RMB定价-totalCost-new_price)/RMB定价,2)

        ELSE
        round((0.82*RMB定价-totalCost-new_price)/RMB定价,2)
        END AS '实际毛利润率',
        case
        when A.platform ='AMAZON' then
        round((0.62*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='ALI' then
        round((0.79*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='WISH' then
        round((0.62*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform in ('CDISCOUNT','WALMART') then
        round((0.67*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='EB' and A.country in ('UK','DE','FR','ES','IT') then
        round((0.482*RMB定价-totalCost-new_price)/RMB定价,2)
        when A.platform ='EB' then
        round((0.652*RMB定价-totalCost-new_price)/RMB定价,2)
        ELSE NULL END AS '实际净利润率',

        CASE
        WHEN  A.platform='EB' THEN  round((A.RMB定价+0.8)/B.rate+0.35,1)-0.01


        WHEN A.platform IN ('AMAZON','WALMART','CDISCOUNT')
        THEN round(A.RMB定价/B.rate,1)-0.01
        WHEN A.platform ='ALI' THEN round(A.RMB定价/6,1)-0.01
        WHEN A.platform ='WISH' THEN A.RMB定价-0.01
        ELSE NULL END AS '本币定价',


        CASE
        WHEN A.platform IN ('AMAZON','EB','WALMART','CDISCOUNT') THEN round(A.净利润百分之五RMB定价_AMAZON/B.rate,1)-0.01
        ELSE NULL END AS 'Amazon5%%净利润本币定价',
        CASE
        WHEN A.platform IN ('AMAZON','EB','WALMART','CDISCOUNT') THEN round(A.净利润百分之十RMB定价_AMAZON/B.rate,1)-0.01
        ELSE NULL END AS 'Amazon10%%净利润本币定价',
        case when A.RMB定价-2<=ROUND(lowest_price,1)-0.01 then '已达到销毁价格'
        else '未达到销毁价格' end as '是否达到销毁价格'
        from
        (
        SELECT
        A.*,
        B.totalCost,
        B.lowest_price,
        B.platform,
        B.country,
        B.shipName,
        case 
        -- 4月30日前可以卖完的不用进行第二阶段降价
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))>=B.lowest_price
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0)),1)-0.01,B.limit_price_rmb)
        
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))<B.lowest_price 
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)

        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02),1)-0.01,B.limit_price_rmb)

        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent),1)-0.01,B.limit_price_rmb)
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        -- 以下四种情况于2023-04-26添加，小包仓和海外虚拟仓历史累积入库数量低的 sku，当前售价以低于目标净利 * 0.5 维持，即 platform_must_percent * 0.5
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02),1)-0.01,B.limit_price_rmb)

        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5),1)-0.01,B.limit_price_rmb)
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days <= DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
        
        -- 4月30日前卖不完的需要清仓的进行第二阶段降价
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0)) >= B.lowest_price
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))*power(0.95,IFNULL(D.times_95_off,0))*power(0.9,IFNULL(D.times_90_off,0)),1)-0.01,B.lowest_price)
        
        when A.adjust_recent_clean IN ('清仓','负利润加快动销','正利润加快动销') AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(A.after_profit, 0))<B.lowest_price 
       THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
       
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02),1)-0.01,B.limit_price_rmb) 
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
       
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent),1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) <> 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)

        -- 以下四种情况于2023-04-26添加，小包仓和海外虚拟仓历史累积入库数量低的 sku，当前售价以低于目标净利 * 0.5 维持，即 platform_must_percent * 0.5
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)>=B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02),1)-0.01,B.limit_price_rmb) 
        
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='是' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5+0.02)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
       
        when A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW())
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)>=B.lowest_price
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5),1)-0.01,B.limit_price_rmb)
        
        when  A.adjust_recent_clean in ('回调','正常')AND is_chargo='否' AND estimated_sales_days > DATEDIFF('2023-10-31',NOW()) 
        AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent*0.5)<B.lowest_price 
        AND IF(ISNULL(E.is_low_storage), 0, E.is_low_storage) = 1
        THEN GREATEST(ROUND(B.lowest_price,1)-0.01,B.limit_price_rmb)
    
        ELSE NULL 
        END AS RMB定价,

        case
        when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05)<B.lowest_price THEN ROUND(B.lowest_price,1)-0.01
         when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05)>=B.lowest_price THEN ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05),1)-0.01
        ELSE NULL
        END AS '净利润百分之五RMB定价_AMAZON',
        case
        when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1)<B.lowest_price THEN ROUND(B.lowest_price,1)-0.01
         when B.platform='AMAZON' and A.adjust_recent_clean in ('回调','正常') AND (A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1)>=B.lowest_price THEN ROUND((A.new_price+totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1),1)-0.01



        ELSE NULL
        END AS '净利润百分之十RMB定价_AMAZON'
         FROM  oversea_age_{get_newest_time()}_dtl A
        INNER JOIN oversea_transport_fee_useful B ON A.SKU=B.SKU AND A.warehouse_id=B.warehouseId  and platform='CDISCOUNT' AND warehouseName not like '%%英国%%'
        INNER join yibai_platform_fee C on B.platform=C.platform AND B.country=C.site
        left join (
            SELECT sku, warehouse_id, times_95_off, times_90_off, date
            FROM circular_reduction_date 
            where date=(SELECT max(date) FROM circular_reduction_date)
            ) D on A.SKU = D.sku AND A.warehouse_id = D.warehouse_id
        left join virtual_oversea_sku E ON A.SKU = E.sku
        )A
        left join domestic_warehouse_clear.erp_rate B on A.country=B.country COLLATE utf8mb4_general_ci
        
        """
        conn.execute(sql_insert_2)
        send_msg('动销组定时任务推送', 'listing调价链接',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓oversea_adjust_platform_dt表生成",
                 is_all=False)
        conn.close()
    except:
        send_msg('动销组定时任务推送', 'listing调价链接',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓oversea_adjust_platform_dt表生成出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def ali():
    now_time = time.strftime('%Y-%m-%d')
    print(now_time)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql1 = """truncate ali_yunfei_selct"""
    conn.execute(sql1)
    # 保持跟IT逻辑一致,选不同子仓到站点对应的最低运费,然后从这些最便宜运费里面选取最贵的warehouse_id发往到该站点的运费
    sql2 = """insert into ali_yunfei_selct select * from (select *,row_number() over (partition by sku,country order by totalCost desc) as m_rank
        from (select *,row_number() over (partition by sku,warehouse_id,country order by totalCost) as r_rank
        
        FROM `oversea_adjust_platform_dtl_temp` a
        where platform='ALI'
        ) A
        having r_rank=1)C
        where m_rank=1"""
    conn.execute(sql2)

    sql_ali_1 = f"""truncate yibai_ali_adjust_temp"""
    conn.execute(sql_ali_1)
    print(50)
    ## 2022/3/16ali运费改成最便宜运费渠道最贵的运费
    sql_ali_2 = f"""INSERT INTO yibai_ali_adjust_temp 
            SELECT
            a.*,
            b.price,
            b.adjust_recent_clean,
            b.warehouse,
            best_warehouse,
            b.totalCost,
            b.is_chargo,
            after_profit
            FROM
            yibai_ali_oversea_listing_price a
            INNER JOIN ali_yunfei_selct  b 
            ON a.SKU = b.SKU and a.site=b.country
            WHERE
            a.date = '{now_time}' 
            AND product_id IN (
            SELECT
            product_id 
            FROM
            ( SELECT product_id, count( DISTINCT sku ) AS flag FROM yibai_ali_oversea_listing_price WHERE date = '{now_time}' GROUP BY 1 ) a 
            WHERE
            flag = 1 
            
            
            )"""
    conn.execute(sql_ali_2)
    # 20220105删除英国仓发往其他站点和非英国仓发往英国站点的数据
    sql_ali_15 = """delete from yibai_ali_adjust_temp where (site='uk' and warehouse!='英国仓')
            or (site!='uk' and warehouse='英国仓') """
    conn.execute(sql_ali_15)
    print(51)
    sql_ali_3 = f"""insert into yibai_oversea_ali_adjust_log
                    select
                    A. *
                    from

                    (
                        select a.*, row_number()
                    over(partition
                    by
                    product_id, sku
                    order
                    by
                    totalCost desc) as flag, CASE

                    WHEN
                    price >= sku_price
                    THEN
                    '涨价'
                    ELSE
                    '降价'
                    END
                    AS
                    'is_up'
                    from yibai_ali_adjust_temp a) A
                    where
                    flag = 1 and (
                            (A.IS_UP = '涨价' AND A.adjust_recent_clean IN ('正利润加快动销', '负利润加快动销') and price - sku_price >= 0.3)
                    OR
                    (A.IS_UP = '涨价'
                    AND
                    A.adjust_recent_clean in ('正常', '回调')
                    AND
                    is_chargo = '否' and price - sku_price >= 0.3 )
                    OR
                    (A.IS_UP = '降价'
                    AND
                    A.adjust_recent_clean in ('正常', '回调')
                    AND
                    is_chargo = '是' and sku_price - price >= 0.3 )
                    OR
                    (A.IS_UP = '涨价'
                    AND
                    A.adjust_recent_clean in ('正常', '回调')
                    AND
                    is_chargo = '是' and price - sku_price >= 0.3 )
                    OR
                    (A.IS_UP = '降价'
                    AND
                    A.adjust_recent_clean
                    IN('清仓', '正利润加快动销', '负利润加快动销') and sku_price - price >= 0.3 ))"""
    conn.execute(sql_ali_3)
    conn.close()
    send_msg('动销组定时任务推送', 'listing调价链接',
             f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓ali调价链接更新完成",
             is_all=False)


def ali_link_price_adjustment():
    try:
        ali()
    except Exception as e:
        print(str(e))
        try:
            time.sleep(60)
            ali()
        except:
            send_msg('动销组定时任务推送', 'listing调价链接',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ali调价链接处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                     status='失败')
            raise Exception(traceback.format_exc())


# 大部小组对应表
def get_oa_dep():
    def create_attr_token():
        token_url = "http://oauth.java.yibainetwork.com/oauth/" \
                    "token?grant_type=client_credentials"
        # 获取token
        token_request = requests.post(token_url, auth=HTTPBasicAuth("prod_libo", "libo321"))
        token_dict_content = token_request.json(strict=True)
        now_token = token_dict_content['access_token']
        return now_token

    header = {'Content-Type': "application/json;charset=UTF-8"}
    base_url = f"http://rest.java.yibainetwork.com/oa/oaDepartment/getOaDepartment"
    # 合成url
    now_token = create_attr_token()
    url = f"{base_url}?access_token={now_token}"
    response = requests.post(url, json={'isDel': 0}, headers=header)
    response_result = response.json(strict=True)
    if "error_description" in response_result.keys():
        err_reason = response_result['error_description']
        if "Access token expired" in err_reason:
            now_token = create_attr_token()
            url = f"{base_url}?access_token={now_token}"
            response = requests.post(url, json={'isDel': 0}, headers=header)
            response_result = response.json(strict=True)
    return response_result


def get_top_dep():
    # 1085984-为最大层级, 54495400-为销售团队, 30046131-Amazon部
    total_data = (get_oa_dep())['data']
    # amazon_sale_dep_list = ["产品线团队", "海外仓团队", "精品团队", "自发货团队"]
    amazon_sale_dep_id = [1079248, 1079249, 1079250, 1079251]
    dep_child_list, dep_sale_df = [], pd.DataFrame()
    for one_top_dep in total_data:
        if one_top_dep["userNumberDetail"] != "" and one_top_dep['pid'] in amazon_sale_dep_id:
            now_dep = one_top_dep['name']
            short_dep = now_dep.split("部")[0] + "部" if "部" in now_dep \
                else now_dep.split("仓")[0] + "仓" if "仓" in now_dep else ""
            update_time = one_top_dep['updateTime']
            now_child_dep_list = list(map(
                lambda m_x: m_x.split("(")[0].strip() if "(" in m_x else m_x.split("（")[0].strip(),
                [one_child['name'] for one_child in one_top_dep['children']]
            ))
            # 当1个小组同时在两个大部时，优先取组中有括号的
            now_child_dep_list1 = list(map(
                lambda m_x: 1 if "(" in m_x or "（" in m_x else 2,
                [one_child['name'] for one_child in one_top_dep['children']]
            ))
            dep_child_list.append(
                pd.DataFrame({'dep_name': [short_dep] * len(now_child_dep_list), 'sale_group_name': now_child_dep_list,
                              'update_time': [update_time] * len(now_child_dep_list),
                              'sale_group_name1': now_child_dep_list1, })
            )
    dep_sale_df = dep_sale_df.append(dep_child_list, sort=False)
    return dep_sale_df


# 匹配大部
def xiaozu_dabu():
    df = get_top_dep()
    df = df.sort_values(['sale_group_name1'], ascending=True)
    df = df.drop_duplicates(['sale_group_name'], 'first')
    df = df[['dep_name', 'sale_group_name']].drop_duplicates()
    df.rename(columns={'dep_name': '大部', 'sale_group_name': 'group_name'}, inplace=True)
    df = df.drop_duplicates()

    df1 = df[df['group_name'] == '深圳3组']
    if df1.shape[0] == 0:
        df1 = pd.DataFrame({'group_name': ['深圳3组'],
                            '大部': ['深圳产品线一部']})
        df = df.append(df1)
    return df


def amazon():
    now_time = time.strftime('%Y-%m-%d')
    print(now_time)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql_amazon_1 = f"""TRUNCATE yibai_amazon_oversea_listing_price_bake"""
    conn.execute(sql_amazon_1)
    print(20)
    sql_amazon_2 = f"""insert into yibai_amazon_oversea_listing_price_bake select * from yibai_amazon_oversea_listing_price where DATE = '{now_time}'"""
    conn.execute(sql_amazon_2)
    print(21)
    sql_amazon_3 = f"""TRUNCATE yibai_amazon_adjust_temp"""
    conn.execute(sql_amazon_3)
    print(22)
    # 20230818 正常品不降价添加限制条件：日销>=0.1
    sql_amazon_4 = f"""
            SELECT
            a. *,
            b.price,
            b.adjust_recent_clean,
            b.warehouse,
            b.best_warehouse,
            b.totalCost,
            b.day_sales,
            CASE

            WHEN
            price >= online_price
            THEN
            '涨价'
            ELSE
            '降价'
            END
            AS
            'is_up',
            b.
            `AMAZON_five_persent_rmb`,
            b.
            `AMAZON_ten_persent_rmb`,
            b.is_chargo
            FROM
            yibai_amazon_oversea_listing_price_bake
            a
            INNER
            JOIN
            oversea_adjust_platform_dtl_temp
            b
            ON
            a.sku = b.sku
            AND
            a.site = b.country
            AND
            b.platform = 'AMAZON'
            ORDER
            BY
            b.totalCost"""
    # conn.execute(sql_amazon_4)
    # sql = f"""select * from yibai_amazon_adjust_temp where date='{datetime.date.today()}'"""
    df = conn.read_sql(sql_amazon_4)
    df.loc[df['site'] == 'uk', '站点'] = '英国'
    df.loc[df['site'] == 'us', '站点'] = '美国'
    df.loc[df['site'] == 'de', '站点'] = '德国'
    df.loc[df['site'] == 'jp', '站点'] = '日本'
    df.loc[df['site'] == 'pl', '站点'] = '波兰'
    df.loc[df['site'] == 'fr', '站点'] = '法国'
    df.loc[df['site'] == 'au', '站点'] = '澳洲'
    df.loc[df['site'] == 'nl', '站点'] = '荷兰'
    df.loc[df['site'] == 'es', '站点'] = '西班牙'
    df.loc[df['site'] == 'it', '站点'] = '意大利'
    df.loc[df['site'] == 'se', '站点'] = '瑞典'
    df.loc[df['site'] == 'ca', '站点'] = '加拿大'
    df.loc[df['site'] == 'mx', '站点'] = '墨西哥'
    df['account_id'] = df['account_id'].astype("int")
    # coupon
    def amazon_coupon_fu(df):
        conn_mx = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
        # coupon获取的sql语句在公共函数库里
        sql = pf.get_amazon_coupon(conn_mx)
        df0 = conn_mx.ck_select_to_df(sql)
        print('亚马逊coupon数据记录共{}条'.format(len(df0)))
        # percentage_off
        df1 = df0[df0['coupon_type'] == 1]
        df1 = df1[['account_id', 'asin', 'percentage_off']]
        df1['percentage_off'] = df1['percentage_off'].apply(lambda m: float(m) / 100)
        df1 = df1.sort_values(['percentage_off'], ascending=False)
        df1 = df1.drop_duplicates(['account_id', 'asin'], 'first')
        df = df.merge(df1, on=['account_id', 'asin'], how='left')
        df['percentage_off'].fillna(0, inplace=True)
        # money_off
        df2 = df0[df0['coupon_type'] == 2]
        df2 = df2[['account_id', 'asin', 'money_off']]
        df2['money_off'] = df2['money_off'].apply(lambda m: float(m))
        df2 = df2.sort_values(['money_off'], ascending=False)
        df2 = df2.drop_duplicates(['account_id', 'asin'], 'first')
        df = df.merge(df2, on=['account_id', 'asin'], how='left')
        df['money_off'].fillna(0, inplace=True)
        # 优惠券手续费
        coupon_shouxufei = pd.DataFrame({'站点': ['美国', '加拿大', '墨西哥', '英国', '法国', '德国', '西班牙', '意大利', '日本'],\
        'Coupon_handling_fee': [0.6, 0.6, 0, 0.45, 0.5, 0.5, 0.5, 0.5, 60]})
        df = df.merge(coupon_shouxufei, on=['站点'], how='left')
        df['Coupon_handling_fee'].fillna(0, inplace=True)
        df.loc[(df['percentage_off'] == 0) & (df['money_off'] == 0), 'Coupon_handling_fee'] = 0
        return df
    print('开始处理coupon数据...')
    df = amazon_coupon_fu(df)
    print('处理完后完成')
    print(df.info())
    # promotion
    def amazon_promotion_sku(df, n=1):
        df1 = get_promotion_sku()
        df = df.merge(df1, on=['account_id', "seller_sku"], how='left')
        df['promotion_source'] = df['promotion_source'].fillna(4)
        df['promotion_source'] = df['promotion_source'].astype(int)
        df['promotion_percent'] = df['promotion_percent'].fillna(0)
        df['promotion_amount'] = df['promotion_amount'].fillna(0)

        if n == 2:
            df.drop(['promotion_source'], axis=1, inplace=True)
        return df

    def get_promotion_sku(n=1):
        d_t = datetime.date.today().isoformat()
        """
            人工促销商品列表：`yibai_product_cloud`.`yibai_amazon_promotion_sku`
            调价促销商品列表：`yibai_product_cloud`.`yibai_amazon_promotion_sku_adj`
            先code在`yibai_product_cloud`.`yibai_amazon_promotion` 找到source是1还是2
            是1就去：`yibai_product_cloud`.`yibai_amazon_promotion_sku`
            是2就去：`yibai_product_cloud`.`yibai_amazon_promotion_sku_adj`
        """
        sql = f"""
            select a.`code` as `code`,a.account_id as account_id,a.method as method,
            a.b2b_price as b2b_price,a.source as source,b.seller_sku as seller_sku
            from yibai_amazon_promotion a
            left join yibai_amazon_promotion_sku b
            on a.`code` = b.`code`
            where a.status in (5, 11, 12) and a.start_date<='{d_t}' and a.end_date>='{d_t}'
            and a.type=1 and a.source=1
            union all
            select a.`code` as `code`,a.account_id as account_id,a.method as method,
            a.b2b_price as b2b_price,a.source as source,b.seller_sku as seller_sku
            from yibai_amazon_promotion a
            left join yibai_amazon_promotion_sku_adj b
            on a.`code` = b.`code`
            where a.status in (5, 11, 12) and a.start_date<='{d_t}' and a.end_date>='{d_t}'
            and a.type=1 and a.source=2
            """
        df = sql_to_pd(database='yibai_product_cloud', sql=sql, data_sys='AMAZON刊登库')
        df = df.drop_duplicates()
        #
        a_list = []
        for key, group in df.groupby(['b2b_price']):
            key = json.loads(key)
            df0 = pd.DataFrame(key)
            df0['qlb'] = df0['qlb'].apply(lambda m: int(m))
            # 只看买一个时的折扣
            df0 = df0[df0['qlb'] == 1]
            if df0.shape[0] > 0:
                df0 = df0.reset_index(drop=True)
                group['b2b_price_qp'] = df0['qp'][0]
                a_list.append(group)
        df = pd.DataFrame()
        df = df.append(a_list)
        df.drop(['b2b_price'], axis=1, inplace=True)
        df['b2b_price_qp'] = df['b2b_price_qp'].astype(float)
        # promotion 折扣分档
        promotion_percent_list = [0.01, 0.02, 0.04, 0.07, 0.1, 0.13, 0.16, 0.19, 0.22, 0.25, 0.28, 0.3, 0.35, 0.4]
        if n == 2:
            df = df[(df['method'] == 1) & (df['source'] == 2)]
            df = df[['code', 'account_id', "seller_sku", 'b2b_price_qp']]
            df['b2b_price_qp'] = df['b2b_price_qp'] / 100
            df = df[df['b2b_price_qp'].isin(promotion_percent_list)]
            df.rename(columns={'b2b_price_qp': 'promotion_percent1'}, inplace=True)
            df = df.reset_index(drop=True)
            return df
        # 存在多条促销时，折扣累加，不改折扣
        df_count = df[['account_id', "seller_sku"]]
        df_count['行数'] = 1
        df_count = df_count.groupby(['account_id', "seller_sku"])[['行数']].sum().reset_index()
        df = df.merge(df_count, on=['account_id', "seller_sku"])
        df.loc[df['行数'] > 1, 'source'] = 3

        df0 = df[['account_id', "seller_sku", 'source']].drop_duplicates()
        # 百分比
        df1 = df[df['method'] == 1]
        df1 = df1[['account_id', "seller_sku", 'b2b_price_qp']]
        df1.columns = ['account_id', "seller_sku", 'b2b_price_percent']
        df1['b2b_price_percent'] = df1['b2b_price_percent'] / 100
        df1 = df1.groupby(['account_id', "seller_sku"])[['b2b_price_percent']].sum().reset_index()
        # 折扣
        df2 = df[df['method'] == 2]
        df2 = df2[['account_id', "seller_sku", 'b2b_price_qp']]
        df2.columns = ['account_id', "seller_sku", 'b2b_price_amount']
        df2 = df2.groupby(['account_id', "seller_sku"])[['b2b_price_amount']].sum().reset_index()

        df = df0.merge(df1, on=['account_id', "seller_sku"], how='left')
        df = df.merge(df2, on=['account_id', "seller_sku"], how='left')
        df['b2b_price_percent'] = df['b2b_price_percent'].fillna(0)
        df['b2b_price_amount'] = df['b2b_price_amount'].fillna(0)
        df = df[['account_id', "seller_sku", "source", "b2b_price_percent", "b2b_price_amount"]]
        df.columns = ['account_id', "seller_sku", "promotion_source", "promotion_percent", "promotion_amount"]
        df = df.reset_index(drop=True)
        return df

    df = amazon_promotion_sku(df, n=2)
    # 日本站点积分
    df = pf.jp_integral(df)
    # 最后价格要加上折扣
    df['no_coupon_price'] = df['price']
    df['price'] = (df['price'] + (df['money_off'] + df['Coupon_handling_fee'] + df['promotion_amount'])) / \
                  (1 - df['percentage_off'] - df['your_price_point'] - df['promotion_percent'])
    df['price'] = df['price'].round(1) - 0.01
    del df['站点']

    # 阶梯定价，30个一组阶梯定价，保证相邻的30个定价不同
    # df_list, df_all=[], pd.DataFrame()
    # for (key1,key2,key3),group in df.groupby(['sku','warehouse','price']):
    #     group=group.reset_index(drop=True)
    #     df_list.append(group)
    # df_all=df.sort_values(by=['sku','warehouse','site','price'])
    # df_all['index']=df_all.index.values % 30
    # df_all.reset_index(inplace=True,drop=True)
    # df_all['price']=df_all['price']+0.01*df_all['index']
    # 删除掉英国仓发往其他站点和英国站点发往其仓的
    df = df[~(((df['site'] == 'uk') & (df['warehouse'] != '英国仓')) | (
                (df['site'] != 'uk') & df['warehouse'] == '英国仓'))]

    conn.to_sql(df, table="yibai_amazon_adjust_temp", if_exists="replace")
    print(23)
    # 删除掉英国仓发往其他站点和英国站点发往其仓的
    # sql_amazon_15=f"""delete from yibai_amazon_adjust_temp where (site='uk' and warehouse!='英国仓')
    #         or (site!='uk' and warehouse='英国仓')"""
    # conn.execute(sql_amazon_15)
    sql_amazon_5 = f"""TRUNCATE yibai_amazon_adjust_dtl_temp"""
    conn.execute(sql_amazon_5)
    print(24)
    # 20230818 正常品不降价添加限制条件：日销>=0.1
    sql_amazon_6 = f"""
            select A.* from (
            SELECT
                A.*,row_number() over (partition by A.account_id,A.seller_sku order by totalCost) as flag
            FROM
            yibai_amazon_adjust_temp A
            where case when warehouse = '英国仓' then site = 'uk' else 1 = 1 end 
            )A
            WHERE FLAG=1 AND short_name not in ('GTE','GTG','GTF','GTS','GTI','GTN','GTQ','GTP','A1P','A1W','A1E','A1G','A1F','A1S','A1I','A1N','H9E','H9G','H9F','H9S','H9I','H9W','H9N','GTW','H9P')
            and (
            (A.IS_UP = '涨价' AND A.adjust_recent_clean IN ('正利润加快动销','负利润加快动销','清仓')and price- online_price>=0.3 )
            OR
            (A.IS_UP = '涨价' AND A.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and price- online_price>=0.3 )
            OR
            (A.IS_UP = '降价' AND A.adjust_recent_clean in ('正常','回调') AND is_chargo='是' and online_price- price>=0.3 )
            OR
            (A.IS_UP = '降价' AND A.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and day_sales < 0.1 and online_price- price>=0.3 )
            OR
            (A.IS_UP = '涨价' AND A.adjust_recent_clean in ('正常','回调') AND is_chargo='是' and price- online_price>=0.3 )
            OR
            (A.IS_UP = '降价'AND A.adjust_recent_clean IN ( '清仓', '正利润加快动销','负利润加快动销') and online_price- price>=0.3 ))"""
    df_sql_amazon_6 = conn.read_sql(sql_amazon_6)
    df_sql_amazon_6.drop('day_sales', axis=1, inplace=True)
    conn.to_sql(df_sql_amazon_6, table="yibai_amazon_adjust_dtl_temp", if_exists="append")
    # conn.execute(sql_amazon_6)
    print(25)
    sql_amazon_7 = f"""TRUNCATE yibai_oversea_amazon_adjust_log"""
    conn.execute(sql_amazon_7)
    print(26)
    sql_amazon_8 = f"""insert into yibai_oversea_amazon_adjust_log
            select
            A. *,B.warehouse, C.status
            FROM
            yibai_amazon_adjust_dtl_temp
            A
            left join yibai_oversea_no_adjust_sku B
            on A.sku=B.sku and A.warehouse=B.warehouse
            left
            join
            yibai_amazon_adjustprice_filter_sku
            C
            ON
            A.account_id = C.account_id
            AND
            A.seller_sku = C.seller_sku
            AND
            start_time <= '{now_time}' and end_time >= '{now_time}' and C.
            `status` = 1"""
    conn.execute(sql_amazon_8)
    print(27)
    sql_amazon_9 = f"""delete from yibai_oversea_amazon_adjust_log where status_no_adjust = '1'"""
    conn.execute(sql_amazon_9)
    print(28)

    # 提取log表
    df_log = conn.read_sql("""select * from yibai_oversea_amazon_adjust_log""")
    # 看看是否时自发货小组
    # self_df=sql_to_pd7(sql="""select * from Self_shipping_model_group""",database='domestic_warehouse_clear')
    # self_list=self_df['group_name'].tolist()
    # df_log.loc[df_log['group_name'].isin(self_list),'去除条件']='自发货'
    # #自发货
    # df_log1=df_log[df_log['去除条件']=='自发货']
    # #非自发货
    # df_log2=df_log[df_log['去除条件']!='自发货']

    # 提取asin并匹配上group_name方便后期的去除
    asin = conn.read_sql(
        f"""select account_id,asin,'需去除' from yibai_oversea_amazon_asin_fba where date >= '{get_date()}' and status = '1'""")
    asin['account_id'] = asin['account_id'].astype('int')
    group = sql_to_pd(sql="""
                            select a.id as account_id,b.group_name,
                            case when a.site='sp' then 'es' else a.site end as site
                            from yibai_amazon_account a
                            left join yibai_amazon_group b 
                            on a.group_id=b.group_id""", database="yibai_system", data_sys='AMAZON刊登库')

    asin1 = asin.merge(group, on=['account_id'], how='left')

    del asin1['account_id']
    # 20220517黄星星告诉FBA对应的FBM已改为同大部，同站点，同asin下有FBA链接的FBM
    dabu = xiaozu_dabu()
    dabu = dabu[['group_name', '大部']]
    asin1 = asin1.merge(dabu, on=['group_name'], how='left')
    asin1.loc[asin1['大部'].isnull(), '大部'] = asin1['group_name']
    asin1 = asin1[['大部', 'site', 'asin', '需去除']]
    df_log = df_log.merge(dabu, on=['group_name'], how='left')
    df_log.loc[df_log['大部'].isnull(), '大部'] = df_log['group_name']
    # 20220517 去除同大部，同站点下，同asin有FBA链接的FBM
    df_log['site'] = df_log['site'].str.lower()
    asin1['site'] = asin1['site'].str.lower()
    df_log = df_log.merge(asin1, on=['大部', 'site', 'asin'], how='left')
    df_log = df_log[df_log['需去除'].isnull()]
    # 删除大部和需去除列
    df_log.drop(['需去除', '大部'], axis=1, inplace=True)

    # 先去除同account_id同asin下有活跃FBA的FBM链接
    # df_log2=df_log2.merge(asin,on=['account_id','asin'],how='left')
    # df_log2=df_log2[df_log2['需去除']!='需去除']
    # 再根据自发货小组去除同小组同站点下

    # df_log1=df_log1.merge(asin1,on=['group_name','site','asin'],how='left')
    # df_log1=df_log1[df_log1['需去除']!='需去除']
    # 将去除好的df_log1和df_log2合起来
    # df_all=pd.concat([df_log1,df_log2])
    # df_all.drop(columns=['去除条件','需去除'],axis=1,inplace=True)

    sql_amazon_7 = f"""TRUNCATE yibai_oversea_amazon_adjust_log"""
    conn.execute(sql_amazon_7)
    conn.to_sql(df_log, table='yibai_oversea_amazon_adjust_log', if_exists='append')

    # #不在自发货小组删除同account_id,同asin下FBA为活跃的对应的FBM链接
    # # 删除非英国站同asin下有FBA且FBA为活跃的对应的FBM
    # sql_amazon_10 = f"""delete from yibai_oversea_amazon_adjust_log
    #                             where  site!='uk' and
    #                             asin in (select asin from yibai_oversea_amazon_asin_fba where date >= '{get_date()}' and status = '1')
    #                         """
    #
    #
    # conn.execute(sql_amazon_10)
    # # 删除同asin下且sku为GB开头下的对应的英国站FBM链接
    # sql_amazon11 = f"""delete from yibai_oversea_amazon_adjust_log
    #                     where  site='uk' and
    #                     asin in (select asin from yibai_oversea_amazon_asin_fba
    #                     where date>='{get_date()}' and status='1' and left(sku,3) = 'GB-')"""
    # conn.execute(sql_amazon11)
    conn.close()
    send_msg('动销组定时任务推送', 'listing调价链接',
             f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓amazon调价链接更新完成",
             is_all=False)


def amazon_link_price_adjustment():
    try:
        amazon()
    except Exception as e:
        print(str(e))
        try:
            time.sleep(60)
            amazon()
        except:
            send_msg('动销组定时任务推送', 'listing调价链接',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓amazon调价链接处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                     status='失败')
            raise Exception(traceback.format_exc())


def cd():
    now_time = time.strftime('%Y-%m-%d')
    print(now_time)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #
    sql_cd_1 = f"""TRUNCATE yibai_cdiscount_oversea_listing_price_bake"""
    conn.execute(sql_cd_1)
    print(30)
    sql_cd_2 = f"""insert into yibai_cdiscount_oversea_listing_price_bake
                    select * from yibai_cdiscount_oversea_listing_price where DATE = '{now_time}'"""
    conn.execute(sql_cd_2)
    print(31)
    sql_cd_3 = f"""TRUNCATE yibai_cdiscount_adjust_temp"""
    conn.execute(sql_cd_3)
    print(32)
    sql_cd_4 = f"""insert into yibai_cdiscount_adjust_temp
                        SELECT
                        DISTINCT
                        a. *,
                        b.price,
                        b.adjust_recent_clean,
                        b.best_warehouse, b.warehouse,
                        b.totalCost,
                        GREATEST(b.price, 1) as post_price,
                        0 as std_shipping,
                        0 as std_add,
                        0 as trk_shipping,
                        0 as trk_add,
                        12.99 as reg_shipping,
                        12.99 as reg_add,
                        999 as fst_shipping,
                        999 as fst_add,
                        CASE

                        WHEN
                        price >= online_price
                        THEN
                        '涨价'
                        ELSE
                        '降价'
                        END
                        AS
                        'is_up',
                        b.is_chargo,b.after_profit,b.day_sales
                        FROM
                        yibai_cdiscount_oversea_listing_price_bake
                        a
                        INNER
                        JOIN
                        oversea_adjust_platform_dtl_temp
                        b
                        ON
                        a.SKU = b.SKU
                        AND
                        b.platform = 'CDISCOUNT'
                        where
                        a.DATE = '{now_time}' and account_id < 100"""
    conn.execute(sql_cd_4)
    print(33)

    sql_cd_5 = f"""insert into yibai_cdiscount_adjust_temp
                    SELECT
                    DISTINCT
                    a. *,
                    b.price,
                    b.adjust_recent_clean,
                    b.best_warehouse, b.warehouse,
                    b.totalCost,
                    GREATEST(b.price, 1) as post_price,
                    0 as std_shipping,
                    0 as std_add,
                    0 as trk_shipping,
                    0 as trk_add,
                    12.99 as reg_shipping,
                    12.99 as reg_add,
                    999 as fst_shipping,
                    999 as fst_add,
                    CASE

                    WHEN
                    price >= online_price
                    THEN
                    '涨价'
                    ELSE
                    '降价'
                    END
                    AS
                    'is_up', b.is_chargo,b.after_profit,b.day_sales
                    FROM
                    yibai_cdiscount_oversea_listing_price_bake
                    a
                    INNER
                    JOIN
                    oversea_adjust_platform_dtl_temp
                    b
                    ON
                    a.SKU = b.SKU
                    AND
                    b.platform = 'CDISCOUNT'
                    where
                    a.DATE = '{now_time}' and account_id >= 100 and account_id < 200"""
    conn.execute(sql_cd_5)
    print(34)
    sql_cd_6 = f"""insert into yibai_cdiscount_adjust_temp
                    SELECT
                    DISTINCT
                    a. *,
                    b.price,
                    b.adjust_recent_clean,
                    b.best_warehouse, b.warehouse,
                    b.totalCost,
                    GREATEST(b.price, 1) as post_price,
                    0 as std_shipping,
                    0 as std_add,
                    0 as trk_shipping,
                    0 as trk_add,
                    12.99 as reg_shipping,
                    12.99 as reg_add,
                    999 as fst_shipping,
                    999 as fst_add,
                    CASE

                    WHEN
                    price >= online_price
                    THEN
                    '涨价'
                    ELSE
                    '降价'
                    END
                    AS
                    'is_up', b.is_chargo,b.after_profit,b.day_sales
                    FROM
                    yibai_cdiscount_oversea_listing_price_bake
                    a
                    INNER
                    JOIN
                    oversea_adjust_platform_dtl_temp
                    b
                    ON
                    a.SKU = b.SKU
                    AND
                    b.platform = 'CDISCOUNT'
                    where
                    a.DATE = '{now_time}' and account_id >= 200 and account_id < 300"""
    conn.execute(sql_cd_6)
    print(35)
    sql_cd_7 = f"""insert into yibai_cdiscount_adjust_temp
                    SELECT
                    DISTINCT
                    a. *,
                    b.price,
                    b.adjust_recent_clean,
                    b.best_warehouse, b.warehouse,
                    b.totalCost,
                    GREATEST(b.price, 1) as post_price,
                    0 as std_shipping,
                    0 as std_add,
                    0 as trk_shipping,
                    0 as trk_add,
                    12.99 as reg_shipping,
                    12.99 as reg_add,
                    999 as fst_shipping,
                    999 as fst_add,
                    CASE
                    WHEN
                    price >= online_price
                    THEN
                    '涨价'
                    ELSE
                    '降价'
                    END
                    AS
                    'is_up', b.is_chargo,b.after_profit,b.day_sales
                    FROM
                    yibai_cdiscount_oversea_listing_price_bake
                    a
                    INNER
                    JOIN
                    oversea_adjust_platform_dtl_temp
                    b
                    ON
                    a.SKU = b.SKU
                    AND
                    b.platform = 'CDISCOUNT'
                    where
                    a.DATE = '{now_time}' and account_id >= 300 and account_id < 400"""
    conn.execute(sql_cd_7)
    print(36)
    sql_cd_8 = f"""insert into yibai_cdiscount_adjust_temp
                    SELECT
                    DISTINCT
                    a. *,
                    b.price,
                    b.adjust_recent_clean,
                    b.best_warehouse, b.warehouse,
                    b.totalCost,
                    GREATEST(b.price, 1) as post_price,
                    0 as std_shipping,
                    0 as std_add,
                    0 as trk_shipping,
                    0 as trk_add,
                    12.99 as reg_shipping,
                    12.99 as reg_add,
                    999 as fst_shipping,
                    999 as fst_add,
                    CASE
                    WHEN
                    price >= online_price
                    THEN
                    '涨价'
                    ELSE
                    '降价'
                    END
                    AS
                    'is_up', b.is_chargo,b.after_profit,b.day_sales
                    FROM
                    yibai_cdiscount_oversea_listing_price_bake
                    a
                    INNER
                    JOIN
                    oversea_adjust_platform_dtl_temp
                    b
                    ON
                    a.SKU = b.SKU
                    AND
                    b.platform = 'CDISCOUNT'
                    where
                    a.DATE = '{now_time}' and account_id >= 400"""
    conn.execute(sql_cd_8)
    print(37)
    while get_cd_temp_count() == 0:
        print('cd_temp表没数据，重新插入')
        sql_cd_3 = f"""TRUNCATE yibai_cdiscount_adjust_temp"""
        conn.execute(sql_cd_3)
        print(32)
        time.sleep(10)
        sql_cd_4 = f"""insert into yibai_cdiscount_adjust_temp
                            SELECT
                            DISTINCT
                            a. *,
                            b.price,
                            b.adjust_recent_clean,
                            b.best_warehouse, b.warehouse,
                            b.totalCost,
                            GREATEST(b.price, 1) as post_price,
                            0 as std_shipping,
                            0 as std_add,
                            0 as trk_shipping,
                            0 as trk_add,
                            12.99 as reg_shipping,
                            12.99 as reg_add,
                            999 as fst_shipping,
                            999 as fst_add,
                            CASE

                            WHEN
                            price >= online_price
                            THEN
                            '涨价'
                            ELSE
                            '降价'
                            END
                            AS
                            'is_up',
                            b.is_chargo,b.after_profit,b.day_sales
                            FROM
                            yibai_cdiscount_oversea_listing_price_bake
                            a
                            INNER
                            JOIN
                            oversea_adjust_platform_dtl_temp
                            b
                            ON
                            a.SKU = b.SKU
                            AND
                            b.platform = 'CDISCOUNT'
                            where
                            a.DATE = '{now_time}' and account_id < 100"""
        conn.execute(sql_cd_4)
        print(33)
        sql_cd_5 = f"""insert into yibai_cdiscount_adjust_temp
                            SELECT
                            DISTINCT
                            a. *,
                            b.price,
                            b.adjust_recent_clean,
                            b.best_warehouse, b.warehouse,
                            b.totalCost,
                            GREATEST(b.price, 1) as post_price,
                            0 as std_shipping,
                            0 as std_add,
                            0 as trk_shipping,
                            0 as trk_add,
                            12.99 as reg_shipping,
                            12.99 as reg_add,
                            999 as fst_shipping,
                            999 as fst_add,
                            CASE

                            WHEN
                            price >= online_price
                            THEN
                            '涨价'
                            ELSE
                            '降价'
                            END
                            AS
                            'is_up', b.is_chargo,b.after_profit,b.day_sales
                            FROM
                            yibai_cdiscount_oversea_listing_price_bake a
                            INNER
                            JOIN
                            oversea_adjust_platform_dtl_temp b
                            ON
                            a.SKU = b.SKU
                            AND
                            b.platform = 'CDISCOUNT'
                            where
                            a.DATE = '{now_time}' and account_id >= 100 and account_id < 200"""
        conn.execute(sql_cd_5)
        print(34)
        sql_cd_6 = f"""insert into yibai_cdiscount_adjust_temp
                            SELECT
                            DISTINCT
                            a. *,
                            b.price,
                            b.adjust_recent_clean,
                            b.best_warehouse, b.warehouse,
                            b.totalCost,
                            GREATEST(b.price, 1) as post_price,
                            0 as std_shipping,
                            0 as std_add,
                            0 as trk_shipping,
                            0 as trk_add,
                            12.99 as reg_shipping,
                            12.99 as reg_add,
                            999 as fst_shipping,
                            999 as fst_add,
                            CASE

                            WHEN
                            price >= online_price
                            THEN
                            '涨价'
                            ELSE
                            '降价'
                            END
                            AS
                            'is_up', b.is_chargo,b.after_profit,b.day_sales
                            FROM
                            yibai_cdiscount_oversea_listing_price_bake
                            a
                            INNER
                            JOIN
                            oversea_adjust_platform_dtl_temp
                            b
                            ON
                            a.SKU = b.SKU
                            AND
                            b.platform = 'CDISCOUNT'
                            where
                            a.DATE = '{now_time}' and account_id >= 200 and account_id < 300"""
        conn.execute(sql_cd_6)
        print(35)
        sql_cd_7 = f"""insert into yibai_cdiscount_adjust_temp
                            SELECT
                            DISTINCT
                            a. *,
                            b.price,
                            b.adjust_recent_clean,
                            b.best_warehouse, b.warehouse,
                            b.totalCost,
                            GREATEST(b.price, 1) as post_price,
                            0 as std_shipping,
                            0 as std_add,
                            0 as trk_shipping,
                            0 as trk_add,
                            12.99 as reg_shipping,
                            12.99 as reg_add,
                            999 as fst_shipping,
                            999 as fst_add,
                            CASE

                            WHEN
                            price >= online_price
                            THEN
                            '涨价'
                            ELSE
                            '降价'
                            END
                            AS
                            'is_up', b.is_chargo ,b.after_profit,b.day_sales
                            FROM
                            yibai_cdiscount_oversea_listing_price_bake
                            a
                            INNER
                            JOIN
                            oversea_adjust_platform_dtl_temp
                            b
                            ON
                            a.SKU = b.SKU
                            AND
                            b.platform = 'CDISCOUNT'
                            where
                            a.DATE = '{now_time}' and account_id >= 300 and account_id < 400"""
        conn.execute(sql_cd_7)
        print(36)
        sql_cd_8 = f"""insert into yibai_cdiscount_adjust_temp
                            SELECT
                            DISTINCT
                            a. *,
                            b.price,
                            b.adjust_recent_clean,
                            b.best_warehouse, b.warehouse,
                            b.totalCost,
                            GREATEST(b.price, 1) as post_price,
                            0 as std_shipping,
                            0 as std_add,
                            0 as trk_shipping,
                            0 as trk_add,
                            12.99 as reg_shipping,
                            12.99 as reg_add,
                            999 as fst_shipping,
                            999 as fst_add,
                            CASE

                            WHEN
                            price >= online_price
                            THEN
                            '涨价'
                            ELSE
                            '降价'
                            END
                            AS
                            'is_up', b.is_chargo ,b.after_profit,b.day_sales
                            FROM
                            yibai_cdiscount_oversea_listing_price_bake
                            a
                            INNER
                            JOIN
                            oversea_adjust_platform_dtl_temp
                            b
                            ON
                            a.SKU = b.SKU
                            AND
                            b.platform = 'CDISCOUNT'
                            where
                            a.DATE = '{now_time}' and account_id >= 400"""
        conn.execute(sql_cd_8)
        print('重新插入完成')

    sql_cd_9 = f"""delete from yibai_oversea_cd_adjust_log where DATE = '{now_time}'"""
    conn.execute(sql_cd_9)
    print(38)
    sql_cd_10 = f"""
                    select
                    A. *
                    from

                    (
                        SELECT
                        A.*, row_number()
                    over(partition
                    by
                    account_id, seller_sku
                    order
                    by
                    totalCost) as flag
                    FROM
                    yibai_cdiscount_adjust_temp
                    A )A
                    WHERE
                    (A.IS_UP = '涨价' AND A.adjust_recent_clean IN ('正利润加快动销', '负利润加快动销') and adjust_price - online_price >= 0.3 and flag = 1)
                    OR
                    (A.IS_UP = '涨价' AND A.adjust_recent_clean in ('正常', '回调')
                    AND
                    is_chargo = '否' and adjust_price - online_price >= 0.3 and flag = 1)
                    OR
                    (A.IS_UP = '涨价' AND A.adjust_recent_clean in ('正常', '回调')
                    AND
                    is_chargo = '是' and adjust_price - online_price >= 0.3 and flag = 1)
                    OR
                    (A.IS_UP
                     = '降价'AND A.adjust_recent_clean IN ( '清仓', '正利润加快动销', '负利润加快动销') and online_price - adjust_price >= 0.5 and flag = 1)
                    OR
                    (A.IS_UP = '降价' AND A.adjust_recent_clean in ('正常', '回调')
                    AND
                    is_chargo = '是' and online_price - adjust_price >= 0.5 and flag = 1)
                    OR
                    (A.IS_UP = '降价' AND A.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and day_sales < 0.1 and online_price- adjust_price>=0.5 and flag = 1)
"""
    df_sql_cd_10 = conn.read_sql(sql_cd_10)
    df_sql_cd_10 = df_sql_cd_10.rename(columns={'adjust_price':'price'})
    conn.to_sql(df_sql_cd_10, table="yibai_oversea_cd_adjust_log", if_exists="append")
    print(39)
    print(get_cd_log_count)
    # while get_cd_log_count == 0:
    #     time.sleep(10)
    #     sql_cd_12 = f"""
    #                             select
    #                             A. *
    #                             from

    #                             (
    #                                 SELECT
    #                                 A.*, row_number()
    #                             over(partition
    #                             by
    #                             account_id, seller_sku
    #                             order
    #                             by
    #                             totalCost) as flag
    #                             FROM
    #                             yibai_cdiscount_adjust_temp
    #                             A )A
    #                             WHERE
    #                             (A.IS_UP = '涨价' AND A.adjust_recent_clean IN ('正利润加快动销', '负利润加快动销') and adjust_price - online_price >= 0.3 and flag = 1)
    #                             OR
    #                             (A.IS_UP = '涨价' AND A.adjust_recent_clean in ('正常', '回调')
    #                             AND
    #                             is_chargo = '否' and adjust_price - online_price >= 0.3 and flag = 1)
    #                             OR
    #                             (A.IS_UP = '涨价' AND A.adjust_recent_clean in ('正常', '回调')
    #                             AND
    #                             is_chargo = '是' and adjust_price - online_price >= 0.3 and flag = 1)
    #                             OR
    #                             (A.IS_UP
    #                              = '降价'AND A.adjust_recent_clean IN ( '清仓', '正利润加快动销', '负利润加快动销') and online_price - adjust_price >= 0.5 and flag = 1)
    #                             OR
    #                             (A.IS_UP = '降价' AND A.adjust_recent_clean in ('正常', '回调')
    #                             AND
    #                             is_chargo = '是' and online_price - adjust_price >= 0.5 and flag = 1)
    #                             OR
    #                 (A.IS_UP = '降价' AND A.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and day_sales < 0.1 and online_price- price>=0.5 and flag = 1)"""
    #     df_sql_cd_10 = conn.read_sql(sql_cd_10)
    #     conn.to_sql(df_sql_cd_10, table="yibai_oversea_cd_adjust_log", if_exists="append")
    #     print('数据库中cd_log没数据，重新插入数据')
    conn.close()
    send_msg('动销组定时任务推送', 'listing调价链接',
             f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓cd调价链接更新完成",
             is_all=False)


def cd_link_price_adjustment():
    try:
        cd()
    except Exception as e:
        print(str(e))
        try:
            time.sleep(60)
            cd()
        except:
            send_msg('动销组定时任务推送', 'listing调价链接',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓cd调价链接处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                     status='失败')
            raise Exception(traceback.format_exc())


def eb():
    now_time = time.strftime('%Y-%m-%d')
    print(now_time)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql_ebay_1 = f"""TRUNCATE yibai_ebay_oversea_listing_price_bake"""
    conn.execute(sql_ebay_1)
    print(10)
    sql_ebay_2 = f"""insert into yibai_ebay_oversea_listing_price_bake
                    select * from yibai_ebay_oversea_listing_price WHERE DATE = '{now_time}'"""
    conn.execute(sql_ebay_2)
    print(11)
    sql_ebay_3 = f"""drop table oversea_adjust_platform_dtl_temp"""
    conn.execute(sql_ebay_3)
    print(12)
    sql_ebay_4 = f"""create table oversea_adjust_platform_dtl_temp like oversea_adjust_platform_dtl"""
    conn.execute(sql_ebay_4)
    print(13)
    sql_ebay_5 = f"""insert into oversea_adjust_platform_dtl_temp select * from oversea_adjust_platform_dtl where DATE = '{now_time}'"""
    conn.execute(sql_ebay_5)
    print(14)
    sql_xiaohui_delete = f"""delete from oversea_daily_destruction_detail where date='{datetime.date.today()}'"""
    conn.execute(sql_xiaohui_delete)
    # 20220429增加每天的销毁明细写入
    print(151)
    sql_xiaohui = f"""insert into oversea_daily_destruction_detail 
        select distinct a.sku,b.warehouse,a.country,a.is_distory,b.is_desctruction,a.date from oversea_adjust_platform_dtl_temp a
        inner join oversea_predict_acost b on a.sku=b.sku and a.warehouse=b.warehouse and b.date='{datetime.date.today()}'
        where a.is_distory='已达到销毁价格' and a.date='{datetime.date.today()}' and b.is_desctruction='是'
        and ((a.warehouse='英国仓' and a.country='UK') or (a.warehouse='德国仓' and a.country='DE')
        or (a.warehouse='法国仓' and a.country='FR') or (a.warehouse='西班牙' and a.country='ES')
        or (a.warehouse='意大利仓' and a.country='IT') or (a.warehouse='美国' and a.country='US')
        or (a.warehouse='加拿大仓' and a.country='CA') or (a.warehouse='澳洲仓' and a.country='AU')
        or (a.warehouse='日本仓' and a.country='JP'))
        """
    conn.execute(sql_xiaohui)
    print(152)

    sql_ebay_6 = f"""delete from yibai_oversea_eb_adjust_log where DATE='{now_time}'"""
    conn.execute(sql_ebay_6)
    print(15)
    # 取消log结果的阶梯定价,在最终执行程序体现
    sql_ebay_7 = f"""
                    SELECT
                      A1.*,
                        CASE WHEN IS_UP='涨价' THEN GREATEST(ROUND(price+0.01*flag-shipping_fee,2),1)
                        when IS_UP='降价' then  GREATEST(ROUND(price+0.01*flag-shipping_fee,2),1) else null end as 'post_price'
                    FROM
                      (
                        SELECT
                          A.*,
                        IF (@prev <> CONCAT(A.sku, A.`name`) ,@curRank := 1 ,@curRank := @curRank + 1) AS flag ,
                            @prev := CONCAT(A.sku, A.`name`) as '变量'
                        FROM
                          (
                            SELECT DISTINCT
                              a.*, b.price,
                              b.adjust_recent_clean,
                              b.totalCost AS '最优运费总支出',
                              CASE
                            WHEN b.price >= online_price THEN
                              '涨价'
                            ELSE
                              '降价'
                            END AS 'IS_UP',
                            profit_rate as acc_profit,
                             b.is_chargo,b.warehouse_id,b.best_warehouse,b.day_sales
                          FROM
                            yibai_ebay_oversea_listing_price_bake a
                          LEFT JOIN oversea_adjust_platform_dtl_temp b ON a.sku = b.sku
                          AND a. NAME = b.warehouse
                          AND a.country = b.country
                          AND b.platform = 'EB'
                                    ORDER BY b.totalCost
                          ) A ,  (SELECT @currentRank := 0) r,
                      (SELECT @prev := '') pr
                        ORDER BY
                          A.sku,
                          A.`name` ASC
                      ) A1

                    WHERE
                    (A1.IS_UP = '涨价' AND A1.adjust_recent_clean IN ('正利润加快动销','负利润加快动销' )and price- online_price>=0.3)
                    OR
                    (A1.IS_UP = '涨价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and price- online_price>=0.3)
                    OR
                    (A1.IS_UP = '降价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='是' and online_price- price>=0.3)
                    OR
                    (A1.IS_UP = '降价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and day_sales < 0.1 and online_price- price>=0.3 )
                    OR
                    (A1.IS_UP = '涨价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='是' and price- online_price>=0.3)
                    OR
                    (A1.IS_UP = '降价'AND A1.adjust_recent_clean IN ( '清仓', '正利润加快动销','负利润加快动销') and online_price- price>=0.3)"""
    df_sql_ebay_7 = conn.read_sql(sql_ebay_7)
    print(df_sql_ebay_7.info())
    conn.to_sql(df_sql_ebay_7, table="yibai_oversea_eb_adjust_log", if_exists="append")
    conn.close()
    send_msg('动销组定时任务推送', 'listing调价链接',
             f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓ebay调价链接更新完成",
             is_all=False)


def eb_link_price_adjustment():
    try:
        eb()
    except Exception as e:
        print(str(e))
        try:
            time.sleep(60)
            eb()
        except:
            send_msg('动销组定时任务推送', 'listing调价链接',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ebay调价链接处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                     status='失败')
            raise Exception(traceback.format_exc())


def walmart():
    now_time = time.strftime('%Y-%m-%d')
    print(now_time)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #
    sql_walmart_1 = f"""TRUNCATE yibai_walmart_oversea_listing_price_bake"""
    conn.execute(sql_walmart_1)
    print(40)
    sql_walmart_2 = f"""insert into yibai_walmart_oversea_listing_price_bake
                    select * from yibai_walmart_oversea_listing_price where DATE = '{now_time}'"""
    conn.execute(sql_walmart_2)
    print(41)
    sql_walmart_3 = f"""TRUNCATE yibai_walmart_adjust_temp"""
    conn.execute(sql_walmart_3)
    print(42)
    sql_walmart_4 = f"""insert into yibai_walmart_adjust_temp
                    SELECT
                    DISTINCT
                    a. *, b.price as post_price,
                    b.adjust_recent_clean,
                    b.available_stock,
                    b.best_warehouse as '最优子仓',
                    b.warehouse,                    
                    b.totalCost AS '最优运费总支出',
                    CASE
                    WHEN
                    b.price >= a.price
                    THEN
                    '涨价'
                    ELSE
                    '降价'
                    END
                    AS
                    'is_up',
                    profit_rate * 100, is_chargo,b.day_sales
                    FROM
                    yibai_walmart_oversea_listing_price_bake
                    a
                    INNER
                    JOIN
                    oversea_adjust_platform_dtl_temp
                    b
                    ON
                    a.sku = b.sku
                    AND
                    a.site = b.country
                    AND
                    b.platform = 'WALMART'
                    WHERE
                    a.DATE = '{now_time}'
                    ORDER
                    BY
                    b.totalCost"""
    conn.execute(sql_walmart_4)
    print(43)
    sql_walmart_5 = f"""delete from yibai_oversea_walmart_adjust_log where DATE = '{now_time}'"""
    conn.execute(sql_walmart_5)
    print(44)
    sql_walmart_6 = f"""
                    select * from (
                    SELECT
                      A1.*,
                        B.`status` AS 'is_adjust_flag',
                        CASE WHEN IS_UP='涨价' THEN GREATEST(ROUND(post_price,2),1)
                        when IS_UP='降价' then  GREATEST(ROUND(post_price,2),1) else null end as '上传调价'
                    FROM
                      (
                        SELECT
                     A.*,if(@prev <> CONCAT(A.sku, A.`site`),@curRank:=1,mod(@curRank := @curRank + 1,50)) as flag,
					    @prev := CONCAT(A.sku, A.`site`) as '变量'
                        FROM
                      yibai_walmart_adjust_temp A ,  (SELECT @currentRank := 0) r,
                      (SELECT @prev := '') pr
                        ORDER BY
                          A.sku,
                          A.`site` ASC
                      ) A1
                        left join (
                            select b.account_id as account_id,a.seller_sku as seller_sku, 1 as is_white_listing
                            from (
                                select distinct account_id,seller_sku 
                                from yibai_sale_center_listing_sync.yibai_walmart_change_price_lock
                                where status = 1 and toUnixTimestamp(now()) <deadline_time
                            ) a
                            inner join (
                                select * from yibai_sale_center_system_sync.yibai_system_account
                                where platform_code = 'WALMART' AND `status` = 1 AND is_del =0
                            ) b 
                            on a.account_id=b.id
                        ) B 
                        ON A1.account_id=B.account_id AND A1.seller_sku=B.seller_sku
                    WHERE
                      A1.adjust_recent_clean IS NOT NULL
                    AND (

                    (A1.IS_UP = '涨价' AND A1.adjust_recent_clean IN ('正利润加快动销','负利润加快动销' )and post_price-price>=0.3 )
                    OR
                    (A1.IS_UP = '涨价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and post_price-price>=0.3 )
                    OR
                    (A1.IS_UP = '降价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='是' and price- post_price>=0.3 )
                    OR
                    (A1.IS_UP = '降价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='否' and day_sales < 0.1 and price- post_price>=0.3)
                    OR
                    (A1.IS_UP = '涨价' AND A1.adjust_recent_clean in ('正常','回调') AND is_chargo='是' and post_price-price>=0.3 )
                    OR
                    (A1.IS_UP = '降价'AND A1.adjust_recent_clean IN ( '清仓', '正利润加快动销','负利润加快动销') and price- post_price>=0.3 )

                        )  and available_stock>4)a
                        where is_adjust_flag is null"""
    df_sql_walmart_6 = conn.read_sql(sql_walmart_6)                    
    conn.to_sql(df_sql_walmart_6, table="yibai_oversea_walmart_adjust_log", if_exists="append")
    #
    conn.close()
    send_msg('动销组定时任务推送', 'listing调价链接',
             f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓walmart调价链接更新完成",
             is_all=False)


def walmart_link_price_adjustment():
    try:
        walmart()
    except Exception as e:
        print(str(e))
        try:
            time.sleep(60)
            walmart()
        except:
            send_msg('动销组定时任务推送', 'listing调价链接',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓walmart调价链接处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                     status='失败')
            raise Exception(traceback.format_exc())


def insert_yibai_oversea_adjust_number_data():
    try:
        now_time = time.strftime('%Y-%m-%d')
        print(now_time)
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        print(1)
        print(get_now_weekday())
        if get_now_weekday() == 1 or get_now_weekday() == 4:
            sql_insert_amazon = f"""
                select DATE as date,'AMAZON' AS platform, count(1) as num 
                from yibai_oversea_amazon_adjust_log 
                WHERE DATE = '{now_time}'
                group by DATE
            """
            df_amazon = conn.read_sql(sql_insert_amazon)
            adjust_post(platform='AMAZON', item='海外仓调价', number=df_amazon['num'][0])
            print(2)

            sql_insert_ebay = f"""
                select DATE as date,'EB' AS platform, count(1) AS num
                from yibai_oversea_eb_adjust_log 
                WHERE DATE = '{now_time}'
                group by DATE
            """
            df_eb = conn.read_sql(sql_insert_ebay)
            adjust_post(platform='EB', item='海外仓调价', number=df_eb['num'][0])
            print(3)
            sql_insert_cd = f"""
                select DATE as date, 'CD' AS platform, count(1) as num
                from yibai_oversea_cd_adjust_log 
                WHERE DATE = '{now_time}'
                group by DATE
            """
            df_cd = conn.read_sql(sql_insert_cd)
            adjust_post(platform='CD', item='海外仓调价', number=df_cd['num'][0])
            print(4)
            sql_insert_walmart = f"""
                select DATE as date, 'WALMART' AS platform, count(1) as num
                from yibai_oversea_walmart_adjust_log 
                WHERE DATE = '{now_time}'
                group by DATE
            """
            df_walmart = conn.read_sql(sql_insert_walmart)
            adjust_post(platform='WALMART', item='海外仓调价', number=df_walmart['num'][0])
            print(5)
            sql_insert_ali = f"""
                select DATE as date, 'ALI' AS platform, count(1) as num
                from yibai_oversea_ali_adjust_log 
                WHERE DATE = '{now_time}'
                group by DATE
            """
            df_ali = conn.read_sql(sql_insert_ali)
            adjust_post(platform='ALI', item='海外仓调价', number=df_ali['num'][0])
        else:
            sql_insert_amazon = f"""
                select DATE as date, 'AMAZON' AS platform, count(1) as num
                from yibai_oversea_amazon_adjust_log 
                WHERE DATE = '{now_time}' and IS_UP='涨价'
                group by DATE
            """
            df_amazon = conn.read_sql(sql_insert_amazon)
            adjust_post(platform='AMAZON', item='海外仓调价', number=df_amazon['num'][0])
            print(2)
            sql_insert_ebay = f"""
                select DATE as date,'EB' AS platform,count(1) as num
                from yibai_oversea_eb_adjust_log 
                WHERE DATE = '{now_time}' and IS_UP='涨价'
                group by DATE
            """
            df_eb = conn.read_sql(sql_insert_ebay)
            adjust_post(platform='EB', item='海外仓调价', number=df_eb['num'][0])
            print(3)
            sql_insert_cd = f"""
                select DATE as date,'CD' AS platform,count(1) as num 
                from yibai_oversea_cd_adjust_log 
                WHERE DATE = '{now_time}' and IS_UP='涨价'
                group by DATE
            """
            df_cd = conn.read_sql(sql_insert_cd)
            adjust_post(platform='CD', item='海外仓调价', number=df_cd['num'][0])
            print(4)
            sql_insert_walmart = f"""
                select DATE as date,'WALMART' AS platform,count(1) as num
                from yibai_oversea_walmart_adjust_log 
                WHERE DATE = '{now_time}' and IS_UP='涨价'
                group by DATE
            """
            df_walmart = conn.read_sql(sql_insert_walmart)
            adjust_post(platform='WALMART', item='海外仓调价', number=df_walmart['num'][0])
            print(5)
            sql_insert_ali = f"""
                select DATE as date, 'ALI' AS platform, count(1) as num
                from yibai_oversea_ali_adjust_log 
                WHERE DATE = '{now_time}' and IS_UP='涨价'
                group by DATE
            """
            df_ali = conn.read_sql(sql_insert_ali)
            adjust_post(platform='ALI', item='海外仓调价', number=df_ali['num'][0])
        conn.close()
        send_msg('动销组定时任务推送', 'listing调价链接',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓插入各平台adjust_log数据完成",
                 is_all=False)
    except:
        send_msg('动销组定时任务推送', 'listing调价链接',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓插入各平台adjust_log数据处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


if __name__ == '__main__':
    insert_oversea_adjust_platform_dtl()
