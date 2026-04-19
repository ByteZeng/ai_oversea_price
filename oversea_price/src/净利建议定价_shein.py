import time
##
import warnings
from dateutil.relativedelta import relativedelta
from utils import utils
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, \
    is_df_exist, read_df, make_path, get_path
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
import datetime
import pandas as pd
import numpy as np
warnings.filterwarnings('ignore')
import src.fetch_data as fd

def main():
    """
    Ebay “FBA正负利润加快动销最新库存”的数据 还需要出一份“美英x德净利建议定价”
    ebay举例：
    0.0747 销毁价综合系数
    10% 是平台佣金 （前三个月免费）后面10%收取
    0.16 是毛净利差值  新平台 （盲拍）
    0.05 定价综合系数

    之前美国FBA转寄shein5%净利率定价的公式：
    以下值取两位小数然后减0.01
    销毁价：
    fba多渠道运费/(1-平台佣金-0.0747-0.03汇损折旧)
    sku维度：
    (成本+fba多渠道运费*汇率+头程)/(1-10%平台佣金-16%毛净利差值-5%目标净利率-3%汇损折旧)/rate
    seller_sku维度：
    (成本+fba多渠道运费*汇率+头程+当前价格*当前净利率*汇率)/(1-平台佣金-毛净利差值-5%目标净利率-0.03汇损折旧)/rate
    """

    utils.program_name = '净利建议定价_shein'
    make_path()
    cur_path, root_path = get_path()
    date_today = datetime.date.today()
    # date_today = (datetime.date.today() - datetime.timedelta(days=1))
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'

    is_high_season = int((today >= '20231015') & (today <= '20240114'))

    client = get_ck_client(user='zengzhijie', password='ze65nG_zHij5ie')

    # 一级产品线
    sql_line = f"""
            select a.sku sku, b.path_name as `产品线路线` from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    print('一级产品线提取完成')

    p_fee = 0.1
    platform_zero = 0.16
    # 美国
    sql1 = f"""
        select a.*,            
            --ceil(fba_to_other_fees/(1-0.15-0.03-0.0747),1)-0.01 as "lowest_price",      
            ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747),1)-0.01 as "shein_lowest_price",      
            -- ceil(fba_to_other_fees/(1-0.1-0.03-0.0747),1)-0.01 as "wish_lowest_price",  

            --ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013)/rate,1)-0.01 as  "ebay_price_sku",  
            ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03)/rate,1)-0.01 as  "shein_price_sku",   
            -- ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03)/rate,1)-0.01 as "walmart_price_sku",      
            -- ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01 as  "wish_price_sku",          

            --GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku",
            GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03)/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku" 
            -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03)/rate,1)-0.01,toFloat64(lowest_price)) as "walmart_price_seller_sku",      
            -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"      
        from 
            (      
                select  A.account_id account_id,
                        A.site site,
                        B.short_name short_name,
                        A.asin asin,
                        A.seller_sku seller_sku,
                        A.Current_price as "当前售价",      
                        A.sku sku,  
                        A.group_name group_name,      
                        A.area area,      
                        A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",      
                        m.afn_fulfillable_quantity as "当前库存数量",      m.afn_fulfillable_quantity*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",      
                        A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",      
                        A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",      
                        A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司", 
                        C.toucheng_kongyun, C.toucheng_haiyun,   
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
                        else null end as "fba_to_other_fees",     
                        -- fba_to_other_fees 多渠道配送费用 
                        C.size_segment       
                        from support_document.fba_clear_seller_sku_{today} A 
                        LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku     
                        LEFT JOIN  yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)      
                        left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku 
                        where available_stock>0 and  A.site='美国' 
                        ORDER BY available_stock DESC
                        LIMIT 1 BY sku 
            )a

    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df1 = conn_ck.ck_select_to_df(sql1)
    print('美国站数据计算完成！')

    # 德国法国西班牙波兰
    # 暂定：Shein欧洲站点平台VAT是20%，佣金10%
    # 1121:暂时只需要美国+法国、日销>0、非负利润加快动销
    p_fee = 0.1
    vat_fee = 0.2
    sql2 = f"""
    select
        a.*,
        ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    from
        (
            select
                A.account_id account_id,
                A.site site,
                B.short_name short_name,
                A.asin asin,
                A.seller_sku seller_sku,
                A.Current_price as "当前售价",
                A.sku sku,
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 7
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 7.2
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 7.45
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 7.65
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 8.25
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 8.43
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 8.98
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.55
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.11
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 10.68
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.24
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 13.62
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 9.32
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.89
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.45
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 11.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.58
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 12.80
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 13.96
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 15.21
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 16.67
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 17.11
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 18.72
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 19.44
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 20.60
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 21.76
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 22.93
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 14.72
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 15.63
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 15.95
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 17.29
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 18.00
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 18.75
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 19.38
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 19.91
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 20.20
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 21.99
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 23.10
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 24.87
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 26.21
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 27.35
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 28.82
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 30.81
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 35.10
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 42.47
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 47.16
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.57*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 26.38
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 34.58
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 36.97
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 42.12
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 50.96
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 56.59
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.8*(C.weight/1000)
                        else null end as "初始收费",
                0 as "初续重收费",
                `初始收费`+`初续重收费` as "fba_to_other_fees", C.size_segment
                from support_document.fba_clear_seller_sku_{today} A
                LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
                LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                where available_stock>0 and Average_daily_sales>0 and A.site='法国'
        )a
    union all
    select
        a.*,
        ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
        -- ceil(fba_to_other_fees/(1-0.1-0.03-0.0747-0.167),1)-0.01 as "wish_lowest_price",

        -- '无' as "walmart_price_sku",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
        -- ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01 as  "wish_price_sku",

        -- '无' as "walmart_price_seller_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    from
        (
            select
                A.account_id account_id,
                A.site site,
                B.short_name short_name,
                A.asin asin,
                A.seller_sku seller_sku,
                A.Current_price as "当前售价",
                A.sku sku,
                A.group_name group_name,
                A.area area,
                A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
                A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 7.45
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 7.65
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 7.90
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 8.10
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 8.23
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 8.45
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 8.64
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 8.91
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 9.24
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 9.56
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 10.19
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 12.09
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 8.85
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.24
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 9.89
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 10.29
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.43
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 12.26
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 13.08
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 14.70
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 16.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 18.25
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 19.01
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 19.75
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 20.32
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 21.88
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 24.23
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 15.64
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 16.20
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 16.81
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 17.40
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 18.15
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 19.77
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 21.35
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 22.40
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 23.11
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 24.07
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 26.12
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 26.99
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 27.87
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 30.67
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 32.48
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 36.29
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 39.76
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 46.70
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 50.54
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.68*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 27.73
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 35.72
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 39.92
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 43.73
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 51.37
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 55.60
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.79*(C.weight/1000)
                    else null end as "初始收费",
                0 as "初续重收费",
                `初始收费`+`初续重收费` as "fba_to_other_fees",  C.size_segment
                from support_document.fba_clear_seller_sku_{today} A
                LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                where available_stock>0 and Average_daily_sales>0 and A.site='西班牙'
        )a
    union all
    select
        a.*,
        ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-0.013-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    from
        (
            select
                A.account_id account_id,
                A.site site,
                B.short_name short_name,
                A.asin asin,
                A.seller_sku seller_sku,
                A.Current_price as "当前售价",
                A.sku sku,
                A.group_name group_name,
                A.area area,
                A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
                A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 4.95
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 5.03
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 5.19
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 5.29
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 5.61
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 5.81
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 5.93
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 6.50
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.32
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.19
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.27
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 11.33
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 6.77
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 7.04
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.66
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.53
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.61
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 10.63
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 11.67
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 11.76
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 12.60
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 13.34
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 14.37
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 15.11
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 16.14
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 17.18
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 18.72
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 12.48
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 12.84
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 13.21
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 13.57
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 14.23
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 14.89
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 15.50
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 16.11
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 16.72
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 17.36
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 17.98
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 18.59
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 19.20
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 19.81
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 20.42
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 23.48
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 26.55
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 32.30
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 39.87
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.34*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 30.13
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 32.91
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 35.69
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 44.72
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 51.82
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 60.87
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.94*(C.weight/1000)
                    else null end as "初始收费",
                0 as "初续重收费",
                `初始收费`+`初续重收费` as "fba_to_other_fees",   C.size_segment
                from support_document.fba_clear_seller_sku_{today} A
                LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                where available_stock>0 and Average_daily_sales>0 and A.site = '波兰'
        )a

    union all

    select
        a.*,
        ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.1597)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    from
        (
          select
              A.account_id account_id,
              A.site site,
              B.short_name short_name,
              A.asin asin,
              A.seller_sku seller_sku,
              A.Current_price as "当前售价",
              A.sku sku,
              A.group_name group_name,
              A.area area,
              A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
              A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
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
                    `初始收费`+`初续重收费` as "fba_to_other_fees",  C.size_segment
                    from support_document.fba_clear_seller_sku_{today} A
                    LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                    left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                    where available_stock>0 and Average_daily_sales>0 and A.site='德国'
        )a

    union all
    select
        a.*,
        ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.1597)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    from
        (
          select
              A.account_id account_id,
              A.site site,
              B.short_name short_name,
              A.asin asin,
              A.seller_sku seller_sku,
              A.Current_price as "当前售价",
              A.sku sku,
              A.group_name group_name,
              A.area area,
              A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
              A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 6.72
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 6.97
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 7.02
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 7.17
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 7.19
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 7.37
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 7.47
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 7.99
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 8.52
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 9.05
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.89
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 12.65
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 8.80
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.49
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.07
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 10.79
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.28
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 12.39
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 13.50
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 15.26
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 16.68
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 18.44
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 20.20
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 21.96
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 23.72
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 25.48
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 27.24
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 14.45
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 15.99
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 16.42
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 16.81
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 17.52
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 18.86
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 19.30
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 20.37
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 21.43
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 22.09
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 24.93
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 24.61
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 26.04
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 27.18
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 27.98
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 32.80
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 34.45
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 38.90
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 44.90
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.5*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 27.22
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 31.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 37.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 43.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 49.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 55.70
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.77*(C.weight/1000)
                    else null end as "初始收费",
                    0 as "初续重收费",
                    `初始收费`+`初续重收费` as "fba_to_other_fees",  C.size_segment
                    from support_document.fba_clear_seller_sku_{today} A
                    LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                    left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                    where available_stock>0 and Average_daily_sales>0 and A.site='意大利'
        )a

    union all
    select
        a.*,
        ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.1597)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"
        -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    from
        (
          select
              A.account_id account_id,
              A.site site,
              B.short_name short_name,
              A.asin asin,
              A.seller_sku seller_sku,
              A.Current_price as "当前售价",
              A.sku sku,
              A.group_name group_name,
              A.area area,
              A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
              A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 8.28
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 8.42
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 8.77
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 8.77
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 8.87
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 9.07
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 9.66
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 10.07
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.76
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 12.56
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 13.36
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 17.20
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 10.16
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 10.84
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 11.70
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 12.81
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 15.14
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 16.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 17.25
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 18.60
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 20.33
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 21.68
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 22.82
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 24.22
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 26.67
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 27.03
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 28.44
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 21.87
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 22.08
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 22.49
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 22.56
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 22.63
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 22.49
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 22.70
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 22.70
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 23.89
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 24.43
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 25.33
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 26.23
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 27.13
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 30.50
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 31.50
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 33.54
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 38.66
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 47.27
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 61.01
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.68*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 34.95
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 35.81
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 47.80
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 53.50
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 60.17
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 76.10
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.79*(C.weight/1000)
                    else null end as "初始收费",
                    0 as "初续重收费",
                    `初始收费`+`初续重收费` as "fba_to_other_fees",  C.size_segment
                    from support_document.fba_clear_seller_sku_{today} A
                    LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
                    left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
                    where available_stock>0 and Average_daily_sales>0 and A.site in ('葡萄牙','瑞典')
        )a
    settings max_memory_usage = 20000000000
    """
    # """
    # select
    #     a.*,
    #     ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
    #     -- ceil(fba_to_other_fees/(1-0.1-0.03-0.0747-0.1597),1)-0.01 as "wish_lowest_price",
    #     -- '无' as "walmart_price_sku",
    #     ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-0.16-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
    #     -- ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.1597)/rate,1)-0.01 as  "ebay_price_sku",
    #     -- ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01 as  "wish_price_sku",
    #     -- '无' as "walmart_price_seller_sku",
    #     GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-0.16-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
    #     -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.15-0.16-0.05-0.03-0.013-0.1597)/rate,1)-0.01,toFloat64(lowest_price)) as  "ebay_price_seller_sku"
    #     -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    # from
    #     (
    #       select
    #           A.account_id account_id,
    #           A.site site,
    #           B.short_name short_name,
    #           A.asin asin,
    #           A.seller_sku seller_sku,
    #           A.Current_price as "当前售价",
    #           A.sku sku,
    #           D.title_cn title_cn,
    #           A.group_name group_name,
    #           A.area area,
    #           A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
    #           A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
    #           A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",
    #           A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",
    #           A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司",
    #                 case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
    #                         when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight > 29760 then '标准大件 > 29.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight > 31500 then '大号大件：> 31.5 千克'
    #                         else null end as "尺寸",
    #                 case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 5.09
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 5.29
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 5.45
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 5.55
    #                         when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 5.87
    #                         when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 5.87
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  0 and 150 then 6.19
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  150.0001 and 400 then 6.76
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.58
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.45
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.53
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 10.55
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 11.59
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 12.02
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 12.86
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 13.6
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 14.63
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 15.37
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 16.4
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 17.44
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 18.98
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 12.74
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 13.1
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 13.47
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 13.83
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 14.49
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 15.15
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 15.76
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 16.37
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 16.98
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 17.62
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 18.24
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 18.85
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 19.46
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 20.07
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 20.68
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 23.74
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 26.81
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 29.87
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 33.33
    #                         when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.11*(C.weight/1000)
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 30.39
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 33.17
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 35.95
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 38.72
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 41.5
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 45.78
    #                         when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.45*(C.weight/1000)
    #                         else null end as "初始收费",
    #                 0 as "初续重收费",
    #                 `初始收费`+`初续重收费` as "fba_to_other_fees",      'DE' as "price_country",      C.size_segment
    #                 from support_document.fba_clear_seller_sku_{today} A
    #                 LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
    #                 left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
    #                 LEFT JOIN  (
    #                     select distinct sku, title_cn
    #                     from yibai_prod_base_sync.yibai_prod_sku
    #                     ) D ON A.sku=D.sku
    #                 where available_stock>0  and A.site='德国'
    #     )a
    #     union all
    # """
    # """
    #     union all
    #                 select
    #         a.*,
    #         ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
    #         -- ceil(fba_to_other_fees/(1-0.1-0.03-0.0747-0.167),1)-0.01 as "wish_lowest_price",
    #
    #         -- '无' as "walmart_price_sku",
    #         ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-0.16-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
    #         -- ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01 as  "wish_price_sku",
    #
    #         -- '无' as "walmart_price_seller_sku",
    #         GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-0.16-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
    #         -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    #     from
    #         (
    #             select
    #                 A.account_id account_id,
    #                 A.site site,
    #                 B.short_name short_name,
    #                 A.asin asin,
    #                 A.seller_sku seller_sku,
    #                 A.Current_price as "当前售价",
    #                 A.sku sku,
    #                 D.title_cn title_cn,
    #                 A.group_name group_name,
    #                 A.area area,
    #                 A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
    #                 A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
    #                 A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",
    #                 A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",
    #                 A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司",
    #                 case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
    #                         when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight > 29760 then '标准大件 > 29.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight > 31500 then '大号大件：> 31.5 千克'
    #                         else null end as "尺寸",
    #                 case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 7.45
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 7.65
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 7.9
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 8.1
    #                         when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 8.35
    #                         when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 8.35
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  0 and 150 then 8.64
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  150.0001 and 400 then 8.91
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  400.0001 and 900 then 9.24
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 9.56
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.89
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 10.86
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 11.92
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 13.09
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 14.45
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 15.82
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 17.39
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 18.75
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 20.32
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 21.88
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 23.45
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 14.84
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 15.48
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 16.11
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 16.74
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 17.37
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 19.57
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 20.58
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 21.6
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 22.61
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 23.62
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 24.63
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 25.64
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 26.65
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 27.67
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 28.68
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 31.18
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 33.68
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 38.68
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 43.68
    #                         when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.46*(C.weight/1000)
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 26.62
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 29.62
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 33.74
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 38.74
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 43.74
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 48.74
    #                         when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.55*(C.weight/1000)
    #                         else null end as "初始收费",
    #                 0 as "初续重收费",
    #                 `初始收费`+`初续重收费` as "fba_to_other_fees",      'es' as "price_country",  C.size_segment
    #                 from support_document.fba_clear_seller_sku_{today} A
    #                 LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
    #                 left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
    #                 LEFT JOIN  (
    #                     select distinct sku, title_cn
    #                     from yibai_prod_base_sync.yibai_prod_sku
    #                     ) D ON A.sku=D.sku
    #                 where available_stock>0  and A.site='西班牙'
    #         )a
    # union all
    #         select
    #         a.*,
    #         ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
    #         -- ceil(fba_to_other_fees/(1-0.1-0.03-0.0747-0.167),1)-0.01 as "wish_lowest_price",
    #
    #         -- '无' as "walmart_price_sku",
    #         ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-0.16-0.05-0.03-0.013-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
    #         -- ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01 as  "wish_price_sku",
    #
    #         -- '无' as "walmart_price_seller_sku",
    #         GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-0.16-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
    #         -- GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-0.1-0.13-0.05-0.03)/rate,1)-0.01,toFloat64(wish_lowest_price)) as  "wish_price_seller_sku"
    #     from
    #         (
    #             select
    #                 A.account_id account_id,
    #                 A.site site,
    #                 B.short_name short_name,
    #                 A.asin asin,
    #                 A.seller_sku seller_sku,
    #                 A.Current_price as "当前售价",
    #                 A.sku sku,
    #                 D.title_cn title_cn,
    #                 A.group_name group_name,
    #                 A.area area,
    #                 A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
    #                 A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
    #                 A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",
    #                 A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",
    #                 A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",      C.weight,      C.weight/28.34 AS "盎司",
    #                 case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
    #                         when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克'
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
    #                         when C.size_segment like '%%标准大件%%' and C.weight > 29760 then '标准大件 > 29.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
    #                         when C.size_segment like '%%大号大件%%' and C.weight > 31500 then '大号大件：> 31.5 千克'
    #                         else null end as "尺寸",
    #                 case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 4.83
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 5.03
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 5.19
    #                         when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 5.29
    #                         when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 5.61
    #                         when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 5.61
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  0 and 150 then 5.93
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  150.0001 and 400 then 6.5
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.32
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.19
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.27
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 10.29
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 11.33
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 11.76
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 12.6
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 13.34
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 14.37
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 15.11
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 16.14
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 17.18
    #                         when C.size_segment like '%%包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 18.72
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 12.48
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 12.84
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 13.21
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 13.57
    #                         when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 14.23
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 14.89
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 15.5
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 16.11
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 16.72
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 17.36
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 17.98
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 18.59
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 19.2
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 19.81
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 20.42
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 23.48
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 26.55
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 29.61
    #                         when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 33.07
    #                         when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.11*(C.weight/1000)
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 30.13
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 32.91
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 35.69
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 38.46
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 41.24
    #                         when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 45.52
    #                         when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.45*(C.weight/1000)
    #                         else null end as "初始收费",
    #                 0 as "初续重收费",
    #                 `初始收费`+`初续重收费` as "fba_to_other_fees",      'pl' as "price_country",  C.size_segment
    #                 from support_document.fba_clear_seller_sku_{today} A
    #                 LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
    #                 left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
    #                 LEFT JOIN  (
    #                     select distinct sku, title_cn
    #                     from yibai_prod_base_sync.yibai_prod_sku
    #                     ) D ON A.sku=D.sku
    #                 where available_stock>0  and A.site='波兰'
    #         )a
    # """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df2 = conn_ck.ck_select_to_df(sql2)
    print('欧洲计算完成...')
    sql = f"""
    select
        a.*,
        ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747-{vat_fee}),1)-0.01 as "shein_lowest_price",
        ceil(GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  "shein_price_sku",
        GREATEST(ceil(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb),0)/(1-{p_fee}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01,toFloat64(shein_lowest_price)) as  "shein_price_seller_sku"
    from (
        select
            A.account_id account_id,
            A.site site,
            B.short_name short_name,
            A.asin asin,
            A.seller_sku seller_sku,
            A.Current_price as "当前售价",
            A.sku sku,
            A.group_name group_name,
            A.area area,
            A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",
            A.available_stock as "当前库存数量",      A.available_stock*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",
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
            `初始收费`+`初续重收费` as "fba_to_other_fees",  C.size_segment
        from support_document.fba_clear_seller_sku_{today} A
        LEFT JOIN yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)
        left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku
        where available_stock>0 and Average_daily_sales>0 and A.site = '英国'
    ) a
    """
    df3 = conn_ck.ck_select_to_df(sql)
    print('英国数据计算完成！')

    df = pd.concat([df1, df2, df3])
    df = df[(df['日均销量']>0) & (df['销售状态'] != '负利润加快动销')]
    # sku处理
    df['new_sku'] = df['sku'].str.split("[*]|[+]", expand=True)[0]
    df = pd.merge(df, df_line, how='left', left_on='new_sku', right_on='sku')
    # 标题匹配
    sql = """
        select distinct sku, title_cn, pur_length_pack,pur_width_pack,pur_height_pack
        from yibai_prod_base_sync.yibai_prod_sku
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_title = conn_ck.ck_select_to_df(sql)
    # df_title = read_sql_ck(sql, client)
    df = pd.merge(df, df_title, how='left', left_on='new_sku', right_on='sku')

    c1 = df['销售状态'].isin(['正常','涨价缩销'])
    c2 = df['销售状态'] == '负利润加快动销'
    c3 = df['销售状态'] == '正利润加快动销'
    df['可转寄库存'] = np.select([c1, c2, c3], [df['当前库存数量']-30, df['当前库存数量'], df['当前库存数量']-5])
    df['可转寄库存'] = np.where(df['可转寄库存']<0, 0, df['可转寄库存'])
    # df.drop('new_sku', axis=1, inplace=True)
    df.drop(['new_sku','sku_y','sku'], axis=1, inplace=True)
    df = df.rename(columns={'sku_x':'sku'})
    print(df.info())

    save_df(df, f'美英德法西波意葡瑞5%净利率fba转寄定价{today}', file_type='xlsx')
    # save_df(df, '德国法国西班牙波兰', file_type='csv')

    print('done!')

def cj_main():

    utils.program_name = '净利建议定价_shein'
    make_path()
    cur_path, root_path = get_path()

    client = get_ck_client(user='zengzhijie', password='ze65nG_zHij5ie')
    date_today = datetime.date.today()
    # date_today = (datetime.date.today() - datetime.timedelta(days=1))
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    date = datetime.date.today()
    is_high_season = int((today >= '20231015') & (today <= '20240114'))
    # 一级产品线
    sql_line = f"""
            select a.sku new_sku, b.path_name as `产品线路线` from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    # df_line = read_sql_ck(sql_line, client)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    print('一级产品线提取完成')

    # 获取CJ的sku + seller_sku信息
    # 没有毛利率、差值
    # 美国站的平台佣金 10%，税率0
    p_fee_us = 0.1
    platform_zero = 0.16
    sql = f"""
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03)/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03)/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,
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
                else null end as "fba_to_other_fees" 
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site = '美国' and A.available_stock > 0
            ) a
    """
    conn = connect_to_sql(database='oversea_explosives_data', data_sys='cj_本地库')
    df1 = conn.read_sql(sql)
    print(df1.info())
    #
    sql = """
        SELECT id as account_id, account_name, short_name 
        FROM cj_system_kd_sync.cj_amazon_account
    """
    conn_ck = pd_to_ck(database='cj_system_kd_sync', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)
    # df_account = read_sql_ck(sql, client)

    sql = """
        select distinct sku new_sku, title_cn, pur_length_pack,pur_width_pack,pur_height_pack
        from yibai_prod_base_sync.yibai_prod_sku
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_sku_title = conn_ck.ck_select_to_df(sql)
    # df_sku_title = read_sql_ck(sql, client)
    # 匹配账号简称、产品标题、产品线信息
    df1 = df1.merge(df_account[['account_id', 'short_name']], on='account_id', how='left')
    # df1 = df1.merge(df_sku_title, on='sku', how='left')
    # df1 = df1.merge(df_line, on='sku', how='left')
    #
    # save_df(df1, f'CJ_5%{utils.program_name}_美国{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')
    print('美国站数据计算完成！')

    # 德国法国西班牙波兰
    # 暂定：Shein欧洲站点平台VAT是20%，佣金10%
    #
    p_fee = 0.1
    vat_fee = 0.2
    sql = f"""
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747-{vat_fee}),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,   
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
                else null end as "fba_to_other_fees",            
                0 as "初续重收费"
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site = '德国' and A.available_stock > 0
            ) a
        UNION ALL
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747-{vat_fee}),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 7
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 7.2
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 7.45
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 7.65
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 8.25
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 8.43
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 8.98
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.55
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.11
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 10.68
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.24
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 13.62
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 9.32
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.89
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.45
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 11.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.58
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 12.80
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 13.96
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 15.21
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 16.67
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 17.11
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 18.72
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 19.44
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 20.60
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 21.76
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 22.93
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 14.72
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 15.63
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 15.95
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 17.29
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 18.00
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 18.75
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 19.38
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 19.91
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 20.20
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 21.99
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 23.10
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 24.87
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 26.21
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 27.35
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 28.82
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 30.81
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 35.10
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 42.47
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 47.16
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.57*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 26.38
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 34.58
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 36.97
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 42.12
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 50.96
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 56.59
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.8*(C.weight/1000)
                    else null end as "fba_to_other_fees",            
                    0 as "初续重收费"  
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site = '法国' and A.available_stock > 0
            ) a
        UNION ALL
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747-{vat_fee}),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 7.45
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 7.65
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 7.90
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 8.10
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 8.23
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 8.45
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 8.64
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 8.91
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 9.24
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 9.56
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 10.19
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 12.09
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 8.85
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.24
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 9.89
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 10.29
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.43
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 12.26
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 13.08
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 14.70
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 16.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 18.25
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 19.01
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 19.75
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 20.32
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 21.88
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 24.23
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 15.64
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 16.20
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 16.81
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 17.40
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 18.15
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 19.77
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 21.35
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 22.40
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 23.11
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 24.07
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 26.12
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 26.99
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 27.87
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 30.67
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 32.48
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 36.29
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 39.76
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 46.70
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 50.54
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.68*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 27.73
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 35.72
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 39.92
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 43.73
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 51.37
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 55.60
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.79*(C.weight/1000)
                    else null end as "fba_to_other_fees",            
                    0 as "初续重收费"
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site = '西班牙' and A.available_stock > 0
            ) a
        UNION ALL
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747-{vat_fee}),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 4.95
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 5.03
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 5.19
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 5.29
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 5.61
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 5.81
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 5.93
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 6.50
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.32
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.19
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.27
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 11.33
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 6.77
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 7.04
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 7.66
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 8.53
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.61
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 10.63
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 11.67
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 11.76
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 12.60
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 13.34
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 14.37
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 15.11
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 16.14
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 17.18
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 18.72
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 12.48
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 12.84
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 13.21
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 13.57
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 14.23
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 14.89
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 15.50
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 16.11
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 16.72
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 17.36
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 17.98
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 18.59
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 19.20
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 19.81
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 20.42
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 23.48
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 26.55
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 32.30
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 39.87
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.34*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 30.13
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 32.91
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 35.69
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 44.72
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 51.82
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 60.87
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.94*(C.weight/1000)
                    else null end as "fba_to_other_fees",            
                    0 as "初续重收费"
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site = '波兰' and A.available_stock > 0
            ) a
        UNION ALL
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747-{vat_fee}),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 6.72
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 6.97
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 7.02
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 7.17
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 7.19
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 7.37
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 7.47
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 7.99
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 8.52
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 9.05
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 9.89
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 12.65
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 8.80
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 9.49
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.07
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 10.79
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 11.28
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 12.39
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 13.50
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 15.26
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 16.68
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 18.44
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 20.20
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 21.96
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 23.72
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 25.48
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 27.24
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 14.45
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 15.99
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 16.42
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 16.81
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 17.52
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 18.86
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 19.30
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 20.37
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 21.43
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 22.09
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 24.93
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 24.61
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 26.04
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 27.18
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 27.98
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 32.80
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 34.45
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 38.90
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 44.90
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.5*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 27.22
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 31.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 37.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 43.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 49.70
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 55.70
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.77*(C.weight/1000)
                    else null end as "fba_to_other_fees",            
                    0 as "初续重收费"
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site = '意大利' and A.available_stock > 0
            ) a
        UNION ALL
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747-{vat_fee}),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,
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
                case    when C.size_segment like '%%小号信封%%' and C.weight BETWEEN  0 and 80 then 8.28
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  0 and 60 then 8.42
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  60.0001 and 210 then 8.77
                        when C.size_segment like '%%标准信封%%' and C.weight BETWEEN  210.0001 and 460 then 8.77
                        when C.size_segment like '%%大号信封%%' and C.weight BETWEEN  0 and 960 then 8.87
                        when C.size_segment like '%%超大号信封%%' and C.weight BETWEEN  0 and 960 then 9.07
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  0 and 150 then 9.66
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  150.0001 and 400 then 10.07
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  400.0001 and 900 then 10.76
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 12.56
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 13.36
                        when C.size_segment like '%%小包裹%%' and C.weight BETWEEN  1900.0001 and 3900 then 17.20
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  0 and 150 then 10.16
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  150.0001 and 400 then 10.84
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  400.0001 and 900 then 11.70
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  900.0001 and 1400 then 12.81
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1400.0001 and 1900 then 15.14
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  1900.0001 and 2900 then 16.02
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  2900.0001 and 3900 then 17.25
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  3900.0001 and 4900 then 18.60
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  4900.0001 and 5900 then 20.33
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  5900.0001 and 6900 then 21.68
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  6900.0001 and 7900 then 22.82
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  7900.0001 and 8900 then 24.22
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  8900.0001 and 9900 then 26.67
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  9900.0001 and 10900 then 27.03
                        when C.size_segment like '%%标准包裹%%' and C.weight BETWEEN  10900.0001 and 11900 then 28.44
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  0 and 760 then 21.87
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  760.0001 and 1010 then 22.08
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1010.0001 and 1260 then 22.49
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1260.0001 and 1510 then 22.56
                        when C.size_segment like '%%小号大件%%' and C.weight BETWEEN  1510.0001 and 1760 then 22.63
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  0 and 760 then 22.49
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  760.0001 and 1760 then 22.70
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  1760.0001 and 2760 then 22.70
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  2760.0001 and 3760 then 23.89
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  3760.0001 and 4760 then 24.43
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  4760.0001 and 5760 then 25.33
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  5760.0001 and 6760 then 26.23
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  6760.0001 and 7760 then 27.13
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  7760.0001 and 8760 then 30.50
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  8760.0001 and 9760 then 31.50
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 33.54
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 38.66
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 47.27
                        when C.size_segment like '%%标准大件%%' and C.weight BETWEEN  24760.0001 and 29760 then 61.01
                        when C.size_segment like '%%标准大件%%' and C.weight > 29760 then 1.68*(C.weight/1000)
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  0 and 4760 then 34.95
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  4760.0001 and 9760 then 35.81
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  9760.0001 and 14760 then 47.80
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  14760.0001 and 19760 then 53.50
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  19760.0001 and 24760 then 60.17
                        when C.size_segment like '%%大号大件%%' and C.weight BETWEEN  24760.0001 and 31500 then 76.10
                        when C.size_segment like '%%大号大件%%' and C.weight > 31500 then 1.79*(C.weight/1000)
                    else null end as "fba_to_other_fees",            
                    0 as "初续重收费"
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site in ('葡萄牙','瑞典') and A.available_stock > 0
            
        UNION ALL
        SELECT
            a.*,     
            round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747-{vat_fee}),1)-0.01 as `shein_lowest_price`,      
            round(GREATEST(product_cost+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01 as  `cj_shein_price_sku`,   
            GREATEST(round(GREATEST(product_cost+relese_monry_rmb+fba_to_other_fees*rate+first_trip_fee_rmb,0)/(1-{p_fee_us}-{platform_zero}-0.05-0.03-{vat_fee})/rate,1)-0.01, round(fba_to_other_fees/(1-{p_fee_us}-0.03-0.0747),1)-0.01) as `cj_shein_price_seller_sku` 
        FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost, Current_net_rate as  "当前售价净利润率",      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态", A.Destruction_price as "销毁价格本币",      
                A.fba_fees, A.first_trip_fee_rmb, A.rate, Current_net_rate*Current_price*A.rate AS "relese_monry_rmb", C.size_segment,
                C.weight,
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
                else null end as "fba_to_other_fees",            
                0 as "初续重收费"
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site = '英国' and A.available_stock > 0
    ) a
    """
    conn = connect_to_sql(database='oversea_explosives_data', data_sys='cj_本地库')
    df2 = conn.read_sql(sql)
    df2 = df2.merge(df_account[['account_id', 'short_name']], on='account_id', how='left')
    df = pd.concat([df1, df2])
    df['new_sku'] = df['sku'].str.split("[*]|[+]", expand=True)[0]
    df = df.merge(df_sku_title, on='new_sku', how='left')
    df = df.merge(df_line, on='new_sku', how='left')

    c1 = df['销售状态'].isin(['正常','涨价缩销'])
    c2 = df['销售状态'] == '负利润加快动销'
    c3 = df['销售状态'] == '正利润加快动销'
    df['可转寄库存'] = np.select([c1, c2, c3], [df['当前库存数量']-30, df['当前库存数量'], df['当前库存数量']-5])
    df['可转寄库存'] = np.where(df['可转寄库存']<0, 0, df['可转寄库存'])
    df.drop('new_sku', axis=1, inplace=True)
    print(df.info())
    save_df(df, f'CJ_美德法西波意葡瑞5%净利率fba转寄定价{today}', file_type='xlsx')

    print('done!')

# 美德法西波 库存信息
def get_stock():
    """
    美德法西波  fba库存信息
    """

    utils.program_name = '净利建议定价_shein'
    make_path()
    cur_path, root_path = get_path()
    date_today = datetime.date.today()
    # date_today = (datetime.date.today() - datetime.timedelta(days=1))
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    date = datetime.date.today()
    is_high_season = int((today >= '20231015') & (today <= '20240114'))

    client = get_ck_client(user='zengzhijie', password='ze65nG_zHij5ie')

    # 一级产品线
    sql_line = f"""
            select a.sku sku, b.path_name as `产品线路线` from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    # df_line = read_sql_ck(sql_line, client)
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    print('一级产品线提取完成')

    # 美国
    sql1 = f"""
        select
            a.*           
        from 
            (      
            select  
                A.account_id account_id,
                A.site site,
                B.short_name short_name,
                A.asin asin,
                A.seller_sku seller_sku,
                A.Current_price as "当前售价",      
                A.sku sku,
                A.adjustment_priority as "销售状态",      
                A.product_cost as "product_cost",      
                m.afn_fulfillable_quantity as "当前库存数量",  A.Average_daily_sales as "日均销量"     
            from support_document.fba_clear_seller_sku_{today} A 
            LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku     
            LEFT JOIN  yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)      
            left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku 
            where available_stock>0 and Average_daily_sales>0 and A.site in ('美国','德国','法国','西班牙','波兰','墨西哥','意大利','葡萄牙','瑞典') 
            )a    
    """
    # df1 = read_sql_ck(sql1, client)
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df1 = conn_ck.ck_select_to_df(sql1)
    # df1 = df1.merge(df_line, on='sku', how='left')
    # save_df(df1, f'5%{utils.program_name}_美国{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')
    print('美国站数据计算完成！')
    # 德国法国西班牙波兰
    # 暂定：Shein欧洲站点平台VAT是20%，佣金10%
    # 1121:暂时只需要美国+法国、日销>0、非负利润加快动销
    #
    p_fee = 0.1
    vat_fee = 0.2
    df = df1.copy()
    df = df[(df['日均销量'] > 0) & (df['销售状态'] != '负利润加快动销')]
    # sku处理
    df['new_sku'] = df['sku'].str.split("[*]|[+]", expand=True)[0]
    df = pd.merge(df, df_line, how='left', left_on='new_sku', right_on='sku')
    # 标题匹配
    sql = """
        select distinct sku, title_cn, pur_length_pack,pur_width_pack,pur_height_pack
        from yibai_prod_base_sync.yibai_prod_sku
    """
    # df_title = read_sql_ck(sql, client)
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_title = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_title, how='left', left_on='new_sku', right_on='sku')

    c1 = df['销售状态'].isin(['正常', '涨价缩销'])
    c2 = df['销售状态'] == '负利润加快动销'
    c3 = df['销售状态'] == '正利润加快动销'
    df['可转寄库存'] = np.select([c1, c2, c3], [df['当前库存数量'] - 30, df['当前库存数量'], df['当前库存数量'] - 5])
    df['可转寄库存'] = np.where(df['可转寄库存'] < 0, 0, df['可转寄库存'])
    df.drop(['new_sku','sku_y','sku'], axis=1, inplace=True)
    df = df.rename(columns={'sku_x':'sku'})
    print(df.info())

    # columns_list = ['']
    # save_df(df[df['site'].isin(['美国', '德国'])], f'美德法西波5%净利率fba转寄定价{today}_1', file_type='xlsx')
    # save_df(df[df['site'].isin(['法国', '西班牙','波兰'])], f'美德法西波5%净利率fba转寄定价{today}_2', file_type='xlsx')
    save_df(df, f'美德法西波墨意瑞fba转寄定价{today}_库存信息', file_type='xlsx')
    # save_df(df, '德国法国西班牙波兰', file_type='csv')

    print('done!')

def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='domestic_warehouse_clear')
    # df_rate = ck_client.ck_select_to_df(sql)
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = conn_ck.ck_select_to_df(sql)

    return df_rate
##
def get_mx_price():
    # 计算shein墨西哥站点的运费数据
    date_today = time.strftime('%%Y-%m-%d')
    sql = f"""
        SELECT 
            sku, best_warehouse_id, best_warehouse_name, warehouse, available_stock, sales_status, country, ship_name, 
            new_price, total_cost, date_id
        FROM over_sea.dwm_oversea_price_dtl
        WHERE date_id = '{date_today}' and platform = 'AMAZON' and warehouse='墨西哥仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_mx_price = conn.read_sql(sql)
    #
    sql = f"""
        SELECT
            distinct sku, title, type, product_status, linest
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_today}'
    """
    df_sku_info = conn.read_sql(sql)
    #
    df_mx_price = pd.merge(df_mx_price, df_sku_info, how='left', on=['sku'])
    #
    df_mx_price['platform_fee'] = 0.1
    df_mx_price['platform_zero'] = 0.16
    df_mx_price['platform_must_profit'] = 0.05
    df_mx_price['extra_fee'] = 0.03
    #
    df_rate = get_rate()

    df_mx_price = pd.merge(df_mx_price, df_rate[['country', 'rate']], how='left', on=['country'])
    df_mx_price[['new_price','total_cost']] = df_mx_price[['new_price','total_cost']].astype(float)
    df_mx_price['price'] = (df_mx_price['new_price'] + df_mx_price['total_cost']) / (1-df_mx_price['platform_fee']-
                                                                                     df_mx_price['platform_zero']-df_mx_price['platform_must_profit']-
                                                                                     df_mx_price['extra_fee'])/df_mx_price['rate']
    #
    df_mx_price.to_excel('df_mx_price.xlsx', index=0)


def get_cj_stock():

    utils.program_name = '净利建议定价_shein'
    make_path()
    cur_path, root_path = get_path()
    client = get_ck_client(user='zengzhijie', password='ze65nG_zHij5ie')
    date_today = datetime.date.today()
    # date_today = (datetime.date.today() - datetime.timedelta(days=1))
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    date = datetime.date.today()
    is_high_season = int((today >= '20231015') & (today <= '20240114'))
    # 一级产品线
    sql_line = f"""
            select a.sku new_sku, b.path_name as `产品线路线` from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    # df_line = read_sql_ck(sql_line, client)
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    print('一级产品线提取完成')

    # 获取CJ的sku + seller_sku信息
    # 没有毛利率、差值
    # 美国站的平台佣金 10%，税率0
    p_fee_us = 0.1
    platform_zero = 0.16
    sql = f"""
        SELECT
            a.*    
            FROM (
            SELECT 
                A.account_id, A.site site, A.asin, A.seller_sku, Current_price as "当前售价",      
                A.sku, group_name, product_cost,      
                available_stock as "当前库存数量", available_stock*product_cost as "当前库存金额", Average_daily_sales as "日均销量",      
                available_days_in_stock as "在库预计可售天数", adjustment_priority as "销售状态"
            FROM oversea_explosives_data.fba_clear_seller_sku_{today} A
            LEFT JOIN oversea_explosives_data.fba_fees C
            ON A.account_id = C.account_id and A.seller_sku = C.seller_sku
            WHERE A.site in ('美国','德国','法国','西班牙','波兰','墨西哥','意大利','葡萄牙','瑞典') and A.available_stock > 0
            ) a
    """
    conn = connect_to_sql(database='oversea_explosives_data', data_sys='cj_本地库')
    df1 = conn.read_sql(sql)
    print(df1.info())
    #
    sql = """
        SELECT id as account_id, account_name, short_name 
        FROM cj_system_kd_sync.cj_amazon_account
    """
    # df_account = read_sql_ck(sql, client)
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)

    sql = """
        select distinct sku new_sku, title_cn, pur_length_pack,pur_width_pack,pur_height_pack
        from yibai_prod_base_sync.yibai_prod_sku
    """
    # df_sku_title = read_sql_ck(sql, client)
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_sku_title = conn_ck.ck_select_to_df(sql)
    # 匹配账号简称、产品标题、产品线信息
    df1 = df1.merge(df_account[['account_id', 'short_name']], on='account_id', how='left')
    # df1 = df1.merge(df_sku_title, on='sku', how='left')
    # df1 = df1.merge(df_line, on='sku', how='left')
    #
    # save_df(df1, f'CJ_5%{utils.program_name}_美国{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')
    print('美国站数据计算完成！')

    df = df1.copy()
    df['new_sku'] = df['sku'].str.split("[*]|[+]", expand=True)[0]
    df = df.merge(df_sku_title, on='new_sku', how='left')
    df = df.merge(df_line, on='new_sku', how='left')

    c1 = df['销售状态'].isin(['正常','涨价缩销'])
    c2 = df['销售状态'] == '负利润加快动销'
    c3 = df['销售状态'] == '正利润加快动销'
    df['可转寄库存'] = np.select([c1, c2, c3], [df['当前库存数量']-30, df['当前库存数量'], df['当前库存数量']-5])
    df['可转寄库存'] = np.where(df['可转寄库存']<0, 0, df['可转寄库存'])
    df.drop('new_sku', axis=1, inplace=True)
    print(df.info())
    save_df(df, f'CJ_美德法西波墨意瑞fba转寄定价{today}_库存信息', file_type='xlsx')

    print('done!')


if __name__ == '__main__':
    main()
    # cj_main()
    # get_stock()
    # get_cj_stock()
    # get_mx_price()
    # temp_main()