##
import warnings

from dateutil.relativedelta import relativedelta

from utils import utils
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, \
    is_df_exist, read_df, make_path, get_path
from all_auto_task.scripts_ck_client import CkClient
import datetime
import pymysql
import pandas as pd
import numpy as np
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck, get_ck_conf_data
warnings.filterwarnings('ignore')
import src.fetch_data as fd
##
# from importlib import reload
# reload(fd)
# ##
# df_tk_fee = pd.read_excel('F:\yibai-price-strategy\data\TK运费模板.xlsx', engine='openpyxl')
# df_tk_fee.columns = ['weight', 'TK_fee']
#
# ##
# df_tk_price = pd.read_excel('F:\Desktop\日常任务\转寄定价\FBA转寄5%净利建议定价_TK_美国20231120.xlsx', engine='openpyxl')
# ##
# df_tk_price['weight'] = df_tk_price['C_weight']/1000
# df_tk_price = df_tk_price.sort_values(by='weight', ascending=True)
# df = pd.merge_asof(df_tk_price, df_tk_fee, on='weight', direction="forward")
# df['TK_fee']= np.where(df['weight']>=30.871, 46.38, df['TK_fee'])
# ##
# df['fee_diff'] = df['fba_to_other_fees'] - df['TK_fee']
# p_fee= 0.02
# df['TK_new_price_sku'] = (df['product_cost']+df['fee_diff']*df['rate']+df['first_trip_fee_rmb'])/(1-p_fee-0.16-0.05-0.03-0.05)/df['rate']+0.3
# df['TK_new_price_seller'] = (df['product_cost']+df['relese_monry_rmb']+df['fee_diff']*df['rate']+df['first_trip_fee_rmb'])/(1-p_fee-0.16-0.05-0.03-0.05)/df['rate']+0.3
# columns_list = ['account_id','site','short_name','asin','seller_sku','当前售价','sku','title_cn','产品线路线','一级产品线','product_cost','当前售价毛利润率','当前售价净利润率','当前库存数量','当前库存金额','销售状态',
#                 'rate','差值','relese_monry_rmb','fba_to_other_fees','first_trip_fee_rmb','weight','tk_lowest_price','tk_price_sku','tk_price_seller_sku','TK_fee','fee_diff','TK_new_price_sku','TK_new_price_seller']
# df_1 = df[columns_list]
##
def main():
    """
    TK 转寄定价计算(暂时只需要美国站【家居园艺，健康美容】的产品）

    TK定价公式需修改：
    1、价格不含尾程运费。
    2、因TK平台运费计算方式与Amazon尾程运费计算方式不同（TK只按重量分段），需在定价公式上体现出差异值

    6% 退款率
    7.47% 销毁价综合系数 （来源不明
    2% 是平台佣金
    16% 是毛净利差值  新平台 （盲拍）
    5% 净利率 （暂定）
    10% 推广费
    3% 汇损折旧
    0.3美金 售价基础上加

    5%净利率定价的公式：
    销毁价：
    fba多渠道运费/(1-平台佣金-0.0747-0.03汇损折旧)
    sku维度：
    (成本+fba多渠道运费*汇率+头程 + 0.3*rate)/(1-平台佣金-毛净利差值-5%目标净利率-0.03汇损折旧-0.1推广费)/rate
    seller_sku维度：
    (成本+fba多渠道运费*汇率+头程+当前价格*当前净利率*汇率 + 0.3*rate)/(1-平台佣金-毛净利差值-5%目标净利率-0.03汇损折旧-0.1推广费)/rate
    修改版：将【fba多渠道运费】替换为【TK运费与Amazon运费差异】
    20231201 修改：TK运费统一按10.99计算。同时再算一版fee_diff<0时，替换为0的定价
    20231205 修改：TK运费统一修改为7.99。
    20231215 修改：0.3美金上浮，放到分子里
    """

    utils.program_name = 'FBA转寄5%净利建议定价_TK'
    make_path()
    cur_path, root_path = get_path()
    # date_today = datetime.date.today()
    date_today = datetime.date.today() - datetime.timedelta(days=1)
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    date = datetime.date.today()
    is_high_season = int((today >= '20231015') & (today <= '20240114'))

    client = get_ck_client(user='zhangyilan', password='zhangyilan2109221544')

    # 一级产品线
    sql_line = f"""
            select a.sku sku, b.path_name as `产品线路线` from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    # df_line = read_sql_ck(sql_line, client)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    print('一级产品线提取完成')

    # 美国
    p_fee = 0.02
    pl_fee = 0.1
    sql1 = f"""
        select a.*,            
            ceil(fba_to_other_fees/(1-{p_fee}-0.03-0.0747),1)-0.01 as "tk_lowest_price",      
            GREATEST(toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb)+0.3*rate,0)/(1-{p_fee}-0.16-0.05-0.03-{pl_fee})/rate as  "tk_price_sku",   
            GREATEST(GREATEST(toFloat64(product_cost)+toFloat64(relese_monry_rmb)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb)+0.3*rate,0)/(1-{p_fee}-0.16-0.05-0.03-{pl_fee})/rate,toFloat64(tk_lowest_price)) as  "tk_price_seller_sku" 
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
                D.title_cn title_cn,
                A.group_name group_name,      
                A.area area,      
                A.product_cost as "product_cost",      A.Current_gross_rate as "当前售价毛利润率",      A.Current_net_rate as  "当前售价净利润率",      
                A.available_stock as "当前库存数量", m.afn_fulfillable_quantity as "可用库存",    
                m.afn_fulfillable_quantity*product_cost as "当前库存金额",      A.Average_daily_sales as "日均销量",      
                A.available_days_in_stock as "在库预计可售天数",      A.adjustment_priority as "销售状态",      A.Destruction_price as "销毁价格本币",      
                A.fba_fees as "fba_fees",      A.first_trip_fee_rmb as "first_trip_fee_rmb",      A.rate as "rate",      A.FBA_difference as "差值",      
                A.Current_net_rate*A.Current_price*A.rate AS "relese_monry_rmb",  C.weight C_weight,      C.weight/28.34 AS "盎司",      
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
                C.size_segment, e.`真实退款率` `真实退款率`   
            from support_document.fba_clear_seller_sku_{today} A
            LEFT JOIN  yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end m ON toUInt64(A.account_id)=toUInt64(m.account_id) and A.seller_sku = m.sku
            LEFT JOIN  yibai_system_kd_sync.yibai_amazon_account B ON toUInt64(A.account_id)=toUInt64(B.id)      
            left join domestic_warehouse_clear.fba_fees C on toUInt64(A.account_id)=toUInt64(C.account_id) AND A.seller_sku=C.seller_sku 
            LEFT JOIN  (
                select distinct sku, title_cn
                from yibai_prod_base_sync.yibai_prod_sku
                ) D ON A.sku=D.sku
            LEFT JOIN (
                SELECT account_id, seller_sku, `真实退款率`
                FROM temp_database_hxx.fba_refund_rate_raw_data a
                LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
                ON toUInt64(a.account_id) = toUInt64(b.id)
                WHERE b.site = 'us'
            ) e
            ON toUInt64(A.account_id)=toUInt64(e.account_id) and A.seller_sku = e.seller_sku
            where available_stock>0  and A.site='美国' 
        )a    
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df1 = conn_ck.ck_select_to_df(sql1)
    # df1 = read_sql_ck(sql1, client)
    df1.drop(['area','group_name'], axis=1, inplace=True)
    df1 = df1.merge(df_line, on='sku', how='left')
    # 暂时只需要【家居园艺，健康美容】的产品
    df1 = df1[df1['一级产品线'].isin(['家居园艺','健康美容','工业用品','电子用品','汽摩用品','影视摄影','智能安防','户外用品'])]
    # df1 = df1[df1['一级产品线'].isin(['家居园艺'])]
    print(df1.info())
    # TK定价不包含尾程运费，但是要在定价中体现TK运费与FBA多渠道运费的差异
    df_tk_fee = pd.read_excel('F:\yibai-price-strategy\data\TK运费模板.xlsx', engine='openpyxl')
    df_tk_fee.columns = ['weight', 'TK_fee']
    df1['weight'] = df1['C_weight'] / 1000
    df1 = df1.sort_values(by='weight', ascending=True)
    # df = pd.merge_asof(df1, df_tk_fee, on='weight', direction="forward")
    # df['TK_fee'] = np.where(df['weight'] >= 30.871, 46.38, df['TK_fee'])
    # 20231201修改，TK运费统一按10.99计算
    # 20231205修改，TK运费统一按7.99计算
    df1['TK_fee'] = 7.99
    df1['fee_diff'] = df1['fba_to_other_fees'] - df1['TK_fee']
    df1['fee_diff_0'] = np.where(df1['fee_diff']<0, 0, df1['fee_diff'])
    df1['TK_new_price_sku'] = (df1['product_cost'] + df1['fee_diff'] * df1['rate'] + df1['first_trip_fee_rmb'] + 0.3*df1['rate']) / (
                1 - p_fee - 0.16 - 0.05 - 0.03 - pl_fee) / df1['rate']
    df1['TK_new_price_seller'] = (df1['product_cost'] + df1['relese_monry_rmb'] + df1['fee_diff'] * df1['rate'] + df1[
        'first_trip_fee_rmb'] + 0.3*df1['rate']) / (1 - p_fee - 0.16 - 0.05 - 0.03 - pl_fee) / df1['rate']

    df1['TK_new_price_sku_0'] = (df1['product_cost'] + df1['fee_diff_0'] * df1['rate'] + df1['first_trip_fee_rmb'] + 0.3*df1['rate']) / (
                1 - p_fee - 0.16 - 0.05 - 0.03 - pl_fee) / df1['rate']
    df1['TK_new_price_seller_0'] = (df1['product_cost'] + df1['relese_monry_rmb'] + df1['fee_diff_0'] * df1['rate'] + df1[
        'first_trip_fee_rmb'] + 0.3*df1['rate']) / (1 - p_fee - 0.16 - 0.05 - 0.03 - pl_fee) / df1['rate']

    c1 = df1['销售状态'].isin(['正常','涨价缩销'])
    c2 = df1['销售状态'] == '负利润加快动销'
    c3 = df1['销售状态'] == '正利润加快动销'
    df1['可转寄库存'] = np.select([c1, c2, c3], [df1['可用库存']-30, df1['可用库存'], df1['可用库存']-5])
    df1['可转寄库存'] = np.where(df1['可转寄库存']<0, 0, df1['可转寄库存'])
    columns_list = ['account_id', 'site', 'short_name', 'asin', 'seller_sku', '当前售价', 'sku', 'title_cn',
                    '产品线路线', '一级产品线', 'product_cost',  '可转寄库存','当前售价毛利润率', '当前售价净利润率',
                    '销售状态', 'rate', '差值', '真实退款率','relese_monry_rmb', 'fba_to_other_fees', 'first_trip_fee_rmb', 'weight',
                    'tk_lowest_price', 'tk_price_sku', 'tk_price_seller_sku', 'TK_fee', 'fee_diff', 'fee_diff_0','TK_new_price_sku',
                    'TK_new_price_seller','TK_new_price_sku_0','TK_new_price_seller_0']
    df = df1[columns_list]
    # df = df[df['可转寄库存']>0]
    # 可转寄库存取数
    account_tuple = tuple(df['account_id'].unique())
    sql = f"""
        SELECT account_id, seller_sku, send_on_stock, create_time
        FROM yibai_product.yibai_ebay_syncstock_fba_stock_amazon_zt
        WHERE account_id in {account_tuple}
    """
    df_stock = fd.fetch_mysql(sql, 71, 'yibai_product')


    df_stock = df_stock.sort_values(by='create_time', ascending=False).drop_duplicates(subset=['account_id','seller_sku'], keep='first')
    df = pd.merge(df, df_stock, how='left', on=['account_id', 'seller_sku'])
    df = df.drop('可转寄库存', axis=1)
    df = df.rename(columns={'send_on_stock':'可转寄库存'})
    df = df.drop_duplicates(keep='first')
    save_df(df, f'{utils.program_name}_美国{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')
    print('美国站数据计算完成！')
##
def get_listing_price():
    # 指定链接的价格计算
    df_listing = pd.read_excel('F:\DESKTOP\TK_listing_price.xlsx')
    ##
    # 获取产品成本、头程运费、多渠道转寄运费（需加工）、账号简称
    # 美国
    seller_list = tuple(df_listing['seller_sku'].unique())
    df_listing['sku'] = df_listing['sku'].astype(str)
    sku_list = tuple(df_listing['sku'].unique())
    date_today = (datetime.date.today() - datetime.timedelta(days=1))
    today = f'{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    is_high_season = int((today >= '20231015') & (today <= '20240114'))
    ##
    sql = f"""
        select 
            a.*   
        from (      
                select  
                    C.account_id account_id,
                    C.site site,
                    B.short_name short_name,
                    C.seller_sku seller_sku,
                    C.sku,     
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
                where C.site='美国' and C.seller_sku in {seller_list}
            ) a    
    """
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='domestic_warehouse_clear')
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_listing_info = conn_ck.ck_select_to_df(sql)
    df_listing_info.columns = [i.split('.')[-1] for i in df_listing_info.columns.to_list()]
    ##
    # 可转寄库存取数
    sql = f"""
        SELECT account_id, seller_sku, send_on_stock, create_time
        FROM yibai_product.yibai_ebay_syncstock_fba_stock_amazon_zt
        WHERE seller_sku in {seller_list}
    """
    df_stock = fd.fetch_mysql(sql, 71, 'yibai_product')
    df_stock = df_stock.sort_values(by='create_time', ascending=False).drop_duplicates(subset=['account_id','seller_sku'], keep='first')
    ##
    df1 = pd.merge(df_listing_info, df_stock[['account_id', 'seller_sku', 'send_on_stock']], how='left', on=['account_id', 'seller_sku'])
    df1['TK_fee'] = 7.99
    p_fee = 0.02
    pl_fee = 0.1
    df1['fee_diff'] = df1['fba_to_other_fees'] - df1['TK_fee']
    df1['fee_diff_0'] = np.where(df1['fee_diff'] < 0, 0, df1['fee_diff'])
    df1['TK_new_price_sku'] = (df1['product_cost'] + df1['fee_diff'] * df1['rate'] + df1['first_trip_fee_rmb'] + 0.3 * df1[
        'rate']) / (
                                      1 - p_fee - 0.16 - 0.05 - 0.03 - pl_fee) / df1['rate']

    df1['TK_new_price_sku_0'] = (df1['product_cost'] + df1['fee_diff_0'] * df1['rate'] + df1['first_trip_fee_rmb'] + 0.3 *
                                 df1['rate']) / (
                                        1 - p_fee - 0.16 - 0.05 - 0.03 - pl_fee) / df1['rate']
    ##
    sql = f"""
    select distinct sku, title_cn
    from yibai_prod_base_sync.yibai_prod_sku
    WHERE sku in {sku_list}
    """
    df_sku_title = fd.fetch_ck(sql, 78, 'yibai_prod_base_sync')
    ##
    df1 = pd.merge(df1, df_sku_title, how='left', on=['sku'])
    ##
    df1.to_excel('FBA转寄_TK目标链接定价0122.xlsx', index=0)
##
if __name__ == '__main__':
    main()
