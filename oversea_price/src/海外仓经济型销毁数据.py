"""
定期输出海外仓经济型销毁数据。销毁条件：
1、超180天库龄的库存
2、已达销毁价
3、日销为0 ？
4、...
"""

##
import pandas as pd
import time
import numpy as np
from src import fetch_data as fd
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
import warnings
warnings.filterwarnings('ignore')
##
def filter_by_country(df):
    if len(df) > 1:
        if df[df['warehouse_country'] == df['country']].empty:
            return df
        else:
            return df[df['warehouse_country'] == df['country']]
    else:
        return df

def filter_by_platform(df):
    if len(df) > 1:
        df_output = df[df['platform'] == 'AMAZON']
        if df_output.empty:
            df_output = df[df['platform'] == 'EB']
            if df_output.empty:
                df_output = df[df['platform'] == 'WALMART']
                if df_output.empty:
                    df_output = df[df['platform'] == 'CDISOUNT']
                    if df_output.empty:
                        df_output = df[df['platform'] == 'WISH']
        return df_output
    else:
        return df
##
def get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """
    sql = """
    SELECT 
        site as country, 0 pay_fee, vat_fee, extra_fee,  
        platform_zero, 0.05 platform_must_percent
    FROM yibai_platform_fee
    WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df
##
# 按国家、平台优先级选取达到销毁价的记录
def main():
    date_today = time.strftime('%Y-%m-%d')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')

    # date_today = '2024-03-29'
    sql = f"""
        SELECT
            sku, title, linest, last_linest, best_warehouse_name, warehouse, new_price, available_stock,available_stock_money,
            warehouse_stock_age, age_180_plus, age_270_plus,charge_total_price_rmb,day_sales,
            estimated_sales_days,overage_esd
        FROM yibai_oversea.dwm_sku_temp_info
        WHERE date_id = '{date_today}' and available_stock > 0 and sales_status != '正常'
    """
    df_sku = conn_ck.ck_select_to_df(sql)
    print(df_sku.info())
    #
    sql = f"""
        SELECT 
            sku, warehouse, platform, country, sales_status, total_cost, target_profit_rate, price_rmb, price, lowest_price, is_distory
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = '{date_today}' and available_stock > 0 and sales_status != '正常'
    """
    df_price_data = conn_ck.ck_select_to_df(sql)
    # df_price_data = conn.read_sql(sql)
    print(df_price_data.info())
    #
    c1 = (df_price_data['warehouse'] == '英国仓') & (~df_price_data['country'].isin(['UK','GB']))
    df_price_data = df_price_data[~c1]

    df_price_data['warehouse_country'] = df_price_data['warehouse'].replace({'美国仓':'US','德国仓':'DE','英国仓':'UK','法国仓':'FR','澳洲仓':'AU'})
    # df = df_price_data.groupby(['sku', 'warehouse'], group_keys=False).apply(lambda x: filter_by_country(x)).reset_index(drop=True)
    df = df_price_data.groupby(['sku', 'warehouse'], group_keys=False).apply(lambda x: filter_by_platform(x)).reset_index(drop=True)
    df = df.groupby(['sku', 'warehouse'], group_keys=False).apply(
        lambda x: filter_by_country(x)).reset_index(drop=True)
    #
    # 合并信息
    df_final = pd.merge(df_sku, df, how='left', on=['sku','warehouse'])
    col = ['new_price','age_180_plus','age_270_plus','charge_total_price_rmb']
    df_final[col] = df_final[col].astype(float)
    df_final = df_final.sort_values(by=['age_180_plus'], ascending=False)
    df_final['age_180_plus_money'] = df_final['age_180_plus'] * df_final['new_price']
    df_final['age_270_plus_money'] = df_final['age_270_plus'] * df_final['new_price']
    #
    df_fee = get_platform_fee()
    df_final = pd.merge(df_final, df_fee, how='left', on=['country'])
    df_final.to_excel('df_final.xlsx', index=0)

def get_bg3_sku_price():
    """ """
    sql = """
        SELECT DISTINCT sku, source
        FROM yibai_oversea.temp_oversea_jp_to_fp_sku
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)

    sku_list = ['SH0124JK005H10002','SH0124JK005V10001','SH0124JK005H10001','SH0124JK005T10001',
                'YM888SH010S24B0002','SH0124CD002X10002','SH0124CD002BG0001','YM888SH01S24R00001']

    sql = """
        SELECT sku, warehouse, type, best_warehouse_name, new_price, available_stock, 
        overage_level,lowest_profit,after_profit
        FROM yibai_oversea.dwm_sku_temp_info
        WHERE date_id = '2026-03-30'
    """
    df = conn_ck.ck_select_to_df(sql)
    df = df[(df['sku'].isin(df_sku['sku'].unique())) | (df['sku'].isin(sku_list))]

    sql = """
        SELECT *
        FROM over_sea.dwm_oversea_profit
        WHERE date_id = '2026-03-30'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_profit = conn.read_sql(sql)

    df = pd.merge(df, df_profit, how='left', on=['sku', 'warehouse'])

    # 是否继续清仓sku
    sql = """ 
        SELECT distinct sku, warehouse , 1 as temp_clear_sku
        FROM yibai_oversea.oversea_temp_clear_sku
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_temp_clear_sku )
    """
    df_clear = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_clear, how='left', on=['sku', 'warehouse'])

    df.to_excel('F://Desktop//df_bg3.xlsx', index=0)



##

if __name__ == '__main__':
    # main()
    get_bg3_sku_price()