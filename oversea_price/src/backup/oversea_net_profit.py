import sys
import math
import datetime
from pulic_func.base_api.mysql_connect import pd_to_sql,sql_to_pd,pd_to_ck, connect_to_sql
import pandas as pd

##
def get_transport_fee():
    """
    获取当前最新运费数据
    """
    # 头程取分摊头程
    sql = """
    SELECT 
        sku, warehouseId as best_warehouse_id, warehouseName as warehouse_name, 
        case 
            when firstCarrierCost >= 0.01 then totalCost_origin 
            else totalCost
        end as total_cost, 
        shipName as ship_name, shippingCost, (totalCost_origin - firstCarrierCost - dutyCost) ship_fee, extraSizeFee,
        lowest_price, platform, country
    FROM oversea_transport_fee_useful_temu
    WHERE not (warehouse = '德国仓' and country in ('GB','UK'))
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_1 = conn.read_sql(sql)
    df_1['is_temu'] = 1
    sql = """
    SELECT 
        sku, warehouseId as best_warehouse_id, warehouseName as warehouse_name, 
        case 
            when firstCarrierCost >= 0.01 then totalCost_origin 
            else totalCost
        end as total_cost, 
        shipName as ship_name,shippingCost,(totalCost_origin - firstCarrierCost - dutyCost) ship_fee, extraSizeFee,
        lowest_price, platform, country
    FROM oversea_transport_fee_useful
    WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_2 = conn.read_sql(sql)
    df_2['is_temu'] = 0

    df = pd.concat([df_1, df_2])
    df = df.sort_values(by='is_temu', ascending=False)
    df = df.drop_duplicates(subset=['sku', 'best_warehouse_id','country'])
    print(df.info())

    return df

def get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """
    sql = """
    SELECT 
        platform, site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df

def get_sku_info():
    """ 计算海外仓sku+大仓维度，定价净利润 """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
    SELECT
        a.*,
        IF(b.sale_status IS NULL ,'正常',b.sale_status) as `销售状态`
    FROM (
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info) 
        and (available_stock > 0)
    ) a
    LEFT JOIN (
        SELECT *
        FROM over_sea.oversea_sale_status
        WHERE end_time IS NULL
    ) b ON a.sku=b.sku AND a.warehouse=b.warehouse
    """
    df = conn.read_sql(sql)
    df = df[df['销售状态']!='正常']

    df['库龄分段'] = 'A:0-30'
    df.loc[df['warehouse_stock']*0.8<df['age_30_plus'],'库龄分段'] = 'B:30-60'
    df.loc[df['warehouse_stock']*0.8<df['age_60_plus'],'库龄分段'] = 'C:60-90'
    df.loc[df['warehouse_stock']*0.8<df['age_90_plus'],'库龄分段'] = 'D:90-120'
    df.loc[df['warehouse_stock']*0.7<df['age_120_plus'],'库龄分段'] = 'E:120-150'
    df.loc[df['warehouse_stock']*0.6<df['age_150_plus'],'库龄分段'] = 'F:150-180'
    df.loc[df['warehouse_stock']*0.5<df['age_180_plus'],'库龄分段'] = 'G:180-270'
    df.loc[df['warehouse_stock']*0.4<df['age_270_plus'],'库龄分段'] = 'H:270-360'
    df.loc[df['warehouse_stock']*0.3<df['age_360_plus'],'库龄分段'] = 'I:360+'

    df['目标净利率'] = 0.06 + df['after_profit']
    df.loc[df['after_profit']<df['lowest_profit'],'目标净利率'] = df['lowest_profit']
    df = df[df['目标净利率']<0]

    df = df[['sku','title','linest','type','product_status','new_price','warehouse',
             'best_warehouse_id', 'best_warehouse_name','available_stock','available_stock_money',
             'day_sales','estimated_sales_days','销售状态','库龄分段','目标净利率']]
    print(f'符合清仓的sku数量共{len(df)}条')

    return df

def main():
    """
    计算海外仓sku+大仓维度，定价净利润
    """

    df_base = get_sku_info()
    df = pd.DataFrame()
    for i in [-2, -1.8, -1.2, -0.8, -0.5, -0.4, -0.3, -0.2, -0.1, -0.05]:
        df_temp = df_base.copy()
        df_temp['目标净利率'] = i
        df = pd.concat([df_temp, df])

    df_fee = get_transport_fee()
    col = ['sku','best_warehouse_id','total_cost','shippingCost','lowest_price','platform', 'country']
    dwm_price = pd.merge(df, df_fee[col], how='left', on=['sku', 'best_warehouse_id'])
    # 差值项
    # 匹配差值表
    df_platform_fee = get_platform_fee()
    dwm_price['country'] = dwm_price['country'].replace('GB','UK')
    dwm_price = pd.merge(dwm_price, df_platform_fee, how='left', on=['platform', 'country'])
    col = ['new_price', 'shippingCost']
    dwm_price[col] = dwm_price[col].astype(float)


    dic = {'美国仓':'US','德国仓':'DE','英国仓':'UK','墨西哥仓':'MX','加拿大仓':'CA','澳洲仓':'AU','法国仓':'FR',
           '西班牙仓':'ES', '意大利仓':'IT', '乌拉圭仓':'UY', '日本仓':'JP'}
    dwm_price['warehouse_country'] = dwm_price['warehouse'].replace(dic)
    dwm_price = dwm_price[dwm_price['warehouse_country']==dwm_price['country']]
    dwm_price = dwm_price[~dwm_price['warehouse'].isin(['乌拉圭仓','日本仓'])]

    # 填充空值
    for column in ['ppve','refound_fee','platform_zero']:
        dwm_price[column] = dwm_price.groupby('platform')[column].fillna(method='ffill')

    dwm_price['lowest_price_new'] = 1.2 * dwm_price['shippingCost'] / (1 - dwm_price['ppve'] - dwm_price['refound_fee'])

    dwm_price['目标定价'] = (dwm_price['new_price']+dwm_price['total_cost'])/(
            1-dwm_price['ppve']-dwm_price['platform_zero']-dwm_price['目标净利率'])

    dwm_price['目标净利润'] = dwm_price['目标定价'] * dwm_price['目标净利率']

    dwm_price['净利润/成本'] = dwm_price['目标净利润'] / dwm_price['new_price']

    dwm_price.to_excel('F://Desktop//dwm_price.xlsx', index=0)

if __name__ == '__main__':
    main()

