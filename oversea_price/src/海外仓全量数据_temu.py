##
import warnings

from utils import utils
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, make_path
from pulic_func.base_api.mysql_connect import connect_to_sql
import pandas as pd
import numpy as np
import datetime, time
from all_auto_task.oversea_price_adjust_tt import tt_get_sku
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import  get_rate
from all_auto_task.oversea_listing_detail_2023 import get_platform_fee


warnings.filterwarnings('ignore')

##
def filter_by_platform(df):
    if len(df) > 1:
        df_output = df[df['platform'] == 'AMAZON']
        if df_output.empty:
            df_output = df[df['platform'] == 'TEMU']
            if df_output.empty:
                df_output = df[df['platform'] == 'EB']
                if df_output.empty:
                    df_output = df[df['platform'] == 'CDISOUNT']
                    if df_output.empty:
                        df_output = df[df['platform'] == 'WALMART']
        return df_output
    else:
        return df

def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku sku, b.path_name as `产品线路线` 
            from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id)
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[1]
    df_line['三级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[2]
    df_line['四级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[3]
    df_line = df_line.drop_duplicates(subset='sku')
    df = pd.merge(df, df_line, how='left', on=['sku'])

    return df

def get_max_stock_warehouse():
    """
    获取库存最多的子仓
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = """
        SELECT sku, warehouse_id, warehouse_name, warehouse, available_stock
        FROM yb_datacenter.v_oversea_stock
        WHERE available_stock > 0 and warehouse_id not in (958, 902) 
        and warehouse_name not like '%TT%' 
        and (warehouse_other_type = 2 or warehouse_name like '%Temu独享%')
        and warehouse_name not in ('云仓美西仓','云仓波兰仓','HC美国西仓','英国仓UK2(移仓换标)',
                'JDHWC--英国海外仓','AG-加拿大海外仓（移仓换标）','加拿大满天星退件仓')
    """
    df_stock = conn_ck.ck_select_to_df(sql)

    df_max_stock = df_stock.sort_values(by='available_stock', ascending=False).drop_duplicates(
        subset=['sku', 'warehouse'])

    return df_max_stock

def main():
    """
    海外仓的全量数据
    """
    utils.program_name = '海外仓全量数据'
    make_path()

    # con = get_mysql_con(host='124.71.5.174', user="209313", password="vlEFa0WsjD", database='over_sea')
    con = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # client = get_ck_client(user='zhangyilan', password='zhangyilan2109221544')

    sql = """
        SELECT a.* ,b.sale_status as `销售状态` 
        FROM (
            SELECT *
            FROM over_sea.dwm_sku_temp_info
            WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
            and warehouse in ('美国仓','德国仓','法国仓', '英国仓','澳洲仓','加拿大仓','墨西哥仓')
            -- and warehouse in ('美国仓','德国仓','法国仓','西班牙仓','意大利仓)
            ) a
        left join (
            select sku ,warehouse ,sale_status from over_sea.oversea_sale_status WHERE end_time is NULL) b
        on a.sku = b.sku AND a.warehouse = b.warehouse;
    """
    df = con.read_sql(sql)
    df.drop(['sales_status', 'begin_profit', 'after_profit','up_profit_rate','charge_currency'], axis=1, inplace=True)
    df = df.sort_values(['sku', 'warehouse'])
    df['销售状态'] = df['销售状态'].fillna('正常')
    print(df.info())
    # 剔除通拓产品
    tt_sku = tt_get_sku()
    tt_sku = tt_sku[tt_sku['is_tt_sku'] == 1]
    df = df[~df['sku'].isin(tt_sku['sku'].unique())]
    # 20250321 隔离精铺非转泛品的sku
    c0 = df['type'].str.contains('海兔|易通兔') | df['type'].str.contains('转VC|转泛品')
    c1 = (df['best_warehouse_name'].str.contains('精铺|精品|凯美晨')) & (~c0)
    df = df[~c1]

    # 匹配gross
    sql_gross = """
    select 
        sku ,
        spu, 
        case 
            when toFloat64(weight_out_storage)=0 then toFloat64(pur_weight_pack) 
            else toFloat64(weight_out_storage) 
        end as gross
    from yibai_prod_base_sync.yibai_prod_sku
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync',data_sys='调价明细历史数据')
    df_gross=conn_ck.ck_select_to_df(sql_gross)
    # df_gross = read_sql_ck(sql_gross, client)
    df_output = df.drop('gross', axis=1).merge(df_gross, on='sku', how='left')
    # 获取最大库龄
    df_max_age = get_max_age()
    df_output = pd.merge(df_output, df_max_age, how='left', on=['sku','warehouse'])
    # 获取产品线
    df_output = get_line(df_output)
    print(df_output.info())
    print('sku信息获取完成...')
    # 取运费
    sql = f"""
    SELECT *
    FROM (
        SELECT 
            sku, warehouseId as best_warehouse_id, warehouse, platform, country, totalCost_origin,totalCost,  
            (totalCost_origin - firstCarrierCost - dutyCost) shippingCost, firstCarrierCost, 
            new_firstCarrierCost,dutyCost, overseasFee, shipName,
            CASE 
                WHEN warehouse='澳洲仓' then 'AU'
                WHEN warehouse='德国仓' then 'DE'
                WHEN warehouse='法国仓' then 'FR'
                WHEN warehouse='加拿大仓' then 'CA'
                WHEN warehouse='美国仓' then 'US'
                WHEN warehouse='墨西哥仓' then 'MX'
                WHEN warehouse='日本仓' then 'JP'
                WHEN warehouse='西班牙仓' then 'ES'
                WHEN warehouse='意大利仓' then 'IT'
                WHEN warehouse='英国仓' then 'UK'
                WHEN warehouseName in ('LD-OZON俄罗斯海外仓','GO--OZON俄罗斯海外仓', 'SD-俄罗斯海外仓') then 'RU'
                WHEN warehouseName in ('YM墨西哥2仓') then 'MX'
                WHEN warehouseName in ('SLM美国仓') then 'US'
            else null end as warehouse_country
        FROM over_sea.oversea_transport_fee_useful
        WHERE 
            platform in ('AMAZON','EB')
            and warehouse in ('美国仓','德国仓','法国仓','英国仓','澳洲仓','加拿大仓','墨西哥仓')
            -- and warehouse in ('美国仓','德国仓','法国仓','西班牙仓')
    ) a
    WHERE 
        (warehouse_country = country or warehouse = '德国仓')
    """
    # or best_warehouse_name = '谷仓捷克仓'
    # or and country in ('DE', 'FR', 'ES', 'IT', 'PL', 'NL','CZ')
    df_fee_1 = con.read_sql(sql)
    df_fee_1['is_temu'] = 0
    sql = f"""
    SELECT *
    FROM (
        SELECT 
            sku, warehouseId as best_warehouse_id, warehouse, platform, country, totalCost_origin,totalCost,  
            (totalCost_origin - firstCarrierCost - dutyCost) shippingCost, firstCarrierCost, 
            new_firstCarrierCost,dutyCost, overseasFee, shipName,
            CASE 
                WHEN warehouse='澳洲仓' then 'AU'
                WHEN warehouse='德国仓' then 'DE'
                WHEN warehouse='法国仓' then 'FR'
                WHEN warehouse='加拿大仓' then 'CA'
                WHEN warehouse='美国仓' then 'US'
                WHEN warehouse='墨西哥仓' then 'MX'
                WHEN warehouse='日本仓' then 'JP'
                WHEN warehouse='西班牙仓' then 'ES'
                WHEN warehouse='意大利仓' then 'IT'
                WHEN warehouse='英国仓' then 'UK'
                WHEN warehouseName in ('LD-OZON俄罗斯海外仓','GO--OZON俄罗斯海外仓', 'SD-俄罗斯海外仓') then 'RU'
                WHEN warehouseName in ('YM墨西哥2仓') then 'MX'
                WHEN warehouseName in ('SLM美国仓') then 'US'
            else null end as warehouse_country
        FROM over_sea.oversea_transport_fee_useful_temu
        WHERE 
            platform in ('TEMU')
            and warehouse in ('美国仓','德国仓','法国仓','英国仓','澳洲仓','加拿大仓','墨西哥仓')
    ) a
    WHERE 
        (warehouse_country = country or warehouse = '德国仓')
    """
    # or best_warehouse_name = '谷仓捷克仓'
    # or and country in ('DE', 'FR', 'ES', 'IT', 'PL', 'NL','CZ')
    df_fee_2 = con.read_sql(sql)
    df_fee_2['is_temu'] = 1

    df_fee = pd.concat([df_fee_1, df_fee_2])
    df_fee = df_fee.sort_values(by='is_temu', ascending=False)
    df_fee = df_fee.drop_duplicates(subset=['sku', 'best_warehouse_id','country'])

    print(df_fee.info())
    # 匹配库存最多子仓
    df_max_stock = get_max_stock_warehouse()
    df_output = pd.merge(df_output, df_max_stock[['sku','warehouse','warehouse_id', 'warehouse_name']], how='left', on=['sku','warehouse'])
    df_output['best_warehouse_name'] = np.where(df_output['warehouse_name'].isna(), df_output['best_warehouse_name'], df_output['warehouse_name'])
    df_output['best_warehouse_id'] = np.where(df_output['warehouse_name'].isna(), df_output['best_warehouse_id'], df_output['warehouse_id'])
    df_output.drop(['warehouse_name','warehouse_id'], axis=1, inplace=True)
    df_fee.drop(['warehouse'], axis=1, inplace=True)

    # 独享仓需替换为非独享仓的运费
    sql = """
        SELECT warehouse_id best_warehouse_id, real_warehouse_id
        FROM over_sea.yibai_warehouse_oversea_temp
        WHERE warehouse_name like '%%独享%%'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_warehouse = conn.read_sql(sql)

    # df_output['warehouse_id'] = df_output['best_warehouse_id']
    df_output = pd.merge(df_output, df_warehouse, how='left', on=['best_warehouse_id'])
    df_output['best_warehouse_id'] = np.where(df_output['real_warehouse_id'].isna(),
                                                df_output['best_warehouse_id'], df_output['real_warehouse_id'])

    df_output = pd.merge(df_output, df_fee, how='left', on=['sku', 'best_warehouse_id'])

    # df_output = df_output.groupby(['sku', 'best_warehouse_name'], group_keys=False).apply(lambda x: filter_by_platform(x)).reset_index(
    #     drop=True)

    df_output = df_output[(df_output['available_stock'] > 0)|(df_output['on_way_stock'] > 0)]
    df_output = df_output[~((df_output['available_stock'] == 0) & (df_output['warehouse'] == '德国仓'))]
    # save_df(df_output, '海外仓全量数据TEMU_test', file_type='xlsx')

    df_output.drop(['3days_sales','7days_sales','30days_sales','90days_sales','day_sales','is_adjust','is_new','overage_esd_bins',
                    'overage_esd','recent_day_sales',
                    'estimated_sales_days','day_sales_bins','esd_bins','section',
                    'lowest_profit','platform','warehouse_country','real_warehouse_id','is_temu'], axis=1, inplace=True)
    print(df_output.info())
    col_name = ['sku','标题','产品类型','产品状态','产品大类','产品子类','成本','产品尺寸','包装尺寸','最优子仓ID','最优子仓','大仓',
                '可用库存','可用库存金额','在途库存','库龄库存','库存库龄明细','超30天库存','超60天库存','超90天库存','超120天库存','超150天库存',
                '超180天库存','超270天库存','超360天库存','超库龄等级','每日仓租','取数日期','销售状态','spu','毛重',
                '最大库龄天数','产品线路线','一级产品线','二级产品线','三级产品线','四级产品线','目的国','总运费_分摊头程',
                '总运费_自算头程','尾程','分摊头程','自算头程','关税','海外仓处理费','尾程渠道']
    df_output.columns = col_name
    # temu剔除转泛品sku
    # df_output = df_output[~df_output['产品类型'].str.contains('转泛品')]

    save_df(df_output, '海外仓全量数据TEMU', file_type='xlsx')
    print(df_output.info())
    print('done!')
    return df_output



## 匹配库龄数据
def get_max_age():
    date_today = datetime.date.today() - datetime.timedelta(days=2)
    sql = f"""
        SELECT sku, warehouse, max(inventory_age) as max_age
        FROM (
            SELECT  
                date as date_id, sku, warehouse_id, warehouse_name,
                case when country = 'GB' then 'UK' when country = 'CS' then 'DE' else country end as country, 
                case
                    when country = 'US' then '美国仓'
                    when country in ('UK', 'GB') then '英国仓'
                    when country in ('CZ', 'CS', 'DE') then '德国仓'
                    when country = 'FR' then '法国仓'
                    when country = 'IT' then '意大利仓'
                    when country = 'AU' then '澳洲仓'
                    when country in ('ES', 'SP') then '西班牙仓'
                    when country = 'CA' then '加拿大仓'
                    when country = 'JP' then '日本仓'
                    when country = 'PL' then '德国仓'
                    when country = 'MX' then '墨西哥仓'
                    else Null
                end as warehouse,
                warehouse_stock, inventory_age, charge_total_price,
                case
                    when inventory_age <= 60 then 0
                    when inventory_age <= 90 and inventory_age > 60 then 60
                    when inventory_age <= 120 and inventory_age > 90 then 90
                    when inventory_age <= 150 and inventory_age > 120 then 120
                    when inventory_age <= 180 and inventory_age > 150 then 150
                    when inventory_age <= 270 and inventory_age > 180 then 180
                    when inventory_age <= 360 and inventory_age > 270 then 270
                    when inventory_age > 360 then 360
                end as overage_level
            FROM yb_datacenter.yb_oversea_sku_age ya
            LEFT JOIN (
                SELECT code warehouse_code, id warehouse_id, name warehouse_name 
                FROM yb_datacenter.yb_warehouse
                WHERE `type` in ('third', 'overseas')
            ) ve
            ON ya.warehouse_code = ve.warehouse_code
            WHERE date = '{date_today}' and status in (0,1) 
            -- and ve.warehouse_name not like '%独享%'
            -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
        ) a
        GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_age = conn_ck.ck_select_to_df(sql)
    return df_age


def get_lowest_temp():
    """ 获取销毁价（临时）"""
    df = pd.read_excel('F://Desktop//海外仓仓租高的产品降价清仓618.xlsx', dtype={'sku':str})
    df = df.rename(columns={'大仓':'warehouse'})
    print(df.info())
    dic = {'美国仓':'US','加拿大仓':'CA','德国仓':'DE','澳洲仓':'AU','英国仓':'UK',}
    df['warehouse_country'] = df['warehouse'].replace(dic)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
            SELECT
                a.sku sku, title, type, product_status, linest, a.warehouse warehouse, available_stock, 
                best_warehouse_name `最优子仓`, age_180_plus,product_package_size,
                new_price, gross,best_warehouse_id, overage_level, `30days_sales`,
                IF(b.sale_status IS NULL ,'正常',b.sale_status) as `销售状态`,
                new_price*0.013 as `国内采购运费`
            FROM (
                SELECT *
                FROM over_sea.dwm_sku_temp_info
                WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
                and (available_stock > 0)
            ) a
            LEFT JOIN (
                SELECT *
                FROM oversea_sale_status
                WHERE end_time IS NULL
            ) b ON a.sku=b.sku AND a.warehouse=b.warehouse
    """
    df_sku = conn.read_sql(sql)

    sql = """
        SELECT 
            sku, warehouseName as `最优子仓`, warehouse `大仓`, shipCode, shipName, shippingCost `尾程`, 
            totalCost_origin - firstCarrierCost - country as `非头程`, 
            firstCarrierCost `头程`, dutyCost `关税`, remoteExtraFee `偏远附加费`, extraSizeFee `超尺寸附加费`,
            overseasFee `海外仓处理费`, country , 0 as is_temu
        FROM oversea_transport_fee_useful
        WHERE platform='EB'  and country in ('US','CA', 'DE','AU','UK')
    """
    df_useful = conn.read_sql(sql)

    platform_fee = get_platform_fee('EB')

    df = pd.merge(df, df_sku[['sku', 'warehouse', '最优子仓']], how='left', on=['sku', 'warehouse'])
    df = pd.merge(df, df_useful[['sku', '最优子仓','尾程','非头程','country']], how='left', on=['sku','最优子仓'])
    df = df[df['warehouse_country'] == df['country']]

    df = pd.merge(df, platform_fee, how='left', on=['country'])

    print(df.info())

    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country','rate']], how='left', on=['country'])

    df['销毁价_人民币'] = 1.2*df['非头程']/(1-df['ppve']-df['refound_fee']-0.03)
    df['销毁价_本币'] = df['销毁价_人民币']/df['rate']

    df = df.rename(columns={'ppve':'平台佣金+paypal费率+vat', 'refound_fee':'退款率', 'platform_zero':'差值',
                            'platform_must_percent':'平台要求净利率'})

    df.to_excel('F://Desktop//ebay海外仓sku销毁价测算0703V2.xlsx', index=0)

##
def sku_temp():
    """ """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT a.* ,b.sale_status as `销售状态` 
        FROM (
            SELECT *
            FROM over_sea.dwm_sku_temp_info
            WHERE date_id in ('2025-06-05', '2025-06-06','2025-06-07','2025-06-08','2025-07-14','2025-07-15','2025-07-16','2025-07-17')
            and warehouse in ('美国仓','德国仓','法国仓', '英国仓','澳洲仓','加拿大仓','墨西哥仓','意大利仓','西班牙仓')
            -- and warehouse in ('美国仓','德国仓','法国仓','西班牙仓','意大利仓)
            and (available_stock > 0 or charge_total_price_rmb > 0)
            ) a
        left join (
            select sku ,warehouse ,sale_status from over_sea.oversea_sale_status WHERE end_time is NULL) b
        on a.sku = b.sku AND a.warehouse = b.warehouse;
    """
    df = conn.read_sql(sql)
    df.to_excel('F://Desktop//df_sku_temp4.xlsx', index=0)

##
if __name__ == '__main__':
    df = main()

    # get_lowest_temp()
    # get_ali_sku_info()
    # df = get_au_fee()
    # print(len(df))
    # get_mx_sku_info()
