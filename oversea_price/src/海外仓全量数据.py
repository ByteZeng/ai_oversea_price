##
import warnings

from utils import utils
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, make_path
from pulic_func.base_api.mysql_connect import connect_to_sql
import pandas as pd
import numpy as np
import datetime, time
from all_auto_task.oversea_listing_detail_2023 import xiaozu_dabu
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import get_stock, get_stock_age, get_rate, get_platform_fee
from all_auto_task.oversea_temu_price import get_line_new
from all_auto_task.yunfei_auto import get_toucheng_price_new
warnings.filterwarnings('ignore')
##
# df = xiaozu_dabu()
# # 筛选14部的账号
# group_list = tuple(df['group_name'][df['大部'] == '武汉产品线十四部'].unique())
# sql = f"""
# SELECT a.id as account_id, a.group_id, g.group_name
# FROM yibai_system_kd_sync.yibai_amazon_account a
# LEFT JOIN yibai_system_kd_sync.yibai_amazon_group g
# ON a.group_id = g.group_id
# WHERE g.group_name in {group_list}
# """
# client = get_ck_client(user='zhangyilan', password='zhangyilan2109221544')
# df_account = read_sql_ck(sql, client)
# account_list = tuple(df_account['account_id'].unique())
##
def filter_by_platform(df):
    if len(df) > 1:
        df_output = df[df['platform'] == 'AMAZON']
        if df_output.empty:
            df_output = df[df['platform'] == 'EB']
            if df_output.empty:
                df_output = df[df['platform'] == 'OZON']
        return df_output
    else:
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
        -- and warehouse_name not like '%TT%' 
        and warehouse_name not in ('云仓美西仓','云仓波兰仓','HC美国西仓','英国仓UK2(移仓换标)',
                'JDHWC--英国海外仓','AG-加拿大海外仓（移仓换标）','加拿大满天星退件仓')
        and warehouse_other_type = 2
    """
    df_stock = conn_ck.ck_select_to_df(sql)

    df_max_stock = df_stock.sort_values(by='available_stock', ascending=False).drop_duplicates(
        subset=['sku', 'warehouse'])

    return df_max_stock

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

def get_958_stock():
    """
    从海外仓库存表，获取SKU及库存信息详情
    """
    # 初始库存表的视图：v_oversea_stock
    t1 = time.time()
    sql = '''
    select
        sku, title, new_price, gross, warehouse_id, product_status,
        -- '' as type, product_status, '' last_linest, '' linest, 
        sum_available_stock as available_stock, sum_available_stock_money as available_stock_money, 
        sum_on_way_stock as on_way_stock, sku_create_time as create_time, 
        product_size, product_package_size, best_warehouse, warehouse
    from (
        with 
        [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17] as product_status_arr,
        ['已创建', '已开发', '待买样', '待品检', '待编辑', '待拍摄', '待编辑待拍摄', '待修图', '在售中', '审核不通过', '停售', 
        '待清仓', '已滞销', '待物流审核', '待关务审核', 'ECN资料变更中', 'ECN资料变更驳回'] as product_status_desc_arr	 
        select
            ps.sku as sku, ps.title_cn as title, ps.new_price as new_price, ps.product_weight_pross as gross, 
            ps.warehouse_code as warehouse_code, ps.warehouse_id as warehouse_id,
            -- case when p.state_type = '1' then '常规产品' end as dev_type,
            transform(ps.product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
            ps.available_stock as available_stock, ps.available_stock*toFloat64(ps.new_price) as available_stock_money,  
            ps.on_way_stock as on_way_stock, ps.create_time as sku_create_time, 
            concat(toString(ps.product_length), '*', toString(ps.product_width), '*', toString(ps.product_height)) as product_size, 
            concat(toString(ps.pur_lenght_pack), '*',toString(ps.pur_width_pack), '*', toString(ps.pur_height_pack) ) as product_package_size, 
            ps.warehouse_name as best_warehouse,
            ps.warehouse as warehouse,
            sum(ps.available_stock) over w as sum_available_stock, 
            sum(available_stock_money) over w as sum_available_stock_money, 
            sum(ps.on_way_stock) over w as sum_on_way_stock
        from
            (
            select 
                ps.sku sku,ps.warehouse warehouse,ps.warehouse_name as warehouse_name, yps.title_cn title_cn,
                yps.product_status product_status
                ,ps.warehouse_code as warehouse_code,ps.warehouse_id as warehouse_id,ps.on_way_stock on_way_stock 
                ,ps.available_stock available_stock ,if(isnull(yps.new_price),yps1.new_price,yps.new_price) as new_price
                ,if(isnull(yps.pur_weight_pack),yps1.pur_weight_pack,yps.pur_weight_pack) as product_weight_pross
                ,if(isnull(yps.product_length),yps1.product_length ,yps.product_length )  as product_length
                ,if(isnull(yps.product_width),yps1.product_width ,yps.product_width ) as product_width
                ,if(isnull(yps.product_height),yps1.product_height ,yps.product_height ) as product_height
                ,if(isnull(yps.pur_length_pack),yps1.pur_length_pack ,yps.pur_length_pack ) as pur_lenght_pack
                ,if(isnull(yps.pur_width_pack),yps1.pur_width_pack ,yps.pur_width_pack ) pur_width_pack
                ,if(isnull(yps.pur_height_pack),yps1.pur_height_pack ,yps.pur_height_pack ) pur_height_pack
                ,if(empty(toString(yps.create_time)),yps1.create_time,yps.create_time) as create_time
            from 
                (
                select 
                    * except (available_stock),
                    case 
                        WHEN sku LIKE 'GB-%' THEN REPLACE(sku,'GB-','') 
                        WHEN sku LIKE 'DE-%' THEN REPLACE(sku,'DE-','') 
                        WHEN sku LIKE 'FR-%' THEN REPLACE(sku,'FR-','') 
                        WHEN sku LIKE 'ES-%' THEN REPLACE(sku,'ES-','') 
                        WHEN sku LIKE 'IT-%' THEN REPLACE(sku,'IT-','') 
                        WHEN sku LIKE 'AU-%' THEN REPLACE(sku,'AU-','') 
                        WHEN sku LIKE 'CA-%' THEN REPLACE(sku,'CA-','') 
                        WHEN sku LIKE 'JP-%' THEN REPLACE(sku,'JP-','') 
                        WHEN sku LIKE 'US-%' THEN REPLACE(sku,'US-','') 
                        WHEN sku LIKE '%DE' THEN REPLACE(sku,'DE','') 
                        else sku 
                    end as skuu,
                    -- 2023-04-26 非澳洲仓下万邑通仓库可用库存不加干预（原来为全部为设置为0），进入调价逻辑
                    available_stock
                from yb_datacenter.v_oversea_stock 
                where warehouse_id in (958)
                order by available_stock desc
                ) ps
            left join yibai_prod_base_sync.yibai_prod_sku yps on ps.sku=yps.sku
            left join yibai_prod_base_sync.yibai_prod_sku yps1 on ps.skuu=yps1.sku
            having new_price > 0
            ) ps
        window w as (partition by sku, warehouse)
        order by available_stock desc, warehouse_id desc
    ) a
    limit 1 by sku, warehouse
    '''
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    df_sku_stock = ck_client.ck_select_to_df(sql)

    print('Time passed {:.4f}'.format(time.time() - t1))
    df_sku_stock.columns = [i.split('.')[-1] for i in df_sku_stock.columns.to_list()]
    df_sku_stock['available_stock'] = np.where(df_sku_stock['available_stock'] < 0, 0,
                                               df_sku_stock['available_stock'])
    df_sku_stock['available_stock_money'] = np.where(df_sku_stock['available_stock_money'] < 0, 0,
                                                     df_sku_stock['available_stock_money'])

    # 产品开发来源及品类
    df_sku_stock = get_line_new(df_sku_stock)
    # 20250321 隔离精铺非转泛品的sku
    c1 = (df_sku_stock['best_warehouse'].str.contains('精铺')) & (~df_sku_stock['type'].str.contains('转泛品'))
    df_sku_stock = df_sku_stock[~c1]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)
    return df_sku_stock

def get_958_stock_age():
    """
    20240807
    获取YM墨西哥仓的库龄数据。
    由于saas系统库龄是YM墨西哥实仓数据，无法区分出虚拟子仓的明细。而易佰销售可使用的仅限子仓YM墨西哥2仓。
    为分离出虚拟子仓YM墨西哥2仓的库龄数据，采用【实体仓库龄库存】 * 【子仓库存】/【实体仓库存】的方式等比例计算子仓的库龄数据。
    """
    # 获取saas系统库龄数据
    sql = """
    SELECT 
        sku, cargo_owner_id, sum(saas_stock) saas_stock,
        max(overage_level) overage_level, sum(age_30_plus) age_30_plus, sum(age_60_plus) age_60_plus, sum(age_90_plus) age_90_plus,
        sum(age_120_plus) age_120_plus, sum(age_150_plus) age_150_plus, sum(age_180_plus) age_180_plus,
        sum(age_270_plus) age_270_plus, sum(age_360_plus) age_360_plus
    FROM (
        SELECT
            sku, cargo_owner_id, saas_stock, stock_age,
            case
                when stock_age >= 30 and stock_age < 60 then 30
                when stock_age >= 60 and stock_age < 90 then 60
                when stock_age >= 90 and stock_age < 120 then 90
                when stock_age >= 120 and stock_age < 150 then 120
                when stock_age >= 150 and stock_age < 180 then 150
                when stock_age >= 180 and stock_age < 270 then 180
                when stock_age >= 270 and stock_age < 360 then 270
                when stock_age >= 360 then 360
            else 0 end as overage_level, 
            case when stock_age >= 30 then saas_stock else 0 end as age_30_plus,       
            case when stock_age >= 60 then saas_stock else 0 end as age_60_plus,
            case when stock_age >= 90 then saas_stock else 0 end as age_90_plus,
            case when stock_age >= 120 then saas_stock else 0 end as age_120_plus,
            case when stock_age >= 150 then saas_stock else 0 end as age_150_plus,
            case when stock_age >= 180 then saas_stock else 0 end as age_180_plus,
            case when stock_age >= 270 then saas_stock else 0 end as age_270_plus,
            case when stock_age >= 360 then saas_stock else 0 end as age_360_plus
        FROM (
            SELECT sku, cargo_owner_id, sum(saas_stock) as saas_stock, stock_age
            FROM (
                SELECT 
                    w, b.sku sku, b.client_sku client_sku, 
                    a.sku as oversea_sku, cargo_owner_id, 
                    instock_stock-out_stock as saas_stock, storage_age_date, 
                    toInt32(today() - toDate(storage_age_date)) as stock_age,
                    today()
                FROM yb_datacenter.yibai_stock_age_detail a
                LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
                ON a.sku = b.oversea_sku
                WHERE w = 'YM-MX-2'
            ) a
            GROUP BY sku, cargo_owner_id, stock_age
            HAVING saas_stock > 0 and cargo_owner_id = 8
        )
    ) a
    GROUP BY sku, cargo_owner_id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)

    # 获取云仓库存数据
    date_today = time.strftime('%Y%m%d')
    sql = f"""
    SELECT 
        sku, warehouse, arrayStringConcat(groupArray(stock_info), ',') AS warehouse_stock_age, sum(available_stock) as available_stock,
        sum(on_way_stock) as on_way_stock, sum(wait_outbound) as wait_outbound, sum(frozen_stock) frozen_stock, max(new_price) new_price
    FROM (
        SELECT
            ps.sku sku, toString(toDate(toString(date_id))) date_id, yw.ebay_category_id AS category_id, yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
            available_stock, allot_on_way_count AS on_way_stock, wait_outbound, frozen_stock, yps.new_price as new_price, cargo_owner_id,
            concat(toString(warehouse_id), ':', toString(available_stock)) as stock_info
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        LEFT JOIN (
            SELECT
                sku, product_status `产品状态`, title_cn `产品名称`,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `new_price`
            FROM yibai_prod_base_sync.yibai_prod_sku    
        ) yps ON ps.sku = yps.sku
        WHERE 
            ps.date_id = '{date_today}'          -- 根据需要取时间
            and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            -- and ps.available_stock > 0
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            and yw.id = 958  
            and ywc.name = '墨西哥仓'
        ORDER BY date_id DESC
    ) a
    GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock_temp = conn_ck.ck_select_to_df(sql)

    df_mx = pd.merge(df_stock_temp, df_mx_stock, how='left', on=['sku'])
    # YM库龄库存按比例分解
    df_mx['占比'] = df_mx['available_stock'] / df_mx['saas_stock']
    df_mx['占比'] = np.where(df_mx['占比'] > 1, 1, df_mx['占比'])
    #
    col_list = ['age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus',
                'age_360_plus']
    for i in col_list:
        df_mx[i] = df_mx[i] * df_mx['占比']
    #
    df_mx['warehouse_stock'] = np.where(df_mx['saas_stock'].isna(), df_mx['saas_stock'], df_mx['available_stock'])
    df_mx['best_warehouse_id'] = 958
    df_mx['best_warehouse_name'] = 'YM墨西哥toB代发仓'
    df_mx['charge_currency'] = 'MXN'
    df_mx['charge_total_price'] = 0

    #
    df_mx = df_mx[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus',
         'age_150_plus',
         'age_180_plus', 'age_270_plus', 'age_360_plus', 'warehouse_stock_age', 'charge_currency', 'overage_level',
         'best_warehouse_id', 'best_warehouse_name']]
    #
    df_mx = df_mx[df_mx['warehouse_stock'] > 0]

    return df_mx
## YM墨西哥toB代发仓定价
def get_mx_tob_stock():
    """ YM墨西哥toB打发仓库存信息获取"""
    df_stock = get_958_stock()
    print('获取库龄信息...')
    # df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    df_stock_age_warehouse = get_958_stock_age()
    dwm_sku = pd.merge(df_stock, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])

    dwm_sku = dwm_sku[dwm_sku['available_stock']>0]
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])

    col = ['sku','title','new_price','gross','product_package_size','best_warehouse_id', 'best_warehouse_name',
            'warehouse', 'available_stock', 'on_way_stock', 'warehouse_stock_age', 'overage_level']
    dwm_sku = dwm_sku[col]


    # dwm_sku.to_excel('F://Desktop//dwm_sku_mx.xlsx', index=0)

    return dwm_sku

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
            and warehouse in ('美国仓','德国仓','法国仓', '英国仓','澳洲仓','加拿大仓')
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
    print('sku信息获取完成...')
    sql = f"""
    SELECT *
    FROM (
        SELECT 
            sku, warehouseName as best_warehouse_name, warehouse, platform, country, totalCost_origin,totalCost,  
            (totalCost_origin - firstCarrierCost - dutyCost) shippingCost, firstCarrierCost, lowest_price,
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
            platform in ('EB')
            and warehouse in ('美国仓','德国仓','法国仓','西班牙仓','意大利仓','英国仓','澳洲仓','加拿大仓','墨西哥仓','俄罗斯仓')
            -- and warehouse in ('美国仓','德国仓','法国仓','西班牙仓','英国仓')
    ) a
    WHERE 
        (warehouse_country = country)
    """
    # or best_warehouse_name = '谷仓捷克仓'
    # or and country in ('DE', 'FR', 'ES', 'IT', 'PL', 'NL','CZ')
    df_fee_1 = con.read_sql(sql)
    print(df_fee_1.info())
    # 匹配库存最多子仓
    df_max_stock = get_max_stock_warehouse()
    df_output = pd.merge(df_output, df_max_stock[['sku','warehouse','warehouse_id', 'warehouse_name']], how='left', on=['sku','warehouse'])
    df_output['best_warehouse_name'] = np.where(df_output['warehouse_name'].isna(), df_output['best_warehouse_name'], df_output['warehouse_name'])
    df_output['best_warehouse_id'] = np.where(df_output['warehouse_name'].isna(), df_output['best_warehouse_id'], df_output['warehouse_id'])
    df_output.drop(['warehouse_name','warehouse_id'], axis=1, inplace=True)
    df_fee_1.drop(['warehouse'], axis=1, inplace=True)

    df_output.loc[df_output['best_warehouse_id'] == 958, 'best_warehouse_name'] = 'YM墨西哥仓'
    df_output = pd.merge(df_output, df_fee_1, how='left', on=['sku', 'best_warehouse_name'])
    df_output.loc[df_output['best_warehouse_id'] == 958, 'best_warehouse_name'] = 'YM墨西哥toB代发仓'

    dic = {'AMAZON':1, 'EB':2, 'OZON':3,'ALI':4}
    df_output['platform_order'] = df_output['platform'].map(dic).fillna(5)
    df_output = df_output.sort_values(by=['platform_order'], ascending=[True]).drop_duplicates(
        subset=['sku','warehouse','country'])
    df_output.drop('platform_order', axis=1, inplace=True)

    # 筛选有库存数据
    df_output = df_output[(df_output['available_stock'] > 0)|(df_output['on_way_stock'] > 0)]

    # ali数据
    df_output = df_output.sort_values(by='30days_sales', ascending=False)
    # df_output = df_output[df_output['30days_sales'] > 0]

    # 销量字段筛选
    df_output.drop(['3days_sales','7days_sales','30days_sales','90days_sales','day_sales','recent_day_sales','age_30_plus',
                    'estimated_sales_days','overage_esd','overage_esd_bins','day_sales_bins','esd_bins','section',
                    'lowest_profit','platform','warehouse_country','is_adjust'], axis=1, inplace=True)
    print(df_output.info())
    col_name = ['sku','标题','产品类型','产品状态','产品大类','产品子类','成本','产品尺寸','包装尺寸','最优子仓ID','最优子仓','大仓',
                '可用库存','可用库存金额','在途库存','库龄库存','库存库龄明细','超60天库存','超90天库存','超120天库存','超150天库存',
                '超180天库存','超270天库存','超360天库存','超库龄等级','每日仓租','是否新品','取数日期','销售状态','spu','毛重',
                '最大库龄天数','产品线路线','一级产品线','二级产品线','三级产品线','四级产品线','目的国','总运费_分摊头程',
                '总运费_自算头程','尾程','分摊头程','销毁价','自算头程','关税','海外仓处理费','尾程渠道']
    df_output.columns = col_name
    save_df(df_output, '海外仓全量数据EBAY', file_type='xlsx')
    print(df_output.info())
    # print('done!')
    return df_output

##
def get_au_fee():
    n = 2
    sql = f"""   select 
            a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost, 
            remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
            firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
            a.country as country,
            round(toDecimal64(1.2,4)*(toFloat64(a.shippingCost)+toFloat64(a.extraSizeFee)) 
                    / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price,
            b.new_price as new_price, b.pur_weight_pack pur_weight_pack, 
            toFloat64(b.pur_length_pack )*toFloat64(b.pur_width_pack)*toFloat64(pur_height_pack) as weight_volume,
            multiIf(arrayExists(x->x.1='ALL', d.limit_arr), arrayFirst(x->x.1='ALL',d.limit_arr).3, 
                    arrayExists(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr), 
                        arrayFirst(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr).3,
                    arrayExists(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)), d.limit_arr),
                        arrayFirst(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)),d.limit_arr).3, 0) as limit_price_rmb,
            null as zero_percent,
            null as five_percent,
            available_stock,
            '澳洲仓' as warehouse,
            case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                 subString(a.sku, 4)
            when subString(a.sku, 1, 2) in ['DE', 'GB'] then 
                 subString(a.sku, 3)
            when subString(a.sku, -2) in ['DE', 'GB'] then 
                 subString(a.sku, 1, -2)
            else 
                 a.sku
            end as son_sku
            from 
            (
                select * except(date_id) 
                from yibai_oversea.oversea_transport_fee_daily
                where (not (platform in ['AMAZON', 'EB', 'WALMART']
                  and shipName in ['谷东-UPS_一票多箱_Multi', '谷东_FEDEX_OVERNIGHT[Fedex_标准次日达]', '谷东_FEDEX_2DAY',
                                             '谷西_FEDEX_2DAY', '谷西_FEDEX_OVERNIGHT[Fedex_标准次日达]'])
                  and date_id =  toYYYYMMDD(today())) - {n}
                  and warehouseId in (36, 352, 353, 769)
                order by toDate(createTime) desc,toDecimal64(totalCost,4) asc 
                limit 2  by sku, platform, country, warehouseId
            ) a
            join
            (
             select 
                sku,toFloat64(new_price) new_price,pur_length_pack,pur_width_pack,pur_height_pack,pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) b
            on (a.sku = b.sku)
            join 
            (
                select sku, warehouse_id as warehouseId, available_stock
                  from yb_datacenter.v_oversea_stock 
            ) g
            on (a.sku = g.sku and a.warehouseId = g.warehouseId)
            left join 
            (
              select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee 
              from yibai_wish.yibai_platform_fee
            ) c
            on (a.country = c.country and a.platform = c.platform)
            left join 
            (
               select 
                     sku, groupArray((platform,country,limit_price_rmb)) as limit_arr
                 from 
                     yibai_oversea.sku_limit_price 
               group by sku
            ) d
            on (a.sku = d.sku)
        """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    #
    # 20220711取最新头程
    sql = """SELECT warehouse_id as warehouseId,price FROM `yibai_toucheng` """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_toucheng = conn.read_sql(sql)
    df = df.merge(df_toucheng, on=['warehouseId'])
    df['price'] = df['price'].astype('float')
    df[['weight_volume', 'pur_weight_pack']] = df[['weight_volume', 'pur_weight_pack']].astype(float)
    df['计费重'] = np.where(df['weight_volume']/6000 > df['pur_weight_pack']/1000,
                           df['weight_volume']/6000, df['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,847]),
                                          df['firstCarrierCost'], df['weight_volume'] / 1000000 * df['price'])
    df['totalCost'] = df['totalCost'].astype('float')
    df['firstCarrierCost'] = df['firstCarrierCost'].astype('float')
    df['new_firstCarrierCost'] = df['new_firstCarrierCost'].astype('float')
    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    df['totalCost'] = df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost']
    df.drop(columns=['price', 'weight_volume', '计费重', 'pur_weight_pack'], inplace=True, axis=1)
    #
    df = df.groupby(['sku', 'warehouse'], group_keys=False).apply(lambda x: filter_by_platform(x)).reset_index(
        drop=True)
    df = df[['sku','warehouseName', 'platform', 'country','warehouse', 'shipName','totalCost','shippingCost',
             'firstCarrierCost','new_firstCarrierCost', 'dutyCost','overseasFee']]
    df['warehouse_country'] = 'AU'
    df = df.rename(columns={'warehouseName':'best_warehouse_name'})
    return df

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
            WHERE date = '{date_today}' and status in (0,1) and ve.warehouse_name not like '%独享%'
            -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
        ) a
        GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_age = conn_ck.ck_select_to_df(sql)
    return df_age
##
def ali_get_ship_name():
    """
    取运费渠道
    """
    n = 0
    sql = f"""   select 
            a.sku as sku, a.warehouseId as warehouseId, warehouseName best_warehouse_name, logisticsId, shipCode, shipName, totalCost, shippingCost, 
            remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
            firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
            a.country as country,
            round(toDecimal64(1.2,4)*(toFloat64(a.shippingCost)+toFloat64(a.extraSizeFee)) 
                    / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price,
            if(isNull(b.new_price),bb.new_price,b.new_price) as new_price,
            if (isNull(b.pur_weight_pack), bb.pur_weight_pack, b.pur_weight_pack) as pur_weight_pack,
            toFloat64( if (isNull(b.pur_length_pack), bb.pur_length_pack, b.pur_length_pack)) *
            toFloat64( if (isNull(b.pur_width_pack), bb.pur_width_pack, b.pur_width_pack)) *
            toFloat64( if (isNull(b.pur_height_pack), bb.pur_height_pack, b.pur_height_pack)) as weight_volume,
            0 as limit_price_rmb,
            null as zero_percent,
            null as five_percent,
            available_stock,
            w.warehouse as warehouse,
            son_sku
            from 
            (
                select 
                    * except(date_id),
                    case when subString(sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                         subString(sku, 4)
                    when subString(sku, 1, 2) in ['DE', 'GB'] then 
                         subString(sku, 3)
                    when subString(sku, -2) in ['DE', 'GB'] then 
                         subString(sku, 1, -2)
                    else sku
                    end as son_sku 
                from yibai_oversea.oversea_transport_fee_daily
                where not (platform in ['AMAZON', 'EB', 'WALMART']
                  and shipName in ['谷东-UPS_一票多箱_Multi', '谷东_FEDEX_OVERNIGHT[Fedex_标准次日达]', '谷东_FEDEX_2DAY',
                                             '谷西_FEDEX_2DAY', '谷西_FEDEX_OVERNIGHT[Fedex_标准次日达]'])
                  and date_id =  toYYYYMMDD(today()) - {n}
                  and shipCode not in ('GCHWC_G2G_STANDARD_1','GCHWC_241217001','GCHWC_241217002')
                  and platform = 'AMAZON'
                  and warehouseId in (1475, 325, 1520, 341, 50, 1473, 1521, 49, 77, 1477)
                order by toDate(createTime) desc,toDecimal64(totalCost,4) asc limit 1
                by sku, platform, country, warehouseId
            ) a
            INNER JOIN (
                SELECT
                    id as warehouse_id, name as warehouse_name, code as warehouse_code, type, country,
                    CASE 
                        WHEN country='US' THEN '美国仓'
                        WHEN country='UK' THEN '英国仓'
                        WHEN country='GB' THEN '英国仓'
                        WHEN country in ('CS','CZ') THEN '德国仓'
                        WHEN country='FR' THEN '法国仓'
                        WHEN country='IT' THEN '意大利仓'
                        WHEN country='AU' THEN '澳洲仓'
                        WHEN country='ES' THEN '西班牙仓'
                        WHEN country='CA' THEN '加拿大仓'
                        WHEN country='DE' THEN '德国仓'
                        WHEN country='JP' THEN '日本仓'
                        WHEN country='PL' THEN '德国仓'
                        WHEN country='MX' THEN '墨西哥仓'
                        WHEN country='UY' THEN '乌拉圭仓'
                        WHEN country='BR' THEN '巴西仓'
                        WHEN country='RU' THEN '俄罗斯仓'
                        ELSE NULL 
                    END AS warehouse
                FROM yb_datacenter.yb_warehouse
                WHERE type IN ('third', 'overseas') and warehouse in ('德国仓', '法国仓','西班牙仓','美国仓')
            ) w ON a.warehouseId = w.warehouse_id
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) b
            on (a.sku = b.sku)
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) bb
            on (a.son_sku = bb.sku)
            join 
            (
                select sku, warehouse_id as warehouseId, available_stock
                  from yb_datacenter.v_oversea_stock 
            ) g
            on (a.sku = g.sku and a.warehouseId = g.warehouseId)
            left join 
            (
              select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee 
              from yibai_wish.yibai_platform_fee
            ) c
            on (a.country = c.country and a.platform = c.platform)
            left join 
            (
               select 
                     sku, groupArray((platform,site,limit_price_rmb)) as limit_arr
                 from 
                     domestic_warehouse_clear.sku_limit_price 
               group by sku
            ) d
            on (a.sku = d.sku)
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(f'指定渠道运费数据共{len(df)}条。')
    # 自算头程替换函数
    df = get_toucheng_price_new(df)

    print(df.info())
    return df
##

def get_ali_sku_info():
    """
    给速卖通的海外仓全量数据。
    匹运费时剔除了部分不可用渠道
    """
    df = main()
    # ali只保留部分大仓数据
    df = df[df['大仓'].isin(['德国仓','美国仓','西班牙仓','法国仓','英国仓','澳洲仓','墨西哥仓'])]
    df_new_fee = ali_get_ship_name()
    col = ['sku', 'best_warehouse_name', 'warehouse', 'platform', 'country', 'totalCost_origin','totalCost',
           'shippingCost', 'firstCarrierCost',
            'new_firstCarrierCost','dutyCost', 'overseasFee', 'shipName']
    df_temp = df_new_fee[col]
    df_temp = df_temp.rename(columns={'best_warehouse_name':'最优子仓','country':'目的国'})
    df = pd.merge(df, df_temp, how='left', on=['sku','最优子仓','目的国'])

    c1 = (df['大仓'].isin(['德国仓','西班牙仓','法国仓','美国仓'])) & (
        df['尾程渠道'].isin(['GC-Yun Express德国国际派送','GC-美西Amazon Ground本地派送','GC-美东Amazon Ground本地派送']))
    df.loc[c1, '总运费_分摊头程'] = df.loc[c1, 'totalCost_origin']
    df.loc[c1, '总运费_自算头程'] = df.loc[c1, 'totalCost']
    df.loc[c1, '尾程'] = df.loc[c1, 'shippingCost']
    df.loc[c1, '分摊头程'] = df.loc[c1, 'firstCarrierCost']
    df.loc[c1, '自算头程'] = df.loc[c1, 'new_firstCarrierCost']
    df.loc[c1, '关税'] = df.loc[c1, 'dutyCost']
    df.loc[c1, '海外仓处理费'] = df.loc[c1, 'overseasFee']
    df.loc[c1, '尾程渠道'] = df.loc[c1, 'shipName']

    save_df(df, '海外仓全量数据_ali', file_type='xlsx')

    return df

def site_table():
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = """
    SELECT site, site1, area as area0
    FROM domestic_warehouse_clear.site_table
    """
    # df_site = pd.DataFrame(con.execute(sql), columns=['site', 'site1', 'area'])
    df_site = conn_ck.ck_select_to_df(sql)
    df_site['area'] = np.where(df_site['site1'].str.contains('德国|法国|西班|意大利|荷兰|瑞士|波兰|比利|土耳其'), '欧洲',
                               df_site['site1'])

    return df_site

def get_aliexpress_listing_temp(df_site):
    print("===aliexpress刊登链接数据===")
    sql = """
        SELECT 
            a.product_id,a.sku sku,sku_price,sku_code, aeop_s_k_u_property_list_str, b.property_value_id, 
            b.sku_property_id, c.name_en, e.account_id, d.short_name
        FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing_skus a
        INNER JOIN (
            SELECT distinct account_id,product_id,freight_template_id 
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing 
            WHERE product_status_type = 1
        ) e ON a.product_id = e.product_id
        INNER JOIN (
            SELECT
                aeop_s_k_u_property_list,aeop_s_k_u_property_list_str,
                arrayJoin(JSONExtractArrayRaw(aeop_s_k_u_property_list_str)) as aeop_ae_product_skus1,
                visitParamExtractFloat(aeop_ae_product_skus1, 'property_value_id') as property_value_id,
                visitParamExtractFloat(aeop_ae_product_skus1, 'sku_property_id') as sku_property_id
            FROM yibai_domestic.yibai_aliexpress_listing_skus_aeop_s_k_u_property_list
            -- 国外发货地的链接
            WHERE sku_property_id=200007763 and property_value_id!=201336100
        ) b ON a.aeop_s_k_u_property_list = b.aeop_s_k_u_property_list
        -- 具体国外发货地
        LEFT JOIN (
            SELECT DISTINCT parent_attr_id, attr_id, name_en
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_category_attribute
        ) c ON toInt64(b.sku_property_id) = toInt64(c.parent_attr_id) and toInt64(b.property_value_id) = toInt64(c.attr_id)
        LEFT JOIN (
            SELECT id, short_name
            FROM yibai_sale_center_system_sync.yibai_system_account
            WHERE platform_code = 'ALI' and is_del=0 and `status`=1
        ) d ON e.account_id = d.id
        where a.sku !='' 
    """
    conn_ck = pd_to_ck(database='temp_database_hxx', data_sys='调价明细历史数据')
    df_ali_listing = conn_ck.ck_select_to_df(sql)
    df_ali_listing.columns = [i.split('.')[-1] for i in df_ali_listing.columns]
    #
    df_ali_listing[['property_value_id', 'sku_property_id']] = df_ali_listing[['property_value_id', 'sku_property_id']].astype(int).astype(str)
    dic = {'United States':'US', 'CZECH REPUBLIC':'DE', 'Czech Republic':'DE', 'Poland':'PL', 'france':'FR', 'France':'FR',
           'Australia':'AU', 'CN':'CN', 'spain':'ES', 'SPAIN':'ES', 'Russian Federation':'RU', 'UNITED KINGDOM':'UK',
           'United Kingdom':'UK','GERMANY':'DE', 'Mexico':'MX', 'cz':'DE', 'ITALY':'IT', 'Italy':'IT'}
    df_ali_listing['country'] = df_ali_listing['name_en'].replace(dic)
    df_ali_listing = df_ali_listing.drop_duplicates(subset=['product_id','sku','country'])
    #
    df_ali_listing = df_ali_listing[df_ali_listing['country']!='CN']
    #
    # 按 SKU + country 聚合计算 item的数量
    df = df_ali_listing[~df_ali_listing['country'].isna()]
    df = df.groupby(['sku', 'country','short_name'])['product_id'].count().reset_index()
    df.rename(columns={'product_id': 'aliexpress_listing_num', 'country': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')

    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
                             df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                             df['sku'].str[:-2], df['new_sku'])
    df_account = df.groupby(['new_sku','site','area','short_name'])['aliexpress_listing_num'].sum().reset_index()
    # df_account['链接分布'] = df['short_name'] + ':' + df['aliexpress_listing_num'].astype(str)
    df_account['链接分布'] = df['short_name'] + '；'
    df = df_account.groupby(['new_sku', 'site', 'area']).agg({'aliexpress_listing_num':'sum','链接分布':list}).reset_index()

    return df

## ebay清仓兜底数据
def get_ebay_price():
    """ 获取ebay清仓兜底价格信息 """
    sql = """
        SELECT sku, warehouse, best_warehouse_name, available_stock, sales_status, overage_level, 
        new_price, total_cost, lowest_profit, country, lowest_price
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE platform = 'EB' and date_id = (
            SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl
            WHERE date_id > '2025-03-20'
        )
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    # df = conn.read_sql(sql)
    df['lowest_profit'] = np.where(df['overage_level'].isin([0, 30]), 0.05, df['lowest_profit'])
    print(df.info())

    df_rate = get_rate()

    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    df['lowest_price'] = df['lowest_price']/df['rate']
    # 差值
    df_platform_fee = get_platform_fee()
    df_platform_fee = df_platform_fee[df_platform_fee['platform']=='EB']
    df = pd.merge(df, df_platform_fee[['country', 'ppve','platform_zero']], how='left', on=['country'])

    # 计算兜底价
    df['兜底价_本币'] = (df['new_price']+df['total_cost'])/(1-df['ppve']-df['platform_zero']-df['lowest_profit'])/df['rate']
    df['兜底价_本币'] = np.where(df['兜底价_本币']<df['lowest_price'], df['lowest_price'], df['兜底价_本币'])
    col = ['sku','warehouse','best_warehouse_id','available_stock', 'sales_status', 'overage_level',
           'lowest_profit', 'country', '兜底价_本币']
    df = df[col]
    df.columns=['sku','warehouse','最优子仓','大仓可用库存', '销售状态', '超库龄等级',
           '兜底净利率', '目的国', '兜底价_本币']

    df.to_excel('F://Desktop//df_ebay_price.xlsx', index=0)

##
def get_ban_info(df):
    # 侵权违禁信息数据读取
    # 读取侵权信息数据表
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    sql_info = f"""
    SELECT sku, arrayStringConcat(groupArray(country_code), ',') AS info_country, any(risk_grade) as `侵权信息`, any(risk_grade_type) as `侵权等级` 
    FROM yibai_prod_base_sync.yibai_prod_inf_country_grade
    WHERE is_del = 0 
    GROUP BY sku
    """
    df_info = conn_ck.ck_select_to_df(sql_info)
    df_info['sku'] = df_info['sku'].astype('str')
    # 读取禁售信息数据表
    sql_js = f"""
    SELECT sku ,arrayStringConcat(groupArray(platform_code), ',') as forbid_plateform, any(risk_grade) as `禁售信息` , any(risk_grade_type) as `禁售等级` 
    FROM yibai_prod_base_sync.yibai_prod_forbidden_grade
    WHERE is_del = 0 
    GROUP BY sku
    """
    df_js = conn_ck.ck_select_to_df(sql_js)
    df_js['sku'] = df_js['sku'].astype('str')

    df = pd.merge(df, df_js, on='sku', how='left')
    df = pd.merge(df, df_info, on='sku', how='left')

    return df

def dwm_sku_temp():
    """ chris 小平台sku库存信息 """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        select sku, type, linest, warehouse, best_warehouse_name, available_stock, new_price, date_id
        from dwm_sku_temp_info 
        WHERE date_id >= '{date_today}' 
        and warehouse in ('美国仓', '澳洲仓', '德国仓','英国仓')
        and available_stock > 0
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # 侵权违禁
    df = get_ban_info(df)

    df.to_excel('F://Desktop//dwm_sku_temp.xlsx', index=0)

    return None

def dwm_sku_temp_info():
    """ 海外仓低销sku数量及占比变化 """
    utils.program_name = '海外仓全量数据'
    make_path()

    # 低销
    date_id = '2026-02-17'
    sql = f"""
    SELECT date_id, `30days_sales`, count(1) as sku_num, sum(available_stock) as available_stock, 
    sum(available_stock_money) available_stock_money
    FROM (
        SELECT sku, title, type, warehouse, best_warehouse_name, available_stock, new_price,available_stock_money, overage_level,
        age_90_plus, `30days_sales`, day_sales, date_id
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_id}' and available_stock > 0 and `30days_sales` <= 3 and best_warehouse_id != 958
        and best_warehouse_name not like '%%精铺%%' 
        and best_warehouse_name not like '%%精品%%' 
        and best_warehouse_name not like '%%凯美晨%%' 
    ) a
    GROUP BY date_id, `30days_sales`
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df2 = conn.read_sql(sql)

    sql =  f"""
    SELECT date_id, 
    sum(available_stock_money) available_stock_money
    FROM (
        SELECT sku, title, type, warehouse, best_warehouse_name, available_stock, new_price,available_stock_money, overage_level,
        age_90_plus, `30days_sales`, day_sales, date_id
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_id}' 
        and best_warehouse_id != 958
        and best_warehouse_name not like '%%精铺%%' 
        and best_warehouse_name not like '%%精品%%' 
        and best_warehouse_name not like '%%凯美晨%%' 
    ) a
    GROUP BY date_id 
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df3 = conn.read_sql(sql)
    # print(df.info())
    save_df(df3, '海外仓低销sku数量及占比变化_总', file_type='xlsx')
    # df1.to_excel('F://Desktop//df_dwm_all.xlsx', index=0)
    # df2.to_excel('F://Desktop//df_dwm.xlsx', index=0)
    save_df(df2, '海外仓低销sku数量及占比变化', file_type='xlsx')

def dwm_sku_temp_info_2():
    """ 海外仓低销sku数量及占比变化 """
    utils.program_name = '海外仓全量数据'
    make_path()

    sql =  """
        SELECT sku, title, type, warehouse, best_warehouse_name, available_stock, new_price,available_stock_money, overage_level,
        age_90_plus, `30days_sales`, day_sales, date_id
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id in ('2026-01-06', '2026-01-13') and available_stock > 0 
        -- and `30days_sales` <= 3
        and best_warehouse_id != 958
        and best_warehouse_name not like '%%精铺%%' 
        and best_warehouse_name not like '%%精品%%' 
        and best_warehouse_name not like '%%凯美晨%%' 
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df2 = conn.read_sql(sql)
    # print(df.info())
    df = df2[df2['date_id'] == df2['date_id'].max()]
    df_1 = df2[df2['date_id'] == df2['date_id'].min()]

    col = ['sku','warehouse','available_stock_money']
    df = pd.merge(df, df_1[col], how='left', on=['sku', 'warehouse'])
    # df1.to_excel('F://Desktop//df_dwm_all.xlsx', index=0)
    # df2.to_excel('F://Desktop//df_dwm.xlsx', index=0)
    save_df(df, '海外仓低销sku数量及占比变化_对比明细3', file_type='xlsx')


def get_platform_lowest_price(platform='CDISCOUNT'):
    """  各平台销毁价数据 """
    sql = f"""
        SELECT 
            sku, warehouse, best_warehouse_name, available_stock, overage_level, sales_status,
            platform, country, lowest_price, is_distory is_destroy, date_id
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE platform = '{platform}' 
        and date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl WHERE date_id > '2025-07-01')
        and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())

    sql = f"""
     SELECT 
         sku, warehouseId as warehouse_id, warehouseName as best_warehouse_name, totalCost as total_cost, totalCost_origin,
         firstCarrierCost, dutyCost, new_firstCarrierCost, (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
         shipName as ship_name,lowest_price, platform, country, warehouse
     FROM oversea_transport_fee_useful
     WHERE platform = '{platform}'
     """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)

    col = ['sku','best_warehouse_name', 'platform', 'country', 'ship_fee', 'firstCarrierCost']
    df = pd.merge(df, df_fee[col], how='left', on=['sku','best_warehouse_name', 'platform', 'country'])

    sql = f"""
    SELECT 
        platform, site as country, pay_fee, paypal_fee, vat_fee, extra_fee, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    WHERE platform = '{platform}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_platform_fee = conn.read_sql(sql)

    df = pd.merge(df, df_platform_fee[['platform','country','pay_fee', 'vat_fee','extra_fee','refound_fee']],
                  how='left', on=['platform','country'])

    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country','rate']], how='left', on='country')

    df.to_excel('F://Desktop//df_platform_lowest.xlsx', index=0)


def get_all_latform_lowest_price():
    """  各平台销毁价数据 """
    sql = f"""
        SELECT 
            sku, type, warehouse, best_warehouse_name, available_stock, overage_level, new_price
        FROM over_sea.dwm_sku_temp_info
        WHERE warehouse = '美国仓' 
        and date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-07-01')
        and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    print(df.info())

    sql = f"""
     SELECT 
         sku, warehouseId as warehouse_id, warehouseName as best_warehouse_name, totalCost as total_cost, totalCost_origin,
         firstCarrierCost, dutyCost, new_firstCarrierCost, (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
         shipName as ship_name,lowest_price, platform, country
     FROM oversea_transport_fee_useful
     WHERE warehouse = '美国仓' and platform not in ('WISH')
     
     union all
     
    SELECT 
         sku, warehouseId as warehouse_id, warehouseName as best_warehouse_name, totalCost as total_cost, totalCost_origin,
         firstCarrierCost, dutyCost, new_firstCarrierCost, (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
         shipName as ship_name,lowest_price, platform, country
     FROM oversea_transport_fee_useful_temu
     WHERE warehouse = '美国仓' 
     """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)

    col = ['sku','best_warehouse_name', 'platform', 'country', 'totalCost_origin', 'ship_fee', 'firstCarrierCost']
    df = pd.merge(df, df_fee[col], how='left', on=['sku','best_warehouse_name'])

    sql = f"""
    SELECT 
        platform, site as country, pay_fee, paypal_fee, vat_fee, extra_fee, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_platform_fee = conn.read_sql(sql)

    df = pd.merge(df, df_platform_fee[['platform','country','pay_fee', 'vat_fee','extra_fee','refound_fee']],
                  how='left', on=['platform','country'])

    df.to_excel('F://Desktop//df_all_platform_lowest.xlsx', index=0)


def get_season_sku():
    """ 季节性产品标签 """
    sql = """
    SELECT DISTINCT sku, 1 as `冬季产品_产品系统标记`
    from yibai_prod_base_sync.yibai_prod_sku 
    where (feature_val  like '%4%' and feature_val not like '%1%' and feature_val not like '%2%' 
    and feature_val not like '%3%')  or for_holiday = 1
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())

    # df.to_excel('F://Desktop//df_season_sku.xlsx', index=0)
    return df


def dwm_sku_clear():
    sql = f"""
        select sku, warehouse, title, linest, type, best_warehouse_name, new_price, available_stock, available_stock_money,
        overage_level, age_180_plus, estimated_sales_days
        from dwm_sku_temp_info 
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info WHERE date_id > '2025-07-20')
        and available_stock > 0
        and warehouse in ('美国仓', '加拿大仓', '英国仓', '德国仓','法国仓','意大利仓','西班牙仓','澳洲仓')
        and overage_level >= 180
        and estimated_sales_days >= 150
        and type not in ('通拓', '通拓精铺','通拓跟卖','海兔转泛品','易通兔')
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df_season = get_season_sku()
    df = pd.merge(df, df_season, how='left', on='sku')
    df['冬季产品_产品系统标记'] = df['冬季产品_产品系统标记'].fillna(0)
    df_jihua = pd.read_excel('F://Ding_workspace//冬季产品.xlsx', dtype={'sku': str})
    df = pd.merge(df, df_jihua[['sku', 'warehouse', '冬季产品_计划系统标记']], how='left', on=['sku', 'warehouse'])

    df.to_excel('F://Desktop//dwm_sku_temp.xlsx', index=0)

    return None

def get_yb_clear_order():
    """

    """
    df_sku = pd.read_excel('F://Desktop//YB清仓sku7月.xlsx', dtype={'sku':str})
    df_sku = df_sku.drop_duplicates(subset=['sku','warehouse'])
    df_sku['is_clear'] = 1
    date_start = '2025-07-01'
    date_end = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and paytime < '{date_end}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            -- and platform_code = 'AMAZON'
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)

    df_order_info = pd.merge(df_order_info, df_sku[['sku','warehouse','is_clear']], how='left', on=['sku','warehouse'])
    df_order_info.to_excel('F://Desktop//df_yb_clear_order_all.xlsx', index=0)


def get_tt_sku_stock():
    """ 获取TTsku库存信息 """
    sql = """
        SELECT *
        FROM dwm_sku_temp_info 
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info WHERE date_id > '2025-07-20')
        and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//df_tt_sku_stock.xlsx', index=0)


def get_sku_line():
    df = pd.read_excel('F://Desktop//sku_temp.xlsx', dtype={'sku': str})

    sku_line = get_line_new(df)

    df = pd.merge(df, sku_line, how='inner', on=['sku'])

    df.to_excel('F://Desktop//df_sku_line.xlsx', index=0)

if __name__ == '__main__':
    # df = main()
    get_ali_sku_info()
    # get_ebay_price()
    # dwm_sku_temp() # 小平台库存数据
    # dwm_sku_temp_info() # 异常低销sku数量及库存金额变化
    # dwm_sku_temp_info_2()  # 异常低销sku数量及库存金额变化
    # dwm_sku_clear() # 每周YB清仓sku数据明细
    # get_yb_clear_order() # 每周YB清仓sku的销库数据
    # get_tt_sku_stock() # tt库存信息

    # get_platform_lowest_price(platform='SHOPEE')
    # get_all_latform_lowest_price()
    # get_sku_line()