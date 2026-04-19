
"""
定价比计算程序：
1、海外仓sku定价比计算，存入CK
"""

##
import time
import warnings
from tqdm import tqdm
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd, connect_to_sql
import datetime
import pandas as pd
import numpy as np
from utils.utils import save_df, make_path
from utils import utils
from pulic_func.price_adjust_web_service.AMAZON_FBA_dingjia import fba_ding_jia_biao, aut
from pulic_func.price_adjust_web_service.daingjia_public import  chicun_zhongliang, sku_and_num_split
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
warnings.filterwarnings('ignore')
from all_auto_task.pricing_ratio_monitor import merge_first_product_line, merge_four_dim_diff, get_diff_new, \
    get_cost_range, get_sku_new
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from concurrent.futures._base import as_completed
##
def get_oversea_sku_order():
    """
    获取海外仓sku + 站点的销量数据
    """
    date_today = time.strftime('%Y-%m-%d')
    date_start = datetime.date.today() - datetime.timedelta(days=100)
    date_today = datetime.date.today() - datetime.timedelta(days=60)
    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_today}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)
            -- and platform_code ='EB' 
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time as paytime, C.total_price,
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        CASE
            when order_status=1 then '下载'
            when order_status=10 then '待确认'
            when order_status=20 then '初始化'
            when order_status=30 then '正常'
            when order_status=40 then '待处理'
            when order_status=50 then '部分出库'
            when order_status=60 then '已出库'
            when order_status=70 then '已完结'
            when order_status=80 then '已取消'
            ELSE ''  
        END AS complete_status,
        case
            when B.order_type=1 then '常规线上平台客户订单'
            when B.order_type=2 then '线下客户订单'
            when B.order_type=3 then '线上客户补款单'
            when B.order_type=4 then '重寄单'
            when B.order_type=5 then '港前重寄'
            when B.order_type=6 then '虚拟发货单'
            ELSE '未知'
        END AS order_typr,
        CASE
            WHEN B.payment_status = 1 THEN '已付款' 
            ELSE '未付款'
        END AS pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id,
        W.warehouse_name,
        W.warehouse,
        A.ship_country,
        A.quantity
        -- F.sales_status as sales_status,
        -- C.product_price,C.shipping_price, commission_fees, pay_cost, escrow_tax, purchase_cost_new1,
        -- shipping_cost, ship_cost_second,true_shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, 
        -- extra_price,exceedprice,stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) A
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
        LEFT JOIN (
            SELECT 
                yw.id AS warehouse_id,   
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
                yw.country, ebay_category_id,
                ywc.name AS warehouse, yw.warehouse_type
            FROM yibai_logistics_tms_sync.yibai_warehouse yw
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
        ) W ON B.warehouse_id = W.warehouse_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    # 订单表
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and paytime < '{date_today}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df_temp, df[['order_id', 'ship_country']], how='inner', on=['order_id'])
    print(df.info())
    df['ship_country'] = df['ship_country'].replace('GB', 'UK')
    col1 = ['德国仓', '美国仓', '澳洲仓', '英国仓', '加拿大仓', '墨西哥仓']
    col2 = ['DE', 'US', 'AU', 'UK', 'CA', 'MX']
    df = df[(df['warehouse'].isin(col1)) & (df['ship_country'].isin(col2))]
    df = df[df['sales_status'] == '正常']
    col = ['total_price', 'true_profit_new1', 'real_profit']
    df[col] = df[col].astype(float)
    df_group = df.groupby(['sku', 'ship_country']).agg({
        'total_price': 'sum', 'true_profit_new1': 'sum', 'real_profit': 'sum'}).reset_index()
    df_group = df_group.rename(columns={'ship_country': 'country'})
    # df.to_excel('F://Desktop//df_eb_order.xlsx', index=0)
    print(df_group.info())
    return df_group

def warehouse_mark(s, codes):
    for code in codes:
        # 替换前缀，包括连字符
        if s.startswith(f"{code}-"):
            s = s[len(code) + 1:]
        # 替换后缀，包括连字符
        elif s.endswith(f"-{code}"):
            s = s[:-len(code) - 1]
        elif s.endswith(f"{code}"):
            s = s[:-len(code)]
    return s

# df_group = get_oversea_sku_order()
# ##
# df_group.to_excel('F://Desktop//df_group.xlsx', index=0)
##
def get_fbm_fee(df_fbm):
    """
    获取国内仓运费
    """
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_fbm['new_sku'] = df_fbm['sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    conn_ck_0 = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器2')
    df = pd.DataFrame()
    for i in df_fbm['站点'].unique():
        df_temp = df_fbm[df_fbm['站点'] == i]
        sql = f"""
            SELECT distinct sku new_sku, total_cost as `fbm运费`,site as `站点`  
            from yibai_temp_hxx.freight_interface_amazon 
            WHERE site = '{i}'
            order by total_cost asc limit 1 by sku
            """
        df_yunfei0 = conn_ck_0.ck_select_to_df(sql)
        df_temp = pd.merge(df_temp, df_yunfei0, how='left', on=['new_sku', '站点'])
        df_yunfei1 = df_yunfei0.copy()
        df_yunfei1 = df_yunfei1.rename(columns={'new_sku':'sku','fbm运费':'fbm运费_new'})
        df_temp = pd.merge(df_temp, df_yunfei1, how='left', on=['sku', '站点'])
        df_temp['fbm运费'] = np.where(df_temp['fbm运费'].isna(), df_temp['fbm运费_new'], df_temp['fbm运费'])
        df_temp.drop('fbm运费_new', axis=1, inplace=True)
        df = pd.concat([df_temp, df])

    return df

def get_fbm_fee_temp():
    """
    获取国内仓运费
    """
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    # df_fbm['new_sku'] = df_fbm['sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    df_fbm = pd.read_excel('F://Desktop//df_us_sku.xlsx', dtype={'new_sku':str})
    print(df_fbm.info())
    conn_ck_0 = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器2')
    df = pd.DataFrame()
    for i in df_fbm['站点'].unique():
        df_temp = df_fbm[df_fbm['站点'] == i]
        sql = f"""
            SELECT distinct sku new_sku, total_cost as `fbm运费`,site as `站点`  
            from yibai_temp_hxx.freight_interface_amazon 
            WHERE site = '{i}'
            order by total_cost asc limit 1 by sku
            """
        df_yunfei0 = conn_ck_0.ck_select_to_df(sql)
        df_temp = pd.merge(df_temp, df_yunfei0, how='left', on=['new_sku', '站点'])
        df_yunfei1 = df_yunfei0.copy()
        df_yunfei1 = df_yunfei1.rename(columns={'new_sku':'sku','fbm运费':'fbm运费_new'})
        df_temp = pd.merge(df_temp, df_yunfei1, how='left', on=['sku', '站点'])
        df_temp['fbm运费'] = np.where(df_temp['fbm运费'].isna(), df_temp['fbm运费_new'], df_temp['fbm运费'])
        df_temp.drop('fbm运费_new', axis=1, inplace=True)
        df = pd.concat([df_temp, df])

    df.to_excel('F://Desktop//df_us_fbm_fee.xlsx', index=0)
    return df

def get_fbm_diff(df_temp):
    """

    """
    df_diff = get_diff_new()
    df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    df_fbm = get_cost_range(df_temp, df_diff_fbm)
    df_fbm = merge_four_dim_diff(df_fbm, df_diff_fbm, ['站点', 'first_product_line', 'cost_range'])
    df_fbm['net_profit2'] = (df_fbm['net_profit2'] / 100).astype('float')
    df_fbm['net_interest_rate_target'] = (df_fbm['net_interest_rate_target'] / 100).astype('float')
    df_fbm['FBM目标毛利润率'] = df_fbm['net_profit2'] + df_fbm['net_interest_rate_target']
    df_fbm['FBM差值'] = df_fbm['net_profit2']
    col = ['sku', '站点', 'FBM目标毛利润率', 'FBM差值']
    df_fbm = df_fbm[col]
    df_temp = pd.merge(df_temp, df_fbm, how='left', on=['sku', '站点'])
    df_temp = df_temp.drop_duplicates()
    df_temp.drop(['first_product_line', 'cost', 'FBM差值'], axis=1, inplace=True)
    return df_temp

def get_oversea_diff():
    """
    获取配置表：平台费率、差值等
    """
    sql = """
    SELECT 
        site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, platform_must_percent,
        (platform_must_percent + platform_zero) as `海外仓毛利率`
    FROM yibai_platform_fee
    WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df

def get_oversea_stock():
    """
    获取海外仓sku库存数据。
    保留非公共仓，例如独享仓、YB墨西哥跨境
    """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        select sku, warehouse, best_warehouse_name warehouse_name, available_stock, age_60_plus, day_sales, estimated_sales_days, esd_bins
        from dwm_sku_temp_info 
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info)
        and warehouse in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓')
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn.read_sql(sql)
    dic = {'美国仓': 'US', '加拿大仓': 'CA', '墨西哥仓': 'MX', '澳洲仓': 'AU', '英国仓': 'UK', '德国仓': 'DE'}
    df_temp['country'] = df_temp['warehouse'].replace(dic)
    # 获取销售状态
    sql = """
        SELECT sku ,warehouse ,sale_status 
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku_stauts = conn.read_sql(sql)
    df_temp = pd.merge(df_temp, df_sku_stauts, how='left', on=['sku','warehouse'])
    # df_temp = df_temp.sample(10000)
    return df_temp


def get_oversea_stock_old():
    """
    获取海外仓sku库存数据。
    保留非公共仓，例如独享仓、YB墨西哥跨境
    """
    date_today = time.strftime('%Y%m%d')
    sql = f"""
    SELECT
        sku, warehouse, country, sum(available_stock) as available_stock, max(new_price) as new_price
    FROM (
        SELECT
            ps.sku sku,  yw.id AS warehouse_id, yw.country country,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
            available_stock, allot_on_way_count AS on_way_stock, wait_outbound, frozen_stock, yps.new_price as new_price
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
            -- and yw.warehouse_other_type = 2   -- 筛选公告仓（非子仓）
            and yw.warehouse_name not in ('HC美国西仓','出口易美西仓-安大略仓','万邑通美南仓-USTX','亿迈CA01仓')  -- 剔除不常用仓库
            and ywc.name in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓','法国仓','俄罗斯仓','乌拉圭仓')
    ) a
    GROUP BY sku, warehouse, country
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    df_temp['country'] = df_temp['country'].replace({'GB': 'UK', 'CZ': 'DE', 'PL': 'DE', 'CS': 'DE'})
    # df_temp = df_temp.sample(10000)
    return df_temp


def tou_cheng_oversea(df, type='空运'):
    # 20240816 用于海外仓空海同价利率差计算
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = f"""
        SELECT 
            site as country,`普货单价` as `普货头程单价`, `头程计泡系数`,
            `敏感货单价` as `敏感头程单价`,`计费方式`,`关税计算方式`
        FROM yibai_oversea.oversea_fees_parameter
        WHERE `物流类型` = '{type}'
        """
    df1 = conn_ck.ck_select_to_df(sql)
    df1 = df1.sort_values(by='普货头程单价', ascending=False).drop_duplicates(subset=['country'])
    df1['country'] = df1['country'].replace({'美国': 'US', '英国': 'UK', '德国': 'DE', '法国': 'FR', '澳大利亚': 'AU',
                                             '加拿大': 'CA', '墨西哥': 'MX', '俄罗斯': 'RU'})
    df = df.merge(df1, on=['country'], how='left')
    df['体积重'] = df['长'] * df['宽'] * df['高'] / df['头程计泡系数']
    df['重量重'] = df['重量'] / 1000
    df['计费重'] = np.where(df['计费方式'] == '体积', df['体积重'], df[['体积重', '重量重']].max(axis=1))

    # 敏感货头程
    if type == '空运':
        df.loc[df['空运头程属性'].isin(['敏感', '带电']), '普货头程单价'] = df.loc[
            df['空运头程属性'].isin(['敏感', '带电']), '敏感头程单价']
    # 运费
    df['运费'] = df['计费重'] * df['普货头程单价']
    # 关税。主要是英国关税，按0.3/kg计算。  捷克0.5/kg
    df['关税'] = np.where(df['关税计算方式'] == 0, 0, df['计费重'] * 0.3)

    # 结果
    df[f'头程费_{type}'] = df['运费'] + df['关税']
    df[f'头程费_{type}'] = df[f'头程费_{type}'].apply(lambda m: round(m, 2))
    #
    df.drop(['普货头程单价', '敏感头程单价', '头程计泡系数', '体积重', '运费', '计费方式', '重量重',
             '关税', '关税计算方式'], axis=1, inplace=True)
    # 返回 sku、country、头程费_{type}、计费重
    return df


def get_transport_fee(df_sku):
    """
    取运费明细
    """
    sql = """   select 
            a.sku as sku, warehouseName warehouse_name, totalCost total_cost, shippingCost `尾程`,  
            firstCarrierCost `分摊头程`, dutyCost,
            a.country as country,
            round(toDecimal64(1.2,4)*(toDecimal64OrZero(a.shippingCost,4)+toDecimal64OrZero(a.extraSizeFee,4)) / 
            (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price
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
                where 
                  date_id =  toYYYYMMDD(today())
                  and shipName not in ('谷西_USPS-LWPARCEL[USPS-小包]', '谷东_USPS-LWPARCEL[USPS-小包]')
                  and (platform = 'AMAZON' or (platform = 'EB' and country = 'AU'))
                  and country in ('US','DE','UK','AU','CA','MX')
                order by toDate(createTime) desc,toDecimal64(totalCost,4) asc limit 1
                by sku, platform, country, warehouseId
            ) a
            INNER JOIN (
                SELECT
                    id as warehouse_id, name as warehouse_name, code as warehouse_code, type, country,
                    CASE 
                        WHEN country='US' THEN '美国仓'
                        WHEN country in ('UK', 'GB') THEN '英国仓'
                        WHEN country in ('CS', 'CZ', 'DE', 'PL') THEN '德国仓'
                        WHEN country='FR' THEN '法国仓'
                        WHEN country='IT' THEN '意大利仓'
                        WHEN country='AU' THEN '澳洲仓'
                        WHEN country='ES' THEN '西班牙仓'
                        WHEN country='CA' THEN '加拿大仓'
                        WHEN country='JP' THEN '日本仓'
                        WHEN country='MX' THEN '墨西哥仓'
                        WHEN country='UY' THEN '乌拉圭仓'
                        WHEN country='BR' THEN '巴西仓'
                        WHEN country='RU' THEN '俄罗斯仓'
                        ELSE NULL 
                    END AS warehouse
                FROM yb_datacenter.yb_warehouse
                WHERE type IN ('third', 'overseas') and warehouse IN ('美国仓','德国仓','英国仓','澳洲仓','加拿大仓','墨西哥仓')
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
                     sku, groupArray((platform,country,limit_price_rmb)) as limit_arr
                 from 
                     yibai_oversea.sku_limit_price 
               group by sku
            ) d
            on (a.sku = d.sku)
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df = df.drop_duplicates(subset=['sku', 'warehouse_name', 'country'])
    #
    df_sku = pd.merge(df_sku, df, how='left', on=['sku', 'warehouse_name', 'country'])
    df_sku[['total_cost', '分摊头程', 'dutyCost']] = df_sku[['total_cost', '分摊头程', 'dutyCost']].fillna(0).astype(
        float)
    df_sku['总运费_空运头程'] = np.where(df_sku['total_cost'] == 0, df_sku['total_cost'],
                                         df_sku['total_cost'] - df_sku['分摊头程'] - df_sku['dutyCost'] + df_sku[
                                             '头程费_空运'])
    df_sku['总运费_慢海头程'] = np.where(df_sku['total_cost'] == 0, df_sku['total_cost'],
                                         df_sku['total_cost'] - df_sku['分摊头程'] - df_sku['dutyCost'] + df_sku[
                                             '头程费_慢海'])

    df_sku.drop('dutyCost', axis=1, inplace=True)
    return df_sku

def get_transport_fee_useful(df_sku):
    """
    取调价总运费
    """
    sql = """
        SELECT 
            distinct sku, warehouseName warehouse_name, country,
            totalCost as `总运费_调价头程`, new_firstCarrierCost `自算头程`
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON' or (platform = 'EB' and country = 'AU')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)
    df_fee = df_fee.drop_duplicates(subset=['sku', 'warehouse_name', 'country'])
    #
    df_sku = pd.merge(df_sku, df_fee, how='left', on=['sku', 'warehouse_name', 'country'])
    col = ['总运费_调价头程','自算头程']
    df_sku[col] = df_sku[col].fillna(0).astype(float)

    df_sku['总运费_慢海头程'] = np.where(df_sku['总运费_调价头程'] != 0, df_sku['总运费_调价头程'], df_sku['总运费_慢海头程'])
    df_sku['头程费_慢海'] = np.where(df_sku['自算头程'] != 0, df_sku['自算头程'], df_sku['头程费_慢海'])

    return df_sku

def get_target_profit(df):
    """ 获取amaozn当前调价目标毛利率"""
    date_today = time.strftime('%Y%m%d')
    sql = """
        SELECT 
            sku, warehouse, target_profit_rate, country
        FROM over_sea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info)
        and platform = 'AMAZON'
        and warehouse in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)

    df = pd.merge(df, df_fee, how='left', on=['sku','warehouse','country'])
    # df['target_profit_rate'] = np.where(~df['target_profit_rate'].isna(), df['target_profit_rate'],
    #                                     df['海外仓毛利率']-df['platform_must_percent'])
    df['调价目标毛利率'] = df['target_profit_rate'] + df['platform_must_percent']
    df['海外仓毛利率'] = np.where(~df['调价目标毛利率'].isna(), df['调价目标毛利率'], df['海外仓毛利率'])

    return df
##
def pricing_oversea():
    """
    计算海外仓sku的空海同价利率差
    """
    utils.program_name = '海外仓SKU空海同价利率差'
    make_path()
    df_stock = get_oversea_stock()
    print(df_stock.info())
    # 取尺寸重量
    df_stock['数量'] = 1
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock = chicun_zhongliang(df_stock, 1, conn_ck)
    df_stock = df_stock[['sku', 'warehouse', 'available_stock', 'sale_status', 'warehouse_name','day_sales',
                         'age_60_plus', 'estimated_sales_days', 'esd_bins','country', '成本', '重量', '长', '宽', '高']]
    df_stock = df_stock[~df_stock['成本'].isna()]
    df_stock['sku1'] = df_stock['sku']
    df_stock['country_code'] = df_stock['country']

    # 取空运头程属性
    aut_ky = aut(df_stock, conn_ck, '空运')
    aut_ky = aut_ky.rename(columns={'fba头程属性': '空运头程属性', 'sku1': 'sku', 'country_code': 'country'})
    df_stock = pd.merge(df_stock, aut_ky, how='left', on=['sku', 'country'])
    df_stock['空运头程属性'] = df_stock['空运头程属性'].fillna('普货')
    df_stock.drop(['sku1', 'country_code'], axis=1, inplace=True)
    # 计算头程运费
    df_para = tou_cheng_oversea(df_stock, '空运')
    df_para.drop('计费重', axis=1, inplace=True)
    df_para = tou_cheng_oversea(df_para, '慢海')
    df_para = df_para.rename(columns={'计费重': '慢海计费重'})
    #
    df_para = df_para[df_para['country'].isin(['US', 'DE', 'UK', 'AU', 'CA', 'MX'])]

    # 当无库存时，子仓替换为常用的子仓
    dic = {'澳洲仓': '谷仓澳洲悉尼仓', '德国仓': '谷仓捷克仓', '法国仓': '谷仓法国仓',
           '加拿大仓': '万邑通加拿大仓-CATO Warehouse',
           '美国仓': '谷仓美国东仓', '英国仓': '谷仓英国仓', '墨西哥仓': 'YM墨西哥仓'}
    df_para['warehouse_name_replace'] = df_para['warehouse'].replace(dic)
    df_para['warehouse_name'] = np.where(df_para['available_stock'] <= 0, df_para['warehouse_name_replace'],
                                         df_para['warehouse_name'])
    df_para.drop('warehouse_name_replace', axis=1, inplace=True)
    print(df_para.warehouse_name.unique())
    # 获取总运费
    print('获取海外仓运费数据...')
    # 取原始运费。用空运头程、慢海头程替换原始分摊头程。
    # 20241128: 用调价运费，做为总运费_慢海头程
    df_result = get_transport_fee(df_para)
    df_result = get_transport_fee_useful(df_result)
    df_result = df_result[df_result['total_cost'] > 0]
    df_result = df_result.drop_duplicates(subset=['sku', 'warehouse_name'])
    print(f'总数据共{len(df_para)}条，有海外仓运费数据共{len(df_result)}条')

    # 获取国内仓运费
    dic = {'US': '美国', 'DE': '德国', 'UK': '英国', 'AU': '澳大利亚', 'CA': '加拿大', 'MX': '墨西哥'}
    df_result['站点'] = df_result['country'].replace(dic)
    df_result = get_fbm_fee(df_result)
    print(f"共{len(df_result)}条数据，其中无国内仓运费数据有{len(df_result[df_result['fbm运费'].isna()])}条")
    df_result = df_result[~df_result['fbm运费'].isna()]
    # 获取差值
    df_result = merge_first_product_line(df_result)
    df_result = get_fbm_diff(df_result)
    df_platform_fee = get_oversea_diff()
    df_result = pd.merge(df_result, df_platform_fee, how='left', on=['country'])
    # 20241128海外仓目标毛利率，取当前amazon调价的目标毛利率
    df_result = get_target_profit(df_result)
    #
    col = ['fbm运费', 'FBM目标毛利润率', 'ppve', '海外仓毛利率']
    df_result[col] = df_result[col].astype(float)
    df_result['定价_fbm'] = (df_result['成本'] + df_result['fbm运费']) / (
            1 - df_result['ppve'] - df_result['FBM目标毛利润率'])
    df_result['定价_空运头程'] = (df_result['成本'] + df_result['总运费_空运头程']) / (
            1 - df_result['ppve'] - df_result['海外仓毛利率'])
    df_result['定价_慢海头程'] = (df_result['成本'] + df_result['总运费_慢海头程']) / (
            1 - df_result['ppve'] - df_result['海外仓毛利率'])
    df_result['lowest_price'] = df_result['lowest_price'].astype(float)
    df_result['lowest_profit'] = 1 - df_result['ppve'] - (df_result['成本']+df_result['总运费_慢海头程'])/df_result['lowest_price']
    df_result['定价_空运头程'] = np.where(df_result['定价_空运头程']<df_result['lowest_price'],
                                          df_result['lowest_price'], df_result['定价_空运头程'])
    df_result['定价_慢海头程'] = np.where(df_result['定价_慢海头程']<df_result['lowest_price'],
                                          df_result['lowest_price'], df_result['定价_慢海头程'])
    df_result['海外仓毛利率'] = np.where(df_result['海外仓毛利率'] < df_result['lowest_profit'],
                                         df_result['lowest_profit'], df_result['海外仓毛利率'])

    df_result['海外仓慢海定价比'] = df_result['定价_慢海头程'] / df_result['定价_fbm']
    df_result['定价比值分段'] = pd.cut(df_result['海外仓慢海定价比'],
                                       bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2],
                                       labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]',
                                               'e:(0.8, 1]',
                                               'f:(1, 1.2]', 'g:(1.2, 1.4]',
                                               'h:(1.4, 1.6]', 'i:(1.6, 1.8]', 'j:(1.8, +]'])
    df_result['定价比值分段'] = np.where(df_result['海外仓慢海定价比'] > 1.8, 'j:(1.8, +]', df_result['定价比值分段'])

    df_result['运费比值'] = df_result['总运费_慢海头程'] / df_result['fbm运费']
    df_result['运费比值分段'] = pd.cut(df_result['运费比值'],
                                       bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2],
                                       labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]',
                                               'e:(0.8, 1]',
                                               'f:(1, 1.2]', 'g:(1.2, 1.4]',
                                               'h:(1.4, 1.6]', 'i:(1.6, 1.8]', 'j:(1.8, +]'])
    df_result['运费比值分段'] = np.where(df_result['运费比值'] > 1.8, 'j:(1.8, +]', df_result['运费比值分段'])
    #
    sku_order = get_oversea_sku_order()
    df_result = pd.merge(df_result, sku_order, how='left', on=['sku', 'country'])

    col = ['空运头程属性','头程费_空运','total_cost','分摊头程','总运费_空运头程','target_profit_rate','调价目标毛利率','定价_空运头程']
    df_result.drop(col, axis=1, inplace=True)
    save_df(df_result, '海外仓SKU定价比', file_type='xlsx')

    return df_para


# df_para = pricing_oversea()
# ##
# # df_para = df_para[df_para['available_stock']<=0]
# dic = {'澳洲仓':'谷仓澳洲悉尼仓','德国仓':'谷仓捷克仓','法国仓':'谷仓法国仓','加拿大仓':'万邑通加拿大仓-CATO Warehouse',
#        '美国仓':'谷仓美国东仓','英国仓':'谷仓英国仓','墨西哥仓':'YM墨西哥仓'}
# df_para['warehouse_name_replace'] = df_para['warehouse'].replace(dic)
# df_para['warehouse_name'] = np.where(df_para['available_stock']<=0, df_para['warehouse_name_replace'], df_para['warehouse_name'])
# ##
# print(df_para.warehouse_name.unique())
#
def temp_test():
    df_result = pricing_oversea()
    #
    df_result = get_fbm_diff(df_result)
    df_platform_fee = get_oversea_diff()
    df_result = pd.merge(df_result, df_platform_fee, how='left', on=['country'])

    #
    col = ['fbm运费', 'FBM目标毛利润率', 'ppve', '海外仓毛利率']
    df_result[col] = df_result[col].astype(float)
    df_result['定价_fbm'] = (df_result['成本'] + df_result['fbm运费']) / (
                1 - df_result['ppve'] - df_result['FBM目标毛利润率'])
    df_result['定价_空运头程'] = (df_result['成本'] + df_result['总运费_空运头程']) / (
                1 - df_result['ppve'] - df_result['海外仓毛利率'])
    df_result['定价_慢海头程'] = (df_result['成本'] + df_result['总运费_慢海头程']) / (
                1 - df_result['ppve'] - df_result['海外仓毛利率'])

    df_result['海外仓慢海定价比'] = df_result['定价_慢海头程'] / df_result['定价_fbm']
    df_result['定价比值分段'] = pd.cut(df_result['海外仓慢海定价比'],
                                       bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2],
                                       labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]',
                                               'e:(0.8, 1]',
                                               'f:(1, 1.2]', 'g:(1.2, 1.4]',
                                               'h:(1.4, 1.6]', 'i:(1.6, 1.8]', 'j:(1.8, +]'])
    df_result['定价比值分段'] = np.where(df_result['海外仓慢海定价比'] > 1.8, 'j:(1.8, +]', df_result['定价比值分段'])
    #
    sku_order = get_oversea_sku_order()
    df_result = pd.merge(df_result, sku_order, how='left', on=['sku', 'country'])

def pricing_oversea_origin():
    """
    计算海外仓sku的空海同价利率差
    """
    utils.program_name = '海外仓SKU空海同价利率差'
    make_path()
    df_stock = get_oversea_stock()
    # 取尺寸重量
    df_stock['数量'] = 1
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock = chicun_zhongliang(df_stock, 1, conn_ck)
    df_stock = df_stock[['sku', 'warehouse', 'available_stock', 'country', '成本', '重量', '长', '宽', '高']]
    df_stock = df_stock[~df_stock['成本'].isna()]
    df_stock['sku1'] = df_stock['sku']
    df_stock['country_code'] = df_stock['country']

    # 取空运头程属性
    aut_ky = aut(df_stock, conn_ck, '空运')
    aut_ky = aut_ky.rename(columns={'fba头程属性': '空运头程属性', 'sku1': 'sku', 'country_code': 'country'})
    df_stock = pd.merge(df_stock, aut_ky, how='left', on=['sku', 'country'])
    df_stock['空运头程属性'] = df_stock['空运头程属性'].fillna('普货')
    df_stock.drop(['sku1', 'country_code'], axis=1, inplace=True)
    # 计算头程运费
    df_para = tou_cheng_oversea(df_stock, '空运')
    df_para.drop('计费重', axis=1, inplace=True)
    df_para = tou_cheng_oversea(df_para, '慢海')
    df_para = df_para.rename(columns={'计费重': '慢海计费重'})
    #
    df_para = df_para[df_para['country'].isin(['US', 'DE', 'UK', 'AU', 'CA', 'MX'])]
    # 获取总运费
    df_warehouse = pd.DataFrame({
        'warehouse': ['澳洲仓', '澳洲仓', '德国仓', '法国仓', '加拿大仓', '美国仓', '美国仓', '英国仓', '墨西哥仓'],
        'warehouse_name': ['谷仓澳洲悉尼仓', '万邑通澳洲墨尔本仓', '谷仓捷克仓', '谷仓法国仓',
                           '万邑通加拿大仓-CATO Warehouse', '谷仓美国东仓', '谷仓美国西仓', '谷仓英国仓', 'YM墨西哥仓']
    })
    df_para = pd.merge(df_para, df_warehouse, how='left', on=['warehouse'])

    df_result = get_transport_fee(df_para)
    df_result = df_result[df_result['total_cost'] > 0]
    df_result = df_result.drop_duplicates(subset=['sku', 'warehouse_name'])

    df_result['库损&汇损费率'] = 0.04
    df_result['订单毛利率'] = 0.25
    df_result['定价_空运头程'] = (df_result['成本'] + df_result['总运费_空运头程']) / (
                1 - df_result['库损&汇损费率'] - df_result['订单毛利率'])
    df_result['定价_慢海头程'] = (df_result['成本'] + df_result['总运费_慢海头程']) / (
                1 - df_result['库损&汇损费率'] - df_result['订单毛利率'])
    df_result['空海利润率'] = 1 - df_result['库损&汇损费率'] - (df_result['成本'] + df_result['总运费_慢海头程']) / \
                              df_result['定价_空运头程']
    df_result['空海同价利率差'] = df_result['空海利润率'] - df_result['订单毛利率']

    df_result['利率差分段'] = pd.cut(df_result['空海同价利率差'], bins=[0, 0.01, 0.05, 0.1, 0.5, 1, 100],
                                     labels=['A:(0,0.01]', 'B:(0.01,0.05]', 'C:(0.05,0.1]', 'D:(0.1,0.5]', 'E:(0.5,1]',
                                             'F:(1,+)'])
    df_result['利率差分段'] = np.where(df_result['空海同价利率差'] > 1, 'F:(1,+)', df_result['利率差分段'])
    save_df(df_result, '海外仓SKU空海同价利率差', file_type='xlsx')

    return df_result

def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    会先清除历史表数据
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    sql = f"""
    DELETE FROM over_sea.{table_name} WHERE date = '{date_id}' and `维度`='tt_sku'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()

######## 美国站有销量sku海外仓定价比 ##############
def get_dom_order_temp():
    """ 获取国内仓订单明细数据 """
    date_today = time.strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    date_30 = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    date_15 = (datetime.date.today() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
    date_7 = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    sql = f"""
        SELECT order_id, platform_code, account_id, seller_sku, total_price, quantity, sku, `sku数量`,
        `毛利润`, `净利润`, `销库金额`, created_time
        FROM domestic_warehouse_clear.monitor_dom_order
        WHERE `站点` = '美国' and created_time >= '{date_start}' and created_time < '{date_today}'
        and `净利润` > 0
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())

    #
    df_90 = df[df['created_time']>=date_start]
    df_90 = df_90.groupby('sku').agg({'sku数量':sum}).reset_index().rename(columns={'sku数量': '90_sales'})
    df_30 = df[df['created_time']>=date_30]
    df_30 = df_30.groupby('sku').agg({'sku数量':sum}).reset_index().rename(columns={'sku数量': '30_sales'})
    df_15 = df[df['created_time']>=date_15]
    df_15 = df_15.groupby('sku').agg({'sku数量':sum}).reset_index().rename(columns={'sku数量': '15_sales'})
    df_7 = df[df['created_time']>=date_7]
    df_7 = df_7.groupby('sku').agg({'sku数量':sum}).reset_index().rename(columns={'sku数量': '7_sales'})

    df = pd.merge(df_90, df_7, how='left', on=['sku'])
    df = pd.merge(df, df_15, how='left', on=['sku'])
    df = pd.merge(df, df_30, how='left', on=['sku'])

    df = df.fillna(0)
    df['day_sales'] = 0.7*df['7_sales']/7 + 0.2*df['15_sales']/15 + 0.1*df['30_sales']/30
    # df.to_excel('F://Desktop//dom_monitor_order.xlsx', index=0)

    return df

def get_sku_sales():
    """ 获取美国站有销量的sku """
    sql = """
        SELECT 
            SKU as sku, `90days_sales`, warehouse_id 
        FROM over_sea.yibai_sku_sales_statistics
        WHERE warehouse_id in (478, 481) and `90days_sales` > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_yibai_sales = conn.read_sql(sql)

    return df_yibai_sales

def get_days_sales():
    """ 获取美国站有销量的sku """
    sql = """
        SELECT 
            SKU as sku, `3days_sales`, `7days_sales`,`15days_sales`,`30days_sales`, warehouse_id 
        FROM over_sea.yibai_sku_sales_statistics
        WHERE warehouse_id in (478, 481) and `90days_sales` > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df = df.groupby(['sku']).agg({'3days_sales': 'sum', '7days_sales': 'sum', '15days_sales': 'sum',
                                                     '30days_sales': 'sum'}).reset_index()
    df['day_sales'] = 0.7 * df['7days_sales'] / 7 + 0.2 * df['15days_sales'] / 15 + 0.1 * df['30days_sales'] / 30

    df = df[['sku', '30days_sales', 'day_sales']]

    df.to_excel('F://Desktop//df_day_sales.xlsx', index=0)

    return df

def get_interf_fee(df, platform='AMAZON'):
    """
    输入：sku, country
    输出：sku, country, 'warehouseName', 'shipName', 'totalCost','shippingCost', 'firstCarrierCost'
    """
    df = df[['sku', 'country']].drop_duplicates()
    #
    df['数量'] = 1
    df = df.reset_index(drop=True).reset_index()
    # 控制country对应的子仓
    dic = {'US': '47,49,50,58', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
           'AU': '353,769','CZ': '325','PL': '325','HU':'325','PT':'325','NL':'325','MX':'956,957','BR':'1467'}
    df['warehouse_id'] = df['country'].replace(dic)
    # print(df.head(5))
    # df_bundle = df_bundle.sample(1000)
    df_bundle = sku_and_num_split(df)
    df_fee = load_interface(df_bundle, platform)
    df_fee = df_fee[['sku', 'shipCountry', 'warehouseName', 'shipName', 'totalCost','shippingCost', 'firstCarrierCost']]

    return df_fee

def load_interface(df, platform='AMAZON'):
    """
    调用运费接口获取sku海外仓运费
    """
    df_result = pd.DataFrame()
    # key2 = '49;769;325;88;50;353;47;680;58;1139'
    for (key1, key2), group in df.groupby(['country', 'warehouse_id']):
        # print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea(platform, key1, key2, '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30','')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost',
             'shippingCost', 'firstCarrierCost']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True).drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])

    return df_result

def toucheng(df):
    """ 计算海外仓头程运费 """
    # 取尺寸重量
    df['数量'] = 1
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = chicun_zhongliang(df, 1, conn_ck)
    print(df.info())
    df = df[['sku', 'country', '成本', '重量', '长', '宽', '高']]
    df = df[~df['成本'].isna()]
    df['sku1'] = df['sku']
    df['country_code'] = df['country']

    # 取空运头程属性
    aut_ky = aut(df, conn_ck, '空运')
    aut_ky = aut_ky.rename(columns={'fba头程属性': '空运头程属性', 'sku1': 'sku', 'country_code': 'country'})
    df = pd.merge(df, aut_ky, how='left', on=['sku', 'country'])
    df['空运头程属性'] = df['空运头程属性'].fillna('普货')
    df.drop(['sku1', 'country_code'], axis=1, inplace=True)
    # 计算头程运费
    df_para = tou_cheng_oversea(df, '空运')
    df_para.drop('计费重', axis=1, inplace=True)

    # 计算海运头程
    df_para = tou_cheng_oversea(df_para, '快海')
    df_para.drop('计费重', axis=1, inplace=True)

    return df_para

def toucheng_temp():
    """ 临时获取头程运费 """
    df = pd.read_excel('F://Desktop//df_us_sku.xlsx', dtype={'new_sku':str})
    print(df.info())

    df['数量'] = 1
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = chicun_zhongliang(df, 1, conn_ck)
    print(df.info())
    df = df[['sku', 'country', '成本', '重量', '长', '宽', '高']]
    df = df[~df['成本'].isna()]
    df['sku1'] = df['sku']
    df['country_code'] = df['country']

    # 取空运头程属性
    aut_ky = aut(df, conn_ck, '慢海')
    aut_ky = aut_ky.rename(columns={'fba头程属性': '海运头程属性', 'sku1': 'sku', 'country_code': 'country'})
    df = pd.merge(df, aut_ky, how='left', on=['sku', 'country'])
    df['海运头程属性'] = df['海运头程属性'].fillna('普货')
    df.drop(['sku1', 'country_code'], axis=1, inplace=True)
    # 计算头程运费
    df_para = tou_cheng_oversea(df, '慢海')
    df_para.drop('计费重', axis=1, inplace=True)

    df_para.to_excel('F://Desktop//df_para.xlsx', index=0)

    return None


def process_df_with_threads(df, thread_count=10):
    # 分割DataFrame为多个小份，每个线程处理一份
    df_split = np.array_split(df, thread_count)

    # 使用ThreadPoolExecutor来管理多线程
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        # 将每个小份DataFrame提交给线程池处理
        futures = [executor.submit(get_interf_fee, df_part) for df_part in df_split]

        # 收集处理结果
        result_df_list = [future.result() for future in futures]

    # 合并处理后的DataFrame
    df_fee = pd.concat(result_df_list)
    return df_fee

def us_sku_pricing_ratio():
    """
    1、海外仓价格，国内仓运费，对应的利润率
    2、海外仓定价比
    """

    # 获取sku
    df_sku = get_sku_new()
    # df_yibai_sales = get_sku_sales()
    # 取美国站近90天正净利销量
    df_us_sales = get_dom_order_temp()
    df_sku = pd.merge(df_sku, df_us_sales, how='inner', on=['sku'])
    df_sku['country'] = 'US'
    print(f'美国站有销量的sku数量共{len(df_sku)}条。')
    df = df_sku[['sku','country']]
    # df = df.sample(100)

    # 获取海外仓运费
    print(df.info())
    df = get_interf_fee(df, platform='AMAZON')
    # df = process_df_with_threads(df, thread_count=10)
    df = df.rename(columns={'shipCountry':'country'})
    # 替换头程
    df_toucheng = toucheng(df)
    col = ['sku','country','成本', '重量', '长', '宽', '高','头程费_空运','头程费_快海']
    df = pd.merge(df, df_toucheng[col], how='left', on=['sku','country'])
    df['总运费_空运头程'] = df['totalCost'] - df['firstCarrierCost'] + df['头程费_空运']
    df['总运费_海运头程'] = df['totalCost'] - df['firstCarrierCost'] + df['头程费_快海']
    df.drop(['数量'], axis=1, inplace=True)

    print(df.info())

    # 获取国内仓运费
    dic = {'US': '美国', 'DE': '德国', 'UK': '英国', 'AU': '澳大利亚', 'CA': '加拿大', 'MX': '墨西哥'}
    df['站点'] = df['country'].replace(dic)
    df = get_fbm_fee(df)
    print(f"共{len(df)}条数据，其中无国内仓运费数据有{len(df[df['fbm运费'].isna()])}条")
    print(df.info())
    # 获取差值
    df = merge_first_product_line(df)
    df = get_fbm_diff(df)
    df_platform_fee = get_oversea_diff()
    df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    print(df.info())
    # 取日销
    df = pd.merge(df, df_us_sales[['sku','day_sales','30_sales']], how='left', on=['sku'])
    # 取fba定价比参数
    df = us_sku_fba_pricing_ratio(df)
    # df_toucheng = df_toucheng[['sku','country','头程费_空运','头程费_快海']].drop_duplicates()
    # print(df_toucheng.info())
    df.to_excel('F://Desktop//df.xlsx', index=0)



def us_sku_fba_pricing_ratio(df):
    """ 国内仓有销sku，fba定价比 """
    # # 获取sku
    # df_sku = get_sku_new()
    # df_yibai_sales = get_sku_sales()
    # df_sku = pd.merge(df_sku, df_yibai_sales, how='inner', on=['sku'])
    # df_sku['country'] = 'US'
    # print(f'美国站有销量的sku数量共{len(df_sku)}条。')
    # df = df_sku[['sku','country']]

    # 取fba定价比数据
    sql = """
        SELECT sku, `站点`, `头程费快运`,`头程费慢运`,`fba_fees`,`FBA目标毛利润率`,`汇率`,`FBA税率`,`FBA快运定价`, `FBA慢运定价`
        FROM support_document.pricing_ratio_20250520
        WHERE `站点` = '美国'
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_fba = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_fba, how='left', on=['sku'])
    df = df.drop_duplicates()

    # df.to_excel('F://Desktop//df_fba.xlsx', index=0)
    return df
##
if __name__ == "__main__":
    #
    # pricing_oversea()
    us_sku_pricing_ratio()
    # us_sku_fba_pricing_ratio()
    # get_dom_order_temp()
    # get_days_sales()
    # get_fbm_fee_temp()
    # toucheng_temp()

