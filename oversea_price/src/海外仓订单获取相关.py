"""
海外仓订单销售状态与净利率不一致问题排查：
1、先获取订单数据。（2024年）
2、获取该订单日期的海外仓链接表、调价表，确认是否进入调价清单内
3、进入调价清单内的订单，获取订单日期的调价明细，
4、对比订单表与调价明细数据，包括sku、子仓是否一致、调价价格与订单价格是否一致、调价运费与订单运费一致、其他参数项是否一致

"""
import IPython.utils.io
##
import pandas as pd
import tqdm
import numpy as np
import warnings
import datetime, time
from clickhouse_driver import Client
from utils.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck, get_ck_conf_data
from all_auto_task.oversea_price_adjust_2023 import get_stock, get_stock_age, get_rate, get_sku_sales_new, cut_bins, \
    is_new_sku, write_to_sql
from all_auto_task.oversea_temu_price import get_temu_listing, get_temu_account
from all_auto_task.oversea_sku_price import get_flash_sku
from all_auto_task.oversea_price_adjust_tt import tt_get_sku, tt_get_warehouse
warnings.filterwarnings('ignore')

##
# 获取订单信息

def get_ebay_listing(df_order):
    # 按日期取订单，及前一天的海外仓链接表信息
    order_date = pd.DataFrame({'date_id': df_order['paytime'].unique()})
    # 获取链接表的日期对照表
    sql = f"""
        SELECT
            DISTINCT `DATE` as date_id
        FROM over_sea.yibai_ebay_oversea_listing_price
        WHERE `DATE` > '2023-12-28' and `DATE` < '2024-04-01'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_oversea_dtl = conn.read_sql(sql)
    #
    df_date_list = df_oversea_dtl['date_id'].sort_values().drop_duplicates()
    df_date_list = pd.DataFrame({'date_id': df_date_list, 'date_id_t': df_date_list})
    #
    # 获取调价链接表的日期对照
    sql = """
        SELECT
            DISTINCT date_id
        FROM over_sea.oversea_ebay_listing_all
        WHERE date_id > '2023-12-28' and date_id < '2024-04-01'
        ORDER BY date_id
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_date_list_2 = conn.read_sql(sql)
    # conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    # df_date_list_2 = conn_ck.ck_select_to_df(sql)
    df_date_list_2['date_id_2'] = df_date_list_2['date_id']
    #
    order_date['date_id'] = pd.to_datetime(order_date['date_id'])
    df_date_list['date_id'] = pd.to_datetime(df_date_list['date_id'])
    df_date_list_2['date_id'] = pd.to_datetime(df_date_list_2['date_id'])
    order_date = order_date.sort_values(by='date_id')
    df_temp = pd.merge_asof(order_date, df_date_list, on='date_id', direction='backward')
    #
    df_temp = pd.merge_asof(df_temp, df_date_list_2, on='date_id', direction='backward')
    #
    df_temp['date_id'] = df_temp['date_id'].astype(str)
    df_temp['date_id_shift'] = df_temp['date_id_t'].shift(1).fillna('2023-12-28')
    df_temp['date_id_shift_2'] = df_temp['date_id_2'].shift(1).fillna('2023-12-28')
    #
    df_stage_1 = pd.DataFrame()
    print('日期获取完成...')
    for i in range(len(df_temp)):
        df_order_i = df_order[df_order['paytime'] == df_temp.iloc[i, 0]]
        order_account = tuple(df_order_i['account_id'].unique())
        sql = f"""
            SELECT
                DISTINCT account_id, item_id, sku,  online_price, `DATE`
            FROM over_sea.yibai_ebay_oversea_listing_price
            WHERE `DATE` = '{df_temp.iloc[i, 3]}' and account_id in {order_account}

        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_oversea_i = conn.read_sql(sql)
        df_oversea_i['account_id'] = df_oversea_i['account_id'].astype(int)
        df_stage_temp = pd.merge(df_order_i, df_oversea_i, how='left', on=['account_id', 'item_id'])
        #
        sql = f"""
            SELECT
                account_id, country, item_id,  date_id, warehouse, sales_status,new_price, total_cost, price,rate, 
                target_profit_rate, ppve, platform_zero, platform_must_percent, is_uk_cdt, is_normal_cdt, is_small_diff,
                is_white_account, is_90_overage
            FROM over_sea.oversea_ebay_listing_all
            WHERE date_id = '{df_temp.iloc[i, 4]}' and account_id in {order_account}
        """
        # conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_listing_i = conn.read_sql(sql)
        df_listing_i['account_id'] = df_listing_i['account_id'].astype(int)
        df_stage_temp = pd.merge(df_stage_temp, df_listing_i, how='left', on=['account_id', 'item_id'])

        df_stage_1 = pd.concat([df_stage_temp, df_stage_1])

    return df_stage_1

def get_amazon_listing(df_order):
    # 按日期取订单，及前一天的海外仓链接表信息
    order_date = pd.DataFrame({'date_id': df_order['paytime'].unique()})
    # 获取链接表的日期对照表
    sql = f"""
        SELECT
            DISTINCT `DATE` as date_id
        FROM over_sea.yibai_amazon_oversea_listing_price
        WHERE `DATE` > '2023-12-28' and `DATE` < '2024-04-01'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_oversea_dtl = conn.read_sql(sql)
    #
    df_date_list = df_oversea_dtl['date_id'].sort_values().drop_duplicates()
    df_date_list = pd.DataFrame({'date_id': df_date_list, 'date_id_t': df_date_list})
    #
    # 获取调价链接表的日期对照
    sql = """
        SELECT
            DISTINCT date_id
        FROM yibai_oversea.oversea_amazon_listing_all
        WHERE date_id > '2023-12-28' and date_id < '2024-04-01'
        ORDER BY date_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df_date_list_2 = conn_ck.ck_select_to_df(sql)
    df_date_list_2['date_id_2'] = df_date_list_2['date_id']
    #
    order_date['date_id'] = pd.to_datetime(order_date['date_id'])
    df_date_list['date_id'] = pd.to_datetime(df_date_list['date_id'])
    df_date_list_2['date_id'] = pd.to_datetime(df_date_list_2['date_id'])
    order_date = order_date.sort_values(by='date_id')
    df_temp = pd.merge_asof(order_date, df_date_list, on='date_id', direction='backward')
    #
    df_temp = pd.merge_asof(df_temp, df_date_list_2, on='date_id', direction='backward')
    #
    df_temp['date_id'] = df_temp['date_id'].astype(str)
    df_temp['date_id_shift'] = df_temp['date_id_t'].shift(1).fillna('2023-12-28')
    df_temp['date_id_shift_2'] = df_temp['date_id_2'].shift(1).fillna('2023-12-28')
    #
    df_stage_1 = pd.DataFrame()
    for i in tqdm.tqdm(range(len(df_temp))):
        df_order_i = df_order[df_order['paytime'] == df_temp.iloc[i,0]]
        order_account = tuple(df_order_i['account_id'].unique())
        sql = f"""
            SELECT
                DISTINCT account_id, seller_sku, sku, asin, online_price, `DATE`
            FROM over_sea.yibai_amazon_oversea_listing_price
            WHERE `DATE` = '{df_temp.iloc[i,3]}' and account_id in {order_account}
        
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_oversea_i = conn.read_sql(sql)
        df_oversea_i['account_id'] = df_oversea_i['account_id'].astype(int)
        df_stage_temp = pd.merge(df_order_i, df_oversea_i, how='left', on=['account_id','seller_sku'])
        #
        sql = f"""
            SELECT
                account_id, country, seller_sku,  date_id, warehouse, sales_status,new_price, total_cost, price,rate, 
                target_profit_rate, ppve, platform_zero, platform_must_percent, is_uk_cdt, is_normal_cdt, is_small_diff,
                is_white_account, is_white_listing, is_fba_asin
            FROM yibai_oversea.oversea_amazon_listing_all
            WHERE date_id = '{df_temp.iloc[i,4]}' and account_id in {order_account}
        """
        conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
        df_listing_i = conn_ck.ck_select_to_df(sql)
        df_stage_temp = pd.merge(df_stage_temp, df_listing_i, how='left', on=['account_id', 'seller_sku'])

        df_stage_1 = pd.concat([df_stage_temp, df_stage_1])

    return df_stage_1

def get_price_dtl_all(platform='AMAZON'):
    """
    1、先找出缺失的日期
    2、再匹配缺失日期对应最近的一次记录
    3、获取最近一次的记录数据，替换日期
    4、合并数据
    """
    # 获取海外仓调价记录表，缺失日期填充最近一次调价数据
    sql = f"""
        SELECT 
            sku, warehouse, sales_status, total_cost, available_stock, date_id, platform, country, price, price_rmb
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id >= '2025-12-01' and platform = '{platform}' 
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_price_dtl = conn_ck.ck_select_to_df(sql)

    df_date_list = df_price_dtl['date_id'].sort_values().drop_duplicates()
    df_date_list = pd.DataFrame({'date_id':df_date_list, 'date_id_t':df_date_list})
    # 时间范围
    start_date = '2025-12-01'
    end_date = '2026-02-01'

    # 使用date_range生成一个按天递增的日期序列
    date_range = pd.date_range(start=start_date, end=end_date)
    date_df = pd.DataFrame(date_range, columns=['date_id'])
    # 找出缺失的日期
    df_date_list['date_id'] = pd.to_datetime(df_date_list['date_id'])
    date_df['date_id'] = pd.to_datetime(date_df['date_id'])
    df_date_nan = pd.merge(date_df, df_date_list, on='date_id',how='left')
    df_date_nan = df_date_nan[df_date_nan['date_id_t'].isna()]
    df_date_nan.columns=['date_id','date_value']
    # 使用模糊匹配，匹配缺失日期最近一次调价的日期
    df_temp = pd.merge_asof(df_date_nan, df_date_list, on='date_id', direction='backward')
    df_temp.drop('date_value', axis=1, inplace=True)

    # 判断是否有未匹配到的日期
    b = df_temp[df_temp['date_id_t'].isna()]
    if len(b) > 0:
        print(f"存在未匹配到的日期数据, 日期为{b['date_id'].unique()}")
    #
    df_temp = df_temp[~df_temp['date_id_t'].isna()]
    print(df_temp.info())
    # 补充缺失日期的调价数据。（用前一次的数据填充）
    df_lost = pd.DataFrame()
    for i in range(len(df_temp)):
        df_price_temp = df_price_dtl[df_price_dtl['date_id'] == df_temp.iloc[i,1]]
        df_price_temp['date_id'] = df_temp.iloc[i,0].strftime('%Y-%m-%d')
        df_lost = pd.concat([df_price_temp, df_lost])

    # 合并数据
    df_result = pd.concat([df_price_dtl, df_lost])
    df_result = df_result.sort_values(by=['sku','warehouse','date_id'])

    return df_result

##   亿迈订单获取
def get_dcm_order():
    start_time = '2026-01-01'
    end_time = '2025-10-16'
    sql = f"""
        SELECT 
            a.order_id, a.sku,  a.quantity `订单销量`,  b.is_ship_process,
            b.platform_code, b.shop_id AS distributor_id, b.account_id, c.seller_sku,
            b.payment_time start_time, b.warehouse_id, b.total_price, b.currency, b.ship_country
        FROM yibai_dcm_order.dcm_order b
        LEFT JOIN yibai_dcm_order.dcm_order_sku a ON a.order_id = b.order_id
        LEFT JOIN yibai_dcm_order.dcm_order_detail c ON a.order_detail_id = c.id
        WHERE 
            b.payment_time >= '{start_time}'
            -- AND b.payment_time < '{end_time}'
            AND b.order_status != 40  -- 订单状态（1=待处理、10=未发货、20=部分发货、30=已发货、 40=已作废）
            AND b.payment_status = 1 -- 平台付款状态（客户向亚马逊平台的付款状态，0=未付款，1=已付款）
            AND b.is_abnormal = 0 -- 是否有异常（0=否，1=是）
            AND b.is_intercept = 0 -- 是否拦截（0=未拦截，1=已拦截）
            AND b.refund_status = 0 -- 退款状态（0=未退款，1=退款中、2=部分退款，3=全部退款）
            AND b.total_price != 0 -- 订单总金额 平台付款金额
            AND b.is_ship_process = 0 -- 是否平台仓发货（0否--FBM；1：是--FBA）
            -- AND a.sku = '10337814'
    """
    # -- AND
    # b.ship_country = 'MX'
    # -- AND
    # e.fulfillment_type = 2 - - 发货类型（0 = 未知、1 = 本地仓发货、2 = 海外仓、10 = 云盒直发）
    conn = connect_to_sql(database='yibai_dcm_order', data_sys='新数仓')
    df_dcm_order = conn.read_sql(sql)
    # 匹配大仓
    sql = """
        SELECT 
            yw.id AS warehouse_id,   
            yw.warehouse_name AS warehouse_name,
            ywc.name AS warehouse, yw.warehouse_type, yw.warehouse_other_type warehouse_other_type
        FROM yibai_logistics_tms_sync.yibai_warehouse yw
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    df_warehouse.columns = [i.split('.')[-1] for i in df_warehouse.columns]
    df_dcm_order = pd.merge(df_dcm_order, df_warehouse, how='left', on=['warehouse_id'])

    # 筛选海外仓仓库
    # df_dcm_order = df_dcm_order[df_dcm_order['type'].isin([2,3])]

    sql = """
        SELECT
            sku, product_status `产品状态`, title_cn `产品名称`, develop_source, b.develop_source_name,
            CASE 
                when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                else toFloat64(product_cost) 
            END as `new_price`
        FROM yibai_prod_base_sync.yibai_prod_sku a
        LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source b
        ON a.develop_source = b.id 
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_cost = conn_ck.ck_select_to_df(sql)

    df_dcm_order = pd.merge(df_dcm_order, df_cost[['sku', 'new_price']], how='left', on=['sku'])

    print(df_dcm_order.info())

    # df_dcm_order.to_excel('F://Desktop//df_dcm_order.xlsx', index=0)

    return df_dcm_order

## 国内仓销售状态表
# 亿迈订单销售状态
def dcm_sale_status():
    sql = """
        SELECT 
            sku, adjustment_priority, start_time, end_time
        FROM domestic_warehouse_clear.domestic_warehouse_clear_new
        WHERE end_time >= '2024-06-01' or end_time is NULL
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    #
    df['end_time'] = df['end_time'].fillna('2024-07-17')

    df['start_time'] = np.where(df['start_time'] < '2024-05-30', '2024-05-30', df['start_time'])

    df1 = df[(df['start_time'] <= '2024-05-30') & (df['end_time'] >= '2024-07-01')]
    df2 = df[~((df['start_time'] <= '2024-05-30') & (df['end_time'] >= '2024-07-01'))]

    # 将时间字符串转换为Pandas的datetime对象
    df2['start_time'] = pd.to_datetime(df2['start_time'])
    df2['end_time'] = pd.to_datetime(df2['end_time'])

    # 创建新的DataFrame来存放扩展后的数据
    extended_df = pd.DataFrame()

    # 遍历原始DataFrame的每一行
    for index, row in df2.iterrows():
        # 使用pd.date_range生成时间范围
        start = row['start_time']
        end = row['end_time']
        range_dates = pd.date_range(start, end, freq='D')

        # 创建子DataFrame
        sub_df = pd.DataFrame(index=range_dates)
        sub_df['start_time'] = range_dates

        # 复制其他列的数据到子DataFrame
        for col in df2.columns:
            if col not in ['start_time', 'end_time']:
                sub_df[col] = row[col]

        # 合并新的子DataFrame到扩展后的DataFrame
        extended_df = extended_df.append(sub_df, ignore_index=True)
##
def get_dcm_result(extended_df, df1):
    df_dcm_order = get_dcm_order()

    df_dcm_order['start_time'] = df_dcm_order['start_time'].dt.strftime('%Y-%m-%d')
    extended_df['start_time'] = extended_df['start_time'].dt.strftime('%Y-%m-%d')
    #
    df = pd.merge(df_dcm_order, extended_df, how='left', on=['sku','start_time'])
    df = pd.merge(df, df1[['sku','adjustment_priority']], how='left', on=['sku'])
    return df

## 易佰订单明细
def get_order_temp():
    """ 获取不同平台订单明细数据 """
    # date_today = datetime.date.today()
    # date_start = date_today - datetime.timedelta(days=10)
    date_start = '2026-01-01'
    date_end = '2026-12-01'

    df_sku = pd.read_excel('F://Desktop//df_three_sku.xlsx', dtype={'sku': str})
    sku_list = tuple(df_sku['sku'].unique())
    # sku_list = (
    #     "YM19922PBJF6",
    #     "YM20239DJQ1",
    #     "YM20239DJQ2",
    #     "YM20239FM01",
    #     "YM20239FM11",
    #     "YM20239JLT01",
    #     "YM20239JLT21",
    #     "YM20239JLT31",
    #     "YM20239JLT41",
    #     "YM20239JLT61",
    #     "YM20239JLT71",
    #     "YM20239JLT81",
    #     "YM20239JZ01",
    #     "YM20239JZ11",
    #     "YM20239NJQ01",
    #     "YM20239PJB1",
    #     "YM20239QB01",
    #     "YM20728ZSH02",
    #     "YM3116250000311",
    #     "YM3116250001111",
    #     "YM3118250000311"
    # )

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            -- and platform_code in ('TEMU')
            -- and warehouse_id = 478
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,
        A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
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
        B.ship_country,
        A.location,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp) 
            -- and  ship_country = 'US'
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
        INNER JOIN (
            SELECT 
                yw.id AS warehouse_id,   
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
                yw.country, ebay_category_id,
                ywc.name AS warehouse, yw.warehouse_type
            FROM yibai_logistics_tms_sync.yibai_warehouse yw
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and ywc.name in ('美国仓','加拿大仓')
        ) W ON B.warehouse_id = W.warehouse_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    # df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    df = df[df['sku'].isin(sku_list) & (df['warehouse']=='美国仓')]
    df_ship_code = get_yibai_logistics_logistics()
    df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])

    df.to_excel('F://Desktop//oversea_order_monitor.xlsx', index=0)

    return df

def ca_order_temp():
    """ 获取不同平台订单明细数据 """
    # date_today = datetime.date.today()
    # date_start = date_today - datetime.timedelta(days=10)
    date_start = '2025-09-01'
    date_end = '2025-10-23'

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            and platform_code in ('TEMU')
            -- and warehouse_id = 478
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,
        A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
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
        B.ship_country,
        A.location,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp) 
        ) A
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
            and  ship_country = 'CA'
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
        INNER JOIN (
            SELECT 
                yw.id AS warehouse_id,   
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
                yw.country, ebay_category_id,
                ywc.name AS warehouse, yw.warehouse_type
            FROM yibai_logistics_tms_sync.yibai_warehouse yw
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            and ywc.name in ('加拿大仓')
        ) W ON B.warehouse_id = W.warehouse_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    # df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    df.to_excel('F://Desktop//oversea_order_monitor_ca.xlsx', index=0)

    return df

def ebay_order_history():
    """ 获取不同平台订单明细数据 """
    # date_today = datetime.date.today()
    # date_start = date_today - datetime.timedelta(days=10)

    account_list = ebay_account_temp()
    account_tuple = tuple(account_list['account_id'].unique())
    print(account_tuple)
    year_y = 2024
    # year_list = [2019, 2020, 2021, 2022]
    year_1 = year_y + 1
    date_start = f'{year_y}-01-01'
    date_end = f'{year_1}-01-01'

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order_{year_y}
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            and platform_code in ('EB')
            and account_id in {account_tuple}
            -- and warehouse_id = 478
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,
        A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
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
        B.ship_country,
        A.location,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail_{year_y} 
            WHERE order_id in (SELECT order_id FROM order_temp) 
        ) A
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_{year_y}
            WHERE order_id in (SELECT order_id FROM order_temp)

        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_profit_{year_y} 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_sku_{year_y}
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
            -- WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and ywc.name in ('加拿大仓')
        ) W ON B.warehouse_id = W.warehouse_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())

    df = pd.merge(df, account_list, how='inner', on=['account_id'])
    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']

    df.to_excel(f'F://Desktop//oversea_order_monitor_ebay_{year_y}.xlsx', index=0)

    return df

def ebay_account_temp():
    """ 账号简称 """
    sql = """
    select account_id, account_s_name
    from yibai_account_manage_sync.yibai_ebay_account
    where account_type = 1 and is_yibai =1
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)
    # account_list = ["WM8", "NCP", "NC3", "PER", "FKR", "HTY", "DOS", "DBG", "SOS", "TOM", "EHO", "OST", "KKT", "MME",
    #                 "GM3", "HSA", "HON", "AOS", "AFE", "BUS", "SSA", "ABG", "MED", "MGC", "NDB", "TOS", "BSB", "SGH",
    #                 "SBO", "ELL", "HTT", "ROL", "BEE", "DRO", "LOT", "GSE", "COB", "XAT", "HUA", "LAK", "GIN", "SMN",
    #                 "DEA", "YRR", "ZUC", "FSD", "GK5"]
    # account_list = ['DOS','DBG','SOS','TOM','EHO','OST','KHS','NG5']
    # account_list = ['ACT','ADM','BEL','BFG','BSD','COD','EFL','FLW','GUS','KUG',
    #                 'LAO','LAY','LIN','OME','SOT','STU','TIM','WES','YCO']
    account_list = ['YSE', 'MD5', 'YAR', 'YAZ', 'WER', 'CHO', 'CHT', 'SHE', 'HA5']

    df_account = df_account[df_account['account_s_name'].isin(account_list)]
    print(df_account.info())
    account_temp = df_account['account_id'].unique()

    return df_account

def get_ca_temp():
    sql = """
        SELECT *
        FROM over_sea.oversea_order_monitor
        WHERE payment_time > '2024-01-01' and payment_time < '2025-01-31'
        and platform_code = 'TEMU' and warehouse = '加拿大仓'
    """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_order = conn.read_sql(sql)
    # print(df_order.info())
    # sql = """
    #     SELECT *
    #     FROM over_sea.dwd_temu_listing
    #     WHERE date_id = '2025-01-13' and warehouse = '加拿大仓'
    #
    # """
    # df_temu = conn.read_sql(sql)
    # df_order = df_order.rename(columns={'seller_sku':'product_sku_id'})
    # df_order['account_id'] = df_order['account_id'].astype(int)
    # df_temu['account_id'] = df_temu['account_id'].astype(int)
    # df = pd.merge(df_order, df_temu, how='left',on=['account_id', 'product_sku_id'])
    #
    # print(df.info())
    #
    # df.to_excel('F://Desktop//df_temu_order_3.xlsx', index=0)

    # 加拿大尾程渠道校验
    df_ca_order = ca_order_temp()
    col = ['order_id','sku','warehouse_id','total_price','quantity','true_profit_new1','ship_code','payment_time',
           'shipping_cost','true_shipping_fee','shipping_fee']
    df_ca_order = df_ca_order[col]
    print(df_ca_order.info())
    sql = """
        SELECT sku, warehouseId warehouse_id, shipCode ship_code, shipName, shippingCost
        FROM yibai_oversea.oversea_transport_fee_daily_1
        WHERE date_id = 20250922 
        -- and warehouse_id = 1139 
        -- and shipCode in ('WYT_240613002', 'WYT_240613003', 'WYT_240613001')
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_ca_fee = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df_ca_order, df_ca_fee, how='left', on=['sku','warehouse_id','ship_code'])

    df.to_excel('F://Desktop//df_ca.xlsx', index=0)

    return None

def get_au_temp():
    """ temu订单明细 """

    df_au_order = get_order_temp()
    df_au_order = df_au_order[df_au_order['warehouse'] == '澳洲仓']
    df_au_order = df_au_order[df_au_order['quantity'] == 1]
    col = ['order_id','sku','warehouse_id','total_price','ship_code','payment_time',
           'shipping_cost','true_shipping_fee','shipping_fee']
    df_au_order = df_au_order[col]
    print(df_au_order.info())
    sql = """
        SELECT sku, warehouseId warehouse_id, shipCode ship_code, shipName, shippingCost
        FROM over_sea.oversea_transport_fee_useful_temu
        WHERE warehouse = '澳洲仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_au_fee = conn.read_sql(sql)

    df = pd.merge(df_au_order, df_au_fee, how='left', on=['sku','warehouse_id'])

    df.to_excel('F://Desktop//df.xlsx', index=0)
    return None

def get_temu_clear_price():
    """ 获取temu清仓定价表数据 """
    df = pd.read_excel('F://Desktop//日常任务//TEMU定价需求//TEMU海外仓清仓定价0127.xlsx')
    col = ['sku', 'warehouse', '成本','总运费','尾程','尾程渠道','目的国']
    df = df[col]
    df = df.rename(columns={'目的国':'country'})

    return df

def get_temu_temp():
    """ temu订单明细 """

    # 1、temu订单
    df = get_order_temp()
    df = df[df['warehouse'] == '美国仓']
    df = df[df['quantity'] == 1]
    col = ['order_id','sku','warehouse_id','total_price','ship_code','payment_time',
           'shipping_cost','true_shipping_fee','shipping_fee']
    # df_au_order = df_au_order[col]
    print(df.info())
    df_ship_code = get_yibai_logistics_logistics()

    # 2、取定价渠道数据
    df_clear = get_temu_clear_price()

    df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    df = df.rename(columns={'ship_country':'country'})
    df = pd.merge(df, df_clear, how='left', on=['sku','warehouse','country'])

    df.to_excel('F://Desktop//df_temu_order.xlsx', index=0)

    return None

# 取海外仓库存
def get_stock():
    """
    获取海外仓库存
    """
    date_today = time.strftime('%Y%m%d')
    sql = f"""

        SELECT
            ps.sku sku, toString(toDate(toString(date_id))) date_id, yw.ebay_category_id AS category_id, yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse, 
            yps.develop_source_name develop_source_name,
            available_stock, allot_on_way_count AS on_way_stock, wait_outbound, frozen_stock, yps.new_price as new_price, cargo_owner_id
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        LEFT JOIN (
            SELECT
                sku, product_status `产品状态`, title_cn `产品名称`, develop_source, b.develop_source_name,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `new_price`
            FROM yibai_prod_base_sync.yibai_prod_sku a
            LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source b
            ON a.develop_source = b.id    
        ) yps ON ps.sku = yps.sku
        WHERE 
            ps.date_id = '{date_today}'          -- 根据需要取时间
            and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            and ps.available_stock > 0
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            and (yw.warehouse_other_type = 2 or yw.warehouse_name like '%独享%')   -- 筛选公告仓（非子仓）、或temu独享仓
            and yw.warehouse_name not in ('HC美国西仓','出口易美西仓-安大略仓','万邑通美南仓-USTX','亿迈CA01仓')  -- 剔除不常用仓库
            and ywc.name in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓','法国仓','西班牙仓','意大利仓')
        ORDER BY date_id DESC
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    df_temp['available_stock_money'] = df_temp['available_stock'] * df_temp['new_price']
    df_temp = df_temp.drop_duplicates(subset=['sku','warehouse','warehouse_id'])
    print(df_temp.info())

    df_group = df_temp.groupby(['sku','warehouse','develop_source_name','new_price']).agg(
        {'available_stock':'sum','available_stock_money':'sum'}).reset_index()

    # df_group.to_excel('F://Desktop//df_group.xlsx', index=0)

    return df_group

def get_yb_ads_order():
    """ yb订单明细 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=31)
    date_start = '2025-10-01'
    date_end = '2025-11-01'
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
            and platform_code = 'ALI'
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)
    df_order_info = df_order_info.drop_duplicates(subset=['order_id','sku','account_id'])

    # temu_account = get_temu_account()
    #
    # df_order_info = pd.merge(df_order_info, temu_account[['account_id', 'main_name']], how='left', on=['account_id'])
    #
    # # 销售额聚合处理
    # df_group = df_order_info[(df_order_info['paytime']<'2025-03-17')]
    # df_group = df_group[~df_group['warehouse'].isna()]
    # df_group = df_group.groupby(['sku','warehouse']).agg({'total_price':'sum', 'real_profit':'sum'}).reset_index()
    # df_group = df_group.rename(columns={'total_price':'total_price_yb', 'real_profit':'real_profit_yb'})
    df_order_info.to_excel('F://Desktop//yb_order_ali.xlsx', index=0)

    return df_order_info

def get_sku_info(df):
    sql = """
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
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_temp[['sku','new_price']], how='left', on=['sku'])
    df['new_price'] = df['new_price'].fillna(0).astype(float)
    return df

def get_tt_order():
    """
    TT获取订单数据
    """
    # 获取tt海外仓
    warehouse = tt_get_warehouse()

    # 取订单表
    date_today = time.strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=20)).strftime('%Y-%m-%d')
    # date_start = '2023-12-01'
    # date_end = '2024-01-01'
    sql = f"""
    with order_table as (
        SELECT distinct order_id from tt_oms_sync.tt_oms_order_base_oms 
        where 
        platform_status not in ('Canceled', 'Pending') 
        -- AND refund_status in (0, 1) 
        and order_status<>80
        and payment_status=1
        and order_id not like '%%-RE' 
        AND payment_time>='{date_start}'
        -- AND payment_time<'date_end'
        and platform_code!='SPH' AND total_price!=0
        and platform_code = 'EB'
        -- LIMIT 1000
    )
        SELECT 
        a.platform_code platform_code,
        a.ship_country ship_country,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        d.sku as sku,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.platform_order_id as platform_order_id,
        a.payment_time as payment_time,
        a.currency as currency,
        toFloat64(c.currency_rate) as currency_rate,
        toFloat64(c.total_price) as total_price,
        toInt64(d.quantity) as quantity,
        case when toFloat64(c.true_profit) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit) end as true_profit,
        d.quantity as release_money,
        d.sales_status as sales_status,
        c.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price
        FROM  (
            select * from tt_oms_sync.tt_oms_order_detail_base_oms
            where order_id in (select order_id from order_table)
        ) b 
        inner JOIN (
            select * from tt_oms_sync.tt_oms_order_base_oms
            where order_id in (select order_id from order_table)
        ) a 
        ON b.order_id=a.order_id 
        inner JOIN (
            select * from tt_oms_sync.tt_oms_order_profit_base_oms
            where order_id in (select order_id from order_table)
        ) c 
        ON a.order_id=c.order_id 
        inner join (
            select * from tt_oms_sync.tt_oms_order_sku_base_oms
            where order_id in (select order_id from order_table)
        ) d 
        on b.id=d.order_detail_id 
        """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    order_base = conn_mx.ck_select_to_df(sql)
    # 匹配成本
    order_base = get_sku_info(order_base)
    order_base['release_money'] = order_base['quantity'] * order_base['new_price']
    print(order_base.info())

    # 匹配仓库
    col = ['warehouse_id', 'warehouse_name', 'warehouse']
    order_base = order_base.merge(warehouse[col], on='warehouse_id', how='left')
    # 净利润计算。先取默认净利率，再取站点维度
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    sql = """
        select platform_code, max(net_profit2)/100 as platform_zero_1
        from tt_sale_center_listing_sync.tt_listing_profit_config 
        where shipping_type = 2 and  is_del = 0 and status = 1
        group by platform_code
    """
    df_fee1 = conn_ck.ck_select_to_df(sql)

    sql = """
        select
            platform_code,
            CASE
                WHEN site = 'US' THEN '美国仓'
                WHEN site IN ('UK', 'GB') THEN '英国仓'
                WHEN site IN ('CZ', 'CS', 'DE', 'PL') THEN '德国仓'
                WHEN site = 'FR' THEN '法国仓'
                WHEN site = 'IT' THEN '意大利仓'
                WHEN site = 'AU' THEN '澳洲仓'
                WHEN site IN ('ES', 'SP') THEN '西班牙仓'
                WHEN site = 'CA' THEN '加拿大仓'
                WHEN site = 'JP' THEN '日本仓'
                WHEN site = 'MX' THEN '墨西哥仓'
                WHEN site ='RU' THEN '俄罗斯仓'
                WHEN site ='BR' THEN '巴西仓'
                WHEN site ='PH' THEN '菲律宾仓'
                WHEN site ='TH' THEN '泰国仓'
                WHEN site ='MY' THEN '马来西亚仓'
                WHEN site ='VN' THEN '越南仓'
                WHEN site ='ID' THEN '印尼仓'
                ELSE 'UNKNOWN WAREHOUSE'
            END as warehouse,
            min(net_profit2)/100 as platform_zero_2
        from tt_sale_center_listing_sync.tt_listing_profit_config 
        where 
            platform_code in ('ALI', 'EB', 'AMAZON') 
            and shipping_type = 2 and  is_del = 0 and status = 1
        group by platform_code,warehouse 
    """
    df_fee2 = conn_ck.ck_select_to_df(sql)
    order_base = order_base.merge(df_fee1, on=['platform_code'], how='left')
    order_base = order_base.merge(df_fee2, on=['platform_code', 'warehouse'], how='left')
    order_base['platform_zero'] = np.where(~order_base['platform_zero_2'].isna(),
                                           order_base['platform_zero_2'], order_base['platform_zero_1'])

    order_base["platform_zero"] = order_base["platform_zero"].astype(float)
    order_base["real_profit"] = order_base["true_profit"] - order_base["platform_zero"] * order_base["total_price"]
    order_base["real_profit"] = order_base["real_profit"].astype(float).round(4)

    order_base.drop(['platform_zero_1', 'platform_zero_2',  'currency', 'new_price'], axis=1,
                    inplace=True)
    order_base = order_base.rename(columns={'true_profit': 'true_profit_new1'})
    order_base['payment_time'] = order_base['payment_time'].dt.strftime('%Y-%m-%d')
    #
    # order_base['warehouse_id'] = order_base['warehouse_id'].astype(str)
    # order_base = order_base[~order_base['warehouse_id'].str.contains('10000')]
    # order_base['warehouse_id'] = order_base['warehouse_id'].astype(int)

    # # 重复订单号的销售额、利润置为0
    # order_base['sales_status'] = order_base['sales_status'].fillna('')
    # column_order = ['负利润加快动销', '正利润加快动销', '正常', '', 'nan', '-']
    # print(order_base['sales_status'].unique())
    # order_base['sales_status'] = pd.Categorical(order_base['sales_status'], categories=column_order, ordered=True)
    # order_base = order_base.sort_values(by=['order_id', 'sales_status'])
    #
    # order_base['rank'] = order_base.groupby(['order_id', 'sales_status']).cumcount() + 1
    # for c in ['total_price', 'true_profit_new1', 'real_profit']:
    #     order_base[c] = np.where(order_base['rank'] != 1, 0, order_base[c])
    # order_base.drop('rank', axis=1, inplace=True)
    # order_base['sales_status'] = order_base['sales_status'].astype(str)

    # order_base.to_excel('F://Desktop//tt_order_temu.xlsx', index=0)
    # # CK存表
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # table_name = 'tt_dashbord_new_data1_base'
    # sql = f"""
    #     ALTER TABLE yibai_oversea.{table_name}
    #     DELETE where payment_time >= '{date_start}'
    # """
    # conn_ck.ck_execute_sql(sql)
    # #
    # conn_ck.ck_insert(order_base, table_name, if_exist='append')

    # # 销售额聚合处理
    # df_group = order_base[(order_base['payment_time']>='2025-03-01') & (order_base['payment_time']<'2025-03-17')]
    # df_group = df_group[~df_group['warehouse'].isna()]
    # df_group = df_group.groupby(['sku','warehouse']).agg({'total_price':'sum', 'real_profit':'sum'}).reset_index()
    # df_group = df_group.rename(columns={'total_price':'total_price_tt', 'real_profit':'real_profit_tt'})

    order_base.to_excel('F://Desktop//tt_ebay_order.xlsx', index=0)

    return order_base

def temp_temp():

    sql = """
              select site as country, 
              case when platform='Wildberries' then 'OZON' else platform end as platform, 
              pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee,
              case when platform in ('AMAZON','ALLEGRO') then 0.05 when platform in ('SHOPEE') then 0
              else 0.03 end as promotion_fee
              from yibai_wish.yibai_platform_fee
              
              UNION ALL
              
              select site as country, 
              'DMSOzon' as platform, 
              pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee,
              case when platform in ('AMAZON','ALLEGRO') then 0.05 when platform in ('SHOPEE') then 0
              else 0.03 end as promotion_fee
              from (SELECT * FROM yibai_wish.yibai_platform_fee WHERE platform = 'Wildberries') a
    """
    conn_ck = pd_to_ck(database='yibai_wish', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.to_excel('F://Desktop//df_wish.xlsx', index=0)
    print(df)


def tt_get_order_detail():
    """ 获取不同平台订单明细数据 """
    # date_today = datetime.date.today()
    # date_start = date_today - datetime.timedelta(days=10)
    date_start = '2026-02-01'
    date_end = '2026-12-01'

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM tt_oms_sync.tt_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            and platform_code in ('SHOPEE')
            -- and warehouse_id = 478
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,
        A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN C.true_shipping_fee > 0 THEN C.true_profit_new1
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
        B.ship_country,
        A.location,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN C.true_shipping_fee > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp) 
            -- and  ship_country = 'US'
        ) A
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
        INNER JOIN (
            SELECT 
                yw.id AS warehouse_id,   
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
                yw.country, ebay_category_id,
                ywc.name AS warehouse, yw.warehouse_type
            FROM tt_logistics_tms_sync.tt_warehouse yw
            LEFT JOIN tt_logistics_tms_sync.tt_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and ywc.name in ('美国仓','加拿大仓')
        ) W ON B.warehouse_id = W.warehouse_id
    """
    # conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    conn_ck = pd_to_ck(database='tt_oms_sync', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    # df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    df.to_excel('F://Desktop//oversea_order_monitor_shopee.xlsx', index=0)

    return df

def stock_and_order():
    """ 海外仓库存，不同开发来源下的订单净利率分布 """
    tt_order = get_tt_order()

    yb_order = get_yb_ads_order()

    df_stock = get_stock()

    df = pd.merge(df_stock, yb_order, how='left', on=['sku','warehouse'])
    df = pd.merge(df, tt_order, how='left', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_result.xlsx', index=0)

    # return df

def get_dom_order_temp():
    """ 获取国内仓订单明细数据 """
    # date_today = datetime.date.today()
    # date_start = date_today - datetime.timedelta(days=10)
    date_start = '2025-09-01'
    date_end = '2025-10-01'

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            and platform_code in ('AMAZON')
            and warehouse_id = 478
            -- and ship_country = 'US'
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        B.warehouse_id,
        B.ship_country,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status
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

    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())
    print(df['ship_country'].unique())

    # df.to_excel('F://Desktop//dom_order_temp.xlsx', index=0)

    return df

def get_dom_order_temp_2():
    """ 获取国内仓订单明细数据 """
    # date_today = datetime.date.today()
    # date_start = date_today - datetime.timedelta(days=10)
    date_start = '2025-02-09'
    date_end = '2025-05-10'

    sql = """
        SELECT order_id, platform_code, account_id, seller_sku, total_price, quantity, sku, `sku数量`,
        `毛利润`, `净利润`, `销库金额`
        FROM domestic_warehouse_clear.monitor_dom_order
        WHERE `站点` = '美国' and created_time > '2025-02-01' 
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())

    df.to_excel('F://Desktop//dom_monitor_order.xlsx', index=0)

    return df

def dashbord_order():
    date_start = '2026-04-01'
    date_end = '2025-10-01'
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,complete_status,platform_code, sku, 
            seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            -- and paytime < '{date_end}'
            -- and `total_price` > 0 
            and `sales_status` not in ('总计')
            and warehouse_name not like '%精品%'
            -- and platform_code in ('SHOPEE', 'LAZADA')
            -- and platform_code in ('WALMART')
            -- and sku = '3118240156511'

    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    print(df_temp.info())

    # df_temp.to_excel('F://Desktop//df_dashbord2.xlsx', index=0)

    return df_temp

def tt_dashbord_order():
    date_start = '2026-01-01'
    date_end = '2025-10-22'
    sql = f"""
        SELECT
            order_id, platform_code, sku, seller_sku, account_id, payment_time, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity,  release_money, sales_status
        FROM yibai_oversea.tt_dashbord_new_data1
        WHERE 
            payment_time >= '{date_start}'
            -- and payment_time < '{date_end}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and platform_code in ('SHOPEE')

    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    print(df_temp.info())

    df_temp.to_excel('F://Desktop//tt_dashbord.xlsx', index=0)

    return df_temp

## 1、订单异常排查
"""
1、获取订单
2、获取调价链接明细（all）
3、获取sku明细（dtl、dwm）
"""
# 调价链接明细
def get_all_listing(platform='AMAZON'):
    """ 获取 oversea_listing_all 待调价数据"""
    if platform == 'AMAZON':
        sql = """
            SELECT *
            FROM yibai_oversea.oversea_amazon_listing_all
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_amazon_listing_all)
        """
    elif platform == 'WALMART':
        sql = """
            SELECT *
            FROM yibai_oversea.oversea_walmart_listing_all
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_walmart_listing_all)
        """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_mx = conn.read_sql(sql)
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # df.to_excel('F://Desktop//df_mx_all.xlsx', index=0)

    return df

def get_order_and_all():
    """ """

    df_all = get_all_listing(platform='WALMART')

    df_order = dashbord_order()

    df = pd.merge(df_order, df_all, how='left', on=['account_id', 'seller_sku'])

    df.to_excel('F://Desktop//df_walmart_order.xlsx', index=0)

    return df

## temp
def get_yibai_logistics_logistics():
    """  渠道名 """
    sql = """
        SELECT distinct ship_code, ship_name
        FROM yibai_tms_logistics_sync.yibai_logistics_logistics
    """
    conn_ck = pd_to_ck(database='', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # df.to_excel('F://Desktop//df_logistics.xlsx', index=0)
    return df

def oversea_sku_orders():
    """ 海外仓正常品sku, 在amazon海外仓及国内仓的销量 """
    sql = """
        SELECT sku,warehouse, new_price, available_stock, available_stock_money
        FROM over_sea.dwm_sku_temp_info a
        WHERE date_id = '2025-09-24' and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT sku ,warehouse ,sale_status
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    df_sale_status = conn.read_sql(sql)

    df = pd.merge(df, df_sale_status, how='left', on=['sku','warehouse'])

    df['sale_status'] = df['sale_status'].fillna('正常')

    # df = df[df['sale_status']=='正常']

    # 海外仓订单
    oversea_order = dashbord_order()
    df_oversea = oversea_order.groupby(['sku','warehouse']).agg({'quantity':'sum','release_money':'sum'}).reset_index()

    # 国内仓订单
    order_fbm = get_dom_order_temp()
    dic = {'ES':'德国仓', 'IT':'德国仓', 'MX':'墨西哥仓', 'CA':'加拿大仓', 'DE':'德国仓', 'TR':'德国仓',
           'FR':'德国仓', 'NL':'德国仓', 'BE':'德国仓', 'US':'美国仓', 'AU':'澳洲仓', 'SE':'德国仓',
           'UK':'英国仓', 'GB':'英国仓', 'AE':'阿联酋仓', 'PL':'德国仓'}
    order_fbm['warehouse'] = order_fbm['ship_country'].replace(dic)
    order_fbm = order_fbm.groupby(['sku', 'warehouse']).agg({'quantity':'sum'}).add_suffix('_fbm').reset_index()
    # print(order_fbm['站点'].unique())

    df = pd.merge(df, df_oversea, how='left', on=['sku','warehouse'])
    df = pd.merge(df, order_fbm, how='left', on=['sku', 'warehouse'])
    df['release_money_fbm'] = df['quantity_fbm'] * df['quantity']

    df.to_excel('F://Desktop//df_order_2.xlsx', index=0)


def get_clear_sku_order():
    """ 每月清仓任务销库统计 """
    # df_sku = pd.read_excel('F://Ding_workspace//YB泛品海外仓202602清仓SKU明细.xlsx', dtype={'sku': str})
    df_sku = pd.read_excel('F://Ding_workspace//YB泛品海外仓202604清仓方案 0409.xlsx', dtype={'sku': str})
    df_sku = df_sku[['sku','warehouse','清仓类别']]
    # df_sku = df_sku[df_sku['是否可放缓']=='不可放缓']
    df_sku = df_sku.drop_duplicates(subset=['sku','warehouse'])
    print(df_sku.info())
    # YB
    df_order = dashbord_order()
    df = pd.merge(df_order, df_sku, how='left', on=['sku','warehouse'])
    df.to_excel('F://Desktop//df_clear_order.xlsx', index=0)

    # # 亿迈销库
    # df_dcm = get_dcm_order()
    # df_dcm = df_dcm[~df_dcm['warehouse'].isna()]
    # df_dcm['销库金额'] = df_dcm['new_price'] + df_dcm['订单销量']
    # df_dcm = pd.merge(df_dcm, df_sku, how='inner', on=['sku','warehouse'])
    # df_dcm.to_excel('F://Desktop//df_dcm_clear_order.xlsx', index=0)



def flash_sku_monitor():
    """
    限时清仓任务监控
    1、完成情况监控
    2、sku出单情况监控
    """
    df_flash = get_flash_sku()
    df_flash = df_flash[['sku','warehouse','source']]
    # 1、整体完成情况
    df_order = dashbord_order()

    df = pd.merge(df_order, df_flash, how='left', on=['sku', 'warehouse'])

    print(df.info())


def get_dwm_sku_info():
    """ sku基础信息 """
    sql = """
        SELECT sku, warehouse, best_warehouse_name, new_price, type, available_stock, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        and warehouse in ('菲律宾仓', '泰国仓', '印度尼西亚仓', '越南仓', '马来西亚仓')
        and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//df_sku.xlsx', index=0)


if __name__ == '__main__':

    # 订单明细
    # get_order_temp()

    # get_yibai_logistics_logistics()
    # dashbord_order()
    # get_dwm_sku_info()
    # yb_order_temp()
    # get_dcm_order()
    get_clear_sku_order()  # 给李总的清仓sku销库金额统计
    # ebay_order_history() # 给ebay的历史订单数据

    # get_ca_temp()
    # oversea_sku_orders()
    # get_yb_ads_order()
    # tt_dashbord_order()
    # flash_sku_monitor()

    # tt_get_order_detail()
    # get_temu_temp()
    # get_order_and_all()
    # tt_get_shopee_listing_temp()