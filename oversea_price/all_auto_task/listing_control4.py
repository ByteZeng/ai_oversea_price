import datetime
import traceback
import pandas as pd
import numpy as np
import time
import os
import warnings
from all_auto_task.clickhouse_con import send_ck_cls
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.scripts_ck_client import CkClient
warnings.filterwarnings("ignore")
os.environ['NUMEXPR_MAX_THREADS'] = '16'
from pulic_func.base_api.mysql_connect import *


def str_to_day(time_str):
    time_str = time_str[0:10]
    return time_str


def days_10_ago_date():
    d = datetime.date.today() - datetime.timedelta(days=100)
    return d

# def days_10_after_date(t2):
#     d = datetime.date.today() - datetime.timedelta(days=t2)
#     return d

def get_jp_account():
    """ 获取精品子仓的数据 """
    sql = """
        select distinct platform_code,account_id 
        from yibai_sale_center_system_sync.yibai_system_account
        where is_yibai != 1
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_system_sync', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)

    return df_account

def delect_jp_data():
    """ 删除精品账号数据 """
    df_account = get_jp_account()
    # print(df_account['platform_code'].value_counts())
    df_account['account_id'] = df_account['account_id'].fillna(0).astype(int)

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    for i in df_account['platform_code'].unique():
        df_temp = df_account[df_account['platform_code']==i]
        sql = f"""
            ALTER TABLE yibai_oversea.dashbord_new_data1 DELETE
            where platform_code = '{i}' and account_id in {tuple(df_temp['account_id'].unique())} 
        """
        # print(sql)
        conn_ck.ck_execute_sql(sql)
    # sql = f"""
    #     SELECT count(1)
    #     FROM over_sea.dashbord_new_data1
    #     where (platform_code, account_id) in {tuple1}
    #     and paytime < '2026-09-01'
    #
    # """


    # conn_ck.ck_execute_sql(sql)


def get_supplier_sku():
    """ 获取供应商sku """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = """
        SELECT YM_sku sku, warehouse, 1 as is_supplier_sku
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and warehouse != 'CN'

        UNION ALL

        SELECT YB_sku sku, warehouse, 0 as is_supplier_sku
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and warehouse != 'CN'
    """
    df = conn_ck.ck_select_to_df(sql)
    df = df[~df['sku'].isna()]
    df = df.drop_duplicates(subset=['sku', 'warehouse'])

    return df

def get_supplier_order_sku():
    """ 获取供应商订单实际配库sku """
    sql = """
    select 
        d.order_id as order_id,b.sku as package_detail_sku,a.similar_sku as `配库sku`
    from yibai_oms_sync.yibai_oms_order_package_sku_allot_match_sup a
    join yibai_oms_sync.yibai_oms_order_package_detail b on b.id = a.package_detail_id and b.sku = a.merchant_sku
    join yibai_oms_sync.yibai_oms_order_package_relation d on d.package_id = a.package_id 
    where a.is_delete = 0 and a.use_status in (5) and d.is_delete = 0 
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    col = ['order_id','配库sku']
    df = df[col].drop_duplicates()

    return df

def get_sku_type(df_order):
    # 开发来源
    sql = f"""
            select a.sku `配库sku`,  develop_source_name
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
            INNER JOIN (
                SELECT distinct id as develop_source, develop_source_name
                FROM yibai_prod_base_sync.yibai_prod_develop_source
                -- WHERE develop_source_name = '供应商货盘'
            ) c on a.develop_source = c.develop_source

        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df = df.drop_duplicates(subset='配库sku')

    df_order = pd.merge(df_order, df[['配库sku', 'develop_source_name']], how='left', on=['配库sku'])

    return df_order

def get_supplier_order(df_order):
    """ 供应商sku订单的差值需要减5% """

    # 1、配库sku
    df_order_sku = get_supplier_order_sku()
    df_order = pd.merge(df_order, df_order_sku, how='left', on='order_id')
    df_order['配库sku'] = np.where(df_order['配库sku'].isna(), df_order['sku'], df_order['配库sku'])

    # 2、供应商sku
    df_order = get_sku_type(df_order)

    # 3、差值替换
    # 20260323 取供应商sku差值。先用平台+国家匹配，再用平台匹配，最后匹配不上的填充0.17
    sql = """

        SELECT platform_code, site, toFloat32(net_profit2)/100 as `差值sup`
        FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
        WHERE shipping_type = 5 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_fee1 = conn_ck.ck_select_to_df(sql)
    df_fee2 = df_fee1[df_fee1['site']=='other']
    df_fee2 = df_fee2.rename(columns={'差值sup':'差值sup2'})
    df_fee2 = df_fee2.drop_duplicates(subset='platform_code')
    df_order = pd.merge(df_order, df_fee1, how='left', on=['platform_code', 'site'])
    df_order = pd.merge(df_order, df_fee2[['platform_code','差值sup2']], how='left', on=['platform_code'])

    df_order['差值sup'] = np.where(df_order['差值sup'].isna(), df_order['差值sup2'], df_order['差值sup'])

    c1 = (df_order['develop_source_name']=='供应商货盘') & (~df_order['差值sup'].isna())
    df_order['差值'] = np.where(c1, df_order['差值sup'], df_order['差值'])
    # df_order['差值'] = np.where(c1, df_order['差值']-0.05, df_order['差值'])

    df_order.drop(['配库sku','差值sup','差值sup2'], axis=1, inplace=True)

    return df_order

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

def get_base_data2(conn_ck, conn_mx):

    sql_oms1 = f"""
    with order_id_list as (
        select distinct order_id from yibai_oms_sync.yibai_oms_order
        where payment_time>='{days_10_ago_date()}'
        and payment_status=1 and order_status <> 80
        and warehouse_id not in (60,323)
        and warehouse_id in (select id from yibai_logistics_tms_sync.yibai_warehouse where warehouse_type in (2,3))
    )
    
    SELECT distinct A.order_id as order_id,B.platform_order_id as platform_order_id,B.platform_code as platform_code,
        F.sku as sku,
        CASE when A.platform_code='EB' then A.item_id else A.seller_sku end as seller_sku,
        B.account_id as account_id,
        B.payment_time as paytime,
        B.payment_time as created_time,
        -- H.total_price,
        toFloat64(C.total_price) as total_price,
        if(toFloat64(C.true_shipping_fee) > 0, toFloat64(C.true_profit_new1), toFloat64(C.profit_new1)) as true_profit_new1,
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
        if(B.payment_status = 1, '已付款', '未付款') as pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        'status' as status,
        'warehouse_name' as warehouse_name,
        B.warehouse_id as warehouse_id,
        case 
            when B.warehouse_id=434 then '意大利仓'
            when B.warehouse_id=529 then '意大利仓'
            when B.warehouse_id=349 then '加拿大仓'
            when B.warehouse_id=443 then '日本仓'
        end as warehouse,
        B.ship_country site,
        B.ship_code,
        1 as new_price,
        A.quantity as quantity,
        A.quantity as release_money,
        -- G.sales_status as sales_status
        F.sales_status as sales_status,
        commission_fees, pay_cost, seller_discount, escrow_tax,
        purchase_cost_new1,
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee,
        toFloat64(first_carrier_cost)+toFloat64(duty_cost) first_carrier_cost,
        toFloat64(processing)+toFloat64(package_cost)+toFloat64(oversea_package_fee)+toFloat64(pack) processing,
        toFloat64(extra_price)+toFloat64(exceedprice)+toFloat64(residence_price) extra, 
        toFloat64(stock_price)+toFloat64(exchange_price) stock_exchange
    FROM (select * from yibai_oms_sync.yibai_oms_order_detail where order_id in order_id_list) A
    inner JOIN (select * from yibai_oms_sync.yibai_oms_order where order_id in order_id_list) B 
    ON A.order_id=B.order_id
    inner JOIN (select * from yibai_oms_sync.yibai_oms_order_profit where order_id in order_id_list) C 
    ON B.order_id=C.order_id
    inner join (select * from yibai_oms_sync.yibai_oms_order_sku where order_id in order_id_list) F 
    on F.order_detail_id=A.id
    """

    sql_oms10 = f"""
    with order_id_list as (
        select distinct order_id from yibai_oms_sync.yibai_oms_order
        where payment_time>='{days_10_ago_date()}'
        and payment_status=1 and order_status <> 80
        and warehouse_id not in (60,323)
        and warehouse_id in (select id from yibai_logistics_tms_sync.yibai_warehouse where warehouse_type in (2,3))
    )
    
    SELECT distinct A.order_id as order_id,B.platform_order_id as platform_order_id,B.platform_code as platform_code,
        F.sku as sku,
        CASE when A.platform_code='EB' then A.item_id else A.seller_sku end as seller_sku,
        B.account_id as account_id,
        B.payment_time as paytime,
        B.payment_time as created_time,
        -- H.total_price,
        toFloat64(C.total_price) as total_price,
        if(toFloat64(C.true_shipping_fee)>0, toFloat64(C.true_profit_new1), toFloat64(C.profit_new1)) as true_profit_new1,
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
        if(B.payment_status = 1, '已付款', '未付款') as pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        'status' as status,
        'warehouse_name' as warehouse_name,
        B.warehouse_id as warehouse_id,
        case 
            when B.warehouse_id=434 then '意大利仓'
            when B.warehouse_id=529 then '意大利仓'
            when B.warehouse_id=349 then '加拿大仓'
            when B.warehouse_id=443 then '日本仓'
        end as warehouse,
        B.ship_country site,
        1 as new_price,
        A.quantity as quantity,
        A.quantity as release_money,
        '总计' as sales_status
    FROM (select * from yibai_oms_sync.yibai_oms_order_detail where order_id in order_id_list) A
    inner JOIN (select * from yibai_oms_sync.yibai_oms_order where order_id in order_id_list) B 
    ON A.order_id=B.order_id
    inner JOIN (select * from yibai_oms_sync.yibai_oms_order_profit where order_id in order_id_list) C 
    ON B.order_id=C.order_id
    inner join (select * from yibai_oms_sync.yibai_oms_order_sku where order_id in order_id_list) F 
    on F.order_detail_id=A.id
    """

    df = pd.DataFrame()
    # conn = connect_to_sql(database='yibai_oms_order', data_sys='新订单系统从库')
    for sql in [sql_oms1]:
        # df_new = conn.read_sql(sql)
        df_new = conn_ck.ck_select_to_df(sql)
        print('df_new', df_new.shape)
        df = df.append(df_new)
    # conn.close()
    print(df.head())
    print(2)
    sql_product = """
        with 
            [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17] as product_status_arr,
            ['已创建', '已开发', '待买样', '待品检', '待编辑', '待拍摄', '待编辑待拍摄', '待修图', '在售中', '审核不通过', '停售', 
            '待清仓', '已滞销', '待物流审核', '待关务审核', 'ECN资料变更中', 'ECN资料变更驳回'] 
        as product_status_desc_arr	 
        select sku,transform(product_status, product_status_arr, product_status_desc_arr, '未知') as product_status_test,
        CASE
            when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price)
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price)
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost)
            when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
            else toFloat64(product_cost)
        END as new_price_test
        FROM yibai_prod_base_sync.yibai_prod_sku
    """
    df_product = conn_ck.ck_select_to_df(sql_product)
    # 匹配上对应的成本，产品状态，并重新计算销库金额
    df = df.merge(df_product, on=['sku'], how='left')
    df['new_price'] = df['new_price_test']
    df['status'] = df['product_status_test']
    df['release_money'] = df['quantity'] * df['new_price']
    df.drop(['new_price_test', 'product_status_test'], axis=1, inplace=True)

    sql_warehouse = """select E.id as warehouse_id,E.warehouse_type,E.warehouse_name as warehouse_name_1,F.name as warehouse_1
                    from yibai_logistics_tms_sync.yibai_warehouse E
                    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category F ON E.ebay_category_id = F.id"""
    df_warehouse = conn_ck.ck_select_to_df(sql_warehouse)
    # df_warehouse = sql_to_pd(sql=sql_warehouse, database='yibai_logistics', data_sys='数据管理部同步服务器')
    print(df_warehouse.head())

    # 对于df需要用到销售状态
    # 先匹配和筛选仓
    df = df.merge(df_warehouse, on=['warehouse_id'], how='left')
    df['warehouse_name'] = df['warehouse_name_1']
    df = df[df['warehouse_type'].isin([2, 3])]  # 选取warehouse_type<>1的数据,即剔除国内仓

    df.loc[df['warehouse'].isnull(), 'warehouse'] = df['warehouse_1']
    df.loc[(df['warehouse'].isnull()) & (df['warehouse_name'].str.contains('FBA')), 'warehouse'] \
        = df['warehouse_name'].apply(lambda x: str(x).split('FBA')[-1])
    df.drop(columns=['warehouse_name_1', 'warehouse_1', 'warehouse_type', 'warehouse_id'], inplace=True)
    #
    df = df[df["total_price"].notnull()]
    df = df[df["total_price"] != '']
    df["created_time"] = df["created_time"].astype(str)
    df["created_time"] = df["created_time"].apply(lambda x: str_to_day(x))
    df["paytime"] = df["paytime"].astype(str)
    df["paytime"] = df["paytime"].apply(lambda x: str_to_day(x))

    # 20251205 切换取中台差值表。先用平台+国家匹配，再用平台匹配，最后匹配不上的填充0.17
    sql = """

        SELECT platform_code, site, toFloat32(net_profit2)/100 as `差值`
        FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
        WHERE shipping_type = 2 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_fee1 = conn_ck.ck_select_to_df(sql)
    df_fee2 = df_fee1[df_fee1['site']=='other']
    df_fee2 = df_fee2.rename(columns={'差值':'差值2'})
    df_fee2 = df_fee2.drop_duplicates(subset='platform_code')
    df = pd.merge(df, df_fee1, how='left', on=['platform_code', 'site'])
    df = pd.merge(df, df_fee2[['platform_code','差值2']], how='left', on=['platform_code'])
    df['差值'] = np.where(df['差值'].isna(), df['差值2'], df['差值'])
    df['差值'] = df['差值'].fillna(0.17).astype(float)

    # 20260313 供应商订单差值需要再减5%
    # 20260316 补充sku开发来源。可以识别出供应商sku
    df = get_supplier_order(df)

    df["real_profit"] = df["true_profit_new1"] - df["差值"] * df["total_price"]
    df["real_profit"] = df["real_profit"].astype(float)
    df["real_profit"] = df["real_profit"].round(4)
    df.drop(['差值2', '差值'], axis=1, inplace=True)
    df = df[(df['total_price'] > 0) | (df['warehouse'] != '墨西哥仓')]

    # 去重
    df = df.drop_duplicates()
    df.insert(loc=20, column='real_profit', value=df.pop("real_profit"))

    col = ['total_price','true_profit_new1','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_fee', 'extra',
           'first_carrier_cost', 'seller_discount', 'stock_exchange']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    df['escrow_tax'] = np.where(df['site'].isin(col), 0, df['escrow_tax'])

    # 渠道名称替换
    df_ship_code = get_yibai_logistics_logistics()
    df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    df.drop('ship_code', axis=1, inplace=True)

    print(df.info())
    # 20260106 去掉精品账号的销库数据
    df_jp_account = get_jp_account()
    df_jp_account['is_jp'] = 1
    df = pd.merge(df, df_jp_account, how='left', on=['platform_code', 'account_id'])
    df['is_jp'] = df['is_jp'].fillna(0).astype(int)

    # df.to_excel('F://Desktop//df_order_temp_2.xlsx', index=0)
    #
    # df = df[df['is_jp']==0]
    # df.drop('is_jp', axis=1, inplace=True)
    #
    print(df.info())
    # 存表
    conn_mx.write_to_ck_json_type(df, 'dashbord_new_data1')


def run_listing_contorl():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn_mx = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
    alter table yibai_oversea.dashbord_new_data1 delete where created_time>='{days_10_ago_date()}'
    """
    conn_mx.ck_execute_sql(sql)
    sql = """
    optimize table yibai_oversea.dashbord_new_data1 final
    """
    conn_mx.ck_execute_sql(sql)
    while 1:
        sql = f"""
        select count() as num from yibai_oversea.dashbord_new_data1
        where created_time>='{days_10_ago_date()}'
        """
        df = conn_mx.ck_select_to_df(sql)
        if df['num'][0] > 0:
            time.sleep(10)
        else:
            break
    print('开始获取订单')
    #
    # sql = f"delete from dashbord_new_data1 where created_time>='{days_10_ago_date()}' "
    # conn.execute(sql)
    #
    get_base_data2(conn_ck, conn_mx)
    conn.close()
    #
    # sql = """
    #     delete from dashbord_new_data1
    #     where order_id in (
    #         select order_id from (
    #             select order_id,count(order_id) as flag
    #             from dashbord_new_data1
    #             group by order_id
    #             order by flag desc) a where flag>10
    #         )
    #     """
    # conn.execute(sql)
    # conn.close()


def write_to_mysql_and_ck():
    # 不再写入mysql并清空mysql的数据
    try:
        # 清空ck数据库
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        conn_ck.ck_clear_table(sheet_name='dashbord_new1', db_name='yibai_oversea')
        # send_ck_cls.ck_clear_table(sheet_name='dashbord_new1')
        sql = """
        INSERT INTO yibai_oversea.dashbord_new1
        SELECT DISTINCT *, now()
        FROM (
            SELECT
                `created_time`,
                `order_id`,
                `platform_order_id`,
                `platform_code`,
                `sku`,
                `seller_sku`,
                `account_id`,
                `paytime`,
                `total_price`,
                `true_profit_new1`,
                `complete_status`,
                `order_typr`,
                `pay_status`,
                `refound_status`,
                `status`,
                `warehouse_name`,
                `warehouse`,
                `new_price`,
                `quantity`,
                `release_money`,
                `real_profit`,
                `sales_status`
            FROM
                yibai_oversea.dashbord_new_data1
        )
        """
        # send_ck_cls.client.execute(sql)
        conn_ck.ck_execute_sql(sql)
    except Exception as e:
        # send_msg('动销组定时任务推送', '海外仓订单监控',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓订单监控数据处理完成处理出现问题,请及时排查,失败原因详情请查看airflow日志",
        #          mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
        #          status='失败')
        raise Exception(traceback.format_exc())


if __name__ == '__main__':
    # run_listing_contorl()

    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn_mx = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    get_base_data2(conn_ck, conn_mx)

    # delect_jp_data()
