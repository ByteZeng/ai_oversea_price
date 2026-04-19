"""
各平台海外仓链接中间表。
1、YB、TT各平台海外仓链接初始数据
2、需满足调价使用、刊登覆盖率统计使用
3、ads命名，存储到CK
4、历史记录保存不超过一周
5、优先用sql实现 CK --> CK

海外仓刊登覆盖率统计

"""

import numpy as np
import pandas as pd
import random
import json
import time, datetime
import tqdm
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from warnings import filterwarnings
from all_auto_task.oversea_temu_price import get_main_resp, get_temu_account, get_freight_subsidy
filterwarnings('ignore')

##
def get_sku_info():
    # 库存表获取
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = time.strftime('%Y%m%d')
    # date_today = '20241106'
    sql = f"""
        SELECT
            sku, warehouse, Max(date_id) date_id,
            argMax(warehouse_id, available_stock) AS max_stock_warehouse_id,
            argMax(warehouse_name, available_stock) AS max_stock_warehouse_name,
            sum(available_stock) AS total_available_stock,
            arrayStringConcat(groupArray(warehouse_stock), ', ') warehouse_stock_info       
        FROM (
            SELECT
                ps.sku sku, toString(toDate(toString(date_id))) date_id,  yw.id AS warehouse_id,
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
                available_stock, warehouse_stock
            FROM (
                SELECT 
                    trim(sku) sku, warehouse_id, date_id, cargo_owner_id, available_stock,
                    concat(toString(warehouse_id),':',toString(available_stock)) as warehouse_stock
                FROM yb_datacenter.yb_stock
                WHERE 
                    date_id = '{date_today}' -- 根据需要取时间
                    and available_stock > 0 
                    and cargo_owner_id = 8  -- 筛选货主ID为8的
            )AS ps
            INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE         
                yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
                and (yw.warehouse_other_type = 2 or warehouse_id in (958))   -- 筛选公共仓(非子仓\独享仓), 墨西哥toB可以保留
                and yw.warehouse_name not like '%独享%'
            ORDER BY date_id DESC
        ) a
        GROUP BY sku, warehouse
    """
    df_stock = conn_ck.ck_select_to_df(sql)
    # print(df_stock.info())
    # df_stock.to_excel('F://Desktop//df_stock.xlsx', index=0)

    # 产品线、成本
    df_stock = get_line(df_stock)

    # 销售状态和库龄表获取
    sql = """
        SELECT
            A.sku sku, A.warehouse warehouse, overage_level, day_sales, IF(D.sale_status IS NULL ,'正常', D.sale_status) as '销售状态'
        FROM dwm_sku_temp_info A
        LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
        WHERE 
            date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
    """
    df_status = conn.read_sql(sql)
    df = pd.merge(df_stock, df_status, how='left', on=['sku','warehouse'])
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    df['销售状态'] = df['销售状态'].fillna('正常')

    # 仓库区域
    df['area'] = np.where(df['warehouse'].str.contains('德国|法国|西班牙|意大利'),'欧洲', df['warehouse'].str[:-1])
    df['area'] = np.where(df['warehouse']=='乌拉圭仓', '巴西', df['area'])
    print(f"仓库区域有：{df['area'].unique()}")
    df['site'] = df['warehouse'].replace(
        {'德国仓': 'de', '美国仓': 'us', '澳洲仓': 'au', '英国仓': 'uk', '墨西哥仓':'mx',  '加拿大仓': 'ca', '法国仓': 'fr',
         '乌拉圭仓':'br', '西班牙仓':'es', '意大利仓':'it','马来西亚仓':'my', '泰国仓':'th', '菲律宾仓':'ph', '俄罗斯仓':'ru',
         '印度尼西亚仓':'id', '越南仓':'vn', '阿联酋仓':'ae', '日本仓':'jp','沙特阿拉伯仓':'sa','巴西仓':'br'})
    print(f"仓库所在国家有：{df['site'].unique()}")

    # 处理仓标数据
    df = warehouse_mark(df)

    # 责任人
    df['主体账号'] = ' '
    df['站点'] = df['area'].replace('澳洲','澳大利亚')
    df = get_main_resp(df)
    df.drop(['一级产品线','二级产品线','主体账号','is_same','站点'], axis=1, inplace=True)
    # print(df.info())

    return df

def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku sku, title_cn `产品名称`, develop_source, b.category_path as `产品线路线`, spu,
            CASE 
                when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                else toFloat64(product_cost) 
            END as `new_price`
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[1]
    # df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    # df_line['四级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[3]
    df_line = df_line.drop_duplicates(subset='sku')

    # 取开发来源
    sql = """
        SELECT distinct id as develop_source, develop_source_name
        FROM yibai_prod_base_sync.yibai_prod_develop_source
    """
    df_source = conn_ck.ck_select_to_df(sql)
    df_line = pd.merge(df_line, df_source, how='left', on=['develop_source'])
    # c1 = df_line['develop_source'].isin([14,15,22])
    # c2 = df_line['产品名称'].str.contains('通拓')
    # df_line['is_tt_sku'] = np.where(c1 | c2, 1, 0)
    df = pd.merge(df, df_line[['sku','spu','develop_source_name','一级产品线','二级产品线','new_price']], how='left', on=['sku'])

    return df

# 补充云仓美西仓、云仓波兰仓、云仓德国仓sku
def get_cloud_stock():
    """ 获取云仓sku """
    sql = """
        SELECT
            trimBoth(ps.sku) AS sku,ywc.name AS warehouse, yw.warehouse_name AS warehouse_name,
            yw.warehouse_code AS warehouse_code, yw.id AS warehouse_id,
            yw.warehouse_other_type AS warehouse_other_type, stock,
            available_stock, allot_on_way_count AS on_way_stock,
            wait_outbound, frozen_stock
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
        WHERE 
            (ps.date_id = toYYYYMMDD(now())) AND (ps.cargo_owner_id = 8) 
            AND (yw.warehouse_type IN (2,3)) 
            AND (yw.warehouse_name IN ('云仓美西仓','云仓德国仓','云仓波兰仓'))
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())



def get_amazon_listing():
    """
    """
    print("===Amazon刊登链接数据(海外仓发货模式)==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_amazon_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    # 分账号取数
    sql = """
    select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from yibai_system_kd_sync.yibai_amazon_account b
        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
        and account_id in (
            select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
            where account_type = 1 and is_yibai =1)
    """
    df_account = conn_ck.ck_select_to_df(sql)
    account_list = list(df_account['account_id'].unique())
    print(len(account_list))
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取amazon海外仓链接...')
            for i in range(0, len(account_list), 2000):
                list_temp = account_list[i:i + 2000]
                sql = f"""
                    insert into yibai_oversea.{table_name}
                    with listing_table as (
                        SELECT DISTINCT account_id, seller_sku
                        FROM yibai_product_kd_sync.yibai_amazon_sku_map a
                        WHERE
                        account_id in {list_temp} and 
                        (
                            (deliver_mode = 2 AND (
                                a.sku IN (SELECT sku FROM yb_datacenter.v_oversea_stock) 
                                OR a.sku LIKE '%*%' OR a.sku LIKE '%+%'))
                            OR
                            (deliver_mode = 1 AND a.sku IN (
                                SELECT sku
                                FROM yibai_prod_base_sync.yibai_prod_sku a
                                INNER JOIN yibai_prod_base_sync.yibai_prod_develop_source b
                                   ON a.develop_source = b.id
                                WHERE develop_source_name = '供应商货盘'))
                        )
                    ) 
                    select distinct b.account_id as account_id, b.account_name as account_name,
                        group_name, short_name, 
                        if(b.site ='sp', 'es', b.site) as site,
                        status, e.sku as sku, a.seller_sku as seller_sku,
                        if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
                        open_date, deliver_mode, 
                        fulfillment_channel,  a.price as online_price, 
                        formatDateTime(today(), '%Y-%m-%d') AS date_id
                    from (
                        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
                        from yibai_product_kd_sync.yibai_amazon_listings_all_raw2
                        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) a
                    inner join (
                        select distinct account_id, sku, seller_sku, deliver_mode
                        from yibai_product_kd_sync.yibai_amazon_sku_map
                        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                        -- and deliver_mode=2
                    ) e
                    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
                    left join (
                        select account_id, seller_sku, asin1
                        from yibai_product_kd_sync.yibai_amazon_listing_alls
                        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) t
                    on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
                    inner join (
                        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
                        from yibai_system_kd_sync.yibai_amazon_account b
                        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
                        and account_id in (
                            select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
                            where account_type = 1 and is_yibai =1)
                    ) b
                    on (a.account_id= b.account_id)
                    inner join (
                        select group_id, group_name
                        from yibai_system_kd_sync.yibai_amazon_group
                        where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
                        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
                    ) c
                    on (b.group_id=c.group_id)
                    left join (
                        select account_id, seller_sku, ListingPrice as sale_price
                        from yibai_product_kd_sync.yibai_amazon_listing_price
                        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) f
                    on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
                    order by a.create_time desc
                    -- limit 1 by sku, site, asin
                    -- settings max_memory_usage = 500000000000
                """
                conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
                conn_ck.ck_execute_sql(sql)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')

    # 数量监控
    check_listing_num(table_name)

    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)


    return None

def get_amazon_listing_all():
    """
    20241206 统计清仓sku链接数量时，需要包含国内仓链接
    """
    print("===Amazon刊登链接数据(含国内发货模式)==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_amazon_listing_all'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    # 分账号取数
    sql = """
    select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from yibai_system_kd_sync.yibai_amazon_account b
        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
        and account_id in (
            select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
            where account_type = 1 and is_yibai =1)
    """
    df_account = conn_ck.ck_select_to_df(sql)
    account_list = list(df_account['account_id'].unique())
    print(len(account_list))
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取amazon海外仓链接...')
            for i in range(0, len(account_list), 1000):
                list_temp = account_list[i:i + 1000]
                sql = f"""
                    insert into yibai_oversea.{table_name}
                    with listing_table as (
                        select distinct account_id, seller_sku
                        from yibai_product_kd_sync.yibai_amazon_sku_map a
                        INNER JOIN yb_datacenter.v_oversea_stock b
                        ON a.sku = b.sku
                        WHERE account_id in {list_temp}
                        -- where deliver_mode=2
                    ) 
                    select b.account_id as account_id, b.account_name as account_name,
                        group_name, short_name, 
                        if(b.site ='sp', 'es', b.site) as site,
                        status, e.sku as sku, a.seller_sku as seller_sku,
                        if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
                        open_date, deliver_mode, 
                        fulfillment_channel,  a.price as online_price, 
                        formatDateTime(today(), '%Y-%m-%d') AS date_id
                    from (
                        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
                        from yibai_product_kd_sync.yibai_amazon_listings_all_raw2
                        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) a
                    inner join (
                        select account_id, sku, seller_sku, deliver_mode
                        from yibai_product_kd_sync.yibai_amazon_sku_map
                        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                        -- and deliver_mode=2
                    ) e
                    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
                    left join (
                        select account_id, seller_sku, asin1
                        from yibai_product_kd_sync.yibai_amazon_listing_alls
                        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) t
                    on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
                    inner join (
                        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
                        from yibai_system_kd_sync.yibai_amazon_account b
                        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
                        and account_id in (
                            select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
                            where account_type = 1 and is_yibai =1)
                    ) b
                    on (a.account_id= b.account_id)
                    inner join (
                        select group_id, group_name
                        from yibai_system_kd_sync.yibai_amazon_group
                        where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
                        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
                    ) c
                    on (b.group_id=c.group_id)
                    left join (
                        select account_id, seller_sku, ListingPrice as sale_price
                        from yibai_product_kd_sync.yibai_amazon_listing_price
                        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) f
                    on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
                    order by a.create_time desc
                    limit 1 by sku, site, asin
                    settings max_memory_usage = 500000000000
                """
                conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
                conn_ck.ck_execute_sql(sql)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')

    # 数量监控
    check_listing_num(table_name)
    # 删除3天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 3, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)


    return None

def failed_amazon_listing():
    """
    20241206 统计清仓sku链接数量时，需要包含国内仓链接
    """
    print("===刊登失败、验证失败的Amazon链接数据(含国内发货模式)==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_amazon_listing_failed'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    # 分账号取数
    sql = """
	SELECT 
		account_id, 
    	FROM_UNIXTIME(created_unix) as create_time_select
    FROM yibai_sale_center_amazon_sync.yibai_amazon_publish_sku_select_result
    WHERE status in (3,5) 
    and created_unix >= toUnixTimestamp(now() - INTERVAL 6 MONTH)
    """
    df_account = conn_ck.ck_select_to_df(sql)
    account_list = list(df_account['account_id'].unique())
    print(len(account_list))
    # account_list = account_list[0:100]
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取amazon海外仓链接...')
            for i in range(0, len(account_list), 200):
                list_temp = account_list[i:i + 200]
                sql = f"""
                    insert into yibai_oversea.{table_name}
                    with sku_temp as (
                        SELECT 
                            account_id, sku, publish_id, is_multi_attribute, status, site_code,
                            FROM_UNIXTIME(created_unix) as create_time_select
                        from yibai_sale_center_amazon_sync.yibai_amazon_publish_sku_select_result 
                        where status in (3,5)
                        and account_id in {list_temp}
                        and created_unix >= toUnixTimestamp(now() - INTERVAL 6 MONTH)
                    )
                    SELECT DISTINCT a.account_id account_id, a.parent_sku parent_sku, a.publish_id publish_id, a.is_multi_attribute is_multi_attribute, 
                    a.status status, a.site_code site_code, a.create_time_select create_time_select, 
                    task_table_sku sku, task_table_seller_sku, date_id
                    FROM (
                        select distinct
                            a.account_id account_id, a.sku parent_sku, a.publish_id publish_id, a.is_multi_attribute is_multi_attribute, 
                            a.status status, a.site_code site_code, a.create_time_select create_time_select, 
                            if(c.sku is null, b.sku, c.sku) as task_table_sku,
                            if(b.item_sku is null , c.item_sku, b.item_sku) as task_table_seller_sku,
                            formatDateTime(today(), '%Y-%m-%d') AS date_id
                        from sku_temp a
                        INNER JOIN (
                            SELECT publish_id, sku, item_sku 
                            FROM yibai_sale_center_amazon_sync.yibai_amazon_publish_task
                            WHERE account_id in {list_temp}
                        ) b ON a.publish_id = b.publish_id
                        LEFT JOIN (
                            SELECT publish_id, sku, item_sku 
                            FROM yibai_sale_center_amazon_sync.yibai_amazon_publish_task_variation
                            WHERE account_id in {list_temp}
                        ) c ON a.publish_id = c.publish_id
                        WHERE if(c.sku IS NULL, b.sku, c.sku) IS NOT NULL
                    ) a
                    INNER JOIN yb_datacenter.v_oversea_stock b ON a.task_table_sku = b.sku
                    settings max_memory_usage = 500000000000
                """
                conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
                # df_temp = conn_ck.ck_select_to_df(sql)
                # print(df_temp.info())
                conn_ck.ck_execute_sql(sql)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    #
    # # 数量监控
    # check_listing_num(table_name)
    # # 删除3天前的数据
    # sql = f"""
    # ALTER TABLE yibai_oversea.{table_name}
    # DELETE WHERE date_id < formatDateTime(today() - 3, '%Y-%m-%d')
    # """
    # conn_ck.ck_execute_sql(sql)


    return None

def tt_get_amazon_listing_all():
    """

    """
    print("===Amazon刊登链接数据(含国内发货模式)==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_amazon_listing_all'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)
    # 取sku
    sql = """
        SELECT distinct sku
        FROM yb_datacenter.v_oversea_stock
    """
    df_sku = conn_ck.ck_select_to_df(sql)
    sku_list = list(df_sku['sku'].unique())
    print(len(sku_list))

    # 分账号取数
    sql = f"""
        select distinct account_id, seller_sku, sku
        from tt_product_kd_sync.tt_amazon_sku_map
    """
    df_account = conn_ck_tt.ck_select_to_df(sql)
    df_account = df_account[df_account['sku'].isin(sku_list)]
    account_list = list(df_account['account_id'].unique())
    random.shuffle(account_list)
    #
    # account_list = account_list[0:100]
    print(len(account_list))

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取tt_amazon海外仓链接...')
            step = 150
            for i in range(0, len(account_list), step):
                # list_temp = account_list[i:i + 1000]
                list_temp = account_list[i:i + step]
                sql = f"""
                    with listing_table as (
                        select distinct account_id, seller_sku
                        from tt_product_kd_sync.tt_amazon_sku_map
                        WHERE account_id in {list_temp}
                        -- where deliver_mode=2
                    )
                    select b.account_id as account_id, b.account_name as account_name,
                        group_name, short_name,
                        if(b.site ='sp', 'es', b.site) as site,
                        status, e.sku as sku, a.seller_sku as seller_sku,
                        if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
                        open_date, deliver_mode,
                        fulfillment_channel,  a.price as online_price,
                        formatDateTime(today(), '%Y-%m-%d') AS date_id
                    from (
                        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
                        from tt_product_kd_sync.tt_amazon_listings_all_raw2
                        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) a
                    inner join (
                        select account_id, sku, seller_sku, deliver_mode
                        from tt_product_kd_sync.tt_amazon_sku_map
                        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                        -- and deliver_mode=2
                    ) e
                    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
                    left join (
                        select account_id, seller_sku, asin1
                        from tt_product_kd_sync.tt_amazon_listing_alls
                        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) t
                    on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
                    inner join (
                        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
                        from tt_system_kd_sync.tt_amazon_account b
                        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
                    ) b
                    on (a.account_id= b.account_id)
                    inner join (
                        select group_id, group_name
                        from tt_system_kd_sync.tt_amazon_group
                        where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
                        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
                    ) c
                    on (b.group_id=c.group_id)
                    left join (
                        select account_id, seller_sku, ListingPrice as sale_price
                        from tt_product_kd_sync.tt_amazon_listing_price
                        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    ) f
                    on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
                    order by a.create_time desc
                    -- limit 1 by sku, site, asin
                    settings max_memory_usage = 500000000000
                """
                df = conn_ck_tt.ck_select_to_df(sql)
                df['online_price'] = df['online_price'].astype(float)
                # df = df[df['sku'].isin(sku_list)]
                df = df[(df['sku'].isin(sku_list)) | (df['sku'].str.contains(r'\*|\+', na=False))]
                # df = pd.merge(df, df_sku[['sku']], how='inner', on=['sku'])
                # print(df.info())
                conn_ck_tt.ck_insert(df, table_name, if_exist='append')
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')

    # 数量监控
    # check_listing_num(table_name)
    # 删除3天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 3, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)


    return None

def tt_failed_amazon_listing():
    """
    20241206 统计清仓sku链接数量时，需要包含国内仓链接
    """
    print("===刊登失败、验证失败的TT-Amazon链接数据(含国内发货模式)==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_amazon_listing_failed'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    # 分账号取数
    sql = """
	SELECT 
		account_id, 
    	FROM_UNIXTIME(created_unix) as create_time_select
    FROM tt_sale_center_amazon_sync.tt_amazon_publish_sku_select_result
    WHERE status in (3,5) 
    and created_unix >= toUnixTimestamp(now() - INTERVAL 6 MONTH)
    """
    df_account = conn_ck_tt.ck_select_to_df(sql)
    account_list = list(df_account['account_id'].unique())
    print(len(account_list))
    # account_list = account_list[0:100]
    # 取sku
    sql = """
        SELECT distinct sku
        FROM yb_datacenter.v_oversea_stock
    """
    df_sku = conn_ck.ck_select_to_df(sql)
    sku_list = list(df_sku['sku'].unique())
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取amazon海外仓链接...')
            for i in range(0, len(account_list), 1000):
                list_temp = account_list[i:i + 1000]
                sql = f"""
                    with sku_temp as (
                        SELECT 
                            account_id, sku, publish_id, is_multi_attribute, status, site_code,
                            FROM_UNIXTIME(created_unix) as create_time_select
                        from tt_sale_center_amazon_sync.tt_amazon_publish_sku_select_result 
                        where status in (3,5)
                        and account_id in {list_temp}
                        and created_unix >= toUnixTimestamp(now() - INTERVAL 6 MONTH)
                    )
                    SELECT DISTINCT a.account_id account_id, a.parent_sku parent_sku, a.publish_id publish_id, a.is_multi_attribute is_multi_attribute, 
                    a.status status, a.site_code site_code, a.create_time_select create_time_select, 
                    task_table_sku sku, task_table_seller_sku, date_id
                    FROM (
                        select distinct
                            a.account_id account_id, a.sku parent_sku, a.publish_id publish_id, a.is_multi_attribute is_multi_attribute, 
                            a.status status, a.site_code site_code, a.create_time_select create_time_select, 
                            if(c.sku is null, b.sku, c.sku) as task_table_sku,
                            if(b.item_sku is null , c.item_sku, b.item_sku) as task_table_seller_sku,
                            formatDateTime(today(), '%Y-%m-%d') AS date_id
                        from sku_temp a
                        inner join (
                            SELECT * FROM tt_sale_center_amazon_sync.tt_amazon_publish_task
                            WHERE publish_id in (SELECT DISTINCT publish_id FROM sku_temp)
                        ) b on a.publish_id  = b.publish_id
                        left join (
                            SELECT * FROM tt_sale_center_amazon_sync.tt_amazon_publish_task_variation
                            WHERE publish_id in (SELECT DISTINCT publish_id FROM sku_temp)
                        ) c on a.publish_id  = c.publish_id
                    ) a
                """
                df = conn_ck_tt.ck_select_to_df(sql)
                df = pd.merge(df, df_sku, how='inner', on=['sku'])
                df["create_time_select"] = df["create_time_select"].dt.strftime("%Y-%d-%m")
                # print(df.info())
                conn_ck_tt.ck_insert(df, table_name, if_exist='append')
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    #
    # 数量监控
    # check_listing_num(table_name)
    # 删除3天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 3, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)


    return None

def get_ebay_listing():
    """
    """
    print("===EBAY刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_ebay_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    # # 分账号取数
    # sql = """
    # select toInt32(b.id) as account_id, account_name, group_id, short_name, site
    #     from yibai_system_kd_sync.yibai_amazon_account b
    #     where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
    #     and account_id in (
    #         select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
    #         where account_type = 1 and is_yibai =1)
    # """
    # df_account = conn_ck.ck_select_to_df(sql)
    # account_list = list(df_account['account_id'].unique())
    # print(len(account_list))
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取ebay海外仓链接...')
            sql = f"""
                insert into yibai_oversea.{table_name}
                with  account_list as (
                    select distinct id,account_id 
                    from yibai_sale_center_system_sync.yibai_system_account a
                    inner join (
                        select account_id
                        from yibai_account_manage_sync.yibai_ebay_account
                        where account_type = 1 and is_yibai =1
                    ) b
                    on a.account_id=b.account_id
                    where platform_code='EB' and status=1 and is_del=0
                )
                SELECT distinct 
                    a.item_id, a.sku, b.sell_sku, b.siteid as site, c.site_code country,
                    CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
                    b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
                    d.account_id, a.start_price,
                    f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price,
                    formatDateTime(today(), '%Y-%m-%d') AS date_id
                FROM (
                    SELECT item_id, sku, start_price
                    FROM (
                        SELECT item_id,sku,start_price, warehouse 
                        FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku a
                        LEFT JOIN yb_datacenter.v_oversea_stock b ON a.sku = b.sku 
                    ) a
                    WHERE warehouse is not null or (sku LIKE '%*%' OR sku LIKE '%+%') 
                ) a 
                INNER JOIN (
                    SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
                    from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
                    where  warehouse_category_id !=1 
                    -- and listing_status = 1  -- 20260304 ebay需计算下架链接的利润率
                    and account_id in (select distinct id from account_list)
                ) b  ON a.item_id=b.item_id
                LEFT JOIN (
                    SELECT distinct item_id, shipping_service_cost
                    FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
                    WHERE shipping_status=1 and shipping_service_priority=1
                ) f  ON a.item_id = f.item_id
                LEFT JOIN (
                    SELECT distinct site_id,site,site1 AS `站点`,is_open,site_code 
                    FROM domestic_warehouse_clear.yibai_site_table_ebay 
                    where is_open='是'
                ) c  ON b.siteid = c.site_id
                LEFT JOIN account_list d on b.account_id = d.id
                LEFT JOIN (
                    SELECT id, warehouse
                    FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
                    WHERE warehouse_type_id in (2, 3)
                ) e ON b.warehouse_category_id = e.id

            """
            conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
            conn_ck.ck_execute_sql(sql)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None

def tt_get_ebay_listing():
    """
    """
    print("===EBAY刊登链接数据==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_ebay_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)
    # 取sku
    sql = """
        SELECT distinct sku
        FROM yb_datacenter.v_oversea_stock
    """
    df_sku = conn_ck.ck_select_to_df(sql)
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取ebay海外仓链接...')
            sql = f"""
                with  account_list as (
                    select distinct id,account_id 
                    from tt_sale_center_system_sync.tt_system_account 
                    where platform_code='EB' and status=1 and is_del=0
                )
                SELECT 
                    a.item_id, a.sku, b.sell_sku, b.siteid as site, c.site_code country,
                    CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
                    b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
                    d.account_id, a.start_price,
                    f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price,
                    formatDateTime(today(), '%Y-%m-%d') AS date_id
                FROM (
                    SELECT item_id,sku,start_price 
                    FROM tt_sale_center_listing_sync.tt_ebay_online_listing_sales_sku 
                ) a 
                INNER JOIN (
                    SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
                    from tt_sale_center_listing_sync.tt_ebay_online_listing 
                    where  warehouse_category_id !=1 
                    -- and listing_status = 1 
                    and account_id in (select distinct id from account_list)
                ) b  ON a.item_id=b.item_id
                LEFT JOIN (
                    SELECT item_id, shipping_service_cost
                    FROM tt_sale_center_listing_sync.tt_ebay_item_shipping
                    WHERE shipping_status=1 and shipping_service_priority=1
                ) f  ON a.item_id = f.item_id
                LEFT JOIN (
                    SELECT site_id,site,site1 AS `站点`,is_open,site_code 
                    FROM domestic_warehouse_clear.tt_site_table_ebay 
                    where is_open='是'
                ) c  ON b.siteid = c.site_id
                LEFT JOIN account_list d on b.account_id = d.id
                INNER JOIN (
                    SELECT id, warehouse
                    FROM tt_sale_center_common_sync.tt_common_big_warehouse
                    WHERE warehouse_type_id in (2, 3)
                ) e ON b.warehouse_category_id = e.id

            """
            df = conn_ck_tt.ck_select_to_df(sql)
            df.columns = [i.split('.')[-1] for i in df.columns]
            col = ['online_price', 'start_price', 'shipping_fee']
            df[col] = df[col].astype(float)
            df['item_id'] = df['item_id'].astype(str)
            print(df.info())
            # df = pd.merge(df, df_sku, how='inner', on=['sku'])
            df = df[(df['sku'].isin(df_sku['sku'].unique())) | (df['sku'].str.contains(r'\*|\+', na=False))]
            df['country'] = df['country'].replace('MOTOR', 'US')
            df = df[~df['name'].str.contains('虚拟')]
            df = df.drop_duplicates(subset=['account_id', 'item_id', 'sku'])
            conn_ck_tt.ck_insert(df, table_name, if_exist='append')
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # 数量监控
    # check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    return None

def get_walmart_listing():
    """
    """
    print("===walmart刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_walmart_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取walmart海外仓链接...')
            sql = f"""
                insert into yibai_oversea.{table_name}
                SELECT 
                    a.erp_id as account_id,b.short_name as short_name,
                    case 
                        when b.site='us_dsv' then 'us' 
                        else b.site 
                    end as site, 
                    a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price,
                    formatDateTime(today(), '%Y-%m-%d') AS date_id
                FROM (
                    select * 
                    from yibai_sale_center_listing_sync.yibai_walmart_report a
                    INNER JOIN yb_datacenter.v_oversea_stock b ON a.sku = b.sku
                    where (upper(publish_status)='PUBLISHED') and price > 0
                    and (sku in (SELECT distinct sku FROM yb_datacenter.v_oversea_stock) 
                        OR a.sku LIKE '%*%' OR a.sku LIKE '%+%')
                    order by updated_unix desc limit 1 by erp_id,seller_sku
                ) a
                LEFT JOIN (
                    SELECT erp_id, short_name, lower(site_code) as site, status
                    FROM yibai_sale_center_system_sync.yibai_system_account
                    WHERE platform_code = 'WALMART' and account_id in (
                        select account_id
                        from yibai_account_manage_sync.yibai_walmart_account
                        where account_type = 1 and is_yibai =1
                    )
                ) b
                ON a.erp_id = b.erp_id
                WHERE b.status=1 and b.site<>'us_dsv'
            """
            conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
            conn_ck.ck_execute_sql(sql)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None

def tt_get_walmart_listing():
    """
    """
    print("===TT walmart刊登链接数据==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_walmart_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)
    # 取sku
    sql = """
        SELECT distinct sku
        FROM yb_datacenter.v_oversea_stock
    """
    df_sku = conn_ck.ck_select_to_df(sql)
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取walmart海外仓链接...')
            sql = f"""
                SELECT 
                    a.erp_id as account_id,b.short_name as short_name,
                    case 
                        when b.site='us_dsv' then 'us' 
                        else b.site 
                    end as site, 
                    a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price,
                    formatDateTime(today(), '%Y-%m-%d') AS date_id
                FROM (
                    select * from tt_sale_center_listing_sync.tt_walmart_report 
                    where upper(publish_status)='PUBLISHED' and price > 0
                    order by updated_unix desc limit 1 by erp_id,seller_sku
                ) a
                left join (
                SELECT erp_id, short_name, lower(site_code) as site, status
                FROM tt_sale_center_system_sync.tt_system_account
                WHERE platform_code = 'WALMART'
                ) b 
                on a.erp_id=b.erp_id
                WHERE b.status=1 and b.site<>'us_dsv'
            """
            df = conn_ck_tt.ck_select_to_df(sql)
            # df = pd.merge(df, df_sku, how='inner', on=['sku'])
            df = df[(df['sku'].isin(df_sku['sku'].unique())) | (df['sku'].str.contains(r'\*|\+', na=False))]
            df = df.drop_duplicates(subset=['account_id', 'item_id', 'sku'])
            df['price'] = df['price'].astype(float)
            print(df.info())
            conn_ck_tt.ck_insert(df, table_name, if_exist='append')
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    # check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    return None


def get_cd_listing():
    print("===cdiscount刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_cdiscount_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取cd海外仓链接...')
            sql = f"""
                insert into yibai_oversea.{table_name}
                select 
                    a.account_id as account_id, b.short_name as account_name,  'FR' AS site,
                    a.sku, a.product_id, a.seller_sku,
                    best_shipping_charges, best_shipping_charges + price as online_price, offer_state,
                    formatDateTime(today(), '%Y-%m-%d') AS date_id
                FROM yibai_sale_center_listing_sync.yibai_cdiscount_listing a 
                inner join (
                    SELECT x.id account_id, a.delivery_country delivery_country, x.short_name
                    FROM yibai_sale_center_system_sync.yibai_system_account x
                    INNER JOIN yibai_sale_center_system_sync.yibai_system_auth_account a
                    ON a.account_id = x.id
                    WHERE platform_code  = 'CDISCOUNT' and delivery_country !='CN'
                ) b 
                on a.account_id=b.account_id
                WHERE 
                    offer_state = 1 and used_status=1 
                    and product_id not in (
                        select distinct sku from yibai_sale_center_listing_sync.yibai_cdiscount_fbc_stock_detail 
                        where create_date >= toString(today()-10)
                    )
                    and (sku in (SELECT distinct sku FROM yb_datacenter.v_oversea_stock) 
                        OR a.sku LIKE '%*%' OR a.sku LIKE '%+%')
            """
            conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
            conn_ck.ck_execute_sql(sql)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None

def get_allegro_listing():

    print("===allegro刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_allegro_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取allegro海外仓链接...')
            sql = f"""
                insert into yibai_oversea.{table_name}
                with listing_temp as (
                    select 
                        a.account_id as account_id, b.erp_id, b.short_name as account_name, a.status status,
                        toInt64(a.offer_id) as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
                        a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price, selling_mode_currency,
                        0 as delivery_amount, a.location1 as location, b.group_name as group_name,
                        'PL' as country, formatDateTime(today(), '%Y-%m-%d') AS date_id
                    FROM (
                        SELECT 
                            *, replace(JSONExtractRaw(location, 'countryCode'), '"', '') as location1
                        FROM yibai_sale_center_listing_sync.yibai_allegro_listing
                        WHERE 
                            selling_mode_price > 0 and status in (1, 4)
                            and location1 not in ('CN', 'GB')
                    ) a 
                    INNER JOIN (
                        SELECT b.id, b.erp_id,  b.short_name, c.group_name
                        FROM yibai_sale_center_system_sync.yibai_system_account b
                        inner join (
                            select account_id,splitByString('->', oa_group_name)[-1] as group_name
                            from yibai_account_manage_sync.yibai_allegro_account
                            where account_type = 1 and is_platform_account != 2 and account_num_name != 'DMSAllegro'
                            and is_yibai =1
                        ) c
                        on b.account_id=c.account_id
                        WHERE platform_code  = 'ALLEGRO' 
                    ) b 
                    on a.account_id=b.id
                )
                
                SELECT account_id, erp_id, account_name, toInt64(offer_id) offer_id, seller_sku, status, sku, product_id,
                toFloat64(online_price) as online_price, selling_mode_currency,
                toFloat64(delivery_amount) as delivery_amount,location,group_name, country, date_id
                FROM listing_temp 
                
                union all
                 
                SELECT a.account_id as account_id, erp_id, account_name, toInt64(a.offer_id) as offer_id,a.seller_sku as seller_sku,
                status,a.sku as sku, a.product_id as product_id, b.amount as online_price, b.currency selling_mode_currency,
                round(toFloat64(a.delivery_amount) * d.`汇率`/c.`汇率`, 2) as delivery_amount, 
                a.location as location, group_name, b.country as country, formatDateTime(today(), '%Y-%m-%d') AS date_id
                FROM listing_temp a 
                INNER JOIN (
                    select 
                        toInt64(offer_id) offer_id,toFloat64(amount) AS amount, currency,
                        multiIf(marketplace_id=1, 'CZ', marketplace_id=2, 'SK', '') as country
                    from yibai_sale_center_listing_sync.yibai_allegro_listing_site_price
                    where amount > 0 and country != ''
                ) b on toInt64(a.offer_id)=toInt64(b.offer_id)
                INNER JOIN (
                    SELECT case when site = '匈牙利' then 'HU' else country end as country ,rate as `汇率`
                    FROM domestic_warehouse_clear.erp_rate
                    order by date_archive desc limit 1 by country
                ) c on b.country=c.country
                cross join (
                    SELECT country,rate as `汇率` FROM domestic_warehouse_clear.erp_rate
                    where site='波兰'
                    order by date_archive desc limit 1 by country
                ) d 
            """
            conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
            conn_ck.ck_execute_sql(sql)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None


def tt_get_allegro_listing():
    print("===TT allegro刊登链接数据==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_allegro_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)
    # 取sku
    sql = """
        SELECT distinct sku
        FROM yb_datacenter.v_oversea_stock
    """
    df_sku = conn_ck.ck_select_to_df(sql)
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取allegro海外仓链接...')
            sql = f"""
                with listing_temp as (
                    select 
                        a.account_id as account_id, b.erp_id, b.short_name as account_name, a.status status,
                        toInt64(a.offer_id) as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
                        a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price, selling_mode_currency,
                        0 as delivery_amount, a.location1 as location, '' as group_name,
                        'PL' as country, formatDateTime(today(), '%Y-%m-%d') AS date_id
                    FROM (
                        SELECT 
                            *, replace(JSONExtractRaw(location, 'countryCode'), '"', '') as location1
                        FROM tt_sale_center_listing_sync.tt_allegro_listing
                        WHERE 
                            selling_mode_price > 0 
                            -- and status in (1, 4)
                            and location1 not in ('CN', 'GB')
                    ) a 
                    INNER JOIN (
                        SELECT b.id, b.erp_id,  b.short_name
                        FROM tt_sale_center_system_sync.tt_system_account b
                        inner join (
                            select account_id,splitByString('->', oa_group_name)[-1] as group_name
                            from tt_account_manage_sync.tt_allegro_account
                            where account_type = 20 and is_platform_account != 2 
                        ) c
                        on b.account_id=c.account_id
                        WHERE platform_code  = 'ALLEGRO' 
                    ) b 
                    on a.account_id=b.id
                )

                SELECT account_id, erp_id, account_name, toInt64(offer_id) offer_id, seller_sku, status, sku, product_id,
                toFloat64(online_price) as online_price, selling_mode_currency,
                toFloat64(delivery_amount) as delivery_amount,location,group_name, country, date_id
                FROM listing_temp 

                union all

                SELECT a.account_id as account_id, erp_id, account_name, toInt64(a.offer_id) as offer_id,a.seller_sku as seller_sku,
                status,a.sku as sku, a.product_id as product_id, b.amount as online_price, b.currency selling_mode_currency,
                round(toFloat64(a.delivery_amount) * d.`汇率`/c.`汇率`, 2) as delivery_amount, 
                a.location as location, group_name, b.country as country, formatDateTime(today(), '%Y-%m-%d') AS date_id
                FROM listing_temp a 
                INNER JOIN (
                    select 
                        toInt64(offer_id) offer_id,toFloat64(amount) AS amount, currency,
                        multiIf(marketplace_id=1, 'CZ', marketplace_id=2, 'SK', '') as country
                    from tt_sale_center_listing_sync.tt_allegro_listing_site_price
                    where amount > 0 and country != ''
                ) b on toInt64(a.offer_id)=toInt64(b.offer_id)
                INNER JOIN (
                    SELECT case when site = '匈牙利' then 'HU' else country end as country ,rate as `汇率`
                    FROM domestic_warehouse_clear.erp_rate
                    order by date_archive desc limit 1 by country
                ) c on b.country=c.country
                cross join (
                    SELECT country,rate as `汇率` FROM domestic_warehouse_clear.erp_rate
                    where site='波兰'
                    order by date_archive desc limit 1 by country
                ) d 
            """
            df = conn_ck_tt.ck_select_to_df(sql)
            df.columns = [i.split('.')[-1] for i in df.columns]
            # df = pd.merge(df, df_sku, how='inner', on=['sku'])
            df = df[(df['sku'].isin(df_sku['sku'].unique())) | (df['sku'].str.contains(r'\*|\+', na=False))]
            df['delivery_amount'] = df['delivery_amount'].astype(float)
            df['offer_id'] = df['offer_id'].astype(str)
            print(df.info())
            # df.to_excel('F://Desktop//tt_allegro.xlsx', index=0)
            conn_ck_tt.ck_insert(df, table_name, if_exist='append')
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    # check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    return None


def ali_country_exploded(df):
    """ 欧洲国家不同目的国拆分多行 """

    # 定义安全解析函数：处理异常格式
    def safe_parse_json(s):
        if pd.isna(s):  # 处理NaN/None
            return []
        s = str(s).strip()  # 转换为字符串并去除首尾空格
        if not s:  # 处理空字符串
            return []
        # 处理单引号（非标准JSON，需替换为双引号）
        s = s.replace("'", '"')
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            # 解析失败时返回空列表（或记录错误日志）
            print(f"解析失败的内容: {s}")
            return []

    # 使用安全解析函数处理字段
    df["aeop_s_k_u_national_discount_price_list"] = df["aeop_s_k_u_national_discount_price_list"].apply(safe_parse_json)

    # 标记每个原始行是否只有GLO（无其他国家）
    df["has_only_glo"] = df["aeop_s_k_u_national_discount_price_list"].apply(
        lambda x: len(x) == 1 and x[0].get("country_code") == "GLO" if x else False)
    print(f"只有GLO国家的链接有{len(df[df['has_only_glo']==1])}条。")
    # df[df['has_only_glo']==1].head(4).to_excel('F://Desktop//df_hasonly_glo.xlsx', index=0)

    # 爆炸字段为多行
    exploded_df = df.explode("aeop_s_k_u_national_discount_price_list")

    # 提取字典键为新列
    exploded_df["country_code"] = exploded_df["aeop_s_k_u_national_discount_price_list"].apply(
        lambda x: x.get("country_code") if isinstance(x, dict) else None)

    # 只保留
    # exploded_df = exploded_df[
    #     (exploded_df["has_only_glo"] & (exploded_df["country_code"] == "GLO")) |
    #     (~exploded_df["has_only_glo"] & (exploded_df["country_code"] != "GLO"))
    #     ]

    exploded_df["money_value"] = exploded_df["aeop_s_k_u_national_discount_price_list"].apply(
        lambda x: x.get("money_value") if isinstance(x, dict) else None
    )
    exploded_df["current_effective_supply_price"] = exploded_df["aeop_s_k_u_national_discount_price_list"].apply(
        lambda x: x.get("current_effective_supply_price") if isinstance(x, dict) else None)

    # 删除原始字典字段
    exploded_df = exploded_df.drop(columns=["aeop_s_k_u_national_discount_price_list"])

    print(exploded_df.info())

    # 如果国家只有GLO, 则将GLO替换为warehouse_country
    c1 = (exploded_df['country_code']=='GLO') & (exploded_df['has_only_glo']==1)
    c2 = (exploded_df['country_code'].isna())
    exploded_df['country_code'] = np.where(c1|c2, exploded_df['warehouse_country'], exploded_df['country_code'])

    exploded_df.drop('has_only_glo', axis=1, inplace=True)

    return exploded_df


def get_ali_listing():
    print("===aliexpress刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_ali_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)
    # 筛选sku
    df_sku = get_sku()
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取aliexpress海外仓链接...')
            sql = """
                    with account_list as (
                        select a.id as account_id,erp_id, short_name, account_name,aliexpress_currency_code1
                        from yibai_sale_center_system_sync.yibai_system_account a
                        LEFT JOIN (
                            select account_id,if(aliexpress_currency_code='', 'USD', aliexpress_currency_code) AS aliexpress_currency_code1
                            from yibai_sale_center_common_sync.yibai_common_account_config
                            where platform_code IN ('ALI') and is_del = 0
                        ) b  on a.id=b.account_id
                        where platform_code = 'ALI' and is_del=0 and `status`=1
                    -- and id in (select account_id from yibai_sale_center_listing_sync.yibai_aliexpress_account_developer where token_status = 1
                    )

                    SELECT 
                        a.product_id, e.account_id, f.erp_id, f.short_name, f.account_name,a.sku sku,e.pop_choice_status,
                        e.product_status_type, e.gmt_create,
                        case when a.currency_code = 1 then 'USD' 
                        when a.currency_code = 2 then 'RUB' else 'CNY' end as currency_code,
                        sku_price,sku_code, aeop_s_k_u_property_list_str, b.property_value_id, 
                        b.sku_property_id, c.name_en, a.aeop_s_k_u_national_discount_price_list
                    FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing_skus a
                    LEFT JOIN (
                        SELECT distinct account_id,product_id, pop_choice_status, product_status_type, gmt_create
                        FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing
                        WHERE account_id in (select account_id from account_list)
                    ) e ON a.product_id = e.product_id
                    INNER JOIN account_list f on e.account_id = f.account_id
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
                    where a.sku !='' 
                """
            conn_ck = pd_to_ck(database='yibai_domestic', data_sys='调价明细历史数据')
            df = conn_ck.ck_select_to_df(sql)
            df.columns = [i.split('.')[-1] for i in df.columns]
            df = df[(df['sku'].isin(df_sku['sku'].unique())) | (df['sku'].str.contains(r'\*|\+', na=False))]
            dic = {'United States': 'US', 'CZECH REPUBLIC': 'DE', 'Czech Republic': 'DE', 'Poland': 'DE',
                   'france': 'FR', 'France': 'FR','JAPAN':'JP',
                   'Australia': 'AU', 'CN': 'CN', 'spain': 'ES', 'SPAIN': 'ES', 'Russian Federation': 'RU',
                   'UNITED KINGDOM': 'UK','japan':'JP', 'POLAND':'DE',
                   'United Kingdom': 'UK', 'GERMANY': 'DE', 'Mexico': 'MX', 'cz': 'DE', 'ITALY': 'IT', 'Italy': 'IT'}
            df['warehouse_country'] = df['name_en'].replace(dic)
            df = df.drop_duplicates(subset=['product_id', 'sku', 'warehouse_country'])
            df = df[df['warehouse_country'] != 'CN']
            print(f'爆炸前链接数量为{len(df)}条.')
            # 目的国拆分
            df = ali_country_exploded(df)
            df = df.drop_duplicates(subset=['product_id', 'sku', 'country_code'])

            df['sku_price'] = np.where(df['money_value'].isna(), df['sku_price'], df['money_value'])
            col = ['product_id', 'property_value_id', 'sku_property_id']
            df[col] = df[col].astype('int64').astype(str)
            df['sku_price'] = df['sku_price'].astype(float)
            df['current_effective_supply_price'] = df['current_effective_supply_price'].fillna(0).astype(float).round(2)
            df['gmt_create'] = df['gmt_create'].astype(str)
            df['date_id'] = time.strftime('%Y-%m-%d')

            df = df.rename(columns={'country_code':'country'})
            df.drop(['aeop_s_k_u_property_list_str','money_value','property_value_id','sku_property_id'], axis=1, inplace=True)
            print(df.info())
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck.ck_insert(df, table_name, if_exist='append')

            # df.to_excel('F://Desktop//df_ali_listing.xlsx', index=0)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    # check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None

def tt_get_ali_listing():
    print("===TT aliexpress刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_ali_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)
    # 筛选sku
    df_sku = get_sku()
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取aliexpress海外仓链接...')
            sql = """
                with account_list as (
                    select a.id as account_id,erp_id, short_name, account_name,
                    from tt_sale_center_system_sync.tt_system_account a
                    where platform_code = 'ALI' and is_del=0 and `status`=1
                )
                    
                SELECT 
                    a.product_id, e.account_id, f.erp_id, f.short_name, f.account_name,a.sku sku,e.pop_choice_status,
                    e.product_status_type, e.gmt_create,
                    case when a.currency_code = 1 then 'USD' 
                    when a.currency_code = 2 then 'RUB' else 'CNY' end as currency_code,
                    sku_price,sku_code, aeop_s_k_u_property_list_str, b.property_value_id, 
                    b.sku_property_id, c.name_en, a.aeop_s_k_u_national_discount_price_list
                FROM tt_sale_center_listing_sync.tt_aliexpress_listing_skus a
                INNER JOIN (
                    SELECT
                        aeop_s_k_u_property_list,aeop_s_k_u_property_list_str,
                        arrayJoin(JSONExtractArrayRaw(aeop_s_k_u_property_list_str)) as aeop_ae_product_skus1,
                        visitParamExtractFloat(aeop_ae_product_skus1, 'property_value_id') as property_value_id,
                        visitParamExtractFloat(aeop_ae_product_skus1, 'sku_property_id') as sku_property_id
                    FROM tt_domestic.tt_aliexpress_listing_skus_aeop_s_k_u_property_list
                    -- 国外发货地的链接
                    WHERE sku_property_id=200007763 and property_value_id!=201336100
                ) b ON a.aeop_s_k_u_property_list = b.aeop_s_k_u_property_list
                INNER JOIN (
                    SELECT
                        product_id, account_id, product_price, product_status_type, freight_template_id, gmt_create, pop_choice_status
                    FROM tt_sale_center_listing_sync.tt_aliexpress_listing
                    WHERE 
                        account_id in (
                            select id as account_id from tt_sale_center_system_sync.tt_system_account
                            where platform_code = 'ALI' and is_del=0 and `status`=1 )
                        -- and product_status_type = 1
                ) e ON a.product_id=e.product_id
                INNER JOIN account_list f on e.account_id = f.account_id
                LEFT join (
                    SELECT t1.*,t2.template_name_id as template_name_id,t2.account_id as account_id
                    FROM tt_sale_center_listing_sync.tt_aliexpress_fee_template_setting t1 
                    left join tt_sale_center_listing_sync.tt_aliexpress_price_type_setting t2 
                    on t1.price_mode_name = t2.price_mode_name
                    where t1.is_delete=0 and t2.is_delete=0
                ) t
                on toString(e.freight_template_id)=t.template_name_id and e.account_id=t.account_id
                -- 具体国外发货地
                LEFT JOIN (
                    SELECT DISTINCT parent_attr_id, attr_id, name_en
                    FROM tt_sale_center_listing_sync.tt_aliexpress_category_attribute
                ) c ON toInt64(b.sku_property_id) = toInt64(c.parent_attr_id) and toInt64(b.property_value_id) = toInt64(c.attr_id)
                where a.sku !='' and t.price_mode_name is not null
            """
            df = conn_ck_tt.ck_select_to_df(sql)
            df.columns = [i.split('.')[-1] for i in df.columns]
            dic = {'United States': 'US', 'CZECH REPUBLIC': 'DE', 'Czech Republic': 'DE', 'Poland': 'DE',
                   'france': 'FR', 'France': 'FR','JAPAN':'JP',
                   'Australia': 'AU', 'CN': 'CN', 'spain': 'ES', 'SPAIN': 'ES', 'Russian Federation': 'RU',
                   'UNITED KINGDOM': 'UK','japan':'JP', 'POLAND':'DE',
                   'United Kingdom': 'UK', 'GERMANY': 'DE', 'Mexico': 'MX', 'cz': 'DE', 'ITALY': 'IT', 'Italy': 'IT'}
            df['warehouse_country'] = df['name_en'].replace(dic)
            df = df.drop_duplicates(subset=['product_id', 'sku', 'warehouse_country'])
            df = df[df['warehouse_country'] != 'CN']
            # df = pd.merge(df, df_sku[['sku']], how='inner', on=['sku'])
            df = df[(df['sku'].isin(df_sku['sku'].unique())) | (df['sku'].str.contains(r'\*|\+', na=False))]
            print(f'爆炸前链接数量为{len(df)}条.')
            # 目的国拆分
            df = ali_country_exploded(df)
            df = df.drop_duplicates(subset=['product_id', 'sku', 'country_code'])

            df['sku_price'] = np.where(df['money_value'].isna(), df['sku_price'], df['money_value'])
            col = ['product_id', 'property_value_id', 'sku_property_id']
            df[col] = df[col].astype('int64').astype(str)
            df['sku_price'] = df['sku_price'].astype(float)
            df['current_effective_supply_price'] = df['current_effective_supply_price'].fillna(0).astype(float).round(2)
            df['gmt_create'] = df['gmt_create'].astype(str)
            df['date_id'] = time.strftime('%Y-%m-%d')

            df = df.rename(columns={'country_code':'country'})
            df.drop(['aeop_s_k_u_property_list_str','money_value','property_value_id','sku_property_id'], axis=1, inplace=True)

            print(df.info())
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck_tt.ck_insert(df, table_name, if_exist='append')

            # df.to_excel('F://Desktop//df_ali_listing.xlsx', index=0)
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    # check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    return None

def get_sku():
    """ 筛选海外仓sku """
    sql = """
        SELECT sku, warehouse
        FROM yb_datacenter.v_oversea_stock
        -- WHERE warehouse in ('泰国仓', '越南仓', '马来西亚仓', '印度尼西亚仓', '菲律宾仓')
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_sku = conn.read_sql(sql)

    # print(df_sku['warehouse'].value_counts())

    return df_sku

def get_shopee_listing():
    print("===shopee刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_shopee_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取shopee海外仓链接...')
            df_sku = get_sku()
            df_sku = df_sku[df_sku['warehouse'].isin(['泰国仓', '越南仓', '马来西亚仓', '印度尼西亚仓', '菲律宾仓'])]
            sku_list = tuple(df_sku['sku'].unique())

            # 取shopee海外仓账号
            sql = """

                 SELECT account_id, account_s_name, account_type
                 FROM yibai_account_manage_sync.yibai_shopee_account a
                 WHERE account_mold != 1 and account_status = 10 and account_type != 20
                 and is_yibai = 1
             """
            conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            df_shopee_account = conn_ck.ck_select_to_df(sql)
            # print(df_shopee_account.info())
            sql = """
                 select distinct account_id,a.account_name,b.shopee_is_3pf, a.id as id
                 from yibai_sale_center_system_sync.yibai_system_account as a
                 left join yibai_sale_center_common_sync.yibai_common_account_config as b
                 on b.account_id = a.id and  b.platform_code='SHOPEE' and b.is_del=0
                 where a.platform_code='SHOPEE' and a.is_del=0
             """
            conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            yibai_shopee_account = conn_ck.ck_select_to_df(sql)

            account_1 = yibai_shopee_account[yibai_shopee_account['shopee_is_3pf'] == 1]
            account_1 = tuple(account_1['id'].unique())

            df_shopee_account = pd.merge(df_shopee_account, yibai_shopee_account, how='inner', on='account_id')
            # print(df_shopee_account.info())
            account_2 = tuple(df_shopee_account['id'].unique())

            df_account = pd.concat([df_shopee_account[['id', 'account_id', 'account_name','shopee_is_3pf']],
                                    yibai_shopee_account[['id', 'account_id', 'account_name','shopee_is_3pf']]])

            account_list = tuple(set(account_1).union(set(account_2)))

            sql = f"""
                 SELECT item_id, account_id id, price, status_online, sku, country_code, reserve_price, is_mulit, parent_sku
                 FROM yibai_sale_center_listing_sync.yibai_shopee_listing
                 WHERE is_delete = 0 and account_id in {account_list}
                 -- and status_online not in ('SELLER_DELETE','SHOPEE_DELETE')
                 -- and status_online in ('NORMAL')
                 and sku in {sku_list}
             """
            conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            yibai_shopee_listing = conn_ck.ck_select_to_df(sql)

            yibai_shopee_listing = pd.merge(yibai_shopee_listing, df_account, how='left', on=['id'])
            yibai_shopee_listing['date_id'] = time.strftime('%Y-%m-%d')
            yibai_shopee_listing = yibai_shopee_listing.rename(
                columns={'price': 'online_price', 'country_code': 'country','account_name': 'short_name','id':'erp_id'})
            col = ['online_price', 'reserve_price']
            yibai_shopee_listing[col] = yibai_shopee_listing[col].astype(float)
            yibai_shopee_listing['item_id'] = yibai_shopee_listing['item_id'].astype(str)

            # print(yibai_shopee_listing.info())
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck.ck_insert(yibai_shopee_listing, table_name, if_exist='append')
            # yibai_shopee_listing.to_excel('F://Desktop//yibai_shopee_listing.xlsx', index=0)

            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')

    # 数量监控
    check_listing_num(table_name)

    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None

def tt_get_shopee_listing():
    print("===TT shopee刊登链接数据==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_shopee_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取shopee海外仓链接...')
            df_sku = get_sku()
            df_sku = df_sku[df_sku['warehouse'].isin(['泰国仓', '越南仓', '马来西亚仓', '印度尼西亚仓', '菲律宾仓'])]
            sku_list = tuple(df_sku['sku'].unique())

            # 取shopee海外仓账号
            sql = """

                 SELECT account_id, account_s_name, account_type
                 FROM yibai_account_manage_sync.yibai_shopee_account a
                 WHERE account_mold != 1 and account_status = 10 and account_type = 20
             """
            conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            df_shopee_account = conn_ck.ck_select_to_df(sql)
            # print(df_shopee_account.info())
            sql = """
                select distinct account_id,a.account_name,b.shopee_is_3pf, a.id as id
                from tt_sale_center_system_sync.tt_system_account as a
                left join tt_sale_center_common_sync.tt_common_account_config as b
                on b.account_id = a.id and  b.platform_code='SHOPEE' and b.is_del=0
                where a.platform_code='SHOPEE' and a.is_del=0
            """
            yibai_shopee_account = conn_ck_tt.ck_select_to_df(sql)

            account_1 = yibai_shopee_account[yibai_shopee_account['shopee_is_3pf'] == 1]
            account_1 = tuple(account_1['id'].unique())

            df_shopee_account = pd.merge(df_shopee_account, yibai_shopee_account, how='inner', on='account_id')
            # print(df_shopee_account.info())
            account_2 = tuple(df_shopee_account['id'].unique())

            df_account = pd.concat([df_shopee_account[['id', 'account_id', 'account_name','shopee_is_3pf']],
                                    yibai_shopee_account[['id', 'account_id', 'account_name','shopee_is_3pf']]])

            account_list = tuple(set(account_1).union(set(account_2)))

            sql = f"""
                SELECT item_id, account_id id, price, status_online, sku, country_code, reserve_price, is_mulit, parent_sku
                FROM tt_sale_center_listing_sync.tt_shopee_listing
                WHERE is_delete = 0 and account_id in {account_list}
                and status_online in ('NORMAL','UNLIST','MODEL_NORMAL')
                and sku in {sku_list}
             """
            yibai_shopee_listing = conn_ck_tt.ck_select_to_df(sql)

            yibai_shopee_listing = pd.merge(yibai_shopee_listing, df_account, how='left', on=['id'])
            yibai_shopee_listing['date_id'] = time.strftime('%Y-%m-%d')
            yibai_shopee_listing = yibai_shopee_listing.rename(
                columns={'price': 'online_price', 'country_code': 'country','account_name': 'short_name','id':'erp_id'})
            col = ['online_price', 'reserve_price']
            yibai_shopee_listing[col] = yibai_shopee_listing[col].astype(float)
            yibai_shopee_listing['item_id'] = yibai_shopee_listing['item_id'].astype(str)

            # print(yibai_shopee_listing.info())
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck_tt.ck_insert(yibai_shopee_listing, table_name, if_exist='append')
            # yibai_shopee_listing.to_excel('F://Desktop//yibai_shopee_listing.xlsx', index=0)

            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')

    # 数量监控
    # check_listing_num(table_name)

    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    return None

def tt_shopee_account():
    # 取shopee海外仓账号
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    # sql = """
    #
    #      SELECT account_id, account_s_name, account_type
    #      FROM yibai_account_manage_sync.yibai_shopee_account a
    #      WHERE account_mold != 1 and account_status = 10 and account_type = 20
    #  """
    # conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    # df_shopee_account = conn_ck.ck_select_to_df(sql)
    # # print(df_shopee_account.info())
    # sql = """
    #     select distinct account_id,a.account_name,b.shopee_is_3pf, a.id as id
    #     from tt_sale_center_system_sync.tt_system_account as a
    #     left join tt_sale_center_common_sync.tt_common_account_config as b
    #     on b.account_id = a.id and  b.platform_code='SHOPEE' and b.is_del=0
    #     where a.platform_code='SHOPEE' and a.is_del=0
    # """
    # yibai_shopee_account = conn_ck_tt.ck_select_to_df(sql)
    #
    # account_1 = yibai_shopee_account[yibai_shopee_account['shopee_is_3pf'] == 1]
    # account_1 = tuple(account_1['id'].unique())
    #
    # df_shopee_account = pd.merge(df_shopee_account, yibai_shopee_account, how='inner', on='account_id')
    # # print(df_shopee_account.info())
    # account_2 = tuple(df_shopee_account['id'].unique())
    # df_shopee_account['type'] = '非3pf'
    # yibai_shopee_account['type'] = '3pf'
    # df_account = pd.concat([df_shopee_account[['id', 'account_id', 'account_name', 'shopee_is_3pf','type']],
    #                         yibai_shopee_account[['id', 'account_id', 'account_name', 'shopee_is_3pf','type']]])


    # YB  取shopee海外仓账号
    # sql = """
    #
    #       SELECT account_id, account_s_name, account_type
    #       FROM yibai_account_manage_sync.yibai_shopee_account a
    #       WHERE account_mold != 1 and account_status = 10 and account_type != 20
    #       and is_yibai = 1
    #   """
    # conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    # df_shopee_account = conn_ck.ck_select_to_df(sql)
    # # print(df_shopee_account.info())
    # sql = """
    #       select distinct account_id,a.account_name,b.shopee_is_3pf, a.id as id
    #       from yibai_sale_center_system_sync.yibai_system_account as a
    #       left join yibai_sale_center_common_sync.yibai_common_account_config as b
    #       on b.account_id = a.id and  b.platform_code='SHOPEE' and b.is_del=0
    #       where a.platform_code='SHOPEE' and a.is_del=0
    #   """
    # conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    # yibai_shopee_account = conn_ck.ck_select_to_df(sql)
    #
    # account_1 = yibai_shopee_account[yibai_shopee_account['shopee_is_3pf'] == 1]
    # account_1 = tuple(account_1['id'].unique())
    #
    # df_shopee_account = pd.merge(df_shopee_account, yibai_shopee_account, how='inner', on='account_id')
    # # print(df_shopee_account.info())
    # account_2 = tuple(df_shopee_account['id'].unique())
    # df_shopee_account['type'] = '非3pf'
    # yibai_shopee_account['type'] = '3pf'
    # df_account = pd.concat([df_shopee_account[['id', 'account_id', 'account_name', 'shopee_is_3pf','type']],
    #                         yibai_shopee_account[['id', 'account_id', 'account_name', 'shopee_is_3pf','type']]])
    # df_account.to_excel('F://Desktop//yb_shopee_account.xlsx', index=0)

    # TT lazada 账号
    sql = """
    
          SELECT account_id, account_s_name, account_type
          FROM yibai_account_manage_sync.yibai_lazada_account a
          WHERE account_mold != 1  and account_status = 10 and account_type = 20
      """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_lazada_account = conn_ck.ck_select_to_df(sql)
    # print(df_lazada_account.info())
    sql = """
         select distinct account_id,a.short_name short_name, a.id as id
         from tt_sale_center_system_sync.tt_system_account as a
         left join tt_sale_center_common_sync.tt_common_account_config as b
         on b.account_id = a.id and  b.platform_code='LAZADA' and b.is_del=0
         where a.platform_code='LAZADA' and a.is_del=0
      """
    yibai_lazada_account = conn_ck_tt.ck_select_to_df(sql)

    df_lazada_account = pd.merge(df_lazada_account, yibai_lazada_account, how='inner', on='account_id')

    # 补充3pf账号
    # df_3pf = pd.read_excel('F://Ding_workspace//tt_lazada_海外仓账号-3PF补充20250729.xlsx')
    sql = """
         SELECT * FROM over_sea.tt_lazada_3pf_account
     """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_3pf = conn.read_sql(sql)
    df_3pf['is_3pf'] = 1
    df_lazada_account = pd.concat([df_lazada_account, df_3pf])

    df_lazada_account.to_excel('F://Desktop//df_lazada_account.xlsx', index=0)


def get_lazada_listing():
    print("===lazada刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_lazada_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取lazada海外仓链接...')
            df_sku = get_sku()
            df_sku = df_sku[df_sku['warehouse'].isin(['泰国仓', '越南仓', '马来西亚仓', '印度尼西亚仓', '菲律宾仓'])]
            sku_list = tuple(df_sku['sku'].unique())

            sql = """

                 SELECT account_id, account_s_name, account_type
                 FROM yibai_account_manage_sync.yibai_lazada_account a
                 WHERE account_mold = 1 and account_status = 10 and account_type != 20
                 and is_yibai = 1
             """
            conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            df_lazada_account = conn_ck.ck_select_to_df(sql)
            # print(df_lazada_account.info())
            sql = """
                 select distinct account_id,a.short_name short_name, a.id as id
                 from yibai_sale_center_system_sync.yibai_system_account as a
                 left join yibai_sale_center_common_sync.yibai_common_account_config as b
                 on b.account_id = a.id and  b.platform_code='LAZADA' and b.is_del=0
                 where a.platform_code='LAZADA' and a.is_del=0
             """
            conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            yibai_lazada_account = conn_ck.ck_select_to_df(sql)

            df_lazada_account = pd.merge(df_lazada_account, yibai_lazada_account, how='inner', on='account_id')
            account_list = tuple(df_lazada_account['id'].unique())
            df_lazada_account = df_lazada_account.rename(columns={'account_id':'system_account_id'})
            # print(df_lazada_account.head(4))

            sql = f"""
                 SELECT item_id, account_id, seller_sku, price, reserve_price, status_online, sku, is_mulit,
                 parent_sku, country_code
                 FROM yibai_sale_center_listing_sync.yibai_lazada_listing
                 WHERE is_delete = 0 and account_id in {account_list} and sku in {sku_list}
                 -- and status_online not in ('Deleted')
                 -- and status_online in ('Active')
             """
            conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            yibai_lazada_listing = conn_ck.ck_select_to_df(sql)
            # print(yibai_lazada_listing.info())

            df = pd.merge(yibai_lazada_listing, df_lazada_account[['id', 'short_name','system_account_id']],
                          how='left', left_on=['account_id'], right_on='id')
            df.drop('id', axis=1, inplace=True)
            df['date_id'] = time.strftime('%Y-%m-%d')
            df = df.rename(columns={'price': 'online_price', 'country_code': 'country'})
            col = ['online_price', 'reserve_price']
            df[col] = df[col].astype(float)
            df['item_id'] = df['item_id'].astype(str)
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck.ck_insert(df, table_name, if_exist='append')
            # yibai_shopee_listing.to_excel('F://Desktop//yibai_shopee_listing.xlsx', index=0)

            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)

    check_listing_num(table_name)

    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    # get_shopee_listing()

    return None

def tt_get_lazada_listing():
    print("===TT lazada刊登链接数据==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_lazada_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取lazada海外仓链接...')
            df_sku = get_sku()
            df_sku = df_sku[df_sku['warehouse'].isin(['泰国仓', '越南仓', '马来西亚仓', '印度尼西亚仓', '菲律宾仓'])]
            sku_list = tuple(df_sku['sku'].unique())

            # sql = """
            #
            #      SELECT account_id, account_s_name, account_type
            #      FROM yibai_account_manage_sync.yibai_lazada_account a
            #      WHERE account_mold != 1  and account_status = 10 and account_type = 20
            #  """
            # conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
            # df_lazada_account = conn_ck.ck_select_to_df(sql)
            # print(df_lazada_account.info())
            # sql = """
            #     select distinct account_id,a.short_name short_name, a.id as id
            #     from tt_sale_center_system_sync.tt_system_account as a
            #     left join tt_sale_center_common_sync.tt_common_account_config as b
            #     on b.account_id = a.id and  b.platform_code='LAZADA' and b.is_del=0
            #     where a.platform_code='LAZADA' and a.is_del=0
            #  """
            # yibai_lazada_account = conn_ck_tt.ck_select_to_df(sql)
            # print(yibai_lazada_account.info())
            # df_lazada_account = pd.merge(df_lazada_account, yibai_lazada_account, how='inner', on='account_id')

            # 补充3pf账号
            # df_3pf = pd.read_excel('F://Ding_workspace//tt_lazada_海外仓账号-3PF补充20250729.xlsx')
            sql = """
                SELECT account_s_name short_name, id, account_id
                FROM over_sea.tt_lazada_3pf_account
            """
            conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
            df_3pf = conn.read_sql(sql)
            # df_lazada_account = pd.concat([df_lazada_account, df_3pf])
            # 20260128 TTlazada目前只有3pf店铺，没有非3pf
            df_lazada_account = df_3pf
            account_list = tuple(df_lazada_account['id'].unique())
            df_lazada_account = df_lazada_account.rename(columns={'account_id':'system_account_id'})
            # print(df_lazada_account.head(4))

            sql = f"""
                SELECT item_id, account_id, seller_sku, price, reserve_price, status_online, sku, is_mulit,
                parent_sku, country_code
                FROM tt_sale_center_listing_sync.tt_lazada_listing
                WHERE is_delete = 0 
                and account_id in {account_list}
                and sku in {sku_list}
                 -- and status_online not in ('Deleted')
                 -- and status_online in ('Active')
             """
            yibai_lazada_listing = conn_ck_tt.ck_select_to_df(sql)
            # print(yibai_lazada_listing.info())

            df = pd.merge(yibai_lazada_listing, df_lazada_account[['id', 'short_name','system_account_id']],
                          how='left', left_on=['account_id'], right_on='id')
            df.drop('id', axis=1, inplace=True)
            df['date_id'] = time.strftime('%Y-%m-%d')
            df = df.rename(columns={'price': 'online_price', 'country_code': 'country'})
            col = ['online_price', 'reserve_price']
            df[col] = df[col].astype(float)
            df['item_id'] = df['item_id'].astype(str)
            # print(df.info())
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck_tt.ck_insert(df, table_name, if_exist='append')
            # yibai_shopee_listing.to_excel('F://Desktop//yibai_shopee_listing.xlsx', index=0)

            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)

    # check_listing_num(table_name)

    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    # get_shopee_listing()

    return None

def get_merc_listing():
    print("===美客多刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_merc_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取美客多海外仓链接...')
            df_sku = get_sku()
            df_sku = df_sku[df_sku['warehouse'].isin(['墨西哥仓'])]
            sku_list = tuple(df_sku['sku'].unique())
            # 链接信息
            sql = """
                SELECT account_id, item_id, sku, item_is_variation, price, base_price, 'mx' as site
                FROM yibai_sale_center_listing_sync.yibai_merc_listing 
                where  user_logistic_type = 'fulfillment' and  item_is_variation<>1
            """
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            df = conn_ck.ck_select_to_df(sql)
            df = df[df['sku'].isin(sku_list)]
            df['date_id'] = time.strftime('%Y-%m-%d')
            conn_ck.ck_insert(df, table_name, if_exist='append')
            # df.to_excel('F://Desktop//yibai_merc_listing.xlsx', index=0)

            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)

    # check_listing_num(table_name)

    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None

def tt_get_merc_listing():
    print("===美客多刊登链接数据==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_merc_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取美客多海外仓链接...')
            df_sku = get_sku()
            df_sku = df_sku[df_sku['warehouse'].isin(['墨西哥仓'])]
            sku_list = tuple(df_sku['sku'].unique())
            # 链接信息
            sql = """
                SELECT account_id, item_id, sku, item_is_variation, price, base_price, 'mx' as site
                FROM tt_sale_center_listing_sync.tt_merc_listing 
                where  user_logistic_type = 'fulfillment' and  item_is_variation<>1
            """
            df = conn_ck_tt.ck_select_to_df(sql)
            df = df[df['sku'].isin(sku_list)]
            df['date_id'] = time.strftime('%Y-%m-%d')
            conn_ck_tt.ck_insert(df, table_name, if_exist='replace')
            # df.to_excel('F://Desktop//yibai_merc_listing.xlsx', index=0)

            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)

    # check_listing_num(table_name)

    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    return None

def check_listing_num(table_name='yibai_ads_oversea_lazada_listing'):
    """ 监控链接数量异常情况 """

    # 取当日和上一次链接数量
    date_id = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT count(1) as t_num
        FROM yibai_oversea.{table_name}
        WHERE date_id = '{date_id}'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_t = conn_ck.ck_select_to_df(sql)
    current_count = df_t.iloc[0, 0]

    sql = f"""
        SELECT count(1) as y_num
        FROM yibai_oversea.{table_name}
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name} WHERE date_id < '{date_id}')
    """
    df_y = conn_ck.ck_select_to_df(sql)
    previous_count = df_y.iloc[0, 0]

    if previous_count == 0:
        raise ValueError("上一次数据量不能为0，无法计算差异")

    # 计算差异
    threshold = 0.05
    diff = current_count - previous_count
    diff_percent = diff / previous_count
    is_abnormal = abs(diff_percent) > threshold

    # 异常情况抛出异常
    if is_abnormal:
        raise Exception(
            f"数据量异常! 差异为{diff_percent:.2%}，超过{threshold:.0%}阈值。\n"
            f"前一日: {previous_count}, 当日: {current_count}"
        )

    print({
        '当日数量':current_count,
        '前一日数量': previous_count,
        '差异': f"{diff_percent:.2%}"
    })


def tt_temp():
    sql = f"""
         with listing_temp as (
             select 
                 a.account_id as account_id, b.erp_id, b.short_name as account_name, a.status status,
                 toString(a.offer_id) as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
                 a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price, selling_mode_currency,
                 0 as delivery_amount, a.location1 as location, '' as group_name,
                 'PL' as country, formatDateTime(today(), '%Y-%m-%d') AS date_id
             FROM (
                 SELECT 
                     *, replace(JSONExtractRaw(location, 'countryCode'), '"', '') as location1
                 FROM tt_sale_center_listing_sync.tt_allegro_listing
                 WHERE 
                     selling_mode_price > 0 
                     -- and status in (1, 4)
                     and location1 not in ('CN', 'GB')
             ) a 
             INNER JOIN (
                 SELECT b.id, b.erp_id,  b.short_name
                 FROM tt_sale_center_system_sync.tt_system_account b
                 inner join (
                     select account_id,splitByString('->', oa_group_name)[-1] as group_name
                     from tt_account_manage_sync.tt_allegro_account
                     where account_type = 20 and is_platform_account != 2 
                 ) c
                 on b.account_id=c.account_id
                 WHERE platform_code  = 'ALLEGRO' 
             ) b 
             on a.account_id=b.id
         )

         SELECT account_id, erp_id, account_name, toInt64(offer_id) offer_id, seller_sku, status, sku, product_id,
         toFloat64(online_price) as online_price, selling_mode_currency,
         toFloat64(delivery_amount) as delivery_amount,location,group_name, country, date_id
         FROM listing_temp 

         union all

         SELECT a.account_id as account_id, erp_id, account_name, toInt64(a.offer_id) as offer_id,a.seller_sku as seller_sku,
         status,a.sku as sku, a.product_id as product_id, b.amount as online_price, b.currency selling_mode_currency,
         round(toFloat64(a.delivery_amount) * d.`汇率`/c.`汇率`, 2) as delivery_amount, 
         a.location as location, group_name, b.country as country, formatDateTime(today(), '%Y-%m-%d') AS date_id
         FROM listing_temp a 
         INNER JOIN (
             select 
                 toInt64(offer_id) offer_id,toFloat64(amount) AS amount, currency,
                 multiIf(marketplace_id=1, 'CZ', marketplace_id=2, 'SK', '') as country
             from tt_sale_center_listing_sync.tt_allegro_listing_site_price
             where amount > 0 and country != ''
         ) b on toInt64(a.offer_id)=toInt64(b.offer_id)
         INNER JOIN (
             SELECT case when site = '匈牙利' then 'HU' else country end as country ,rate as `汇率`
             FROM domestic_warehouse_clear.erp_rate
             order by date_archive desc limit 1 by country
         ) c on b.country=c.country
         cross join (
             SELECT country,rate as `汇率` FROM domestic_warehouse_clear.erp_rate
             where site='波兰'
             order by date_archive desc limit 1 by country
         ) d 
     """
    conn_ck_tt = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    df = conn_ck_tt.ck_select_to_df(sql)
    df['offer_id'] = df['offer_id'].astype(str)
    print(df.info())
    print(df[['sku','offer_id']].head(2))
    # df_ali_listing.to_excel('F://Desktop//tt_df_ali_listing.xlsx', index=0)

def get_temu_listing():
    """
    """
    print("===TEMU刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    table_name = 'yibai_ads_oversea_temu_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取temu海外仓链接...')
            df = get_temu_listing_info()
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck.ck_insert(df, table_name, if_exist='append')
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    # check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

    return None

def get_temu_listing_info():
    """ """
    sql = """
    with d as
    (select product_spu_id,product_sku_id,max(id) as id from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log
    group by product_spu_id,product_sku_id),
    c as (select * from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log where id in (select id from d))

    select 
        e.account_id,a.site_code,p.site_code country, a.item_id,a.product_sku_id,a.product_skc_id,
        a.stock_number,c.online_status,a.sku, p.select_status lazada_account_operation_mode,
        c.added_to_site_time,p.supplier_price,date(a.create_time) as create_time, a.select_status
    from yibai_sale_center_listing_sync.yibai_temu_listing a
    left join yibai_sale_center_common_sync.yibai_common_account_config b on a.account_id=b.account_id
    left join c on a.item_id =c.product_spu_id and a.product_sku_id=c.product_sku_id
    left join yibai_sale_center_listing_sync.yibai_temu_listing_supplier_price_site p 
    on a.product_sku_id = p.product_sku_id and a.account_id = p.account_id
    left join yibai_sale_center_system_sync.yibai_system_account as e on a.account_id=e.id
    where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    and a.select_status = 12 -- 筛选已加入站点的链接
    and p.is_del = 0
    and p.select_status not in (1, 3)  -- 剔除价格申报状态为：待买家确认、已作废
    -- LIMIT 1000
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    # 20250212 更新链接状态取数表
    listing_t['online_status'] = np.where(listing_t['select_status']==-1,
                                          listing_t['online_status'], listing_t['select_status'])
    dic = {-1:'未同步', 0:'已弃用', 1:'待平台选品', 14:'待卖家修改', 15:'已修改', 16:'服饰可加色', 2:'待上传生产资料',
           3:'待寄样', 4:'寄样中',5:'待平台审版', 6:'审版不合格', 7:'平台核价中', 8:'待修改生产资料',
           9:'核价未通过', 10:'待下首单', 11:'已下首单', 12:'已加入站点', 13:'已下架', 17:'已终止'}
    listing_t['online_status'] = listing_t['online_status'].replace(dic)
    listing_t.loc[listing_t['online_status'] == '', 'online_status'] = '待申报'
    listing_t.loc[listing_t['online_status'].isnull(), 'online_status'] = '未知'
    listing_t.drop('select_status', axis=1, inplace=True)

    listing_t = listing_t.sort_values(by='added_to_site_time', ascending=False).\
        drop_duplicates(subset=['account_id','product_sku_id','sku','country'])
    # 有活动的链接，申报价格替换为活动价
    promotion_listing = get_promotion_listing()
    promotion_listing['activity_price'] = promotion_listing['activity_price'].astype(float)
    idx = promotion_listing.groupby(['account_id','product_skc_id'])['activity_price'].idxmin()
    # 使用这些索引来选择记录
    listing_prom = promotion_listing.loc[idx].rename(columns={'activity_price':'链接最低活动价'})
    listing_prom = listing_prom[['account_id','product_skc_id','activity_stock','链接最低活动价']]
    col = ['product_sku_id','product_skc_id','sku','item_id']
    listing_t[col] = listing_t[col].astype('str')

    listing_t = pd.merge(listing_t, listing_prom[[
        'account_id','product_skc_id','链接最低活动价']], how='left', on=['account_id','product_skc_id'])
    listing_t['链接最低活动价'] = listing_t['链接最低活动价'].fillna(0).astype(float)
    listing_t['origin_supplier_price'] = listing_t['supplier_price']
    c1 = (listing_t['链接最低活动价'] > 0) & (listing_t['链接最低活动价'] < listing_t['supplier_price'])
    listing_t['is_promotion'] = np.where(c1, 1, 0)
    listing_t['supplier_price'] = np.where(c1, listing_t['链接最低活动价'], listing_t['supplier_price'])
    listing_t.drop('链接最低活动价', axis=1, inplace=True)
    # 获取捆绑链接表的sku信息
    sql = """
    select erp_id as account_id,platform_sku as product_sku_id,company_sku as sku 
    from yibai_sale_center_listing_sync.yibai_temu_bind_sku
    order by update_time desc
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    yibai_temu_bind_sku = conn_ck.ck_select_to_df(sql)
    yibai_temu_bind_sku.drop_duplicates(subset=['account_id', 'product_sku_id'], inplace=True)

    listing_t = listing_t.merge(yibai_temu_bind_sku, on=['account_id', 'product_sku_id'], how='left',
                                      suffixes=['', '1'])
    listing_t.loc[listing_t['sku']=='','sku']=np.nan
    listing_t['sku'].fillna(listing_t['sku1'],inplace=True)
    listing_t.drop('sku1', axis=1, inplace=True)
    # listing_t.rename(columns={'added_to_site_time': '加入站点时间', 'supplier_price': '申报价格'}, inplace=True)

    listing_t['country'] = listing_t['country'].replace({'SP':'ES','GB':'UK'})
    dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
           'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲','SE': '欧洲', 'BE':'欧洲',
           'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大', 'MX': '墨西哥'}
    listing_t['站点'] = listing_t['site_code'].apply(lambda x: next((dic[code] for code in x.split(',') if code in dic), None))

    dic = {'欧洲': '德国仓', '澳大利亚': '澳洲仓', '美国': '美国仓', '英国': '英国仓',
           '新西兰': '澳洲仓', '加拿大': '加拿大仓', '墨西哥':'墨西哥仓'}
    listing_t['warehouse'] = listing_t['站点'].replace(dic)
    listing_t.drop('站点', axis=1, inplace=True)
    listing_t = listing_t.drop_duplicates(subset=['product_sku_id', 'account_id', 'sku', 'country'])

    # temu账号获取
    df_temu_account = get_temu_account()
    col = ['account_id', 'short_name', 'main_name','account_status','account_operation_mode']
    listing_t = pd.merge(listing_t, df_temu_account[col], how='left', on=['account_id'])

    # 有效链接筛选
    listing_t = listing_t[listing_t['online_status'].isin(['已加入站点'])]
    listing_t = listing_t[(listing_t['account_status']=='启用') & (listing_t['account_operation_mode']=='半托管')]
    listing_t['date_id'] = time.strftime('%Y-%m-%d')
    listing_t['added_to_site_time'] = listing_t['added_to_site_time'].astype(str)

    # 运费补贴计算
    listing_t = listing_t.rename(columns={'supplier_price':'supplier_price_final','origin_supplier_price':'supplier_price'})
    listing_t = get_freight_subsidy(listing_t)
    listing_t.drop(['rate','limit_price'], axis=1, inplace=True)
    listing_t = listing_t.rename(columns={'supplier_price':'origin_supplier_price',
                                          'supplier_price_final':'supplier_price',
                                          'freight_subsidy':'origin_freight_subsidy'})
    listing_t = get_freight_subsidy(listing_t)
    listing_t.drop(['rate', 'limit_price'], axis=1, inplace=True)
    listing_t = listing_t.drop_duplicates()

    print(listing_t.info())

    return listing_t


def get_promotion_listing():
    """
    获取temu活动链接信息
    """
    sql = f"""
        SELECT 
            c.account_id account_id, product_sku_id, a.product_skc_id, name, a.promotion_id, e.start_time, e.end_time, 
            activity_price, currency,activity_stock,e.status,e.session_status
        FROM (
            SELECT
                a.account_id, a.product_sku_id, a.product_skc_id product_skc_id, a.name, a.id as promotion_id, a.end_time,
                b.activity_price, b.currency, a.activity_stock
            FROM yibai_sale_center_listing_sync.yibai_temu_listing_promotion a
            LEFT JOIN yibai_sale_center_listing_sync.yibai_temu_listing_promotion_price b
            ON a.id = b.promotion_id
            WHERE 
                a.status != 2
                -- and a.start_time > '2024-09-26'  -- 筛选报名时间大于9月26日的 
                -- and a.end_time >= '{time.strftime('%Y-%m-%d')}' 
                and b.is_del = 0 and b.activity_price > 0
        ) a
        LEFT JOIN yibai_sale_center_system_sync.yibai_system_account as c 
        ON a.account_id=c.id
        -- 筛选活动状态：进行中、报名成功待开始
        INNER JOIN (
            SELECT id, promotion_id, session_status, status, start_time, end_time
            FROM yibai_sale_center_listing_sync.yibai_temu_listing_promotion_join_record
            WHERE start_time > '2024-09-26'  -- 筛选报名时间大于9月26日的
            -- and status not in (1, 5)  # 0未知 1报名失败 2进行中 3报名成功待开始 4已售罄 5活动已结束
            and status not in (1)   
            and is_del = 0
            -- and end_time >= '{time.strftime('%Y-%m-%d')}'
            and end_time >= today() - INTERVAL 7 DAY
        ) e ON toInt64(a.promotion_id) = toInt64(e.promotion_id)
        ORDER BY activity_price ASC
        LIMIT 1 BY account_id, product_sku_id, product_skc_id
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_prom = conn_ck.ck_select_to_df(sql)
    print(df_prom.info())
    df_prom.columns = [i.split('.')[-1] for i in df_prom.columns]
    df_prom[['product_sku_id', 'product_skc_id']] = df_prom[['product_sku_id', 'product_skc_id']].astype(str)
    df_prom = df_prom.drop_duplicates(subset=['account_id','product_skc_id'])

    return df_prom

def tt_get_promotion_listing():
    """
    获取temu活动链接信息
    """
    sql = f"""
        SELECT 
            c.account_id account_id, product_sku_id, a.product_skc_id, name, a.promotion_id, e.start_time, e.end_time, 
            activity_price, currency,activity_stock,e.status,e.session_status
        FROM (
            SELECT
                a.account_id, a.product_sku_id, a.product_skc_id product_skc_id, a.name, a.id as promotion_id, a.end_time,
                b.activity_price, b.currency, a.activity_stock
            FROM tt_sale_center_listing_sync.tt_temu_listing_promotion a
            LEFT JOIN tt_sale_center_listing_sync.tt_temu_listing_promotion_price b
            ON a.id = b.promotion_id
            WHERE 
                a.status != 2
                -- and a.start_time > '2024-09-26'  -- 筛选报名时间大于9月26日的 
                -- and a.end_time >= '{time.strftime('%Y-%m-%d')}' 
                and b.is_del = 0 and b.activity_price > 0
        ) a
        LEFT JOIN tt_sale_center_system_sync.tt_system_account as c 
        ON a.account_id=c.id
        -- 筛选活动状态：进行中、报名成功待开始
        INNER JOIN (
            SELECT id, promotion_id, session_status, status, start_time, end_time
            FROM tt_sale_center_listing_sync.tt_temu_listing_promotion_join_record
            WHERE start_time > '2024-09-26'  -- 筛选报名时间大于9月26日的
            -- and status not in (1, 5)  # 0未知 1报名失败 2进行中 3报名成功待开始 4已售罄 5活动已结束
            and status not in (1)   
            and is_del = 0
            -- and end_time >= '{time.strftime('%Y-%m-%d')}'
            and end_time >= today() - INTERVAL 7 DAY
        ) e ON toInt64(a.promotion_id) = toInt64(e.promotion_id)
        ORDER BY activity_price ASC
        LIMIT 1 BY account_id, product_sku_id, product_skc_id
    """
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    df_prom = conn_ck.ck_select_to_df(sql)
    print(df_prom.info())
    df_prom.columns = [i.split('.')[-1] for i in df_prom.columns]
    df_prom[['product_sku_id', 'product_skc_id']] = df_prom[['product_sku_id', 'product_skc_id']].astype(str)
    df_prom = df_prom.drop_duplicates(subset=['account_id','product_skc_id'])

    # df_prom.to_excel('F://Desktop//df_tt_prom.xlsx', index=0)
    return df_prom


def tt_get_temu_listing_info():
    sql = """
    with d as
    (select product_spu_id,product_sku_id,max(id) as id from tt_sale_center_listing_sync.tt_temu_listing_crawling_log
    group by product_spu_id,product_sku_id),
    c as (select * from tt_sale_center_listing_sync.tt_temu_listing_crawling_log where id in (select id from d))

    select 
        e.account_id, a.site_code,p.site_code country, a.item_id,a.product_sku_id,a.product_skc_id,
        a.stock_number,c.online_status, a.select_status, a.sku, p.select_status lazada_account_operation_mode,
        c.added_to_site_time,p.supplier_price,date(a.create_time) as create_time 
    from tt_sale_center_listing_sync.tt_temu_listing a
    left join tt_sale_center_common_sync.tt_common_account_config b on a.account_id=b.account_id
    left join c on a.item_id =c.product_spu_id and a.product_sku_id=c.product_sku_id
    left join tt_sale_center_listing_sync.tt_temu_listing_supplier_price_site p 
    on a.product_sku_id = p.product_sku_id and a.account_id = p.account_id
    left join tt_sale_center_system_sync.tt_system_account as e on a.account_id=e.id
    where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    and a.select_status = 12 -- 筛选已加入站点的链接
    """
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    # 20250212 更新链接状态取数表
    listing_t['online_status'] = np.where(listing_t['select_status']==-1,
                                          listing_t['online_status'], listing_t['select_status'])
    dic = {-1:'未同步', 0:'已弃用', 1:'待平台选品', 14:'待卖家修改', 15:'已修改', 16:'服饰可加色', 2:'待上传生产资料',
           3:'待寄样', 4:'寄样中',5:'待平台审版', 6:'审版不合格', 7:'平台核价中', 8:'待修改生产资料',
           9:'核价未通过', 10:'待下首单', 11:'已下首单', 12:'已加入站点', 13:'已下架', 17:'已终止'}
    listing_t['online_status'] = listing_t['online_status'].replace(dic)
    listing_t.loc[listing_t['online_status'] == '', 'online_status'] = '待申报'
    listing_t.loc[listing_t['online_status'].isnull(), 'online_status'] = '未知'
    listing_t.drop('select_status', axis=1, inplace=True)

    listing_t = listing_t.sort_values(by='added_to_site_time', ascending=False).\
        drop_duplicates(subset=['account_id','product_sku_id','sku','country'])
    col = ['product_sku_id','product_skc_id','sku','item_id']
    listing_t[col] = listing_t[col].astype('str')
    # 筛选sku
    df_sku = get_sku()
    listing_t = listing_t[(listing_t['sku'].isin(df_sku['sku'].unique())) | (
        listing_t['sku'].str.contains(r'\*|\+', na=False))]
    # listing_t = pd.merge(listing_t, df_sku[['sku']], how='inner', on=['sku'])

    # 20251009：TTtemu活动链接信息暂未补全
    # 有活动的链接，申报价格替换为活动价
    promotion_listing = tt_get_promotion_listing()
    promotion_listing['activity_price'] = promotion_listing['activity_price'].astype(float)
    idx = promotion_listing.groupby(['account_id','product_skc_id'])['activity_price'].idxmin()
    # 使用这些索引来选择记录
    listing_prom = promotion_listing.loc[idx].rename(columns={'activity_price':'链接最低活动价'})
    listing_prom = listing_prom[['account_id','product_skc_id','activity_stock','链接最低活动价']]
    col = ['product_sku_id','product_skc_id','sku','item_id']
    listing_t[col] = listing_t[col].astype('str')

    listing_t = pd.merge(listing_t, listing_prom[[
        'account_id','product_skc_id','链接最低活动价']], how='left', on=['account_id','product_skc_id'])
    listing_t['链接最低活动价'] = listing_t['链接最低活动价'].fillna(0).astype(float)
    listing_t['origin_supplier_price'] = listing_t['supplier_price']
    c1 = (listing_t['链接最低活动价'] > 0) & (listing_t['链接最低活动价'] < listing_t['supplier_price'])
    listing_t['is_promotion'] = np.where(c1, 1, 0)
    listing_t['supplier_price'] = np.where(c1, listing_t['链接最低活动价'], listing_t['supplier_price'])
    listing_t.drop('链接最低活动价', axis=1, inplace=True)
    # listing_t['is_promotion'] = 0
    # listing_t['origin_supplier_price'] = listing_t['supplier_price']
    # 获取捆绑链接表的sku信息
    sql = """
    select erp_id as account_id,platform_sku as product_sku_id,company_sku as sku 
    from tt_sale_center_listing_sync.tt_temu_bind_sku
    order by update_time desc
    """
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    tt_temu_bind_sku = conn_ck.ck_select_to_df(sql)
    tt_temu_bind_sku.drop_duplicates(subset=['account_id', 'product_sku_id'], inplace=True)

    listing_t = listing_t.merge(tt_temu_bind_sku, on=['account_id', 'product_sku_id'], how='left',
                                      suffixes=['', '1'])
    listing_t.loc[listing_t['sku']=='','sku']=np.nan
    listing_t['sku'].fillna(listing_t['sku1'],inplace=True)
    listing_t.drop('sku1', axis=1, inplace=True)
    # listing_t.rename(columns={'added_to_site_time': '加入站点时间', 'supplier_price': '申报价格'}, inplace=True)

    listing_t['country'] = listing_t['country'].replace({'SP':'ES','GB':'UK'})
    dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
           'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲','SE': '欧洲', 'BE':'欧洲',
           'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大', 'MX': '墨西哥'}
    listing_t['站点'] = listing_t['site_code'].apply(lambda x: next((dic[code] for code in x.split(',') if code in dic), None))

    dic = {'欧洲': '德国仓', '澳大利亚': '澳洲仓', '美国': '美国仓', '英国': '英国仓',
           '新西兰': '澳洲仓', '加拿大': '加拿大仓', '墨西哥':'墨西哥仓'}
    listing_t['warehouse'] = listing_t['站点'].replace(dic)
    listing_t.drop('站点', axis=1, inplace=True)
    listing_t = listing_t.drop_duplicates(subset=['product_sku_id', 'account_id', 'sku', 'country'])

    # temu账号获取
    df_temu_account = get_temu_account(org='TT')
    col = ['account_id', 'short_name', 'main_name','account_status','account_operation_mode']
    listing_t = pd.merge(listing_t, df_temu_account[col], how='left', on=['account_id'])

    # 有效链接筛选
    listing_t = listing_t[listing_t['online_status'].isin(['已加入站点'])]
    # listing_t = listing_t[(listing_t['account_status']=='启用') & (listing_t['account_operation_mode']=='半托管')]
    listing_t['date_id'] = time.strftime('%Y-%m-%d')
    listing_t['added_to_site_time'] = listing_t['added_to_site_time'].astype(str)

    # 运费补贴计算
    listing_t = listing_t.rename(columns={'supplier_price':'supplier_price_final','origin_supplier_price':'supplier_price'})
    listing_t = get_freight_subsidy(listing_t)
    listing_t.drop(['rate','limit_price'], axis=1, inplace=True)
    listing_t = listing_t.rename(columns={'supplier_price':'origin_supplier_price',
                                          'supplier_price_final':'supplier_price',
                                          'freight_subsidy':'origin_freight_subsidy'})
    listing_t = get_freight_subsidy(listing_t)
    listing_t.drop(['rate', 'limit_price'], axis=1, inplace=True)
    listing_t = listing_t.drop_duplicates()
    print(listing_t.info())

    return listing_t

def tt_get_temu_listing():
    """
    """
    print("===TT TEMU刊登链接数据==")
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    table_name = 'tt_ads_oversea_temu_listing'

    # 删除当天数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM tt_oversea.{table_name}
            WHERE date_id = formatDateTime(today(), '%Y-%m-%d')
        """
        df_temp = conn_ck_tt.ck_select_to_df(sql)
        if len(df_temp) == 0:
            t1 = time.time()
            print('开始获取temu海外仓链接...')
            df = tt_get_temu_listing_info()
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn_ck_tt.ck_insert(df, table_name, if_exist='append')
            t2 = time.time()
            print(f'获取完成，耗时{t2 - t1 :.2f}s')
            break
        else:
            n += 1
            time.sleep(5)
    if n == 5:
        raise Exception('当日数据未清除！')
    # df_listing = conn_ck.ck_select_to_df(sql)
    # 数量监控
    # check_listing_num(table_name)
    # 删除7天前的数据
    sql = f"""
    ALTER TABLE tt_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 7, '%Y-%m-%d')
    """
    conn_ck_tt.ck_execute_sql(sql)

    return None

def site_table():
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = """
    SELECT distinct site, site1, area as area0
    FROM domestic_warehouse_clear.site_table
    """
    # df_site = pd.DataFrame(con.execute(sql), columns=['site', 'site1', 'area'])
    df_site = conn_ck.ck_select_to_df(sql)
    df_site.loc[len(df_site)] = ['es', '西班牙', '泛欧']
    dic = {'my': '马来西亚', 'th': '泰国', 'ph': '菲律宾', 'id': '印度尼西亚', 'vn': '越南'}
    new_data = []
    for site, area0 in dic.items():
        new_data.append({
            'site': site,
            'site1': area0,  # 取对应中文名称
            'area0': area0  # 直接用字典的值
        })
    new_df = pd.DataFrame(new_data)
    df_site = pd.concat([df_site, new_df], ignore_index=True)
    df_site['area'] = np.where(df_site['area0'].str.contains('泛欧'), '欧洲', df_site['site1'])
    df_site['area'] = df_site['area'].replace('澳大利亚', '澳洲')
    # print(df_site['area'].unique())
    # print(df_site)
    # df_site.to_excel('F://Desktop//df_site.xlsx', index=0)
    return df_site


def warehouse_mark(df):
    """ 处理仓标 """
    df['sku'] = df['sku'].str.strip()
    df['new_sku'] = np.where(
        df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                 df['sku'].str[:-2], df['new_sku'])

    return df

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


def yb_listing_num_main():
    """ 获取各平台链接数据 """

    # 1、sku库存信息。（取云仓当前的sku信息）
    df = get_sku_info()

    # 2、各平台链接信息
    df_site = site_table()
    col = ['amazon', 'amazon_failed', 'ebay', 'cdiscount', 'walmart', 'allegro', 'ali', 'shopee', 'lazada','merc']
    # col = ['amazon', 'amazon_failed']
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    for i in col:
        if i in ['amazon']:
            # 先统计非欧洲站点链接数量
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM (
                    SELECT distinct sku, site, account_id, asin
                    FROM yibai_oversea.yibai_ads_oversea_{i}_listing_all
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing_all)
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
            df_num = pd.merge(df_num, df_site[['site', 'area']], how='left', on=['site'])
            df_num_eu = df_num[df_num['area'] == '欧洲']
            df_num = df_num[df_num['area']!='欧洲']
            df_num.drop('area', axis=1, inplace=True)
        if i in ['amazon_failed']:
            # 先统计非欧洲站点链接数量
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM (
                    SELECT distinct sku, site_code site, account_id, task_table_seller_sku
                    FROM yibai_oversea.yibai_ads_oversea_amazon_listing_failed
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_amazon_listing_failed)
                    and (account_id, task_table_seller_sku) not in (
                        SELECT distinct account_id, seller_sku
                        FROM yibai_oversea.yibai_ads_oversea_amazon_listing_all
                        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_amazon_listing_all)
                    )
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
            df_num = pd.merge(df_num, df_site[['site', 'area']], how='left', on=['site'])
            df_num_eu_failed = df_num[df_num['area'] == '欧洲']
            df_num = df_num[df_num['area']!='欧洲']
            df_num.drop('area', axis=1, inplace=True)

        elif i == 'ebay':
            sql = f"""
                SELECT sku, lower(country) site, count(1) as {i}_listing_num
                FROM yibai_oversea.yibai_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
            df_num['site'] = df_num['site'].replace('motor', 'us')

        elif i == 'cdiscount':
            sql = f"""
                SELECT sku, site, count(1) as {i}_listing_num
                FROM yibai_oversea.yibai_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
            df_num_de = df_num.copy()
            df_num_de['site'] = 'de'
            df_num = pd.concat([df_num, df_num_de])

        elif i in ['walmart']:
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM yibai_oversea.yibai_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        elif i == 'allegro':
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM ( 
                    SELECT distinct account_id, offer_id, sku, 
                    case when location='CZ' then 'DE' else location end as site
                    FROM yibai_oversea.yibai_ads_oversea_{i}_listing
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing)
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        elif i == 'ali':
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM ( 
                    SELECT distinct account_id, product_id, sku, warehouse_country site
                    FROM yibai_oversea.yibai_ads_oversea_{i}_listing
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing)
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        elif i in ['shopee','lazada']:
            sql = f"""
                SELECT sku, lower(country) site, count(1) as {i}_listing_num
                FROM yibai_oversea.yibai_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
        elif i in ['merc']:
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM yibai_oversea.yibai_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        # 仓库区域
        df_num = pd.merge(df_num, df_site[['site', 'area']], on='site', how='left')

        # 处理仓标数据
        df_num = warehouse_mark(df_num)
        df_num = df_num.drop_duplicates()
        df_num = df_num.groupby(['new_sku', 'area'])[f'{i}_listing_num'].sum().reset_index()

        df = pd.merge(df, df_num, how='left', on=['new_sku','area'])
        df[f'{i}_listing_num'] = df[f'{i}_listing_num'].fillna(0).astype(int)
        print(f"{i}平台链接数量共{df[f'{i}_listing_num'].sum()}条。")

    # amazon 欧洲站点
    df_num_eu = df_num_eu[df_num_eu['site'].isin(['de','fr','it','es'])]
    df_num_eu = warehouse_mark(df_num_eu)
    df_num_eu = df_num_eu.drop_duplicates()
    amazon_temp = df_num_eu.pivot_table(index='new_sku', columns='site', aggfunc='sum').reset_index()
    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_': 'new_sku'}, inplace=True)
    amazon_temp['area'] = '欧洲'
    # amazon_temp.to_excel('F://Desktop//amazon_temp.xlsx', index=0)
    df = pd.merge(df, amazon_temp, how='left',on=['new_sku','area'])

    # 刊登失败、验证失败的 amazon 欧洲站点
    df_num_eu_failed = df_num_eu_failed[df_num_eu_failed['site'].isin(['de','fr','it','es'])]
    df_num_eu_failed = warehouse_mark(df_num_eu_failed)
    df_num_eu_failed = df_num_eu_failed.drop_duplicates()
    amazon_temp = df_num_eu_failed.pivot_table(index='new_sku', columns='site', aggfunc='sum').reset_index()
    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_failed_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_failed_': 'new_sku'}, inplace=True)
    amazon_temp['area'] = '欧洲'
    print(amazon_temp.info())
    # amazon_temp.to_excel('F://Desktop//amazon_temp.xlsx', index=0)
    df = pd.merge(df, amazon_temp, how='left',on=['new_sku','area'])
    col = ["amazon_failed_listing_num","amazon_failed_de", "amazon_failed_fr", "amazon_failed_es", "amazon_failed_it"]
    col_2 = ["amazon_listing_num","amazon_de", "amazon_fr", "amazon_es", "amazon_it"]
    df[col] = df[col].fillna(0).astype(int)
    df[col_2] = df[col_2].fillna(0).astype(int)
    for c1, c2 in zip(col, col_2):
        df[c1] = df[c1] + df[c2]

    # temu
    sql = """
        SELECT sku, warehouse, count(1) as temu_listing_num
        FROM (
            SELECT distinct account_id, product_sku_id, sku, warehouse
            FROM yibai_oversea.yibai_ads_oversea_temu_listing
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_temu_listing )
        ) a
        GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_num = conn_ck.ck_select_to_df(sql)
    df_num = warehouse_mark(df_num)
    df_num = df_num.groupby(['new_sku', 'warehouse'])[f'temu_listing_num'].sum().reset_index()
    df = pd.merge(df, df_num, how='left', on=['new_sku', 'warehouse'])
    col = ["temu_listing_num"]
    df[col] = df[col].fillna(0).astype(int)
    print(f"temu平台链接数量共{df_num['temu_listing_num'].sum()}条。")

    # 3、补充信息
    df = get_ban_info(df)

    # 库存转化
    df['库存_分段'] = np.where(df['total_available_stock'] > 5, '5+', df['total_available_stock'])
    df['库存金额'] = df['total_available_stock'] * df['new_price']

    need_columns = ["sku", "spu", "warehouse", "max_stock_warehouse_id", "max_stock_warehouse_name", "date_id",
                    "total_available_stock", "库存_分段", "库存金额", "new_price", "warehouse_stock_info", "develop_source_name",
                    "overage_level", "day_sales", "销售状态", "area","site", "new_sku", "责任人",
                    "amazon_listing_num", "amazon_de", "amazon_fr", "amazon_es", "amazon_it",
                    "amazon_failed_listing_num","amazon_failed_de", "amazon_failed_fr", "amazon_failed_es", "amazon_failed_it",
                    "ebay_listing_num",  "cdiscount_listing_num", "walmart_listing_num", "temu_listing_num",
                    "allegro_listing_num", "ali_listing_num", "shopee_listing_num", "lazada_listing_num",
                    "info_country", "侵权信息", "侵权等级", "forbid_plateform", "禁售信息", "禁售等级"]
    df = df[need_columns]
    change_columns = ["sku", "spu", "warehouse", "最大库存子仓ID", "最大库存子仓", "date_id", "当前库存", "库存_分段", "库存金额", "成本",
                      "子仓库存明细", "开发来源", "超库龄等级", "日销", "销售状态","仓库区域", "仓库所在国家", "new_sku（主SKU）",
                      "责任人", "amazon_listing_num", "amazon_DE", "amazon_FR", "amazon_ES", "amazon_IT",
                      "amazon_failed_listing_num", "amazon_failed_de", "amazon_failed_fr", "amazon_failed_es",
                      "amazon_failed_it",
                      "ebay_listing_num",  "cdiscount_listing_num", "walmart_listing_num", "temu_listing_num",
                      "allegro_listing_num", "ali_listing_num", "shopee_listing_num", "lazada_listing_num",
                      "info_country", "侵权信息", "侵权等级", "forbid_plateform", "禁售信息", "禁售等级"]
    df.columns = change_columns

    # 4、存表
    df.to_excel('F://Desktop//yibai_num_new.xlsx', index=0)

    date_id = time.strftime('%Y-%m-%d')
    sql = f"ALTER TABLE 'oversea_platform_listing_num' DELETE WHERE date_id = '{date_id}'"
    conn_ck.ck_execute_sql(sql)
    conn_ck.ck_insert(df, 'oversea_platform_listing_num', if_exist='appdend')


    return df

def temp_amazon():
    # temu
    sql = """
        SELECT sku, warehouse, count(1) as temu_listing_num
        FROM (
            SELECT distinct account_id, product_sku_id, sku, warehouse
            FROM yibai_oversea.tt_ads_oversea_temu_listing
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_temu_listing )
        ) a
        GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_num = conn_ck.ck_select_to_df(sql)
    df_num = warehouse_mark(df_num)
    df_num = df_num.groupby(['new_sku', 'warehouse'])[f'temu_listing_num'].sum().reset_index()

    df_num.to_excel('F://Desktop//df_num_temu.xlsx', index=0)

def tt_listing_num_main():
    """ 获取TT各平台链接数据 """

    # 1、sku库存信息。（取云仓当前的sku信息）
    df = get_sku_info()

    # 2、各平台链接信息
    df_site = site_table()
    col = ['amazon', 'amazon_failed', 'ebay', 'walmart', 'allegro', 'ali', 'shopee', 'lazada']
    # col = ['amazon']
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    for i in col:
        if i in ['amazon']:
            # 先统计非欧洲站点链接数量
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM (
                    SELECT distinct sku, site, account_id, asin
                    FROM yibai_oversea.tt_ads_oversea_{i}_listing_all
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_{i}_listing_all)
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
            df_num = pd.merge(df_num, df_site[['site', 'area']], how='left', on=['site'])
            df_num_eu = df_num[df_num['area'] == '欧洲']
            df_num = df_num[df_num['area']!='欧洲']
            df_num.drop('area', axis=1, inplace=True)
        if i in ['amazon_failed']:
            # 先统计非欧洲站点链接数量
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM (
                    SELECT distinct sku, site_code site, account_id, task_table_seller_sku
                    FROM yibai_oversea.tt_ads_oversea_amazon_listing_failed
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_amazon_listing_failed)
                    and (account_id, task_table_seller_sku) not in (
                        SELECT distinct account_id, seller_sku
                        FROM yibai_oversea.tt_ads_oversea_amazon_listing_all
                        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_amazon_listing_all)
                    )
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
            df_num = pd.merge(df_num, df_site[['site', 'area']], how='left', on=['site'])
            df_num_eu_failed = df_num[df_num['area'] == '欧洲']
            df_num = df_num[df_num['area']!='欧洲']
            df_num.drop('area', axis=1, inplace=True)

        elif i == 'ebay':
            sql = f"""
                SELECT sku, lower(country) site, count(1) as {i}_listing_num
                FROM yibai_oversea.tt_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)
            df_num['site'] = df_num['site'].replace('motor', 'us')

        elif i in ['walmart']:
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM yibai_oversea.tt_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        elif i == 'allegro':
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM ( 
                    SELECT distinct account_id, offer_id, sku, 
                    case when location='CZ' then 'DE' else location end as site
                    FROM yibai_oversea.tt_ads_oversea_{i}_listing
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_{i}_listing)
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        elif i == 'ali':
            sql = f"""
                SELECT sku, lower(site) site, count(1) as {i}_listing_num
                FROM ( 
                    SELECT distinct account_id, product_id, sku, warehouse_country site
                    FROM yibai_oversea.tt_ads_oversea_{i}_listing
                    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_{i}_listing)
                ) a
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        elif i in ['shopee','lazada']:
            sql = f"""
                SELECT sku, lower(country) site, count(1) as {i}_listing_num
                FROM yibai_oversea.tt_ads_oversea_{i}_listing
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_{i}_listing)
                GROUP BY sku, site
            """
            df_num = conn_ck.ck_select_to_df(sql)

        # 仓库区域
        df_num = pd.merge(df_num, df_site[['site', 'area']], on='site', how='left')

        # 处理仓标数据
        df_num = warehouse_mark(df_num)
        df_num = df_num.drop_duplicates()
        df_num = df_num.groupby(['new_sku', 'area'])[f'{i}_listing_num'].sum().reset_index()

        df = pd.merge(df, df_num, how='left', on=['new_sku','area'])
        df[f'{i}_listing_num'] = df[f'{i}_listing_num'].fillna(0).astype(int)
        print(f"{i}平台链接数量共{df[f'{i}_listing_num'].sum()}条。")

    # amazon 欧洲站点
    df_num_eu = df_num_eu[df_num_eu['site'].isin(['de','fr','it','es'])]
    df_num_eu = warehouse_mark(df_num_eu)
    df_num_eu = df_num_eu.drop_duplicates()
    amazon_temp = df_num_eu.pivot_table(index='new_sku', columns='site', aggfunc='sum').reset_index()
    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_': 'new_sku'}, inplace=True)
    amazon_temp['area'] = '欧洲'
    # amazon_temp.to_excel('F://Desktop//amazon_temp.xlsx', index=0)
    df = pd.merge(df, amazon_temp, how='left',on=['new_sku','area'])
    # 刊登失败、验证失败的 amazon 欧洲站点
    df_num_eu_failed = df_num_eu_failed[df_num_eu_failed['site'].isin(['de','fr','it','es'])]
    df_num_eu_failed = warehouse_mark(df_num_eu_failed)
    df_num_eu_failed = df_num_eu_failed.drop_duplicates()
    amazon_temp = df_num_eu_failed.pivot_table(index='new_sku', columns='site', aggfunc='sum').reset_index()
    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_failed_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_failed_': 'new_sku'}, inplace=True)
    amazon_temp['area'] = '欧洲'
    print(amazon_temp.info())
    # amazon_temp.to_excel('F://Desktop//amazon_temp.xlsx', index=0)
    df = pd.merge(df, amazon_temp, how='left',on=['new_sku','area'])
    col = ["amazon_failed_listing_num","amazon_failed_de", "amazon_failed_fr", "amazon_failed_es", "amazon_failed_it"]
    col_2 = ["amazon_listing_num","amazon_de", "amazon_fr", "amazon_es", "amazon_it"]
    df[col] = df[col].fillna(0).astype(int)
    df[col_2] = df[col_2].fillna(0).astype(int)
    for c1, c2 in zip(col, col_2):
        df[c1] = df[c1] + df[c2]

    # temu
    sql = """
        SELECT sku, warehouse, count(1) as temu_listing_num
        FROM (
            SELECT distinct account_id, product_sku_id, sku, warehouse
            FROM yibai_oversea.tt_ads_oversea_temu_listing
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_ads_oversea_temu_listing )
        ) a
        GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_num = conn_ck.ck_select_to_df(sql)
    df_num = warehouse_mark(df_num)
    df_num = df_num.groupby(['new_sku', 'warehouse'])[f'temu_listing_num'].sum().reset_index()
    df = pd.merge(df, df_num, how='left', on=['new_sku', 'warehouse'])
    col = ["temu_listing_num", "amazon_de", "amazon_fr", "amazon_es", "amazon_it"]
    df[col] = df[col].fillna(0).astype(int)
    print(f"temu平台链接数量共{df_num['temu_listing_num'].sum()}条。")

    # 3、补充信息
    df = get_ban_info(df)

    # 库存转化
    df['库存_分段'] = np.where(df['total_available_stock'] > 5, '5+', df['total_available_stock'])
    df['库存金额'] = df['total_available_stock'] * df['new_price']

    need_columns = ["sku", "spu", "warehouse", "max_stock_warehouse_id", "max_stock_warehouse_name", "date_id",
                    "total_available_stock", "库存_分段", "库存金额", "new_price", "warehouse_stock_info", "develop_source_name",
                    "overage_level", "day_sales", "销售状态", "area","site", "new_sku", "责任人",
                    "amazon_listing_num", "amazon_de", "amazon_fr", "amazon_es", "amazon_it",
                    "amazon_failed_listing_num", "amazon_failed_de", "amazon_failed_fr", "amazon_failed_es",
                    "amazon_failed_it",
                    "ebay_listing_num",  "walmart_listing_num", "temu_listing_num",
                    "allegro_listing_num", "ali_listing_num", "shopee_listing_num", "lazada_listing_num",
                    "info_country", "侵权信息", "侵权等级", "forbid_plateform", "禁售信息", "禁售等级"]
    df = df[need_columns]
    change_columns = ["sku", "spu", "warehouse", "最大库存子仓ID", "最大库存子仓", "取数日期", "当前库存", "库存_分段", "库存金额", "成本",
                      "子仓库存明细", "开发来源", "超库龄等级", "日销", "销售状态","仓库区域", "仓库所在国家", "new_sku（主SKU）",
                      "责任人", "amazon_listing_num", "amazon_DE", "amazon_FR", "amazon_ES", "amazon_IT",
                      "amazon_failed_listing_num", "amazon_failed_de", "amazon_failed_fr", "amazon_failed_es",
                      "amazon_failed_it",
                      "ebay_listing_num", "walmart_listing_num", "temu_listing_num",
                      "allegro_listing_num", "ali_listing_num", "shopee_listing_num", "lazada_listing_num",
                      "info_country", "侵权信息", "侵权等级", "forbid_plateform", "禁售信息", "禁售等级"]
    df.columns = change_columns

    # 4、存表
    df.to_excel('F://Desktop//df_num_new_tt.xlsx', index=0)

    date_id = time.strftime('%Y-%m-%d')
    sql = f"ALTER TABLE 'oversea_platform_listing_num_tt' DELETE WHERE date_id = '{date_id}'"
    conn_ck.ck_execute_sql(sql)
    conn_ck.ck_insert(df, 'oversea_platform_listing_num_tt', if_exist='appdend')

    return df


# 各平台正常品负利润动销链接
def get_normal_sku_negative_profit():
    """ 正常品负利润订单的链接，单独取出来调价 """
    pass

def get_flash_sku_price():
    """ 限时清仓sku降价到位情况。未到位的单独取出来调价 """
    pass


def temp_temp():
    sql = """
    select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from yibai_system_kd_sync.yibai_amazon_account b
        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
        and account_id in (
            select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
            where account_type = 1 and is_yibai =1)
    """
    conn_ck = pd_to_ck(database='yibai_system_kd_sync', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)

    df_account.to_excel('F://Desktop//df_account.xlsx', index=0)


if __name__ == '__main__':
    get_amazon_listing()
    # get_ebay_listing()
    # get_walmart_listing()
    # get_cd_listing()
    # get_ali_listing()
    # get_shopee_listing()
    # get_temu_listing()
    # get_allegro_listing()
    # get_lazada_listing()
    # get_merc_listing()
    # failed_amazon_listing()

    # tt_get_amazon_listing_all()
    # tt_get_ebay_listing()
    # tt_get_walmart_listing()
    # tt_get_allegro_listing()
    tt_get_ali_listing()
    # tt_get_shopee_listing()
    # tt_get_lazada_listing()
    # tt_get_temu_listing()
    # tt_get_merc_listing()
    # tt_failed_amazon_listing()
    # temp_temp()
    # yb_listing_num_main()
    # tt_listing_num_main()
    # df = tt_get_temu_listing_info()
    # print(df.info())
    # temp_yb_listing_num_main()
    # temp_amazon()
    # tt_temp()
    # tt_shopee_account()
    # get_cloud_stock()
