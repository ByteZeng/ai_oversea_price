
"""
【统计TT海外仓链接数据条数】
"""
##
import numpy as np
import pandas as pd
import time, datetime
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.oversea_platform_listing import get_sku_info, site_table
from warnings import filterwarnings
from utils import utils
from utils.utils import  save_df, make_path, get_path
filterwarnings('ignore')

##


# def get_sku_info():
#     # 库存表获取
#     conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
#     conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
#     date_today = time.strftime('%Y%m%d')
#     sql = f"""
#         SELECT
#             sku, warehouse, date_id,
#             argMax(warehouse_id, available_stock) AS max_stock_warehouse_id,
#             argMax(warehouse_name, available_stock) AS max_stock_warehouse_name,
#             sum(available_stock) AS total_available_stock,
#             arrayStringConcat(groupArray(warehouse_stock), ', ') warehouse_stock_info
#         FROM (
#             SELECT
#                 ps.sku sku, toString(toDate(toString(date_id))) date_id,  yw.id AS warehouse_id,
#                 yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
#                 available_stock, warehouse_stock
#             FROM (
#                 SELECT
#                     sku, warehouse_id, date_id, cargo_owner_id, available_stock,
#                     concat(toString(warehouse_id),':',toString(available_stock)) as warehouse_stock
#                 FROM yb_datacenter.yb_stock
#                 WHERE
#                     date_id >= 20220901
#                     and date_id <= '{date_today}'  -- 根据需要取时间
#                     and available_stock > 0
#                     and cargo_owner_id = 8  -- 筛选货主ID为8的
#                     and warehouse_id in (
#                         SELECT id FROM yb_datacenter.yb_warehouse WHERE type IN ('third', 'overseas'))
#                 ORDER BY date_id DESC
#                 LIMIT 1 BY sku, warehouse_id
#             )AS ps
#             INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
#             LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
#             WHERE
#                 yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
#                 and (yw.warehouse_other_type = 2 or warehouse_id in (958))
#                 -- and yw.warehouse_name not in ('HC美国西仓','出口易美西仓-安大略仓','万邑通美南仓-USTX','亿迈CA01仓')  -- 剔除不常用仓库
#                 -- and ywc.name in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓','法国仓','乌拉圭仓','西班牙仓','意大利仓',
#                 -- '马来西亚仓','泰国仓','菲律宾仓','印度尼西亚仓','越南仓')
#                 -- and yw.warehouse_name like '%TT%'
#                 and yw.warehouse_name not like '%独享%'
#             ORDER BY date_id DESC
#         ) a
#         GROUP BY sku, warehouse, date_id
#     """
#     df_stock = conn_ck.ck_select_to_df(sql)
#     date = time.strftime('%Y-%m-%d')
#     df_stock['total_available_stock'] = np.where(df_stock['date_id'] < date, 0, df_stock['total_available_stock'])
#     df_stock['warehouse_stock_info'] = np.where(df_stock['date_id'] < date, '',
#                                                  df_stock['warehouse_stock_info'])
#     df_stock = df_stock.sort_values(by='date_id', ascending=False).drop_duplicates(subset=['sku','warehouse'])
#     # 销售状态和库龄表获取
#     sql = """
#         SELECT
#             A.sku sku, A.warehouse warehouse, overage_level, IF(D.sale_status IS NULL ,'正常', D.sale_status) as '销售状态'
#         FROM dwm_sku_temp_info A
#         LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
#         WHERE
#             date_id = (SELECT max(date_id) FROM dwm_sku_temp_info)
#     """
#     df_status = conn.read_sql(sql)
#     #
#     df_oversea_sku_final = pd.merge(df_stock, df_status, how='left', on=['sku','warehouse'])
#     df_oversea_sku_final = df_oversea_sku_final.drop_duplicates(subset=['sku','warehouse'])
#
#     # df_oversea_sku = pd.merge(df_stock, df_stock_age[['sku', 'warehouse_id', 'inventory_age']], how='left',
#     #                           on=['sku', 'warehouse_id'])
#     # 筛选常见仓库
#     df_sku_listing_new = df_oversea_sku_final[~df_oversea_sku_final['warehouse'].isna()]
#     df_sku_listing_new['area'] = np.where(df_sku_listing_new['warehouse'].str.contains('德国|法国|西班牙|意大利'),
#                                             '欧洲', df_sku_listing_new['warehouse'].str[:-1])
#
#     df_sku_listing_new = df_sku_listing_new[~df_sku_listing_new['area'].isin(['日本'])]
#     df_sku_listing_new['site'] = df_sku_listing_new['warehouse'].replace(
#         {'德国仓': 'de', '美国仓': 'us', '澳洲仓': 'au', '英国仓': 'uk', '墨西哥仓':'mx',  '加拿大仓': 'ca', '法国仓': 'fr',
#          '乌拉圭仓':'br', '西班牙仓': 'es', '意大利仓': 'it','马来西亚仓':'my', '泰国仓':'th', '菲律宾仓':'ph',
#          '印度尼西亚仓':'id', '越南仓':'vn'})
#
#     # 处理仓标数据
#     df_sku_listing_new['sku'] = df_sku_listing_new['sku'].str.strip()
#     df_sku_listing_new = df_sku_listing_new.sort_values(by='date_id', ascending=False).drop_duplicates(subset=['sku','warehouse'])
#
#     df_sku_listing_new['new_sku'] = np.where(
#         df_sku_listing_new['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
#         df_sku_listing_new['sku'].str[3:], df_sku_listing_new['sku'])
#     df_sku_listing_new['new_sku'] = np.where(df_sku_listing_new['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
#                                              df_sku_listing_new['sku'].str[:-2], df_sku_listing_new['new_sku'])
#     df_sku_listing_new['overage_level'] = df_sku_listing_new['overage_level'].fillna(0).astype(int)
#     df_sku_listing_new['销售状态'] = df_sku_listing_new['销售状态'].fillna('正常')
#     print(df_sku_listing_new.info())
#
#     return df_sku_listing_new
# df_sku = get_sku_info()
# df_sku.to_excel('df_sku.xlsx', index=0)
##
# def site_table():
#     conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
#     sql = """
#     SELECT site, site1, area as area0
#     FROM domestic_warehouse_clear.site_table
#     """
#     # df_site = pd.DataFrame(con.execute(sql), columns=['site', 'site1', 'area'])
#     df_site = conn_ck.ck_select_to_df(sql)
#     df_site['area'] = np.where(df_site['site1'].str.contains('德国|法国|西班|意大利|荷兰|瑞士|波兰|比利|土耳其'), '欧洲',
#                                df_site['site1'])
#     df_site['area'] = df_site['area'].replace('澳大利亚', '澳洲')
#     temp = pd.DataFrame({'site':['es'], 'site1':['西班牙'],'area':['欧洲']})
#     print(temp)
#     df_site = pd.concat([df_site, temp], ignore_index=True)
#
#     return df_site
#
# df_sku_listing_new = get_sku_info()


def get_amazon_listing_num(sku_list, df_site):
    print("===Amazon刊登链接数据==")
    # conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    li_am = []
    # sku_list = sku_list[0:100]
    for n in range(0,len(sku_list), 10000):
        sku_list_x = sku_list[n:n+10000]
        sql = """
        with listing_table as (
            select distinct account_id, seller_sku
            from tt_product_kd_sync.tt_amazon_sku_map
            where sku in {}
            -- and deliver_mode=2
        )
        SELECT sku, platform, site, count(1) as amazon_listing_num
        FROM (
            SELECT distinct sku, platform, site, account_id, asin
            FROM (
                select b.account_id as account_id, b.account_name as account_name,
                    group_name, short_name, 'AMAZON' as platform,
                    if(b.site ='sp', 'es', b.site) as site,
                    status, e.sku as sku, a.seller_sku as seller_sku,
                    open_date,
                    if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
                    a.price AS your_price, fulfillment_channel, f.sale_price as sale_price, a.price as online_price
                from (
                    select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
                    from tt_product_kd_sync.tt_amazon_listings_all_raw2
                    where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                ) a
                inner join (
                    select account_id, sku, seller_sku
                    from tt_product_kd_sync.tt_amazon_sku_map
                    where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                    -- where deliver_mode=2
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
                    where account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163
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
                limit 1 by sku, site, asin
            ) a
        ) b
        GROUP BY sku, platform, site
        settings max_memory_usage = 500000000000
        """.format(sku_list_x)
        # data = con.execute(sql)
        # columns1 = ['sku', 'platform', 'site', 'amazon_listing_num']
        # df_amazon_listing_x = pd.DataFrame(data=data, columns=columns1)
        df_amazon_listing_x = conn_ck.ck_select_to_df(sql)

        df = pd.merge(df_amazon_listing_x, df_site[['site', 'area']], on='site', how='left')
        df['area'] = np.where(df['site'] == 'es', '欧洲', df['area'])
        # 处理仓标数据
        df['new_sku'] = np.where(
            df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
            df['sku'].str[3:], df['sku'])
        df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']), df['sku'].str[:-2], df['new_sku'])

        df = df.groupby(['new_sku', 'site', 'area'])['amazon_listing_num'].sum().reset_index()
        print(f'{n}:{len(df)}')

        li_am.append(df)

    df_amazon_listing = pd.concat(li_am)


    df = df_amazon_listing.groupby(['new_sku', 'site', 'area'])['amazon_listing_num'].sum().reset_index()


    # df.to_excel(r"C:\Users\Administrator\Desktop\df_sku_listing_site_change_columns-4.xlsx")
    print(df.info())

    return df

def get_ebay_listing_num_new(sku_list, df_site):
    print( "===Ebay刊登链接数据===")
    # conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_ebay_listing_all = pd.DataFrame()
    for n in range(0, len(sku_list), 5000):
        sku = sku_list[n:n+5000]
        sql = f"""
            with  account_list as (
                select distinct id,account_id from tt_sale_center_system_sync.tt_system_account
                where platform_code='EB' and status=1 and is_del=0
            )
            SELECT 
                a.item_id, a.sku, '' system_sku, b.sell_sku, b.siteid as site, c.site_code country,
                CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
                b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
                d.account_id, a.start_price,
                f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price
            FROM (
                SELECT item_id,sku,start_price 
                FROM tt_sale_center_listing_sync.tt_ebay_online_listing_sales_sku 
                WHERE sku in ({sku})
            ) a 
            INNER JOIN (
                SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
                from tt_sale_center_listing_sync.tt_ebay_online_listing 
                where  warehouse_category_id !=1 and listing_status = 1 
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
        df_ebay_listing = conn_ck.ck_select_to_df(sql)
        df_ebay_listing.columns = [i.split('.')[-1] for i in df_ebay_listing.columns]
        df_ebay_listing['DATE'] = time.strftime('%Y-%m-%d')
        df_ebay_listing_all = pd.concat([df_ebay_listing, df_ebay_listing_all])
    print(df_ebay_listing_all.info())
    df_ebay_listing_all['item_id'] = df_ebay_listing_all['item_id'].astype(str)

    # 按 SKU + country 聚合计算 item的数量
    df_ebay_listing_all = df_ebay_listing_all.drop_duplicates(subset=['account_id', 'item_id', 'sku'])
    df_ebay_listing_temp = df_ebay_listing_all.groupby(['sku', 'country'])['item_id'].count().reset_index()
    df_ebay_listing_temp.rename(columns={'item_id': 'ebay_listing_num', 'country': 'site'}, inplace=True)
    df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].str.lower()
    df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].replace('motor', 'us')
    df_ebay_listing_temp = pd.merge(df_ebay_listing_temp, df_site[['site', 'area']], how='left', on='site')

    # 处理仓标数据
    df_ebay_listing_temp_2 = df_ebay_listing_temp
    df_ebay_listing_temp_2['new_sku'] = np.where(
        df_ebay_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_ebay_listing_temp_2['sku'].str[3:], df_ebay_listing_temp_2['sku'])
    df_ebay_listing_temp_2['new_sku'] = np.where(df_ebay_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                 df_ebay_listing_temp_2['sku'].str[:-2], df_ebay_listing_temp_2['new_sku'])

    df_ebay_listing_temp_2 = df_ebay_listing_temp_2.groupby(['new_sku', 'site', 'area'])[
        'ebay_listing_num'].sum().reset_index()

    return df_ebay_listing_temp_2


def get_walmart_listing_num(sku_list, df_site):
    print("===Walmart刊登链接数据===")
    # conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    li_wal = []
    for n in range(0, len(sku_list), 1000):
        sku_list_x = sku_list[n : n+1000]
        #  【20240524 修改】
        sql_walmart = """
                SELECT 
                    a.erp_id as account_id,b.short_name as short_name,
                    case 
                        when b.site='us_dsv' then 'us' 
                        else b.site 
                    end as site, 
                    a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price
                FROM (
                    select * from tt_sale_center_listing_sync.tt_walmart_report 
                    where upper(publish_status)='PUBLISHED' and sku in {}
                    order by updated_unix desc limit 1 by erp_id,seller_sku
                ) a
                left join (
                SELECT erp_id, short_name, lower(site_code) as site, status
                FROM tt_sale_center_system_sync.tt_system_account
                WHERE platform_code = 'WALMART'
                ) b 
                on a.erp_id=b.erp_id
                WHERE b.status=1 and b.site<>'us_dsv'
            """.format(sku_list_x)
        df_walmart_listing_x = conn_ck.ck_select_to_df(sql_walmart)
        li_wal.append(df_walmart_listing_x)

    df_walmart_listing = pd.concat(li_wal)
    df_walmart_listing.columns = [i.split('.')[-1] for i in df_walmart_listing.columns]
    print(df_walmart_listing)

    df_walmart_listing_temp = df_walmart_listing.drop_duplicates(subset=['item_id', 'sku'])

    df_walmart_listing_temp = df_walmart_listing_temp.groupby(['sku', 'site'])['item_id'].count().reset_index()
    df_walmart_listing_temp.rename(columns={'item_id': 'walmart_listing_num'}, inplace=True)
    df_walmart_listing_temp['site'] = df_walmart_listing_temp['site'].str.lower()
    df_walmart_listing_temp = pd.merge(df_walmart_listing_temp, df_site[['site', 'area']], how='left', on='site')

    df_walmart_listing_temp_2 = df_walmart_listing_temp
    df_walmart_listing_temp_2['new_sku'] = np.where(
        df_walmart_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_walmart_listing_temp_2['sku'].str[3:], df_walmart_listing_temp_2['sku'])
    df_walmart_listing_temp_2['new_sku'] = np.where(
        df_walmart_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']), df_walmart_listing_temp_2['sku'].str[:-2],
        df_walmart_listing_temp_2['new_sku'])
    df_walmart_listing_temp_2 = df_walmart_listing_temp_2.groupby(['new_sku', 'site', 'area'])[
        'walmart_listing_num'].sum().reset_index()

    return df_walmart_listing_temp_2

def get_aliexpress_listing_num(df_site):
    print("===aliexpress刊登链接数据===")
    sql = """
        SELECT 
            a.product_id, d.account_id, a.sku sku,sku_price,sku_code, aeop_s_k_u_property_list_str, b.property_value_id, 
            b.sku_property_id, c.name_en, d.freight_template_id, f.price_mode_name,pop_choice_status,
            aliexpress_currency_code1 aliexpress_currency_code
            -- if(e.aliexpress_currency_code1='', 'USD', e.aliexpress_currency_code1) AS aliexpress_currency_code
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
                product_id, account_id, product_price, freight_template_id, pop_choice_status
            FROM tt_sale_center_listing_sync.tt_aliexpress_listing
            WHERE 
                account_id in (
                    select id as account_id from tt_sale_center_system_sync.tt_system_account
                    where platform_code = 'ALI' and is_del=0 and `status`=1 )
                -- and product_status_type = 1
        ) d ON a.product_id=d.product_id
        LEFT join (
            SELECT t1.*,t2.template_name_id as template_name_id,t2.account_id as account_id
            FROM tt_sale_center_listing_sync.tt_aliexpress_fee_template_setting t1 
            left join tt_sale_center_listing_sync.tt_aliexpress_price_type_setting t2 
            on t1.price_mode_name = t2.price_mode_name
            where t1.is_delete=0 and t2.is_delete=0
        ) f
        on toString(d.freight_template_id)=f.template_name_id and d.account_id=f.account_id
        LEFT JOIN (
            select account_id,if(aliexpress_currency_code='', 'USD', aliexpress_currency_code) AS aliexpress_currency_code1
            from tt_sale_center_common_sync.tt_common_account_config
            where platform_code IN ('ALI') and is_del = 0
        ) e  on d.account_id=e.account_id
        -- 具体国外发货地
        LEFT JOIN (
            SELECT DISTINCT parent_attr_id, attr_id, name_en
            FROM tt_sale_center_listing_sync.tt_aliexpress_category_attribute
        ) c ON toInt64(b.sku_property_id) = toInt64(c.parent_attr_id) and toInt64(b.property_value_id) = toInt64(c.attr_id)
        where a.sku !='' and f.price_mode_name is not null
    """
    # conn_ck = pd_to_ck(database='temp_database_hxx', data_sys='调价明细历史数据1')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    df_ali_listing = conn_ck.ck_select_to_df(sql)
    df_ali_listing.columns = [i.split('.')[-1] for i in df_ali_listing.columns]
    #
    df_ali_listing[['property_value_id', 'sku_property_id']] = df_ali_listing[['property_value_id', 'sku_property_id']].astype(int).astype(str)
    dic = {'United States':'US', 'CZECH REPUBLIC':'DE', 'Czech Republic':'DE', 'Poland':'DE', 'france':'DE', 'France':'DE',
           'Australia':'AU', 'CN':'CN', 'spain':'DE', 'SPAIN':'DE', 'Russian Federation':'RU', 'UNITED KINGDOM':'UK',
           'United Kingdom':'UK','GERMANY':'DE', 'Mexico':'MX', 'cz':'DE', 'ITALY':'DE', 'Italy':'DE'}
    df_ali_listing['country'] = df_ali_listing['name_en'].replace(dic)
    df_ali_listing = df_ali_listing.drop_duplicates(subset=['product_id','sku','country'])
    #
    df_ali_listing = df_ali_listing[df_ali_listing['country']!='CN']

    # df_ali_listing.to_excel('F://Desktop//tt_ali_listing.xlsx', index=0)
    #
    # 按 SKU + country 聚合计算 item的数量
    df = df_ali_listing[~df_ali_listing['country'].isna()]
    df = df.groupby(['sku', 'country'])['product_id'].count().reset_index()
    df.rename(columns={'product_id': 'aliexpress_listing_num', 'country': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')
    # df['area'] = np.where(df['site'] == 'es', '欧洲', df['area'])

    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
                             df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                             df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['aliexpress_listing_num'].sum().reset_index()

    return df

def get_temu_listing_num():
    print("===temu刊登链接数据===")
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    sql = """
    with d as
    (select product_spu_id,product_sku_id,max(id) as id from tt_sale_center_listing_sync.tt_temu_listing_crawling_log
    group by product_spu_id,product_sku_id),
    c as (select * from tt_sale_center_listing_sync.tt_temu_listing_crawling_log where id in (select id from d))

    select 
        e.account_id,e.short_name,a.site_code,a.site_code country, a.item_id,a.product_sku_id,a.product_skc_id,
        a.stock_number,c.online_status,a.sku, 
        -- p.select_status lazada_account_operation_mode,
        c.added_to_site_time,a.supplier_price,date(a.create_time) as create_time, a.select_status
    from tt_sale_center_listing_sync.tt_temu_listing a
    left join tt_sale_center_common_sync.tt_common_account_config b on a.account_id=b.account_id
    left join c on a.item_id =c.product_spu_id and a.product_sku_id=c.product_sku_id
    -- left join tt_sale_center_listing_sync.tt_temu_listing_supplier_price_site p 
    -- on a.product_sku_id = p.product_sku_id and a.account_id = p.account_id
    left join tt_sale_center_system_sync.tt_system_account as e on a.account_id=e.id
    where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    and a.select_status = 12 -- 筛选已加入站点的链接
    """
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    listing_t['product_sku_id'] = listing_t['product_sku_id'].astype(str)
    print(listing_t.info())
    # 20250212 更新链接状态取数表
    listing_t['online_status'] = np.where(listing_t['select_status'] == -1,
                                          listing_t['online_status'], listing_t['select_status'])
    dic = {-1: '未同步', 0: '已弃用', 1: '待平台选品', 14: '待卖家修改', 15: '已修改', 16: '服饰可加色',
           2: '待上传生产资料',
           3: '待寄样', 4: '寄样中', 5: '待平台审版', 6: '审版不合格', 7: '平台核价中', 8: '待修改生产资料',
           9: '核价未通过', 10: '待下首单', 11: '已下首单', 12: '已加入站点', 13: '已下架', 17: '已终止'}
    listing_t['online_status'] = listing_t['online_status'].replace(dic)
    listing_t.loc[listing_t['online_status'] == '', 'online_status'] = '待申报'
    listing_t.loc[listing_t['online_status'].isnull(), 'online_status'] = '未知'
    listing_t.drop('select_status', axis=1, inplace=True)

    listing_t = listing_t.sort_values(by='added_to_site_time', ascending=False). \
        drop_duplicates(subset=['account_id', 'product_sku_id', 'sku', 'country'])

    # 20251009：TTtemu活动链接信息暂未补全
    listing_t['is_promotion'] = 0
    listing_t['origin_supplier_price'] = listing_t['supplier_price']
    # 获取捆绑链接表的sku信息
    sql = """
    select erp_id as account_id,platform_sku as product_sku_id,company_sku as sku 
    from tt_sale_center_listing_sync.tt_temu_bind_sku
    order by update_time desc
    """
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    tt_temu_bind_sku = conn_ck.ck_select_to_df(sql)
    print(tt_temu_bind_sku.info())
    tt_temu_bind_sku.drop_duplicates(subset=['account_id', 'product_sku_id'], inplace=True)

    listing_t = listing_t.merge(tt_temu_bind_sku, on=['account_id', 'product_sku_id'], how='left',
                                suffixes=['', '1'])
    listing_t.loc[listing_t['sku'] == '', 'sku'] = np.nan
    listing_t['sku'].fillna(listing_t['sku1'], inplace=True)
    listing_t.drop('sku1', axis=1, inplace=True)
    # listing_t.rename(columns={'added_to_site_time': '加入站点时间', 'supplier_price': '申报价格'}, inplace=True)

    # listing_t['country'] = listing_t['country'].replace({'SP': 'ES', 'GB': 'UK'})
    dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
           'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲', 'SE': '欧洲', 'BE': '欧洲',
           'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大', 'MX': '墨西哥'}
    listing_t['站点'] = listing_t['site_code'].apply(
        lambda x: next((dic[code] for code in x.split(',') if code in dic), None))

    dic = {'欧洲': '德国仓', '澳大利亚': '澳洲仓', '美国': '美国仓', '英国': '英国仓',
           '新西兰': '澳洲仓', '加拿大': '加拿大仓', '墨西哥': '墨西哥仓'}
    listing_t['warehouse'] = listing_t['站点'].replace(dic)
    # listing_t.drop('站点', axis=1, inplace=True)
    listing_t = listing_t.drop_duplicates()


    # # temu账号获取
    # sql = """
    # select
    #     a.account_id `账号ID`,a.account_status `账号状态`,a.account_id `平台账号ID`, b.main_name `主体账号`,a.oa_department_name `大部`,
    #     a.oa_group_name `小组` ,a.account_name `账号全称`,a.account_s_name `账号简称`,a.account_num_name `账号店铺名`,a.account_operation_mode `账号运营模式`
    # from yibai_account_manage_sync.yibai_temu_account a
    # left join yibai_account_manage_sync.yibai_account_main b
    # on a.main_id=b.main_id
    # where a.account_type=1
    # """
    # conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    # df_temu_account = conn_ck.ck_select_to_df(sql)
    # df_temu_account['账号状态'] = df_temu_account['账号状态'].map({10: '启用', 20: '停用', 30: '异常', })
    # df_temu_account['账号运营模式'] = df_temu_account['账号运营模式'].map({1: '全托管', 2: '半托管'})
    # df_temu_account.rename(columns={'账号ID': 'account_id'}, inplace=True)
    #
    # listing_t = pd.merge(listing_t,
    #                          df_temu_account[['account_id', '主体账号', '大部', '小组', '账号运营模式']],
    #                          how='left', on=['account_id'])
    # 1、有效链接
    df_temu_listing = listing_t[
        (listing_t['online_status'] == '已加入站点') & (~listing_t['sku'].isna()) & (listing_t['sku'] != '')]
    # 整体的链接数量
    df_temu = df_temu_listing.groupby(['sku', 'warehouse']).agg({'item_id': 'count'}).reset_index()
    # df_temu['链接数量分段'] = pd.cut(df_temu['item_id'], bins=[-1, 0, 1, 3, 5, 10, 20, 40, 60, 10000],
    #                                  labels=['A:0', 'B:1', 'C:(1,3]', 'D:(3,5]', 'E:(5,10]', 'F:(10,20]', 'G:(20,40]',
    #                                          'H:(40,60]', 'I:60+'])
    # df_temu['链接数量分段'] = np.where(df_temu['item_id'] > 60, 'I:60+', df_temu['链接数量分段'])
    df_temu = df_temu[~df_temu['sku'].isna()]
    df_temu = df_temu.rename(columns={'item_id': 'temu_listing_num'})

    # df_temu.to_excel('F://Desktop//tt_df_temu.xlsx', index=0)

    return df_temu

##
def get_shopee_listing_num():
    """ 统计shopee平台海外仓链接 """
    # 取shopee海外仓账号
    sql = """

        SELECT account_id, account_s_name, account_type
        FROM yibai_account_manage_sync.yibai_shopee_account a
        WHERE account_mold != 1 and account_status = 10 and account_type = 20 -- 筛选tt账号

    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_shopee_account = conn_ck.ck_select_to_df(sql)
    print(df_shopee_account.info())
    sql = """
        select distinct account_id,a.short_name,b.shopee_is_3pf, a.id as id
        from tt_sale_center_system_sync.tt_system_account as a
        left join tt_sale_center_common_sync.tt_common_account_config as b
        on b.account_id = a.id and  b.platform_code='SHOPEE' and b.is_del=0
        where a.platform_code='SHOPEE' and a.is_del=0
    """
    conn_ck = pd_to_ck(database='tt_account_manage_sync', data_sys='通拓-新')
    tt_shopee_account = conn_ck.ck_select_to_df(sql)
    account_1 = tt_shopee_account[tt_shopee_account['shopee_is_3pf']==1]
    account_1 = tuple(account_1['id'].unique())

    df_shopee_account = pd.merge(df_shopee_account, tt_shopee_account, how='inner', on='account_id')
    account_2 = tuple(df_shopee_account['id'].unique())

    account_list = tuple(set(account_1).union(set(account_2)))
    sql = f"""
        SELECT item_id, account_id, price, status_online, sku, country_code
        FROM tt_sale_center_listing_sync.tt_shopee_listing
        WHERE is_delete = 0 and account_id in {account_list}
        and status_online in ('NORMAL','UNLIST','MODEL_NORMAL')
    """
    conn_ck = pd_to_ck(database='tt_account_manage_sync', data_sys='通拓-新')
    tt_shopee_listing = conn_ck.ck_select_to_df(sql)
    print(tt_shopee_listing.info())

    # 按 SKU + country 聚合计算 item的数量
    df = tt_shopee_listing[(tt_shopee_listing['country_code']!='') & (tt_shopee_listing['sku']!='')]
    df = df.groupby(['sku', 'country_code'])['item_id'].count().reset_index()
    df.rename(columns={'item_id': 'shopee_listing_num', 'country_code': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    # df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')
    dic = {'my':'马来西亚', 'th':'泰国', 'ph':'菲律宾仓', 'id':'印度尼西亚仓', 'vn':'越南仓'}
    df['area'] = df['site'].replace(dic)
    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
                             df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                             df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['shopee_listing_num'].sum().reset_index()

    # df.to_excel('F://Desktop//tt_shopee_listing.xlsx', index=0)

    return df

##
def get_lazada_listing_num():
    """ 统计lazada平台海外仓链接 """
    # 取lazada海外仓账号
    sql = """
    SELECT item_id, a.account_id, price, seller_sku, sku, country_code
    FROM yibai_sale_center_listing_sync.yibai_shopee_listing a
    INNER JOIN (
        SELECT b.id account_id, account_s_name, shopee_is_3pf, account_type
        FROM yibai_account_manage_sync.yibai_shopee_account a
        LEFT JOIN (
            select distinct account_id,a.short_name,b.shopee_is_3pf, a.id as id
            from yibai_sale_center_system_sync.yibai_system_account as a
            left join yibai_sale_center_common_sync.yibai_common_account_config as b
            on b.account_id = a.id and  b.platform_code='SHOPEE' and b.is_del=0
            where a.platform_code='SHOPEE' and a.is_del=0
        ) b
        ON a.account_id = b.account_id
        WHERE account_mold != 1 and account_status = 10 and account_type = 20 -- 筛选tt账号
    ) b ON a.account_id = b.account_id 
    WHERE is_delete = 0 
    LIMIT 100
    """
    sql = """

        SELECT account_id, account_s_name, account_type, account_mold, account_status
        FROM yibai_account_manage_sync.yibai_lazada_account a
        WHERE account_status = 10 
        and account_mold != 1 
        and  account_type = 20 -- 筛选tt账号

    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_lazada_account = conn_ck.ck_select_to_df(sql)
    print(df_lazada_account.info())
    sql = """
        select distinct account_id,a.short_name, a.id as id
        from tt_sale_center_system_sync.tt_system_account as a
        left join tt_sale_center_common_sync.tt_common_account_config as b
        on b.account_id = a.id and  b.platform_code='LAZADA' and b.is_del=0
        where a.platform_code='LAZADA' and a.is_del=0
    """
    conn_ck = pd_to_ck(database='tt_account_manage_sync', data_sys='通拓-新')
    tt_lazada_account = conn_ck.ck_select_to_df(sql)

    df_lazada_account = pd.merge(df_lazada_account, tt_lazada_account, how='inner', on='account_id')
    # 补充3pf账号
    df_3pf = pd.read_excel('F://Ding_workspace//tt_lazada_海外仓账号-3PF补充20250729.xlsx')
    df_lazada_account = pd.concat([df_lazada_account, df_3pf])
    # account_list = tuple(np.concatenate((df_lazada_account['id'].unique(),df_3pf['id'].unique())))
    account_list = tuple(df_lazada_account['id'].unique())
    # df_lazada_account.to_excel('F://Desktop//df_lazada_account.xlsx', index=0)
    sql = f"""
        SELECT item_id, account_id, price, status_online, sku, country_code
        FROM tt_sale_center_listing_sync.tt_lazada_listing
        WHERE is_delete = 0 
        and account_id in {account_list}
        and status_online not in ('Deleted')
    """
    conn_ck = pd_to_ck(database='tt_account_manage_sync', data_sys='通拓-新')
    tt_lazada_listing = conn_ck.ck_select_to_df(sql)
    print(tt_lazada_listing.info())
    # tt_lazada_listing.to_excel('F://Desktop//tt_lazada_listing.xlsx', index=0)
    # 按 SKU + country 聚合计算 item的数量
    df = tt_lazada_listing[(tt_lazada_listing['country_code']!='') & (tt_lazada_listing['sku']!='')]
    df = df.groupby(['sku', 'country_code'])['item_id'].count().reset_index()
    df.rename(columns={'item_id': 'lazada_listing_num', 'country_code': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    # df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')
    dic = {'my':'马来西亚', 'th':'泰国', 'ph':'菲律宾仓', 'id':'印度尼西亚仓', 'vn':'越南仓'}
    df['area'] = df['site'].replace(dic)
    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
                             df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                             df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['lazada_listing_num'].sum().reset_index()

    # df.to_excel('F://Desktop//tt_lazada_listing.xlsx', index=0)

    return df

def get_allegro_listing_num(sku_list, df_site):
    print("===allegro刊登链接数据===")
    sql = f"""
    select 
        a.account_id as account_id, b.erp_id, b.short_name as account_name,  
        a.offer_id as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
        a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price,
        toFloat64(a.delivery_amount) as delivery_amount, a.location1 as location
    FROM (
        SELECT 
            *, replace(JSONExtractRaw(location, 'countryCode'), '"', '') as location1
        FROM tt_sale_center_listing_sync.tt_allegro_listing
        WHERE 
            selling_mode_price > 0 
            -- and status in (1,4)
            and location1 not in ('CN', 'GB')
        ) a 
    INNER JOIN (
        SELECT b.id, b.erp_id,  b.short_name
        FROM tt_sale_center_system_sync.tt_system_account b
        WHERE platform_code  = 'ALLEGRO' 
    ) b 
    on a.account_id=b.id

    """
    conn_ck = pd_to_ck(database='tt_account_manage_sync', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)
    # 获取海外仓sku
    sql = f"""
        select distinct sku 
        from dwm_sku_temp_info 
        WHERE date_id>=(SELECT max(date_id) FROM dwm_sku_temp_info)
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)

    # df = pd.merge(df, df_sku, how='inner', on=['sku'])
    df = df[df['sku'].isin(sku_list)]

    # 按 SKU + country 聚合计算 item的数量
    df = df[~df['location'].isna()]
    df['location'] = df['location'].replace({'CZ':'DE'})
    df = df.drop_duplicates(subset=['account_id', 'offer_id', 'sku'])
    df = df.groupby(['sku', 'location'])['offer_id'].count().reset_index()
    df.rename(columns={'offer_id': 'allegro_listing_num', 'location': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')

    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                 df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['allegro_listing_num'].sum().reset_index()

    return df

##
def listing_num_section(df):
    """
    对各平台链接数量添加分段列
    """
    # 分段值可根据实际数量分布调整
    for i in df.columns[12:]:
        new_column = i + '_section'
        df[new_column] = pd.cut(df[i], bins=[-1, 0, 1, 3, 5, 10, 20, 40, 60, 10000],
                                labels=['A:0', 'B:1', 'C:(1,3]', 'D:(3,5]', 'E:(5,10]', 'F:(10,20]', 'G:(20,40]',
                                        'H:(40,60]', 'I:60+'])
    return df


def get_ban_info(df):
    # 侵权违禁信息数据读取
    # 读取侵权信息数据表
    # conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    sql_info = f"""
    SELECT sku, arrayStringConcat(groupArray(country_code), ',') AS info_country, any(risk_grade) as `侵权信息`, any(risk_grade_type) as `侵权等级` 
    FROM tt_prod_base_sync.tt_prod_inf_country_grade
    WHERE is_del = 0 
    GROUP BY sku
    """
    df_info = conn_ck.ck_select_to_df(sql_info)
    if len(df_info) == 0:
        df_info = pd.DataFrame(columns=['sku','info_country','侵权信息','侵权等级'])
    else:
        df_info['sku'] = df_info['sku'].astype('str')
    # 读取禁售信息数据表
    sql_js = f"""
    SELECT sku ,arrayStringConcat(groupArray(platform_code), ',') as forbid_plateform, any(risk_grade) as `禁售信息` , any(risk_grade_type) as `禁售等级` 
    FROM tt_prod_base_sync.tt_prod_forbidden_grade
    WHERE is_del = 0 
    GROUP BY sku
    """
    df_js = conn_ck.ck_select_to_df(sql_js)
    df_js['sku'] = df_js['sku'].astype('str')

    df = pd.merge(df, df_js, on='sku', how='left')
    df = pd.merge(df, df_info, on='sku', how='left')

    return df

def temp():
    """
    合并信息
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    df_sku_listing_new = get_sku_info()
    sku_list = df_sku_listing_new['sku'].to_list()
    df_site = site_table()

    # df_amazon_listing_temp_2 = get_amazon_listing_num(sku_list, df_site)
    # df_ebay_listing_temp_2 = get_ebay_listing_num_new(sku_list, df_site)
    # df_cd_listing_temp_2 = get_cd_listing_num(sku_list, df_site)
    # df_walmart_listing_temp_2 = get_walmart_listing_num(sku_list, df_site)
    # df_wish = get_wish_listing_num(sku_list, df_site)
    # df_temu_listing = get_temu_listing_num()
    # df = get_allegro_listing_num(sku_list, df_site)
    df_ali = get_aliexpress_listing_num(df_site)
    # df_lazada = get_lazada_listing_num()
    # print(df_ali.info())
    # df_sku_listing_site = pd.merge(df_sku_listing_new, df_ebay_listing_temp_2[['new_sku', 'ebay_listing_num', 'site']],
    #                                how='left', on=['new_sku', 'site'])
    # df_sku_listing_site = pd.merge(df_sku_listing_site, df_ebay_listing_temp_3[['new_sku', 'ebay_listing_num', 'site']],
    #                                how='left', on=['new_sku', 'site'])
    # df = pd.merge(df_sku_listing_new, df[['new_sku', 'allegro_listing_num', 'site']], how='left', on=['new_sku', 'site'])
    # df = pd.merge(df, df_wish[['new_sku', 'wish_listing_num', 'site']],
    #                                how='left', on=['new_sku', 'site'])
    # df = pd.merge(df, df_temu_listing[['sku', 'warehouse', 'temu_listing_num']],
    #                                how='left', on=['sku', 'warehouse'])
    # df = pd.merge(df_sku_listing_new, df_ali[['new_sku','area', 'aliexpress_listing_num']],
    #                                how='left', on=['new_sku', 'area'])
    # save_df(df, '海外仓SKU各平台刊登情况_ali_temp', file_type='xlsx')
    # save_df(df_cd_listing_temp_2, '海外仓SKU各平台刊登情况_test', file_type='xlsx')

    # return df
##
# df_temp = temp()
##
def temu_group():
    """
    temu链接覆盖海外仓sku情况，分团队
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    df_sku_listing_new = get_sku_info()

    temu_group, df_temu_listing = get_temu_listing_num()
    temu_group = temu_group.fillna(0)  #

    df_sku_listing_new = pd.merge(df_sku_listing_new, temu_group, how='left', on=['sku', 'warehouse'])
    df_sku_listing_new.iloc[:, 12:] = df_sku_listing_new.iloc[:, 12:].fillna(0).astype(int)
    df_sku_listing_new = listing_num_section(df_sku_listing_new)

    save_df(df_sku_listing_new, 'TEMU团队海外仓SKU刊登情况', file_type='xlsx')

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
            END as `成本`
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[1]
    df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    # df_line['四级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[3]
    df_line = df_line.drop_duplicates(subset='sku')

    # 取开发来源
    sql = """
        SELECT distinct id as develop_source, develop_source_name is_tt_sku
        FROM yibai_prod_base_sync.yibai_prod_develop_source
    """
    df_source =conn_ck.ck_select_to_df(sql)
    df_line = pd.merge(df_line, df_source, how='left', on=['develop_source'])
    # c1 = df_line['develop_source'].isin([14,15,22])
    # c2 = df_line['产品名称'].str.contains('通拓')
    # df_line['is_tt_sku'] = np.where(c1 | c2, 1, 0)
    df = pd.merge(df, df_line[['sku','is_tt_sku','一级产品线','二级产品线','三级产品线','spu','成本']], how='left', on=['sku'])

    return df

def get_sales():
    sql = """
        SELECT
            A.sku sku, A.warehouse warehouse, overage_level,day_sales, IF(D.sale_status IS NULL ,'正常', D.sale_status) as '销售状态'
        FROM dwm_sku_temp_info A
        LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
        WHERE 
            date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)
    # df_sku.to_excel('F://Desktop//df_sku_sales.xlsx', index=0)
    return df_sku

##
def main():
    """
    合并信息
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    df_sku_listing_new = get_sku_info()
    sku_list = df_sku_listing_new['sku'].to_list()
    df_site = site_table()

    df_amazon_listing_temp_2 = get_amazon_listing_num(sku_list, df_site)
    df_ebay_listing_temp_2 = get_ebay_listing_num_new(sku_list, df_site)
    # df_cd_listing_temp_2 = get_cd_listing_num(sku_list, df_site)
    df_walmart_listing_temp_2 = get_walmart_listing_num(sku_list, df_site)
    # df_wish_listing_temp_2 = get_wish_listing_num(sku_list, df_site)
    df_temu_listing = get_temu_listing_num()
    df_allegro = get_allegro_listing_num(sku_list, df_site)
    df_ali = get_aliexpress_listing_num(df_site)
    df_shopee = get_shopee_listing_num()
    df_lazada = get_lazada_listing_num()
    # 按 sku + site 合并
    df_sku_listing_site = pd.merge(df_sku_listing_new, df_amazon_listing_temp_2[['new_sku', 'amazon_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ebay_listing_temp_2[['new_sku', 'ebay_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site,
                                   df_walmart_listing_temp_2[['new_sku', 'walmart_listing_num', 'site']], how='left',
                                   on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_temu_listing[['sku', 'warehouse', 'temu_listing_num']],
                                   how='left', on=['sku', 'warehouse'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ali[['new_sku','area', 'aliexpress_listing_num']],
                                   how='left', on=['new_sku', 'area'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_shopee[['new_sku','site', 'shopee_listing_num']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_lazada[['new_sku','site', 'lazada_listing_num']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_allegro[['new_sku','site', 'allegro_listing_num']],
                                   how='left', on=['new_sku', 'site'])
    # 统计欧洲区域sku亚马逊和ebay平台欧洲各站点（德法西意）的刊登情况
    # amazon_temp = df_amazon_listing_temp_2[df_amazon_listing_temp_2['area'] == '欧洲']
    #
    amazon_temp = df_amazon_listing_temp_2.loc[df_amazon_listing_temp_2['area'].isin(['欧洲','巴西', '墨西哥'])]  # 新增墨西哥
    amazon_temp.drop(['area'], axis=1, inplace=True)

    amazon_temp = amazon_temp.pivot_table(index='new_sku', columns='site').reset_index()

    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_': 'new_sku'}, inplace=True)
    print('打印amazon站点：')
    print(amazon_temp.info())
    df_sku_listing_site = pd.merge(df_sku_listing_site, amazon_temp[['new_sku', 'amazon_de', 'amazon_fr', 'amazon_es', 'amazon_it']], how='left', on=['new_sku'])

    ebay_temp = df_ebay_listing_temp_2[df_ebay_listing_temp_2['area'] == '欧洲']
    ebay_temp.drop(['area'], axis=1, inplace=True)

    ebay_temp = ebay_temp.pivot_table(index='new_sku', columns='site').reset_index()
    ebay_temp.columns = ebay_temp.columns.droplevel(0)
    ebay_temp.columns = 'ebay_' + ebay_temp.columns
    ebay_temp.rename(columns={'ebay_': 'new_sku'}, inplace=True)
    print('打印ebay站点：')
    print(ebay_temp.info())

    df_sku_listing_site = pd.merge(df_sku_listing_site, ebay_temp[['new_sku', 'ebay_de']], how='left', on=['new_sku'])
    df_sku_listing_site.fillna(0, inplace=True)
    #

    df_sku_listing_site_test = listing_num_section(df_sku_listing_site)
    # 侵权违禁
    df_sku_listing_site_test = get_ban_info(df_sku_listing_site_test)

    need_columns = ["sku", "warehouse", "max_stock_warehouse_id","max_stock_warehouse_name","date_id","total_available_stock",
                    "warehouse_stock_info","overage_level","销售状态","area","site","new_sku","amazon_listing_num","amazon_de","amazon_fr","amazon_es","amazon_it",
                    "ebay_listing_num","ebay_de","walmart_listing_num",
                    "temu_listing_num","aliexpress_listing_num","shopee_listing_num","lazada_listing_num","allegro_listing_num",
                    "amazon_listing_num_section","amazon_de_section","amazon_fr_section","amazon_es_section",
                    "amazon_it_section","ebay_listing_num_section","ebay_de_section",
                    "walmart_listing_num_section","temu_listing_num_section","aliexpress_listing_num_section",
                    "shopee_listing_num_section", "lazada_listing_num_section","allegro_listing_num_section",
                    "info_country","侵权信息","侵权等级",
                    "forbid_plateform","禁售信息","禁售等级"]
    df_sku_listing_site_test = df_sku_listing_site_test[need_columns]
    change_columns = ["sku","warehouse","最大库存子仓ID","最大库存子仓","最近有库存日期","当前库存","子仓库存明细","超库龄等级","销售状态",
                      "仓库区域","仓库所在国家","new_sku（主SKU）","amazon_listing_num","amazon_DE","amazon_FR","amazon_ES",
                      "amazon_IT","ebay_listing_num","ebay_DE",
                      "walmart_listing_num","temu_listing_num","aliexpress_listing_num",
                      "shopee_listing_num", "lazada_listing_num","allegro_listing_num",
                      "Amazon链接数量分段","Amazon_DE链接数量分段",
                      "Amazon_FR链接数量分段","Amazon_ES链接数量分段","Amazon_IT链接数量分段","ebay链接数量分段",
                      "ebay_DE链接数量分段","walmat链接数量分段","temu链接数量分段","aliexpress链接数量分段",
                      "shopee链接数量分段","lazada链接数量分段","allegro链接数量分段",
                      "info_country","侵权信息", "侵权等级","forbid_plateform","禁售信息","禁售等级"]
    df_sku_listing_site_test.columns = change_columns

    # 补充一级二级三级类目
    df_sku_listing_site_test = get_line(df_sku_listing_site_test)
    # df_sku_listing_site_test = df_sku_listing_site_test[df_sku_listing_site_test['is_tt_sku'] == 1]
    # 匹配日销
    df_sales = get_sales()

    df = pd.merge(df_sku_listing_site_test, df_sales[['sku', 'warehouse', 'day_sales']],
                                        how='left', on=['sku', 'warehouse'])
    daysales = df.pop('day_sales')
    df.insert(df.columns.get_loc('超库龄等级') + 1, '日销', daysales)

    # 库存转化
    df['库存_分段'] = np.where(df['当前库存']>5, '5+', df['当前库存'])
    stock_bin = df.pop('库存_分段')
    df.insert(df.columns.get_loc('当前库存') + 1, '库存_分段', stock_bin)

    df['库存金额'] = df['当前库存'] * df['成本']
    stock_money = df.pop('库存金额')
    df.insert(df.columns.get_loc('库存_分段') + 1, '库存金额', stock_money)

    save_df(df, 'TT海外仓SKU各平台刊登情况', file_type='xlsx')
    print('done!')
    # df_sku_listing_site_test.to_excel('df_sku_listing_site.xlsx', index=0)

def main_ym():
    """
    合并信息
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    # df_sku_listing_new = get_sku_info()
    df_sku_listing_new = get_ym_sku()
    df_sku_listing_new['site'] = df_sku_listing_new['site'].apply(lambda x: x.lower())
    df_sku_listing_new['new_sku'] = df_sku_listing_new['sku']
    df_sku_listing_new['warehouse'] = df_sku_listing_new['site'].replace({'us':'美国仓','de':'德国仓'})
    df_sku_listing_new['area'] = df_sku_listing_new['site'].replace({'us': '美国', 'de': '欧洲'})
    print(df_sku_listing_new.info())
    sku_list = df_sku_listing_new['sku'].to_list()

    df_site = site_table()

    df_amazon_listing_temp_2 = get_amazon_listing_num(sku_list, df_site)
    df_ebay_listing_temp_2 = get_ebay_listing_num_new(sku_list, df_site)
    # df_cd_listing_temp_2 = get_cd_listing_num(sku_list, df_site)
    df_walmart_listing_temp_2 = get_walmart_listing_num(sku_list, df_site)
    # df_wish_listing_temp_2 = get_wish_listing_num(sku_list, df_site)
    df_temu_listing = get_temu_listing_num()
    df_allegro = get_allegro_listing_num(sku_list, df_site)
    df_ali = get_aliexpress_listing_num(df_site)
    # 按 sku + site 合并
    df_sku_listing_site = pd.merge(df_sku_listing_new, df_amazon_listing_temp_2[['new_sku', 'amazon_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ebay_listing_temp_2[['new_sku', 'ebay_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site,
                                   df_walmart_listing_temp_2[['new_sku', 'walmart_listing_num', 'site']], how='left',
                                   on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_temu_listing[['sku', 'warehouse', 'temu_listing_num']],
                                   how='left', on=['sku', 'warehouse'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_allegro[['new_sku','site', 'allegro_listing_num']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ali[['new_sku','area', 'aliexpress_listing_num']],
                                   how='left', on=['new_sku', 'area'])
    # 统计欧洲区域sku亚马逊和ebay平台欧洲各站点（德法西意）的刊登情况
    # amazon_temp = df_amazon_listing_temp_2[df_amazon_listing_temp_2['area'] == '欧洲']
    #
    amazon_temp = df_amazon_listing_temp_2.loc[df_amazon_listing_temp_2['area'].isin(['欧洲'])]  # 新增墨西哥
    amazon_temp.drop(['area'], axis=1, inplace=True)

    amazon_temp = amazon_temp.pivot_table(index='new_sku', columns='site').reset_index()

    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_': 'new_sku'}, inplace=True)
    amazon_temp['area'] = '欧洲'
    print('打印amazon站点：')
    print(amazon_temp.info())
    df_sku_listing_site = pd.merge(df_sku_listing_site,
                                   amazon_temp[['new_sku','area', 'amazon_de', 'amazon_fr', 'amazon_es', 'amazon_it']],
                                   how='left', on=['new_sku','area'])

    ebay_temp = df_ebay_listing_temp_2[df_ebay_listing_temp_2['area'] == '欧洲']
    ebay_temp.drop(['area'], axis=1, inplace=True)

    ebay_temp = ebay_temp.pivot_table(index='new_sku', columns='site').reset_index()
    ebay_temp.columns = ebay_temp.columns.droplevel(0)
    ebay_temp.columns = 'ebay_' + ebay_temp.columns
    ebay_temp.rename(columns={'ebay_': 'new_sku'}, inplace=True)
    print('打印ebay站点：')
    print(ebay_temp.info())

    df_sku_listing_site = pd.merge(df_sku_listing_site, ebay_temp[['new_sku', 'ebay_de']], how='left', on=['new_sku'])
    df_sku_listing_site.fillna(0, inplace=True)
    #

    df_sku_listing_site_test = listing_num_section(df_sku_listing_site)
    # 侵权违禁
    df_sku_listing_site_test = get_ban_info(df_sku_listing_site_test)


    # 补充一级二级三级类目
    df = get_line(df_sku_listing_site_test)
    # df_sku_listing_site_test = df_sku_listing_site_test[df_sku_listing_site_test['is_tt_sku'] == 1]
    date_today = time.strftime('%m%d')
    save_df(df, f'YM-TT海外仓SKU各平台刊登情况{date_today}', file_type='xlsx')
    print('done!')
    # df_sku_listing_site_test.to_excel('df_sku_listing_site.xlsx', index=0)

def get_ym_sku():
    """ 供应商货盘sku """
    df = pd.read_excel('F://Ding_workspace//供应商货盘SKU链接数量统计-20260324-给Roger.xlsx', dtype={'公司SKU':str})
    df_1 = df[['公司SKU','仓库所在国家']]
    df_1.columns = ['sku','site']
    df_1['develop_source_name'] = '供应商货盘_YMSKU'
    # df_2 = df[['YB-SKU','仓库所在国家']]
    # df_2.columns = ['sku', 'site']
    # df_2['develop_source_name'] = '供应商货盘_YBSKU'

    # df = pd.concat([df_1, df_2])
    df = df_1[~df_1['sku'].isna()]

    return df

if __name__ == '__main__':
    # main()
    main_ym()
    # temu_group()
    # temp()
    # get_temu_listing_num()
