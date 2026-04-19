##
# import fetch_data as fd
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from clickhouse_sqlalchemy import make_session
import pymysql
import time
import warnings
warnings.filterwarnings("ignore")
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck

##
sql = '''
SELECT  
    sku, charge_currency, cargo_type, ya.warehouse_code,warehouse_id, warehouse_name,
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
        else '美国仓'
    end as warehouse,
    warehouse_stock, inventory_age, charge_total_price
FROM yb_datacenter.yb_oversea_sku_age ya
LEFT JOIN (SELECT warehouse_code, warehouse_id, warehouse_name FROM yb_datacenter.v_warehouse_erp) ve
ON ya.warehouse_code = ve.warehouse_code
WHERE date  = formatDateTime(subtractDays(now(),2), '%Y-%m-%d') and status in (0,1)
ORDER BY sku, warehouse_id, inventory_age DESC
LIMIT 1 BY sku, warehouse_id
'''
conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
df_stock_age = conn_ck.ck_select_to_df(sql)
##
sql = """
SELECT ys.*, vs.warehouse, vs.warehouse_code, vs.warehouse_name, vs.available_stock
FROM (
    SELECT sku, warehouse_id, date_id
    FROM yb_datacenter.yb_stock
    WHERE date_id >= 20220901 and available_stock > 0 and warehouse_id in (
        SELECT id
        FROM yb_datacenter.yb_warehouse
        WHERE type IN ('third', 'overseas')
        )
    ORDER BY date_id DESC
    LIMIT 1 BY sku, warehouse_id
) ys
LEFT JOIN (
    SELECT sku, warehouse_code, warehouse_name, warehouse_id,  warehouse, available_stock
    FROM yb_datacenter.v_oversea_stock
) vs
ON ys.sku = vs.sku and ys.warehouse_id = vs.warehouse_id
settings max_memory_usage = 20000000000
"""
df_stock = conn_ck.ck_select_to_df(sql)
##
df_oversea_sku = pd.merge(df_stock, df_stock_age[['sku', 'warehouse_id', 'inventory_age']], how='left', on=['sku', 'warehouse_id'])
df_oversea_sku = df_stock.copy()
# 筛选常见仓库
df_oversea_sku_final = df_oversea_sku[~df_oversea_sku['warehouse'].isna()]
df_oversea_sku_final['area'] = np.where(df_oversea_sku_final['warehouse'].str.contains('德国|法国|西班牙|意大利'), '欧洲', df_oversea_sku_final['warehouse'].str[:-1])
##
# 暂不统计墨西哥、加拿大、日本仓的数据
df_sku_listing_new = df_oversea_sku_final[~df_oversea_sku_final['area'].isin(['加拿大', '日本'])]
df_sku_listing_new['site'] = df_sku_listing_new['warehouse'].replace({'德国仓':'de', '美国仓':'us', '澳洲仓':'au', '墨西哥仓':'mx','英国仓':'uk', '西班牙仓':'es', '法国仓':'fr','乌拉圭仓':'br','俄罗斯仓':'ru', '意大利仓':'it'})
##
# 处理仓标数据
df_sku_listing_new['sku'] = df_sku_listing_new['sku'].str.strip()

df_sku_listing_new['new_sku'] = np.where(df_sku_listing_new['sku'].str[:3].isin(['US-','GB-','DE-','FR-','ES-','IT-','AU-']), df_sku_listing_new['sku'].str[3:], df_sku_listing_new['sku'])
df_sku_listing_new['new_sku'] = np.where(df_sku_listing_new['sku'].str[-2:].isin(['HW','DE','US','AU']), df_sku_listing_new['sku'].str[:-2], df_sku_listing_new['new_sku'])
##
sku_list = tuple(df_sku_listing_new['sku'].unique())
##
len(sku_list)
###############
# AMAZON海外仓链接
df_amazon_listing = pd.DataFrame()
for i in range(int(len(sku_list)/2000)+1):
    sku_temp = sku_list[i*2000:(i+1)*2000]
    sql = """ 
    with listing_table as (
        select distinct account_id, seller_sku 
        from yibai_product_kd_sync.yibai_amazon_sku_map 
        where deliver_mode=2 and sku in {}
    )
    
    SELECT sku, platform, site, count(1) as amazon_listing_num
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
            from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
            where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
        ) a 
        inner join ( 
            select account_id, sku, seller_sku 
            from yibai_product_kd_sync.yibai_amazon_sku_map 
            where deliver_mode=2 
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
            and account_type = 1 and  main_code='YB_ZT_FP'
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
            where  (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
        ) f
        on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
        order by a.create_time desc 
        limit 1 by sku, site, asin
    ) a
    GROUP BY sku, platform, site
    settings max_memory_usage = 20000000000
    settings max_query_size=100000000
    """.format(sku_temp)
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_amazon_temp= conn_ck.ck_select_to_df(sql)
    df_amazon_listing = pd.concat([df_amazon_temp, df_amazon_listing])
##
# site_table
sql = """

SELECT site, site1, area as area0
FROM domestic_warehouse_clear.site_table
"""
conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
df_site = conn_ck.ck_select_to_df(sql)
df_site['area'] = np.where(df_site['site1'].str.contains('德国|法国|西班|意大利|荷兰|瑞士|波兰|比利|土耳其'), '欧洲', df_site['site1'])
df_amazon_listing_temp = pd.merge(df_amazon_listing, df_site[['site', 'area']], how='left', on='site')

# es 不在site_table中，单独处理
df_amazon_listing_temp['area'] = np.where(df_amazon_listing_temp['site']=='es', '欧洲', df_amazon_listing_temp['area'])

# 处理仓标数据
df_amazon_listing_temp = df_amazon_listing_temp
df_amazon_listing_temp['new_sku'] = np.where(df_amazon_listing_temp['sku'].str[:3].isin(['US-','GB-','DE-','FR-','ES-','IT-','AU-']), df_amazon_listing_temp['sku'].str[3:], df_amazon_listing_temp['sku'])
df_amazon_listing_temp['new_sku'] = np.where(df_amazon_listing_temp['sku'].str[-2:].isin(['HW','DE','US','AU']), df_amazon_listing_temp['sku'].str[:-2], df_amazon_listing_temp['new_sku'])
##
# 深度分析报表暂不处理仓标
df_amazon_listing_temp_2 = df_amazon_listing_temp.groupby(['sku', 'site', 'area'])['amazon_listing_num'].sum().reset_index()
##
# ebay
sql = """
    select 
        a.account_id, a.itemid as item_id, a.sku, a.system_sku, a.sell_sku, a.site,
    CASE
        WHEN a.site='Germany' then 'DE'
        WHEN a.site='Spain' then 'ES'
        WHEN a.site='Australia' then 'AU'
        WHEN a.site='France' then 'FR'
        WHEN a.site='Canada' then 'CA'
        WHEN a.site='Italy' then 'IT'
        WHEN a.site='eBayMotors' then 'US'
        ELSE a.site 
        END as country, a.seller_user, a.product_line, a.listing_status, a.start_price, 
        b.shipping_service_cost as shipping_fee,a.start_price+b.shipping_service_cost as online_price,
    CASE 
        WHEN e.name in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' 
        ELSE e.name 
        end AS name, formatDateTime(NOW(),'%%Y-%%m-%%d') as DATE
    from yibai_product_kd_sync.yibai_ebay_online_listing a
    left join 
        yibai_product_kd_sync.yibai_ebay_online_listing_shipping b on a.itemid=b.item_id
    left join 
        yibai_product_kd_sync.yibai_ebay_location_map_warehouse d 
    on a.location=d.location
    left join 
        yibai_product_kd_sync.yibai_ebay_warehouse_warehouse_category e 
    on d.warehouse_category_id=e.id
    where 
        a.listing_status='Active' and a.listing_type <> 'Chinese' and b.shipping_status=1 and b.shipping_service_priority=1 and 
        e.name  not in ('中国仓','临时仓') and a.variation_multi=0 and sku in {}
    settings max_memory_usage = 20000000000
    """.format(sku_list)
df_ebay_listing = fd.fetch_ck(sql, 78, 'yibai_product_kd_sync')
df_ebay_listing.columns = [i.split('.')[-1] for i in df_ebay_listing.columns.to_list()]

# ebay多属性SKU
sql = """
select 
    b.account_id, a.item_id, a.sku, b.system_sku, a.sell_sku, site,
CASE
    WHEN site='Germany' then 'DE'
    WHEN site='Spain' then 'ES'
    WHEN site='Australia' then 'AU'
    WHEN site='France' then 'FR'
    WHEN site='Canada' then 'CA'
    WHEN site='Italy' then 'IT'
    WHEN site='eBayMotors' then 'US'
    ELSE site 
    END as country, b.seller_user, b.product_line, b.listing_status, a.start_price,
    c.shipping_service_cost as shipping_fee,a.start_price+c.shipping_service_cost as online_price,
CASE WHEN e.name in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' 
    ELSE e.name 
    end AS name, formatDateTime(NOW(),'%%Y-%%m-%%d') as DATE
from 
    yibai_ebay_online_listing_variation a
left join (
    SELECT account_id, itemid, system_sku, seller_user, listing_status, listing_type, location, product_line, site
    FROM yibai_ebay_online_listing 
    WHERE itemid in (SELECT distinct item_id FROM yibai_ebay_online_listing_variation)
    ) b on a.item_id=b.itemid
left join 
    yibai_ebay_online_listing_shipping c on a.item_id=c.item_id
left join 
    yibai_product_kd_sync.yibai_ebay_location_map_warehouse d on b.location=d.location
left join 
    yibai_product_kd_sync.yibai_ebay_warehouse_warehouse_category e on d.warehouse_category_id=e.id
where 
    b.listing_status='Active' and b.listing_type <> 'Chinese' and c.shipping_status=1 and shipping_service_priority=1 
    and e.name not in ('中国仓','临时仓') and sku in {}
    settings max_memory_usage = 40000000000
""".format(sku_list)
df_ebay_listing_vary = fd.fetch_ck(sql, 78, 'yibai_product_kd_sync')
df_ebay_listing_vary.columns = [i.split('.')[-1] for i in df_ebay_listing.columns.to_list()]

# 单属性SKU与多属性SKU的链接数据合并
df_ebay_listing_all = pd.concat([df_ebay_listing, df_ebay_listing_vary])

# 按 SKU + country 聚合计算 item的数量
df_ebay_listing_all = df_ebay_listing_all.drop_duplicates(subset=['account_id','item_id','sku'])
df_ebay_listing_temp = df_ebay_listing_all.groupby(['sku', 'country'])['item_id'].count().reset_index()
df_ebay_listing_temp.rename(columns={'item_id':'ebay_listing_num', 'country':'site'}, inplace=True)
df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].str.lower()
df_ebay_listing_temp = pd.merge(df_ebay_listing_temp, df_site[['site', 'area']], how='left', on='site')
# df_ebay_listing_temp = df_ebay_listing_temp.groupby(['sku', 'area'])['ebay_listing_num'].sum().reset_index()

# 处理仓标数据
df_ebay_listing_temp_2 = df_ebay_listing_temp
df_ebay_listing_temp_2['new_sku'] = np.where(df_ebay_listing_temp_2['sku'].str[:3].isin(['US-','GB-','DE-','FR-','ES-','IT-','AU-']), df_ebay_listing_temp_2['sku'].str[3:], df_ebay_listing_temp_2['sku'])
df_ebay_listing_temp_2['new_sku'] = np.where(df_ebay_listing_temp_2['sku'].str[-2:].isin(['HW','DE','US','AU']), df_ebay_listing_temp_2['sku'].str[:-2], df_ebay_listing_temp_2['new_sku'])

df_ebay_listing_temp_2 = df_ebay_listing_temp_2.groupby(['new_sku', 'site', 'area'])['ebay_listing_num'].sum().reset_index()

##
# cd
sql = """
    SELECT
        a.account_id,
        b.short_name AS account_name,
        'CDISCOUNT' as platform,
        'FR' AS site,
        a.erp_sku as sku,
        a.product_id as product_id,
        a.seller_sku,
        best_shipping_charges,
        a.price + best_shipping_charges AS online_price,
        a.offer_state 
    FROM  yibai_product_kd_sync.yibai_cdiscount_listing a
    LEFT JOIN yibai_system_kd_sync.yibai_cdiscount_account b ON a.account_id = b.id 
    WHERE
        a.offer_state = 'Active' 
        and a.used_status = 1
        and b.token_status=1
        AND warehouse <> '中国' and sku in {}
        AND isfbc = 0 and used_status <> 2
      """.format(sku_list)
df_cd_listing = fd.fetch_ck(sql, 78, 'yibai_product_kd_sync')
# 按 SKU + country 聚合计算 item的数量
df_cd_listing_temp = df_cd_listing.drop_duplicates(subset=['product_id','sku'])

df_cd_listing_temp = df_cd_listing_temp.groupby(['sku', 'site'])['product_id'].count().reset_index()
df_cd_listing_temp.rename(columns={'product_id':'cd_listing_num'}, inplace=True)
df_cd_listing_temp['site'] = df_cd_listing_temp['site'].str.lower()
df_cd_listing_temp = pd.merge(df_cd_listing_temp, df_site[['site', 'area']], how='left', on='site')
# df_cd_listing_temp = df_cd_listing_temp.groupby(['sku', 'area'])['cd_listing_num'].sum().reset_index()
# 仓标处理
df_cd_listing_temp_2 = df_cd_listing_temp.copy()
df_cd_listing_temp_2['new_sku'] = np.where(df_cd_listing_temp_2['sku'].str[:3].isin(['US-','GB-','DE-','FR-','ES-','IT-','AU-']), df_cd_listing_temp_2['sku'].str[3:], df_cd_listing_temp_2['sku'])
df_cd_listing_temp_2['new_sku'] = np.where(df_cd_listing_temp_2['sku'].str[-2:].isin(['HW','DE','US','AU']), df_cd_listing_temp_2['sku'].str[:-2], df_cd_listing_temp_2['new_sku'])

df_cd_listing_temp_2 = df_cd_listing_temp_2.groupby(['new_sku', 'site', 'area'])['cd_listing_num'].sum().reset_index()
# CD平台链接国家都为FR。但是对于德国仓有库存的SKU，统计时也应算上FR的链接数量
df_cd_listing_temp_de = df_cd_listing_temp_2.copy()
df_cd_listing_temp_de['site'] = 'de'
df_cd_listing_temp_2 = pd.concat([df_cd_listing_temp_de, df_cd_listing_temp_2])


##
# Walmart
sql = """
    SELECT 
        a.erp_id as account_id,b.short_name as short_name,
        case 
            when b.site='us_dsv' then 'us' 
            else b.site 
        end as site, 
        a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price
    FROM (
        select * from yibai_sale_center_listing_sync.yibai_walmart_report 
        where upper(publish_status)='PUBLISHED' and sku in {}
        order by updated_unix desc limit 1 by erp_id,seller_sku
    ) a
    left join yibai_system_kd_sync.yibai_walmart_account b 
    on a.erp_id=b.id
    WHERE b.warehouse_delivery<>4 and b.status=1 and b.site<>'us_dsv'
""".format(sku_list)
df_walmart_listing = fd.fetch_ck(sql, 78, 'yibai_sale_center_listing_sync')

df_walmart_listing_temp = df_walmart_listing.drop_duplicates(subset=['item_id','sku'])

df_walmart_listing_temp = df_walmart_listing_temp.groupby(['sku', 'site'])['item_id'].count().reset_index()
df_walmart_listing_temp.rename(columns={'item_id':'walmart_listing_num'}, inplace=True)
df_walmart_listing_temp['site'] = df_walmart_listing_temp['site'].str.lower()
df_walmart_listing_temp = pd.merge(df_walmart_listing_temp, df_site[['site', 'area']], how='left', on='site')
# df_walmart_listing_temp = df_walmart_listing_temp.groupby(['sku', 'area'])['walmart_listing_num'].sum().reset_index()

df_walmart_listing_temp_2 = df_walmart_listing_temp
df_walmart_listing_temp_2['new_sku'] = np.where(df_walmart_listing_temp_2['sku'].str[:3].isin(['US-','GB-','DE-','FR-','ES-','IT-','AU-']), df_walmart_listing_temp_2['sku'].str[3:], df_walmart_listing_temp_2['sku'])
df_walmart_listing_temp_2['new_sku'] = np.where(df_walmart_listing_temp_2['sku'].str[-2:].isin(['HW','DE','US','AU']), df_walmart_listing_temp_2['sku'].str[:-2], df_walmart_listing_temp_2['new_sku'])

df_walmart_listing_temp_2 = df_walmart_listing_temp_2.groupby(['new_sku', 'site', 'area'])['walmart_listing_num'].sum().reset_index()

##
# wish
sql = """
SELECT sku, lower(country_code) as site, count(1) as wish_listing_num
FROM yibai_wish_over_sea_listing
WHERE sku in {}
GROUP BY sku, site
""".format(sku_list)
df_wish_listing = fd.fetch_ck(sql, 78, 'yibai_oversea')

sql = """
SELECT account_id, sku, lower(country_code) as site, seller_sku, product_id, country_code, `直发当前价格` as online_price
FROM yibai_wish_over_sea_listing
WHERE sku in {}
""".format(sku_list)
df_wish_listing_base = fd.fetch_ck(sql, 78, 'yibai_oversea')

# 将 we_listing(已勾选的国家）拆分成行
df_wish_listing_code = df_wish_listing_code.drop(['we_listing'], axis=1).join(df_wish_listing_code['we_listing'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('we_listing_code'))

df_wish_listing_code.rename(columns={'we_listing_code':'country_code'}, inplace=True)
df_wish_listing_code['country_code'] = df_wish_listing_code['country_code'].replace('GB','UK')
df_wish_listing_code.drop_duplicates(inplace=True)

# 按 SKU + site 维度统计链接数量
df_wish_listing_temp = pd.merge(df_wish_listing_base, df_wish_listing_code, how='inner', on=['account_id', 'sku', 'product_id', 'seller_sku', 'country_code'])

df_wish_listing_temp = df_wish_listing_temp.drop_duplicates()
df_wish_listing_temp = df_wish_listing_temp.groupby(['sku', 'site'])['seller_sku'].count().reset_index()
df_wish_listing_temp.rename(columns={'seller_sku':'wish_listing_num'}, inplace=True)
df_wish_listing_temp = pd.merge(df_wish_listing_temp, df_site[['site', 'area']], how='left', on='site')
# df_wish_listing_temp = df_wish_listing_temp.groupby(['sku', 'area'])['wish_listing_num'].sum().reset_index()

# 仓标的处理
df_wish_listing_temp_2 = df_wish_listing_temp
df_wish_listing_temp_2['new_sku'] = np.where(df_wish_listing_temp_2['sku'].str[:3].isin(['US-','GB-','DE-','FR-','ES-','IT-','AU-']), df_wish_listing_temp_2['sku'].str[3:], df_wish_listing_temp_2['sku'])
df_wish_listing_temp_2['new_sku'] = np.where(df_wish_listing_temp_2['sku'].str[-2:].isin(['HW','DE','US','AU']), df_wish_listing_temp_2['sku'].str[:-2], df_wish_listing_temp_2['new_sku'])

df_wish_listing_temp_2 = df_wish_listing_temp_2.groupby(['new_sku', 'site', 'area'])['wish_listing_num'].sum().reset_index()


#######
# 合并处理
# 按 sku + 区域合并
# df_sku_listing = pd.merge(df_oversea_sku_final, df_amazon_listing_temp[['sku', 'amazon_listing_num', 'area']], how='left', on=['sku', 'area'])
# df_sku_listing = pd.merge(df_sku_listing, df_ebay_listing_temp, how='left', on=['sku', 'area'])
# df_sku_listing = pd.merge(df_sku_listing, df_cd_listing_temp, how='left', on=['sku', 'area'])
# df_sku_listing = pd.merge(df_sku_listing, df_walmart_listing_temp, how='left', on=['sku', 'area'])
# df_sku_listing = pd.merge(df_sku_listing, df_wish_listing_temp, how='left', on=['sku', 'area'])

# 按 sku + site 合并
# df_sku_listing_site = pd.merge(df_sku_listing_new, df_amazon_listing_temp_2[['new_sku', 'amazon_listing_num', 'site']], how='left', on=['new_sku', 'site'])
df_sku_listing_site = pd.merge(df_sku_listing_new, df_amazon_listing_temp_2[['sku', 'amazon_listing_num', 'site']], how='left', on=['sku', 'site'])
# df_sku_listing_site = pd.merge(df_sku_listing_site, df_ebay_listing_temp_2[['new_sku', 'ebay_listing_num', 'site']], how='left', on=['new_sku', 'site'])
# df_sku_listing_site = pd.merge(df_sku_listing_site, df_cd_listing_temp_2[['new_sku', 'cd_listing_num', 'site']], how='left', on=['new_sku', 'site'])
# df_sku_listing_site = pd.merge(df_sku_listing_site, df_walmart_listing_temp_2[['new_sku', 'walmart_listing_num', 'site']], how='left', on=['new_sku', 'site'])
# df_sku_listing_site = pd.merge(df_sku_listing_site, df_wish_listing_temp_2[['new_sku', 'wish_listing_num', 'site']], how='left', on=['new_sku', 'site'])
# 统计欧洲区域sku亚马逊和ebay平台欧洲各站点（德法西意）的刊登情况
amazon_temp = df_amazon_listing_temp_2[df_amazon_listing_temp_2['area'] == '欧洲']
amazon_temp.drop(['area'], axis=1, inplace=True)
##
amazon_temp = amazon_temp.pivot_table(index='new_sku',columns='site').reset_index()

amazon_temp.columns = amazon_temp.columns.droplevel(0)
amazon_temp.columns = 'amazon_' + amazon_temp.columns
amazon_temp.rename(columns={'amazon_':'new_sku'}, inplace=True)
df_sku_listing_site = pd.merge(df_sku_listing_site, amazon_temp[['new_sku', 'amazon_de', 'amazon_fr', 'amazon_es', 'amazon_it']], how='left', on=['new_sku'])

ebay_temp = df_ebay_listing_temp_2[df_ebay_listing_temp_2['area'] == '欧洲']
ebay_temp.drop(['area'], axis=1, inplace=True)

ebay_temp = ebay_temp.pivot_table(index='new_sku',columns='site').reset_index()
ebay_temp.columns = ebay_temp.columns.droplevel(0)
ebay_temp.columns = 'ebay_' + ebay_temp.columns
ebay_temp.rename(columns={'ebay_':'new_sku'}, inplace=True)

df_sku_listing_site = pd.merge(df_sku_listing_site, ebay_temp[['new_sku', 'ebay_de', 'ebay_fr']], how='left', on=['new_sku'])
##
def listing_num_section(df):
    """
    对各平台链接数量添加分段列
    """
    # 分段值可根据实际数量分布调整
    for i in df.columns[10:]:
        new_column = i + '_section'
        df[new_column] = pd.cut(df[i], bins=[-1, 0, 1, 3, 5, 10, 20, 40, 60, 10000],
                                 labels=['A:0', 'B:1','C:(1,3]','D:(3,5]','E:(5,10]','F:(10,20]','G:(20,40]','H:(40,60]','I:60+'])
    return df
df_sku_listing_site['amazon_listing_num'] = df_sku_listing_site['amazon_listing_num'].fillna(0)
df_sku_listing_site_test = listing_num_section(df_sku_listing_site)
##
# 匹配sku情况
date_today = time.strftime('%Y-%m-%d')
sql = f"""
    SELECT
        date_id, sku, best_warehouse_name, warehouse, new_price, available_stock,available_stock_money, warehouse_stock, overage_level, 
        age_90_plus, age_120_plus, age_150_plus, age_180_plus, age_270_plus,age_360_plus, charge_total_price_rmb,day_sales,
        estimated_sales_days,overage_esd, age_90_plus-age_120_plus as 90_120, age_120_plus-age_150_plus as 120_150,
        age_150_plus-age_180_plus 150_180, age_180_plus-age_270_plus 180_270, age_270_plus-age_360_plus 270_360,
        age_90_plus*new_price age_90_money, age_120_plus*new_price age_120_money, age_150_plus*new_price age_150_money,
        age_180_plus*new_price age_180_money, age_270_plus*new_price age_270_money, age_360_plus*new_price age_360_money
    FROM over_sea.dwm_sku_temp_info
    WHERE date_id = '{date_today}' and available_stock > 0
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_stock = conn.read_sql(sql)
##
df_sku_listing_site_test = pd.merge(df_sku_listing_site_test, df_stock, how='left', on=['sku', 'warehouse'])
##
df_sku_listing_site_test.to_excel('df_sku_listing.xlsx', index=0)
##