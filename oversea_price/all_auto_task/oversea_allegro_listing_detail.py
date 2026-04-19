##
import time, datetime
import pandas as pd
import numpy as np
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_listing_detail_2023 import write_to_ck
from requests.auth import HTTPBasicAuth
import requests

##
def get_listing():
    """ allegro海外仓链接信息 """
    date_today = time.strftime('%Y-%m-%d')
    sql = """
    select 
        a.account_id as account_id, b.erp_id, b.short_name as account_name,  
        a.offer_id as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
        a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price, selling_mode_currency,
        toFloat64(a.delivery_amount) as delivery_amount, a.location1 as location, b.group_name as group_name
    FROM (
        SELECT 
            *, replace(JSONExtractRaw(location, 'countryCode'), '"', '') as location1
        FROM yibai_sale_center_listing_sync.yibai_allegro_listing
        WHERE 
            selling_mode_price > 0 and status in (1,4)
            and location1 not in ('CN', 'GB')
        ) a 
    INNER JOIN (
        SELECT b.id, b.erp_id,  b.short_name, c.group_name
        FROM yibai_sale_center_system_sync.yibai_system_account b
        inner join (
            select account_id,splitByString('->', oa_group_name)[-1] as group_name
            from yibai_account_manage_sync.yibai_allegro_account
            where account_type = 1 and is_platform_account != 2 and account_num_name != 'DMSAllegro'
        ) c
        on b.account_id=c.account_id
        WHERE platform_code  = 'ALLEGRO' 
    ) b 
    on a.account_id=b.id

    """
    conn_kd = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df = conn_kd.ck_select_to_df(sql)
    df = df.drop_duplicates()
    df['date_id'] = date_today
    df['country'] = 'PL'

    write_to_ck(df, 'yibai_oversea_allegro_listing')
    # df.to_excel('F://Desktop//df_allegro.xlsx', index=0)


def get_listing_all_site():
    """ allegro海外仓链接信息, 包含不同站点的价格信息 """
    date_today = time.strftime('%Y-%m-%d')
    sql = """
    with listing_temp as (
        select 
            a.account_id as account_id, b.erp_id, b.short_name as account_name,  
            toInt64(a.offer_id) as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
            a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price, selling_mode_currency,
            toFloat64(a.delivery_amount) as delivery_amount, a.location1 as location, b.group_name as group_name,
            'PL' as country
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
    
    SELECT account_id, erp_id, account_name, toInt64(offer_id) offer_id, seller_sku, sku, product_id,
    toFloat64(online_price) as online_price, selling_mode_currency,
    toFloat64(delivery_amount) as delivery_amount,location,group_name, country
    FROM listing_temp 
    
    union all
     
    SELECT a.account_id as account_id, erp_id, account_name, toInt64(a.offer_id) as offer_id,a.seller_sku as seller_sku,
    a.sku as sku, a.product_id as product_id, b.amount as online_price, b.currency selling_mode_currency,
    round(toFloat64(a.delivery_amount) * d.`汇率`/c.`汇率`, 2) as delivery_amount,
    a.location as location, group_name, b.country as country
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
    conn_kd = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df = conn_kd.ck_select_to_df(sql)
    df = df.drop_duplicates()
    df['date_id'] = date_today
    # allegro海外仓免运费
    df['delivery_amount'] = 0
    df['offer_id'] = df['offer_id'].astype(str)
    print(df[['sku','offer_id']].head(4))
    print(df.info())

    write_to_ck(df, 'yibai_oversea_allegro_all_site')
    # df.to_excel('F://Desktop//df_allegro_all_site.xlsx', index=0)

def get_sku():
    """ 筛选海外仓sku """
    sql = """
        SELECT sku, warehouse
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-05-01')
        and warehouse in ('泰国仓', '越南仓', '马来西亚仓', '印度尼西亚仓', '菲律宾仓')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)

    print(df_sku['warehouse'].value_counts())

    return df_sku

def get_shopee_listing():
    """ 获取shopee海外仓链接 """
    # 取海外仓sku
    df_sku = get_sku()
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
    print(df_shopee_account.info())
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
    print(df_shopee_account.info())
    account_2 = tuple(df_shopee_account['id'].unique())

    df_account = pd.concat([df_shopee_account[['id', 'account_id','account_name']], yibai_shopee_account[['id', 'account_id','account_name']]])
    df_account = df_account.rename(columns={'account_name':'short_name'})

    account_list = tuple(set(account_1).union(set(account_2)))

    sql = f"""
         SELECT item_id, account_id id, price, status_online, sku, country_code, reserve_price, is_mulit, parent_sku
         FROM yibai_sale_center_listing_sync.yibai_shopee_listing
         WHERE is_delete = 0 and account_id in {account_list}
         -- and status_online not in ('SELLER_DELETE','SHOPEE_DELETE')
         and status_online in ('NORMAL')
         and sku in {sku_list}
     """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    yibai_shopee_listing = conn_ck.ck_select_to_df(sql)

    yibai_shopee_listing = pd.merge(yibai_shopee_listing, df_account, how='left', on=['id'])
    yibai_shopee_listing.drop('id', axis=1, inplace=True)
    yibai_shopee_listing['date_id'] = time.strftime('%Y-%m-%d')
    yibai_shopee_listing = yibai_shopee_listing.rename(columns={'price':'online_price','country_code':'country'})
    col = ['online_price','reserve_price']
    yibai_shopee_listing[col] = yibai_shopee_listing[col].astype(float)
    yibai_shopee_listing['item_id'] = yibai_shopee_listing['item_id'].astype(str)

    print(yibai_shopee_listing.info())

    # df_temp = yibai_shopee_listing.groupby('country_code').count().reset_index()
    # print(df_temp)

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(yibai_shopee_listing, 'yibai_shopee_oversea_listing_price', if_exists='replace')
    write_to_ck(yibai_shopee_listing, 'yibai_oversea_shopee_listing')

    # yibai_shopee_listing.to_excel('F://Desktop//df_shopee_listing.xlsx', index=0)

def get_lazada_listing():
    """ 获取lazada海外仓链接 """
    # 取海外仓sku
    df_sku = get_sku()
    sku_list = tuple(df_sku['sku'].unique())

    sql = """

        SELECT account_id, account_s_name, account_type
        FROM yibai_account_manage_sync.yibai_lazada_account a
        WHERE account_mold = 1 and account_status = 10 and account_type != 20
        and is_yibai = 1
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_lazada_account = conn_ck.ck_select_to_df(sql)
    print(df_lazada_account.info())
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
    print(df_lazada_account.head(4))

    sql = f"""
        SELECT item_id, account_id, seller_sku, price, reserve_price, status_online, sku, is_mulit,
        parent_sku, country_code
        FROM yibai_sale_center_listing_sync.yibai_lazada_listing
        WHERE is_delete = 0 and account_id in {account_list} and sku in {sku_list}
        -- and status_online not in ('Deleted')
        and status_online in ('Active')
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    yibai_lazada_listing = conn_ck.ck_select_to_df(sql)
    print(yibai_lazada_listing.info())

    df = pd.merge(yibai_lazada_listing, df_lazada_account[['id','short_name']],
                                    how='left', left_on=['account_id'], right_on='id')
    df.drop('id', axis=1, inplace=True)
    df['date_id'] = time.strftime('%Y-%m-%d')
    df = df.rename(columns={'price':'online_price','country_code':'country'})
    col = ['online_price','reserve_price']
    df[col] = df[col].astype(float)
    df['item_id'] = df['item_id'].astype(str)

    print(df.info())

    # df_temp = yibai_shopee_listing.groupby('country_code').count().reset_index()
    # print(df_temp)

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(yibai_shopee_listing, 'yibai_shopee_oversea_listing_price', if_exists='replace')
    write_to_ck(df, 'yibai_oversea_lazada_listing')

    get_shopee_listing()
    # df.to_excel('F://Desktop//df_lazada_listing.xlsx', index=0)

if __name__ == '__main__':
    # get_listing()
    # get_listing_all_site()

    get_shopee_listing()
    # get_lazada_listing()
