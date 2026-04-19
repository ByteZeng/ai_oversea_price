"""
定价比计算程序：
1、sku维度定价比计算，并上传中台
2、listing维度定价比计算，并上传中台
"""
##
import time
import traceback
import warnings
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from all_auto_task.dingding import send_msg
from pulic_func.base_api.base_function import mysql_escape_list, roundup, mysql_escape
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from all_auto_task.scripts_ck_client import CkClient
import datetime
import pandas as pd
import numpy as np
from pulic_func.base_api.all_freight_interface import freight_interface_fu
from pulic_func.base_api.adjust_price_function_amazon import shuilv,fanou_fanmei
from pulic_func.price_adjust_web_service.AMAZON_FBA_dingjia import fba_ding_jia_biao, aut, tou_cheng_ky, tou_cheng_hy, \
    tou_cheng_tl, tou_cheng_kh
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, astype_int, \
    str_int, mobanxiuzheng, jisuanfenlei, chaxun_chengben, tar_profit, yunfei_biangeng, sku_and_num_split, \
    DMS_getCalculation_api_fbm
from pulic_func.price_adjust_web_service.make_price import amazon_fbm_para
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee
from pulic_func.adjust_price_base_api.FBA_fee import tou_cheng_dingjiabi, tou_cheng_api
warnings.filterwarnings('ignore')

##
def merge_four_dim_diff(df, df_diff, col_names_list):
    """
    多维度逐级匹配差值，每个级别都使用之前所有级别的信息进行匹配，匹配不到的将最高层级字段赋值 'other' 或 -1。
    """
    print(f'四维差值匹配...')
    for i in range(len(col_names_list)):
        col_names = col_names_list[:(i + 1)]
        df_diff_temp = df_diff[col_names].drop_duplicates().reset_index(drop=True)
        df_diff_temp['is_filter'] = 1
        df_temp = df.merge(df_diff_temp, on=col_names, how='left')
        df_temp['is_filter'] = df_temp['is_filter'].fillna(0)
        df_1 = df_temp[df_temp['is_filter'] == 1]
        df_0 = df_temp[df_temp['is_filter'] == 0]
        if not df_0.empty:
            df_0[col_names[-1]] = 'other' if col_names[-1] in ('site', 'cost_range') else -1
        else:
            pass
        df = pd.concat([df_1, df_0], ignore_index=True).drop(['is_filter'], axis=1)

    df = df.merge(df_diff, on=col_names_list, how='left')
    print(f'四维差值匹配完成！')
    return df

def get_cost_range(df, df_diff):
    print(f'成本段匹配...')
    list_cost_range = df_diff['cost_range'].drop_duplicates()
    list_cost_range = list_cost_range[list_cost_range != 'other']
    list_cost_margin = pd.concat([list_cost_range.map(lambda x: x.split(',')[0]), list_cost_range.map(
        lambda x: x.split(',')[1])]).drop_duplicates().reset_index(drop=True).astype('int')
    list_cost_margin = list_cost_margin.values.tolist()
    list_cost_margin.remove(0)
    list_cost_margin.sort()

    df_output = pd.DataFrame()
    for i in range(len(list_cost_margin)):
        if i == 0:
            df_temp = df[df['cost'] <= list_cost_margin[i]]
            df_temp['cost_range'] = f'0,{list_cost_margin[i]}'
        else:
            df_temp = df[(df['cost'] <= list_cost_margin[i]) & (df['cost'] > list_cost_margin[i - 1])]
            df_temp['cost_range'] = f'{list_cost_margin[i - 1]},{list_cost_margin[i]}'
        df_output = df_temp if df_output.empty else pd.concat([df_output, df_temp], ignore_index=True)
        if i == len(list_cost_margin) - 1:
            df_temp = df[df['cost'] > list_cost_margin[i]]
            df_temp['cost_range'] = 'other'
            df_output = df_temp if df_output.empty else pd.concat([df_output, df_temp], ignore_index=True)
    print(f'成本段匹配完成！')
    return df_output

def get_sku_new():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 产品表里取所有sku（筛选在售 product_status = 9 国内仓属性 attr_value_id = 67）
    t1 = time.time()
    sql = f"""
        with sku_table2 as (
            SELECT sku, sum(purchase_on_way_count + stock) as stock
            FROM yb_datacenter.yb_stock
            WHERE 
                date_id = (SELECT max(date_id) FROM yb_datacenter.yb_stock)
                and toInt64(warehouse_id) in (SELECT id FROM yibai_logistics_tms_sync.yibai_warehouse where warehouse_type=1)
            GROUP BY sku
            HAVING stock > 0
        )

        select
            distinct sku
        from yibai_prod_base_sync.yibai_prod_sku a
        INNER JOIN (
            SELECT DISTINCT sku
            FROM yibai_prod_base_sync.yibai_prod_sku_select_attr
            where attr_value_id = 67
            ) b
        ON a.sku = b.sku
        where 
            (
                product_status = 9 
                or (product_status != 9 and sku in (select distinct sku from sku_table2))
            )
            and (sku not like '%%*%%' and sku not like '%%+%%')
        """
    df = conn_mx.ck_select_to_df(sql)
    t2 = time.time()
    # df = df.sample(10000)
    print(f'获取sku完成，共{len(df)}条！共耗时{t2 - t1:.2f}s')

    return df
def get_diff():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 四维差值 之后使用需要进行多次匹配来修正各个字段信息从而能够最终用所有字段进行匹配
    sql_diff = """
        select * from yibai_sale_center_listing_sync.yibai_listing_profit_config
        where is_del=0 and status=1 and platform_code='AMAZON'
    """
    df_diff = conn_mx.ck_select_to_df(sql_diff)
    sql_site_table = """
        select site2 as site_en, site1 as site from domestic_warehouse_clear.yibai_site_table_amazon
    """
    df_site_table = conn_mx.ck_select_to_df(sql_site_table)
    df_diff = df_diff.rename(columns={'site': 'site_en'}).merge(df_site_table, on='site_en', how='left')
    df_diff['site'] = df_diff.apply(lambda x: 'other' if x['site_en'] == 'other' else x['site'], axis=1)
    df_diff.dropna(inplace=True)
    return df_diff

def get_diff_new():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 四维差值 之后使用需要进行多次匹配来修正各个字段信息从而能够最终用所有字段进行匹配
    sql_diff = """
        select * from yibai_sale_center_listing_sync.yibai_listing_profit_config
        where is_del=0 and status=1 and platform_code='AMAZON'
    """
    df_diff = conn_mx.ck_select_to_df(sql_diff)
    sql_site_table = """
        select site2 as site_en, site1 as site from domestic_warehouse_clear.yibai_site_table_amazon
    """
    df_site_table = conn_mx.ck_select_to_df(sql_site_table)
    df_diff = df_diff.rename(columns={'site': 'site_en'}).merge(df_site_table, on='site_en', how='left')
    df_diff['站点'] = df_diff.apply(lambda x: 'other' if x['site_en'] == 'other' else x['site'], axis=1)
    df_diff.dropna(inplace=True)
    return df_diff

def merge_first_product_line(df):
    print(f'匹配第一产品线...')
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 一级产品线
    sql_line = f"""
                    select a.sku sku1,
                    b.path_name as `产品线路线` from yb_datacenter.yb_product a
                    left join yb_datacenter.yb_product_linelist b
                    on toInt32(a.product_linelist_id) = toInt32(b.id) 
                """
    df_line = conn_mx.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]

    sql_line_2 = f"""
                select distinct id first_product_line, 
                linelist_cn_name as `一级产品线` from yb_datacenter.yb_product_linelist
                where linelist_cn_name = path_name
            """
    df_line_2 = conn_mx.ck_select_to_df(sql_line_2)
    df_line = df_line.merge(df_line_2, on='一级产品线', how='left')
    df_line = df_line[['sku1', 'first_product_line']]

    # 拆分捆绑sku
    df['sku1'] = df['sku'].str.split('+', expand=True)[0].str.split('*', expand=True)[0]

    df = df.merge(df_line, on='sku1', how='left')
    df['first_product_line'] = df['first_product_line'].fillna(-1)
    df.drop('sku1', axis=1, inplace=True)
    print('匹配第一产品线完成！')
    return df


def filter_stock(df):
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    step = 10000
    df_1 = df['sku'].drop_duplicates().reset_index()
    list_group = [df_1[i:i + step] for i in range(0, len(df), step)]
    process_len = len(list_group)
    process_index = 1
    df1 = pd.DataFrame()
    for df_member in list_group:
        sql2 = f"""
        with listing as (
        select account_id, seller_sku
        from yibai_product_kd_sync.yibai_amazon_sku_map
        where sku in {tuple(df_member['sku'].values)}
        )
        select b.account_name account_name, b.sku sku, a.`在库库存` `在库库存`
        from 
        (
        select account_id, seller_sku, sku, account_name
        from yibai_product_kd_sync.yibai_amazon_sku_map
        where (account_id, seller_sku) in (select * from listing)
        ) b
        inner join 
        (
        SELECT account_id,sku as seller_sku,
        GREATEST(if(isNull(afn_fulfillable_quantity),0,afn_fulfillable_quantity), if(isNull(afn_fulfillable_quantity_local),0,afn_fulfillable_quantity_local)) as `在库库存`
        FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end 
        WHERE month=today() AND `condition`='New' and afn_listing_exists='Yes'
        and (account_id, seller_sku) in (select * from listing)
        ) a
        on a.account_id = b.account_id and a.seller_sku = b.seller_sku
        """
        df2 = conn_mx2.ck_select_to_df(sql2)
        df1 = df2 if df1.empty else pd.concat([df1, df2], ignore_index=True)
        print(f'匹配库存进度: {process_index}/{process_len}')
        process_index += 1
    if df1.empty:
        df['在库库存'] = 0
    else:
        df1['site'] = df1['account_name'].str.replace(r'[^\u4e00-\u9fa5]', '')
        df1.drop('account_name', axis=1, inplace=True)
        df1 = df1.groupby(['sku', 'site']).sum().reset_index()
        df = df.merge(df1, on=['sku', 'site'], how='left')
        df['在库库存'] = df['在库库存'].fillna(0).astype('int')
    df = df[((df['product_status'] != 9) & df['在库库存'] > 0) | (df['product_status'] == 9)]
    df.drop(['product_status', '在库库存'], axis=1, inplace=True)
    print('匹配库存完成！')
    return df


def add_kunbang_new():
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    sql = """
    select distinct `站点`,sku 
    from (
        select distinct c.site1 as `站点`,a.sku as sku
        from (
            select distinct account_id,sku 
            from yibai_product_kd_sync.yibai_amazon_sku_map 
            where sku like '%%*%%' or sku like '%%+%%'
        ) a 
        left join (
            select toInt64(id) as account_id,site from yibai_system_kd_sync.yibai_amazon_account
        ) b 
        on a.account_id=b.account_id
        left join domestic_warehouse_clear.yibai_site_table_amazon c 
        on b.site=c.site

        union all 

        select distinct c.site1 as `站点`,a.sku as sku
        from (
            select distinct account_id,sku 
            from cj_product_kd_sync.cj_amazon_sku_map
            where sku like '%%*%%' or sku like '%%+%%'
        ) a 
        left join (
            select toInt64(id) as account_id,site from cj_system_kd_sync.cj_amazon_account
        ) b 
        on a.account_id=b.account_id
        left join domestic_warehouse_clear.yibai_site_table_amazon c 
        on b.site=c.site

        union all 

        select distinct c.site1 as `站点`,a.sku as sku
        from (
            select distinct account_id,sku 
            from yxh_product_kd_sync.yxh_amazon_sku_map
            where sku like '%%*%%' or sku like '%%+%%'
        ) a 
        left join (
            select toInt64(id) as account_id,site from yxh_system_kd_sync.yxh_amazon_account
        ) b 
        on a.account_id=b.account_id
        left join domestic_warehouse_clear.yibai_site_table_amazon c 
        on b.site=c.site
    )
    -- 筛选欧洲仅德国站点
    where `站点` not in ('比利时','法国','意大利','西班牙','荷兰','波兰','瑞典','土耳其')
    """
    df2 = conn_mx2.ck_select_to_df(sql)
    # df2['site'] = df2['站点']
    # df2 = df2.sample(1000)
    print('添加捆绑sku完成！')

    return df2


def get_fba_first_fee(df_temp, df_diff, i):
    # 差值fba
    t1 = time.time()
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    df_fba = get_cost_range(df_temp, df_diff_fba)
    df_fba = merge_four_dim_diff(df_fba, df_diff_fba, ['站点', 'first_product_line', 'cost_range'])
    df_fba['net_profit2'] = (df_fba['net_profit2'] / 100).astype('float')
    df_fba['net_interest_rate_target'] = (df_fba['net_interest_rate_target'] / 100).astype('float')
    df_fba['FBA目标毛利润率'] = df_fba['net_profit2'] + df_fba['net_interest_rate_target']
    df_fba['FBA差值'] = df_fba['net_profit2']
    col = ['sku', '站点', '重量', '成本', '长', '宽', '高', 'FBA目标毛利润率', 'FBA差值']
    df_fba = df_fba[col]
    ## 获取运费
    ## 优化头程计算代码
    # 0、获取头程属性。 输入 sku1 + contry_code ,输出 fba头程属性
    # 1、各站点都有空运。['德国','印度','新加坡','沙特']没有海运
    #
    sql = """
        select site1 as `站点`,site3 as country_code FROM domestic_warehouse_clear.yibai_site_table_amazon
        """
    df_code = conn_ck.ck_select_to_df(sql)
    df_fba = pd.merge(df_fba, df_code, how='left', on=['站点'])
    ## 头程属性
    # for i in df_site['site1'].unique():
    df_fba['sku1'] = df_fba['sku']
    aut_ky = aut(df_fba, conn_ck, '空运')
    df_fba = pd.merge(df_fba, aut_ky, how='left', on=['sku1', 'country_code'])
    # df_fba.drop(['sku1'], axis=1, inplace=True)
    df_fba['fba头程属性'] = df_fba['fba头程属性'].fillna('普货')
    ##
    df_fba['站点1'] = df_fba['站点']
    df_kongyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '空运')
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    ##
    if i not in ['印度', '新加坡']:
        aut_hy = aut(df_fba, conn_ck, '海运')
        df_fba = pd.merge(df_fba, aut_hy, how='left', on=['sku1', 'country_code'])
        df_haiyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '海运')
        df_haiyun = df_haiyun[['sku', '站点', '头程费_海运']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_haiyun = pd.DataFrame(columns=['sku', '站点', '头程费_海运'])
    if i in ['美国']:
        aut_hy = aut(df_fba, conn_ck, '海运')
        df_fba = pd.merge(df_fba, aut_hy, how='left', on=['sku1', 'country_code'])
        df_manhai = tou_cheng_api(df_fba, 'FBA', conn_ck, '慢海')
        df_manhai = df_manhai[['sku', '站点', '头程费_慢海']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_manhai = pd.DataFrame(columns=['sku', '站点', '头程费_慢海'])
    if i in ['德国']:
        aut_tl = aut(df_fba, conn_ck, '铁路')
        df_fba = pd.merge(df_fba, aut_tl, how='left', on=['sku1', 'country_code'])
        df_tielu = tou_cheng_api(df_fba, 'FBA', conn_ck, '铁路')
        df_tielu = df_tielu[['sku', '站点', '头程费_铁路']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_tielu = pd.DataFrame(columns=['sku', '站点', '头程费_铁路'])
    if i in ['英国', '德国']:
        aut_ly = aut(df_fba, conn_ck, '卡航')
        df_fba = pd.merge(df_fba, aut_ly, how='left', on=['sku1', 'country_code'])
        df_luyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '卡航')
        df_luyun = df_luyun[['sku', '站点', '头程费_卡航']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_luyun = pd.DataFrame(columns=['sku', '站点', '头程费_卡航'])

    ## 快运\慢运
    # df_fba.drop(['fba头程属性_x','fba头程属性_y'], axis=1, inplace=True)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    if i in ['英国', '德国']:
        aut_fast = aut(df_fba, conn_ck, '卡航')
    else:
        aut_fast = aut(df_fba, conn_ck, '空运')
    df_fba = pd.merge(df_fba, aut_fast, how='left', on=['sku1', 'country_code'])
    df_kuaiyun = tou_cheng_dingjiabi(df_fba, conn, '快运')
    df_kuaiyun = df_kuaiyun.rename(columns={'头程费_人民币': '头程费_快运'})
    df_kuaiyun = df_kuaiyun[['sku', '站点', '头程费_快运']]
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    ##

    aut_slow = aut(df_fba, conn_ck, '海运')
    df_fba = pd.merge(df_fba, aut_slow, how='left', on=['sku1', 'country_code'])
    df_manyun = tou_cheng_dingjiabi(df_fba, conn, '慢运')
    df_manyun = df_manyun.rename(columns={'头程费_人民币': '头程费_慢运'})
    df_manyun = df_manyun[['sku', '站点', '头程费_慢运']]
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    t2 = time.time()
    ##
    df_price = pd.merge(df_fba[['sku', '站点', '成本', 'FBA目标毛利润率', 'FBA差值']],
                        df_kongyun[['sku', '站点', '头程费_空运']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_haiyun[['sku', '站点', '头程费_海运']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_tielu[['sku', '站点', '头程费_铁路']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_luyun[['sku', '站点', '头程费_卡航']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_manhai[['sku', '站点', '头程费_慢海']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_kuaiyun[['sku', '站点', '头程费_快运']], how='left',
                        on=['sku', '站点'])
    df_price = pd.merge(df_price, df_manyun[['sku', '站点', '头程费_慢运']], how='left', on=['sku', '站点'])
    print(f'fba头程运费获取完成，共耗时{t2 - t1:.2f}s')
    return df_price

def get_fbm_fee(df_temp, df_diff, i):
    """
    获取国内仓运费数据，包括捆绑sku的运费
    """
    t1 = time.time()
    df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    df_fbm = get_cost_range(df_temp, df_diff_fbm)
    df_fbm = merge_four_dim_diff(df_fbm, df_diff_fbm, ['站点', 'first_product_line', 'cost_range'])
    df_fbm['net_profit2'] = (df_fbm['net_profit2'] / 100).astype('float')
    df_fbm['net_interest_rate_target'] = (df_fbm['net_interest_rate_target'] / 100).astype('float')
    df_fbm['FBM目标毛利润率'] = df_fbm['net_profit2'] + df_fbm['net_interest_rate_target']
    df_fbm['FBM差值'] = df_fbm['net_profit2']
    col = ['sku', 'is_bundle', '站点', '重量', '成本', '长', '宽', '高', 'FBM目标毛利润率', 'FBM差值']
    df_fbm = df_fbm[col]

    conn_ck_0 = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器')
    sql = f"""
        SELECT distinct sku,ship_name,warehouse_id,total_cost as `运费`,site as `站点`,ship_type  
        from yibai_temp_hxx.old_freight_interface_amazon 
        where site='{i}' 
        order by total_cost asc limit 1 by sku
        """
    df_yunfei0 = conn_ck_0.ck_select_to_df(sql)
    df_fbm = pd.merge(df_fbm, df_yunfei0, how='left', on=['sku', '站点'])

    # 捆绑sku的运费
    sql = """
        SELECT sku, total_cost, site as `站点`
        FROM (
            SELECT sku, warehouse_id, total_cost, site, update_date
            FROM yibai_temp_hxx.freight_interface_amazon2
            UNION ALL
            SELECT sku, warehouse_id, total_cost, site, update_date
            FROM yibai_temp_hxx.freight_interface_amazon3
        ) a
        ORDER BY a.update_date DESC, a.total_cost ASC
        LIMIT 1 BY a.sku, a.site
    """
    df_kunbang_fee = conn_ck_0.ck_select_to_df(sql)
    #
    df_fbm = pd.merge(df_fbm, df_kunbang_fee, how='left', on=['sku', '站点'])
    df_fbm['运费'] = np.where(df_fbm['is_bundle'] == 1, df_fbm['total_cost'], df_fbm['运费'])
    t2 = time.time()
    print(f'国内仓运费获取完成，共耗时{t2 - t1:.2f}s')
    return df_fbm


def update_pricing_ratio_config():
    """ 更新定价比头程单价配置表 """
    """`站点`,`对照表`, `头程价格`,`申报比率`,`关税率`,`计泡系数`,
            `敏感货单价`,`关税/kg（日本）`,`计费方式`,`关税计算方式`"""
    sql = f"""
        select *
        from over_sea.pricing_ratio_config_table    
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    # 备份原表
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'pricing_ratio_config_table_backup', if_exists='replace')
    conn.close()

    df.loc[df['站点'] != '美国', '对照表'] = df['对照表'].replace('慢海', '海运')
    df.loc[df['站点'] == '美国', '对照表'] = df['对照表'].replace('快海', '海运')

    type = df['对照表'].unique()

    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = f"""
    select site as `站点`, unit_price, sensitive_price, tc_type as `对照表`
    from domestic_warehouse_clear.fba_fees_parameter_tc
    where  platform='FBA'
    """
    price_table = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, price_table, how='left', on=['站点', '对照表'])

    df['头程价格'] = np.where(~df['unit_price'].isna(), df['unit_price'], df['头程价格'])
    df['敏感货单价'] = np.where(~df['sensitive_price'].isna(), df['sensitive_price'], df['敏感货单价'])

    df.loc[df['站点'] != '美国', '对照表'] = df['对照表'].replace('海运', '慢海')
    df.loc[df['站点'] == '美国', '对照表'] = df['对照表'].replace('海运', '快海')

    df.drop(['unit_price', 'sensitive_price'], axis=1, inplace=True)

    # 存表
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'pricing_ratio_config_table', if_exists='replace')
    conn.close()

# 20240515 定价比计算程序优化
def pricing_ratio_new():
    """
    定价比计算程序
    """
    # 头程单价更新
    update_pricing_ratio_config()
    print('头程单价已更新...')
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    # 表准备
    table_name = f'pricing_ratio_{date_today}'
    # table_name = f'pricing_ratio_test'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表删除成功！')
    sql = f"""
    CREATE TABLE IF NOT EXISTS support_document.{table_name}
    (
        `id` Int64,
        `date` String,
        `sku` String,
        `站点` String COMMENT '站点',
        `FBA目标毛利润率` Float64 COMMENT 'FBA目标毛利润率',
        `FBA空运定价` Float64 COMMENT 'FBA空运定价',
        `FBA海运定价` Float64 COMMENT 'FBA海运定价',
        `FBA铁路定价` Float64 COMMENT 'FBA铁路定价',
        `FBA卡航定价` Float64 COMMENT 'FBA卡航定价',
        `FBA慢海定价` Float64 COMMENT 'FBA慢海定价',
        `FBA快运定价` Float64 COMMENT 'FBA快运定价',
        `FBA慢运定价` Float64 COMMENT 'FBA慢运定价',
        `FBM目标毛利润率` Float64 COMMENT 'FBM目标毛利润率',
        `FBM定价` Float64 COMMENT 'FBM定价',
        `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
        `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
        `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
        `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
        `FBA慢海定价/FBM定价` Float64 COMMENT 'FBA慢海定价/FBM定价',
        `FBA快运定价/FBM定价` Float64 COMMENT 'FBA快运定价/FBM定价',
        `FBA慢运定价/FBM定价` Float64 COMMENT 'FBA慢运定价/FBM定价',
        `成本` Float64 COMMENT '成本',
        `头程费空运` Float64 COMMENT '头程费空运',
        `头程费海运` Float64 COMMENT '头程费海运',
        `头程费铁路` Float64 COMMENT '头程费铁路',
        `头程费卡航` Float64 COMMENT '头程费卡航',
        `头程费慢海` Float64 COMMENT '头程费慢海',
        `头程费快运` Float64 COMMENT '头程费快运',
        `头程费慢运` Float64 COMMENT '头程费慢运',
        `调拨费` Float64 COMMENT '调拨费',
        `fba_fees` Float64 COMMENT '尾程',
        `运费` Float64 COMMENT 'fbm运费',
        `FBA差值` Float64 COMMENT 'FBA差值',
        `FBM差值` Float64 COMMENT 'FBM差值',
        `FBA税率` Float64 COMMENT 'FBA税率',
        `FBM税率` Float64 COMMENT 'FBM税率',
        `汇率` Float64 COMMENT '汇率',
        `空海利润率反算` Nullable(Float64),
        `空海同价利率差` Nullable(Float64),
        `空铁利润率反算` Nullable(Float64),
        `空铁同价利率差` Nullable(Float64),
        `空卡利润率反算` Nullable(Float64),
        `空卡同价利率差` Nullable(Float64),
        `快海慢海同价利率差` Nullable(Float64),
        `快慢利润率反算` Nullable(Float64),
        `快慢同价利率差` Nullable(Float64),
        `update_time` String DEFAULT toString(now()) COMMENT '更新时间'
    )
    ENGINE = MergeTree
    ORDER BY (sku, `站点`)
    SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('结果表建立成功！')
    sql = f"""
        ALTER TABLE support_document.{table_name}
        DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表今日数据删除成功！')

    # 需要计算的站点
    sql = """
        SELECT 
            site, site1
        from domestic_warehouse_clear.yibai_site_table_amazon 
        where site in ('us', 'ca', 'mx', 'de', 'uk', 'au', 'ae', 'sa','sg','jp') 
        -- where site not in ('be', 'fr', 'it', 'sp', 'nl', 'pl', 'se', 'tr','br','in')
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_site = conn_ck.ck_select_to_df(sql)
    #
    df_sku = get_sku_new()
    df_sku['is_bundle'] = 0
    df_kunbang = add_kunbang_new()
    df_kunbang['is_bundle'] = 1
    #
    df = pd.concat([df_sku, df_kunbang])
    df = merge_first_product_line(df)
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量', '重量来源'], axis=1, inplace=True)
    #
    df_diff = get_diff_new()
    # 默认参数：平台佣金、汇率
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select * from domestic_warehouse_clear.price_calculate_parameters
    """
    df_site_df = conn_ck.ck_select_to_df(sql)
    df_site_df = amazon_fbm_para(df_site_df, [], pd.DataFrame(), mode='AMAZON-FBA')
    ##
    # 主循环。获取FBA运费、FBM运费、计算定价比
    for i in tqdm(df_site['site1'].unique()):
        # if i == '美国':
        print(f'开始计算{i}站点数据...')
        df.loc[df['is_bundle'] == 0, '站点'] = i
        df_temp = df[df['站点'] == i]

        ## fba头程运费
        df_price = get_fba_first_fee(df_temp, df_diff, i)

        ## fba尾程运费
        df_temp['fbafee计算方式'] = '普通'
        df_temp['最小购买数量'] = 1
        df_fba_fee = fba_ding_jia_biao(df_temp)
        print(f"尾程费为空的数量有{len(df_fba_fee[(df_fba_fee['fba_fees'].isna()) | (df_fba_fee['fba_fees'] == '')])}")
        df_fba_fee['fba_fees'] = pd.to_numeric(df_fba_fee['fba_fees'], errors='coerce')
        df_fba_fee['fba_fees'] = df_fba_fee['fba_fees'].fillna(0).astype(float)
        # FBM运费
        df_fbm = get_fbm_fee(df_temp, df_diff, i)
        ## 定价计算
        df_price = pd.merge(df_price, df_fba_fee[['sku', '站点', 'fba_fees']], how='left', on=['sku', '站点'])
        df_price = pd.merge(df_price, df_fbm[['sku', '站点', '运费', 'FBM目标毛利润率', 'FBM差值']], how='left',
                            on=['sku', '站点'])
        df_price = pd.merge(df_price, df_site_df[['站点', '汇率', '平台抽成比例', '冗余系数']], how='left',
                            on=['站点'])
        ##
        tax_rates_fba = {
            '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
            '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187
        }
        tax_rates_fbm = {
            '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
            '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187, '印度': 0.3, '土耳其': 0.15,
            '澳大利亚': 0.09
        }
        # 使用map方法将税率应用到对应的站点
        df_price['FBA税率'] = df_price['站点'].map(tax_rates_fba)
        df_price['FBM税率'] = df_price['站点'].map(tax_rates_fbm)
        df_price[['FBA税率', 'FBM税率']] = df_price[['FBA税率', 'FBM税率']].fillna(0).astype(float)

        ##
        df_price['调拨费'] = 0
        for fbafee_type in ['空运', '海运', '铁路', '卡航','慢海', '快运', '慢运']:
            df_price[f"FBA{fbafee_type}定价"] = (df_price["成本"] + df_price[f"头程费_{fbafee_type}"] + df_price[
                "调拨费"] + 2 + df_price["fba_fees"] * df_price["汇率"]) / (
                                                            1 - df_price["平台抽成比例"] - df_price["冗余系数"] -
                                                            df_price["FBA目标毛利润率"] - df_price["FBA税率"]) / \
                                                df_price["汇率"]
            df_price[f"FBA{fbafee_type}定价"] = df_price[f"FBA{fbafee_type}定价"].astype('float')
        df_price["FBM定价"] = (df_price["成本"] + df_price["运费"]) / (
                1 - df_price["平台抽成比例"] - df_price["冗余系数"] - df_price["FBM目标毛利润率"] - df_price[
            "FBM税率"]) / df_price["汇率"]
        ##
        dic = {'头程费_海运': '头程费海运', '头程费_空运': '头程费空运', '头程费_铁路': '头程费铁路','头程费_卡航': '头程费卡航',
               '头程费_慢海':'头程费慢海', '头程费_快运': '头程费快运', '头程费_慢运': '头程费慢运'}
        df_price = df_price.rename(columns=dic)
        ##
        df_price['空海利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费海运'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空海同价利率差'] = df_price['空海利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空铁利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费铁路'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空铁同价利率差'] = df_price['空铁利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空卡利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费卡航'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空卡同价利率差'] = df_price['空卡利润率反算'] - df_price['FBA目标毛利润率']
        df_price['快海慢海利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费慢海'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA慢运定价'] * df_price['汇率'])
        df_price['快海慢海同价利率差'] = df_price['快海慢海利润率反算'] - df_price['FBA目标毛利润率']
        df_price['快慢利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费慢运'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA快运定价'] * df_price['汇率'])
        df_price['快慢同价利率差'] = df_price['快慢利润率反算'] - df_price['FBA目标毛利润率']
        df_price['FBA空运定价/FBM定价'] = df_price['FBA空运定价'] / df_price['FBM定价']
        # df_price['FBA海运定价/FBM定价'] = np.where(df_price['站点']=='美国', df_price['FBA慢运定价'] / df_price['FBA海运定价'],
        #                                            df_price['FBA海运定价'] / df_price['FBM定价'])
        df_price['FBA海运定价/FBM定价'] = df_price['FBA海运定价'] / df_price['FBM定价']
        df_price['FBA铁路定价/FBM定价'] = df_price['FBA铁路定价'] / df_price['FBM定价']
        df_price['FBA卡航定价/FBM定价'] = df_price['FBA卡航定价'] / df_price['FBM定价']
        df_price['FBA慢海定价/FBM定价'] = df_price['FBA慢海定价'] / df_price['FBM定价']
        df_price['FBA快运定价/FBM定价'] = df_price['FBA快运定价'] / df_price['FBM定价']
        df_price['FBA慢运定价/FBM定价'] = df_price['FBA慢运定价'] / df_price['FBM定价']
        df_price['date'] = time.strftime('%Y-%m-%d')

        ##
        # 数值处理
        # df_price = df_price[df_price['fba_fees'] != '不符合FBA物流条件']
        df_res = df_price[['date', 'sku', '站点','FBA目标毛利润率', 'FBA空运定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价','FBA慢海定价',
                           'FBA快运定价','FBA慢运定价','FBM目标毛利润率', 'FBM定价', 'FBA空运定价/FBM定价', 'FBA海运定价/FBM定价',
                           'FBA铁路定价/FBM定价','FBA卡航定价/FBM定价', 'FBA慢海定价/FBM定价','FBA快运定价/FBM定价', 'FBA慢运定价/FBM定价', '成本',
                           '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费慢海','头程费快运', '头程费慢运','调拨费',
                           'fba_fees', '运费', 'FBA差值', 'FBM差值', 'FBA税率', 'FBM税率', '汇率', '空海利润率反算',
                           '空海同价利率差', '空铁利润率反算', '空铁同价利率差', '空卡利润率反算', '空卡同价利率差','快海慢海同价利率差',
                           '快慢利润率反算', '快慢同价利率差']]
        col = ['成本', '运费', '调拨费', 'fba_fees', 'FBA空运定价/FBM定价', '空海利润率反算', '空海同价利率差','空铁利润率反算',
               '空铁同价利率差', '空卡利润率反算', '空卡同价利率差','快海慢海同价利率差',  '快慢利润率反算', '快慢同价利率差']
        df_res[col] = df_res[col].astype(float).round(4)
        col = ['FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', 'FBA慢海定价/FBM定价','FBA快运定价/FBM定价',
               'FBA慢运定价/FBM定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价',  'FBA慢海定价','FBA快运定价', 'FBA慢运定价',
               '头程费海运', '头程费铁路', '头程费卡航', '头程费慢海', '头程费快运', '头程费慢运']
        df_res[col] = df_res[col].astype('float').round(4).fillna(-999)
        # ## 异常数据存表
        # # 表准备
        # # table_name = f'pricing_ratio_test_{date_today}'
        # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        # df_price_null = df_res[(df_res['FBA空运定价'].isna()) | (df_res['运费'].isna()) | (df_res['fba_fees'] == 0)]
        # sql = f"""
        #     ALTER TABLE yibai_oversea.pricing_ratio_null_data
        #     DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
        # """
        # conn_ck.ck_execute_sql(sql)
        # conn_ck.ck_insert(df_price_null, 'pricing_ratio_null_data', if_exist='append')
        ## 最终结果存表
        df_res = df_res[(~df_res['FBA空运定价'].isna()) & (~df_res['运费'].isna()) & (df_res['fba_fees'] != 0)]
        conn_mx2.ck_insert(df_res, table_name, if_exist='append')
        # conn_ck.write_to_ck_json_type(df_res, table_name)
        print('done!')


def yunfei_check():
    # 检查运费是否波动异常
    date_today = datetime.datetime.today()
    table_name = f'yibai_totalcost_change_count_{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    limit = 100000
    try:
        sql = f"""
        select *
        from monitor_process_data.{table_name}
        """
        df = sql_to_pd(database='monitor_process_data', sql=sql, data_sys='数据部服务器')
        n = df[
            (df['涨幅'] == '下降50%以上') 
            | (df['涨幅'] == '下降30%-50%') 
            | (df['涨幅'] == '上涨30%-50%') 
            | (df['涨幅'] == '上涨50%-100倍') 
            | (df['涨幅'] == '100倍以上')]['计数'].sum()
        print(f'运费异常数量: {n}！')
        if n >= limit:
            send_msg('动销组定时任务推送', '定价比监控',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}运费异常数量超过{limit},为{n},异常,请检查!",
                     mobiles=['+86-13922822326'],
                     is_all=False)
            raise Exception(f'运费异常数量超过{limit}，请检查是否正常！')
        else:
            print('运费检查完成，正常！')
    except:
        send_msg('动销组定时任务推送', '定价比监控',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}运费检查出现问题,请及时排查,失败原因详情请查看airflow日志!",
                 mobiles=['+86-13922822326'],
                 is_all=False)
        raise Exception(traceback.format_exc())


def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    会先清除历史表数据
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    # sql = f"""
    # DELETE FROM over_sea.{table_name} WHERE date = '{date_id}'
    # """
    # conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()

# 墨西哥站点区分企业税率（0%）和个人税率（9%）
# 实现方式为：原表维持不变，新增一张表单独存个人税率的计算结果
def pricing_ratio_mx():
    """
    单独存MX个人税率的计算结果

    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.datetime.today()

    """
    方案：
    FBA空运目标净利定价/FBM目标净利定价 或 FBA海运目标净利定价/FBM目标净利定价
    FBA定价：(2+成本+头程+尾程fba_fees*汇率) / [(1-佣金率-折损率-订单毛利率-税率)*汇率]
    FBM定价：(成本+运费) / [(1-佣金率-折损率-订单毛利率-税率)*汇率]
    """

    # 表准备
    table_name = f'pricing_ratio_mx_{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表删除成功！')
    sql = f"""
                        CREATE TABLE IF NOT EXISTS support_document.{table_name}
                        (
                        `id` Int64,
                        `date` String,
                        `sku` String,
                        `站点` String COMMENT '站点',
                        `FBA目标毛利润率` Float64 COMMENT 'FBA目标毛利润率',
                        `FBA空运定价` Float64 COMMENT 'FBA空运定价',
                        `FBA海运定价` Float64 COMMENT 'FBA海运定价',
                        `FBA铁路定价` Float64 COMMENT 'FBA铁路定价',
                        `FBA卡航定价` Float64 COMMENT 'FBA卡航定价',
                        `FBA快运定价` Float64 COMMENT 'FBA快运定价',
                        `FBA慢运定价` Float64 COMMENT 'FBA慢运定价',
                        `FBM目标毛利润率` Float64 COMMENT 'FBM目标毛利润率',
                        `FBM定价` Float64 COMMENT 'FBM定价',
                        `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
                        `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
                        `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
                        `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
                        `FBA快运定价/FBM定价` Float64 COMMENT 'FBA快运定价/FBM定价',
                        `FBA慢运定价/FBM定价` Float64 COMMENT 'FBA慢运定价/FBM定价',
                        `成本` Float64 COMMENT '成本',
                        `头程费空运` Float64 COMMENT '头程费空运',
                        `头程费海运` Float64 COMMENT '头程费海运',
                        `头程费铁路` Float64 COMMENT '头程费铁路',
                        `头程费卡航` Float64 COMMENT '头程费卡航',
                        `头程费快运` Float64 COMMENT '头程费快运',
                        `头程费慢运` Float64 COMMENT '头程费慢运',
                        `调拨费` Float64 COMMENT '调拨费',
                        `fba_fees` Float64 COMMENT '尾程',
                        `运费` Float64 COMMENT 'fbm运费',
                        `FBA差值` Float64 COMMENT 'FBA差值',
                        `FBM差值` Float64 COMMENT 'FBM差值',
                        `FBA税率` Float64 COMMENT 'FBA税率',
                        `FBM税率` Float64 COMMENT 'FBM税率',
                        `汇率` Float64 COMMENT '汇率',
                        `空海利润率反算` Nullable(Float64),
                        `空海同价利率差` Nullable(Float64),
                        `空铁利润率反算` Nullable(Float64),
                        `空铁同价利率差` Nullable(Float64),
                        `空卡利润率反算` Nullable(Float64),
                        `空卡同价利率差` Nullable(Float64),
                        `快慢利润率反算` Nullable(Float64),
                        `快慢同价利率差` Nullable(Float64),
                        `update_time` String COMMENT '更新时间'
                        )
                        ENGINE = MergeTree
                        ORDER BY (sku, `站点`)
                        SETTINGS index_granularity = 8192
                        """
    conn_mx2.ck_create_table(sql)
    print('结果表建立成功！')
    sql = f"""
        ALTER TABLE support_document.{table_name}
        DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表今日数据删除成功！')


    # 需要计算的站点
    sql = """
        SELECT 
            site, site1
        from domestic_warehouse_clear.yibai_site_table_amazon bb
        where bb.site  in ('mx')
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_site = conn_ck.ck_select_to_df(sql)
    #
    df_sku = get_sku_new()
    df_sku['is_bundle'] = 0
    df_kunbang = add_kunbang_new()
    df_kunbang['is_bundle'] = 1
    df_kunbang = df_kunbang[df_kunbang['站点']=='墨西哥']

    df = pd.concat([df_sku, df_kunbang])
    df = merge_first_product_line(df)

    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量', '重量来源'], axis=1, inplace=True)

    df_diff = get_diff_new()
    # 默认参数：平台佣金、汇率
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select * from domestic_warehouse_clear.price_calculate_parameters
    """
    df_site_df = conn_ck.ck_select_to_df(sql)
    df_site_df = amazon_fbm_para(df_site_df, [], pd.DataFrame(), mode='AMAZON-FBA')
    # 主循环。获取FBA运费、FBM运费、计算定价比
    for i in tqdm(df_site['site1'].unique()):
        print(f'开始计算{i}站点数据...')
        df.loc[df['is_bundle'] == 0, '站点'] = i
        df_temp = df[df['站点'] == i]

        # fba头程运费
        df_price = get_fba_first_fee(df_temp, df_diff, i)

        # fba尾程运费
        df_temp['fbafee计算方式'] = '普通'
        df_temp['最小购买数量'] = 1
        df_fba_fee = fba_ding_jia_biao(df_temp)
        print(f"尾程费为空的数量有{len(df_fba_fee[(df_fba_fee['fba_fees'].isna()) | (df_fba_fee['fba_fees'] == '')])}")
        df_fba_fee['fba_fees'] = pd.to_numeric(df_fba_fee['fba_fees'], errors='coerce')
        df_fba_fee['fba_fees'] = df_fba_fee['fba_fees'].fillna(0).astype(float)
        # FBM运费
        df_fbm = get_fbm_fee(df_temp, df_diff, i)
        # 定价计算
        df_price = pd.merge(df_price, df_fba_fee[['sku', '站点', 'fba_fees']], how='left', on=['sku', '站点'])
        df_price = pd.merge(df_price, df_fbm[['sku', '站点', '运费', 'FBM目标毛利润率', 'FBM差值']], how='left',
                            on=['sku', '站点'])
        df_price = pd.merge(df_price, df_site_df[['站点', '汇率', '平台抽成比例', '冗余系数']], how='left',
                            on=['站点'])

        tax_rates_fba = {'墨西哥':0.09}
        tax_rates_fbm = {'墨西哥':0.09}
        # 使用map方法将税率应用到对应的站点
        df_price['FBA税率'] = df_price['站点'].map(tax_rates_fba)
        df_price['FBM税率'] = df_price['站点'].map(tax_rates_fbm)
        df_price[['FBA税率', 'FBM税率']] = df_price[['FBA税率', 'FBM税率']].fillna(0).astype(float)

        #
        df_price['调拨费'] = 0
        for fbafee_type in ['空运', '海运', '铁路', '卡航', '快运', '慢运']:
            df_price[f"FBA{fbafee_type}定价"] = (df_price["成本"] + df_price[f"头程费_{fbafee_type}"] + df_price[
                "调拨费"] + 2 + df_price["fba_fees"] * df_price["汇率"]) / (
                                                            1 - df_price["平台抽成比例"] - df_price["冗余系数"] -
                                                            df_price["FBA目标毛利润率"] - df_price["FBA税率"]) / \
                                                df_price["汇率"]
            df_price[f"FBA{fbafee_type}定价"] = df_price[f"FBA{fbafee_type}定价"].astype('float')
        df_price["FBM定价"] = (df_price["成本"] + df_price["运费"]) / (
                1 - df_price["平台抽成比例"] - df_price["冗余系数"] - df_price["FBM目标毛利润率"] - df_price[
            "FBM税率"]) / df_price["汇率"]

        dic = {'头程费_海运': '头程费海运', '头程费_空运': '头程费空运', '头程费_铁路': '头程费铁路','头程费_卡航': '头程费卡航',
               '头程费_快运': '头程费快运', '头程费_慢运': '头程费慢运'}
        df_price = df_price.rename(columns=dic)

        df_price['空海利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费海运'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空海同价利率差'] = df_price['空海利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空铁利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费铁路'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空铁同价利率差'] = df_price['空铁利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空卡利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费卡航'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空卡同价利率差'] = df_price['空卡利润率反算'] - df_price['FBA目标毛利润率']
        df_price['快慢利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费慢运'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA快运定价'] * df_price['汇率'])
        df_price['快慢同价利率差'] = df_price['快慢利润率反算'] - df_price['FBA目标毛利润率']
        df_price['FBA空运定价/FBM定价'] = df_price['FBA空运定价'] / df_price['FBM定价']
        df_price['FBA海运定价/FBM定价'] = df_price['FBA海运定价'] / df_price['FBM定价']
        df_price['FBA铁路定价/FBM定价'] = df_price['FBA铁路定价'] / df_price['FBM定价']
        df_price['FBA卡航定价/FBM定价'] = df_price['FBA卡航定价'] / df_price['FBM定价']
        df_price['FBA快运定价/FBM定价'] = df_price['FBA快运定价'] / df_price['FBM定价']
        df_price['FBA慢运定价/FBM定价'] = df_price['FBA慢运定价'] / df_price['FBM定价']
        df_price['date'] = time.strftime('%Y-%m-%d')

        # 数值处理
        # df_price = df_price[df_price['fba_fees'] != '不符合FBA物流条件']
        df_res = df_price[['date', 'sku', '站点','FBA目标毛利润率', 'FBA空运定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价',
                           'FBA快运定价','FBA慢运定价','FBM目标毛利润率', 'FBM定价', 'FBA空运定价/FBM定价', 'FBA海运定价/FBM定价',
                           'FBA铁路定价/FBM定价','FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价', 'FBA慢运定价/FBM定价', '成本',
                           '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运','调拨费',
                           'fba_fees', '运费', 'FBA差值', 'FBM差值', 'FBA税率', 'FBM税率', '汇率', '空海利润率反算',
                           '空海同价利率差', '空铁利润率反算', '空铁同价利率差', '空卡利润率反算', '空卡同价利率差',
                           '快慢利润率反算', '快慢同价利率差']]
        col = ['成本', '运费', '调拨费', 'fba_fees', 'FBA空运定价/FBM定价', '空海利润率反算', '空海同价利率差','空铁利润率反算',
               '空铁同价利率差', '空卡利润率反算', '空卡同价利率差', '快慢利润率反算', '快慢同价利率差']
        df_res[col] = df_res[col].astype(float).round(4)
        col = ['FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价',
               'FBA慢运定价/FBM定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价', 'FBA快运定价', 'FBA慢运定价',
               '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运']
        df_res[col] = df_res[col].astype('float').round(4).fillna(-999)

        # 最终结果存表
        df_res = df_res[(~df_res['FBA空运定价'].isna()) & (~df_res['运费'].isna()) & (df_res['fba_fees'] != 0)]
        conn_mx2.ck_insert(df_res, table_name, if_exist='append')
        # conn_ck.write_to_ck_json_type(df_res, table_name)
        print('done!')

def pricing_ratio_mx_monitor():
    """
    单独计算MX站点个人税率的数据
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat()
    date_today1 = date_today.replace('-', '')
    # 建表
    table_name = f'pricing_ratio_mx_monitor_{date_today1}'
    # table_name = f'pricing_ratio_monitor_test'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    print('监控结果表删除完成!')
    conn_mx2.ck_execute_sql(sql)
    sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `date` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
            `FBA空运定价/FBM定价_修正` Float64 COMMENT 'FBA空运定价/FBM定价_修正',
            `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
            `FBA海运定价/FBM定价_修正` Float64 COMMENT 'FBA海运定价/FBM定价_修正',
            `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
            `FBA铁路定价/FBM定价_修正` Float64 COMMENT 'FBA铁路定价/FBM定价_修正',
            `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
            `FBA卡航定价/FBM定价_修正` Float64 COMMENT 'FBA卡航定价/FBM定价_修正',
            `FBA快运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA快运定价比',
            `FBA快运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA快运定价比_修正',
            `FBA慢运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA慢运定价比',
            `FBA慢运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA慢运定价比_修正',
            `FBA空运定价_修正` Float64 COMMENT 'FBA空运定价_修正',
            `FBA海运定价_修正` Float64 COMMENT 'FBA海运定价_修正',
            `FBA铁路定价_修正` Float64 COMMENT 'FBA铁路定价_修正',
            `FBA卡航定价_修正` Float64 COMMENT 'FBA卡航定价_修正',
            `FBA快运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA快运定价_修正',
            `FBA慢运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_修正',
            `适合空运连续天数` Int32 COMMENT '适合空运连续天数',
            `适合海运连续天数` Int32 COMMENT '适合海运连续天数',
            `适合铁路连续天数` Int32 COMMENT '适合铁路连续天数',
            `适合卡航连续天数` Int32 COMMENT '适合卡航连续天数',
            `适合快运连续天数` Int32 DEFAULT 0 COMMENT '适合快运连续天数',
            `适合慢运连续天数` Int32 DEFAULT 0 COMMENT '适合慢运连续天数',
            `FBA空运定价_参考` Float64 COMMENT 'FBA空运定价_参考',
            `FBA海运定价_参考` Float64 COMMENT 'FBA海运定价_参考',
            `FBA铁路定价_参考` Float64 COMMENT 'FBA铁路定价_参考',
            `FBA卡航定价_参考` Float64 COMMENT 'FBA卡航定价_参考',
            `FBA快运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA快运定价_参考',
            `FBA慢运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_参考',
            `FBA空运定价波幅` Float64 COMMENT 'FBA空运定价波幅',
            `FBA海运定价波幅` Float64 COMMENT 'FBA海运定价波幅',
            `FBA铁路定价波幅` Float64 COMMENT 'FBA铁路定价波幅',
            `FBA卡航定价波幅` Float64 COMMENT 'FBA卡航定价波幅',
            `FBA快运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA快运定价波幅',
            `FBA慢运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA慢运定价波幅',
            `空海同价利率差_修正` Float64,
            `空铁同价利率差_修正` Float64,
            `空卡同价利率差_修正` Float64,
            `快慢同价利率差_修正` Float64 DEFAULT 999  COMMENT '快慢同价利率差_修正',
            `建议物流方式` String COMMENT '建议物流方式：11:快海；12:慢海；13:铁路；15:快卡；16:慢卡；17/6:空运；0:不建议转FBA；5:卡航',
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('监控结果表创建完成!')

    # 定价比表准备
    table_now = f'pricing_ratio_mx_{date_today1}'
    sql_table = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_mx_%'
        and table < '{table_now}'
        order by table desc
        limit 5
    """
    df_table = conn_mx2.ck_select_to_df(sql_table)
    #
    table_monitor_now = f'pricing_ratio_mx_monitor_{date_today1}'
    sql_table_monitor = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_mx_monitor%'
        and table < '{table_monitor_now}'
        order by table desc
        limit 2
    """
    df_table_monitor = conn_mx2.ck_select_to_df(sql_table_monitor)
    # 1 定价比列
    col_list = ['成本', '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运','头程费慢运','调拨费', 'fba_fees', '运费',
                'FBA差值','FBM差值', 'FBA税率', 'FBM税率', '汇率']
    col_list_str1 = ','.join([f'if(`{item}`<0, null, `{item}`) as `{item}`' for item in col_list])
    col_list_str2 = ','.join([f'toNullable(avg(`{item}`)) as `{item}`' for item in col_list])
    col_list_str3 = ','.join([f'`{item}`' for item in col_list])
    col_list_str4 = ','.join([f'a.`{item}` as `{item}_now`' for item in col_list])
    col_list_str5 = ','.join([f'ifNull(b.`{item}`, a.`{item}`) as `{item}_avg`' for item in col_list])
    col_list_str6 = ','.join([f'`{item}_now`' for item in col_list])
    col_list_str7 = ','.join([f'`{item}_avg`' for item in col_list])
    col_list_str8 = ','.join([
                                 f'if(`{item}_now`>1.2 * `{item}_avg` or `{item}_now`<0.8 * `{item}_avg`, 0.5*(`{item}_now`+`{item}_avg`), `{item}_now`) as `{item}`'
                                 for item in col_list])

    # 2 定价比监控列
    monitor_col_list = ['适合空运连续天数', '适合海运连续天数', '适合铁路连续天数', '适合卡航连续天数','适合快运连续天数','适合慢运连续天数',
                    'FBA空运定价_修正','FBA海运定价_修正', 'FBA铁路定价_修正', 'FBA卡航定价_修正','FBA快运定价_修正','FBA慢运定价_修正']
    monitor_col_list_str1 = ','.join([f'`{item}` as `{item}_old`' for item in monitor_col_list])

    # 3
    sql = f"""
    select distinct `站点` from support_document.{table_now}
    """
    df_site = conn_mx2.ck_select_to_df(sql)
    for site in tqdm(list(df_site['站点'])):
        # 1. 定价比
        sql_list = []
        for table in list(df_table['table']):
            sql = f"""
            select sku,`站点`,{col_list_str1}
            from support_document.{table}
            where `站点`='{site}'
            """
            sql_list.append(sql)
        sql_str = '\n union all \n'.join(sql_list)
        # 2. 定价比监控
        monitor_sql_list = []
        for table in list(df_table_monitor['table']):
            sql = f"""
            select sku,`站点`,`date`,`FBA空运定价_参考`,`FBA海运定价_参考`,`FBA铁路定价_参考`,`FBA卡航定价_参考`,`FBA快运定价_参考`,
            `FBA慢运定价_参考`,{monitor_col_list_str1}
            from support_document.{table}
            where `站点`='{site}'
            """
            monitor_sql_list.append(sql)
            monitor_sql_str = '\n union all \n'.join(monitor_sql_list)

        # 3. 计算过程
        print(f'开始计算{site}数据...')
        sql = f"""
        insert into support_document.{table_name}
        with monitor_table as (
            select * from (
                {monitor_sql_str}
            )
            order by `date` desc limit 1 by sku,`站点`
        )
        select toString(today()) as `date`,sku,`站点`,
            `FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
            `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
            `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
            `FBA空运定价_修正`,ifNull(`FBA海运定价_修正`, 0) as `FBA海运定价_修正`,
            ifNull(`FBA铁路定价_修正`, 0) as `FBA铁路定价_修正`,ifNull(`FBA卡航定价_修正`, 0) as `FBA卡航定价_修正`,
            ifNull(`FBA快运定价_修正`, 0) as `FBA快运定价_修正`,ifNull(`FBA慢运定价_修正`, 0) as `FBA慢运定价_修正`,
            `适合空运连续天数`, `适合海运连续天数`, `适合铁路连续天数`, `适合卡航连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
            `FBA空运定价_参考`, `FBA海运定价_参考`, `FBA铁路定价_参考`, `FBA卡航定价_参考`,`FBA快运定价_参考`,`FBA慢运定价_参考`,
            `FBA空运定价波幅`, `FBA海运定价波幅`, `FBA铁路定价波幅`, `FBA卡航定价波幅`,`FBA快运定价波幅`,`FBA慢运定价波幅`,
            `空海同价利率差_修正`, `空铁同价利率差_修正`, `空卡同价利率差_修正`,`快慢同价利率差_修正`, `建议物流方式`,
            toString(now()) as update_time
        from (
            select a.* except(limit_day,`适合空运连续天数`,`适合海运连续天数`,`适合铁路连续天数`,`适合卡航连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
                `FBA空运定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,
                `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`,
                `快慢同价利率差_修正`),
                ifNull(a.`FBA空运定价/FBM定价_修正`, 999) as `FBA空运定价/FBM定价_修正`,
                ifNull(a.`FBA卡航定价/FBM定价_修正`, 999) as `FBA卡航定价/FBM定价_修正`,
                ifNull(a.`FBA铁路定价/FBM定价_修正`, 999) as `FBA铁路定价/FBM定价_修正`,
                ifNull(a.`FBA海运定价/FBM定价_修正`, 999) as `FBA海运定价/FBM定价_修正`,
                ifNull(a.`FBA快运定价/FBM定价_修正`, 999) as `FBA快运定价/FBM定价_修正`,
                ifNull(a.`FBA慢运定价/FBM定价_修正`, 999) as `FBA慢运定价/FBM定价_修正`,
                ifNull(a.`空海同价利率差_修正`, 999) as `空海同价利率差_修正`,
                ifNull(a.`空铁同价利率差_修正`, 999) as `空铁同价利率差_修正`,
                ifNull(a.`空卡同价利率差_修正`, 999) as `空卡同价利率差_修正`,
                ifNull(a.`快慢同价利率差_修正`, 999) as `快慢同价利率差_修正`,
                a.`适合空运连续天数`*(ifNull(b.`适合空运连续天数_old`,0)+1) as `适合空运连续天数`,
                a.`适合海运连续天数`*(ifNull(b.`适合海运连续天数_old`,0)+1) as `适合海运连续天数`,
                a.`适合铁路连续天数`*(ifNull(b.`适合铁路连续天数_old`,0)+1) as `适合铁路连续天数`,
                a.`适合卡航连续天数`*(ifNull(b.`适合卡航连续天数_old`,0)+1) as `适合卡航连续天数`,
                a.`适合快运连续天数`*(ifNull(b.`适合快运连续天数_old`,0)+1) as `适合快运连续天数`,
                a.`适合慢运连续天数`*(ifNull(b.`适合慢运连续天数_old`,0)+1) as `适合慢运连续天数`,
                ifNull(b.`FBA空运定价_修正_old`, a.`FBA空运定价_修正`) as `FBA空运定价_修正_old`,
                ifNull(b.`FBA海运定价_修正_old`, a.`FBA海运定价_修正`) as `FBA海运定价_修正_old`,
                ifNull(b.`FBA铁路定价_修正_old`, a.`FBA铁路定价_修正`) as `FBA铁路定价_修正_old`,
                ifNull(b.`FBA卡航定价_修正_old`, a.`FBA卡航定价_修正`) as `FBA卡航定价_修正_old`,
                ifNull(b.`FBA快运定价_修正_old`, a.`FBA快运定价_修正`) as `FBA快运定价_修正_old`,
                ifNull(b.`FBA慢运定价_修正_old`, a.`FBA慢运定价_修正`) as `FBA慢运定价_修正_old`,
                if(`适合空运连续天数`=0, `FBA空运定价_修正_old`, ifNull(b.`FBA空运定价_参考`, a.`FBA空运定价_修正`)) as `FBA空运定价_参考`,
                if(`适合海运连续天数`=0, `FBA海运定价_修正_old`, ifNull(b.`FBA海运定价_参考`, a.`FBA海运定价_修正`)) as `FBA海运定价_参考`,
                if(`适合铁路连续天数`=0, `FBA铁路定价_修正_old`, ifNull(b.`FBA铁路定价_参考`, a.`FBA铁路定价_修正`)) as `FBA铁路定价_参考`,
                if(`适合卡航连续天数`=0, `FBA卡航定价_修正_old`, ifNull(b.`FBA卡航定价_参考`, a.`FBA卡航定价_修正`)) as `FBA卡航定价_参考`,
                if(`适合快运连续天数`=0, `FBA快运定价_修正_old`, ifNull(b.`FBA快运定价_参考`, a.`FBA快运定价_修正`)) as `FBA快运定价_参考`,
                if(`适合慢运连续天数`=0, `FBA慢运定价_修正_old`, ifNull(b.`FBA慢运定价_参考`, a.`FBA慢运定价_修正`)) as `FBA慢运定价_参考`,
                if(`FBA空运定价_参考`=0 or `FBA空运定价_修正` is null or `FBA空运定价_参考` is null, 0, abs(a.`FBA空运定价_修正`-`FBA空运定价_参考`)/`FBA空运定价_参考`) as `FBA空运定价波幅`,
                if(`FBA海运定价_参考`=0 or `FBA海运定价_修正` is null or `FBA海运定价_参考` is null, 0, abs(a.`FBA海运定价_修正`-`FBA海运定价_参考`)/`FBA海运定价_参考`) as `FBA海运定价波幅`,
                if(`FBA铁路定价_参考`=0 or `FBA铁路定价_修正` is null or `FBA铁路定价_参考` is null, 0, abs(a.`FBA铁路定价_修正`-`FBA铁路定价_参考`)/`FBA铁路定价_参考`) as `FBA铁路定价波幅`,
                if(`FBA卡航定价_参考`=0 or `FBA卡航定价_修正` is null or `FBA卡航定价_参考` is null, 0, abs(a.`FBA卡航定价_修正`-`FBA卡航定价_参考`)/`FBA卡航定价_参考`) as `FBA卡航定价波幅`,
                if(`FBA快运定价_参考`=0 or `FBA快运定价_修正` is null or `FBA快运定价_参考` is null, 0, abs(a.`FBA快运定价_修正`-`FBA快运定价_参考`)/`FBA快运定价_参考`) as `FBA快运定价波幅`,
                if(`FBA慢运定价_参考`=0 or `FBA慢运定价_修正` is null or `FBA慢运定价_参考` is null, 0, abs(a.`FBA慢运定价_修正`-`FBA慢运定价_参考`)/`FBA慢运定价_参考`) as `FBA慢运定价波幅`,
                multiIf(
                    `适合快运连续天数` >= 0 and `FBA快运定价波幅`>= 0.1, '快运定价比修正后数值连续一周低于2且FBA快运定价为主要原因，建议可发快运',
                    `适合慢运连续天数` >= 0 and `FBA慢运定价波幅`>= 0.1, '慢运定价比修正后数值连续一周低于2且FBA慢运定价为主要原因，建议可发慢运',
                '无建议') as `发运建议`,
                multiIf(
                    (`FBA慢运定价/FBM定价_修正`>2 or (`适合慢运连续天数`<0 and `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`> 0.1)) and a.`站点` in ('阿联酋','日本','沙特'), '0', 
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>=0 and a.`站点` in ('阿联酋','日本','沙特'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`> 2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA快运定价/FBM定价_修正`>2 and a.`站点` in ('新加坡'), '0',
                    `FBA快运定价/FBM定价_修正`<=2 and a.`站点` in ('新加坡'), '6',
                    ((`FBA慢运定价/FBM定价_修正`> 1.8) or (`适合慢运连续天数`< 0 and `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1)) and a.`站点` not in ('日本','新加坡','阿联酋','沙特'), '0',         
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '5;6', 
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '5;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` > 1.8, '11',    
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` > 1.8, '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国','英国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国') and `FBA快运定价/FBM定价_修正` > 1.8, '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('英国') and `FBA快运定价/FBM定价_修正` > 1.8, '5',
                    '-1') as `建议物流方式`
            from (
                select sku,`站点`,`FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
                   `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
                   `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
                   `FBA空运定价_修正`, `FBA海运定价_修正`, `FBA铁路定价_修正`, `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`,
                   `空海同价利率差_修正`, `空铁同价利率差_修正`,`空卡同价利率差_修正`,`快慢同价利率差_修正`,
                   if(`站点` in ('阿联酋','新加坡','沙特','日本'), 2, 1.8) as `limit_day`,
                   if(`FBA空运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合空运连续天数`,
                   multiIf(`FBA海运定价/FBM定价_修正` is null, 0, `FBA海运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合海运连续天数`,
                   multiIf(`FBA铁路定价/FBM定价_修正` is null, 0, `FBA铁路定价/FBM定价_修正`<=limit_day, 1, 0) as `适合铁路连续天数`,
                   multiIf(`FBA卡航定价/FBM定价_修正` is null, 0, `FBA卡航定价/FBM定价_修正`<=limit_day, 1, 0) as `适合卡航连续天数`,
                   multiIf(`FBA快运定价/FBM定价_修正` is null, 0, `FBA快运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合快运连续天数`,
                   multiIf(`FBA慢运定价/FBM定价_修正` is null, 0, `FBA慢运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合慢运连续天数`
                from (
                    select a.sku as sku,a.`站点` as `站点`,
                    a.`FBA空运定价/FBM定价` as `FBA空运定价/FBM定价`,a.`FBA卡航定价/FBM定价` as `FBA卡航定价/FBM定价`,
                    a.`FBA海运定价/FBM定价` as `FBA海运定价/FBM定价`,a.`FBA铁路定价/FBM定价` as `FBA铁路定价/FBM定价`,
                    a.`FBA快运定价/FBM定价` as `FBA快运定价/FBM定价`,a.`FBA慢运定价/FBM定价` as `FBA慢运定价/FBM定价`,
                    (a.`成本`+a.`头程费空运`+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA空运定价_修正`,
                    (a.`成本`+a.`运费`)/(1-c.`FBM平台抽成比例`-0.04-a.`FBM目标毛利润率`-a.`FBM税率`)/a.`汇率` as `FBM定价_修正`,
                    `FBA空运定价_修正`/`FBM定价_修正` as `FBA空运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA海运定价_修正`,
                    `FBA海运定价_修正`/`FBM定价_修正` as `FBA海运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA铁路定价_修正`,
                    `FBA铁路定价_修正`/`FBM定价_修正` as `FBA铁路定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA卡航定价_修正`,
                    `FBA卡航定价_修正`/`FBM定价_修正` as `FBA卡航定价/FBM定价_修正`,            
                    (a.`成本`+if(a.`头程费快运`<0, null, a.`头程费快运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA快运定价_修正`,
                    `FBA快运定价_修正`/`FBM定价_修正` as `FBA快运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA慢运定价_修正`,
                    `FBA慢运定价_修正`/`FBM定价_修正` as `FBA慢运定价/FBM定价_修正`,           
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空海利润率反算_修正`,
                    `空海利润率反算_修正`-a.`FBA目标毛利润率` as `空海同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空铁利润率反算_修正`,
                    `空铁利润率反算_修正`-a.`FBA目标毛利润率` as `空铁同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空卡利润率反算_修正`,
                    `空卡利润率反算_修正`-a.`FBA目标毛利润率` as `空卡同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA快运定价_修正`*a.`汇率`) as `快慢利润率反算_修正`,
                    `快慢利润率反算_修正`-a.`FBA目标毛利润率` as `快慢同价利率差_修正`
                    from (
                        select * except({col_list_str6},{col_list_str7}),
                            {col_list_str8},
                            `站点` as `FBA站点`,replace(`站点`, '-泛美', '') as `FBM站点`
                        from (
                            select a.* except ({col_list_str3}),
                                {col_list_str4},
                                {col_list_str5}     
                            from (select * except (id, date, update_time) from support_document.{table_now} where `站点` = '{site}') a 
                            left join (
                                select sku, `站点`,{col_list_str2}
                                from (
                                    {sql_str}
                                )
                                group by sku, `站点`
                            ) b 
                            on a.sku=b.sku and a.`站点`=b.`站点`
                        )
                    ) a 
                    left join (
                        select site `FBA站点`, platform_percentage `FBA平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBA'
                    ) b 
                    on a.`FBA站点`=b.`FBA站点`
                    left join (
                        select site `FBM站点`, platform_percentage `FBM平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBM'
                    ) c 
                    on a.`FBM站点`=c.`FBM站点` 
                ) 
            ) a 
            left join monitor_table b 
            on a.sku=b.sku and a.`站点`=b.`站点`
        )
        """
        conn_mx2.ck_execute_sql(sql)

    print('墨西哥定价比波幅监控完成！')

def center_interface(group, item):
    """
    中台SKU定价比接口调用函数
    """
    url = 'http://salescenter.yibainetwork.com:91/apis/open/amazon_open_api/set_sku_price_data?org_code=org_00001'
    data_post0 = []
    for i in range(len(group['sku'])):
        data_dict = {
            "sku": str(group['sku'][i]),
            "site": str(group['站点'][i]),
            "tax_number": str(group['税号'][i]),
            "fba_ari_price": float(group['FBA空运定价/FBM定价_修正'][i]),
            "fba_qatar_ari_price": float(group['FBA卡航定价/FBM定价_修正'][i]),
            "fba_shipping_price": float(group['FBA海运定价/FBM定价_修正'][i]),
            "fba_railway_price": float(group['FBA铁路定价/FBM定价_修正'][i]),
            "fba_ari_amend": float(group['FBA空运定价_修正'][i]),
            "fba_qatar_ari_amend": float(group['FBA卡航定价_修正'][i]),
            "fba_shipping_amend": float(group['FBA海运定价_修正'][i]),
            "fba_railway_amend": float(group['FBA铁路定价_修正'][i]),
            "ari_train_same_amend": float(group['空铁同价利率差_修正'][i]),
            "ari_shipping_same_amend": float(group['空海同价利率差_修正'][i]),
            "air_qatar_same_amend": float(group['空卡同价利率差_修正'][i]),
            "proposed_logistics_mode": str(group['建议物流方式'][i]),
            "fba_fast_shipping_price": float(group['FBA快运定价_修正'][i]),
            "fba_slow_shipping_price": float(group['FBA慢运定价_修正'][i]),
            "fast_and_slow_rate_diff": float(group['快慢同价利率差_修正'][i]),
            "fba_fast_shipping_price_div_fbm": float(group['FBA快运定价/FBM定价_修正'][i]),
            "fba_slow_shipping_price_div_fbm": float(group['FBA慢运定价/FBM定价_修正'][i])
        }
        # print(data_dict)
        data_post0.append(data_dict)
    data_post = {'data_list': data_post0}
    #
    n = 0
    while n < 5:
        try:
            res = requests.post(url, json=data_post).json()
            # print(res)
            if res['status'] == 1:
                break
            else:
                print(f'定价比{item}:', res['error_mess'])
                # print(data_post0)
                n += 1
        except:
            print(f'定价比{item}:接口失败，重新上传')
            time.sleep(10)
            n += 1

def map_values(s):
    # 分割字符串，根据包含的分隔符来决定使用哪种分割方式
    if ';' in s:
        parts = s.split(';')
    elif ':' in s:
        parts = s.split(':')
    else:
        parts = [s]  # 如果没有分隔符，直接将字符串作为单个元素处理
    # 映射到对应的值，并过滤掉不在字典中的键
    dic = {'11': '快海', '12': '慢海', '13': '铁路', '15': '快卡', '16': '慢卡', '6': '空运', '0': '不建议发FBA',
           '-1': '不建议发FBA', '17': '快空', '18': '慢空', '19': '快铁', '20': '慢铁', '21': '海运', '5': '卡航'}
    mapped_parts = [dic.get(p, p) for p in parts]
    # 合并结果，用分号分隔
    return ';'.join(mapped_parts)

def upload_pricing_ratio():
    """
    需将定价比数据上传到销售中台。但目前存在两个表，主要是墨西哥税号的区别
    将两个表合并一个，并重新上传到数据库中备份，以备中台调用
    数据量太大，分站点获取数据、存表
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = time.strftime('%Y%m%d')
    sql = f"""
        SELECT 
            DISTINCT `站点`
        FROM support_document.pricing_ratio_monitor_{date_today}
    """
    df_site = conn_ck.ck_select_to_df(sql)
    def get_sku_data(conn_ck, site):
        date_today = time.strftime('%Y%m%d')
        # date_today = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        # 取定价比数据
        sql = f"""
            SELECT 
                sku, `站点`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,
                `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价_修正`,`FBA空运定价_修正`,`FBA海运定价_修正`,`FBA铁路定价_修正`,
                `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`,
                `快慢同价利率差_修正`,`建议物流方式`
            FROM support_document.pricing_ratio_monitor_{date_today}
            WHERE `站点` = '{site}'
        """
        df = conn_ck.ck_select_to_df(sql)
        df['税号'] = ' '
        if site == '墨西哥':
            sql = f"""
                SELECT 
                    sku, `站点`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,
                    `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价_修正`,`FBA空运定价_修正`,`FBA海运定价_修正`,`FBA铁路定价_修正`,
                    `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`,
                    `快慢同价利率差_修正`,`建议物流方式`
                FROM support_document.pricing_ratio_mx_monitor_{date_today}
            """
            df_2 = conn_ck.ck_select_to_df(sql)
            df_2['税号'] = '个人税'
            df.loc[df['站点'] == '墨西哥', '税号'] = '企业税'
            df = pd.concat([df, df_2])

        # df['空卡同价利率差_修正'] = df['空卡同价利率差_修正'].fillna(999).astype(float)
        df['税号'] = df['税号'].fillna(' ')
        print(df.info())
        df_null = df[df.isnull().T.any()]
        df_null = df_null.fillna(0)
        df_null['FBA空运定价_修正'] = df_null['FBA空运定价_修正'].replace(np.inf, 999)
        df_null['update_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
        # print(df_null.info())
        # 空值异常数据存入mysql中
        write_to_sql(df_null, 'pricing_ratio_null_data')

        df = df[~df.isnull().T.any()]

        return df

    # 表准备
    table_name = 'pricing_ratio_to_center'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    print('监控结果表删除完成!')
    conn_ck.ck_execute_sql(sql)
    sql = f"""
                        CREATE TABLE IF NOT EXISTS support_document.{table_name}
                        (
                        `id` Int64,
                        `sku` String,
                        `站点` String COMMENT '站点',
                        `税号` String COMMENT '针对墨西哥站点，企业0%税率，个人9%税率',
                        `FBA空运定价/FBM定价_修正` Float64 COMMENT 'FBA空运定价/FBM定价_修正',
                        `FBA海运定价/FBM定价_修正` Float64 COMMENT 'FBA海运定价/FBM定价_修正',
                        `FBA铁路定价/FBM定价_修正` Float64 COMMENT 'FBA铁路定价/FBM定价_修正',
                        `FBA卡航定价/FBM定价_修正` Float64 COMMENT 'FBA卡航定价/FBM定价_修正',
                        `FBA快运定价/FBM定价_修正` Float64 COMMENT 'FBA快运定价/FBM定价_修正',
                        `FBA慢运定价/FBM定价_修正` Float64 COMMENT 'FBA慢运定价/FBM定价_修正',
                        `FBA空运定价_修正` Float64 COMMENT 'FBA空运定价_修正',
                        `FBA海运定价_修正` Float64 COMMENT 'FBA海运定价_修正',
                        `FBA铁路定价_修正` Float64 COMMENT 'FBA铁路定价_修正',
                        `FBA卡航定价_修正` Float64 COMMENT 'FBA卡航定价_修正',
                        `FBA快运定价_修正` Float64 COMMENT 'FBA快运定价_修正',
                        `FBA慢运定价_修正` Float64 COMMENT 'FBA慢运定价_修正',
                        `空海同价利率差_修正` Float64,
                        `空铁同价利率差_修正` Float64,
                        `空卡同价利率差_修正` Float64,
                        `快慢同价利率差_修正` Float64,
                        `建议物流方式` String COMMENT '建议物流方式：11:快海；12:慢海；13:铁路；15:快卡；16:慢卡；6:空运；0:不建议转FBA',
                        `update_time` String COMMENT '更新时间'
                        )
                        ENGINE = MergeTree
                        ORDER BY (sku, `站点`)
                        SETTINGS index_granularity = 8192
                        """
    conn_ck.ck_create_table(sql)
    print('结果表建立成功！')
    conn_ck.ck_execute_sql(sql='truncate table support_document.pricing_ratio_to_center')
    # 判断ck备份表数据是否清除干净
    n = 1
    while n < 5:
        print(f'删除当前表里的数据，第{n}次测试...')
        sql = """
            SELECT count()
            FROM support_document.pricing_ratio_to_center
        """
        df_cnt = conn_ck.ck_select_to_df(sql)
        if df_cnt.iloc[0,0] == 0:
            print('结果表删除成功！')
            break
        else:
            n += 1
            time.sleep(60)

    # 存表
    print('数据开始上传至中台...')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    TRUNCATE TABLE over_sea.pricing_ratio_to_center
    """
    conn.execute(sql)
    conn.close()
    for i in tqdm(df_site['站点'].unique()):
        print(f"开始上传{i}数据")
        df_temp = get_sku_data(conn_ck, i)
        conn_ck.ck_insert(df_temp, table_name, if_exist='append')
        print(f'{i}数据存表完成！')
        # 20240428 mysql也备份一次，用于otter同步
        df_temp['update_time'] = time.strftime('%Y-%m-%d')
        write_to_sql(df_temp, 'pricing_ratio_to_center')

        # 20240516 使用otter同步方式上传中台，暂时停用接口上传
        # # 转化为字典格式
        # df_temp = df_temp.reset_index(drop=True)
        # df_temp['index'] = df_temp.index
        # df_temp['index'] = df_temp['index'].apply(lambda m_data: int(m_data / 1000))
        # #
        # threadPool = ThreadPoolExecutor(max_workers=8)
        # thread_list = []
        # for key, group in df_temp.groupby(['index']):
        #     group = group.reset_index(drop=True)
        #     future = threadPool.submit(center_interface, group, key)
        #     thread_list.append(future)

    # 20240521 新增监控
    sql = f"""
        SELECT date, `站点`, `建议物流方式`, count(1) as `数量`, 'sku' as `维度`
        FROM support_document.pricing_ratio_monitor_{date_today}
        GROUP BY date, `站点`, `建议物流方式`
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_m = conn_ck.ck_select_to_df(sql)
    df_m['建议物流方式2'] = df_m['建议物流方式'].map(map_values)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date = time.strftime('%Y-%m-%d')
    table_name = 'ads_pricing_ratio'
    sql = f"""
    DELETE FROM over_sea.{table_name} WHERE date = '{date}' and `维度` = 'sku'
    """
    conn.execute(sql)
    conn.close()
    write_to_sql(df_m, 'ads_pricing_ratio')

# 阈值修改
def pricing_ratio_monitor_old():
    """
    使用各字段近期平均水平来计算修正数值。如果一两天突然公式中部分因子有大幅度变化，直接计算的定价比会变化很大，但是修正的定价比就不会。
    如果修正后的定价比持续一周都和以往的相差较大，说明各因子已经有了稳定的变化，发运模式就可以建议转变。
    FBA定价的参考值，是修正后的定价比第一次开始适合空运或者海运时候的FBA定价数值，记录下历史中的该值用于一周时和当前修正后的FBA定价
    对比，来判断定价比变化是否主要来源于FBA定价的波动。
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat()
    date_today1 = date_today.replace('-', '')
    # 建表
    table_name = f'pricing_ratio_monitor_{date_today1}'
    # table_name = f'pricing_ratio_monitor_test'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    print('监控结果表删除完成!')
    conn_mx2.ck_execute_sql(sql)
    sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `date` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
            `FBA空运定价/FBM定价_修正` Float64 COMMENT 'FBA空运定价/FBM定价_修正',
            `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
            `FBA海运定价/FBM定价_修正` Float64 COMMENT 'FBA海运定价/FBM定价_修正',
            `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
            `FBA铁路定价/FBM定价_修正` Float64 COMMENT 'FBA铁路定价/FBM定价_修正',
            `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
            `FBA卡航定价/FBM定价_修正` Float64 COMMENT 'FBA卡航定价/FBM定价_修正',
            `FBA快运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA快运定价比',
            `FBA快运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA快运定价比_修正',
            `FBA慢运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA慢运定价比',
            `FBA慢运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA慢运定价比_修正',
            `FBA空运定价_修正` Float64 COMMENT 'FBA空运定价_修正',
            `FBA海运定价_修正` Float64 COMMENT 'FBA海运定价_修正',
            `FBA铁路定价_修正` Float64 COMMENT 'FBA铁路定价_修正',
            `FBA卡航定价_修正` Float64 COMMENT 'FBA卡航定价_修正',
            `FBA快运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA快运定价_修正',
            `FBA慢运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_修正',
            `适合空运连续天数` Int32 COMMENT '适合空运连续天数',
            `适合海运连续天数` Int32 COMMENT '适合海运连续天数',
            `适合铁路连续天数` Int32 COMMENT '适合铁路连续天数',
            `适合卡航连续天数` Int32 COMMENT '适合卡航连续天数',
            `适合快运连续天数` Int32 DEFAULT 0 COMMENT '适合快运连续天数',
            `适合慢运连续天数` Int32 DEFAULT 0 COMMENT '适合慢运连续天数',
            `FBA空运定价_参考` Float64 COMMENT 'FBA空运定价_参考',
            `FBA海运定价_参考` Float64 COMMENT 'FBA海运定价_参考',
            `FBA铁路定价_参考` Float64 COMMENT 'FBA铁路定价_参考',
            `FBA卡航定价_参考` Float64 COMMENT 'FBA卡航定价_参考',
            `FBA快运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA快运定价_参考',
            `FBA慢运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_参考',
            `FBA空运定价波幅` Float64 COMMENT 'FBA空运定价波幅',
            `FBA海运定价波幅` Float64 COMMENT 'FBA海运定价波幅',
            `FBA铁路定价波幅` Float64 COMMENT 'FBA铁路定价波幅',
            `FBA卡航定价波幅` Float64 COMMENT 'FBA卡航定价波幅',
            `FBA快运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA快运定价波幅',
            `FBA慢运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA慢运定价波幅',
            `空海同价利率差_修正` Float64,
            `空铁同价利率差_修正` Float64,
            `空卡同价利率差_修正` Float64,
            `快慢同价利率差_修正` Float64 DEFAULT 999  COMMENT '快慢同价利率差_修正',
            `建议物流方式` String COMMENT '建议物流方式：11:快海；12:慢海；13:铁路；15:快卡；16:慢卡；17/6:空运；0:不建议转FBA',
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('监控结果表创建完成!')

    # 定价比表准备
    table_now = f'pricing_ratio_{date_today1}'
    sql_table = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_%'
        and table < '{table_now}'
        order by table desc
        limit 10
    """
    df_table = conn_mx2.ck_select_to_df(sql_table)
    #
    table_monitor_now = f'pricing_ratio_monitor_{date_today1}'
    sql_table_monitor = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_monitor%'
        and table < '{table_monitor_now}'
        order by table desc
        limit 2
    """
    df_table_monitor = conn_mx2.ck_select_to_df(sql_table_monitor)
    # 1 定价比列
    col_list = ['成本', '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运','头程费慢运','调拨费', 'fba_fees', '运费',
                'FBA差值','FBM差值', 'FBA税率', 'FBM税率', '汇率']
    col_list_str1 = ','.join([f'if(`{item}`<0, null, `{item}`) as `{item}`' for item in col_list])
    col_list_str2 = ','.join([f'toNullable(avg(`{item}`)) as `{item}`' for item in col_list])
    col_list_str3 = ','.join([f'`{item}`' for item in col_list])
    col_list_str4 = ','.join([f'a.`{item}` as `{item}_now`' for item in col_list])
    col_list_str5 = ','.join([f'ifNull(b.`{item}`, a.`{item}`) as `{item}_avg`' for item in col_list])
    col_list_str6 = ','.join([f'`{item}_now`' for item in col_list])
    col_list_str7 = ','.join([f'`{item}_avg`' for item in col_list])
    col_list_str8 = ','.join([
                                 f'if(`{item}_now`>1.2 * `{item}_avg` or `{item}_now`<0.8 * `{item}_avg`, 0.5*(`{item}_now`+`{item}_avg`), `{item}_now`) as `{item}`'
                                 for item in col_list])

    # 2 定价比监控列
    monitor_col_list = ['适合空运连续天数', '适合海运连续天数', '适合铁路连续天数', '适合卡航连续天数','适合快运连续天数','适合慢运连续天数',
                    'FBA空运定价_修正','FBA海运定价_修正', 'FBA铁路定价_修正', 'FBA卡航定价_修正','FBA快运定价_修正','FBA慢运定价_修正']
    monitor_col_list_str1 = ','.join([f'`{item}` as `{item}_old`' for item in monitor_col_list])

    # 3
    sql = f"""
    select distinct `站点` from support_document.{table_now}
    """
    df_site = conn_mx2.ck_select_to_df(sql)
    for site in tqdm(list(df_site['站点'])):
        # 1. 定价比
        sql_list = []
        for table in list(df_table['table']):
            sql = f"""
            select sku,`站点`,{col_list_str1}
            from support_document.{table}
            where `站点`='{site}'
            """
            sql_list.append(sql)
        sql_str = '\n union all \n'.join(sql_list)
        # 2. 定价比监控
        monitor_sql_list = []
        for table in list(df_table_monitor['table']):
            sql = f"""
            select sku,`站点`,`date`,`FBA空运定价_参考`,`FBA海运定价_参考`,`FBA铁路定价_参考`,`FBA卡航定价_参考`,`FBA快运定价_参考`,
            `FBA慢运定价_参考`,{monitor_col_list_str1}
            from support_document.{table}
            where `站点`='{site}'
            """
            monitor_sql_list.append(sql)
            monitor_sql_str = '\n union all \n'.join(monitor_sql_list)

        print(f'开始计算{site}数据...')
        sql = f"""
        insert into support_document.{table_name}
        with monitor_table as (
            select * from (
                {monitor_sql_str}
            )
            order by `date` desc limit 1 by sku,`站点`
        )
        select toString(today()) as `date`,sku,`站点`,
            `FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
            `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
            `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
            `FBA空运定价_修正`,ifNull(`FBA海运定价_修正`, 0) as `FBA海运定价_修正`,
            ifNull(`FBA铁路定价_修正`, 0) as `FBA铁路定价_修正`,ifNull(`FBA卡航定价_修正`, 0) as `FBA卡航定价_修正`,
            ifNull(`FBA快运定价_修正`, 0) as `FBA快运定价_修正`,ifNull(`FBA慢运定价_修正`, 0) as `FBA慢运定价_修正`,
            `适合空运连续天数`, `适合海运连续天数`, `适合铁路连续天数`, `适合卡航连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
            `FBA空运定价_参考`, `FBA海运定价_参考`, `FBA铁路定价_参考`, `FBA卡航定价_参考`,`FBA快运定价_参考`,`FBA慢运定价_参考`,
            `FBA空运定价波幅`, `FBA海运定价波幅`, `FBA铁路定价波幅`, `FBA卡航定价波幅`,`FBA快运定价波幅`,`FBA慢运定价波幅`,
            `空海同价利率差_修正`, `空铁同价利率差_修正`, `空卡同价利率差_修正`,`快慢同价利率差_修正`, `建议物流方式`,
            toString(now()) as update_time
        from (
            select a.* except(limit_day,`适合空运连续天数`,`适合海运连续天数`,`适合铁路连续天数`,`适合卡航连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
                `FBA空运定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,
                `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`,
                `快慢同价利率差_修正`),
                ifNull(a.`FBA空运定价/FBM定价_修正`, 999) as `FBA空运定价/FBM定价_修正`,
                ifNull(a.`FBA卡航定价/FBM定价_修正`, 999) as `FBA卡航定价/FBM定价_修正`,
                ifNull(a.`FBA铁路定价/FBM定价_修正`, 999) as `FBA铁路定价/FBM定价_修正`,
                ifNull(a.`FBA海运定价/FBM定价_修正`, 999) as `FBA海运定价/FBM定价_修正`,
                ifNull(a.`FBA快运定价/FBM定价_修正`, 999) as `FBA快运定价/FBM定价_修正`,
                ifNull(a.`FBA慢运定价/FBM定价_修正`, 999) as `FBA慢运定价/FBM定价_修正`,
                ifNull(a.`空海同价利率差_修正`, 999) as `空海同价利率差_修正`,
                ifNull(a.`空铁同价利率差_修正`, 999) as `空铁同价利率差_修正`,
                ifNull(a.`空卡同价利率差_修正`, 999) as `空卡同价利率差_修正`,
                ifNull(a.`快慢同价利率差_修正`, 999) as `快慢同价利率差_修正`,
                a.`适合空运连续天数`*(ifNull(b.`适合空运连续天数_old`,0)+1) as `适合空运连续天数`,
                a.`适合海运连续天数`*(ifNull(b.`适合海运连续天数_old`,0)+1) as `适合海运连续天数`,
                a.`适合铁路连续天数`*(ifNull(b.`适合铁路连续天数_old`,0)+1) as `适合铁路连续天数`,
                a.`适合卡航连续天数`*(ifNull(b.`适合卡航连续天数_old`,0)+1) as `适合卡航连续天数`,
                a.`适合快运连续天数`*(ifNull(b.`适合快运连续天数_old`,0)+1) as `适合快运连续天数`,
                a.`适合慢运连续天数`*(ifNull(b.`适合慢运连续天数_old`,0)+1) as `适合慢运连续天数`,
                ifNull(b.`FBA空运定价_修正_old`, a.`FBA空运定价_修正`) as `FBA空运定价_修正_old`,
                ifNull(b.`FBA海运定价_修正_old`, a.`FBA海运定价_修正`) as `FBA海运定价_修正_old`,
                ifNull(b.`FBA铁路定价_修正_old`, a.`FBA铁路定价_修正`) as `FBA铁路定价_修正_old`,
                ifNull(b.`FBA卡航定价_修正_old`, a.`FBA卡航定价_修正`) as `FBA卡航定价_修正_old`,
                ifNull(b.`FBA快运定价_修正_old`, a.`FBA快运定价_修正`) as `FBA快运定价_修正_old`,
                ifNull(b.`FBA慢运定价_修正_old`, a.`FBA慢运定价_修正`) as `FBA慢运定价_修正_old`,
                if(`适合空运连续天数`=0, `FBA空运定价_修正_old`, ifNull(b.`FBA空运定价_参考`, a.`FBA空运定价_修正`)) as `FBA空运定价_参考`,
                if(`适合海运连续天数`=0, `FBA海运定价_修正_old`, ifNull(b.`FBA海运定价_参考`, a.`FBA海运定价_修正`)) as `FBA海运定价_参考`,
                if(`适合铁路连续天数`=0, `FBA铁路定价_修正_old`, ifNull(b.`FBA铁路定价_参考`, a.`FBA铁路定价_修正`)) as `FBA铁路定价_参考`,
                if(`适合卡航连续天数`=0, `FBA卡航定价_修正_old`, ifNull(b.`FBA卡航定价_参考`, a.`FBA卡航定价_修正`)) as `FBA卡航定价_参考`,
                if(`适合快运连续天数`=0, `FBA快运定价_修正_old`, ifNull(b.`FBA快运定价_参考`, a.`FBA快运定价_修正`)) as `FBA快运定价_参考`,
                if(`适合慢运连续天数`=0, `FBA慢运定价_修正_old`, ifNull(b.`FBA慢运定价_参考`, a.`FBA慢运定价_修正`)) as `FBA慢运定价_参考`,
                if(`FBA空运定价_参考`=0 or `FBA空运定价_修正` is null or `FBA空运定价_参考` is null, 0, abs(a.`FBA空运定价_修正`-`FBA空运定价_参考`)/`FBA空运定价_参考`) as `FBA空运定价波幅`,
                if(`FBA海运定价_参考`=0 or `FBA海运定价_修正` is null or `FBA海运定价_参考` is null, 0, abs(a.`FBA海运定价_修正`-`FBA海运定价_参考`)/`FBA海运定价_参考`) as `FBA海运定价波幅`,
                if(`FBA铁路定价_参考`=0 or `FBA铁路定价_修正` is null or `FBA铁路定价_参考` is null, 0, abs(a.`FBA铁路定价_修正`-`FBA铁路定价_参考`)/`FBA铁路定价_参考`) as `FBA铁路定价波幅`,
                if(`FBA卡航定价_参考`=0 or `FBA卡航定价_修正` is null or `FBA卡航定价_参考` is null, 0, abs(a.`FBA卡航定价_修正`-`FBA卡航定价_参考`)/`FBA卡航定价_参考`) as `FBA卡航定价波幅`,
                if(`FBA快运定价_参考`=0 or `FBA快运定价_修正` is null or `FBA快运定价_参考` is null, 0, abs(a.`FBA快运定价_修正`-`FBA快运定价_参考`)/`FBA快运定价_参考`) as `FBA快运定价波幅`,
                if(`FBA慢运定价_参考`=0 or `FBA慢运定价_修正` is null or `FBA慢运定价_参考` is null, 0, abs(a.`FBA慢运定价_修正`-`FBA慢运定价_参考`)/`FBA慢运定价_参考`) as `FBA慢运定价波幅`,
                multiIf(
                    `适合快运连续天数` >= 0 and `FBA快运定价波幅`>= 0.1, '快运定价比修正后数值连续一周低于2且FBA快运定价为主要原因，建议可发快运',
                    `适合慢运连续天数` >= 0 and `FBA慢运定价波幅`>= 0.1, '慢运定价比修正后数值连续一周低于2且FBA慢运定价为主要原因，建议可发慢运',
                '无建议') as `发运建议`,
                multiIf(
              (`FBA慢运定价/FBM定价_修正`>2 or (`适合慢运连续天数`<0 and `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`> 0.1)) and a.`站点` in ('阿联酋','日本','沙特'), '0', 
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>=0 and a.`站点` in ('阿联酋','日本','沙特'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`> 2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA快运定价/FBM定价_修正`>2 and a.`站点` in ('新加坡'), '0',
                    `FBA快运定价/FBM定价_修正`<=2 and a.`站点` in ('新加坡'), '6',
                    ((`FBA慢运定价/FBM定价_修正`> 1.8) or (`适合慢运连续天数`< 0 and `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1)) and a.`站点` not in ('日本','新加坡','阿联酋','沙特'), '0',         
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '21;6', 
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` > 1.8, '11',    
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` > 1.8, '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国','英国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国') and `FBA快运定价/FBM定价_修正` > 1.8, '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('英国') and `FBA快运定价/FBM定价_修正` > 1.8, '21',
                    '-1') as `建议物流方式`
            from (
                select sku,`站点`,`FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
                   `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
                   `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
                   `FBA空运定价_修正`, `FBA海运定价_修正`, `FBA铁路定价_修正`, `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`,
                   `空海同价利率差_修正`, `空铁同价利率差_修正`,`空卡同价利率差_修正`,`快慢同价利率差_修正`,
                   if(`站点` in ('阿联酋','新加坡','沙特','日本'), 2, 1.8) as `limit_day`,
                   if(`FBA空运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合空运连续天数`,
                   multiIf(`FBA海运定价/FBM定价_修正` is null, 0, `FBA海运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合海运连续天数`,
                   multiIf(`FBA铁路定价/FBM定价_修正` is null, 0, `FBA铁路定价/FBM定价_修正`<=limit_day, 1, 0) as `适合铁路连续天数`,
                   multiIf(`FBA卡航定价/FBM定价_修正` is null, 0, `FBA卡航定价/FBM定价_修正`<=limit_day, 1, 0) as `适合卡航连续天数`,
                   multiIf(`FBA快运定价/FBM定价_修正` is null, 0, `FBA快运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合快运连续天数`,
                   multiIf(`FBA慢运定价/FBM定价_修正` is null, 0, `FBA慢运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合慢运连续天数`
                from (
                    select a.sku as sku,a.`站点` as `站点`,
                    a.`FBA空运定价/FBM定价` as `FBA空运定价/FBM定价`,a.`FBA卡航定价/FBM定价` as `FBA卡航定价/FBM定价`,
                    a.`FBA海运定价/FBM定价` as `FBA海运定价/FBM定价`,a.`FBA铁路定价/FBM定价` as `FBA铁路定价/FBM定价`,
                    a.`FBA快运定价/FBM定价` as `FBA快运定价/FBM定价`,a.`FBA慢运定价/FBM定价` as `FBA慢运定价/FBM定价`,
                    (a.`成本`+a.`头程费空运`+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA空运定价_修正`,
                    (a.`成本`+a.`运费`)/(1-c.`FBM平台抽成比例`-0.04-a.`FBM目标毛利润率`-a.`FBM税率`)/a.`汇率` as `FBM定价_修正`,
                    `FBA空运定价_修正`/`FBM定价_修正` as `FBA空运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA海运定价_修正`,
                    `FBA海运定价_修正`/`FBM定价_修正` as `FBA海运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA铁路定价_修正`,
                    `FBA铁路定价_修正`/`FBM定价_修正` as `FBA铁路定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA卡航定价_修正`,
                    `FBA卡航定价_修正`/`FBM定价_修正` as `FBA卡航定价/FBM定价_修正`,            
                    (a.`成本`+if(a.`头程费快运`<0, null, a.`头程费快运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA快运定价_修正`,
                    `FBA快运定价_修正`/`FBM定价_修正` as `FBA快运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA慢运定价_修正`,
                    `FBA慢运定价_修正`/`FBM定价_修正` as `FBA慢运定价/FBM定价_修正`,           
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空海利润率反算_修正`,
                    `空海利润率反算_修正`-a.`FBA目标毛利润率` as `空海同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空铁利润率反算_修正`,
                    `空铁利润率反算_修正`-a.`FBA目标毛利润率` as `空铁同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空卡利润率反算_修正`,
                    `空卡利润率反算_修正`-a.`FBA目标毛利润率` as `空卡同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA快运定价_修正`*a.`汇率`) as `快慢利润率反算_修正`,
                    `快慢利润率反算_修正`-a.`FBA目标毛利润率` as `快慢同价利率差_修正`
                    from (
                        select * except({col_list_str6},{col_list_str7}),
                            {col_list_str8},
                            `站点` as `FBA站点`,replace(`站点`, '-泛美', '') as `FBM站点`
                        from (
                            select a.* except ({col_list_str3}),
                                {col_list_str4},
                                {col_list_str5}     
                            from (select * except (id, date, update_time) from support_document.{table_now} where `站点` = '{site}') a 
                            left join (
                                select sku, `站点`,{col_list_str2}
                                from (
                                    {sql_str}
                                )
                                group by sku, `站点`
                            ) b 
                            on a.sku=b.sku and a.`站点`=b.`站点`
                        )
                    ) a 
                    left join (
                        select site `FBA站点`, platform_percentage `FBA平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBA'
                    ) b 
                    on a.`FBA站点`=b.`FBA站点`
                    left join (
                        select site `FBM站点`, platform_percentage `FBM平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBM'
                    ) c 
                    on a.`FBM站点`=c.`FBM站点` 
                ) 
            ) a 
            left join monitor_table b 
            on a.sku=b.sku and a.`站点`=b.`站点`
        )
        """
        conn_mx2.ck_execute_sql(sql)
    print('定价比波幅监控完成！')

def pricing_ratio_monitor_new():
    """
    使用各字段近期平均水平来计算修正数值。如果一两天突然公式中部分因子有大幅度变化，直接计算的定价比会变化很大，但是修正的定价比就不会。
    如果修正后的定价比持续一周都和以往的相差较大，说明各因子已经有了稳定的变化，发运模式就可以建议转变。
    FBA定价的参考值，是修正后的定价比第一次开始适合空运或者海运时候的FBA定价数值，记录下历史中的该值用于一周时和当前修正后的FBA定价
    对比，来判断定价比变化是否主要来源于FBA定价的波动。
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat()
    date_today1 = date_today.replace('-', '')

    # 建表
    table_name = f'pricing_ratio_monitor_{date_today1}'
    # table_name = f'pricing_ratio_monitor_test'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    print('监控结果表删除完成!')
    conn_mx2.ck_execute_sql(sql)
    sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `date` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
            `FBA空运定价/FBM定价_修正` Float64 COMMENT 'FBA空运定价/FBM定价_修正',
            `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
            `FBA海运定价/FBM定价_修正` Float64 COMMENT 'FBA海运定价/FBM定价_修正',
            `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
            `FBA铁路定价/FBM定价_修正` Float64 COMMENT 'FBA铁路定价/FBM定价_修正',
            `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
            `FBA卡航定价/FBM定价_修正` Float64 COMMENT 'FBA卡航定价/FBM定价_修正',
            `FBA慢海定价/FBM定价` Float64 COMMENT 'FBA慢海定价/FBM定价',
            `FBA慢海定价/FBM定价_修正` Float64 COMMENT 'FBA慢海定价/FBM定价_修正',
            `FBA快运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA快运定价比',
            `FBA快运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA快运定价比_修正',
            `FBA慢运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA慢运定价比',
            `FBA慢运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA慢运定价比_修正',
            `FBA空运定价_修正` Float64 COMMENT 'FBA空运定价_修正',
            `FBA海运定价_修正` Float64 COMMENT 'FBA海运定价_修正',
            `FBA铁路定价_修正` Float64 COMMENT 'FBA铁路定价_修正',
            `FBA卡航定价_修正` Float64 COMMENT 'FBA卡航定价_修正',
            `FBA慢海定价_修正` Float64 COMMENT 'FBA慢海定价_修正',
            `FBA快运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA快运定价_修正',
            `FBA慢运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_修正',
            `适合空运连续天数` Int32 COMMENT '适合空运连续天数',
            `适合海运连续天数` Int32 COMMENT '适合海运连续天数',
            `适合铁路连续天数` Int32 COMMENT '适合铁路连续天数',
            `适合卡航连续天数` Int32 COMMENT '适合卡航连续天数',
            `适合慢海连续天数` Int32 COMMENT '适合慢海连续天数',
            `适合快运连续天数` Int32 DEFAULT 0 COMMENT '适合快运连续天数',
            `适合慢运连续天数` Int32 DEFAULT 0 COMMENT '适合慢运连续天数',
            `FBA空运定价_参考` Float64 COMMENT 'FBA空运定价_参考',
            `FBA海运定价_参考` Float64 COMMENT 'FBA海运定价_参考',
            `FBA铁路定价_参考` Float64 COMMENT 'FBA铁路定价_参考',
            `FBA卡航定价_参考` Float64 COMMENT 'FBA卡航定价_参考',
            `FBA慢海定价_参考` Float64 COMMENT 'FBA慢海定价_参考',
            `FBA快运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA快运定价_参考',
            `FBA慢运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_参考',
            `FBA空运定价波幅` Float64 COMMENT 'FBA空运定价波幅',
            `FBA海运定价波幅` Float64 COMMENT 'FBA海运定价波幅',
            `FBA铁路定价波幅` Float64 COMMENT 'FBA铁路定价波幅',
            `FBA卡航定价波幅` Float64 COMMENT 'FBA卡航定价波幅',
            `FBA慢海定价波幅` Float64 COMMENT 'FBA慢海定价波幅',
            `FBA快运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA快运定价波幅',
            `FBA慢运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA慢运定价波幅',
            `空海同价利率差_修正` Float64,
            `空铁同价利率差_修正` Float64,
            `空卡同价利率差_修正` Float64,
            `快海慢海同价利率差_修正` Float64,
            `快慢同价利率差_修正` Float64 DEFAULT 999  COMMENT '快慢同价利率差_修正',
            `建议物流方式` String COMMENT '建议物流方式：11:快海；12:慢海；13:铁路；15:快卡；16:慢卡；17/6:空运；0:不建议转FBA',
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('监控结果表创建完成!')

    # 定价比表准备
    table_now = f'pricing_ratio_{date_today1}'
    sql_table = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_%'
        and table < '{table_now}'
        order by table desc
        limit 3
    """
    df_table = conn_mx2.ck_select_to_df(sql_table)
    #
    table_monitor_now = f'pricing_ratio_monitor_{date_today1}'
    # table_monitor_now = f'pricing_ratio_monitor_20250616'
    sql_table_monitor = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_monitor%'
        and table < '{table_monitor_now}'
        order by table desc
        limit 3
    """
    df_table_monitor = conn_mx2.ck_select_to_df(sql_table_monitor)
    print(df_table_monitor)
    # 1 定价比列
    col_list = ['成本', '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费慢海','头程费快运','头程费慢运','调拨费', 'fba_fees', '运费',
                'FBA差值','FBM差值', 'FBA税率', 'FBM税率', '汇率']
    col_list_str1 = ','.join([f'if(`{item}`<0, null, `{item}`) as `{item}`' for item in col_list])
    col_list_str2 = ','.join([f'toNullable(avg(`{item}`)) as `{item}`' for item in col_list])
    col_list_str3 = ','.join([f'`{item}`' for item in col_list])
    col_list_str4 = ','.join([f'a.`{item}` as `{item}_now`' for item in col_list])
    col_list_str5 = ','.join([f'ifNull(b.`{item}`, a.`{item}`) as `{item}_avg`' for item in col_list])
    col_list_str6 = ','.join([f'`{item}_now`' for item in col_list])
    col_list_str7 = ','.join([f'`{item}_avg`' for item in col_list])
    col_list_str8 = ','.join([
                                 f'if(`{item}_now`>1.2 * `{item}_avg` or `{item}_now`<0.8 * `{item}_avg`, 0.5*(`{item}_now`+`{item}_avg`), `{item}_now`) as `{item}`'
                                 for item in col_list])

    # 2 定价比监控列
    monitor_col_list = ['适合空运连续天数', '适合海运连续天数', '适合铁路连续天数', '适合卡航连续天数','适合慢海连续天数','适合快运连续天数','适合慢运连续天数',
                    'FBA空运定价_修正','FBA海运定价_修正', 'FBA铁路定价_修正', 'FBA卡航定价_修正','FBA慢海定价_修正','FBA快运定价_修正','FBA慢运定价_修正']
    monitor_col_list_str1 = ','.join([f'`{item}` as `{item}_old`' for item in monitor_col_list])

    # 3
    sql = f"""
    select distinct `站点` from support_document.{table_now}
    """
    df_site = conn_mx2.ck_select_to_df(sql)
    for site in tqdm(list(df_site['站点'])):
        # 1. 定价比
        sql_list = []
        for table in list(df_table['table']):
            sql = f"""
            select sku,`站点`,{col_list_str1}
            from support_document.{table}
            where `站点`='{site}'
            """
            sql_list.append(sql)
        sql_str = '\n union all \n'.join(sql_list)
        # 2. 定价比监控
        monitor_sql_list = []
        for table in list(df_table_monitor['table']):
            sql = f"""
            select sku,`站点`,`date`,`FBA空运定价_参考`,`FBA海运定价_参考`,`FBA铁路定价_参考`,`FBA卡航定价_参考`,
            `FBA慢海定价_参考`,`FBA快运定价_参考`,
            `FBA慢运定价_参考`,{monitor_col_list_str1}
            from support_document.{table}
            where `站点`='{site}'
            """
            monitor_sql_list.append(sql)
            monitor_sql_str = '\n union all \n'.join(monitor_sql_list)

        print(f'开始计算{site}数据...')
        sql = f"""
        insert into support_document.{table_name}
        with monitor_table as (
            select * from (
                {monitor_sql_str}
            )
            order by `date` desc limit 1 by sku,`站点`
        )
        select toString(today()) as `date`,sku,`站点`,
            `FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
            `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
            `FBA慢海定价/FBM定价`, `FBA慢海定价/FBM定价_修正`,
            `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
            `FBA空运定价_修正`,ifNull(`FBA海运定价_修正`, 0) as `FBA海运定价_修正`,
            ifNull(`FBA铁路定价_修正`, 0) as `FBA铁路定价_修正`,ifNull(`FBA卡航定价_修正`, 0) as `FBA卡航定价_修正`,
            ifNull(`FBA慢海定价_修正`, 0) as `FBA慢海定价_修正`,
            ifNull(`FBA快运定价_修正`, 0) as `FBA快运定价_修正`,ifNull(`FBA慢运定价_修正`, 0) as `FBA慢运定价_修正`,
            `适合空运连续天数`, `适合海运连续天数`, `适合铁路连续天数`, `适合卡航连续天数`,`适合慢海连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
            `FBA空运定价_参考`, `FBA海运定价_参考`, `FBA铁路定价_参考`, `FBA卡航定价_参考`,`FBA慢海定价_参考`,`FBA快运定价_参考`,`FBA慢运定价_参考`,
            `FBA空运定价波幅`, `FBA海运定价波幅`, `FBA铁路定价波幅`, `FBA卡航定价波幅`,`FBA慢海定价波幅`,`FBA快运定价波幅`,`FBA慢运定价波幅`,
            `空海同价利率差_修正`, `空铁同价利率差_修正`, `空卡同价利率差_修正`,`快海慢海同价利率差_修正`,`快慢同价利率差_修正`, `建议物流方式`,
            toString(now()) as update_time
        from (
            select a.* except(limit_day,`适合空运连续天数`,`适合海运连续天数`,`适合铁路连续天数`,`适合卡航连续天数`,`适合慢海连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
                `FBA空运定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,`FBA慢海定价/FBM定价_修正`,
                `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`,`快海慢海同价利率差_修正`,
                `快慢同价利率差_修正`),
                ifNull(a.`FBA空运定价/FBM定价_修正`, 999) as `FBA空运定价/FBM定价_修正`,
                ifNull(a.`FBA卡航定价/FBM定价_修正`, 999) as `FBA卡航定价/FBM定价_修正`,
                ifNull(a.`FBA铁路定价/FBM定价_修正`, 999) as `FBA铁路定价/FBM定价_修正`,
                ifNull(a.`FBA海运定价/FBM定价_修正`, 999) as `FBA海运定价/FBM定价_修正`,
                ifNull(a.`FBA慢海定价/FBM定价_修正`, 999) as `FBA慢海定价/FBM定价_修正`,
                ifNull(a.`FBA快运定价/FBM定价_修正`, 999) as `FBA快运定价/FBM定价_修正`,
                ifNull(a.`FBA慢运定价/FBM定价_修正`, 999) as `FBA慢运定价/FBM定价_修正`,
                ifNull(a.`空海同价利率差_修正`, 999) as `空海同价利率差_修正`,
                ifNull(a.`空铁同价利率差_修正`, 999) as `空铁同价利率差_修正`,
                ifNull(a.`空卡同价利率差_修正`, 999) as `空卡同价利率差_修正`,
                ifNull(a.`快海慢海同价利率差_修正`, 999) as `快海慢海同价利率差_修正`,
                ifNull(a.`快慢同价利率差_修正`, 999) as `快慢同价利率差_修正`,
                a.`适合空运连续天数`*(ifNull(b.`适合空运连续天数_old`,0)+1) as `适合空运连续天数`,
                a.`适合海运连续天数`*(ifNull(b.`适合海运连续天数_old`,0)+1) as `适合海运连续天数`,
                a.`适合铁路连续天数`*(ifNull(b.`适合铁路连续天数_old`,0)+1) as `适合铁路连续天数`,
                a.`适合卡航连续天数`*(ifNull(b.`适合卡航连续天数_old`,0)+1) as `适合卡航连续天数`,
                a.`适合慢海连续天数`*(ifNull(b.`适合慢海连续天数_old`,0)+1) as `适合慢海连续天数`,
                a.`适合快运连续天数`*(ifNull(b.`适合快运连续天数_old`,0)+1) as `适合快运连续天数`,
                a.`适合慢运连续天数`*(ifNull(b.`适合慢运连续天数_old`,0)+1) as `适合慢运连续天数`,
                ifNull(b.`FBA空运定价_修正_old`, a.`FBA空运定价_修正`) as `FBA空运定价_修正_old`,
                ifNull(b.`FBA海运定价_修正_old`, a.`FBA海运定价_修正`) as `FBA海运定价_修正_old`,
                ifNull(b.`FBA铁路定价_修正_old`, a.`FBA铁路定价_修正`) as `FBA铁路定价_修正_old`,
                ifNull(b.`FBA卡航定价_修正_old`, a.`FBA卡航定价_修正`) as `FBA卡航定价_修正_old`,
                ifNull(b.`FBA慢海定价_修正_old`, a.`FBA慢海定价_修正`) as `FBA慢海定价_修正_old`,
                ifNull(b.`FBA快运定价_修正_old`, a.`FBA快运定价_修正`) as `FBA快运定价_修正_old`,
                ifNull(b.`FBA慢运定价_修正_old`, a.`FBA慢运定价_修正`) as `FBA慢运定价_修正_old`,
                if(`适合空运连续天数`=0, `FBA空运定价_修正_old`, ifNull(b.`FBA空运定价_参考`, a.`FBA空运定价_修正`)) as `FBA空运定价_参考`,
                if(`适合海运连续天数`=0, `FBA海运定价_修正_old`, ifNull(b.`FBA海运定价_参考`, a.`FBA海运定价_修正`)) as `FBA海运定价_参考`,
                if(`适合铁路连续天数`=0, `FBA铁路定价_修正_old`, ifNull(b.`FBA铁路定价_参考`, a.`FBA铁路定价_修正`)) as `FBA铁路定价_参考`,
                if(`适合卡航连续天数`=0, `FBA卡航定价_修正_old`, ifNull(b.`FBA卡航定价_参考`, a.`FBA卡航定价_修正`)) as `FBA卡航定价_参考`,
                if(`适合慢海连续天数`=0, `FBA慢海定价_修正_old`, ifNull(b.`FBA慢海定价_参考`, a.`FBA慢海定价_修正`)) as `FBA慢海定价_参考`,
                if(`适合快运连续天数`=0, `FBA快运定价_修正_old`, ifNull(b.`FBA快运定价_参考`, a.`FBA快运定价_修正`)) as `FBA快运定价_参考`,
                if(`适合慢运连续天数`=0, `FBA慢运定价_修正_old`, ifNull(b.`FBA慢运定价_参考`, a.`FBA慢运定价_修正`)) as `FBA慢运定价_参考`,
                if(`FBA空运定价_参考`=0 or `FBA空运定价_修正` is null or `FBA空运定价_参考` is null, 0, abs(a.`FBA空运定价_修正`-`FBA空运定价_参考`)/`FBA空运定价_参考`) as `FBA空运定价波幅`,
                if(`FBA海运定价_参考`=0 or `FBA海运定价_修正` is null or `FBA海运定价_参考` is null, 0, abs(a.`FBA海运定价_修正`-`FBA海运定价_参考`)/`FBA海运定价_参考`) as `FBA海运定价波幅`,
                if(`FBA铁路定价_参考`=0 or `FBA铁路定价_修正` is null or `FBA铁路定价_参考` is null, 0, abs(a.`FBA铁路定价_修正`-`FBA铁路定价_参考`)/`FBA铁路定价_参考`) as `FBA铁路定价波幅`,
                if(`FBA卡航定价_参考`=0 or `FBA卡航定价_修正` is null or `FBA卡航定价_参考` is null, 0, abs(a.`FBA卡航定价_修正`-`FBA卡航定价_参考`)/`FBA卡航定价_参考`) as `FBA卡航定价波幅`,
                if(`FBA慢海定价_参考`=0 or `FBA慢海定价_修正` is null or `FBA慢海定价_参考` is null, 0, abs(a.`FBA慢海定价_修正`-`FBA慢海定价_参考`)/`FBA慢海定价_参考`) as `FBA慢海定价波幅`,
                if(`FBA快运定价_参考`=0 or `FBA快运定价_修正` is null or `FBA快运定价_参考` is null, 0, abs(a.`FBA快运定价_修正`-`FBA快运定价_参考`)/`FBA快运定价_参考`) as `FBA快运定价波幅`,
                if(`FBA慢运定价_参考`=0 or `FBA慢运定价_修正` is null or `FBA慢运定价_参考` is null, 0, abs(a.`FBA慢运定价_修正`-`FBA慢运定价_参考`)/`FBA慢运定价_参考`) as `FBA慢运定价波幅`,
                multiIf(
                    `适合快运连续天数` >= 0 and `FBA快运定价波幅`>= 0.1, '快运定价比修正后数值连续一周低于2且FBA快运定价为主要原因，建议可发快运',
                    `适合慢运连续天数` >= 0 and `FBA慢运定价波幅`>= 0.1, '慢运定价比修正后数值连续一周低于2且FBA慢运定价为主要原因，建议可发慢运',
                '无建议') as `发运建议`,
                multiIf(
              (`FBA慢运定价/FBM定价_修正`>2 or (`适合慢运连续天数`<0 and `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`> 0.1)) and a.`站点` in ('阿联酋','日本','沙特'), '0', 
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>=0 and a.`站点` in ('阿联酋','日本','沙特'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`> 2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA快运定价/FBM定价_修正`>2 and a.`站点` in ('新加坡'), '0',
                    `FBA快运定价/FBM定价_修正`<=2 and a.`站点` in ('新加坡'), '6',
                    ((`FBA慢运定价/FBM定价_修正`> 1.8) or (`适合慢运连续天数`< 0 and `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1)) and a.`站点` not in ('日本','新加坡','阿联酋','沙特'), '0',         
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '5;6', 
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '5;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` > 1.8, '11',    
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` > 1.8, '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国','英国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国') and `FBA快运定价/FBM定价_修正` > 1.8, '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('英国') and `FBA快运定价/FBM定价_修正` > 1.8, '5',
                    '-1') as `建议物流方式`
            from (
                select sku,`站点`,`FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
                   `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
                   `FBA慢海定价/FBM定价`, `FBA慢海定价/FBM定价_修正`,
                   `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
                   `FBA空运定价_修正`, `FBA海运定价_修正`, `FBA铁路定价_修正`, `FBA卡航定价_修正`,`FBA慢海定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`,
                   `空海同价利率差_修正`, `空铁同价利率差_修正`,`空卡同价利率差_修正`,`快海慢海同价利率差_修正`,`快慢同价利率差_修正`,
                   if(`站点` in ('阿联酋','新加坡','沙特','日本'), 2, 1.8) as `limit_day`,
                   if(`FBA空运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合空运连续天数`,
                   multiIf(`FBA海运定价/FBM定价_修正` is null, 0, `FBA海运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合海运连续天数`,
                   multiIf(`FBA铁路定价/FBM定价_修正` is null, 0, `FBA铁路定价/FBM定价_修正`<=limit_day, 1, 0) as `适合铁路连续天数`,
                   multiIf(`FBA卡航定价/FBM定价_修正` is null, 0, `FBA卡航定价/FBM定价_修正`<=limit_day, 1, 0) as `适合卡航连续天数`,
                   multiIf(`FBA慢海定价/FBM定价_修正` is null, 0, `FBA慢海定价/FBM定价_修正`<=limit_day, 1, 0) as `适合慢海连续天数`,
                   multiIf(`FBA快运定价/FBM定价_修正` is null, 0, `FBA快运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合快运连续天数`,
                   multiIf(`FBA慢运定价/FBM定价_修正` is null, 0, `FBA慢运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合慢运连续天数`
                from (
                    select a.sku as sku,a.`站点` as `站点`,
                    a.`FBA空运定价/FBM定价` as `FBA空运定价/FBM定价`,a.`FBA卡航定价/FBM定价` as `FBA卡航定价/FBM定价`,
                    a.`FBA海运定价/FBM定价` as `FBA海运定价/FBM定价`,a.`FBA铁路定价/FBM定价` as `FBA铁路定价/FBM定价`,
                    a.`FBA慢海定价/FBM定价` as `FBA慢海定价/FBM定价`,
                    a.`FBA快运定价/FBM定价` as `FBA快运定价/FBM定价`,a.`FBA慢运定价/FBM定价` as `FBA慢运定价/FBM定价`,
                    (a.`成本`+a.`头程费空运`+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA空运定价_修正`,
                    (a.`成本`+a.`运费`)/(1-c.`FBM平台抽成比例`-0.04-a.`FBM目标毛利润率`-a.`FBM税率`)/a.`汇率` as `FBM定价_修正`,
                    `FBA空运定价_修正`/`FBM定价_修正` as `FBA空运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA海运定价_修正`,
                    `FBA海运定价_修正`/`FBM定价_修正` as `FBA海运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA铁路定价_修正`,
                    `FBA铁路定价_修正`/`FBM定价_修正` as `FBA铁路定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA卡航定价_修正`,
                    `FBA卡航定价_修正`/`FBM定价_修正` as `FBA卡航定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费慢海`<0, null, a.`头程费慢海`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA慢海定价_修正`,
                    `FBA慢海定价_修正`/`FBM定价_修正` as `FBA慢海定价/FBM定价_修正`,               
                    (a.`成本`+if(a.`头程费快运`<0, null, a.`头程费快运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA快运定价_修正`,
                    `FBA快运定价_修正`/`FBM定价_修正` as `FBA快运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA慢运定价_修正`,
                    `FBA慢运定价_修正`/`FBM定价_修正` as `FBA慢运定价/FBM定价_修正`,           
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空海利润率反算_修正`,
                    `空海利润率反算_修正`-a.`FBA目标毛利润率` as `空海同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空铁利润率反算_修正`,
                    `空铁利润率反算_修正`-a.`FBA目标毛利润率` as `空铁同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空卡利润率反算_修正`,
                    `空卡利润率反算_修正`-a.`FBA目标毛利润率` as `空卡同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费慢海`<0, null, a.`头程费慢海`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA慢运定价_修正`*a.`汇率`) as `快海慢海利润率反算_修正`,
                    `快海慢海利润率反算_修正`-a.`FBA目标毛利润率` as `快海慢海同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA快运定价_修正`*a.`汇率`) as `快慢利润率反算_修正`,
                    `快慢利润率反算_修正`-a.`FBA目标毛利润率` as `快慢同价利率差_修正`
                    from (
                        select * except({col_list_str6},{col_list_str7}),
                            {col_list_str8},
                            `站点` as `FBA站点`,replace(`站点`, '-泛美', '') as `FBM站点`
                        from (
                            select a.* except ({col_list_str3}),
                                {col_list_str4},
                                {col_list_str5}     
                            from (select * except (id, date, update_time) from support_document.{table_now} where `站点` = '{site}') a 
                            left join (
                                select sku, `站点`,{col_list_str2}
                                from (
                                    {sql_str}
                                )
                                group by sku, `站点`
                            ) b 
                            on a.sku=b.sku and a.`站点`=b.`站点`
                        )
                    ) a 
                    left join (
                        select site `FBA站点`, platform_percentage `FBA平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBA'
                    ) b 
                    on a.`FBA站点`=b.`FBA站点`
                    left join (
                        select site `FBM站点`, platform_percentage `FBM平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBM'
                    ) c 
                    on a.`FBM站点`=c.`FBM站点` 
                ) 
            ) a 
            left join monitor_table b 
            on a.sku=b.sku and a.`站点`=b.`站点`
        )
        """
        conn_mx2.ck_execute_sql(sql)
    print('定价比波幅监控完成！')

# 链接维度的定价比计算
def get_listing_account():
    """
    获取账号信息，以便分批获取链接信息
    """
    #
    sql = f"""
        SELECT distinct account_id, site
        FROM (
            SELECT 
                distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site,
                case 
                    when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
                    else c.site1
                end as site2
            FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
            LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
            ON toInt32(a.account_id) = toInt32(b.id)
            LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon c
            ON b.site = c.site
            WHERE a.fulfillment_channel = 'AMA'
        )
    """
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)

    return df_account

def get_listing_data(df_account):
    """
    获取FBA全量链接。
    同时获取链接维度的：平台佣金率、尾程运费
    """
    date_today = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    # 获取FBA全量链接信息
    # 由于数量较大，采用分批取数的方式

    time1 = time.time()
    account_tuple = tuple(df_account['account_id'].unique())
    sql = f"""
        SELECT 
            f.account_id, f.asin, f.seller_sku as seller_sku, a.seller_sku as fbm_seller, e.sku, f.site, f.site2, f.country_code,
            case when r.fba_commission_rate is Null then 0.15 else r.fba_commission_rate end fba_commission_rate, 
            case when r2.fbm_commission_rate is Null then fba_commission_rate else r2.fbm_commission_rate end fbm_commission_rate,
            cost, first_trip_source, first_trip_fee_rmb, toucheng_kongyun,toucheng_haiyun,toucheng_tl,
            toucheng_kh, e.fba_fees_rmb
        FROM (
            SELECT 
                distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site, c.site2 country_code,
                case 
                    when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
                    else c.site1
                end as site2
            FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
            LEFT JOIN yibai_system_kd_sync.yibai_amazon_account b
            ON toInt32(a.account_id) = toInt32(b.id)
            LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon c
            ON b.site = c.site
            WHERE a.fulfillment_channel = 'AMA' and a.account_id in {account_tuple}
        ) f
        INNER JOIN (
            SELECT 
                account_id, seller_sku, asin, sku, cost, length, width, high, weight, first_trip_source, fba_fees*rate as fba_fees_rmb,
                rate,first_trip_fee_rmb,toucheng_kongyun,toucheng_haiyun,toucheng_tl,toucheng_kh 
            FROM yibai_fba.fba_fees
            WHERE account_id in {account_tuple}
        ) e
        ON f.account_id = e.account_id and f.seller_sku = e.seller_sku and f.asin = e.asin
        LEFT JOIN (
            SELECT distinct account_id, asin1 as asin, seller_sku
            FROM yibai_product_kd_sync.yibai_amazon_listing_alls
            WHERE fulfillment_channel != 'AMA' and account_id in {account_tuple}
        ) a 
        ON f.account_id = a.account_id and f.asin = a.asin
        LEFT JOIN (
            SELECT account_id, seller_sku, `佣金率` fba_commission_rate
            FROM yibai_fba.yibai_amazon_referral_fee
            WHERE account_id in {account_tuple} 
        ) r
        ON f.account_id = r.account_id and f.seller_sku = r.seller_sku
        LEFT JOIN (
            SELECT account_id, seller_sku, `佣金率` fbm_commission_rate
            FROM yibai_fba.yibai_amazon_referral_fee
            WHERE account_id in {account_tuple} 
        ) r2 ON f.account_id = r2.account_id and a.seller_sku = r2.seller_sku
        ORDER BY f.account_id DESC, f.asin DESC, f.seller_sku DESC
        LIMIT 1 BY f.account_id, f.asin, f.seller_sku
    """
    df_listing = conn_ck.ck_select_to_df(sql)
    df_listing.columns = [i.split('.')[-1] for i in df_listing.columns]
    time2 = time.time()
    t = time2 - time1
    print(f"FBA链接信息获取完成. 共耗时{'%.0f' % t}s, 链接数量为{len(df_listing)}")
    return df_listing

def get_price(df):
    df['fba_ky_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['toucheng_kongyun'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.04)
    df['fba_hy_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['toucheng_haiyun'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.04)
    df['fba_tl_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['toucheng_tl'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.04)
    df['fba_kh_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['toucheng_kh'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.04)
    df['fba_fast_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['头程费快运'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.04)
    df['fba_slow_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['头程费慢运'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.04)
    df['fbm_price_rmb'] = (df['cost'] + df['fbm运费']) / (
                1 - df['fbm_commission_rate'] - 0.04 - df['fbm_diff'] - df['FBM_profit'] - df['FBM税率'])
    df['air_price_ratio'] = df['fba_ky_price_rmb'] / df['fbm_price_rmb']
    df['fast_price_ratio'] = df['fba_fast_price_rmb'] / df['fbm_price_rmb']
    df['slow_price_ratio'] = df['fba_slow_price_rmb'] / df['fbm_price_rmb']
    df['fast_and_slow_rate_diff'] = 1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA税率'] - 0.04 - \
                                    (df['cost']+df['fba_fees_rmb']+df['头程费慢运']+2)/df['fba_fast_price_rmb'] - df['FBA_profit']

    col = ['fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb', 'fba_kh_price_rmb','fba_fast_price_rmb',
           'fba_slow_price_rmb','fbm_price_rmb']
    df[col] = df[col].astype(float).round(2)
    col = ['fast_price_ratio','slow_price_ratio', 'fast_and_slow_rate_diff']
    df[col] = df[col].astype(float).round(4)
    return df


def pricing_ratio_listing():
    """
    链接维度的定价比数据计算：
    1、按站点分批获取FBA链接信息。数据量过大时，按账号
    2、获取不同头程的运费数据
    3、计算链接维度的定价、定价比
    4、输出建议物流方式
    """
    # 表准备
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    table_name = f'pricing_ratio_listing_{date_today}'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表删除成功！')
    sql = f"""
    CREATE TABLE IF NOT EXISTS support_document.{table_name}
    (
        `id` Int64,
        `date` String,
        `account_id` Int64,
        `asin` String,
        `seller_sku` String,
        `fbm_seller` String,
        `sku` String,
        `site` String COMMENT '站点',
        `头程费快运` Nullable(Float64) ,
        `头程费慢运` Nullable(Float64) ,
        `fbm运费` Nullable(Float64) ,
        `fba_fast_price_rmb` Nullable(Float64) ,
        `fba_slow_price_rmb` Nullable(Float64) ,
        `fbm_price_rmb` Nullable(Float64) ,
        `air_price_ratio` Nullable(Float64) COMMENT '空运定价比',
        `fast_price_ratio` Nullable(Float64) COMMENT '快运定价比',
        `slow_price_ratio` Nullable(Float64) COMMENT '慢运定价比',
        `fast_and_slow_rate_diff` Nullable(Float64) COMMENT '快慢同价利率差',
        `适合快运连续天数` Int64 ,
        `适合慢运连续天数` Int64 ,
        `proposed_transport` String COMMENT '建议物流方式',
        `update_time` String DEFAULT toString(now()) COMMENT '更新时间'
    )
    ENGINE = MergeTree
    ORDER BY (sku, `site`)
    SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('结果表建立成功！')
    sql = f"""
        ALTER TABLE support_document.{table_name}
        DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表今日数据删除成功！')
    #
    df_account = get_listing_account()
    print(f'店铺总数量：{len(df_account)}')
    df_account_all = pd.DataFrame()
    for key, group in df_account.groupby(['site']):
        group = group.reset_index(drop=True)
        group['index'] = group.index
        # 每次计算60万条
        group['index'] = group['index'].apply(lambda m: int(m / 400))
        if key not in ['印度', '瑞典', '荷兰', '比利时']:
            df_account_all = pd.concat([group, df_account_all])
        else:
            continue
    # 抽样
    # df_account_all = df_account_all.sample(100)
    # df_account_all = df_account_all[df_account_all['site'] == '加拿大']
    #
    sql_table = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_listing_%'
        and table < \'{table_name}\'
        order by table desc
        limit 1
    """
    df_table = conn_mx2.ck_select_to_df(sql_table)
    #
    # 结果表汇总
    df_result_all = pd.DataFrame()
    for (site, key), df_temp in tqdm(df_account_all.groupby(['site', 'index']), desc='process: '):
        print(f'开始计算{site}站点数据，本次为第{key}批次，账号数量共{len(df_temp)}')
        df_listing_temp = get_listing_data(df_temp)
        if len(df_listing_temp) == 0:
            continue
        #
        # 匹配快运、慢运运费数据
        #
        df_listing_temp['站点'] = df_listing_temp['site']
        # df_fba_kuaiyun = FBA_jisuan(df_listing_temp, df_site_df, fbafee_type='快运')
        df_listing_temp.rename(columns={"sku": "sku1"}, inplace=True)
        conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
        if len(df_listing_temp[~df_listing_temp['站点'].isin(['德国', '英国'])]) != 0:
            result_ky = aut(df_listing_temp[~df_listing_temp['站点'].isin(['德国', '英国'])], conn_mx, '空运')
        else:
            result_ky = pd.DataFrame(columns=['sku1', 'country_code', 'fba头程属性'])
        if len(df_listing_temp[df_listing_temp['站点'].isin(['德国', '英国'])]) != 0:
            result_kh = aut(df_listing_temp[df_listing_temp['站点'].isin(['德国', '英国'])], conn_mx, '卡航')
        else:
            result_kh = pd.DataFrame(columns=['sku1', 'country_code', 'fba头程属性'])
        result = pd.concat([result_ky, result_kh])
        #
        df_listing = df_listing_temp.merge(result, on=["sku1", "country_code"], how='left')
        df_listing['fba头程属性'].fillna('普货', inplace=True)
        df_listing.rename(columns={"sku1": "sku"}, inplace=True)

        #
        # 匹配快运、慢运的运费，只需要SKU+site维度的数据
        col = ['sku', 'site', '站点', 'country_code', 'cost', 'fba头程属性']
        df_listing = df_listing[col]
        df_listing = df_listing.drop_duplicates(subset=['sku', 'site'], keep='first')
        #
        df_listing['fbafee计算方式'] = '普通'
        df_listing['最小购买数量'] = 1
        df_listing['数量'] = 1
        df_listing['站点1'] = df_listing['站点']
        #
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
        df_listing = chicun_zhongliang(df_listing, Organization=1, conn_mx=conn_mx)
        #
        df_fast = tou_cheng_dingjiabi(df_listing, conn, dingjia_type='快运')
        df_fast = df_fast[['sku', 'site', '头程费_人民币']]
        df_fast.columns = ['sku', 'site', '头程费快运']
        #
        df_slow = tou_cheng_dingjiabi(df_listing, conn, dingjia_type='慢运')
        df_slow = df_slow[['sku', 'site', '头程费_人民币']]
        df_slow.columns = ['sku', 'site', '头程费慢运']
        #
        # FBM运费
        df_fbm = df_listing.copy()
        # df_fbm['站点'] = df_fbm['站点FBM']
        df_fbm['发货仓库'] = None
        df_fbm['物流类型'] = None
        df_jie_kou = freight_interface_fu(df_fbm, table_name='freight_interface_amazon')
        df_jie_kou = df_jie_kou.sort_values(by=['sku', '站点', 'warehouse_id', 'ship_name'],
                                            ascending=[True, True, True, True])
        df_jie_kou = df_jie_kou.drop_duplicates(subset=['sku', '站点'], keep='first')
        df_jie_kou = df_jie_kou.rename(columns={'运费': 'fbm运费'})
        #
        df_listing_temp.rename(columns={"sku1": "sku"}, inplace=True)
        df_result = pd.merge(df_listing_temp, df_fast, how='left', on=['sku', 'site'])
        df_result = pd.merge(df_result, df_slow, how='left', on=['sku', 'site'])
        df_result = pd.merge(df_result, df_jie_kou[['sku', '站点', 'fbm运费']], how='left', on=['sku', '站点'])

        # 差值
        df_diff = get_diff()
        #
        df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
            ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
        df_diff_fba = df_diff_fba.rename(columns={'net_profit2': 'fba_diff', 'net_interest_rate_target': 'FBA_profit'})
        df_diff_fba[['fba_diff', 'FBA_profit']] = (df_diff_fba[['fba_diff', 'FBA_profit']] / 100).astype(float)
        df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
            ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
        df_diff_fbm = df_diff_fbm.rename(columns={'net_profit2': 'fbm_diff', 'net_interest_rate_target': 'FBM_profit'})
        df_diff_fbm[['fbm_diff', 'FBM_profit']] = (df_diff_fbm[['fbm_diff', 'FBM_profit']] / 100).astype(float)
        #
        df_result = merge_first_product_line(df_result)
        df_result = get_cost_range(df_result, df_diff_fba)
        df_result = merge_four_dim_diff(df_result, df_diff_fba, ['site', 'first_product_line', 'cost_range'])
        #
        df_result.drop('cost_range', axis=1, inplace=True)
        df_result = get_cost_range(df_result, df_diff_fbm)
        df_result = merge_four_dim_diff(df_result, df_diff_fbm, ['site', 'first_product_line', 'cost_range'])


        df_result.drop('cost_range', axis=1, inplace=True)
        col = ['fba_diff', 'FBA_profit', 'fbm_diff', 'FBM_profit']
        df_result[col] = df_result[col].astype(float)

        df_result = shuilv(df_result, fb_type='FBA')
        df_result = df_result.rename(columns={'税率': 'FBA税率'})
        df_result = shuilv(df_result, fb_type='FBM')
        df_result = df_result.rename(columns={'税率': 'FBM税率'})

        # 计算定价
        # print('开始计算定价比...')
        df_result = get_price(df_result)

        ratio_limit = 1.8
        col = ['英国', '德国', '法国', '西班牙', '意大利', '美国', '加拿大', '墨西哥', '澳大利亚']
        c1 = (df_result['site'].isin(col)) & (df_result['fast_price_ratio'] <= ratio_limit)
        c2 = (df_result['site'].isin(['波兰', '日本', '阿联酋', '沙特', '新加坡'])) & (
                    df_result['fast_price_ratio'] <= 2)
        df_result['适合快运连续天数'] = np.select([c1, c2], [1, 1], 0)

        c1 = (df_result['site'].isin(col)) & (df_result['slow_price_ratio'] <= ratio_limit)
        c2 = (df_result['site'].isin(['波兰', '日本', '阿联酋', '沙特', '新加坡'])) & (
                    df_result['slow_price_ratio'] <= 2)
        df_result['适合慢运连续天数'] = np.select([c1, c2], [1, 1], 0)

        # 取前一天的定价比数据表, 更新适合*运连续天数
        sql = f"""
            SELECT account_id, seller_sku, sku,  `适合快运连续天数` `适合快运连续天数_前一日`, `适合慢运连续天数` `适合慢运连续天数_前一日`
            FROM support_document.{df_table.iloc[0, 0]}
            WHERE account_id in {tuple(df_temp['account_id'].unique())} and site in '{site}'
        """
        conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
        df_result_y = conn_mx2.ck_select_to_df(sql)
        if len(df_result_y) == 0:
            df_result_y = pd.DataFrame(columns=['account_id', 'seller_sku', 'sku','适合快运连续天数_前一日','适合慢运连续天数_前一日'])
        df_result = pd.merge(df_result, df_result_y, how='left', on=['account_id', 'seller_sku', 'sku'])
        df_result[['适合快运连续天数_前一日', '适合慢运连续天数_前一日']] = df_result[
            ['适合快运连续天数_前一日', '适合慢运连续天数_前一日']].fillna(0).astype(int)
        df_result['适合快运连续天数'] = df_result['适合快运连续天数'] + df_result['适合快运连续天数'] * \
                                        df_result['适合快运连续天数_前一日']
        df_result['适合慢运连续天数'] = df_result['适合慢运连续天数'] + df_result['适合慢运连续天数'] * \
                                        df_result['适合慢运连续天数_前一日']
        # df_result.drop(['适合快运连续天数_前一日', '适合慢运连续天数_前一日'], axis=1, inplace=True)

        ratio_limit = 1.8
        # c1 = (df_result['site'].isin(['英国', '德国', '法国', '西班牙', '意大利'])) & \
        #      (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.1)
        c101 = (df_result['site'].isin(['英国', '德国', '法国', '西班牙', '意大利'])) & \
               (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
               (df_result['fast_price_ratio'] <= ratio_limit)
        c102 = (df_result['site'].isin(['英国'])) & \
               (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
               (df_result['fast_price_ratio'] > ratio_limit)
        c103 = (df_result['site'].isin(['德国', '法国', '西班牙', '意大利'])) & \
               (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
               (df_result['fast_price_ratio'] > ratio_limit)
        c2 = (df_result['site'].isin(['美国', '加拿大', '墨西哥', '澳大利亚'])) & (df_result['fast_price_ratio'] <= ratio_limit) & \
             (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.1)
        c201 = (df_result['site'].isin(['美国'])) & (df_result['fast_price_ratio'] > ratio_limit) & \
             (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.1)
        c202 = (df_result['site'].isin(['加拿大', '墨西哥', '澳大利亚'])) & (df_result['fast_price_ratio'] > ratio_limit) & \
             (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.1)
        c3 = (df_result['site'].isin(['阿联酋', '波兰','日本','沙特'])) & (df_result['fast_price_ratio'] <= 2) & \
             (df_result['slow_price_ratio'] <= 2) & (df_result['fast_and_slow_rate_diff'] <= 0.1)
        c301 = (df_result['site'].isin(['阿联酋','日本','沙特'])) & (df_result['fast_price_ratio'] > ratio_limit) & \
             (df_result['slow_price_ratio'] <= 2) & (df_result['fast_and_slow_rate_diff'] <= 0.1)
        c302 = (df_result['site'].isin(['波兰'])) & (df_result['fast_price_ratio'] > ratio_limit) & \
             (df_result['slow_price_ratio'] <= 2) & (df_result['fast_and_slow_rate_diff'] <= 0.1)
        c4 = (df_result['site'].isin(['新加坡'])) & (df_result['fast_price_ratio'] <= 2)
        # c5 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] <= 2)

        c5 = (df_result['site'].isin(['英国'])) & (df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 0)
        c6 = (df_result['site'].isin(['德国', '法国', '西班牙', '意大利'])) & (
                    df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 0)
        c7 = (df_result['site'].isin(['美国'])) & (df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 0)
        c8 = (df_result['site'].isin(['加拿大', '墨西哥', '澳大利亚'])) & (
                    df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 0)
        c9 = (df_result['site'].isin(['阿联酋','日本','沙特'])) & (df_result['slow_price_ratio'] <= 2) & \
             (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 0)
        c901 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] <= 2) & \
               (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
               (df_result['适合慢运连续天数'] >= 0)

        c10 = (df_result['site'].isin(['英国'])) & (
                df_result['slow_price_ratio'] <= ratio_limit) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 0)
        c1001 = (df_result['site'].isin(['德国', '法国', '西班牙', '意大利'])) & (
                df_result['slow_price_ratio'] <= ratio_limit) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 0)
        c11 = (df_result['site'].isin(['美国'])) & (df_result['slow_price_ratio'] <= ratio_limit) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 0)
        c12 = (df_result['site'].isin(['加拿大', '墨西哥', '澳大利亚'])) & (
                    df_result['slow_price_ratio'] <= ratio_limit) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 0)
        c13 = (df_result['site'].isin(['阿联酋','日本','沙特'])) & (df_result['slow_price_ratio'] <= 2) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 0)
        c131 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] <= 2) & \
               (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 0)

        col = ['英国', '德国', '法国', '西班牙', '意大利', '美国', '加拿大', '墨西哥', '澳大利亚']
        c14 = ((df_result['site'].isin(col)) & (df_result['slow_price_ratio'] > ratio_limit)) | (
                df_result['适合慢运连续天数'] < 0)
        c15 = ((df_result['site'].isin(['阿联酋','日本','沙特'])) & (df_result['slow_price_ratio'] > 2)) | (
                    df_result['适合慢运连续天数'] < 0)
        c16 = (df_result['site'].isin(['新加坡'])) & (df_result['fast_price_ratio'] > 2)
        c17 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] > 2)

        df_result['proposed_transport'] = np.select(
            [c101, c102, c103, c2, c201, c202, c3, c301, c302, c4,
             c5, c6, c7, c8, c9, c901,
             c10, c1001, c11, c12, c13, c131,
             c14, c15, c16, c17],
            ['6', '5', '5', '6', '11', '21', '6', '21', '21', '6',
             '5;6', '5;6', '11;6', '21;6', '21;6', '21;6',
             '5', '5', '11', '21', '21', '21',
             '0', '0', '0', '0'])

        #
        df_result['date'] = time.strftime('%Y-%m-%d')
        col = ['account_id', 'asin', 'seller_sku', 'fbm_seller', 'sku', 'site', '头程费快运', '头程费慢运', 'fbm运费',
               'fba_fast_price_rmb','fba_slow_price_rmb', 'fbm_price_rmb', 'air_price_ratio', 'fast_price_ratio',
               'slow_price_ratio', 'fast_and_slow_rate_diff',
               '适合快运连续天数', '适合慢运连续天数', 'proposed_transport', 'date']

        # df_result_all = pd.concat([df_result, df_result_all])
        #
        # print(df_result[col].info())
        df_result = df_result.drop_duplicates(subset=['account_id','asin','seller_sku'])

        conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
        conn_mx2.write_to_ck_json_type(df_result[col], table_name)
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='support_document')
        # ck_client.write_to_ck_json_type(table_name, df_result[col])

def center_interface_listing(group, item):
    """
    中台SKU定价比接口调用函数
    """
    # url = 'http://salescenter.yibainetwork.com:91/apis/open/amazon_open_api/set_sku_price_data?org_code=org_00001'
    # url = 'http://192.168.86.142:100/apis/open/amazon_price_link_rate_api/save_data?org_code=org_00001'
    url = 'http://salescenter.yibainetwork.com:91/apis/open/amazon_price_link_rate_api/save_data?org_code=org_00001'

    # 出现上传失败数据时，剔除异常数据后，重复上传
    n = 0
    while n < 5:
        try:
            data_post0 = []
            for i in range(len(group['sku'])):
                data_dict = {
                    "asin": str(group['asin'].iloc[i]),
                    "sku": str(group['sku'].iloc[i]),
                    "account_id": int(group['account_id'].iloc[i]),
                    "fba_seller_sku": str(group['seller_sku'].iloc[i]),
                    "fbm_seller_sku": str(group['fbm_seller'].iloc[i]),
                    "tc_fast_freight": float(group['头程费快运'].iloc[i]),
                    "tc_slow_freight": float(group['头程费慢运'].iloc[i]),
                    "fbm_freight": float(group['fbm运费'].iloc[i]),
                    "fba_fast_price_rmb": float(group['fba_fast_price_rmb'].iloc[i]),
                    "fba_slow_price_rmb": float(group['fba_slow_price_rmb'].iloc[i]),
                    "fbm_price_rmb": float(group['fbm_price_rmb'].iloc[i]),
                    "fast_price_ratio": float(group['fast_price_ratio'].iloc[i]),
                    "slow_price_ratio": float(group['slow_price_ratio'].iloc[i]),
                    "air_price_ratio": float(group['air_price_ratio'].iloc[i]),
                    "fast_and_slow_rate_diff": float(group['fast_and_slow_rate_diff'].iloc[i]),
                    "fast_delivery_day": str(group['适合快运连续天数'].iloc[i]),
                    "slow_delivery_day": float(group['适合慢运连续天数'].iloc[i]),
                    "calculate_date": str(group['date'].iloc[i]),
                    "proposed_transport": str(group['proposed_transport'].iloc[i])
                }
                # print(data_dict)
                data_post0.append(data_dict)
            res = requests.post(url, json=data_post0).json()
            # print(res)
            if res['status'] == 1:
                if len(res['data_list']) > 0:
                    df_err = pd.DataFrame(
                        {'status': res['status'], 'error_list': res['data_list'], 'error_mess': res['error_mess'],
                         'record': time.strftime('%Y-%m-%d')})
                    df_err['account_id'] = df_err['error_list'].apply(lambda x: x[0]).astype(int)
                    df_err['seller_sku'] = df_err['error_list'].apply(lambda x: x[1])
                    df_err['err_reason'] = df_err['error_list'].apply(lambda x: x[2])
                    # 报错数据存表
                    print(df_err.info())
                    conn_ck = CkClient(user='zengzhijie', password='ze65nG_zHij5ie', host='121.37.30.78', port='9001',
                                       db_name='yibai_oversea')
                    conn_ck.write_to_ck_json_type('pricing_ratio_listing_err_log', df_err)

                    # 剔除报错数据后重新上传
                    df_err_data = df_err[['account_id', 'seller_sku', 'status']].drop_duplicates(
                        subset=['account_id', 'seller_sku'])
                    group = pd.merge(group, df_err_data[['account_id', 'seller_sku', 'status']], how='left',
                                     on=['account_id', 'seller_sku'])
                    group = group[group['status'].isna()]
                    group = group.reset_index(drop=True)
                    group['index'] = group.index
                break
            else:
                if len(res['data_list']) > 0:
                    df_err = pd.DataFrame(
                        {'status': res['status'], 'error_list': res['data_list'], 'error_mess': res['error_mess'],
                         'record': time.strftime('%Y-%m-%d')})
                    df_err['account_id'] = df_err['error_list'].apply(lambda x: x[0]).astype(int)
                    df_err['seller_sku'] = df_err['error_list'].apply(lambda x: x[1])
                    df_err['err_reason'] = df_err['error_list'].apply(lambda x: x[2])
                    # 报错数据存表
                    print(df_err.info())
                    conn_ck = CkClient(user='zengzhijie', password='ze65nG_zHij5ie', host='121.37.30.78', port='9001',
                                       db_name='yibai_oversea')
                    conn_ck.write_to_ck_json_type('pricing_ratio_listing_err_log', df_err)

                    # 剔除报错数据后重新上传
                    df_err_data = df_err[['account_id', 'seller_sku', 'status']].drop_duplicates(
                        subset=['account_id', 'seller_sku'])
                    group = pd.merge(group, df_err_data[['account_id', 'seller_sku', 'status']], how='left',
                                     on=['account_id', 'seller_sku'])
                    group = group[group['status'].isna()]
                    group = group.reset_index(drop=True)
                    group['index'] = group.index
                # print(data_post0)
                n += 1
        except:
            print(f'定价比{item}:接口失败，重新上传')
            time.sleep(10)
            n += 1


##
def upload_listing_pricing_ratio():
    """
    需将链接维度的定价比数据上传至中台
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = time.strftime('%Y%m%d')
    # date_today = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    # 取定价比数据
    sql = f"""
        SELECT
            distinct site
        FROM support_document.pricing_ratio_listing_{date_today}
    """
    df_site = conn_ck.ck_select_to_df(sql)
    # df.iloc[:, 8:17] = df.iloc[:, 8:17].fillna(-999)
    # df = df[~df['asin'].isna()]
    # df.drop('id', axis=1, inplace=True)

    #
    # 表准备
    table_name = 'pricing_ratio_to_center_listing'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    print('监控结果表删除完成!')
    conn_ck.ck_execute_sql(sql)
    sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
        `id` int,
        `date` String,
        `account_id` Int64,
        `asin` String,
        `seller_sku` String,
        `fbm_seller` String,
        `sku` String,
        `site` String COMMENT '站点',
        `头程费快运` Nullable(Float64),
        `头程费慢运` Nullable(Float64),
        `fbm运费` Nullable(Float64),
        `fba_fast_price_rmb` Nullable(Float64),
        `fba_slow_price_rmb` Nullable(Float64),
        `fbm_price_rmb` Nullable(Float64),
        `air_price_ratio` Nullable(Float64) COMMENT '空运定价比',
        `fast_price_ratio` Nullable(Float64) COMMENT '快运定价比',
        `slow_price_ratio` Nullable(Float64) COMMENT '慢运定价比',
        `fast_and_slow_rate_diff` Nullable(Float64) COMMENT '快慢同价利率差',
        `适合快运连续天数` Int64,
        `适合慢运连续天数` Int64,
        `proposed_transport` String COMMENT '建议物流方式：11:快海；12:慢海；13:铁路；15:快卡；16:慢卡；17/6:空运；0:不建议转FBA',
        `update_time` String DEFAULT toString(now()) COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `site`)
        SETTINGS index_granularity = 8192
        """
    conn_ck.ck_create_table(sql)
    print('结果表建立成功！')
    #
    conn_ck.ck_execute_sql(sql=f'truncate table support_document.{table_name}')
    n = 1
    while n < 5:
        print(f'删除当前表里的数据，第{n}次测试...')
        sql = f"""
            SELECT count()
            FROM support_document.{table_name}
        """
        df_cnt = conn_ck.ck_select_to_df(sql)
        if df_cnt.iloc[0,0] == 0:
            print('结果表删除成功！')
            break
        else:
            n += 1
            time.sleep(60)

    # 存表并上传
    print('数据开始上传至中台...')
    for i in tqdm(df_site['site'].unique()):
        # 取定价比数据
        sql = f"""
            SELECT
                *
            FROM support_document.pricing_ratio_listing_{date_today}
            WHERE site = '{i}'
        """
        df = conn_ck.ck_select_to_df(sql)
        df.iloc[:, 8:18] = df.iloc[:, 8:18].fillna(-999)
        df = df[~df['asin'].isna()]
        df.drop('id', axis=1, inplace=True)
        conn_ck.ck_insert(df, table_name, if_exist='append')
        print(f'{i}数据存表完成！')

        print(f"开始上传{i}数据, 共{len(df)}条")
        # df_temp = df[df['site'] == i]
        # 转化为字典格式
        df = df.reset_index(drop=True)
        df['index'] = df.index
        df['index'] = df['index'].apply(lambda m_data: int(m_data / 1000))

        # for key, group in df.groupby(['index']):
        #     center_interface_listing(group, key)
        # 多线程上传
        threadPool = ThreadPoolExecutor(max_workers=10)
        thread_list = []
        for key, group in df.groupby(['index']):
            group = group.reset_index(drop=True)
            future = threadPool.submit(center_interface_listing, group, key)
            thread_list.append(future)

    # time.sleep(60)
    # 20240521 新增监控
    date_today = time.strftime('%Y%m%d')
    sql = f"""
        SELECT date, site as `站点`, proposed_transport as `建议物流方式`, count(1) as `数量`, 'listing' as `维度` 
        FROM support_document.pricing_ratio_listing_{date_today} 
        GROUP BY date, site, proposed_transport
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_2 = conn_ck.ck_select_to_df(sql)
    df_2['建议物流方式2'] = df_2['建议物流方式'].map(map_values)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date = time.strftime('%Y-%m-%d')
    table_name = 'ads_pricing_ratio'
    sql = f"""
    DELETE FROM over_sea.{table_name} WHERE date = '{date}' and `维度` = 'listing'
    """
    conn.execute(sql)
    conn.close()
    write_to_sql(df_2, 'ads_pricing_ratio')
    # 测试


def add_kunbang_tt():
    conn_mx2 = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    # conn_mx2 = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    sql = """
    select distinct `站点`,sku 
    from (
        select distinct c.site1 as `站点`,a.sku as sku
        from (
            select distinct account_id,sku 
            from tt_product_kd_sync.tt_amazon_sku_map 
            where sku like '%%*%%' or sku like '%%+%%'
        ) a 
        left join (
            select toInt64(id) as account_id,site from tt_system_kd_sync.tt_amazon_account
        ) b 
        on a.account_id=b.account_id
        left join domestic_warehouse_clear.tt_site_table_amazon c 
        on b.site=c.site
    )
    -- 筛选欧洲仅德国站点
    where `站点` not in ('比利时','法国','意大利','西班牙','荷兰','波兰','瑞典','土耳其')
    """
    df2 = conn_mx2.ck_select_to_df(sql)
    # df2['site'] = df2['站点']
    # df2 = df2.sample(1000)
    print('添加捆绑sku完成！')
    # print(df2['站点'].value_counts())
    return df2
def tt_get_sku():
    # conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='调价明细历史数据t')
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # 产品表里取所有sku（筛选在售 product_status = 9 国内仓属性 attr_value_id = 67）
    t1 = time.time()
    sql = """
        WITH sku_table as (
            SELECT sku
            FROM domestic_warehouse_clear.domestic_warehouse_age
            WHERE stock > 0
        )

        SELECT DISTINCT sku 
        FROM yibai_prod_base_sync.yibai_prod_sku 
        where 
            -- develop_source in (14, 15, 22)
            -- title_cn like '%通拓%' 
            (product_status  = 9 or (product_status != 9 and sku in (select distinct sku from sku_table)))
        """
    df = conn_ck.ck_select_to_df(sql)
    df['is_bundle'] = 0
    df_kunbang = add_kunbang_tt()
    df_kunbang['is_bundle'] = 1
    df = pd.concat([df, df_kunbang])
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量', '重量来源'], axis=1, inplace=True)
    df[['重量', '成本', '长', '宽', '高']] = df[['重量', '成本', '长', '宽', '高']].astype(float)
    t2 = time.time()
    # df = df.sample(10000)
    print(f'获取sku完成，共{len(df)}条！共耗时{t2 - t1:.2f}s')

    return df

def tt_get_diff():
    # conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='调价明细历史数据t')
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    # 四维差值 之后使用需要进行多次匹配来修正各个字段信息从而能够最终用所有字段进行匹配
    sql_diff = """
        select * 
        from tt_sale_center_listing_sync.tt_listing_profit_config
        where is_del=0 and status=1 and platform_code='AMAZON'
        ORDER BY update_date DESC
        LIMIT 1 by shipping_type, site, first_product_line, cost_range
    """
    df_diff = conn_ck.ck_select_to_df(sql_diff)
    df_diff['site'] = df_diff['site'].replace('GB','UK')
    df_diff = df_diff.drop_duplicates(subset=['first_product_line', 'cost_range', 'site','shipping_type'])
    sql_site_table = """
        select site2 as site_en, site1 as site from domestic_warehouse_clear.yibai_site_table_amazon
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_site_table = conn_mx.ck_select_to_df(sql_site_table)
    df_diff = df_diff.rename(columns={'site': 'site_en'}).merge(df_site_table, on='site_en', how='left')
    df_diff['站点'] = df_diff.apply(lambda x: 'other' if x['site_en'] == 'other' else x['site'], axis=1)
    df_diff.dropna(inplace=True)
    return df_diff

def tt_merge_first_product_line(df):
    print(f'匹配第一产品线...')
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    # conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='调价明细历史数据t')
    # 一级产品线
    sql = """
        SELECT 
            id as product_category_id, category_id_level_1 as first_product_line
        FROM tt_prod_base_sync.tt_prod_category
    """
    df_line = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_line, how='left', on='product_category_id')
    df.drop('product_category_id', axis=1, inplace=True)
    df['first_product_line'] = df['first_product_line'].fillna(0).astype(int)
    print('匹配第一产品线完成！')
    return df

def tt_get_fba_first_fee(df_temp, df_diff, i):
    # 差值fba
    t1 = time.time()
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    # df_fba = get_cost_range(df_temp, df_diff_fba)
    df_fba = df_temp.copy()
    df_fba['cost_range'] = 'other'
    df_fba['first_product_line'] = -1
    df_fba = merge_four_dim_diff(df_fba, df_diff_fba, ['站点', 'first_product_line', 'cost_range'])
    df_fba['net_profit2'] = (df_fba['net_profit2'] / 100).astype('float')
    df_fba['net_interest_rate_target'] = (df_fba['net_interest_rate_target'] / 100).astype('float')
    df_fba['FBA目标毛利润率'] = df_fba['net_profit2'] + df_fba['net_interest_rate_target']
    df_fba['FBA差值'] = df_fba['net_profit2']
    col = ['sku', '站点', '重量', '成本', '长', '宽', '高', 'FBA目标毛利润率', 'FBA差值']
    df_fba = df_fba[col]
    # 获取运费
    # 优化头程计算代码
    # 0、获取头程属性。 输入 sku1 + contry_code ,输出 fba头程属性
    # 1、各站点都有空运。['德国','印度','新加坡','沙特']没有海运
    #
    sql = """
        select site1 as `站点`,site3 as country_code FROM domestic_warehouse_clear.yibai_site_table_amazon
        """
    df_code = conn_ck.ck_select_to_df(sql)
    df_fba = pd.merge(df_fba, df_code, how='left', on=['站点'])
    # 头程属性
    # for i in df_site['site1'].unique():
    df_fba['sku1'] = df_fba['sku']
    # aut_ky = aut(df_fba, conn_ck, '空运')
    # df_fba = pd.merge(df_fba, aut_ky, how='left', on=['sku1', 'country_code'])
    # # df_fba.drop(['sku1'], axis=1, inplace=True)
    # df_fba['fba头程属性'] = df_fba['fba头程属性'].fillna('普货')
    df_fba['fba头程属性'] = '普货'
    #
    df_fba['站点1'] = df_fba['站点']
    df_kongyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '空运')
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    #
    if i not in ['印度', '新加坡', '沙特']:
        # aut_hy = aut(df_fba, conn_ck, '海运')
        # df_fba = pd.merge(df_fba, aut_hy, how='left', on=['sku1', 'country_code'])
        df_fba['fba头程属性'] = '普货'
        df_haiyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '海运')
        df_haiyun = df_haiyun[['sku', '站点', '头程费_海运']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_haiyun = pd.DataFrame(columns=['sku', '站点', '头程费_海运'])
    if i in ['德国']:
        # aut_tl = aut(df_fba, conn_ck, '铁路')
        # df_fba = pd.merge(df_fba, aut_tl, how='left', on=['sku1', 'country_code'])
        df_fba['fba头程属性'] = '普货'
        df_tielu = tou_cheng_api(df_fba, 'FBA', conn_ck, '铁路')
        df_tielu = df_tielu[['sku', '站点', '头程费_铁路']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_tielu = pd.DataFrame(columns=['sku', '站点', '头程费_铁路'])
    if i in ['英国', '德国']:
        # aut_ly = aut(df_fba, conn_ck, '卡航')
        # df_fba = pd.merge(df_fba, aut_ly, how='left', on=['sku1', 'country_code'])
        df_fba['fba头程属性'] = '普货'
        df_luyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '卡航')
        df_luyun = df_luyun[['sku', '站点', '头程费_卡航']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_luyun = pd.DataFrame(columns=['sku', '站点', '头程费_卡航'])

    # 快运\慢运
    # df_fba.drop(['fba头程属性_x','fba头程属性_y'], axis=1, inplace=True)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # if i in ['英国', '德国']:
    #     aut_fast = aut(df_fba, conn_ck, '卡航')
    # else:
    #     aut_fast = aut(df_fba, conn_ck, '空运')
    # df_fba = pd.merge(df_fba, aut_fast, how='left', on=['sku1', 'country_code'])
    df_fba['fba头程属性'] = '普货'
    df_kuaiyun = tou_cheng_dingjiabi(df_fba, conn, '快运')
    df_kuaiyun = df_kuaiyun.rename(columns={'头程费_人民币': '头程费_快运'})
    df_kuaiyun = df_kuaiyun[['sku', '站点', '头程费_快运']]
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    #

    # aut_slow = aut(df_fba, conn_ck, '海运')
    # df_fba = pd.merge(df_fba, aut_slow, how='left', on=['sku1', 'country_code'])
    df_fba['fba头程属性'] = '普货'
    df_manyun = tou_cheng_dingjiabi(df_fba, conn, '慢运')
    df_manyun = df_manyun.rename(columns={'头程费_人民币': '头程费_慢运'})
    df_manyun = df_manyun[['sku', '站点', '头程费_慢运']]
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    t2 = time.time()
    #
    df_price = pd.merge(df_fba[['sku', '站点', '成本', 'FBA目标毛利润率', 'FBA差值']],
                        df_kongyun[['sku', '站点', '头程费_空运']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_haiyun[['sku', '站点', '头程费_海运']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_tielu[['sku', '站点', '头程费_铁路']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_luyun[['sku', '站点', '头程费_卡航']], how='left', on=['sku', '站点'])
    df_price = pd.merge(df_price, df_kuaiyun[['sku', '站点', '头程费_快运']], how='left',
                        on=['sku', '站点'])
    df_price = pd.merge(df_price, df_manyun[['sku', '站点', '头程费_慢运']], how='left', on=['sku', '站点'])
    print(f'fba头程运费获取完成，共耗时{t2 - t1:.2f}s')
    return df_price

def tt_get_fbm_fee(df_temp, df_diff, i):
    """
    获取国内仓运费数据，包括捆绑sku的运费
    """
    t1 = time.time()
    df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    # df_fbm = get_cost_range(df_temp, df_diff_fbm)
    df_fbm = df_temp.copy()
    df_fbm['cost_range'] = 'other'
    df_fbm['first_product_line'] = -1
    df_fbm = merge_four_dim_diff(df_fbm, df_diff_fbm, ['站点', 'first_product_line', 'cost_range'])
    df_fbm['net_profit2'] = (df_fbm['net_profit2'] / 100).astype('float')
    df_fbm['net_interest_rate_target'] = (df_fbm['net_interest_rate_target'] / 100).astype('float')
    df_fbm['FBM目标毛利润率'] = df_fbm['net_profit2'] + df_fbm['net_interest_rate_target']
    df_fbm['FBM差值'] = df_fbm['net_profit2']
    col = ['sku', 'is_bundle', '站点', '重量', '成本', '长', '宽', '高', 'FBM目标毛利润率', 'FBM差值']
    df_fbm = df_fbm[col]

    conn_ck = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器')
    # conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='调价明细历史数据t')
    sql = f"""
        SELECT distinct sku,ship_name,warehouse_id,total_cost as `运费`,site as `站点`  
        from domestic_warehouse_clear.freight_interface_amazon 
        where site='{i}' 
        order by total_cost asc limit 1 by sku
        """
    sql = f"""
        SELECT distinct sku,ship_name,warehouse_id,total_cost as `运费`,site as `站点`  
        from yibai_temp_hxx.freight_interface_amazon 
        where site='{i}' 
        order by total_cost asc limit 1 by sku
        """
    df_yunfei0 = conn_ck.ck_select_to_df(sql)


    df_fbm = pd.merge(df_fbm, df_yunfei0, how='left', on=['sku', '站点'])

    # 捆绑sku的运费
    sql = """
        SELECT sku, total_cost, site as `站点`
        FROM (
            SELECT sku, warehouse_id, total_cost, site, update_date
            FROM yibai_temp_hxx.freight_interface_amazon2
            UNION ALL
            SELECT sku, warehouse_id, total_cost, site, update_date
            FROM yibai_temp_hxx.freight_interface_amazon3
        ) a
        ORDER BY a.update_date DESC, a.total_cost ASC
        LIMIT 1 BY a.sku, a.site
    """
    df_kunbang_fee = conn_ck.ck_select_to_df(sql)
    #
    df_fbm = pd.merge(df_fbm, df_kunbang_fee, how='left', on=['sku', '站点'])
    df_fbm['运费'] = np.where(df_fbm['is_bundle'] == 1, df_fbm['total_cost'], df_fbm['运费'])
    t2 = time.time()
    print(f'国内仓运费获取完成，共耗时{t2 - t1:.2f}s')
    return df_fbm


def tt_pricing_ratio():
    """
    通拓定价比计算程序
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    # 表准备
    # table_name = f'pricing_ratio_test'
    table_name = f'tt_pricing_ratio_{date_today}'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表删除成功！')
    sql = f"""
    CREATE TABLE IF NOT EXISTS support_document.{table_name}
    (
        `id` Int64,
        `date` String,
        `sku` String,
        `站点` String COMMENT '站点',
        `税号` String COMMENT '针对墨西哥站点，企业0%税率，个人9%税率',
        `FBA目标毛利润率` Float64 COMMENT 'FBA目标毛利润率',
        `FBA空运定价` Float64 COMMENT 'FBA空运定价',
        `FBA海运定价` Float64 COMMENT 'FBA海运定价',
        `FBA铁路定价` Float64 COMMENT 'FBA铁路定价',
        `FBA卡航定价` Float64 COMMENT 'FBA卡航定价',
        `FBA快运定价` Float64 COMMENT 'FBA快运定价',
        `FBA慢运定价` Float64 COMMENT 'FBA慢运定价',
        `FBM目标毛利润率` Float64 COMMENT 'FBM目标毛利润率',
        `FBM定价` Float64 COMMENT 'FBM定价',
        `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
        `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
        `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
        `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
        `FBA快运定价/FBM定价` Float64 COMMENT 'FBA快运定价/FBM定价',
        `FBA慢运定价/FBM定价` Float64 COMMENT 'FBA慢运定价/FBM定价',
        `成本` Float64 COMMENT '成本',
        `头程费空运` Float64 COMMENT '头程费空运',
        `头程费海运` Float64 COMMENT '头程费海运',
        `头程费铁路` Float64 COMMENT '头程费铁路',
        `头程费卡航` Float64 COMMENT '头程费卡航',
        `头程费快运` Float64 COMMENT '头程费快运',
        `头程费慢运` Float64 COMMENT '头程费慢运',
        `调拨费` Float64 COMMENT '调拨费',
        `fba_fees` Float64 COMMENT '尾程',
        `运费` Float64 COMMENT 'fbm运费',
        `FBA差值` Float64 COMMENT 'FBA差值',
        `FBM差值` Float64 COMMENT 'FBM差值',
        `FBA税率` Float64 COMMENT 'FBA税率',
        `FBM税率` Float64 COMMENT 'FBM税率',
        `汇率` Float64 COMMENT '汇率',
        `空海利润率反算` Nullable(Float64),
        `空海同价利率差` Nullable(Float64),
        `空铁利润率反算` Nullable(Float64),
        `空铁同价利率差` Nullable(Float64),
        `空卡利润率反算` Nullable(Float64),
        `空卡同价利率差` Nullable(Float64),
        `快慢利润率反算` Nullable(Float64),
        `快慢同价利率差` Nullable(Float64),
        `update_time` String DEFAULT toString(now()) COMMENT '更新时间'
    )
    ENGINE = MergeTree
    ORDER BY (sku, `站点`)
    SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('结果表建立成功！')
    sql = f"""
        ALTER TABLE support_document.{table_name}
        DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表今日数据删除成功！')
    sql = """
        SELECT 
            site, site1
        from domestic_warehouse_clear.yibai_site_table_amazon 
        where site in ('us', 'ca', 'mx', 'de', 'uk', 'au', 'ae', 'sa','sg') 
        -- where site not in ('be', 'fr', 'it', 'sp', 'nl', 'pl', 'se', 'tr','br','in')
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_site = conn_ck.ck_select_to_df(sql)
    #
    df = tt_get_sku()
    # df_sku['is_bundle'] = 0
    # df_kunbang = add_kunbang_tt()
    # df_kunbang['is_bundle'] = 1
    # df = pd.concat([df_sku, df_kunbang])

    # df = tt_merge_first_product_line(df_sku)
    df['first_product_line'] = -1
    # 四维差值

    df_diff = tt_get_diff()
    # 默认参数：平台佣金、汇率
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select * from domestic_warehouse_clear.price_calculate_parameters
    """
    df_site_df = conn_ck.ck_select_to_df(sql)
    df_site_df = amazon_fbm_para(df_site_df, [], pd.DataFrame(), mode='AMAZON-FBA')
    #
    # 主循环。获取FBA运费、FBM运费、计算定价比
    for i in tqdm(df_site['site1'].unique()):
        print(f'开始计算{i}站点数据...')
        df.loc[df['is_bundle'] == 0, '站点'] = i
        df_temp = df[df['站点'] == i]
        #
        # fba头程运费
        df_price = tt_get_fba_first_fee(df_temp, df_diff, i)
        #
        # fba尾程运费
        df_temp['fbafee计算方式'] = '普通'
        df_temp['最小购买数量'] = 1
        df_fba_fee = fba_ding_jia_biao(df_temp)
        print(f"尾程费为空的数量有{len(df_fba_fee[(df_fba_fee['fba_fees'].isna()) | (df_fba_fee['fba_fees'] == '')])}")
        df_fba_fee['fba_fees'] = pd.to_numeric(df_fba_fee['fba_fees'], errors='coerce')
        df_fba_fee['fba_fees'] = df_fba_fee['fba_fees'].fillna(0).astype(float)
        # FBM运费
        df_fbm = tt_get_fbm_fee(df_temp, df_diff, i)
        print(f"FBM运费获取完成，共{len(df_fbm)}条.")
        # 定价计算
        df_price = pd.merge(df_price, df_fba_fee[['sku', '站点', 'fba_fees']], how='left', on=['sku', '站点'])
        df_price = pd.merge(df_price, df_fbm[['sku', '站点', '运费', 'FBM目标毛利润率', 'FBM差值']], how='left',
                            on=['sku', '站点'])
        df_price = pd.merge(df_price, df_site_df[['站点', '汇率', '平台抽成比例', '冗余系数']], how='left',
                            on=['站点'])
        df_price = df_price.drop_duplicates()
        #
        if i != '墨西哥':
            tax_rates_fba = {
                '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
                '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187
            }
            tax_rates_fbm = {
                '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
                '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187, '印度': 0.3, '土耳其': 0.15,
                '澳大利亚': 0.09
            }
            df_price['FBA税率'] = df_price['站点'].map(tax_rates_fba)
            df_price['FBM税率'] = df_price['站点'].map(tax_rates_fbm)
            df_price[['FBA税率', 'FBM税率']] = df_price[['FBA税率', 'FBM税率']].fillna(0).astype(float)
            df_price['税号'] = ''
        else:
            df_price_1 = df_price.copy()
            df_price_1['FBA税率'] = 0
            df_price_1['FBM税率'] = 0
            df_price_1['税号'] = '企业税'
            df_price['FBA税率'] = 0.09
            df_price['FBM税率'] = 0.09
            df_price['税号'] = '个人税'
            df_price = pd.concat([df_price_1, df_price])

        #
        df_price['调拨费'] = 0
        for fbafee_type in ['空运', '海运', '铁路', '卡航', '快运', '慢运']:
            df_price[f"FBA{fbafee_type}定价"] = (df_price["成本"] + df_price[f"头程费_{fbafee_type}"] + df_price[
                "调拨费"] + 2 + df_price["fba_fees"] * df_price["汇率"]) / (
                                                        1 - df_price["平台抽成比例"] - df_price["冗余系数"] -
                                                        df_price["FBA目标毛利润率"] - df_price["FBA税率"]) / \
                                                df_price["汇率"]
            df_price[f"FBA{fbafee_type}定价"] = df_price[f"FBA{fbafee_type}定价"].astype('float')
        df_price["FBM定价"] = (df_price["成本"] + df_price["运费"]) / (
                1 - df_price["平台抽成比例"] - df_price["冗余系数"] - df_price["FBM目标毛利润率"] - df_price[
            "FBM税率"]) / df_price["汇率"]
        #
        dic = {'头程费_海运': '头程费海运', '头程费_空运': '头程费空运', '头程费_铁路': '头程费铁路',
               '头程费_卡航': '头程费卡航',
               '头程费_快运': '头程费快运', '头程费_慢运': '头程费慢运'}
        df_price = df_price.rename(columns=dic)
        #
        df_price['空海利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费海运'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空海同价利率差'] = df_price['空海利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空铁利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费铁路'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空铁同价利率差'] = df_price['空铁利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空卡利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费卡航'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空卡同价利率差'] = df_price['空卡利润率反算'] - df_price['FBA目标毛利润率']
        df_price['快慢利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费慢运'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA快运定价'] * df_price['汇率'])
        df_price['快慢同价利率差'] = df_price['快慢利润率反算'] - df_price['FBA目标毛利润率']
        df_price['FBA空运定价/FBM定价'] = df_price['FBA空运定价'] / df_price['FBM定价']
        df_price['FBA海运定价/FBM定价'] = df_price['FBA海运定价'] / df_price['FBM定价']
        df_price['FBA铁路定价/FBM定价'] = df_price['FBA铁路定价'] / df_price['FBM定价']
        df_price['FBA卡航定价/FBM定价'] = df_price['FBA卡航定价'] / df_price['FBM定价']
        df_price['FBA快运定价/FBM定价'] = df_price['FBA快运定价'] / df_price['FBM定价']
        df_price['FBA慢运定价/FBM定价'] = df_price['FBA慢运定价'] / df_price['FBM定价']
        df_price['date'] = time.strftime('%Y-%m-%d')

        #
        # 数值处理
        # df_price = df_price[df_price['fba_fees'] != '不符合FBA物流条件']
        df_res = df_price[
            ['date', 'sku', '站点', '税号', 'FBA目标毛利润率', 'FBA空运定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价',
             'FBA快运定价', 'FBA慢运定价', 'FBM目标毛利润率', 'FBM定价', 'FBA空运定价/FBM定价', 'FBA海运定价/FBM定价',
             'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价', 'FBA慢运定价/FBM定价', '成本',
             '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运', '调拨费',
             'fba_fees', '运费', 'FBA差值', 'FBM差值', 'FBA税率', 'FBM税率', '汇率', '空海利润率反算',
             '空海同价利率差', '空铁利润率反算', '空铁同价利率差', '空卡利润率反算', '空卡同价利率差',
             '快慢利润率反算', '快慢同价利率差']]
        col = ['成本', '运费', '调拨费', 'fba_fees', 'FBA空运定价/FBM定价', '空海利润率反算', '空海同价利率差',
               '空铁利润率反算',
               '空铁同价利率差', '空卡利润率反算', '空卡同价利率差', '快慢利润率反算', '快慢同价利率差']
        df_res[col] = df_res[col].astype(float).round(4)
        col = ['FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价',
               'FBA慢运定价/FBM定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价', 'FBA快运定价', 'FBA慢运定价',
               '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运']
        df_res[col] = df_res[col].astype('float').round(4).fillna(-999)
        #
        # df_res.to_excel('df_res.xlsx', index=0)
        # 最终结果存表
        df_upload = df_res[(~df_res['FBA空运定价'].isna()) & (~df_res['运费'].isna()) & (df_res['fba_fees'] != 0)]
        print(f'结果数据存表，共{len(df_upload)}条.')
        if len(df_upload) > 100000:
            conn_mx2.ck_insert(df_upload.iloc[0:100000], table_name, if_exist='append')
            conn_mx2.ck_insert(df_upload.iloc[100000:], table_name, if_exist='append')
        else:
            conn_mx2.ck_insert(df_upload, table_name, if_exist='append')

        # 异常数据存表
        # 表准备
        # table_name = f'pricing_ratio_test_{date_today}'
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df_price_null = df_res[(df_res['FBA空运定价'].isna()) | (df_res['运费'].isna()) | (df_res['fba_fees'] == 0)]
        sql = f"""
            ALTER TABLE yibai_oversea.tt_pricing_ratio_null_data
            DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
        """
        conn_ck.ck_execute_sql(sql)
        print(f'异常数据存表，共{len(df_price_null)}条.')
        try:
            conn_ck.ck_insert(df_price_null, 'tt_pricing_ratio_null_data', if_exist='append')
        except Exception as e:
            print(f"异常数据存表失败: {e}")

        print('done!')

def tt_pricing_ratio_monitor():
    """
    使用各字段近期平均水平来计算修正数值。如果一两天突然公式中部分因子有大幅度变化，直接计算的定价比会变化很大，但是修正的定价比就不会。
    如果修正后的定价比持续一周都和以往的相差较大，说明各因子已经有了稳定的变化，发运模式就可以建议转变。
    FBA定价的参考值，是修正后的定价比第一次开始适合空运或者海运时候的FBA定价数值，记录下历史中的该值用于一周时和当前修正后的FBA定价
    对比，来判断定价比变化是否主要来源于FBA定价的波动。
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat()
    date_today1 = date_today.replace('-', '')
    # 建表
    table_name = f'tt_pricing_ratio_monitor_{date_today1}'
    # table_name = f'pricing_ratio_monitor_test'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    print('监控结果表删除完成!')
    conn_mx2.ck_execute_sql(sql)
    sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `date` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `税号` String COMMENT '针对墨西哥站点，企业0%税率，个人9%税率',
            `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
            `FBA空运定价/FBM定价_修正` Float64 COMMENT 'FBA空运定价/FBM定价_修正',
            `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
            `FBA海运定价/FBM定价_修正` Float64 COMMENT 'FBA海运定价/FBM定价_修正',
            `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
            `FBA铁路定价/FBM定价_修正` Float64 COMMENT 'FBA铁路定价/FBM定价_修正',
            `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
            `FBA卡航定价/FBM定价_修正` Float64 COMMENT 'FBA卡航定价/FBM定价_修正',
            `FBA快运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA快运定价比',
            `FBA快运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA快运定价比_修正',
            `FBA慢运定价/FBM定价` Float64 DEFAULT -999 COMMENT 'FBA慢运定价比',
            `FBA慢运定价/FBM定价_修正` Float64 DEFAULT 999 COMMENT 'FBA慢运定价比_修正',
            `FBA空运定价_修正` Float64 COMMENT 'FBA空运定价_修正',
            `FBA海运定价_修正` Float64 COMMENT 'FBA海运定价_修正',
            `FBA铁路定价_修正` Float64 COMMENT 'FBA铁路定价_修正',
            `FBA卡航定价_修正` Float64 COMMENT 'FBA卡航定价_修正',
            `FBA快运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA快运定价_修正',
            `FBA慢运定价_修正` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_修正',
            `适合空运连续天数` Int32 COMMENT '适合空运连续天数',
            `适合海运连续天数` Int32 COMMENT '适合海运连续天数',
            `适合铁路连续天数` Int32 COMMENT '适合铁路连续天数',
            `适合卡航连续天数` Int32 COMMENT '适合卡航连续天数',
            `适合快运连续天数` Int32 DEFAULT 0 COMMENT '适合快运连续天数',
            `适合慢运连续天数` Int32 DEFAULT 0 COMMENT '适合慢运连续天数',
            `FBA空运定价_参考` Float64 COMMENT 'FBA空运定价_参考',
            `FBA海运定价_参考` Float64 COMMENT 'FBA海运定价_参考',
            `FBA铁路定价_参考` Float64 COMMENT 'FBA铁路定价_参考',
            `FBA卡航定价_参考` Float64 COMMENT 'FBA卡航定价_参考',
            `FBA快运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA快运定价_参考',
            `FBA慢运定价_参考` Float64 DEFAULT 0 COMMENT 'FBA慢运定价_参考',
            `FBA空运定价波幅` Float64 COMMENT 'FBA空运定价波幅',
            `FBA海运定价波幅` Float64 COMMENT 'FBA海运定价波幅',
            `FBA铁路定价波幅` Float64 COMMENT 'FBA铁路定价波幅',
            `FBA卡航定价波幅` Float64 COMMENT 'FBA卡航定价波幅',
            `FBA快运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA快运定价波幅',
            `FBA慢运定价波幅` Float64 DEFAULT 0  COMMENT 'FBA慢运定价波幅',
            `空海同价利率差_修正` Float64,
            `空铁同价利率差_修正` Float64,
            `空卡同价利率差_修正` Float64,
            `快慢同价利率差_修正` Float64 DEFAULT 999  COMMENT '快慢同价利率差_修正',
            `建议物流方式` String COMMENT '建议物流方式：11:快海；12:慢海；13:铁路；15:快卡；16:慢卡；17/6:空运；0:不建议转FBA',
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('监控结果表创建完成!')

    # 定价比表准备
    table_now = f'tt_pricing_ratio_{date_today1}'
    sql_table = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'tt_pricing_ratio_2%'
        -- and table < '{table_now}'
        order by table desc
        limit 10
    """
    df_table = conn_mx2.ck_select_to_df(sql_table)
    #
    table_monitor_now = f'tt_pricing_ratio_monitor_{date_today1}'
    sql_table_monitor = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'tt_pricing_ratio_monitor%'
        -- and table < '{table_monitor_now}'
        order by table desc
        limit 2
    """
    df_table_monitor = conn_mx2.ck_select_to_df(sql_table_monitor)
    # 1 定价比列
    col_list = ['成本', '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运','头程费慢运','调拨费', 'fba_fees', '运费',
                'FBA差值','FBM差值', 'FBA税率', 'FBM税率', '汇率']
    col_list_str1 = ','.join([f'if(`{item}`<0, null, `{item}`) as `{item}`' for item in col_list])
    col_list_str2 = ','.join([f'toNullable(avg(`{item}`)) as `{item}`' for item in col_list])
    col_list_str3 = ','.join([f'`{item}`' for item in col_list])
    col_list_str4 = ','.join([f'a.`{item}` as `{item}_now`' for item in col_list])
    col_list_str5 = ','.join([f'ifNull(b.`{item}`, a.`{item}`) as `{item}_avg`' for item in col_list])
    col_list_str6 = ','.join([f'`{item}_now`' for item in col_list])
    col_list_str7 = ','.join([f'`{item}_avg`' for item in col_list])
    col_list_str8 = ','.join([
                                 f'if(`{item}_now`>1.2 * `{item}_avg` or `{item}_now`<0.8 * `{item}_avg`, 0.5*(`{item}_now`+`{item}_avg`), `{item}_now`) as `{item}`'
                                 for item in col_list])

    # 2 定价比监控列
    monitor_col_list = ['适合空运连续天数', '适合海运连续天数', '适合铁路连续天数', '适合卡航连续天数','适合快运连续天数','适合慢运连续天数',
                    'FBA空运定价_修正','FBA海运定价_修正', 'FBA铁路定价_修正', 'FBA卡航定价_修正','FBA快运定价_修正','FBA慢运定价_修正']
    monitor_col_list_str1 = ','.join([f'`{item}` as `{item}_old`' for item in monitor_col_list])

    # 3
    sql = f"""
    select distinct `站点`,`税号` from support_document.{table_now}
    """
    df_site = conn_mx2.ck_select_to_df(sql)
    for site, key in tqdm(zip(list(df_site['站点']), list(df_site['税号']))):
        # 1. 定价比
        sql_list = []
        for table in list(df_table['table']):
            sql = f"""
            select sku,`站点`,`税号`,{col_list_str1}
            from support_document.{table}
            where `站点`='{site}' and `税号`='{key}'
            """
            sql_list.append(sql)
        sql_str = '\n union all \n'.join(sql_list)
        # 2. 定价比监控
        monitor_sql_list = []
        for table in list(df_table_monitor['table']):
            sql = f"""
            select sku,`站点`,`税号`,`date`,`FBA空运定价_参考`,`FBA海运定价_参考`,`FBA铁路定价_参考`,`FBA卡航定价_参考`,`FBA快运定价_参考`,
            `FBA慢运定价_参考`,{monitor_col_list_str1}
            from support_document.{table}
            where `站点`='{site}' and `税号`='{key}'
            """
            monitor_sql_list.append(sql)
            monitor_sql_str = '\n union all \n'.join(monitor_sql_list)

        print(f'开始计算{site}数据...')
        sql = f"""
        insert into support_document.{table_name}
        with monitor_table as (
            select * from (
                {monitor_sql_str}
            )
            order by `date` desc limit 1 by sku,`站点`,`税号`
        )
        select toString(today()) as `date`,sku,`站点`,`税号`,
            `FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
            `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
            `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
            `FBA空运定价_修正`,ifNull(`FBA海运定价_修正`, 0) as `FBA海运定价_修正`,
            ifNull(`FBA铁路定价_修正`, 0) as `FBA铁路定价_修正`,ifNull(`FBA卡航定价_修正`, 0) as `FBA卡航定价_修正`,
            ifNull(`FBA快运定价_修正`, 0) as `FBA快运定价_修正`,ifNull(`FBA慢运定价_修正`, 0) as `FBA慢运定价_修正`,
            `适合空运连续天数`, `适合海运连续天数`, `适合铁路连续天数`, `适合卡航连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
            `FBA空运定价_参考`, `FBA海运定价_参考`, `FBA铁路定价_参考`, `FBA卡航定价_参考`,`FBA快运定价_参考`,`FBA慢运定价_参考`,
            `FBA空运定价波幅`, `FBA海运定价波幅`, `FBA铁路定价波幅`, `FBA卡航定价波幅`,`FBA快运定价波幅`,`FBA慢运定价波幅`,
            `空海同价利率差_修正`, `空铁同价利率差_修正`, `空卡同价利率差_修正`,`快慢同价利率差_修正`, `建议物流方式`,
            toString(now()) as update_time
        from (
            select a.* except(limit_day,`适合空运连续天数`,`适合海运连续天数`,`适合铁路连续天数`,`适合卡航连续天数`,`适合快运连续天数`,`适合慢运连续天数`,
                `FBA空运定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,
                `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`,
                `快慢同价利率差_修正`),
                ifNull(a.`FBA空运定价/FBM定价_修正`, 999) as `FBA空运定价/FBM定价_修正`,
                ifNull(a.`FBA卡航定价/FBM定价_修正`, 999) as `FBA卡航定价/FBM定价_修正`,
                ifNull(a.`FBA铁路定价/FBM定价_修正`, 999) as `FBA铁路定价/FBM定价_修正`,
                ifNull(a.`FBA海运定价/FBM定价_修正`, 999) as `FBA海运定价/FBM定价_修正`,
                ifNull(a.`FBA快运定价/FBM定价_修正`, 999) as `FBA快运定价/FBM定价_修正`,
                ifNull(a.`FBA慢运定价/FBM定价_修正`, 999) as `FBA慢运定价/FBM定价_修正`,
                ifNull(a.`空海同价利率差_修正`, 999) as `空海同价利率差_修正`,
                ifNull(a.`空铁同价利率差_修正`, 999) as `空铁同价利率差_修正`,
                ifNull(a.`空卡同价利率差_修正`, 999) as `空卡同价利率差_修正`,
                ifNull(a.`快慢同价利率差_修正`, 999) as `快慢同价利率差_修正`,
                a.`适合空运连续天数`*(ifNull(b.`适合空运连续天数_old`,0)+1) as `适合空运连续天数`,
                a.`适合海运连续天数`*(ifNull(b.`适合海运连续天数_old`,0)+1) as `适合海运连续天数`,
                a.`适合铁路连续天数`*(ifNull(b.`适合铁路连续天数_old`,0)+1) as `适合铁路连续天数`,
                a.`适合卡航连续天数`*(ifNull(b.`适合卡航连续天数_old`,0)+1) as `适合卡航连续天数`,
                a.`适合快运连续天数`*(ifNull(b.`适合快运连续天数_old`,0)+1) as `适合快运连续天数`,
                a.`适合慢运连续天数`*(ifNull(b.`适合慢运连续天数_old`,0)+1) as `适合慢运连续天数`,
                ifNull(b.`FBA空运定价_修正_old`, a.`FBA空运定价_修正`) as `FBA空运定价_修正_old`,
                ifNull(b.`FBA海运定价_修正_old`, a.`FBA海运定价_修正`) as `FBA海运定价_修正_old`,
                ifNull(b.`FBA铁路定价_修正_old`, a.`FBA铁路定价_修正`) as `FBA铁路定价_修正_old`,
                ifNull(b.`FBA卡航定价_修正_old`, a.`FBA卡航定价_修正`) as `FBA卡航定价_修正_old`,
                ifNull(b.`FBA快运定价_修正_old`, a.`FBA快运定价_修正`) as `FBA快运定价_修正_old`,
                ifNull(b.`FBA慢运定价_修正_old`, a.`FBA慢运定价_修正`) as `FBA慢运定价_修正_old`,
                if(`适合空运连续天数`=0, `FBA空运定价_修正_old`, ifNull(b.`FBA空运定价_参考`, a.`FBA空运定价_修正`)) as `FBA空运定价_参考`,
                if(`适合海运连续天数`=0, `FBA海运定价_修正_old`, ifNull(b.`FBA海运定价_参考`, a.`FBA海运定价_修正`)) as `FBA海运定价_参考`,
                if(`适合铁路连续天数`=0, `FBA铁路定价_修正_old`, ifNull(b.`FBA铁路定价_参考`, a.`FBA铁路定价_修正`)) as `FBA铁路定价_参考`,
                if(`适合卡航连续天数`=0, `FBA卡航定价_修正_old`, ifNull(b.`FBA卡航定价_参考`, a.`FBA卡航定价_修正`)) as `FBA卡航定价_参考`,
                if(`适合快运连续天数`=0, `FBA快运定价_修正_old`, ifNull(b.`FBA快运定价_参考`, a.`FBA快运定价_修正`)) as `FBA快运定价_参考`,
                if(`适合慢运连续天数`=0, `FBA慢运定价_修正_old`, ifNull(b.`FBA慢运定价_参考`, a.`FBA慢运定价_修正`)) as `FBA慢运定价_参考`,
                if(`FBA空运定价_参考`=0 or `FBA空运定价_修正` is null or `FBA空运定价_参考` is null, 0, abs(a.`FBA空运定价_修正`-`FBA空运定价_参考`)/`FBA空运定价_参考`) as `FBA空运定价波幅`,
                if(`FBA海运定价_参考`=0 or `FBA海运定价_修正` is null or `FBA海运定价_参考` is null, 0, abs(a.`FBA海运定价_修正`-`FBA海运定价_参考`)/`FBA海运定价_参考`) as `FBA海运定价波幅`,
                if(`FBA铁路定价_参考`=0 or `FBA铁路定价_修正` is null or `FBA铁路定价_参考` is null, 0, abs(a.`FBA铁路定价_修正`-`FBA铁路定价_参考`)/`FBA铁路定价_参考`) as `FBA铁路定价波幅`,
                if(`FBA卡航定价_参考`=0 or `FBA卡航定价_修正` is null or `FBA卡航定价_参考` is null, 0, abs(a.`FBA卡航定价_修正`-`FBA卡航定价_参考`)/`FBA卡航定价_参考`) as `FBA卡航定价波幅`,
                if(`FBA快运定价_参考`=0 or `FBA快运定价_修正` is null or `FBA快运定价_参考` is null, 0, abs(a.`FBA快运定价_修正`-`FBA快运定价_参考`)/`FBA快运定价_参考`) as `FBA快运定价波幅`,
                if(`FBA慢运定价_参考`=0 or `FBA慢运定价_修正` is null or `FBA慢运定价_参考` is null, 0, abs(a.`FBA慢运定价_修正`-`FBA慢运定价_参考`)/`FBA慢运定价_参考`) as `FBA慢运定价波幅`,
                multiIf(
                    `适合快运连续天数` >= 0 and `FBA快运定价波幅`>= 0.1, '快运定价比修正后数值连续一周低于2且FBA快运定价为主要原因，建议可发快运',
                    `适合慢运连续天数` >= 0 and `FBA慢运定价波幅`>= 0.1, '慢运定价比修正后数值连续一周低于2且FBA慢运定价为主要原因，建议可发慢运',
                '无建议') as `发运建议`,
                multiIf(
                    (`FBA慢运定价/FBM定价_修正`>2 or (`适合慢运连续天数`<0 and `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`> 0.1)) and a.`站点` in ('阿联酋','日本','沙特'), '0', 
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>=0 and a.`站点` in ('阿联酋','日本','沙特'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '6',
                    `FBA慢运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`> 2 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('阿联酋','日本','沙特'), '21',
                    `FBA快运定价/FBM定价_修正`>2 and a.`站点` in ('新加坡'), '0',
                    `FBA快运定价/FBM定价_修正`<=2 and a.`站点` in ('新加坡'), '6',
                    ((`FBA慢运定价/FBM定价_修正`> 1.8) or (`适合慢运连续天数`< 0 and `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1)) and a.`站点` not in ('日本','新加坡','阿联酋','沙特'), '0',         
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('美国'), '11;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '21;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('德国'), '5;6', 
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 0 and a.`站点` in ('英国'), '5;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('美国') and `FBA快运定价/FBM定价_修正` > 1.8, '11',    
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚') and `FBA快运定价/FBM定价_修正` > 1.8, '21',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国','英国') and `FBA快运定价/FBM定价_修正` <= 1.8, '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('英国') and `FBA快运定价/FBM定价_修正` > 1.8, '5',
                    `FBA慢运定价/FBM定价_修正`<= 1.8 and `快慢同价利率差_修正`<= 0.1 and a.`站点` in ('德国') and `FBA快运定价/FBM定价_修正` > 1.8, '5',
                    '-1') as `建议物流方式`
            from (
                select sku,`站点`,`税号`,`FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
                   `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
                   `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
                   `FBA空运定价_修正`, `FBA海运定价_修正`, `FBA铁路定价_修正`, `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`,
                   `空海同价利率差_修正`, `空铁同价利率差_修正`,`空卡同价利率差_修正`,`快慢同价利率差_修正`,
                   if(`站点` in ('阿联酋','新加坡','沙特','日本'), 2, 1.8) as `limit_day`,
                   if(`FBA空运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合空运连续天数`,
                   multiIf(`FBA海运定价/FBM定价_修正` is null, 0, `FBA海运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合海运连续天数`,
                   multiIf(`FBA铁路定价/FBM定价_修正` is null, 0, `FBA铁路定价/FBM定价_修正`<=limit_day, 1, 0) as `适合铁路连续天数`,
                   multiIf(`FBA卡航定价/FBM定价_修正` is null, 0, `FBA卡航定价/FBM定价_修正`<=limit_day, 1, 0) as `适合卡航连续天数`,
                   multiIf(`FBA快运定价/FBM定价_修正` is null, 0, `FBA快运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合快运连续天数`,
                   multiIf(`FBA慢运定价/FBM定价_修正` is null, 0, `FBA慢运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合慢运连续天数`
                from (
                    select a.sku as sku,a.`站点` as `站点`, a.`税号` `税号`,
                    a.`FBA空运定价/FBM定价` as `FBA空运定价/FBM定价`,a.`FBA卡航定价/FBM定价` as `FBA卡航定价/FBM定价`,
                    a.`FBA海运定价/FBM定价` as `FBA海运定价/FBM定价`,a.`FBA铁路定价/FBM定价` as `FBA铁路定价/FBM定价`,
                    a.`FBA快运定价/FBM定价` as `FBA快运定价/FBM定价`,a.`FBA慢运定价/FBM定价` as `FBA慢运定价/FBM定价`,
                    (a.`成本`+a.`头程费空运`+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA空运定价_修正`,
                    (a.`成本`+a.`运费`)/(1-c.`FBM平台抽成比例`-0.04-a.`FBM目标毛利润率`-a.`FBM税率`)/a.`汇率` as `FBM定价_修正`,
                    `FBA空运定价_修正`/`FBM定价_修正` as `FBA空运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA海运定价_修正`,
                    `FBA海运定价_修正`/`FBM定价_修正` as `FBA海运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA铁路定价_修正`,
                    `FBA铁路定价_修正`/`FBM定价_修正` as `FBA铁路定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA卡航定价_修正`,
                    `FBA卡航定价_修正`/`FBM定价_修正` as `FBA卡航定价/FBM定价_修正`,            
                    (a.`成本`+if(a.`头程费快运`<0, null, a.`头程费快运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA快运定价_修正`,
                    `FBA快运定价_修正`/`FBM定价_修正` as `FBA快运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.04-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA慢运定价_修正`,
                    `FBA慢运定价_修正`/`FBM定价_修正` as `FBA慢运定价/FBM定价_修正`,           
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空海利润率反算_修正`,
                    `空海利润率反算_修正`-a.`FBA目标毛利润率` as `空海同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空铁利润率反算_修正`,
                    `空铁利润率反算_修正`-a.`FBA目标毛利润率` as `空铁同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空卡利润率反算_修正`,
                    `空卡利润率反算_修正`-a.`FBA目标毛利润率` as `空卡同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.04-a.`FBA税率`-(a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA快运定价_修正`*a.`汇率`) as `快慢利润率反算_修正`,
                    `快慢利润率反算_修正`-a.`FBA目标毛利润率` as `快慢同价利率差_修正`
                    from (
                        select * except({col_list_str6},{col_list_str7}),
                            {col_list_str8},
                            `站点` as `FBA站点`,replace(`站点`, '-泛美', '') as `FBM站点`
                        from (
                            select a.* except ({col_list_str3}),
                                {col_list_str4},
                                {col_list_str5}     
                            from (select * except (id, date, update_time) from support_document.{table_now} where `站点` = '{site}' and `税号` = '{key}') a 
                            left join (
                                select sku, `站点`,`税号`,{col_list_str2}
                                from (
                                    {sql_str}
                                )
                                group by sku, `站点`,`税号`
                            ) b 
                            on a.sku=b.sku and a.`站点`=b.`站点` and a.`税号` = b.`税号`
                        )
                    ) a 
                    left join (
                        select site `FBA站点`, platform_percentage `FBA平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBA'
                    ) b 
                    on a.`FBA站点`=b.`FBA站点`
                    left join (
                        select site `FBM站点`, platform_percentage `FBM平台抽成比例` 
                        from domestic_warehouse_clear.price_calculate_parameters
                        where mode = 'AMAZON-FBM'
                    ) c 
                    on a.`FBM站点`=c.`FBM站点` 
                ) 
            ) a 
            left join monitor_table b 
            on a.sku=b.sku and a.`站点`=b.`站点` and a.`税号` = b.`税号`
        )
        """
        conn_mx2.ck_execute_sql(sql)

    # mysql备份
    print('mysql备份')
    # mysql备份表清空
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    TRUNCATE TABLE over_sea.tt_pricing_ratio_to_center
    """
    conn.execute(sql)
    conn.close()
    # 取数
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = time.strftime('%Y%m%d')
    # 建表
    table_name = f'tt_pricing_ratio_monitor_{date_today}'
    sql = f"""
        SELECT 
            sku, `站点`, `税号`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价_修正`,
            `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价_修正`,`FBA空运定价_修正`,`FBA海运定价_修正`,`FBA铁路定价_修正`,
            `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`, `空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`,
            `快慢同价利率差_修正`,`建议物流方式`, update_time
        FROM support_document.{table_name}
    """
    df = conn_mx2.ck_select_to_df(sql)
    # 备份
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    for i in df['站点'].unique():
        df_temp = df[df['站点']==i]
        conn.to_sql(df_temp, 'tt_pricing_ratio_to_center', if_exists='append')

    conn.close()
    print('通拓定价比计算程序完成！')

    # 监控程序
    sql = f"""
        SELECT date, `站点`, `建议物流方式`, count(1) as `数量`, 'tt_sku' as `维度`
        FROM support_document.tt_pricing_ratio_monitor_{date_today}
        GROUP BY date, `站点`, `建议物流方式`
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_m = conn_ck.ck_select_to_df(sql)
    df_m['建议物流方式2'] = df_m['建议物流方式'].map(map_values)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date = time.strftime('%Y-%m-%d')
    table_name = 'ads_pricing_ratio'
    sql = f"""
    DELETE FROM over_sea.{table_name} WHERE date = '{date}' and `维度` = 'tt_sku'
    """
    conn.execute(sql)
    conn.close()
    write_to_sql(df_m, 'ads_pricing_ratio')

##
def temp():
    """
     通拓定价比计算程序
     """
    df_sku = get_sku_new()
    df = df_sku[df_sku['sku']=='BZ00042']
    df = merge_first_product_line(df)
    df['数量'] = 1
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量', '重量来源'], axis=1, inplace=True)
    print(df)



## fbal定价比数据上传中台
def get_fbal_pr():
    """ allegro定价比数据上传中台 """
    date = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM yibai_fba.yibai_fbal_pricing_ratio_monitor x
        WHERE insert_date = '{date}'
        ORDER BY sku
    """
    conn_ck = pd_to_ck(database='yibai_fba', data_sys='调价明细历史数据')
    df_test = conn_ck.ck_select_to_df(sql)
    df_test = df_test.rename(columns={'insert_date':'date_id'})
    print(f'初始数据总量共{len(df_test)}条.')
    # 2025-07-28 筛选近90天有销量的sku
    df_sales_sku = get_allegro_sales_sku()
    df_test = df_test[df_test['sku'].isin(df_sales_sku['sku'].unique())]
    print(df_test.info())
    # df_test.to_excel('F://Desktop//df_test.xlsx', index=0)

    col = ['sku','site','cost','fbm_margin_diff','fbm_target_profit','fbal_target_profit','fbal_margin_diff',
           'domestic_shipping','exchange_rate_pl','exchange_rate','basic_fee','last_mile_fee','domestic_target_price',
           'express_first_leg','slow_first_leg','platform_price_express','platform_price_slow','express_ratio',
           'slow_ratio','same_price_margin_diff','suggested_logistics','date_id']
    df_test.columns = col
    print(df_test.info())
    #
    table_name = 'pricing_ratio_to_center_fbal'
    date = time.strftime('%y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    DELETE FROM over_sea.{table_name} WHERE date_id <= '{date}'
    """
    conn.execute(sql)

    write_to_sql(df_test, 'pricing_ratio_to_center_fbal')


def get_allegro_sales_sku():
    """ 获取allegro近100天有出单sku """
    sql = """

        SELECT 
            SKU as sku, warehouse_id, 3days_sales,
            7days_sales,15days_sales,30days_sales,
            60days_sales,90days_sales 
        FROM `yibai_sku_sales_statistics` 
        WHERE 
            platform_code in ('ALLEGRO')
            and 90days_sales > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_allegro_sales = conn.read_sql(sql)
    print(df_allegro_sales.info())
    print(df_allegro_sales['warehouse_id'].value_counts())

    return df_allegro_sales
##
if __name__ == "__main__":
    yunfei_check()
    # pricing_ratio()
    # pricing_ratio_new()
    # pricing_ratio_monitor_new()
    # pricing_ratio_mx()
    # pricing_ratio_mx_monitor()
    # tt_pricing_ratio()
    # df = tt_get_diff()
    # temp()
    get_fbal_pr()
    # get_allegro_sales_sku()
    # add_kunbang_tt()
    # upload_pricing_ratio()
    # pricing_ratio_listing()
    # upload_listing_pricing_ratio()
