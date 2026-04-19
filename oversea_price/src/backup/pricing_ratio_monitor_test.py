##
import time
import warnings
from tqdm import tqdm
from pulic_func.base_api.mysql_connect import pd_to_ck, connect_to_sql
import datetime
import pandas as pd
import numpy as np
from pulic_func.base_api.all_freight_interface import freight_interface_fu
from pulic_func.base_api.adjust_price_function_amazon import shuilv,fanou_fanmei
from pulic_func.price_adjust_web_service.AMAZON_FBA_dingjia import fba_ding_jia_biao, aut, tou_cheng_ky, tou_cheng_hy, \
    tou_cheng_tl, tou_cheng_kh
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang
from pulic_func.price_adjust_web_service.make_price import amazon_fba_para

from pulic_func.adjust_price_base_api.FBA_fee import tou_cheng_dingjiabi, tou_cheng_api
warnings.filterwarnings('ignore')
##
def merge_four_dim_diff(df, df_diff, col_names_list):
    """
    多维度逐级匹配差值，每个级别都使用之前所有级别的信息进行匹配，匹配不到将最高层级字段赋值 'other' 或 -1。
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

def get_diff():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 四维差值 之后使用需要进行多次匹配来修正各个字段信息从而能够最终用所有字段进行匹配
    sql_diff = """
        select * from yibai_sale_center_listing_sync.yibai_listing_profit_config
        where is_del=0 and status=1 and platform_code='AMAZON'
    """
    df_diff = conn_mx.ck_select_to_df(sql_diff)
    sql_site_table = """
        select site2 as site_en, site1 as site from domestic_warehouse_clear.site_table
    """
    df_site_table = conn_mx.ck_select_to_df(sql_site_table)
    df_diff = df_diff.rename(columns={'site': 'site_en'}).merge(df_site_table, on='site_en', how='left')
    df_diff['site'] = df_diff.apply(lambda x: 'other' if x['site_en'] == 'other' else x['site'], axis=1)
    df_diff.dropna(inplace=True)
    return df_diff

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
                and warehouse_id in (SELECT id FROM yb_datacenter.yb_warehouse WHERE type = 'inland')
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
    df_0 = df[df['sku'].isin(['2410210024311','1618230242311'])]
    df = df.sample(10000)
    df = pd.concat([df, df_0])
    print(f'获取sku完成，共{len(df)}条！共耗时{t2 - t1:.2f}s')

    return df


def get_diff_new():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 四维差值 之后使用需要进行多次匹配来修正各个字段信息从而能够最终用所有字段进行匹配
    sql_diff = """
        select * from yibai_sale_center_listing_sync.yibai_listing_profit_config
        where is_del=0 and status=1 and platform_code='AMAZON'
    """
    df_diff = conn_mx.ck_select_to_df(sql_diff)
    sql_site_table = """
        select site2 as site_en, site1 as site from domestic_warehouse_clear.site_table
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
                    b.path_name as `产品线路线` 
                    from yb_datacenter.yb_product a
                    left join yb_datacenter.yb_product_linelist b
                    on toInt32(a.product_linelist_id) = toInt32(b.id) 
                """
    df_line = conn_mx.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]

    sql_line_2 = f"""
                select 
                    distinct id first_product_line, linelist_cn_name as `一级产品线` 
                from yb_datacenter.yb_product_linelist
                where linelist_cn_name = path_name
            """
    df_line_2 = conn_mx.ck_select_to_df(sql_line_2)
    df_line = df_line.merge(df_line_2, on='一级产品线', how='left')
    df_line = df_line[['sku1', 'first_product_line']]

    # 拆分捆绑sku
    df['sku1'] = df['sku'].str.split('+', expand=True)[0].str.split('*', expand=True)[0]

    df = df.merge(df_line, on='sku1', how='left')
    print(f"匹配第一产品线完成！未匹配到产品线的sku共{len(df[df['first_product_line'].isna()])}条")
    df['first_product_line'] = df['first_product_line'].fillna(-1)
    df.drop('sku1', axis=1, inplace=True)
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
        left join domestic_warehouse_clear.site_table c 
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
        left join domestic_warehouse_clear.site_table c 
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
        left join domestic_warehouse_clear.site_table c 
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
    df_fba = get_cost_range(df_temp, df_diff_fba)
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
        select site1 as `站点`,site3 as country_code FROM domestic_warehouse_clear.site_table
        """
    df_code = conn_ck.ck_select_to_df(sql)
    df_fba = pd.merge(df_fba, df_code, how='left', on=['站点'])
    # 头程属性
    # for i in df_site['site1'].unique():
    df_fba['sku1'] = df_fba['sku']
    aut_ky = aut(df_fba, conn_ck, '空运')
    df_fba = pd.merge(df_fba, aut_ky, how='left', on=['sku1', 'country_code'])
    # df_fba.drop(['sku1'], axis=1, inplace=True)
    df_fba['fba头程属性'] = df_fba['fba头程属性'].fillna('普货')
    #
    df_fba['站点1'] = df_fba['站点']
    df_kongyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '空运')
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    #
    if i not in ['德国', '印度', '新加坡', '沙特']:
        aut_hy = aut(df_fba, conn_ck, '海运')
        df_fba = pd.merge(df_fba, aut_hy, how='left', on=['sku1', 'country_code'])
        df_haiyun = tou_cheng_api(df_fba, 'FBA', conn_ck, '海运')
        df_haiyun = df_haiyun[['sku', '站点', '头程费_海运']]
        df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    else:
        df_haiyun = pd.DataFrame(columns=['sku', '站点', '头程费_海运'])
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

    # 快运\慢运
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
    #
    aut_slow = aut(df_fba, conn_ck, '海运')
    df_fba = pd.merge(df_fba, aut_slow, how='left', on=['sku1', 'country_code'])
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


def get_fbm_fee(df_temp, df_diff, i):
    """
    获取国内仓运费数据，包括捆绑sku的运费
    """
    t1 = time.time()
    df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
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
##
# 20240515定价比计算程序优化
def pricing_ratio_new():
    """
    定价比计算程序
    """
    conn_mx2 = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # date_today = datetime.date.today().isoformat().replace('-', '')
    # # 表准备
    # table_name = f'pricing_ratio_test'
    # sql = f"""
    #     DROP TABLE IF EXISTS support_document.{table_name}
    # """
    # conn_mx2.ck_execute_sql(sql)
    # print('结果表删除成功！')
    # sql = f"""
    # CREATE TABLE IF NOT EXISTS yibai_oversea.{table_name}
    # (
    #     `id` Int64,
    #     `date` String,
    #     `sku` String,
    #     `站点` String COMMENT '站点',
    #     `FBA目标毛利润率` Float64 COMMENT 'FBA目标毛利润率',
    #     `FBA空运定价` Float64 COMMENT 'FBA空运定价',
    #     `FBA海运定价` Float64 COMMENT 'FBA海运定价',
    #     `FBA铁路定价` Float64 COMMENT 'FBA铁路定价',
    #     `FBA卡航定价` Float64 COMMENT 'FBA卡航定价',
    #     `FBA快运定价` Float64 COMMENT 'FBA快运定价',
    #     `FBA慢运定价` Float64 COMMENT 'FBA慢运定价',
    #     `FBM目标毛利润率` Float64 COMMENT 'FBM目标毛利润率',
    #     `FBM定价` Float64 COMMENT 'FBM定价',
    #     `FBA空运定价/FBM定价` Float64 COMMENT 'FBA空运定价/FBM定价',
    #     `FBA海运定价/FBM定价` Float64 COMMENT 'FBA海运定价/FBM定价',
    #     `FBA铁路定价/FBM定价` Float64 COMMENT 'FBA铁路定价/FBM定价',
    #     `FBA卡航定价/FBM定价` Float64 COMMENT 'FBA卡航定价/FBM定价',
    #     `FBA快运定价/FBM定价` Float64 COMMENT 'FBA快运定价/FBM定价',
    #     `FBA慢运定价/FBM定价` Float64 COMMENT 'FBA慢运定价/FBM定价',
    #     `成本` Float64 COMMENT '成本',
    #     `头程费空运` Float64 COMMENT '头程费空运',
    #     `头程费海运` Float64 COMMENT '头程费海运',
    #     `头程费铁路` Float64 COMMENT '头程费铁路',
    #     `头程费卡航` Float64 COMMENT '头程费卡航',
    #     `头程费快运` Float64 COMMENT '头程费快运',
    #     `头程费慢运` Float64 COMMENT '头程费慢运',
    #     `调拨费` Float64 COMMENT '调拨费',
    #     `fba_fees` Float64 COMMENT '尾程',
    #     `运费` Float64 COMMENT 'fbm运费',
    #     `FBA差值` Float64 COMMENT 'FBA差值',
    #     `FBM差值` Float64 COMMENT 'FBM差值',
    #     `FBA税率` Float64 COMMENT 'FBA税率',
    #     `FBM税率` Float64 COMMENT 'FBM税率',
    #     `汇率` Float64 COMMENT '汇率',
    #     `空海利润率反算` Nullable(Float64),
    #     `空海同价利率差` Nullable(Float64),
    #     `空铁利润率反算` Nullable(Float64),
    #     `空铁同价利率差` Nullable(Float64),
    #     `空卡利润率反算` Nullable(Float64),
    #     `空卡同价利率差` Nullable(Float64),
    #     `快慢利润率反算` Nullable(Float64),
    #     `快慢同价利率差` Nullable(Float64),
    #     `update_time` String DEFAULT toString(now()) COMMENT '更新时间'
    # )
    # ENGINE = MergeTree
    # ORDER BY (sku, `站点`)
    # SETTINGS index_granularity = 8192
    # """
    # conn_mx2.ck_create_table(sql)
    # print('结果表建立成功！')
    # sql = f"""
    #     ALTER TABLE yibai_oversea.{table_name}
    #     DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
    # """
    # conn_mx2.ck_execute_sql(sql)
    # print('结果表今日数据删除成功！')

    sql = """
        SELECT 
            site, site1
        from domestic_warehouse_clear.site_table bb
        where bb.site not in ('be', 'fr', 'it', 'sp', 'nl', 'pl', 'se', 'tr','br','in')
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_site = conn_ck.ck_select_to_df(sql)
    # 获取sku
    df_sku = get_sku_new()
    df_sku['is_bundle'] = 0
    df_kunbang = add_kunbang_new()
    df_kunbang['is_bundle'] = 1
    # 获取sku基础信息
    df = pd.concat([df_sku, df_kunbang])
    df = merge_first_product_line(df)

    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量','重量来源'], axis=1, inplace=True)
    # 获取差值表
    df_diff = get_diff_new()
    # 默认参数
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select * from domestic_warehouse_clear.price_calculate_parameters
    """
    df_site_df = conn_ck.ck_select_to_df(sql)
    df_site_df = amazon_fba_para(df_site_df, [], pd.DataFrame())

    # 主循环。获取FBA运费、FBM运费、计算定价比
    for i in df_site['site1'].unique():
        if i == '美国':
            df.loc[df['is_bundle']==0,'站点'] = i
            df_temp = df[df['站点'] == i]

            # fba头程运费
            df_price = get_fba_first_fee(df_temp, df_diff, i)

            # fba尾程运费
            df_temp['fbafee计算方式'] = '普通'
            df_temp['最小购买数量'] = 1
            df_fba_fee = fba_ding_jia_biao(df_temp)
            print(f"尾程费为空的数量有{len(df_fba_fee[(df_fba_fee['fba_fees'].isna()) | (df_fba_fee['fba_fees']=='')])}")
            df_fba_fee['fba_fees'] = pd.to_numeric(df_fba_fee['fba_fees'], errors='coerce')
            df_fba_fee['fba_fees'] = df_fba_fee['fba_fees'].fillna(0).astype(float)
            # FBM运费
            df_fbm = get_fbm_fee(df_temp, df_diff, i)
            # 定价计算
            df_price = pd.merge(df_price, df_fba_fee[['sku','站点','fba_fees']], how='left', on=['sku','站点'])
            df_price = pd.merge(df_price, df_fbm[['sku','站点','运费','FBM目标毛利润率','FBM差值']], how='left', on=['sku','站点'])
            df_price = pd.merge(df_price, df_site_df[['站点','汇率','平台抽成比例','冗余系数']], how='left', on=['站点'])
            #
            tax_rates_fba = {
                '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
                '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187
            }
            tax_rates_fbm = {
                '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
                '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187, '印度':0.3, '土耳其':0.15, '澳大利亚':0.09
            }
            # 使用map方法将税率应用到对应的站点
            df_price['FBA税率'] = df_price['站点'].map(tax_rates_fba)
            df_price['FBM税率'] = df_price['站点'].map(tax_rates_fbm)
            df_price[['FBA税率','FBM税率']] = df_price[['FBA税率','FBM税率']].fillna(0).astype(float)

            #
            df_price['调拨费'] = 0
            for fbafee_type in ['空运','海运','铁路','卡航','快运','慢运']:
                df_price[f"FBA{fbafee_type}定价"] = (df_price["成本"] + df_price[f"头程费_{fbafee_type}"] + df_price["调拨费"] + 2 + df_price["fba_fees"] * df_price[
                    "汇率"]) / (1 - df_price["平台抽成比例"] - df_price["冗余系数"] - df_price["FBA目标毛利润率"] - df_price["FBA税率"]) / df_price["汇率"]
                df_price[f"FBA{fbafee_type}定价"] = df_price[f"FBA{fbafee_type}定价"].astype('float')
            df_price["FBM定价"] = (df_price["成本"] + df_price["运费"]) / (
                    1 - df_price["平台抽成比例"] - df_price["冗余系数"] - df_price["FBM目标毛利润率"] - df_price["FBM税率"]) / df_price["汇率"]

            dic = {'头程费_海运':'头程费海运', '头程费_空运':'头程费空运','头程费_铁路':'头程费铁路','头程费_卡航':'头程费卡航',
                   '头程费_快运':'头程费快运', '头程费_慢运':'头程费慢运'}
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
            df_res = df_price[['date', 'sku', '站点',
                               'FBA目标毛利润率', 'FBA空运定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价', 'FBA快运定价',
                               'FBA慢运定价',
                               'FBM目标毛利润率', 'FBM定价', 'FBA空运定价/FBM定价', 'FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价',
                               'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价', 'FBA慢运定价/FBM定价', '成本',
                               '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运', '调拨费',
                               'fba_fees', '运费', 'FBA差值', 'FBM差值', 'FBA税率', 'FBM税率', '汇率', '空海利润率反算',
                               '空海同价利率差', '空铁利润率反算', '空铁同价利率差', '空卡利润率反算', '空卡同价利率差',
                               '快慢利润率反算', '快慢同价利率差']]
            col = ['成本', '运费', '调拨费', 'fba_fees', 'FBA空运定价/FBM定价', '空海利润率反算', '空海同价利率差',
                   '空铁利润率反算', '空铁同价利率差',
                   '空卡利润率反算', '空卡同价利率差', '快慢利润率反算', '快慢同价利率差']
            df_res[col] = df_res[col].astype(float).round(4)
            col = ['FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价',
                   'FBA慢运定价/FBM定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价', 'FBA快运定价', 'FBA慢运定价',
                   '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运']
            df_res[col] = df_res[col].astype('float').round(4).fillna(-999)
            # 异常数据存表
            # 表准备
            # table_name = f'pricing_ratio_test_{date_today}'
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            df_price_null = df_res[(df_res['FBA空运定价'].isna()) | (df_res['运费'].isna()) | (df_res['fba_fees']==0)]
            conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            sql = f"""
                ALTER TABLE yibai_oversea.pricing_ratio_null_data
                DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
            """
            conn_ck.ck_execute_sql(sql)
            conn_ck.ck_insert(df_price_null, 'pricing_ratio_null_data', if_exist='append')
            # 最终结果存表
            df_res = df_res[(~df_res['FBA空运定价'].isna()) & (~df_res['运费'].isna()) & (df_res['fba_fees']!=0)]
            # conn_ck.ck_insert(df_res, table_name, if_exist='append')
            # conn_ck.write_to_ck_json_type(df_res, table_name)
            print('done!')

def pricing_ratio():
    """
    监控两种方案指定前后日期的比值变化情况及主要变化归因。
    目前考虑采用四维差值策略的背景。
    四种特殊情况的尾程修正：
    1.本地变更为泛美，尾程配送费用从本地变为多渠道配送费；
    2.轻小状态时效，尾程配送费从轻小配送变更为本地，
    3.本地变更 EFN链接，尾程配送费 从本地变为EFN配送费
    4.配送费从本地变更为EFN配送费：泛欧失效、从英国配送至欧盟，或者从欧盟配送至UK这两种场景，
    以上四种 尾程配送费 都会由低变高，反之变小。
    这个导致的定价比波动  不构成影响我们是海运/空运链接
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')

    """
    方案：
    FBA空运目标净利定价/FBM目标净利定价 或 FBA海运目标净利定价/FBM目标净利定价
    FBA定价：(2+成本+头程+尾程fba_fees*汇率) / [(1-佣金率-折损率-订单毛利率-税率)*汇率]
    FBM定价：(成本+运费) / [(1-佣金率-折损率-订单毛利率-税率)*汇率]
    """

    # 表准备
    table_name = f'pricing_ratio_{date_today}'
    # table_name = f'pricing_ratio_test_{date_today}'
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

    # sku准备
    df = get_sku()
    df = add_kunbang(df)
    df = merge_first_product_line(df)
    df = df[~df['site'].isin(['巴西','印度'])]
    df_diff = get_diff()

    # 默认参数
    conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select * from domestic_warehouse_clear.price_calculate_parameters
    """
    df_site_df = conn.ck_select_to_df(sql)
    df_site_df = amazon_fba_para(df_site_df, [], pd.DataFrame())
    df['account_id'] = None
    df['seller_sku'] = None
    df['asin'] = None
    df['数量'] = 1

    #
    df_all = pd.DataFrame()
    for key, group in df.groupby(['site']):
        group = group.reset_index(drop=True)
        group['index'] = group.index
        # 每次计算60万条
        group['index'] = group['index'].apply(lambda m: int(m/1200000))
        df_all = df_all.append(group)
    del df, group
    #
    for (site, key), df_temp in tqdm(df_all.groupby(['site', 'index']), desc='process: '):
        df_temp.drop(['index'], axis=1, inplace=True)
        try:
            # FBA计算
            df_fba = df_temp.copy()
            df_fba['站点'] = df_fba['站点FBA']
            # 差值fba
            df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
                ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
            df_fba = get_cost_range(df_fba, df_diff_fba)
            df_fba = merge_four_dim_diff(df_fba, df_diff_fba, ['site', 'first_product_line', 'cost_range'])
            df_fba['net_profit2'] = (df_fba['net_profit2'] / 100).astype('float')
            df_fba['net_interest_rate_target'] = (df_fba['net_interest_rate_target'] / 100).astype('float')
            df_fba['FBA目标毛利润率'] = df_fba['net_profit2'] + df_fba['net_interest_rate_target']
            df_fba['FBA差值'] = df_fba['net_profit2']

            df_fba_kongyun = FBA_jisuan(df_fba, df_site_df, fbafee_type='空运')
            # 如果有站点没有海运/铁路/卡航，需手动创建df
            if len(df_fba[~df_fba['站点'].isin(['德国','印度','新加坡','沙特'])]) != 0:
                df_fba_haiyun = FBA_jisuan(df_fba[~df_fba['站点'].isin(['德国','印度','新加坡','沙特'])], df_site_df, fbafee_type='海运')
                df_fba_haiyun = df_fba_haiyun[['sku', '站点', 'date', 'FBA海运定价', '头程费海运']]
            else:
                df_fba_haiyun = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA海运定价', '头程费海运'])

            if len(df_fba[df_fba['站点'] == '德国']) != 0:
                df_fba_tielu = FBA_jisuan(df_fba[df_fba['站点'] == '德国'], df_site_df, fbafee_type='铁路')
                df_fba_tielu = df_fba_tielu[['sku', '站点', 'date', 'FBA铁路定价', '头程费铁路']]
            else:
                df_fba_tielu = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA铁路定价', '头程费铁路'])

            if len(df_fba[df_fba['站点'].isin(['德国', '英国'])]) != 0:
                df_fba_luyun = FBA_jisuan(df_fba[df_fba['站点'].isin(['德国', '英国'])], df_site_df, fbafee_type='卡航')
                df_fba_luyun = df_fba_luyun[['sku', '站点', 'date', 'FBA卡航定价', '头程费卡航']]
            else:
                df_fba_luyun = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA卡航定价', '头程费卡航'])
            # 20240125 新增快运、慢运维度
            if len(df_fba[~df_fba['站点'].isin(['印度', '巴西'])]) != 0:
                df_fba_kuaiyun = FBA_jisuan(df_fba[~df_fba['站点'].isin(['印度', '巴西'])], df_site_df,
                                                 fbafee_type='快运')
                df_fba_kuaiyun = df_fba_kuaiyun[['sku', '站点', 'date', 'FBA快运定价', '头程费快运']]
            else:
                df_fba_kuaiyun = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA快运定价', '头程费快运'])

            if len(df_fba[~df_fba['站点'].isin(['印度', '巴西'])]) != 0:
                df_fba_manyun = FBA_jisuan(df_fba[~df_fba['站点'].isin(['印度', '巴西'])], df_site_df,
                                                fbafee_type='慢运')
                df_fba_manyun = df_fba_manyun[['sku', '站点', 'date', 'FBA慢运定价', '头程费慢运']]
            else:
                df_fba_manyun = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA慢运定价', '头程费慢运'])
            # 定价比准备
            # df_fba_kongyun = df_fba_kongyun.rename(columns={'站点': '站点FBA'}).merge(
            #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
            # df_fba_haiyun = df_fba_haiyun.rename(columns={'站点': '站点FBA'}).merge(
            #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
            # df_fba_tielu = df_fba_tielu.rename(columns={'站点': '站点FBA'}).merge(
            #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
            # df_fba_luyun = df_fba_luyun.rename(columns={'站点': '站点FBA'}).merge(
            #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')

            df_price = df_fba_kongyun.merge(df_fba_haiyun, on=['sku', '站点', 'date'], how='left')
            df_price = df_price.merge(df_fba_tielu, on=['sku', '站点', 'date'], how='left')
            df_price = df_price.merge(df_fba_luyun, on=['sku', '站点', 'date'], how='left')
            df_price = df_price.merge(df_fba_kuaiyun, on=['sku', '站点', 'date'], how='left')
            df_price = df_price.merge(df_fba_manyun, on=['sku', '站点', 'date'], how='left')
            del df_fba_haiyun
            del df_fba_kongyun
            del df_fba_tielu
            del df_fba_luyun
            del df_fba
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
            # FBM计算
            df_fbm = df_temp.copy()
            df_fbm['站点'] = df_fbm['站点FBM']
            df_fbm['发货仓库'] = None
            df_fbm['物流类型'] = None
            # 差值fbm
            df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
                ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
            df_fbm = get_cost_range(df_fbm, df_diff_fbm)
            df_fbm = merge_four_dim_diff(df_fbm, df_diff_fbm, ['site', 'first_product_line', 'cost_range'])
            df_fbm['net_profit2'] = (df_fbm['net_profit2'] / 100).astype('float')
            df_fbm['net_interest_rate_target'] = (df_fbm['net_interest_rate_target'] / 100).astype('float')
            df_fbm['FBM目标毛利润率'] = df_fbm['net_profit2'] + df_fbm['net_interest_rate_target']
            df_fbm['FBM差值'] = df_fbm['net_profit2']

            df_fbm_res = FBM_jisuan(df_fbm, df_site_df)

            # 定价比准备
            df_fbm_res = df_fbm_res.rename(columns={'site': '站点'})
            df_price = df_price.merge(df_fbm_res, on=['sku', '站点', 'date'], how='inner')

            df_price['FBA空运定价/FBM定价'] = df_price['FBA空运定价'] / df_price['FBM定价']
            df_price['FBA海运定价/FBM定价'] = df_price['FBA海运定价'] / df_price['FBM定价']
            df_price['FBA铁路定价/FBM定价'] = df_price['FBA铁路定价'] / df_price['FBM定价']
            df_price['FBA卡航定价/FBM定价'] = df_price['FBA卡航定价'] / df_price['FBM定价']
            df_price['FBA快运定价/FBM定价'] = df_price['FBA快运定价'] / df_price['FBM定价']
            df_price['FBA慢运定价/FBM定价'] = df_price['FBA慢运定价'] / df_price['FBM定价']
            # 定价比
            df_price = df_price[df_price['fba_fees'] != '不符合FBA物流条件']
            df_res = df_price[['date', 'sku', '站点',
                               'FBA目标毛利润率', 'FBA空运定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价','FBA快运定价','FBA慢运定价',
                               'FBM目标毛利润率', 'FBM定价','FBA空运定价/FBM定价', 'FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价',
                               'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价', 'FBA慢运定价/FBM定价', '成本',
                               '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运','调拨费',
                               'fba_fees', '运费', 'FBA差值', 'FBM差值', 'FBA税率', 'FBM税率', '汇率', '空海利润率反算',
                               '空海同价利率差', '空铁利润率反算', '空铁同价利率差', '空卡利润率反算', '空卡同价利率差',
                               '快慢利润率反算', '快慢同价利率差']]
            col = ['成本', '运费', '调拨费', 'fba_fees', 'FBA空运定价/FBM定价', '空海利润率反算', '空海同价利率差',
                   '空铁利润率反算', '空铁同价利率差',
                   '空卡利润率反算', '空卡同价利率差', '快慢利润率反算', '快慢同价利率差']
            df_res[col] = df_res[col].astype(float).round(4)
            col = ['FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价',
                   'FBA慢运定价/FBM定价', 'FBA海运定价','FBA铁路定价', 'FBA卡航定价', 'FBA快运定价','FBA慢运定价',
                   '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运']
            df_res[col] = df_res[col].astype('float').round(4).fillna(-999)
            # 写入ck
            conn_mx2.write_to_ck_json_type(df_res, table_name)
            #
            # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
            #                      db_name='support_document')
            # ck_client.write_to_ck_json_type(table_name, df_res)
            del df_res
        except:
            print(traceback.format_exc())

    # 每次计算10万条
    # step = 100000
    # list_group = [df[i:i + step] for i in range(0, len(df), step)]
    # process_len = len(list_group)
    # process_index = 1
    # is_try = False
    # for list_member in list_group:
    #     df_temp = list_member
    #     if process_index >= 1:
    #         is_try = True
    #     else:
    #         print(f'process: {process_index}/{process_len}')
    #         process_index += 1
    #     sleep_time = 10
    #     while is_try:
    #         try:
    #             # FBA计算
    #             df_fba = df_temp.copy()
    #             df_fba['站点'] = df_fba['站点FBA']
    #             # 差值fba
    #             df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
    #                 ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    #             df_fba = get_cost_range(df_fba, df_diff_fba)
    #             df_fba = merge_four_dim_diff(df_fba, df_diff_fba, ['site', 'first_product_line', 'cost_range'])
    #             df_fba['net_profit2'] = (df_fba['net_profit2'] / 100).astype('float')
    #             df_fba['net_interest_rate_target'] = (df_fba['net_interest_rate_target'] / 100).astype('float')
    #             df_fba['FBA目标毛利润率'] = df_fba['net_profit2'] + df_fba['net_interest_rate_target']
    #             df_fba['FBA差值'] = df_fba['net_profit2']
    #
    #             df_fba_kongyun = FBA_jisuan(df_fba, df_site_df, fbafee_type='空运')
    #             df_fba_haiyun = FBA_jisuan(df_fba[df_fba['站点'] != '德国'], df_site_df, fbafee_type='海运')
    #             df_fba_haiyun = df_fba_haiyun[['sku', '站点', 'date', 'FBA海运定价', '头程费海运']]
    #             df_fba_tielu = FBA_jisuan(df_fba[df_fba['站点'] == '德国'], df_site_df, fbafee_type='铁路')
    #             df_fba_tielu = df_fba_tielu[['sku', '站点', 'date', 'FBA铁路定价', '头程费铁路']]
    #
    #             df_fba_luyun = FBA_jisuan(df_fba[df_fba['站点'].isin(['德国', '英国'])], df_site_df, fbafee_type='卡航')
    #             df_fba_luyun = df_fba_luyun[['sku', '站点', 'date', 'FBA卡航定价', '头程费卡航']]
    #
    #             # 定价比准备
    #             df_fba_kongyun = df_fba_kongyun.rename(columns={'站点': '站点FBA'}).merge(
    #                 df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
    #             df_fba_haiyun = df_fba_haiyun.rename(columns={'站点': '站点FBA'}).merge(
    #                 df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
    #             df_fba_tielu = df_fba_tielu.rename(columns={'站点': '站点FBA'}).merge(
    #                 df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
    #             df_fba_luyun = df_fba_luyun.rename(columns={'站点': '站点FBA'}).merge(
    #                 df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
    #
    #             df_price = df_fba_kongyun.merge(df_fba_haiyun, on=['sku', '站点FBA', 'date', 'site'], how='left')
    #             df_price = df_price.merge(df_fba_tielu, on=['sku', '站点FBA', 'date', 'site'], how='left')
    #             df_price = df_price.merge(df_fba_luyun, on=['sku', '站点FBA', 'date', 'site'], how='left')
    #             del df_fba_haiyun
    #             del df_fba_kongyun
    #             del df_fba_tielu
    #             del df_fba_luyun
    #             del df_fba
    #             df_price['空海利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
    #                                          (df_price['成本'] + df_price['头程费海运'] + df_price['调拨费'] + df_price[
    #                                              'fba_fees'] * df_price['汇率']) / (
    #                                                      df_price['FBA空运定价'] * df_price['汇率'])
    #             df_price['空海同价利率差'] = df_price['空海利润率反算'] - df_price['FBA目标毛利润率']
    #             df_price['空铁利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
    #                                          (df_price['成本'] + df_price['头程费铁路'] + df_price['调拨费'] + df_price[
    #                                              'fba_fees'] * df_price['汇率']) / (
    #                                                      df_price['FBA空运定价'] * df_price['汇率'])
    #             df_price['空铁同价利率差'] = df_price['空铁利润率反算'] - df_price['FBA目标毛利润率']
    #             # FBM计算
    #             df_fbm = df_temp.copy()
    #             df_fbm['站点'] = df_fbm['站点FBM']
    #             df_fbm['发货仓库'] = None
    #             df_fbm['物流类型'] = None
    #             # 差值fbm
    #             df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
    #                 ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    #             df_fbm = get_cost_range(df_fbm, df_diff_fbm)
    #             df_fbm = merge_four_dim_diff(df_fbm, df_diff_fbm, ['site', 'first_product_line', 'cost_range'])
    #             df_fbm['net_profit2'] = (df_fbm['net_profit2'] / 100).astype('float')
    #             df_fbm['net_interest_rate_target'] = (df_fbm['net_interest_rate_target'] / 100).astype('float')
    #             df_fbm['FBM目标毛利润率'] = df_fbm['net_profit2'] + df_fbm['net_interest_rate_target']
    #             df_fbm['FBM差值'] = df_fbm['net_profit2']
    #
    #             df_fbm_res = FBM_jisuan(df_fbm, df_site_df)
    #
    #             # 定价比准备
    #             df_fbm_res = df_fbm_res.rename(columns={'站点': '站点FBM'}).merge(
    #                 df_fbm[['sku', '站点FBM', 'site']].drop_duplicates(), on=['sku', '站点FBM'], how='left')
    #
    #             df_price = df_price.merge(df_fbm_res, on=['sku', 'site', 'date'], how='inner')
    #             df_price['FBA空运定价/FBM定价'] = df_price['FBA空运定价'] / df_price['FBM定价']
    #             df_price['FBA海运定价/FBM定价'] = df_price['FBA海运定价'] / df_price['FBM定价']
    #             df_price['FBA铁路定价/FBM定价'] = df_price['FBA铁路定价'] / df_price['FBM定价']
    #             df_price['FBA卡航定价/FBM定价'] = df_price['FBA卡航定价'] / df_price['FBM定价']
    #             # 定价比
    #             df_price = df_price[df_price['fba_fees'] != '不符合FBA物流条件']
    #             df_price['站点'] = df_price['站点FBA']
    #             df_res = df_price[['date', 'sku', '站点',
    #                              'FBA目标毛利润率', 'FBA空运定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价','FBM目标毛利润率','FBM定价',
    #                              'FBA空运定价/FBM定价', 'FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', '成本',
    #                              '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '调拨费',
    #                              'fba_fees', '运费', 'FBA差值', 'FBM差值', 'FBA税率', 'FBM税率', '汇率','空海利润率反算','空海同价利率差','空铁利润率反算','空铁同价利率差']]
    #             df_res['成本'] = df_res['成本'].astype('float')
    #             df_res['运费'] = df_res['运费'].astype('float')
    #             df_res['调拨费'] = df_res['调拨费'].astype('float')
    #             df_res['fba_fees'] = df_res['fba_fees'].astype('float')
    #             df_res['FBA空运定价/FBM定价'] = df_res['FBA空运定价/FBM定价'].astype('float')
    #             df_res['FBA海运定价/FBM定价'] = df_res['FBA海运定价/FBM定价'].astype('float').fillna(-999)
    #             df_res['FBA铁路定价/FBM定价'] = df_res['FBA铁路定价/FBM定价'].astype('float').fillna(-999)
    #             df_res['FBA卡航定价/FBM定价'] = df_res['FBA卡航定价/FBM定价'].astype('float').fillna(-999)
    #             df_res['FBA海运定价'] = df_res['FBA海运定价'].astype('float').fillna(-999)
    #             df_res['FBA铁路定价'] = df_res['FBA铁路定价'].astype('float').fillna(-999)
    #             df_res['FBA卡航定价'] = df_res['FBA卡航定价'].astype('float').fillna(-999)
    #             df_res['头程费海运'] = df_res['头程费海运'].astype('float').fillna(-999)
    #             df_res['头程费铁路'] = df_res['头程费铁路'].astype('float').fillna(-999)
    #             df_res['头程费卡航'] = df_res['头程费卡航'].astype('float').fillna(-999)
    #             df_res[['空海利润率反算', '空海同价利率差', '空铁利润率反算', '空铁同价利率差']] = df_res[
    #                 ['空海利润率反算', '空海同价利率差', '空铁利润率反算', '空铁同价利率差']].astype(float)
    #             # 写入ck
    #             conn_mx2.ck_insert(df_res, table_name, if_exist='append')
    #             del df_res
    #             print(f'process: {process_index}/{process_len}')
    #             process_index += 1
    #             is_try = False
    #         except Exception as err3:
    #             print('报错721', err3)
    #             print('sleep start')
    #             time.sleep(sleep_time)
    #             print('sleep end')
    print('每日计算定价比完成!')

def pricing_ratio_mx_backup():
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

    # sku准备
    # 需带上【美国】站，不然后续计算fba_fee时函数会报错
    # 计算完删除美国站
    df = get_sku()
    df = df[df['site'].isin(['美国','墨西哥'])]
    # df = add_kunbang(df)
    df = df[df['site'].isin(['美国','墨西哥'])]
    df = merge_first_product_line(df)
    df_diff = get_diff()

    # 默认参数
    conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select * from domestic_warehouse_clear.price_calculate_parameters
    """
    df_site_df = conn.ck_select_to_df(sql)
    df_site_df = amazon_fba_para(df_site_df, [], pd.DataFrame())
    df['account_id'] = None
    df['seller_sku'] = None
    df['asin'] = None
    df['数量'] = 1

    # 美国站数据只保留少部分就行
    df1 = df[df['site'] == '美国'].sample(10)
    df2 = df[df['site'] == '墨西哥']
    df = pd.concat([df1, df2])

    try:
        # FBA计算
        df_fba = df.copy()
        df_fba['站点'] = df_fba['站点FBA']
        # 差值fba
        df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
            ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
        df_fba = get_cost_range(df_fba, df_diff_fba)
        df_fba = merge_four_dim_diff(df_fba, df_diff_fba, ['site', 'first_product_line', 'cost_range'])
        df_fba['net_profit2'] = (df_fba['net_profit2'] / 100).astype('float')
        df_fba['net_interest_rate_target'] = (df_fba['net_interest_rate_target'] / 100).astype('float')
        df_fba['FBA目标毛利润率'] = df_fba['net_profit2'] + df_fba['net_interest_rate_target']
        df_fba['FBA差值'] = df_fba['net_profit2']
        # 墨西哥个人税率配置
        df_fba['税率'] = 0.09
        df_fba_kongyun = FBA_jisuan(df_fba, df_site_df, fbafee_type='空运')
        df_fba_haiyun = FBA_jisuan(df_fba[df_fba['站点'] != '德国'], df_site_df, fbafee_type='海运')
        df_fba_haiyun = df_fba_haiyun[['sku', '站点', 'date', 'FBA海运定价', '头程费海运']]
        # 墨西哥站点没有铁路和卡航
        df_fba_tielu = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA铁路定价', '头程费铁路'])
        df_fba_luyun = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA卡航定价', '头程费卡航'])

        # 20240125 新增快运、慢运维度
        if len(df_fba[~df_fba['站点'].isin(['印度', '巴西'])]) != 0:
            df_fba_kuaiyun = FBA_jisuan(df_fba[~df_fba['站点'].isin(['印度', '巴西'])], df_site_df,
                                        fbafee_type='快运')
            df_fba_kuaiyun = df_fba_kuaiyun[['sku', '站点', 'date', 'FBA快运定价', '头程费快运']]
        else:
            df_fba_kuaiyun = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA快运定价', '头程费快运'])

        if len(df_fba[~df_fba['站点'].isin(['印度', '巴西'])]) != 0:
            df_fba_manyun = FBA_jisuan(df_fba[~df_fba['站点'].isin(['印度', '巴西'])], df_site_df,
                                       fbafee_type='慢运')
            df_fba_manyun = df_fba_manyun[['sku', '站点', 'date', 'FBA慢运定价', '头程费慢运']]
        else:
            df_fba_manyun = pd.DataFrame(columns=['sku', '站点', 'date', 'FBA慢运定价', '头程费慢运'])
        # 定价比准备
        # df_fba_kongyun = df_fba_kongyun.rename(columns={'站点': '站点FBA'}).merge(
        #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
        # df_fba_haiyun = df_fba_haiyun.rename(columns={'站点': '站点FBA'}).merge(
        #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
        # df_fba_tielu = df_fba_tielu.rename(columns={'站点': '站点FBA'}).merge(
        #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')
        # df_fba_luyun = df_fba_luyun.rename(columns={'站点': '站点FBA'}).merge(
        #     df_fba[['sku', '站点FBA', 'site']], on=['sku', '站点FBA'], how='left')

        df_price = df_fba_kongyun.merge(df_fba_haiyun, on=['sku', '站点', 'date'], how='left')
        df_price = df_price.merge(df_fba_tielu, on=['sku', '站点', 'date'], how='left')
        df_price = df_price.merge(df_fba_luyun, on=['sku', '站点', 'date'], how='left')
        df_price = df_price.merge(df_fba_kuaiyun, on=['sku', '站点', 'date'], how='left')
        df_price = df_price.merge(df_fba_manyun, on=['sku', '站点', 'date'], how='left')

        del df_fba_haiyun
        del df_fba_kongyun
        del df_fba_tielu
        del df_fba_luyun
        del df_fba
        df_price['空海利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费海运'] + df_price['调拨费'] + 2 + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空海同价利率差'] = df_price['空海利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空铁利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费铁路'] + df_price['调拨费'] + 2 + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空铁同价利率差'] = df_price['空铁利润率反算'] - df_price['FBA目标毛利润率']
        df_price['空卡利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费卡航'] + df_price['调拨费'] + 2 + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA空运定价'] * df_price['汇率'])
        df_price['空卡同价利率差'] = df_price['空卡利润率反算'] - df_price['FBA目标毛利润率']

        df_price['快慢利润率反算'] = 1 - df_price['平台抽成比例'] - df_price['冗余系数'] - df_price['FBA税率'] - \
                                     (df_price['成本'] + df_price['头程费慢运'] + 2 + df_price['调拨费'] + df_price[
                                         'fba_fees'] * df_price['汇率']) / (
                                             df_price['FBA快运定价'] * df_price['汇率'])
        df_price['快慢同价利率差'] = df_price['快慢利润率反算'] - df_price['FBA目标毛利润率']
        # FBM计算
        df_fbm = df.copy()
        df_fbm['站点'] = df_fbm['站点FBM']
        df_fbm['发货仓库'] = None
        df_fbm['物流类型'] = None
        # 差值fbm
        df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
            ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
        df_fbm = get_cost_range(df_fbm, df_diff_fbm)
        df_fbm = merge_four_dim_diff(df_fbm, df_diff_fbm, ['site', 'first_product_line', 'cost_range'])
        df_fbm['net_profit2'] = (df_fbm['net_profit2'] / 100).astype('float')
        df_fbm['net_interest_rate_target'] = (df_fbm['net_interest_rate_target'] / 100).astype('float')
        df_fbm['FBM目标毛利润率'] = df_fbm['net_profit2'] + df_fbm['net_interest_rate_target']
        df_fbm['FBM差值'] = df_fbm['net_profit2']
        # 墨西哥税率配置
        df_fbm['税率'] = 0.09
        df_fbm_res = FBM_jisuan(df_fbm, df_site_df)

        # 定价比准备
        df_fbm_res = df_fbm_res.rename(columns={'site': '站点'})
        df_price = df_price.merge(df_fbm_res, on=['sku', '站点', 'date'], how='inner')

        df_price['FBA空运定价/FBM定价'] = df_price['FBA空运定价'] / df_price['FBM定价']
        df_price['FBA海运定价/FBM定价'] = df_price['FBA海运定价'] / df_price['FBM定价']
        df_price['FBA铁路定价/FBM定价'] = df_price['FBA铁路定价'] / df_price['FBM定价']
        df_price['FBA卡航定价/FBM定价'] = df_price['FBA卡航定价'] / df_price['FBM定价']
        df_price['FBA快运定价/FBM定价'] = df_price['FBA快运定价'] / df_price['FBM定价']
        df_price['FBA慢运定价/FBM定价'] = df_price['FBA慢运定价'] / df_price['FBM定价']

        # 定价比
        df_price = df_price[df_price['fba_fees'] != '不符合FBA物流条件']
        df_res = df_price[['date', 'sku', '站点',
                           'FBA目标毛利润率', 'FBA空运定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价', 'FBA快运定价',
                           'FBA慢运定价',
                           'FBM目标毛利润率', 'FBM定价', 'FBA空运定价/FBM定价', 'FBA海运定价/FBM定价',
                           'FBA铁路定价/FBM定价',
                           'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价', 'FBA慢运定价/FBM定价', '成本',
                           '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运', '调拨费',
                           'fba_fees', '运费', 'FBA差值', 'FBM差值', 'FBA税率', 'FBM税率', '汇率', '空海利润率反算',
                           '空海同价利率差', '空铁利润率反算', '空铁同价利率差', '空卡利润率反算', '空卡同价利率差',
                           '快慢利润率反算', '快慢同价利率差']]
        col = ['成本', '运费', '调拨费', 'fba_fees', 'FBA空运定价/FBM定价', '空海利润率反算', '空海同价利率差',
               '空铁利润率反算', '空铁同价利率差',
               '空卡利润率反算', '空卡同价利率差', '快慢利润率反算', '快慢同价利率差']
        df_res[col] = df_res[col].astype(float).round(4)
        col = ['FBA海运定价/FBM定价', 'FBA铁路定价/FBM定价', 'FBA卡航定价/FBM定价', 'FBA快运定价/FBM定价',
               'FBA慢运定价/FBM定价', 'FBA海运定价', 'FBA铁路定价', 'FBA卡航定价', 'FBA快运定价', 'FBA慢运定价',
               '头程费海运', '头程费铁路', '头程费卡航', '头程费快运', '头程费慢运']
        df_res[col] = df_res[col].astype('float').round(4).fillna(-999)
        # 筛除美国站数据
        df_res = df_res[df_res['站点'] == '墨西哥']
        print(df_res.info())
        # 写入ck
        conn_mx2.ck_insert(df_res, table_name, if_exist='append')
        del df_res

    except Exception as err3:
        print('报错721', err3)
        print('sleep start')
        time.sleep(10)
        print('sleep end')
    print('墨西哥个人税率每日计算定价比完成!')
##
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
            LEFT JOIN domestic_warehouse_clear.site_table c
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
            LEFT JOIN domestic_warehouse_clear.site_table c
            ON b.site = c.site
            WHERE a.fulfillment_channel = 'AMA' and a.account_id in {account_tuple}
        ) f
        INNER JOIN (
            SELECT 
                account_id, seller_sku, asin, sku, cost, length, width, high, weight, first_trip_source, fba_fees*rate as fba_fees_rmb,
                rate,first_trip_fee_rmb,toucheng_kongyun,toucheng_haiyun,toucheng_tl,toucheng_kh 
            FROM domestic_warehouse_clear.fba_fees
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
            FROM domestic_warehouse_clear.yibai_amazon_referral_fee
            WHERE account_id in {account_tuple} 
        ) r
        ON f.account_id = r.account_id and f.seller_sku = r.seller_sku
        LEFT JOIN (
            SELECT account_id, seller_sku, `佣金率` fbm_commission_rate
            FROM domestic_warehouse_clear.yibai_amazon_referral_fee
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
        'FBA税率'] - 0.03)
    df['fba_hy_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['toucheng_haiyun'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.03)
    df['fba_tl_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['toucheng_tl'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.03)
    df['fba_kh_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['toucheng_kh'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.03)
    df['fba_fast_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['头程费快运'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.03)
    df['fba_slow_price_rmb'] = (df['cost'] + df['fba_fees_rmb'] + df['头程费慢运'] + 2
                              ) / (1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA_profit'] - df[
        'FBA税率'] - 0.03)
    df['fbm_price_rmb'] = (df['cost'] + df['fbm运费']) / (
                1 - df['fbm_commission_rate'] - 0.03 - df['fbm_diff'] - df['FBM_profit'] - df['FBM税率'])
    df['air_price_ratio'] = df['fba_ky_price_rmb'] / df['fbm_price_rmb']
    df['fast_price_ratio'] = df['fba_fast_price_rmb'] / df['fbm_price_rmb']
    df['slow_price_ratio'] = df['fba_slow_price_rmb'] / df['fbm_price_rmb']
    df['fast_and_slow_rate_diff'] = 1 - df['fba_commission_rate'] - df['fba_diff'] - df['FBA税率'] - 0.03 - \
                                    (df['cost']+df['fba_fees_rmb']+df['头程费慢运']+2)/df['fba_fast_price_rmb'] - df['FBA_profit']

    col = ['fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb', 'fba_kh_price_rmb','fba_fast_price_rmb',
           'fba_slow_price_rmb','fbm_price_rmb']
    df[col] = df[col].astype(float).round(2)
    col = ['air_price_ratio', 'fast_price_ratio','slow_price_ratio', 'fast_and_slow_rate_diff']
    df[col] = df[col].astype(float).round(4)
    return df

##
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

        ratio_limit = 1.5
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

        # 建议物流方式
        ratio_limit = 1.5
        c1 = (df_result['site'].isin(['英国','德国','法国','西班牙','意大利']))  & \
             (df_result['slow_price_ratio']<=ratio_limit) & (df_result['fast_and_slow_rate_diff']<=0.01)
        c2 = (df_result['site'].isin(['美国','加拿大','墨西哥','澳大利亚'])) & \
             (df_result['slow_price_ratio']<=ratio_limit) & (df_result['fast_and_slow_rate_diff']<=0.01)
        c3 = (df_result['site'].isin(['阿联酋'])) & \
             (df_result['slow_price_ratio']<=2) & (df_result['fast_and_slow_rate_diff']<=0.01)
        c4 = (df_result['site'].isin(['日本','新加坡','沙特'])) & (df_result['fast_price_ratio']<=2)
        c5 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio']<=2)

        c6 = (df_result['site'].isin(['英国','德国','法国','西班牙','意大利'])) & (df_result['slow_price_ratio']<=ratio_limit) & \
             (df_result['fast_and_slow_rate_diff']>0.01) & (df_result['fast_and_slow_rate_diff']<=0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c7 = (df_result['site'].isin(['美国'])) & (df_result['slow_price_ratio']<=ratio_limit) & \
             (df_result['fast_and_slow_rate_diff']>0.01) & (df_result['fast_and_slow_rate_diff']<=0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c8 = (df_result['site'].isin(['加拿大','墨西哥','澳大利亚'])) & (df_result['slow_price_ratio']<=ratio_limit) & \
             (df_result['fast_and_slow_rate_diff']>0.01) & (df_result['fast_and_slow_rate_diff']<=0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c9 = (df_result['site'].isin(['阿联酋'])) & (df_result['slow_price_ratio']<=2) & \
             (df_result['fast_and_slow_rate_diff']>0.01) & (df_result['fast_and_slow_rate_diff']<=0.1) & \
             (df_result['适合慢运连续天数'] >= 7)

        c10 = (df_result['site'].isin(['英国','德国','法国','西班牙','意大利'])) & (df_result['slow_price_ratio']<=ratio_limit) & \
             (df_result['fast_and_slow_rate_diff']>0.1) & (df_result['适合慢运连续天数'] >= 7)
        c11 = (df_result['site'].isin(['美国'])) & (df_result['slow_price_ratio']<=ratio_limit) & \
             (df_result['fast_and_slow_rate_diff']>0.1) & (df_result['适合慢运连续天数'] >= 7)
        c12 = (df_result['site'].isin(['加拿大','墨西哥','澳大利亚'])) & (df_result['slow_price_ratio']<=ratio_limit) & \
             (df_result['fast_and_slow_rate_diff']>0.1) & (df_result['适合慢运连续天数'] >= 7)
        c13 = (df_result['site'].isin(['阿联酋'])) & (df_result['slow_price_ratio']<=2) & \
             (df_result['fast_and_slow_rate_diff']>0.1) & (df_result['适合慢运连续天数'] >= 7)

        col = ['英国','德国','法国','西班牙','意大利','美国','加拿大','墨西哥','澳大利亚']
        c14 = ((df_result['site'].isin(col)) & (df_result['slow_price_ratio']>ratio_limit)) | (df_result['适合慢运连续天数'] < 7)
        c15 = ((df_result['site'].isin(['阿联酋'])) & (df_result['slow_price_ratio']>2)) | (df_result['适合慢运连续天数'] < 7)
        c16 = (df_result['site'].isin(['日本','新加坡','沙特'])) & (df_result['fast_price_ratio']>2)
        c17 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio']>2)

        df_result['proposed_transport'] = np.select(
            [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17],
            ['15', '6', '6', '6', '13', '16;15', '11;6', '12;6', '12;6', '16', '11', '12', '12', '0', '0', '0', '0'])
        #
        df_result['date'] = time.strftime('%Y-%m-%d')
        col = ['account_id', 'asin', 'seller_sku', 'fbm_seller', 'sku', 'site', '头程费快运', '头程费慢运', 'fbm运费',
               'fba_fast_price_rmb',
               'fba_slow_price_rmb', 'fbm_price_rmb', 'fast_price_ratio', 'slow_price_ratio', 'fast_and_slow_rate_diff',
               '适合快运连续天数', '适合慢运连续天数', 'proposed_transport', 'date']

        # df_result_all = pd.concat([df_result, df_result_all])
        #
        # print(df_result[col].info())
        conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
        conn_mx2.write_to_ck_json_type(df_result[col], table_name)
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='support_document')
        # ck_client.write_to_ck_json_type(table_name, df_result[col])

##

if __name__ == "__main__":
    print('testing')
    # yunfei_check()
    t1 = time.time()
    pricing_ratio_new()
    t2 = time.time()
    print(f'测试完成，共耗时{t2 -t1:.2f}s')
    # pricing_ratio_monitor()
