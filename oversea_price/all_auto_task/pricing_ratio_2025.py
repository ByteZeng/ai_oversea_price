"""

FBA新品发运建议物流方式：
定价比计算程序：
1、sku维度定价比计算，并上传中台

"""
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
import pandas as pd
import numpy as np
from tqdm import tqdm
import traceback
from all_auto_task.dingding import send_msg
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

# -------------------------- 逻辑1的配置规则 --------------------------

fba_rule_vectorized_new = {
    # 1. 非指定站点规则（对应“逻辑1(老品)-非沙特、阿联酋、澳洲、墨西哥”）
    None: [
        {"speed_diff_range": (0, 1), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "快运")]},
        {"speed_diff_range": (1, 2), "slow_target_conditions": [(lambda x: x < 20, "混发"), (lambda x: x >= 20, "快运")]},
        {"speed_diff_range": (2, 3), "slow_target_conditions": [(lambda x: x < 10, "慢运"), (lambda x: (10 <= x) & (x < 25), "混发"), (lambda x: x >= 25, "快运")]},
        {"speed_diff_range": (3, 5), "slow_target_conditions": [(lambda x: x < 15, "慢运"), (lambda x: (15 <= x) & (x < 30), "混发"), (lambda x: x >= 30, "快运")]},
        {"speed_diff_range": (5, 7), "slow_target_conditions": [(lambda x: x < 20, "慢运"), (lambda x: (20 <= x) & (x < 40), "混发"), (lambda x: x >= 40, "快运")]},
        {"speed_diff_range": (7, 10), "slow_target_conditions": [(lambda x: x < 25, "慢运"), (lambda x: (25 <= x) & (x < 50), "混发"), (lambda x: x >= 50, "快运")]},
        {"speed_diff_range": (10, 15), "slow_target_conditions": [(lambda x: x < 30, "慢运"), (lambda x: (30 <= x) & (x < 75), "混发"), (lambda x: x >= 75, "快运")]},
        {"speed_diff_range": (15, 20), "slow_target_conditions": [(lambda x: x < 40, "慢运"), (lambda x: (40 <= x) & (x < 100), "混发"), (lambda x: x >= 100, "快运")]},
        {"speed_diff_range": (20, 25), "slow_target_conditions": [(lambda x: x < 50, "慢运"), (lambda x: x >= 50, "混发")]},
        {"speed_diff_range": (25, np.inf), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "慢运")]}
    ],
    # 2. 阿联酋站点规则（对应“逻辑1(老品)-阿联酋”）
    "阿联酋": [
        {"speed_diff_range": (0, 2), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "快运")]},
        {"speed_diff_range": (2, 3), "slow_target_conditions": [(lambda x: x < 25, "混发"), (lambda x: x >= 25, "快运")]},
        {"speed_diff_range": (3, 5), "slow_target_conditions": [(lambda x: x < 10, "慢运"), (lambda x: (10 <= x) & (x < 30), "混发"), (lambda x: x >= 30, "快运")]},
        {"speed_diff_range": (5, 7), "slow_target_conditions": [(lambda x: x < 15, "慢运"), (lambda x: (15 <= x) & (x < 40), "混发"), (lambda x: x >= 40, "快运")]},
        {"speed_diff_range": (7, 10), "slow_target_conditions": [(lambda x: x < 20, "慢运"), (lambda x: (20 <= x) & (x < 50), "混发"), (lambda x: x >= 50, "快运")]},
        {"speed_diff_range": (10, 15), "slow_target_conditions": [(lambda x: x < 25, "慢运"), (lambda x: (25 <= x) & (x < 75), "混发"), (lambda x: x >= 75, "快运")]},
        {"speed_diff_range": (15, 20), "slow_target_conditions": [(lambda x: x < 30, "慢运"), (lambda x: (30 <= x) & (x < 100), "混发"), (lambda x: x >= 100, "快运")]},
        {"speed_diff_range": (20, 25), "slow_target_conditions": [(lambda x: x < 40, "慢运"), (lambda x: x >= 40, "混发")]},
        {"speed_diff_range": (25, np.inf), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "慢运")]}
    ],
    # 3. 澳大利亚站点规则（对应“逻辑1(老品)-澳大利亚”）
    "澳大利亚": [
        {"speed_diff_range": (0, 2), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "快运")]},
        {"speed_diff_range": (2, 3), "slow_target_conditions": [(lambda x: x < 20, "混发"), (lambda x: x >= 20, "快运")]},
        {"speed_diff_range": (3, 5), "slow_target_conditions": [(lambda x: x < 10, "慢运"), (lambda x: (10 <= x) & (x < 25), "混发"), (lambda x: x >= 25, "快运")]},
        {"speed_diff_range": (5, 7), "slow_target_conditions": [(lambda x: x < 15, "慢运"), (lambda x: (15 <= x) & (x < 30), "混发"), (lambda x: x >= 30, "快运")]},
        {"speed_diff_range": (7, 10), "slow_target_conditions": [(lambda x: x < 20, "慢运"), (lambda x: (20 <= x) & (x < 40), "混发"), (lambda x: x >= 40, "快运")]},
        {"speed_diff_range": (10, 15), "slow_target_conditions": [(lambda x: x < 25, "慢运"), (lambda x: (25 <= x) & (x < 50), "混发"), (lambda x: x >= 50, "快运")]},
        {"speed_diff_range": (15, 20), "slow_target_conditions": [(lambda x: x < 30, "慢运"), (lambda x: (30 <= x) & (x < 75), "混发"), (lambda x: x >= 75, "快运")]},
        {"speed_diff_range": (20, 25), "slow_target_conditions": [(lambda x: x < 40, "慢运"), (lambda x: (40 <= x) & (x < 100), "混发"), (lambda x: x >= 100, "快运")]},
        {"speed_diff_range": (30, np.inf), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "慢运")]}
    ],
    # 4. 墨西哥站点规则（对应“逻辑1(老品)-墨西哥”）
    "墨西哥": [
        {"speed_diff_range": (0, 5), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "快运")]},
        {"speed_diff_range": (5, 7), "slow_target_conditions": [(lambda x: x < 15, "混发"), (lambda x: x >= 15, "快运")]},
        {"speed_diff_range": (7, 10), "slow_target_conditions": [(lambda x: x < 20, "混发"), (lambda x: x >= 20, "快运")]},
        {"speed_diff_range": (10, 15), "slow_target_conditions": [(lambda x: x < 25, "混发"), (lambda x: x >= 25, "快运")]},
        {"speed_diff_range": (15, 20), "slow_target_conditions": [(lambda x: x < 10, "慢运"), (lambda x: (10 <= x) & (x < 30), "混发"), (lambda x: x >= 30, "快运")]},
        {"speed_diff_range": (20, 25), "slow_target_conditions": [(lambda x: x < 15, "慢运"), (lambda x: (15 <= x) & (x < 40), "混发"), (lambda x: x >= 40, "快运")]},
        {"speed_diff_range": (25, 30), "slow_target_conditions": [(lambda x: x < 20, "慢运"), (lambda x: (20 <= x) & (x < 50), "混发"), (lambda x: x >= 50, "快运")]},
        {"speed_diff_range": (30, 35), "slow_target_conditions": [(lambda x: x < 25, "慢运"), (lambda x: (25 <= x) & (x < 75), "混发"), (lambda x: x >= 75, "快运")]},
        {"speed_diff_range": (35, 40), "slow_target_conditions": [(lambda x: x < 30, "慢运"), (lambda x: x >= 30, "混发")]},
        {"speed_diff_range": (40, np.inf), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "慢运")]}
    ],
    # 5. 沙特站点规则（对应“逻辑1(老品)-沙特”）
    "沙特": [
        {"speed_diff_range": (0, 3), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "快运")]},
        {"speed_diff_range": (3, 5), "slow_target_conditions": [(lambda x: x < 20, "混发"), (lambda x: x >= 20, "快运")]},
        {"speed_diff_range": (5, 7), "slow_target_conditions": [(lambda x: x < 25, "混发"), (lambda x: x >= 25, "快运")]},
        {"speed_diff_range": (7, 10), "slow_target_conditions": [(lambda x: x < 10, "慢运"), (lambda x: (10 <= x) & (x < 30), "混发"), (lambda x: x >= 30, "快运")]},
        {"speed_diff_range": (10, 15), "slow_target_conditions": [(lambda x: x < 15, "慢运"), (lambda x: (15 <= x) & (x < 40), "混发"), (lambda x: x >= 40, "快运")]},
        {"speed_diff_range": (15, 20), "slow_target_conditions": [(lambda x: x < 20, "慢运"), (lambda x: (20 <= x) & (x < 50), "混发"), (lambda x: x >= 50, "快运")]},
        {"speed_diff_range": (20, 25), "slow_target_conditions": [(lambda x: x < 25, "慢运"), (lambda x: (25 <= x) & (x < 75), "混发"), (lambda x: x >= 75, "快运")]},
        {"speed_diff_range": (25, 30), "slow_target_conditions": [(lambda x: x < 30, "慢运"), (lambda x: (30 <= x) & (x < 100), "混发"), (lambda x: x >= 100, "快运")]},
        {"speed_diff_range": (30, 35), "slow_target_conditions": [(lambda x: x < 40, "慢运"), (lambda x: x >= 40, "混发")]},
        {"speed_diff_range": (35, 40), "slow_target_conditions": [(lambda x: x < 50, "慢运"), (lambda x: x >= 50, "混发")]},
        {"speed_diff_range": (40, np.inf), "slow_target_conditions": [(lambda x: np.ones_like(x, dtype=bool), "慢运")]}
    ]
}
##
def get_sku():
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
    # df = df.sample(100)
    print(f'获取sku完成，共{len(df)}条！共耗时{t2 - t1:.2f}s')

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

def get_site():
    """  获取需要计算的站点 """
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

    # 20260303 暂时不计算阿联酋
    # df_site = df_site[df_site['site'] != 'ae']

    return df_site

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
    df_diff['站点'] = df_diff.apply(lambda x: 'other' if x['site_en'] == 'other' else x['site'], axis=1)
    df_diff.dropna(inplace=True)
    return df_diff

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
    df_diff_sg = df_diff[df_diff['site']=='other']
    df_diff_sg['site'] = 'SG'
    df_diff = pd.concat([df_diff, df_diff_sg])
    sql_site_table = """
        select site2 as site_en, site1 as site from domestic_warehouse_clear.yibai_site_table_amazon
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_site_table = conn_mx.ck_select_to_df(sql_site_table)
    df_diff = df_diff.rename(columns={'site': 'site_en'}).merge(df_site_table, on='site_en', how='left')
    df_diff['站点'] = df_diff.apply(lambda x: 'other' if x['site_en'] == 'other' else x['site'], axis=1)
    df_diff.dropna(inplace=True)
    # df_diff.to_excel('F://Desktop//df_diff.xlsx', index=0)
    # print(df_diff)
    return df_diff

def fee_para():
    # 默认参数：平台佣金、汇率
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select * from domestic_warehouse_clear.price_calculate_parameters
    """
    df_site_df = conn_ck.ck_select_to_df(sql)
    df_site_df = amazon_fbm_para(df_site_df, [], pd.DataFrame(), mode='AMAZON-FBA')

    return df_site_df

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

def get_fba_first_fee(df_temp, df_diff, i, org='YB'):
    # 差值fba
    t1 = time.time()
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    # print(df_temp)
    # print(df_diff_fba)
    if org == 'YB':
        df_fba = get_cost_range(df_temp, df_diff_fba)
    elif org == 'TT':
        df_fba = df_temp.copy()
        df_fba['cost_range'] = 'other'
        df_fba['first_product_line'] = -1
    df_fba = merge_four_dim_diff(df_fba, df_diff_fba, ['站点', 'first_product_line', 'cost_range'])
    df_fba['net_profit2'] = (df_fba['net_profit2'] / 100).astype('float')
    df_fba['net_interest_rate_target'] = (df_fba['net_interest_rate_target'] / 100).astype('float')
    df_fba['FBA目标毛利润率'] = df_fba['net_profit2'] + df_fba['net_interest_rate_target']
    df_fba['FBA差值'] = df_fba['net_profit2']
    col = ['sku', '站点', '重量', '成本', '长', '宽', '高', '货物价值密度', 'FBA目标毛利润率', 'FBA差值']
    df_fba = df_fba[col]
    ## 获取运费
    ## 优化头程计算代码
    # 0、获取头程属性。 输入 sku1 + contry_code ,输出 fba头程属性
    # 1、各站点都有空运。['德国','印度','新加坡','沙特']没有海运
    sql = """
        select site1 as `站点`,site3 as country_code FROM domestic_warehouse_clear.yibai_site_table_amazon
        """
    df_code = conn_ck.ck_select_to_df(sql)
    df_fba = pd.merge(df_fba, df_code, how='left', on=['站点'])
    # 头程属性
    df_fba['sku1'] = df_fba['sku']
    df_fba['站点1'] = df_fba['站点']
    ## 快运\慢运
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    if i in ['其他']:
        aut_fast = aut(df_fba, conn_ck, '海运')
    else:
        # print(df_fba)
        aut_fast = aut(df_fba, conn_ck, '空运')
    df_fba = pd.merge(df_fba, aut_fast, how='left', on=['sku1', 'country_code'])
    df_kuaiyun = tou_cheng_dingjiabi(df_fba, conn, '快运')
    df_kuaiyun = df_kuaiyun.rename(columns={'头程费_人民币': '头程费_快运'})
    df_kuaiyun = df_kuaiyun[['sku', '站点', '头程费_快运']]
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)

    aut_slow = aut(df_fba, conn_ck, '海运')
    df_fba = pd.merge(df_fba, aut_slow, how='left', on=['sku1', 'country_code'])
    df_manyun = tou_cheng_dingjiabi(df_fba, conn, '慢运')
    df_manyun = df_manyun.rename(columns={'头程费_人民币': '头程费_慢运'})
    df_manyun = df_manyun[['sku', '站点', '头程费_慢运']]
    df_fba.drop(['fba头程属性'], axis=1, inplace=True)
    t2 = time.time()
    #
    df_price = pd.merge(df_fba, df_kuaiyun[['sku', '站点', '头程费_快运']], how='left',
                        on=['sku', '站点'])
    df_price = pd.merge(df_price, df_manyun[['sku', '站点', '头程费_慢运']], how='left', on=['sku', '站点'])
    print(f'fba头程运费获取完成，共耗时{t2 - t1:.2f}s')
    return df_price

def get_fbm_fee(df_temp, df_diff, i, org='YB'):
    """
    获取国内仓运费数据，包括捆绑sku的运费
    """
    t1 = time.time()
    df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    if org == 'YB':
        df_fbm = get_cost_range(df_temp, df_diff_fbm)
    elif org == 'TT':
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

    conn_ck_0 = pd_to_ck(database='yibai_temp_hxx', data_sys='调价明细历史数据')
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

# 真实佣金率
def calculate_real_commission():
    """ 计算FBM链接真实佣金率，并存表 """
    sql = """
        SELECT sku, site, avg(`佣金率`) `平台抽成比例`, count(1) as `链接数量`, varPop(`佣金率`) AS `佣金率方差`
        FROM yibai_domestic.yibai_amazon_listing_profit
        WHERE site in ('美国', '加拿大', '墨西哥', '德国', '英国', '澳大利亚', '日本', '新加坡', '阿联酋', '沙特')
        and `渠道类型` = '国内仓'
        GROUP BY sku, site
    """
    conn_ck = pd_to_ck(database='yibai_domestic', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # print(df.info())
    if len(df) > 0:
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        conn_ck.ck_clear_table('amazon_sku_site_commission', db_name='yibai_oversea')

        conn_ck.ck_insert(df, 'amazon_sku_site_commission', if_exist='append')

    # df[df['site']=='美国'].to_excel('F://Desktop//df_fbm_commis.xlsx', index=0)

    # return df

def tt_calculate_real_commission():
    """ 计算FBM链接真实佣金率，并存表 """
    sql = """
        SELECT sku, site, avg(`佣金率`) `平台抽成比例`, count(1) as `链接数量`, varPop(`佣金率`) AS `佣金率方差`
        FROM tt_domestic.tt_amazon_listing_profit
        WHERE site in ('美国', '加拿大', '墨西哥', '德国', '英国', '澳大利亚', '日本', '新加坡', '阿联酋', '沙特')
        and `渠道类型` = '国内仓'
        GROUP BY sku, site
    """
    conn_ck = pd_to_ck(database='tt_domestic', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)

    # print(df.info())
    if len(df) > 0:
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        # conn_ck.ck_clear_table('tt_amazon_sku_site_commission', db_name='yibai_oversea')

        conn_ck.ck_insert(df, 'tt_amazon_sku_site_commission', if_exist='replace')

    # df[df['site']=='美国'].to_excel('F://Desktop//df_fbm_commis.xlsx', index=0)

    # return df

def get_real_commission(site='美国', org='YB'):
    """ 获取真实佣金率 """
    if org == 'YB':
        sql = f"""
            SELECT sku, site `站点`, `平台抽成比例`
            FROM yibai_oversea.amazon_sku_site_commission
            WHERE site in '{site}'
        """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df_commission = conn_ck.ck_select_to_df(sql)
    elif org == 'TT':
        sql = f"""
            SELECT sku, site `站点`, `平台抽成比例`
            FROM yibai_oversea.tt_amazon_sku_site_commission
            WHERE site in '{site}'
        """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df_commission = conn_ck.ck_select_to_df(sql)

    return df_commission

def prepare_table(table_type='sku'):
    """ 表准备 """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    if table_type == 'sku':
        table_name = f'pricing_ratio_sku_{date_today}'
    elif table_type == 'sku_final':
        table_name = f'pricing_ratio_final_{date_today}'
    elif table_type == 'center':
        table_name = 'pricing_ratio_to_center'

    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    conn_ck.ck_execute_sql(sql)
    print('结果表删除成功！')

    # 表创建
    if table_type == 'sku':
        sql = f"""
        CREATE TABLE support_document.{table_name}
        (
            `id` Int64,
            `date_id` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `税号` Nullable(String) COMMENT '针对墨西哥站点：本土企业税10.5%、跨境企业税16%、个人税22.8%',
            `成本` Nullable(Float32),
            `重量` Nullable(Float32),
            `长` Nullable(Float32),
            `宽` Nullable(Float32),
            `高` Nullable(Float32),
            `FBA目标毛利润率` Nullable(Float32),
            `FBA差值` Nullable(Float32),
            `头程费_快运` Nullable(Float32),
            `头程费_慢运` Nullable(Float32),
            `fba_fees` Nullable(Float32),
            `运费` Nullable(Float32),
            `FBM目标毛利润率` Nullable(Float32),
            `FBM差值` Nullable(Float32),
            `汇率` Nullable(Float32),
            `FBA税率` Nullable(Float32),
            `FBM税率` Nullable(Float32),
            `FBA快运定价` Nullable(Float32),
            `FBA慢运定价` Nullable(Float32),
            `FBA目标净利定价` Nullable(Float32) COMMENT '若建议物流方式为快运，则为FBA快运目标净利定价 ，若为慢运，则为FBA慢运目标净利定价',
            `FBM定价` Nullable(Float32) COMMENT 'FBM目标净利定价',
            `快运定价比` Nullable(Float32) COMMENT 'FBA快运定价/FBM定价',
            `慢运定价比` Nullable(Float32) COMMENT 'FBA慢运定价/FBM定价',
            `FBA快慢定价差_美元` Nullable(Float32),
            `发运方式_逻辑1` Nullable(String) COMMENT '',
            `发运方式_逻辑2` Nullable(String) COMMENT '',
            `发运方式_逻辑3` Nullable(String) COMMENT '',
            `发运方式_最终` Nullable(String) COMMENT '',
            `建议物流方式` Nullable(String) COMMENT '',
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
        """
    elif table_type == 'sku_final':
        sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `id` Int64,
            `date_id` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `税号` Nullable(String) COMMENT '针对墨西哥站点：本土企业税10.5%、跨境企业税16%、个人税22.8%',
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
            `update_time` String DEFAULT toString(now()) COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
        """
    elif table_type == 'center':
        sql = f"""
            CREATE TABLE IF NOT EXISTS support_document.{table_name}
            (
            `id` Int64,
            `sku` String,
            `站点` String COMMENT '站点',
            `税号` String COMMENT '针对墨西哥站点：本土企业税10.5%、跨境企业税16%、个人税22.8%',
            `FBA空运定价/FBM定价_修正` Float64 COMMENT 'FBA空运定价/FBM定价_修正',
            `FBA海运定价/FBM定价_修正` Float64 COMMENT 'FBA海运定价/FBM定价_修正',
            `FBA铁路定价/FBM定价_修正` Float64 COMMENT 'FBA铁路定价/FBM定价_修正',
            `FBA卡航定价/FBM定价_修正` Float64 COMMENT 'FBA卡航定价/FBM定价_修正',
            `FBA快运定价/FBM定价_修正` Float64 COMMENT 'FBA快运定价/FBM定价_修正',
            `FBA慢运定价/FBM定价_修正` Float64 COMMENT 'FBA慢运定价/FBM定价_修正',
            `FBA快运定价_修正` Float64 COMMENT 'FBA快运定价_修正',
            `FBA慢运定价_修正` Float64 COMMENT 'FBA慢运定价_修正',
            `FBA目标净利定价USD` Float64 COMMENT 'FBA目标净利定价USD',
            `FBM目标净利定价USD` Float64 COMMENT 'FBM目标净利定价USD',
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

    if table_type in ['sku', 'sku_final']:
        sql = f"""
            ALTER TABLE support_document.{table_name}
            DELETE where date_id = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
        """
        conn_ck.ck_execute_sql(sql)
        print('结果表今日数据删除成功！')
    elif table_type in ['center']:
        # ck
        sql = f"""
            truncate table support_document.{table_name}
        """
        conn_ck.ck_execute_sql(sql)
        # mysql
        sql = f"""
            TRUNCATE TABLE over_sea.{table_name}
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        conn.execute(sql)
        print('结果表今日数据删除成功！')

def tt_prepare_table(table_type='sku'):
    """ 表准备 """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    if table_type == 'sku':
        table_name = f'tt_pricing_ratio_sku_{date_today}'
    elif table_type == 'sku_final':
        table_name = f'tt_pricing_ratio_final_{date_today}'
    elif table_type == 'center':
        table_name = 'tt_pricing_ratio_to_center'

    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    conn_ck.ck_execute_sql(sql)
    print('结果表删除成功！')

    # 表创建
    if table_type == 'sku':
        sql = f"""
        CREATE TABLE support_document.{table_name}
        (
            `id` Int64,
            `date_id` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `税号` Nullable(String) COMMENT '针对墨西哥站点：本土企业税10.5%、跨境企业税16%、个人税22.8%',
            `成本` Nullable(Float32),
            `重量` Nullable(Float32),
            `长` Nullable(Float32),
            `宽` Nullable(Float32),
            `高` Nullable(Float32),
            `FBA目标毛利润率` Nullable(Float32),
            `FBA差值` Nullable(Float32),
            `头程费_快运` Nullable(Float32),
            `头程费_慢运` Nullable(Float32),
            `fba_fees` Nullable(Float32),
            `运费` Nullable(Float32),
            `FBM目标毛利润率` Nullable(Float32),
            `FBM差值` Nullable(Float32),
            `汇率` Nullable(Float32),
            `FBA税率` Nullable(Float32),
            `FBM税率` Nullable(Float32),
            `FBA快运定价` Nullable(Float32),
            `FBA慢运定价` Nullable(Float32),
            `FBA目标净利定价` Nullable(Float32) COMMENT '若建议物流方式为快运，则为FBA快运目标净利定价 ，若为慢运，则为FBA慢运目标净利定价',
            `FBM定价` Nullable(Float32) COMMENT 'FBM目标净利定价',
            `快运定价比` Nullable(Float32) COMMENT 'FBA快运定价/FBM定价',
            `慢运定价比` Nullable(Float32) COMMENT 'FBA慢运定价/FBM定价',
            `FBA快慢定价差_美元` Nullable(Float32),
            `发运方式_逻辑1` Nullable(String) COMMENT '',
            `发运方式_逻辑2` Nullable(String) COMMENT '',
            `发运方式_逻辑3` Nullable(String) COMMENT '',
            `发运方式_最终` Nullable(String) COMMENT '',
            `建议物流方式` Nullable(String) COMMENT '',
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
        """
    elif table_type == 'sku_final':
        sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `id` Int64,
            `date_id` String,
            `sku` String,
            `站点` String COMMENT '站点',
            `税号` Nullable(String) COMMENT '针对墨西哥站点：本土企业税10.5%、跨境企业税16%、个人税22.8%',
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
            `update_time` String DEFAULT toString(now()) COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `站点`)
        SETTINGS index_granularity = 8192
        """
    elif table_type == 'center':
        sql = f"""
            CREATE TABLE IF NOT EXISTS support_document.{table_name}
            (
            `id` Int64,
            `sku` String,
            `站点` String COMMENT '站点',
            `税号` String COMMENT '针对墨西哥站点：本土企业税10.5%、跨境企业税16%、个人税22.8%',
            `FBA空运定价/FBM定价_修正` Float64 COMMENT 'FBA空运定价/FBM定价_修正',
            `FBA海运定价/FBM定价_修正` Float64 COMMENT 'FBA海运定价/FBM定价_修正',
            `FBA铁路定价/FBM定价_修正` Float64 COMMENT 'FBA铁路定价/FBM定价_修正',
            `FBA卡航定价/FBM定价_修正` Float64 COMMENT 'FBA卡航定价/FBM定价_修正',
            `FBA快运定价/FBM定价_修正` Float64 COMMENT 'FBA快运定价/FBM定价_修正',
            `FBA慢运定价/FBM定价_修正` Float64 COMMENT 'FBA慢运定价/FBM定价_修正',
            `FBA快运定价_修正` Float64 COMMENT 'FBA快运定价_修正',
            `FBA慢运定价_修正` Float64 COMMENT 'FBA慢运定价_修正',
            `FBA目标净利定价USD` Float64 COMMENT 'FBA目标净利定价USD',
            `FBM目标净利定价USD` Float64 COMMENT 'FBM目标净利定价USD',
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

    if table_type in ['sku', 'sku_final']:
        sql = f"""
            ALTER TABLE support_document.{table_name}
            DELETE where date_id = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
        """
        conn_ck.ck_execute_sql(sql)
        print('结果表今日数据删除成功！')
    elif table_type in ['center']:
        # ck
        sql = f"""
            truncate table support_document.{table_name}
        """
        conn_ck.ck_execute_sql(sql)
        # mysql
        sql = f"""
            TRUNCATE TABLE over_sea.{table_name}
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        conn.execute(sql)
        print('结果表今日数据删除成功！')

# 202509新版sku定价比计算程序
def pricing_ratio_sku():
    """ 主程序 """

    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    df = get_sku()
    df['is_bundle'] = 0
    df_kunbang = add_kunbang_new()
    df_kunbang['is_bundle'] = 1
    # #
    df = pd.concat([df, df_kunbang])
    df = merge_first_product_line(df)
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量', '重量来源'], axis=1, inplace=True)
    w1 = df['长']*df['宽']*df['高']/5000
    df['计费重'] = np.where(w1 > (df['重量']/1000), w1,  (df['重量']/1000))
    df['货物价值密度'] = df['成本'] / df['计费重']
    # print(df.info())
    # df.to_excel('F://Desktop//df_pr.xlsx', index=0)
    df = df[(df['长']>0) & (df['宽']>0) & (df['高']>0) & (df['重量']>0)]
    # 站点、差值、佣金、vat
    df_site = get_site()
    # df_site = df_site[df_site['site']=='au']
    df_diff = get_diff()
    df_site_df = fee_para()
    calculate_real_commission()
    # df_all = pd.DataFrame()
    # 表准备
    prepare_table(table_type='sku')
    prepare_table(table_type='center')
    # 主循环。获取FBA运费、FBM运费、计算定价比
    for i in tqdm(df_site['site1'].unique()):
        # if i == '美国':
        print(f'开始计算{i}站点数据...')
        df.loc[df['is_bundle'] == 0, '站点'] = i
        df_temp = df[df['站点'] == i]

        # fba头程运费
        df_price = get_fba_first_fee(df_temp, df_diff, i)

        # fba尾程运费
        df_temp['fbafee计算方式'] = '普通'
        df_temp['最小购买数量'] = 1
        # df_temp.to_excel('F://Desktop//df_temp.xlsx', index=0)
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
        df_price = pd.merge(df_price, df_site_df[['站点', '汇率', '冗余系数']], how='left',
                            on=['站点'])
        # 接入真实佣金率
        df_commission = get_real_commission(site=i)
        if len(df_commission) == 0:
            df_commission = pd.DataFrame(columns=['sku', '站点', '平台抽成比例'])
        df_price = pd.merge(df_price, df_commission[['sku', '站点', '平台抽成比例']], how='left',
                            on=['sku', '站点'])
        df_price['平台抽成比例'] = df_price['平台抽成比例'].fillna(0.15)
        ##
        tax_rates_fba = {
            '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
            '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187
        }
        # tax_rates_fbm = {
        #     '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
        #     '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187, '印度': 0.3, '土耳其': 0.15,
        #     '澳大利亚': 0.09
        # }

        # 20251224：FBM税率取公共函数
        df_price['FBA税率'] = df_price['站点'].map(tax_rates_fba)
        df_price = shuilv(df_price, 'FBM', 'yb')
        df_price = df_price.rename(columns={'税率':'FBM税率'})
        # df_price['FBM税率'] = df_price['站点'].map(tax_rates_fbm)
        df_price[['FBA税率', 'FBM税率']] = df_price[['FBA税率', 'FBM税率']].fillna(0).astype(float)

        df_price['税号'] = ''
        # 墨西哥需要区分个人税和企业税。
        if i == '墨西哥':
            df_price['税号'] = '本土企业税'
            df_price['FBA税率'] = 0.095
            df_price_2 = df_price.copy()
            df_price_2['税号'] = '个人税'
            df_price_2['FBA税率'] = 0.228
            df_price_3 = df_price.copy()
            df_price_3['税号'] = '跨境企业税'
            df_price_3['FBA税率'] = 0.138
            df_price = pd.concat([df_price, df_price_2, df_price_3])

        ##
        df_price['调拨费'] = 0
        for fbafee_type in ['快运', '慢运']:
            df_price[f"FBA{fbafee_type}定价"] = (df_price["成本"] + df_price[f"头程费_{fbafee_type}"] + df_price[
                "调拨费"] + 2 + df_price["fba_fees"] * df_price["汇率"]) / (
                                                            1 - df_price["平台抽成比例"] - df_price["冗余系数"] -
                                                            df_price["FBA目标毛利润率"] - df_price["FBA税率"])
            df_price[f"FBA{fbafee_type}定价"] = df_price[f"FBA{fbafee_type}定价"].astype('float')
        df_price["FBM定价"] = (df_price["成本"] + df_price["运费"]) / (
                1 - df_price["平台抽成比例"] - df_price["冗余系数"] - df_price["FBM目标毛利润率"] - df_price[
            "FBM税率"])

        df_price['快运定价比'] = df_price['FBA快运定价'] / df_price['FBM定价']
        df_price['慢运定价比'] = df_price['FBA慢运定价'] / df_price['FBM定价']
        df_price['date_id'] = time.strftime('%Y-%m-%d')

        # 数值处理

        df_result = df_price[['date_id', 'sku', '站点', '税号','成本', '重量', '长', '宽', '高', 'FBA目标毛利润率', 'FBA差值',
                           '头程费_快运', '头程费_慢运', 'fba_fees', '运费', 'FBM目标毛利润率', 'FBM差值', '汇率', 'FBA税率', 'FBM税率',
                           'FBA快运定价','FBA慢运定价', 'FBM定价', '快运定价比', '慢运定价比']]

        # 异常数据存表
        # 表准备
        # table_name = f'pricing_ratio_test_{date_today}'
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df_price_null = df_result[(df_result['FBA快运定价'].isna()) | (df_result['运费'].isna()) | (df_result['fba_fees'] == 0)]
        conn_ck.ck_insert(df_price_null, 'pricing_ratio_sku_null_data', if_exist='append')

        ## 最终结果

        date_today = datetime.date.today().isoformat().replace('-', '')
        # table_name = f'pricing_ratio_sku_{date_today}'
        # table_name = f'pricing_ratio_sku_test'
        df_result = df_result[(~df_result['FBA快运定价'].isna()) & (~df_result['运费'].isna()) & (df_result['fba_fees'] != 0)]
        print(f'今日SKU维度定价比数据共{len(df_result)}条.')

        # 建议物流方式逻辑判断、上传中台数据备份
        df_result = fba_pricing_ratio_final(df_result, site=i)

        # df_all = pd.concat([df_result, df_all])

        # df_all.to_excel('F://Desktop//df_fba_2.xlsx', index=0)

        # 存表
        backup_data(df_result)

    # allegro定价比更新
    get_fbal_pr()

def tt_pricing_ratio_sku():
    """ 主程序 """

    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    df = get_sku()
    df['is_bundle'] = 0
    # df_kunbang = add_kunbang_new()
    # df_kunbang['is_bundle'] = 1
    # #
    # df = pd.concat([df, df_kunbang])
    df = merge_first_product_line(df)
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量', '重量来源'], axis=1, inplace=True)
    w1 = df['长']*df['宽']*df['高']/5000
    df['计费重'] = np.where(w1 > (df['重量']/1000), w1,  (df['重量']/1000))
    df['货物价值密度'] = df['成本'] / df['计费重']
    df = df[(df['长'] > 0) & (df['宽'] > 0) & (df['高'] > 0) & (df['重量'] > 0)]
    print(df.info())
    # df.to_excel('F://Desktop//df_pr.xlsx', index=0)

    # 站点、差值、佣金、vat
    df_site = get_site()
    df_diff = tt_get_diff()
    df_site_df = fee_para()
    tt_calculate_real_commission()

    # 表准备
    tt_prepare_table(table_type='sku')
    tt_prepare_table(table_type='center')
    # 主循环。获取FBA运费、FBM运费、计算定价比
    for i in tqdm(df_site['site1'].unique()):
        # if i == '美国':
        print(f'开始计算{i}站点数据...')
        df.loc[df['is_bundle'] == 0, '站点'] = i
        df_temp = df[df['站点'] == i]

        # fba头程运费
        df_price = get_fba_first_fee(df_temp, df_diff, i, org='TT')

        # fba尾程运费
        df_temp['fbafee计算方式'] = '普通'
        df_temp['最小购买数量'] = 1
        df_fba_fee = fba_ding_jia_biao(df_temp)
        print(f"尾程费为空的数量有{len(df_fba_fee[(df_fba_fee['fba_fees'].isna()) | (df_fba_fee['fba_fees'] == '')])}")
        df_fba_fee['fba_fees'] = pd.to_numeric(df_fba_fee['fba_fees'], errors='coerce')
        df_fba_fee['fba_fees'] = df_fba_fee['fba_fees'].fillna(0).astype(float)
        # FBM运费
        df_fbm = get_fbm_fee(df_temp, df_diff, i, org='TT')

        ## 定价计算
        df_price = pd.merge(df_price, df_fba_fee[['sku', '站点', 'fba_fees']], how='left', on=['sku', '站点'])
        df_price = pd.merge(df_price, df_fbm[['sku', '站点', '运费', 'FBM目标毛利润率', 'FBM差值']], how='left',
                            on=['sku', '站点'])
        df_price = pd.merge(df_price, df_site_df[['站点', '汇率', '冗余系数']], how='left',
                            on=['站点'])
        # 接入真实佣金率
        df_commission = get_real_commission(site=i, org='TT')
        if len(df_commission) == 0:
            df_commission = pd.DataFrame(columns=['sku', '站点', '平台抽成比例'])
        df_price = pd.merge(df_price, df_commission[['sku', '站点', '平台抽成比例']], how='left',
                            on=['sku', '站点'])
        df_price['平台抽成比例'] = df_price['平台抽成比例'].fillna(0.15)
        ##
        tax_rates_fba = {
            '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
            '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187
        }
        # tax_rates_fbm = {
        #     '英国': 0.1667, '德国': 0.1597, '法国': 0.1667, '意大利': 0.1803, '西班牙': 0.1736,
        #     '荷兰': 0.1736, '比利时': 0.1736, '瑞典': 0.2, '波兰': 0.187, '印度': 0.3, '土耳其': 0.15,
        #     '澳大利亚': 0.09
        # }
        # 20251224：FBM税率取公共函数
        df_price['FBA税率'] = df_price['站点'].map(tax_rates_fba)
        df_price = shuilv(df_price, 'FBM', 'tt')
        df_price = df_price.rename(columns={'税率':'FBM税率'})
        # df_price['FBM税率'] = df_price['站点'].map(tax_rates_fbm)
        df_price[['FBA税率', 'FBM税率']] = df_price[['FBA税率', 'FBM税率']].fillna(0).astype(float)

        df_price['税号'] = ''
        # 墨西哥需要区分个人税和企业税。
        if i == '墨西哥':
            df_price['税号'] = '本土企业税'
            df_price['FBA税率'] = 0.095
            df_price_2 = df_price.copy()
            df_price_2['税号'] = '个人税'
            df_price_2['FBA税率'] = 0.228
            df_price_3 = df_price.copy()
            df_price_3['税号'] = '跨境企业税'
            df_price_3['FBA税率'] = 0.138
            df_price = pd.concat([df_price, df_price_2, df_price_3])

        ##
        df_price['调拨费'] = 0
        for fbafee_type in ['快运', '慢运']:
            df_price[f"FBA{fbafee_type}定价"] = (df_price["成本"] + df_price[f"头程费_{fbafee_type}"] + df_price[
                "调拨费"] + 2 + df_price["fba_fees"] * df_price["汇率"]) / (
                                                            1 - df_price["平台抽成比例"] - df_price["冗余系数"] -
                                                            df_price["FBA目标毛利润率"] - df_price["FBA税率"])
            df_price[f"FBA{fbafee_type}定价"] = df_price[f"FBA{fbafee_type}定价"].astype('float')
        df_price["FBM定价"] = (df_price["成本"] + df_price["运费"]) / (
                1 - df_price["平台抽成比例"] - df_price["冗余系数"] - df_price["FBM目标毛利润率"] - df_price[
            "FBM税率"])

        df_price['快运定价比'] = df_price['FBA快运定价'] / df_price['FBM定价']
        df_price['慢运定价比'] = df_price['FBA慢运定价'] / df_price['FBM定价']
        df_price['date_id'] = time.strftime('%Y-%m-%d')

        # 数值处理
        # df_price = df_price[df_price['fba_fees'] != '不符合FBA物流条件']
        df_result = df_price[['date_id', 'sku', '站点', '税号','成本', '重量', '长', '宽', '高', 'FBA目标毛利润率', 'FBA差值',
                           '头程费_快运', '头程费_慢运', 'fba_fees', '运费', 'FBM目标毛利润率', 'FBM差值', '汇率', 'FBA税率', 'FBM税率',
                           'FBA快运定价','FBA慢运定价', 'FBM定价', '快运定价比', '慢运定价比']]
        print(df_result.info())
        # col = ['成本', '运费',  'fba_fees']
        # df_res[col] = df_res[col].astype(float).round(4)
        # col = ['FBA快运定价', 'FBA慢运定价', '头程费快运', '头程费慢运']
        # df_res[col] = df_res[col].astype('float').round(4).fillna(-999)
        # df_price.to_excel('F://Desktop//df_price_temp.xlsx', index=0)

        # 异常数据存表
        # 表准备
        # table_name = f'pricing_ratio_test_{date_today}'
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df_price_null = df_result[(df_result['FBA快运定价'].isna()) | (df_result['运费'].isna()) | (df_result['fba_fees'] == 0)]
        conn_ck.ck_insert(df_price_null, 'tt_pricing_ratio_sku_null_data', if_exist='replace')

        ## 最终结果

        date_today = datetime.date.today().isoformat().replace('-', '')
        # table_name = f'pricing_ratio_sku_{date_today}'
        # table_name = f'pricing_ratio_sku_test'
        df_result = df_result[(~df_result['FBA快运定价'].isna()) & (~df_result['运费'].isna()) & (df_result['fba_fees'] != 0)]
        print(f'今日SKU维度定价比数据共{len(df_result)}条.')

        # 建议物流方式逻辑判断、上传中台数据备份
        tt_fba_pricing_ratio_final(df_result, site=i)

        # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        # conn_ck.ck_insert(df_result, table_name, if_exist='append')

def fba_pricing_ratio_final(df, site='墨西哥'):
    """
    fba新品物流方式 202509新逻辑
    """
    # sql = """
    #     SELECT *
    #     FROM yibai_oversea.pricing_ratio_sku_test
    #     WHERE date = (SELECT max(date) FROM yibai_oversea.pricing_ratio_sku_test)
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df = conn_ck.ck_select_to_df(sql)
    # df = df.sample(100000)
    # print(df.info())
    df_site_df = fee_para()

    # 价格统一转化为美元
    us_rate = df_site_df.loc[df_site_df['站点']=='美国','汇率'].values[0]
    df['FBA快运定价'] = df['FBA快运定价']/us_rate
    df['FBA慢运定价'] = df['FBA慢运定价'] / us_rate
    df['FBM定价'] = df['FBM定价'] / us_rate
    df['FBA快慢定价差_美元'] = (df['FBA快运定价'] - df['FBA慢运定价'])
    df['FBA快慢定价差_美元'] = np.where(df['FBA快慢定价差_美元']<=0, 0.001, df['FBA快慢定价差_美元'])

    # 条件分箱
    df = cut_bins(df)

    # 逻辑1
    # df = fba_rule_1(df)
    df = fba_rule_1_new(df, site=site)

    # 逻辑2
    df = fba_rule_2(df)

    # 逻辑3
    df = fba_rule_3(df)

    # 最终逻辑
    df = fba_rule_final(df)

    c1 = df['发运方式_最终'] == '快运'
    c2 = df['发运方式_最终'] == '慢运'
    df['FBA目标净利定价'] = np.select([c1,c2], [df['FBA快运定价'],df['FBA慢运定价']], np.nan)

    # 字段整理
    df = df.rename(columns={'date':'date_id'})
    col = ['FBA快慢定价差_分段','FBA慢运目标定价_分段','发运方式_逻辑2_快运',
           '发运方式_逻辑2_慢运','计费重']
    df.drop(col, axis=1, inplace=True)
    df['建议物流方式'] = df['建议物流方式'].astype(str)
    df['发运方式_逻辑1'] = df['发运方式_逻辑1'].fillna('').astype(str)

    df['成本'] = df['成本'].fillna(0).astype(float)
    float_cols = df.select_dtypes(include=['float']).columns
    df[float_cols] = df[float_cols].round(4)

    print(df.info())

    # df['update_date'] = time.strftime('%Y-%m-%d')

    # date_today = datetime.date.today().isoformat().replace('-', '')
    # table_name = f'pricing_ratio_sku_{date_today}'
    # conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    # conn_ck.ck_insert(df, table_name, if_exist='append')
    #
    # # 上传中台数据备份
    # col = ['sku','站点','税号', '建议物流方式', 'FBA快运定价','FBA慢运定价','FBA目标净利定价','FBM定价','快运定价比','慢运定价比']
    # df = df[col]
    # # 'FBA目标净利定价': 'FBA目标净利定价', 'FBM定价': 'FBM目标净利定价',
    # dic = {'FBA快运定价':'FBA快运定价_修正','FBA慢运定价':'FBA慢运定价_修正','快运定价比':'FBA快运定价/FBM定价_修正','慢运定价比':'FBA慢运定价/FBM定价_修正',
    #        'FBA目标净利定价':'FBA目标净利定价USD','FBM定价':'FBM目标净利定价USD'}
    # df = df.rename(columns=dic)
    # # df.drop(['FBA目标净利定价USD','FBA目标净利定价USD'], axis=1, inplace=True)
    # col_2 = ['FBA空运定价/FBM定价_修正','FBA海运定价/FBM定价_修正','FBA铁路定价/FBM定价_修正','FBA卡航定价/FBM定价_修正',
    #          '快慢同价利率差_修正']
    # df = df.assign(**{col: 0 for col in col_2})
    #
    # # print(df.info())
    #
    # upload_pricing_ratio(df, 'pricing_ratio_to_center')

    return df


def backup_data(df):
    """ 存表 """
    date_today = datetime.date.today().isoformat().replace('-', '')
    table_name = f'pricing_ratio_sku_{date_today}'
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, table_name, if_exist='append')

    # 上传中台数据备份
    col = ['sku','站点','税号', '建议物流方式', 'FBA快运定价','FBA慢运定价','FBA目标净利定价','FBM定价','快运定价比','慢运定价比']
    df = df[col]
    # 'FBA目标净利定价': 'FBA目标净利定价', 'FBM定价': 'FBM目标净利定价',
    dic = {'FBA快运定价':'FBA快运定价_修正','FBA慢运定价':'FBA慢运定价_修正','快运定价比':'FBA快运定价/FBM定价_修正','慢运定价比':'FBA慢运定价/FBM定价_修正',
           'FBA目标净利定价':'FBA目标净利定价USD','FBM定价':'FBM目标净利定价USD'}
    df = df.rename(columns=dic)
    # df.drop(['FBA目标净利定价USD','FBA目标净利定价USD'], axis=1, inplace=True)
    col_2 = ['FBA空运定价/FBM定价_修正','FBA海运定价/FBM定价_修正','FBA铁路定价/FBM定价_修正','FBA卡航定价/FBM定价_修正',
             '快慢同价利率差_修正']
    df = df.assign(**{col: 0 for col in col_2})

    # print(df.info())

    upload_pricing_ratio(df, 'pricing_ratio_to_center')

def tt_fba_pricing_ratio_final(df, site='墨西哥'):
    """
    fba新品物流方式 202509新逻辑
    """
    # sql = """
    #     SELECT *
    #     FROM yibai_oversea.pricing_ratio_sku_test
    #     WHERE date = (SELECT max(date) FROM yibai_oversea.pricing_ratio_sku_test)
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df = conn_ck.ck_select_to_df(sql)
    # df = df.sample(100000)
    # print(df.info())
    df_site_df = fee_para()

    # 价格统一转化为美元
    us_rate = df_site_df.loc[df_site_df['站点']=='美国','汇率'].values[0]
    df['FBA快运定价'] = df['FBA快运定价']/us_rate
    df['FBA慢运定价'] = df['FBA慢运定价'] / us_rate
    df['FBM定价'] = df['FBM定价'] / us_rate
    df['FBA快慢定价差_美元'] = (df['FBA快运定价'] - df['FBA慢运定价'])
    df['FBA快慢定价差_美元'] = np.where(df['FBA快慢定价差_美元']<=0, 0.001, df['FBA快慢定价差_美元'])

    # 条件分箱
    df = cut_bins(df)

    # 逻辑1
    df = fba_rule_1_new(df, site=site)

    # 逻辑2
    df = fba_rule_2(df)

    # 逻辑3
    df = fba_rule_3(df)

    # 最终逻辑
    df = fba_rule_final(df)

    c1 = df['发运方式_最终'] == '快运'
    c2 = df['发运方式_最终'] == '慢运'
    df['FBA目标净利定价'] = np.select([c1,c2], [df['FBA快运定价'],df['FBA慢运定价']], np.nan)

    # 字段整理
    df = df.rename(columns={'date':'date_id'})
    col = ['FBA快慢定价差_分段','FBA慢运目标定价_分段','发运方式_逻辑2_快运',
           '发运方式_逻辑2_慢运','计费重']
    df.drop(col, axis=1, inplace=True)
    df['建议物流方式'] = df['建议物流方式'].astype(str)
    df['发运方式_逻辑1'] = df['发运方式_逻辑1'].fillna('').astype(str)

    df['成本'] = df['成本'].fillna(0).astype(float)
    float_cols = df.select_dtypes(include=['float']).columns
    df[float_cols] = df[float_cols].round(2)

    print(df.info())

    # df['update_date'] = time.strftime('%Y-%m-%d')
    # df.to_excel('F://Desktop//df_fba.xlsx', index=0)

    date_today = datetime.date.today().isoformat().replace('-', '')
    table_name = f'tt_pricing_ratio_sku_{date_today}'
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, table_name, if_exist='append')

    # 上传中台数据备份
    col = ['sku','站点','税号', '建议物流方式', 'FBA快运定价','FBA慢运定价','FBA目标净利定价','FBM定价','快运定价比','慢运定价比']
    df = df[col]
    # 'FBA目标净利定价': 'FBA目标净利定价', 'FBM定价': 'FBM目标净利定价',
    dic = {'FBA快运定价':'FBA快运定价_修正','FBA慢运定价':'FBA慢运定价_修正','快运定价比':'FBA快运定价/FBM定价_修正','慢运定价比':'FBA慢运定价/FBM定价_修正',
           'FBA目标净利定价':'FBA目标净利定价USD','FBM定价':'FBM目标净利定价USD'}
    df = df.rename(columns=dic)
    # df.drop(['FBA目标净利定价USD','FBA目标净利定价USD'], axis=1, inplace=True)
    col_2 = ['FBA空运定价/FBM定价_修正','FBA海运定价/FBM定价_修正','FBA铁路定价/FBM定价_修正','FBA卡航定价/FBM定价_修正',
             '快慢同价利率差_修正']
    df = df.assign(**{col: 0 for col in col_2})

    print(df.info())

    upload_pricing_ratio(df, 'tt_pricing_ratio_to_center')


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


def upload_pricing_ratio(df, table_name='pricing_ratio_to_center'):
    """
    需将定价比数据上传到销售中台。
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = time.strftime('%Y%m%d')

    # 存表
    print('上传至中台数据备份...')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck.ck_insert(df, table_name, if_exist='append')
    # 20240428 mysql也备份一次，用于otter同步
    df['update_time'] = time.strftime('%Y-%m-%d')
    write_to_sql(df, table_name)

    # # 20240521 新增监控
    # sql = f"""
    #     SELECT date, `站点`, `建议物流方式`, count(1) as `数量`, 'sku' as `维度`
    #     FROM support_document.pricing_ratio_sku_{date_today}
    #     GROUP BY date, `站点`, `建议物流方式`
    # """
    # conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    # df_m = conn_ck.ck_select_to_df(sql)
    # df_m['建议物流方式2'] = df_m['建议物流方式'].map(map_values)
    #
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # date = time.strftime('%Y-%m-%d')
    # table_name = 'ads_pricing_ratio'
    # sql = f"""
    # DELETE FROM over_sea.{table_name} WHERE date = '{date}' and `维度` = 'sku'
    # """
    # conn.execute(sql)
    # conn.close()
    # write_to_sql(df_m, 'ads_pricing_ratio')


def cut_bins(df):
    """
    对主要条件分箱：FBA快慢定价差、FBA慢运目标定价
    """
    df['FBA快慢定价差_分段'] = pd.cut(df['FBA快慢定价差_美元'], bins=[-1, 1, 2, 3, 5, 7, 10, 15, 20, 25, 999],
                                    labels=['A∈(0,1]', 'A∈(1,2]', 'A∈(2,3]', 'A∈(3,5]', 'A∈(5,7]','A∈(7,10]',
                                            'A∈(10,15]','A∈(15,20]','A∈(20,25]','A∈(25,∞)'])
    df['FBA快慢定价差_分段'] = np.where(df['FBA快慢定价差_美元'] > 25, 'A∈(25,∞)', df['FBA快慢定价差_分段'])

    df['FBA慢运目标定价_分段'] = pd.cut(df['FBA慢运定价'], bins=[-1, 10, 15, 20, 25, 30, 40, 50, 75, 100, 999],
                                  labels=['B∈(0,10]', 'B∈(10,15]', 'B∈(15,20]', 'B∈(20,25]', 'B∈(25,30]', 'B∈(30,40]',
                                          'B∈(40,50]', 'B∈(50,75]', 'B∈(75,100]', 'B∈(100,∞)'])
    df['FBA慢运目标定价_分段'] = np.where(df['FBA慢运定价'] > 100, 'B∈(100,∞)', df['FBA慢运目标定价_分段'])


    return df

# def fba_rule_1(df):
#     """ 逻辑1 """
#     sql = """
#         SELECT *
#         FROM over_sea.fba_pricing_ratio_para
#     """
#     conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
#     df_para = conn.read_sql(sql)
#     df_para = df_para.fillna(np.inf)
#     df = pd.merge(df, df_para, how='left', on=['FBA快慢定价差_分段','FBA慢运目标定价_分段'])
#
#     return df


def process_site_rules(df, rules, speed_diff_col, slow_target_col):
    """
    批量处理单个站点的规则
    :param df: 单个站点的子DataFrame
    :param rules: 该站点对应的规则列表
    :param speed_diff_col: 快慢定价差的列名
    :param slow_target_col: 慢运目标售价的列名
    :return: 匹配后的发运方式Series
    """
    result = pd.Series(np.nan, index=df.index)  # 初始化结果，索引与输入一致

    for rule in rules:
        # 1. 批量判断「快慢定价差」是否在当前规则区间内（左闭右开）
        sd_min, sd_max = rule["speed_diff_range"]
        mask_speed = df[speed_diff_col].between(sd_min, sd_max, inclusive="left")

        # 2. 筛选出“未匹配结果”且“符合当前区间”的行
        mask_unmatched = result.isna()
        mask_current = mask_speed & mask_unmatched

        if not mask_current.any():
            continue  # 无符合条件的行，跳过当前规则

        # 3. 批量匹配「慢运目标售价」的子条件
        slow_target_vals = df.loc[mask_current, slow_target_col]  # 当前区间内的目标售价
        for cond, shipping in rule["slow_target_conditions"]:
            # 对当前区间内的未匹配行，批量判断子条件
            mask_cond = cond(slow_target_vals)
            # 批量赋值结果（仅更新符合条件的行）
            result.loc[slow_target_vals[mask_cond].index] = shipping

    return result


def fba_rule_1_new(df, speed_diff_col="FBA快慢定价差_美元", slow_target_col="FBA慢运定价", site_col="站点", site='墨西哥'):
    """
    应用FBA规则到整个DataFrame（区分站点批量处理）
    :param df: 原始DataFrame，需包含站点、快慢定价差、慢运目标售价列
    :param speed_diff_col: 快慢定价差的列名（默认："FBA快慢定价差"）
    :param slow_target_col: 慢运目标售价的列名（默认："FBA慢运目标售价"）
    :param site_col: 站点列名（默认："站点"）
    :return: 新增「发运方式_逻辑1」列的DataFrame
    """
    # 初始化结果列
    df = df.copy()  # 避免修改原数据
    df["发运方式_逻辑1"] = ''

    # 提取配置规则中的站点列表
    valid_sites = [k for k in fba_rule_vectorized_new.keys() if k is not None]

    mask_site = df[site_col] == site

    # 校验逻辑：site不在规则列表则置为None
    if site not in valid_sites:
        site = None

    if mask_site.any():
        df_site = df.loc[mask_site]
        result_site = process_site_rules(
            df=df_site,
            rules=fba_rule_vectorized_new[site],
            speed_diff_col=speed_diff_col,
            slow_target_col=slow_target_col
        )
        df.loc[mask_site, "发运方式_逻辑1"] = result_site

    return df

def fba_rule_2(df):
    """ 逻辑2 """
    for t in ['快运', '慢运']:
        c1 = (df[f'FBM定价'] >= 0) & (df['FBM定价'] < 20) & (df[f'{t}定价比'] <= 2)
        c2 = (df[f'FBM定价'] >= 20) & (df['FBM定价'] < 40) & (df[f'{t}定价比'] <= 1.8)
        c3 = (df[f'FBM定价'] >= 40) & (df['FBM定价'] < 60) & (df[f'{t}定价比'] <= 1.7)
        c4 = (df[f'FBM定价'] >= 60) & (df['FBM定价'] < 80) & (df[f'{t}定价比'] <= 1.6)
        c5 = (df[f'FBM定价'] >= 80) & (df['FBM定价'] < 100) & (df[f'{t}定价比'] <= 1.6)
        c6 = (df[f'FBM定价'] >= 100) & (df[f'{t}定价比'] <= 1.4)
        combined = np.logical_or.reduce([c1, c2, c3, c4, c5, c6])
        df[f'发运方式_逻辑2_{t}'] = np.where(combined, t, '无建议物流方式')

    df['发运方式_逻辑2'] = np.where(df['发运方式_逻辑2_快运'] == '无建议物流方式', df['发运方式_逻辑2_慢运'], df['发运方式_逻辑2_快运'])

    return df

def fba_rule_3(df):
    """ 逻辑三 """
    c1 = (df['FBM定价'] >= 0) & (df['FBM定价'] < 20) & (df['慢运定价比'] <= 1.6) & (df['快运定价比'] <= 2.3)
    c2 = (df['FBM定价'] >= 20) & (df['FBM定价'] < 40) & (df['慢运定价比'] <= 1.5) & (df['快运定价比'] <= 2.1)
    c3 = (df['FBM定价'] >= 40) & (df['FBM定价'] < 60) & (df['慢运定价比'] <= 1.4) & (df['快运定价比'] <= 2)
    c4 = (df['FBM定价'] >= 60) & (df['FBM定价'] < 80) & (df['慢运定价比'] <= 1.4) & (df['快运定价比'] <= 1.9)
    c5 = (df['FBM定价'] >= 80) & (df['FBM定价'] < 100) & (df['慢运定价比'] <= 1.3) & (df['快运定价比'] <= 1.9)
    c6 = (df['FBM定价'] >= 100) & (df['慢运定价比'] <= 1.2) & (df['快运定价比'] <= 1.7)
    combined = np.logical_or.reduce([c1, c2, c3, c4, c5, c6])
    df['发运方式_逻辑3'] = np.where(combined, '慢运', '无建议物流方式')

    return df

def fba_rule_final(df):
    """ 最终判断逻辑 """
    c1 = (df['发运方式_逻辑1'] == '快运') & (df['发运方式_逻辑2'] == '快运')
    c2 = (df['发运方式_逻辑1'] == '快运') & (df['发运方式_逻辑2'] == '慢运')
    c3 = (df['发运方式_逻辑1'] == '快运') & (df['发运方式_逻辑2'] == '无建议物流方式')
    c4 = (df['发运方式_逻辑1'] == '慢运') & (df['发运方式_逻辑2'] == '快运')
    c5 = (df['发运方式_逻辑1'] == '慢运') & (df['发运方式_逻辑2'] == '慢运')
    c6 = (df['发运方式_逻辑1'] == '慢运') & (df['发运方式_逻辑2'] == '无建议物流方式')
    c7 = (df['发运方式_逻辑1'] == '混发') & (df['发运方式_逻辑2'] == '快运')
    c8 = (df['发运方式_逻辑1'] == '混发') & (df['发运方式_逻辑2'] == '慢运')
    c9 = (df['发运方式_逻辑1'] == '混发') & (df['发运方式_逻辑2'] == '无建议物流方式')

    df['发运方式_最终'] = np.select([c1,c2,c3,c4,c5,c6,c7,c8,c9],[
        '快运','无建议物流方式','无建议物流方式','慢运','慢运','无建议物流方式','快运','8','无建议物流方式'])

    df['发运方式_最终'] = np.where(df['发运方式_最终']=='8', df['发运方式_逻辑3'], df['发运方式_最终'])

    # 新加坡、日本站点逻辑处理
    c1 = (df['站点'].isin(['新加坡']))
    df['发运方式_最终'] = np.where(c1, df['发运方式_逻辑2_快运'], df['发运方式_最终'])

    col = ['重量','长','宽','高']
    df[col] = df[col].fillna(0).astype(float)
    df['计费重'] = np.maximum(df['重量'] / 1000, df['长'] * df['宽'] * df['高'] / 5000)
    c1 = (df['站点']=='日本') & (df['计费重']<1) & (df['发运方式_最终']=='慢运')
    df['发运方式_最终'] = np.where(c1, '无建议物流方式', df['发运方式_最终'])

    # 匹配建议物流方式（数字枚举值）
    sql = """
        SELECT `站点`,`发运方式` '发运方式_最终',`建议物流方式`
        FROM over_sea.fba_pricing_ratio_para
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_para = conn.read_sql(sql)
    df = pd.merge(df, df_para, how='left', on=['站点', '发运方式_最终'])

    df['建议物流方式'] = df['建议物流方式'].fillna(0).astype(int)

    # df.drop('计费重', axis=1, inplace=True)

    return df

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



def temp_df():
    sql = """
        SELECT *
        FROM support_document.pricing_ratio_sku_20260126
        WHERE sku = '2214250094112' and `站点` in ('美国', '英国','德国')
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    df.to_excel('F://Desktop//df_pr.xlsx', index=0)

## fbal定价比数据上传中台
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



if __name__ == '__main__':

    pricing_ratio_sku()

    # fba_pricing_ratio_final()

    # tt_pricing_ratio_sku()
    # df_site_df = fee_para()
    # print(df_site_df.info())
    # us_rate = df_site_df.loc[df_site_df['站点']=='美国','汇率'].values[0]
    # print(us_rate)
    # tt_get_diff()
    # temp_df()
    # get_real_commission()
    # get_fbal_pr()
