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
from pulic_func.price_adjust_web_service.make_price import amazon_fba_para
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

def get_sku():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 产品表里取所有sku（筛选在售 product_status = 9 国内仓属性 attr_value_id = 67）
    sql = f"""
    with sku_table as (
        select distinct sku from yibai_prod_base_sync.yibai_prod_sku_select_attr
        where attr_value_id = 67
    ),
    sku_table2 as (
        SELECT sku, sum(purchase_on_way_count + stock) as stock
        FROM yb_datacenter.yb_stock
        WHERE 
            date_id = (SELECT max(date_id) FROM yb_datacenter.yb_stock)
            and warehouse_id in (SELECT id FROM yb_datacenter.yb_warehouse WHERE type = 'inland')
        GROUP BY sku
        HAVING stock > 0
    ),
    sku_table3 as (
        select sku from sku_table2
        where stock > 0
    )
    select 
    a.sku sku,
    --a.product_status product_status,
    a.cost as cost,
    b.`站点FBA` `站点FBA`,
    b.`站点FBM` `站点FBM`,
    b.site site
    from (
        select
            distinct sku sku,
            --product_status product_status,
            toFloat64(product_cost) as cost,
            '1' as temp
        from yibai_prod_base_sync.yibai_prod_sku
        where sku in (select * from sku_table)
        and (
            product_status = 9 
            or (product_status != 9 and sku in (select * from sku_table3))
        )
        and (sku not like '%%*%%' and sku not like '%%+%%')
    ) a
    left join (
        SELECT 
        site1 as `站点FBA`,
        site1 as `站点FBM`,
        site1 as `site`,
        '1' as temp
    from domestic_warehouse_clear.site_table bb
    where bb.site not in ('be', 'fr', 'it', 'sp', 'nl', 'pl', 'se', 'tr','br','in')
    ) b
    on a.temp = b.temp
    """
    df = conn_mx.ck_select_to_df(sql)
    # df = df.sample(10000)
    print('获取sku完成！')
    return df

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
        select site2 as site_en, site1 as site from domestic_warehouse_clear.site_table
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

def FBA_jisuan(df, df_site, Organization=1, worker_group='易佰', fbafee_type='空运'):
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')

    if '数量' not in list(df.columns):
        df['数量'] = 1
    df['数量'] = df['数量'].fillna(1)
    if 'id' not in list(df.columns):
        df['id'] = df.index

    df['fbafee计算方式'] = '普通'
    # 增加最小购买数量
    df['最小购买数量'] = 1
    df = df[df['sku'].notnull()]

    df['头程方式'] = fbafee_type

    for col in ['税率', '售价', 'account_id']:
        if col not in df.columns:
            df[col] = None
    df['售价'] = df['售价'].astype(float)
    df['account_id'] = df['account_id'].fillna(0)
    df['seller_sku'] = df['seller_sku'].fillna('')
    df.loc[df['站点'] == '英国', '税率'] = 0.1667
    df.loc[df['站点'] == '德国', '税率'] = 0.1597
    df.loc[df['站点'] == '法国', '税率'] = 0.1667
    df.loc[df['站点'] == '意大利', '税率'] = 0.1803
    df.loc[df['站点'] == '西班牙', '税率'] = 0.1736
    df.loc[df['站点'] == '荷兰', '税率'] = 0.1736
    df.loc[df['站点'] == '比利时', '税率'] = 0.1736
    df.loc[df['站点'] == '瑞典', '税率'] = 0.2
    df.loc[df['站点'] == '波兰', '税率'] = 0.187
    df['税率'] = df['税率'].fillna(0)
    df['税率'] = df['税率'].astype(float)
    sql = """
        SELECT site1 as `站点`,FBA_refund_rate as `退款率` 
        FROM domestic_warehouse_clear.site_table
        """
    df_amazon = conn_mx.ck_select_to_df(sql)
    df = df.merge(df_amazon, on=['站点'], how='left')
    df['index'] = df.index
    # 匹配尺寸重量
    df = chicun_zhongliang(df, Organization, conn_mx)
    # 计算头程
    df_bucunzai = df[df['成本'].isnull()]
    df_bucunzai['成本'] = '不存在'
    df_bucunzai1 = df[(df['成本'].notnull()) & (df['重量'] == 0)]
    sql = """
        select site1 as `站点`,site3 as country_code FROM domestic_warehouse_clear.site_table
        """
    df1 = conn_mx.ck_select_to_df(sql)
    df0 = pd.DataFrame({'站点': ['加拿大-泛美', '墨西哥-泛美'],
                        'country_code': ['US', 'US']})
    df1 = df1.append(df0)
    df = df.merge(df1, on=['站点'], how='left')

    df_HY = df[(df['头程方式'] == '海运') & (df['成本'].notnull()) & (df['重量'] > 0)]
    df_TL = df[(df['头程方式'] == '铁路') & (df['成本'].notnull()) & (df['重量'] > 0)]
    df_kh = df[(df['头程方式'] == '卡航') & (df['成本'].notnull()) & (df['重量'] > 0)]
    df_ky = df[(df['头程方式'] == '空运') & (df['成本'].notnull()) & (df['重量'] > 0)]

    # 快慢维度
    df_fast = df[(df['头程方式'] == '快运') & (df['成本'].notnull()) & (df['重量'] > 0)]
    df_slow = df[(df['头程方式'] == '慢运') & (df['成本'].notnull()) & (df['重量'] > 0)]
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # 快运
    if len(df_fast['sku']) > 0:
        df_fast.rename(columns={"sku": "sku1"}, inplace=True)
        # result = aut(df_fast, conn_mx)
        # 20240131 快运中，空运和卡航的头程属性可能不同
        if len(df_fast[~df_fast['站点'].isin(['德国', '英国'])]) != 0:
            result_ky = aut(df_fast[~df_fast['站点'].isin(['德国', '英国'])], conn_mx, '空运')
        else:
            result_ky = pd.DataFrame(columns=['sku1', 'country_code', 'fba头程属性'])
        if len(df_fast[df_fast['站点'].isin(['德国', '英国'])]) != 0:
            result_kh = aut(df_fast[df_fast['站点'].isin(['德国', '英国'])], conn_mx, '卡航')
        else:
            result_kh = pd.DataFrame(columns=['sku1', 'country_code', 'fba头程属性'])
        result = pd.concat([result_ky, result_kh])
        df_fast = df_fast.merge(result, on=["sku1", "country_code"], how='left')
        df_fast['fba头程属性'].fillna('普货', inplace=True)
        df_fast.rename(columns={"sku1": "sku"}, inplace=True)
        df_fast['站点1'] = df_fast['站点']
        df_fast = tou_cheng_dingjiabi(df_fast, conn, dingjia_type='快运')
    # 慢运
    if len(df_slow['sku']) > 0:
        df_slow.rename(columns={"sku": "sku1"}, inplace=True)
        # 20240131 慢运中，只有卡航有头程属性
        result = aut(df_slow, conn_mx, '卡航')
        df_slow = df_slow.merge(result, on=["sku1", "country_code"], how='left')
        df_slow['fba头程属性'].fillna('普货', inplace=True)
        df_slow.rename(columns={"sku1": "sku"}, inplace=True)
        df_slow['站点1'] = df_slow['站点']
        df_slow = tou_cheng_dingjiabi(df_slow, conn, dingjia_type='慢运')

    if len(df_ky['sku']) > 0:
        # 空运头程
        df_ky.rename(columns={"sku": "sku1"}, inplace=True)
        result = aut(df_ky, conn_mx)
        df_ky = df_ky.merge(result, on=["sku1", "country_code"], how='left')
        df_ky['fba头程属性'].fillna('普货', inplace=True)
        df_ky.rename(columns={"sku1": "sku"}, inplace=True)
        df_ky = tou_cheng_ky(df_ky, df_site, conn_mx)
    else:
        df_ky = pd.DataFrame()    # 海运头程
    if len(df_HY['sku']) > 0:
        df_HY = tou_cheng_hy(df_HY, df_site, conn_mx)
    # FBA 铁路头程
    if len(df_TL['sku']) > 0:
        df_TL = tou_cheng_tl(df_TL, df_site, conn_mx)
    # 卡航头程
    if len(df_kh['sku']) > 0:
        df_kh.rename(columns={"sku": "sku1"}, inplace=True)
        result = aut(df_kh, conn_mx, '卡航')
        df_kh = df_kh.merge(result, on=["sku1", "country_code"], how='left')
        df_kh['fba头程属性'].fillna('普货', inplace=True)
        df_kh.rename(columns={"sku1": "sku"}, inplace=True)
        df_kh = tou_cheng_kh(df_kh, df_site, conn_mx)
    res = df_ky.append([df_HY, df_TL, df_kh,df_fast, df_slow])

    if '调拨费' not in list(res.columns):
        res['调拨费'] = 0
    res['调拨费'] = res['调拨费'].fillna(0)
    # 计算FBAfee
    res = fba_ding_jia_biao(res)
    res_fba = res[(res['fba_fees'].isnull()) | (res['fba_fees'] == '')]
    res_fba['fba_fees'] = '不符合FBA物流条件'

    res = res[(res['fba_fees'].notnull()) & (res['fba_fees'] != '')]

    res = res.sort_values(['index'], ascending=True)
    res_wu = res[res['尺寸分段'] == '无法发货']
    res = res[res['尺寸分段'] != '无法发货']
    df_site3 = df_site[['站点', '汇率', '平台抽成比例', '冗余系数']]
    df_site4 = df_site3[df_site3['站点'] == '美国']
    for site in ['墨西哥-泛美', '加拿大-泛美']:
        df_site4['站点'] = site
        df_site3 = df_site3.append(df_site4)
    df_site3 = df_site3.drop_duplicates(['站点'], 'first')

    df3 = df_site3.drop_duplicates(['站点'], 'first')
    res = res.merge(df3, on=['站点'], how='left')
    # 增加使用真实佣金数据
    if worker_group in ['易佰', '楚晋', 'yxh']:
        res = get_amazon_referral_fee(res, conn_mx, worker_group)

    res[f"FBA{fbafee_type}定价"] = (res["成本"] + res["头程费_人民币"] + res["调拨费"] + 2 + res["fba_fees"] * res[
        "汇率"]) / (1 - res["平台抽成比例"] - res["冗余系数"] - res["FBA目标毛利润率"] - res["税率"]) / res["汇率"]
    res[f"FBA{fbafee_type}定价"] = res[f"FBA{fbafee_type}定价"].astype('float')
    res = res.append([df_bucunzai, df_bucunzai1, res_wu, res_fba])
    res = res.sort_values(['index'], ascending=True)
    res["FBA税率"] = res["税率"]
    res[f'头程费{fbafee_type}'] = res['头程费_人民币']
    res = res[['sku', '站点', 'FBA目标毛利润率', f'FBA{fbafee_type}定价', '成本', f'头程费{fbafee_type}', '调拨费',
               'fba_fees', 'FBA差值', 'FBA税率', '平台抽成比例', '冗余系数', '汇率']]
    res['date'] = datetime.datetime.today().strftime('%Y-%m-%d')
    res = res.drop_duplicates()

    return res

def bendiyunfei(df, conn_ck, table=None, ship_list=None):
    join_list = ['old_freight_interface_amazon', 'old_freight_interface_walmart',
                 'old_freight_interface_cd', 'old_freight_interface_allegro']
    if table in join_list:
        col = 'sku_join'
        df_other = df[df['数量_join'] != '1']
        df = df[df['数量_join'] == '1']
    else:
        col = 'sku'
        df_other = df[df['数量'] != 1]
        df = df[df['数量'] == 1]

    # 20230605 为加快行数较多文件的计算速度，删除不符合条件的数据
    df_copy = df[df.index >= 0]

    df_yunfei = pd.DataFrame(columns=['sku', 'ship_name', 'warehouse_id', '运费', '站点', 'ship_type'])
    for key, group in tqdm(df.groupby(['站点']), desc='获取运费'):
        # group = group[[col]].drop_duplicates()
        # group = group.reset_index(drop=True)
        # group['index'] = group.index
        # group['index'] = group['index'].apply(lambda m: int(m / 3000))
        # for key1, group1 in group.groupby(['index']):
        #     sku_list = mysql_escape(group1, col=col)
        sql = f"""
            SELECT distinct sku,ship_name,warehouse_id,total_cost as `运费`,site as `站点`,ship_type  
            from yibai_temp_hxx.{table} 
            where site='{key}' 
            order by total_cost asc limit 1 by sku
            """
        df_yunfei0 = conn_ck.ck_select_to_df(sql)
        df_yunfei = df_yunfei.append(df_yunfei0)
        del df_yunfei0
    df_yunfei['数量'] = 1
    # df_yunfei['ship_type'] = df_yunfei['ship_type'].astype(int)
    # df_yunfei['warehouse_id'] = df_yunfei['warehouse_id'].astype(int)
    #
    if table in join_list:
        df_yunfei.rename(columns={'sku': 'sku_join', '数量': '数量_join'}, inplace=True)
        df_yunfei['数量_join'] = '1'
        df = df.merge(df_yunfei, on=['sku_join', '数量_join', '站点'], how='left')
    else:
        df = df.merge(df_yunfei, on=['sku', '数量', '站点'], how='left')
    del df_yunfei

    # 仓库、物流方式选择
    df['发货仓库1'] = df['warehouse_id']

    df['物流类型1'] = df['ship_type']

    # 20230605 为加快行数较多文件的计算速度，删除不符合条件的数据
    df1 = df[['index']].drop_duplicates()
    df1['运费选择'] = '是'
    df_copy = df_copy.merge(df1, on=['index'], how='left')
    df_copy = df_copy[df_copy['运费选择'] != '是']
    df_copy.drop(['运费选择'], axis=1, inplace=True)
    df = df.append([df_copy, df_other])
    del df_copy, df_other

    df['运费选择'] = '否'
    df.loc[(df['发货仓库1'] == df['warehouse_id']) & (df['物流类型1'] == df['ship_type']), '运费选择'] = '是'
    if ship_list:
        df.loc[(df['账号类型'] == '虚拟仓账号') & (~df['ship_name'].isin(ship_list)), '运费选择'] = '否'
    return df


def FBM_jisuan(df, df_site, type=1, Organization=1, worker_group='易佰', distributor_id=None, business_type=None):
    conn_ck = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器')
    conn_mx = pd_to_ck(database='yibai_product_erp_sync', data_sys='调价明细历史数据')
    #
    df.rename(columns={'销售自定订单毛利润率': '毛利润率1', '0净利对应的订单毛利润率': '毛利润率2',
                       '平台目标净利对应的订单毛利润率': '毛利润率3'}, inplace=True)
    #
    df = df[df['sku'].notnull()]
    for col in ['税率', '售价', 'account_id', 'seller_sku', 'asin']:
        if col not in df.columns:
            df[col] = None
    df['account_id'] = df['account_id'].fillna(0)
    for col in ['seller_sku', 'asin']:
        df[col] = df[col].astype(str)
    for col in ["税率", "毛利润率1", '毛利润率2', '毛利润率3', '销售自定订单毛利润率2', '销售自定订单毛利润率3',
                '销售自定目标净利润率']:
        if col not in df.columns:
            df[col] = 0
        if df[col].dtypes == 'object':
            df[col] = df[col].str.strip()
            df.loc[df[col] == '', col] = 0
        df[col].fillna(0, inplace=True)
    df['售价'] = df['售价'].astype(float)
    # 模版修正
    df = mobanxiuzheng(df)
    #
    df.loc[df['站点'] == '英国', '税率'] = 0.1667
    df.loc[df['站点'] == '德国', '税率'] = 0.1597
    df.loc[df['站点'] == '法国', '税率'] = 0.1667
    df.loc[df['站点'] == '意大利', '税率'] = 0.1803
    df.loc[df['站点'] == '西班牙', '税率'] = 0.1736
    df.loc[df['站点'] == '荷兰', '税率'] = 0.1736
    df.loc[df['站点'] == '比利时', '税率'] = 0.1736
    df.loc[df['站点'] == '瑞典', '税率'] = 0.2
    df.loc[df['站点'] == '波兰', '税率'] = 0.187
    #
    df.loc[df['站点'] == '印度', '税率'] = 0.3
    df.loc[df['站点'] == '土耳其', '税率'] = 0.15
    df.loc[df['站点'] == '澳大利亚', '税率'] = 0.09
    #
    df['税率'] = df['税率'].fillna(0)
    df['税率'] = df['税率'].astype(float)
    sql = """
        SELECT site1 as `站点`,site3 as shipCountry,FBM_refund_rate as `fbm退款率` 
        FROM domestic_warehouse_clear.site_table WHERE platform='亚马逊'
        """
    df_amazon = conn_mx.ck_select_to_df(sql)
    df = df.merge(df_amazon, on=['站点'], how='left')
    # 20230419 为处理A+B捆绑形式，对模板做处理
    df = df[~df['sku'].str.contains('\*3-tshop')]
    df = sku_and_num_split(df)
    # 20230314 如果物流运费和前一天相比，站点维度行数减少10% ，此站点不计算
    if distributor_id:
        df_tichu_site = pd.DataFrame()
    else:
        sql = """
        select site from yibai_temp_hxx.freight_interface_count_site
        """
        df_count_site = conn_ck.ck_select_to_df(sql)
        if df_count_site.shape[0] == 0:
            df_count_site = pd.DataFrame(columns=['site'])
        df_tichu_site = df[df['站点'].isin(list(df_count_site['site']))]
        df_tichu_site['运费'] = '当前站点物流运费不全，无法计算'
        df = df[~df['站点'].isin(list(df_count_site['site']))]
    # 获取运费，优先使用已存库数据，没有的再调用接口（数据库中是所有仓库所有渠道的优选）
    df = bendiyunfei(df, conn_ck, 'old_freight_interface_amazon')
    # 计算分类：本地数据库运费、停售且库存为0、不存在sku, 其他（接口实时运费）
    df_bendi, df_tingshou, df_bucunzai, df = jisuanfenlei(df, conn_ck, conn_mx, 'amazon')
    df_bendi = df_bendi.sort_values(['运费'], ascending=True)
    df_bendi = df_bendi.drop_duplicates(['index'], 'first')
    # 20221020 数量大于1的可以调接口
    df_one = df[df['数量_join'] == '1']
    df = df[df['数量_join'] != '1']
    if len(df.index) > 0:
        df_yun = pd.DataFrame()
        for (shipCountry, warehouse, shipType), group in df.groupby(['shipCountry', '发货仓库', '物流类型']):
            group = group[['sku_join', '数量_join']].drop_duplicates()
            group.columns = ['sku', '数量']
            group = group.reset_index(drop=True)
            yunfei_jisuan = get_trip_fee('AMAZON', shipCountry, warehouse, shipType)
            df1 = yunfei_jisuan.batch_df_order(group)
            df1['shipCountry'] = shipCountry
            df1['发货仓库'] = warehouse
            df1['物流类型'] = shipType
            df_yun = df_yun.append(df1)
        df_yun = df_yun.sort_values(['运费'], ascending=True)
        df_yun = df_yun.drop_duplicates(['sku', '数量', 'shipCountry', '发货仓库', '物流类型'], 'first')
        df_yun.rename(columns={'sku': 'sku_join', '数量': '数量_join'}, inplace=True)
        df = df.merge(df_yun, on=['sku_join', '数量_join', 'shipCountry', '发货仓库', '物流类型'], how='left')
    if '运费' not in df.columns:
        df['运费'] = None
    df['运费'] = df['运费'].fillna('物流运费接口未返回运费')
    df_wuliu = df[df['运费'] == '物流运费接口未返回运费']
    df = df[df['运费'] != '物流运费接口未返回运费']
    # 本地和实时接口合并
    df = df.append([df_bendi, df_one])
    # 成本
    df = chaxun_chengben(df, Organization, conn_mx, 'amazon')
    df.drop_duplicates(['index'], inplace=True)
    # 站点配置
    df1 = df_site.drop_duplicates(['站点'], 'first')
    df = df.merge(df1, on=['站点'], how='left')
    # 20211022 毛利润率2设置为0净利， 毛利润率3设置为目标净利
    if distributor_id:
        df["毛利润率2"] = 0
        df["毛利润率3"] = 0
    else:
        df = tar_profit(df, conn_mx, f_type='FBM', worker_group=worker_group)
    df = yunfei_biangeng(df, Organization)
    if distributor_id:
        # 20230116 增加分销逻辑：根据distributor_id，调接口，成本=product_amount，运费=operate_amount（操作费）+domestic_amount（国内仓运费）
        df = DMS_getCalculation_api_fbm(df, df_amazon, distributor_id, business_type, conn_mx)
        df_ym = df[df['成本'].isnull()]
        df_ym['成本'] = 'ym接口未返回成本'
        df_ym['运费'] = 'ym接口未返回运费'
        df = df[df['成本'].notnull()]
    else:
        df_ym = pd.DataFrame()
    # 20230228 增加使用真实佣金数据
    if worker_group in ['易佰', '楚晋', 'yxh']:
        df = get_amazon_referral_fee(df, conn_mx, worker_group)
    df["FBM定价"] = (df["成本"] + df["运费"]) / (
            1 - df["平台抽成比例"] - df["冗余系数"] - df["FBM目标毛利润率"] - df["税率"]) / df["汇率"]

    df1 = df[df['运费'] == '无可用渠道']
    df2 = df[df['成本'].isnull()]

    df = df[(df['运费'] != '无可用渠道') & (df['成本'].notnull())]

    df = df.append([df1, df_tingshou, df_bucunzai, df2, df_ym, df_tichu_site, df_wuliu])
    df = df.sort_values(['index'], ascending=True)
    # df.drop(['平台抽成比例', '冗余系数', 'index', 'shipCountry', 'ship_type', '成本'], axis=1, inplace=True)

    df['FBM税率'] = df['税率']
    res = df[['sku', '站点', 'FBM目标毛利润率', 'FBM定价', '运费', 'FBM差值', 'FBM税率']]
    res.dropna(inplace=True)
    res['date'] = datetime.datetime.today().strftime('%Y-%m-%d')
    res = res.drop_duplicates()
    res['FBM定价'] = res['FBM定价'].astype('float')

    return res


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


def add_kunbang(df):
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
    df2['site'] = df2['站点']
    df2['站点FBA'] = df2['站点']
    df2['站点FBM'] = df2['站点']
    df2['cost'] = 1000000
    df2.drop('站点', axis=1, inplace=True)
    df = pd.concat([df, df2], ignore_index=True)
    print('添加捆绑sku完成！')
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
        select site1 as `站点`,site3 as country_code FROM domestic_warehouse_clear.site_table
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

# 20240515 定价比计算程序优化
def pricing_ratio_new():
    """
    定价比计算程序
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.date.today().isoformat().replace('-', '')
    # 表准备
    # table_name = f'pricing_ratio_test'
    table_name = f'pricing_ratio_{date_today}'
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

    # 需要计算的站点
    sql = """
        SELECT 
            site, site1
        from domestic_warehouse_clear.site_table bb
        where bb.site not in ('be', 'fr', 'it', 'sp', 'nl', 'pl', 'se', 'tr','br','in')
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
    df_site_df = amazon_fba_para(df_site_df, [], pd.DataFrame())
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
        ##
        dic = {'头程费_海运': '头程费海运', '头程费_空运': '头程费空运', '头程费_铁路': '头程费铁路','头程费_卡航': '头程费卡航',
               '头程费_快运': '头程费快运', '头程费_慢运': '头程费慢运'}
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

        ##
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
        ## 异常数据存表
        # 表准备
        # table_name = f'pricing_ratio_test_{date_today}'
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df_price_null = df_res[(df_res['FBA空运定价'].isna()) | (df_res['运费'].isna()) | (df_res['fba_fees'] == 0)]
        sql = f"""
            ALTER TABLE yibai_oversea.pricing_ratio_null_data
            DELETE where date = \'{datetime.datetime.today().strftime('%Y-%m-%d')}\'
        """
        conn_ck.ck_execute_sql(sql)
        conn_ck.ck_insert(df_price_null, 'pricing_ratio_null_data', if_exist='append')
        ## 最终结果存表
        df_res = df_res[(~df_res['FBA空运定价'].isna()) & (~df_res['运费'].isna()) & (df_res['fba_fees'] != 0)]
        conn_mx2.ck_insert(df_res, table_name, if_exist='append')
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


# 20240130 该函数暂未使用
def pricing_ratio_monitor():
    """
    使用各字段近期平均水平来计算修正数值。如果一两天突然公式中部分因子有大幅度变化，直接计算的定价比会变化很大，但是修正的定价比就不会。
    如果修正后的定价比持续一周都和以往的相差较大，说明各因子已经有了稳定的变化，发运模式就可以建议转变。
    FBA定价的参考值，是修正后的定价比第一次开始适合空运或者海运时候的FBA定价数值，记录下历史中的该值用于一周时和当前修正后的FBA定价
    对比，来判断定价比变化是否主要来源于FBA定价的波动。
    """
    conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_today = datetime.datetime.today()

    # 定价比表准备
    table_now = f'pricing_ratio_{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    sql_table = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_%'
        and table < \'{table_now}\'
        order by table desc
        limit 10
    """
    df_table = conn_mx2.ck_select_to_df(sql_table)
    # 修正的定价比计算
    # 数据量过大，分站点取平均
    def get_avg(df_table, table_now, site='德国'):
        if site == '德国':
            sql = f"""
                select * except (id, date, update_time)        
                from support_document.{table_now}
                where `站点` = '德国'
            """
        else:
            sql = f"""
                select * except (id, date, update_time)        
                from support_document.{table_now}
                where `站点` != '德国'
            """
        df = conn_mx2.ck_select_to_df(sql)
        print('当日定价比表计算完成！')
        # 计算近几日影响因子平均水平，用于后续修正影响因子和定价比
        list_factors = ['成本', '头程费空运', '头程费海运', '头程费铁路', '头程费卡航', '调拨费', 'fba_fees', '运费', 'FBA差值',
                    'FBM差值', 'FBA税率', 'FBM税率', '汇率']
        df_avg = pd.DataFrame()
        if df_table.empty:
            df_avg = df[['sku', '站点'] + list_factors]
        else:
            for i in range(len(df_table)):
                table_temp = df_table.iloc[-(i + 1)]['table']
                if site == '德国':
                    sql_temp = f"""
                        select * except (id, date, update_time,`FBA目标毛利润率`,`FBA空运定价`,`FBA海运定价`,`FBA铁路定价`,`FBA卡航定价`,`FBM目标毛利润率`,`FBM定价`,
                        `FBA空运定价/FBM定价`,`FBA海运定价/FBM定价`,`FBA铁路定价/FBM定价`,`FBA卡航定价/FBM定价`,`空海利润率反算`,`空海同价利率差`,`空铁利润率反算`,`空铁同价利率差`)        
                        from support_document.{table_temp}
                        where `站点` = '德国'
                    """
                else:
                    sql_temp = f"""
                        select * except (id, date, update_time,`FBA目标毛利润率`,`FBA空运定价`,`FBA海运定价`,`FBA铁路定价`,`FBA卡航定价`,`FBM目标毛利润率`,`FBM定价`,
                        `FBA空运定价/FBM定价`,`FBA海运定价/FBM定价`,`FBA铁路定价/FBM定价`,`FBA卡航定价/FBM定价`,`空海利润率反算`,`空海同价利率差`,`空铁利润率反算`,`空铁同价利率差`)        
                        from support_document.{table_temp}
                        where `站点` != '德国'
                    """
                df_temp = conn_mx2.ck_select_to_df(sql_temp)
                if '头程费卡航' not in df_temp.columns.to_list():
                    df_temp['头程费卡航'] = np.nan
                df_temp = df_temp[['sku', '站点'] + list_factors]
                for i in list_factors:
                    df_temp[i] = np.where(df_temp[i] < 0, np.nan, df_temp[i])
                    df_temp[f'{i}_num'] = np.where(df_temp[i].isna(), 0, 1)
                df_avg = pd.concat([df_temp, df_avg])
                df_avg = df_avg.groupby(['sku', '站点']).sum().reset_index()
            for i in list_factors:
                df_avg[i] = df_avg[i] / df_avg[f'{i}_num']
                df_avg.drop(f'{i}_num', axis=1, inplace=True)
        print('影响因子平均值计算完成！')
        # 基于当日可计算的 erp sku + 站点 维度，结合历史数据进行修正（left join）
        df = df.merge(df_avg, on=['sku', '站点'], how='left', suffixes=("_now", "_avg"))
        del df_avg
        volatility_limit = 0.2
        for k in list_factors:
            df.loc[pd.isna(df[f'{k}_avg']), f'{k}_avg'] = df.loc[pd.isna(df[f'{k}_avg']), f'{k}_now']
            df[f'{k}_now'] = 0.5 * (df[f'{k}_avg'] + df[f'{k}_now']) * (
                    (df[f'{k}_now'] > (1 + volatility_limit) * df[f'{k}_avg']) |
                    (df[f'{k}_now'] < (1 - volatility_limit) * df[f'{k}_avg'])).astype('int') \
                             + df[f'{k}_now'] * ((df[f'{k}_now'] <= (1 + volatility_limit) * df[f'{k}_avg']) &
                                                 (df[f'{k}_now'] >= (1 - volatility_limit) * df[f'{k}_avg'])).astype('int')
            df.drop(f'{k}_avg', axis=1, inplace=True)
            print(f'{k} avg 完成！')
        df.columns = df.columns.str.replace('_now', '')
        return df
    df1 = get_avg(df_table, table_now, site='德国')
    df2 = get_avg(df_table, table_now, site='非德国')
    df = pd.concat([df1, df2])
    df['FBA站点'] = df['站点']
    df['FBM站点'] = df['站点'].str.replace('-泛美', '')
    # 平台抽成比例
    sql_platform_fba = """
        select site `FBA站点`, platform_percentage `FBA平台抽成比例` from domestic_warehouse_clear.price_calculate_parameters
        where mode = 'AMAZON-FBA'
    """
    df_platform_fba = conn_mx2.ck_select_to_df(sql_platform_fba)
    df = df.merge(df_platform_fba, on='FBA站点', how='left')
    sql_platform_fbm = """
        select site `FBM站点`, platform_percentage `FBM平台抽成比例` from domestic_warehouse_clear.price_calculate_parameters
        where mode = 'AMAZON-FBM'
    """
    df_platform_fbm = conn_mx2.ck_select_to_df(sql_platform_fbm)
    df = df.merge(df_platform_fbm, on='FBM站点', how='left')
    # 修正的定价
    df['FBA空运定价_修正'] = (df["成本"] + df["头程费空运"] + df['调拨费'] + 2 + df["fba_fees"] * df[
        "汇率"]) / (1 - df["FBA平台抽成比例"] - 0.03 - df["FBA目标毛利润率"] - df["FBA税率"]) / df["汇率"]
    df["FBM定价_修正"] = (df["成本"] + df["运费"]) / (1 - df["FBM平台抽成比例"] - 0.03 - df[
        "FBM目标毛利润率"] - df["FBM税率"]) / df["汇率"]
    df['FBA空运定价/FBM定价_修正'] = df['FBA空运定价_修正'] / df['FBM定价_修正']

    df['头程费海运'] = df['头程费海运'].replace(-999, np.nan)
    df['FBA海运定价_修正'] = (df["成本"] + df["头程费海运"] + df['调拨费'] + 2 + df["fba_fees"] * df["汇率"]) / (
            1 - df["FBA平台抽成比例"] - 0.03 - df["FBA目标毛利润率"] - df["FBA税率"]) / df["汇率"]
    df['FBA海运定价/FBM定价_修正'] = df['FBA海运定价_修正'] / df['FBM定价_修正']

    df['头程费铁路'] = df['头程费铁路'].replace(-999, np.nan)
    df['FBA铁路定价_修正'] = (df["成本"] + df["头程费铁路"] + df['调拨费'] + 2 + df["fba_fees"] * df["汇率"]) / (
            1 - df["FBA平台抽成比例"] - 0.03 - df["FBA目标毛利润率"] - df["FBA税率"]) / df["汇率"]
    df['FBA铁路定价/FBM定价_修正'] = df['FBA铁路定价_修正'] / df['FBM定价_修正']

    df['头程费卡航'] = df['头程费卡航'].replace(-999, np.nan)
    df['FBA卡航定价_修正'] = (df["成本"] + df["头程费卡航"] + df['调拨费'] + 2 + df["fba_fees"] * df["汇率"]) / (
            1 - df["FBA平台抽成比例"] - 0.03 - df["FBA目标毛利润率"] - df["FBA税率"]) / df["汇率"]
    df['FBA卡航定价/FBM定价_修正'] = df['FBA卡航定价_修正'] / df['FBM定价_修正']

    df['空海利润率反算_修正'] = 1 - df['FBA平台抽成比例'] - 0.03 - df['FBA税率'] - \
                                 (df['成本'] + df['头程费海运'] + df['调拨费'] + 2 + df[
                                     'fba_fees'] * df['汇率']) / (df['FBA空运定价_修正'] * df['汇率'])
    df['空海同价利率差_修正'] = df['空海利润率反算_修正'] - df['FBA目标毛利润率']
    df['空铁利润率反算_修正'] = 1 - df['FBA平台抽成比例'] - 0.03 - df['FBA税率'] - \
                                 (df['成本'] + df['头程费铁路'] + df['调拨费'] + 2 + df[
                                     'fba_fees'] * df['汇率']) / (df['FBA空运定价_修正'] * df['汇率'])
    df['空铁同价利率差_修正'] = df['空铁利润率反算_修正'] - df['FBA目标毛利润率']

    df_res = df[['sku', '站点',
                 'FBA空运定价/FBM定价', 'FBA空运定价/FBM定价_修正',
                 'FBA海运定价/FBM定价', 'FBA海运定价/FBM定价_修正',
                 'FBA铁路定价/FBM定价', 'FBA铁路定价/FBM定价_修正',
                 'FBA卡航定价/FBM定价', 'FBA卡航定价/FBM定价_修正',
                 'FBA空运定价_修正', 'FBA海运定价_修正', 'FBA铁路定价_修正', 'FBA卡航定价_修正',
                 '空海同价利率差_修正','空铁同价利率差_修正']]
    del df

    print('修正的定价计算完成！')

    # 分析
    table_monitor_now = f'pricing_ratio_monitor_{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    df_res['limit'] = np.where(df_res['站点'].isin(['日本','阿联酋','新加坡','沙特']), 2, 1.5)
    df_res['适合空运连续天数'] = (df_res['FBA空运定价/FBM定价_修正'] <= df_res['limit']).astype('int')
    df_res['适合海运连续天数'] = (df_res['FBA海运定价/FBM定价_修正'] <= df_res['limit']).fillna(0).astype('int')
    df_res['适合铁路连续天数'] = (df_res['FBA铁路定价/FBM定价_修正'] <= df_res['limit']).fillna(0).astype('int')
    df_res['适合卡航连续天数'] = (df_res['FBA卡航定价/FBM定价_修正'] <= df_res['limit']).fillna(0).astype('int')
    df_res.drop('limit', axis=1, inplace=True)
    # 此处可输出倒挂的数据

    sql_table_monitor = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_monitor%'
        and table < \'{table_monitor_now}\'
        order by table desc
        limit 7
    """
    df_table_monitor = conn_mx2.ck_select_to_df(sql_table_monitor)
    print('获取历史监控表...')
    if len(df_table_monitor) > 0:
        # 最近历史监控数据 - 实现某天某条无法计算定价比时仍保留适合的连续天数，而不会因为缺失而直接中断
        df_latest = pd.DataFrame()
        for index, row in df_table_monitor.iterrows():
            sql_monitor_latest = f"""
                     select *
                     from support_document.{row['table']}
                 """
            df_latest_temp = conn_mx2.ck_select_to_df(sql_monitor_latest)
            col_list = ['适合卡航连续天数', 'FBA卡航定价_修正', 'FBA卡航定价_参考']
            for i in col_list:
                if i not in df_latest_temp.columns.to_list():
                    df_latest_temp[i] = 0
            col_list = ['sku', '站点', 'date', '适合空运连续天数', '适合海运连续天数', '适合铁路连续天数',
                        '适合卡航连续天数', 'FBA空运定价_修正', 'FBA海运定价_修正',
                        'FBA铁路定价_修正', 'FBA卡航定价_修正', 'FBA空运定价_参考', 'FBA海运定价_参考',
                        'FBA铁路定价_参考', 'FBA卡航定价_参考']
            df_latest_temp = df_latest_temp[col_list]
            df_latest_temp.columns = ['sku', '站点', 'date', '适合空运连续天数_old', '适合海运连续天数_old',
                                      '适合铁路连续天数_old', '适合卡航连续天数_old',
                                      'FBA空运定价_修正_old', 'FBA海运定价_修正_old', 'FBA铁路定价_修正_old',
                                      'FBA卡航定价_修正_old', 'FBA空运定价_参考',
                                      'FBA海运定价_参考', 'FBA铁路定价_参考', 'FBA卡航定价_参考']
            df_latest = df_latest_temp if df_latest.empty else pd.concat([df_latest, df_latest_temp], ignore_index=True)
            df_latest = df_latest.sort_values(['sku', '站点', 'date'], ascending=False)
            df_latest = df_latest.groupby(['sku', '站点']).head(1)
        del df_latest_temp
    else:
        pass

    # 监控表准备
    table_name = f'pricing_ratio_monitor_{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    print('监控结果表删除完成!')
    conn_mx2.ck_execute_sql(sql)
    sql = f"""
                        CREATE TABLE IF NOT EXISTS support_document.{table_name}
                        (
                        `id` Int64,
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
                        `FBA空运定价_修正` Float64 COMMENT 'FBA空运定价_修正',
                        `FBA海运定价_修正` Float64 COMMENT 'FBA海运定价_修正',
                        `FBA铁路定价_修正` Float64 COMMENT 'FBA铁路定价_修正',
                        `FBA卡航定价_修正` Float64 COMMENT 'FBA卡航定价_修正',
                        `适合空运连续天数` Int32 COMMENT '适合空运连续天数',
                        `适合海运连续天数` Int32 COMMENT '适合海运连续天数',
                        `适合铁路连续天数` Int32 COMMENT '适合铁路连续天数',
                        `适合卡航连续天数` Int32 COMMENT '适合卡航连续天数',
                        `FBA空运定价_参考` Float64 COMMENT 'FBA空运定价_参考',
                        `FBA海运定价_参考` Float64 COMMENT 'FBA海运定价_参考',
                        `FBA铁路定价_参考` Float64 COMMENT 'FBA铁路定价_参考',
                        `FBA卡航定价_参考` Float64 COMMENT 'FBA卡航定价_参考',
                        `FBA空运定价波幅` Float64 COMMENT 'FBA空运定价波幅',
                        `FBA海运定价波幅` Float64 COMMENT 'FBA海运定价波幅',
                        `FBA铁路定价波幅` Float64 COMMENT 'FBA铁路定价波幅',
                        `FBA卡航定价波幅` Float64 COMMENT 'FBA卡航定价波幅',
                        `空海同价利率差_修正` Float64,
                        `空铁同价利率差_修正` Float64,
                        `发运建议` String COMMENT '发运建议',
                        `建议物流方式` String COMMENT '建议物流方式：1：海运整柜；2：海运散货；3：铁路整柜；4：铁路散货；5：陆运/卡航；6：空运；9：快递；',
                        `update_time` String COMMENT '更新时间'
                        )
                        ENGINE = MergeTree
                        ORDER BY (sku, `站点`)
                        SETTINGS index_granularity = 8192
                        """
    conn_mx2.ck_create_table(sql)
    print('监控结果表创建完成!')

    # 数据过大，内存会爆，循环处理
    df_res = df_res.reset_index()
    df_res['index'] = df_res['index'].apply(lambda m: int(m / 500000))
    process_length = df_res['index'].max() + 1
    t = 1
    for item, df_res_group in df_res.groupby(['index']):
        df_res_group.drop(['index'], axis=1, inplace=True)
        # FBA空运定价_参考 是历史的参考数据，用于计算波幅
        if df_table_monitor.empty:
            df_res_group['FBA空运定价_参考'] = df_res_group['FBA空运定价_修正']
            df_res_group['FBA海运定价_参考'] = df_res_group['FBA海运定价_修正']
            df_res_group['FBA铁路定价_参考'] = df_res_group['FBA铁路定价_修正']
            df_res_group['FBA卡航定价_参考'] = df_res_group['FBA卡航定价_修正']
        else:
            df_res_group = df_res_group.merge(df_latest, on=['sku', '站点'], how='left')
            df_res_group['适合空运连续天数_old'] = df_res_group['适合空运连续天数_old'].fillna(0)
            df_res_group['适合海运连续天数_old'] = df_res_group['适合海运连续天数_old'].fillna(0)
            df_res_group['适合铁路连续天数_old'] = df_res_group['适合铁路连续天数_old'].fillna(0)
            df_res_group['适合卡航连续天数_old'] = df_res_group['适合卡航连续天数_old'].fillna(0)
            df_res_group['适合空运连续天数'] = (
                    df_res_group['适合空运连续天数'] * (df_res_group['适合空运连续天数_old'] + 1)).astype('int')
            df_res_group['适合海运连续天数'] = (
                    df_res_group['适合海运连续天数'] * (df_res_group['适合海运连续天数_old'] + 1)).astype('int')
            df_res_group['适合铁路连续天数'] = (
                    df_res_group['适合铁路连续天数'] * (df_res_group['适合铁路连续天数_old'] + 1)).astype('int')
            df_res_group['适合卡航连续天数'] = (
                    df_res_group['适合卡航连续天数'] * (df_res_group['适合卡航连续天数_old'] + 1)).astype('int')
            # 其实两种情况源头上都是修正的数值，简单把参考理解成历史上次数值就可以
            df_res_group['FBA空运定价_修正_old'] = pd.isna(df_res_group['FBA空运定价_修正_old']).astype('int') * \
                                                   df_res_group['FBA空运定价_修正'] \
                                                   + df_res_group['FBA空运定价_修正_old'].fillna(0)
            df_res_group['FBA海运定价_修正_old'] = pd.isna(df_res_group['FBA海运定价_修正_old']).astype('int') * \
                                                   df_res_group['FBA海运定价_修正'] \
                                                   + df_res_group['FBA海运定价_修正_old'].fillna(0)
            df_res_group['FBA铁路定价_修正_old'] = pd.isna(df_res_group['FBA铁路定价_修正_old']).astype('int') * \
                                                   df_res_group['FBA铁路定价_修正'] \
                                                   + df_res_group['FBA铁路定价_修正_old'].fillna(0)
            df_res_group['FBA卡航定价_修正_old'] = pd.isna(df_res_group['FBA卡航定价_修正_old']).astype('int') * \
                                                   df_res_group['FBA卡航定价_修正'] \
                                                   + df_res_group['FBA卡航定价_修正_old'].fillna(0)
            df_res_group['FBA空运定价_参考'] = pd.isna(df_res_group['FBA空运定价_参考']).astype('int') * df_res_group[
                'FBA空运定价_修正'] + df_res_group['FBA空运定价_参考'].fillna(0)
            df_res_group['FBA海运定价_参考'] = pd.isna(df_res_group['FBA海运定价_参考']).astype('int') * df_res_group[
                'FBA海运定价_修正'] + df_res_group['FBA海运定价_参考'].fillna(0)
            df_res_group['FBA铁路定价_参考'] = pd.isna(df_res_group['FBA铁路定价_参考']).astype('int') * df_res_group[
                'FBA铁路定价_修正'] + df_res_group['FBA铁路定价_参考'].fillna(0)
            df_res_group['FBA卡航定价_参考'] = pd.isna(df_res_group['FBA卡航定价_参考']).astype('int') * df_res_group[
                'FBA卡航定价_修正'] + df_res_group['FBA卡航定价_参考'].fillna(0)
            df_res_group['FBA空运定价_参考'] = df_res_group['FBA空运定价_修正_old'] * (
                    df_res_group['适合空运连续天数'] == 0).astype('int') \
                                               + df_res_group['FBA空运定价_参考'] * (
                                                       df_res_group['适合空运连续天数'] != 0).astype('int')
            df_res_group['FBA海运定价_参考'] = df_res_group['FBA海运定价_修正_old'] * (
                    df_res_group['适合海运连续天数'] == 0).astype('int') \
                                               + df_res_group['FBA海运定价_参考'] * (
                                                       df_res_group['适合海运连续天数'] != 0).astype('int')
            df_res_group['FBA铁路定价_参考'] = df_res_group['FBA铁路定价_修正_old'] * (
                    df_res_group['适合铁路连续天数'] == 0).astype('int') \
                                               + df_res_group['FBA铁路定价_参考'] * (
                                                       df_res_group['适合铁路连续天数'] != 0).astype('int')
            df_res_group['FBA卡航定价_参考'] = df_res_group['FBA卡航定价_修正_old'] * (
                    df_res_group['适合卡航连续天数'] == 0).astype('int') \
                                               + df_res_group['FBA卡航定价_参考'] * (
                                                       df_res_group['适合卡航连续天数'] != 0).astype('int')

        df_res_group['FBA空运定价波幅'] = ((df_res_group['FBA空运定价_修正'] - df_res_group['FBA空运定价_参考']).abs() /
                                           df_res_group['FBA空运定价_参考']).replace(np.inf, 0).replace(np.nan, 0)
        df_res_group['FBA海运定价波幅'] = ((df_res_group['FBA海运定价_修正'] - df_res_group['FBA海运定价_参考']).abs() /
                                           df_res_group['FBA海运定价_参考']).replace(np.inf, 0).replace(np.nan, 0)
        df_res_group['FBA铁路定价波幅'] = ((df_res_group['FBA铁路定价_修正'] - df_res_group['FBA铁路定价_参考']).abs() /
                                           df_res_group['FBA铁路定价_参考']).replace(np.inf, 0).replace(np.nan, 0)
        df_res_group['FBA卡航定价波幅'] = ((df_res_group['FBA卡航定价_修正'] - df_res_group['FBA卡航定价_参考']).abs() /
                                           df_res_group['FBA卡航定价_参考']).replace(np.inf, 0).replace(np.nan, 0)

        df_res_group.loc[
            (df_res_group['适合空运连续天数'] >= 7) & (df_res_group['FBA空运定价波幅'] >= 0.1), '发运建议'] = \
            '空运定价比修正后数值连续一周低于2且FBA空运定价为主要原因，建议可发空运'
        df_res_group.loc[
            (df_res_group['适合海运连续天数'] >= 7) & (df_res_group['FBA海运定价波幅'] >= 0.1), '发运建议'] = \
            '海运定价比修正后数值连续一周低于2且FBA海运定价为主要原因，建议可发海运'
        df_res_group.loc[
            (df_res_group['适合铁路连续天数'] >= 7) & (df_res_group['FBA铁路定价波幅'] >= 0.1), '发运建议'] = \
            '铁路定价比修正后数值连续一周低于2且FBA铁路定价为主要原因，建议可发铁路'
        df_res_group.loc[
            (df_res_group['适合卡航连续天数'] >= 7) & (df_res_group['FBA卡航定价波幅'] >= 0.1), '发运建议'] = \
            '卡航定价比修正后数值连续一周低于2且FBA卡航定价为主要原因，建议可发卡航'
        df_res_group['发运建议'] = df_res_group['发运建议'].fillna('无建议')

        # 需先确保判断条件没有空值，否则输出也是空值
        df_res_group['适合海运连续天数'] = df_res_group['适合海运连续天数'].fillna(0).astype('int')
        df_res_group['适合铁路连续天数'] = df_res_group['适合铁路连续天数'].fillna(0).astype('int')
        df_res_group['适合卡航连续天数'] = df_res_group['适合卡航连续天数'].fillna(0).astype('int')
        df_res_group['FBA空运定价/FBM定价_修正'] = df_res_group['FBA空运定价/FBM定价_修正'].fillna(999)
        df_res_group['FBA卡航定价/FBM定价_修正'] = df_res_group['FBA卡航定价/FBM定价_修正'].fillna(999)
        df_res_group['FBA铁路定价/FBM定价_修正'] = df_res_group['FBA铁路定价/FBM定价_修正'].fillna(999)
        df_res_group['FBA海运定价/FBM定价_修正'] = df_res_group['FBA海运定价/FBM定价_修正'].fillna(999)
        df_res_group['空海同价利率差_修正'] = df_res_group['空海同价利率差_修正'].fillna(999)
        df_res_group['空铁同价利率差_修正'] = df_res_group['空铁同价利率差_修正'].fillna(999)

        df_res_group.loc[
            (df_res_group['FBA空运定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['FBA铁路定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['空铁同价利率差_修正'] <= 0.01) & (df_res_group['空铁同价利率差_修正'] > 0) &
            (df_res_group['站点'].isin(['德国'])), '建议物流方式'] = '6;9'
        df_res_group.loc[
            (df_res_group['FBA铁路定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['空铁同价利率差_修正'] <= 0.1) & (df_res_group['空铁同价利率差_修正'] > 0.01) &
            (df_res_group['适合铁路连续天数'] >= 7) &
            (df_res_group['站点'].isin(['德国'])), '建议物流方式'] = '4;6;9'
        df_res_group.loc[
            (df_res_group['FBA铁路定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['空铁同价利率差_修正'] > 0.1) &
            (df_res_group['适合铁路连续天数'] >= 7) &
            (df_res_group['站点'].isin(['德国'])), '建议物流方式'] = '4'
        df_res_group.loc[
            ((df_res_group['FBA铁路定价/FBM定价_修正'] > 1.5) | (
                    (df_res_group['适合铁路连续天数'] < 7) & (df_res_group['FBA铁路定价/FBM定价_修正'] <= 1.5) &
                    ((df_res_group['空铁同价利率差_修正'] > 0.01)))) &
            (df_res_group['站点'].isin(['德国'])), '建议物流方式'] = '0'

        df_res_group.loc[
            (df_res_group['FBA空运定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['FBA海运定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['空海同价利率差_修正'] <= 0.01) & (df_res_group['空海同价利率差_修正'] > 0) &
            (~df_res_group['站点'].isin(['德国', '日本','新加坡','阿联酋','沙特'])), '建议物流方式'] = '6;9'
        df_res_group.loc[
            (df_res_group['FBA海运定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['空海同价利率差_修正'] <= 0.1) & (df_res_group['空海同价利率差_修正'] > 0.01) &
            (df_res_group['适合海运连续天数'] >= 7) &
            (~df_res_group['站点'].isin(['德国', '日本','新加坡','阿联酋','沙特'])), '建议物流方式'] = '2;6;9'
        df_res_group.loc[
            (df_res_group['FBA海运定价/FBM定价_修正'] <= 1.5) &
            (df_res_group['空海同价利率差_修正'] > 0.1) &
            (df_res_group['适合海运连续天数'] >= 7) &
            (~df_res_group['站点'].isin(['德国', '日本','新加坡','阿联酋','沙特'])), '建议物流方式'] = '2'
        df_res_group.loc[
            ((df_res_group['FBA海运定价/FBM定价_修正'] > 1.5) | (
                    (df_res_group['适合海运连续天数'] < 7) & (df_res_group['FBA海运定价/FBM定价_修正'] <= 1.5) &
                    ((df_res_group['空海同价利率差_修正'] > 0.01)))) &
            (~df_res_group['站点'].isin(['德国', '日本','新加坡','阿联酋','沙特'])), '建议物流方式'] = '0'

        df_res_group.loc[
            (df_res_group['FBA空运定价/FBM定价_修正'] <= 2) &
            (df_res_group['站点'].isin(['日本','新加坡','沙特'])), '建议物流方式'] = '1;2;6;9'
        df_res_group.loc[
            (df_res_group['FBA空运定价/FBM定价_修正'] > 2) &
            (df_res_group['站点'].isin(['日本','新加坡','沙特'])), '建议物流方式'] = '0'

        df_res_group.loc[
            (df_res_group['FBA空运定价/FBM定价_修正'] <= 2) &
            (df_res_group['FBA海运定价/FBM定价_修正'] <= 2) &
            (df_res_group['空海同价利率差_修正'] <= 0.01) & (df_res_group['空海同价利率差_修正'] > 0) &
            (df_res_group['站点'].isin(['阿联酋'])), '建议物流方式'] = '6;9'
        df_res_group.loc[
            (df_res_group['FBA海运定价/FBM定价_修正'] <= 2) &
            (df_res_group['空海同价利率差_修正'] <= 0.1) & (df_res_group['空海同价利率差_修正'] > 0.01) &
            (df_res_group['适合海运连续天数'] >= 7) &
            (df_res_group['站点'].isin(['阿联酋'])), '建议物流方式'] = '2;6;9'
        df_res_group.loc[
            (df_res_group['FBA海运定价/FBM定价_修正'] <= 2) &
            (df_res_group['空海同价利率差_修正'] > 0.1) &
            (df_res_group['适合海运连续天数'] >= 7) &
            (df_res_group['站点'].isin(['阿联酋'])), '建议物流方式'] = '2'
        df_res_group.loc[
            ((df_res_group['FBA海运定价/FBM定价_修正'] > 2) | (
                    (df_res_group['适合海运连续天数'] < 7) & (df_res_group['FBA海运定价/FBM定价_修正'] <= 2) &
                    ((df_res_group['空海同价利率差_修正'] > 0.01)))) &
            (df_res_group['站点'].isin(['阿联酋'])), '建议物流方式'] = '0'

        df_res_group['date'] = date_today.strftime('%Y-%m-%d')
        df_res_group = df_res_group[
            ['date', 'sku', '站点',
             'FBA空运定价/FBM定价', 'FBA空运定价/FBM定价_修正',
             'FBA海运定价/FBM定价', 'FBA海运定价/FBM定价_修正',
             'FBA铁路定价/FBM定价', 'FBA铁路定价/FBM定价_修正',
             'FBA卡航定价/FBM定价', 'FBA卡航定价/FBM定价_修正',
             'FBA空运定价_修正', 'FBA海运定价_修正', 'FBA铁路定价_修正', 'FBA卡航定价_修正',
             '适合空运连续天数', '适合海运连续天数', '适合铁路连续天数', '适合卡航连续天数',
             'FBA空运定价_参考', 'FBA海运定价_参考', 'FBA铁路定价_参考', 'FBA卡航定价_参考',
             'FBA空运定价波幅', 'FBA海运定价波幅', 'FBA铁路定价波幅', 'FBA卡航定价波幅',
             '空海同价利率差_修正','空铁同价利率差_修正','发运建议', '建议物流方式']]
        df_res_group['适合海运连续天数'] = df_res_group['适合海运连续天数'].fillna(0).astype('int')
        df_res_group['适合铁路连续天数'] = df_res_group['适合铁路连续天数'].fillna(0).astype('int')
        df_res_group['适合卡航连续天数'] = df_res_group['适合卡航连续天数'].fillna(0).astype('int')

        # 写入ck
        conn_mx2.ck_insert(df_res_group, table_name, if_exist='append')
        print(f'process: {t}/{process_length}')
        t += 1

    print('定价比波幅监控完成！')


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
    # DELETE FROM over_sea.{table_name} WHERE update_time < '{date_id}'
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
        from domestic_warehouse_clear.site_table bb
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
    df_site_df = amazon_fba_para(df_site_df, [], pd.DataFrame())
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
                    `适合快运连续天数` >= 7 and `FBA快运定价波幅`>= 0.1, '快运定价比修正后数值连续一周低于2且FBA快运定价为主要原因，建议可发快运',
                    `适合慢运连续天数` >= 7 and `FBA慢运定价波幅`>= 0.1, '慢运定价比修正后数值连续一周低于2且FBA慢运定价为主要原因，建议可发慢运',
                '无建议') as `发运建议`,
                multiIf(
                    (`FBA慢运定价/FBM定价_修正`>2 or (`适合慢运连续天数`<7 and `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`> 0.01)) and a.`站点` in ('阿联酋'), '0', 
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('阿联酋'), '12',
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`>0.01 and `适合慢运连续天数`>=7 and a.`站点` in ('阿联酋'), '12;6',
                    `FBA快运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.01 and a.`站点` in ('阿联酋'), '6',
                    `FBA快运定价/FBM定价_修正`>2 and a.`站点` in ('日本','新加坡','沙特'), '0',
                    `FBA快运定价/FBM定价_修正`<=2 and a.`站点` in ('日本','新加坡','沙特'), '6',
                    (`FBA慢运定价/FBM定价_修正`> 1.5 or (`适合慢运连续天数`< 7 and `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.01)) and a.`站点` not in ('日本','新加坡','阿联酋','沙特'), '0',         
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('美国'), '11',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '12',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('德国','英国'), '16',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.01 and `适合慢运连续天数`>= 7 and a.`站点` in ('美国'), '11;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.01 and `适合慢运连续天数`>= 7 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '12;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.01 and `适合慢运连续天数`>= 7 and a.`站点` in ('德国'), '16;15',    
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.01 and a.`站点` in ('美国','加拿大', '墨西哥', '澳大利亚'), '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.01 and a.`站点` in ('德国','英国'), '15',
                    '-1') as `建议物流方式`
            from (
                select sku,`站点`,`FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
                   `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
                   `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
                   `FBA空运定价_修正`, `FBA海运定价_修正`, `FBA铁路定价_修正`, `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`,
                   `空海同价利率差_修正`, `空铁同价利率差_修正`,`空卡同价利率差_修正`,`快慢同价利率差_修正`,
                   if(`站点` in ('阿联酋','新加坡','沙特','日本'), 2, 1.5) as `limit_day`,
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
                    (a.`成本`+a.`头程费空运`+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA空运定价_修正`,
                    (a.`成本`+a.`运费`)/(1-c.`FBM平台抽成比例`-0.03-a.`FBM目标毛利润率`-a.`FBM税率`)/a.`汇率` as `FBM定价_修正`,
                    `FBA空运定价_修正`/`FBM定价_修正` as `FBA空运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA海运定价_修正`,
                    `FBA海运定价_修正`/`FBM定价_修正` as `FBA海运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA铁路定价_修正`,
                    `FBA铁路定价_修正`/`FBM定价_修正` as `FBA铁路定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA卡航定价_修正`,
                    `FBA卡航定价_修正`/`FBM定价_修正` as `FBA卡航定价/FBM定价_修正`,            
                    (a.`成本`+if(a.`头程费快运`<0, null, a.`头程费快运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA快运定价_修正`,
                    `FBA快运定价_修正`/`FBM定价_修正` as `FBA快运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA慢运定价_修正`,
                    `FBA慢运定价_修正`/`FBM定价_修正` as `FBA慢运定价/FBM定价_修正`,           
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空海利润率反算_修正`,
                    `空海利润率反算_修正`-a.`FBA目标毛利润率` as `空海同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空铁利润率反算_修正`,
                    `空铁利润率反算_修正`-a.`FBA目标毛利润率` as `空铁同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空卡利润率反算_修正`,
                    `空卡利润率反算_修正`-a.`FBA目标毛利润率` as `空卡同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA快运定价_修正`*a.`汇率`) as `快慢利润率反算_修正`,
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
           '-1': '不建议发FBA'}
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
                        `建议物流方式` String COMMENT '建议物流方式：11:快海；12:慢海；13:铁路；15:快卡；16:慢卡；17/6:空运；0:不建议转FBA',
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
    write_to_sql(df_m, 'ads_pricing_ratio')

##
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

        # 3. 计算过程
        # sql = f"""
        # insert into support_document.{table_name}
        # with monitor_table as (
        #     select * from (
        #         {monitor_sql_str}
        #     )
        #     order by `date` desc limit 1 by sku,`站点`
        # )
        #
        # select toString(today()) as `date`,sku,`站点`,
        #     `FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
        #     `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
        #     `FBA空运定价_修正`,ifNull(`FBA海运定价_修正`, 0) as `FBA海运定价_修正`,
        #     ifNull(`FBA铁路定价_修正`, 0) as `FBA铁路定价_修正`,ifNull(`FBA卡航定价_修正`, 0) as `FBA卡航定价_修正`,
        #     `适合空运连续天数`, `适合海运连续天数`, `适合铁路连续天数`, `适合卡航连续天数`,
        #     `FBA空运定价_参考`, `FBA海运定价_参考`, `FBA铁路定价_参考`, `FBA卡航定价_参考`,
        #     `FBA空运定价波幅`, `FBA海运定价波幅`, `FBA铁路定价波幅`, `FBA卡航定价波幅`,
        #     `空海同价利率差_修正`, `空铁同价利率差_修正`, `空卡同价利率差_修正`,`发运建议`, `建议物流方式`,
        #     toString(now()) as update_time
        # from (
        #     select a.* except(limit_day,`适合空运连续天数`,`适合海运连续天数`,`适合铁路连续天数`,`适合卡航连续天数`,`FBA空运定价/FBM定价_修正`,
        #         `FBA卡航定价/FBM定价_修正`,`FBA铁路定价/FBM定价_修正`,`FBA海运定价/FBM定价_修正`,`空海同价利率差_修正`,`空铁同价利率差_修正`,`空卡同价利率差_修正`),
        #         ifNull(a.`FBA空运定价/FBM定价_修正`, 999) as `FBA空运定价/FBM定价_修正`,
        #         ifNull(a.`FBA卡航定价/FBM定价_修正`, 999) as `FBA卡航定价/FBM定价_修正`,
        #         ifNull(a.`FBA铁路定价/FBM定价_修正`, 999) as `FBA铁路定价/FBM定价_修正`,
        #         ifNull(a.`FBA海运定价/FBM定价_修正`, 999) as `FBA海运定价/FBM定价_修正`,
        #         ifNull(a.`空海同价利率差_修正`, 999) as `空海同价利率差_修正`,
        #         ifNull(a.`空铁同价利率差_修正`, 999) as `空铁同价利率差_修正`,
        #         ifNull(a.`空卡同价利率差_修正`, 999) as `空卡同价利率差_修正`,
        #         --
        #         a.`适合空运连续天数`*(ifNull(b.`适合空运连续天数_old`,0)+1) as `适合空运连续天数`,
        #         a.`适合海运连续天数`*(ifNull(b.`适合海运连续天数_old`,0)+1) as `适合海运连续天数`,
        #         a.`适合铁路连续天数`*(ifNull(b.`适合铁路连续天数_old`,0)+1) as `适合铁路连续天数`,
        #         a.`适合卡航连续天数`*(ifNull(b.`适合卡航连续天数_old`,0)+1) as `适合卡航连续天数`,
        #         ifNull(b.`FBA空运定价_修正_old`, a.`FBA空运定价_修正`) as `FBA空运定价_修正_old`,
        #         ifNull(b.`FBA海运定价_修正_old`, a.`FBA海运定价_修正`) as `FBA海运定价_修正_old`,
        #         ifNull(b.`FBA铁路定价_修正_old`, a.`FBA铁路定价_修正`) as `FBA铁路定价_修正_old`,
        #         ifNull(b.`FBA卡航定价_修正_old`, a.`FBA卡航定价_修正`) as `FBA卡航定价_修正_old`,
        #         if(`适合空运连续天数`=0, `FBA空运定价_修正_old`, ifNull(b.`FBA空运定价_参考`, a.`FBA空运定价_修正`)) as `FBA空运定价_参考`,
        #         if(`适合海运连续天数`=0, `FBA海运定价_修正_old`, ifNull(b.`FBA海运定价_参考`, a.`FBA海运定价_修正`)) as `FBA海运定价_参考`,
        #         if(`适合铁路连续天数`=0, `FBA铁路定价_修正_old`, ifNull(b.`FBA铁路定价_参考`, a.`FBA铁路定价_修正`)) as `FBA铁路定价_参考`,
        #         if(`适合卡航连续天数`=0, `FBA卡航定价_修正_old`, ifNull(b.`FBA卡航定价_参考`, a.`FBA卡航定价_修正`)) as `FBA卡航定价_参考`,
        #         if(`FBA空运定价_参考`=0 or `FBA空运定价_修正` is null or `FBA空运定价_参考` is null, 0, abs(a.`FBA空运定价_修正`-`FBA空运定价_参考`)/`FBA空运定价_参考`) as `FBA空运定价波幅`,
        #         if(`FBA海运定价_参考`=0 or `FBA海运定价_修正` is null or `FBA海运定价_参考` is null, 0, abs(a.`FBA海运定价_修正`-`FBA海运定价_参考`)/`FBA海运定价_参考`) as `FBA海运定价波幅`,
        #         if(`FBA铁路定价_参考`=0 or `FBA铁路定价_修正` is null or `FBA铁路定价_参考` is null, 0, abs(a.`FBA铁路定价_修正`-`FBA铁路定价_参考`)/`FBA铁路定价_参考`) as `FBA铁路定价波幅`,
        #         if(`FBA卡航定价_参考`=0 or `FBA卡航定价_修正` is null or `FBA卡航定价_参考` is null, 0, abs(a.`FBA卡航定价_修正`-`FBA卡航定价_参考`)/`FBA卡航定价_参考`) as `FBA卡航定价波幅`,
        #         multiIf(
        #             `适合卡航连续天数` >= 7 and `FBA卡航定价波幅`>= 0.1, '卡航定价比修正后数值连续一周低于2且FBA卡航定价为主要原因，建议可发卡航',
        #             `适合铁路连续天数` >= 7 and `FBA铁路定价波幅`>= 0.1, '铁路定价比修正后数值连续一周低于2且FBA铁路定价为主要原因，建议可发铁路',
        #             `适合海运连续天数` >= 7 and `FBA海运定价波幅`>= 0.1, '海运定价比修正后数值连续一周低于2且FBA海运定价为主要原因，建议可发海运',
        #             `适合空运连续天数` >= 7 and `FBA空运定价波幅`>= 0.1, '空运定价比修正后数值连续一周低于2且FBA空运定价为主要原因，建议可发空运',
        #         '无建议') as `发运建议`,
        #         multiIf(
        #             (`FBA海运定价/FBM定价_修正`>2 or (`适合海运连续天数`<7 and `FBA海运定价/FBM定价_修正`<=2 and `空海同价利率差_修正`> 0.01)) and a.`站点` in ('阿联酋'), '0',
        #             `FBA海运定价/FBM定价_修正`<=2 and `空海同价利率差_修正`>0.1 and `适合海运连续天数`>= 7 and a.`站点` in ('阿联酋'), '2',
        #             `FBA海运定价/FBM定价_修正`<=2 and `空海同价利率差_修正`<= 0.1 and `空海同价利率差_修正`>0.01 and `适合海运连续天数`>=7 and a.`站点` in ('阿联酋'), '2;6;9',
        #             `FBA空运定价/FBM定价_修正`<= 2 and `FBA海运定价/FBM定价_修正`<=2 and `空海同价利率差_修正`<= 0.01 and `空海同价利率差_修正`>0 and a.`站点` in ('阿联酋'), '6;9',
        #
        #             `FBA空运定价/FBM定价_修正`>2 and a.`站点` in ('日本','新加坡','沙特'), '0',
        #             `FBA空运定价/FBM定价_修正`<=2 and a.`站点` in ('日本','新加坡','沙特'), '1;2;6;9',
        #
        #             (`FBA海运定价/FBM定价_修正`> 1.5 or (`适合海运连续天数`< 7 and `FBA海运定价/FBM定价_修正`<= 1.5 and `空海同价利率差_修正`> 0.01)) and a.`站点` not in ('德国', '日本','新加坡','阿联酋','沙特','英国'), '0',
        #             `FBA海运定价/FBM定价_修正`<= 1.5 and `空海同价利率差_修正`> 0.1 and `适合海运连续天数`>= 7 and a.`站点` not in ('德国', '日本','新加坡','阿联酋','沙特','英国'), '2',
        #             `FBA海运定价/FBM定价_修正`<= 1.5 and `空海同价利率差_修正`<= 0.1 and `空海同价利率差_修正`> 0.01 and `适合海运连续天数`> 7 and a.`站点` not in ('德国', '日本','新加坡','阿联酋','沙特','英国'), '2;6;9',
        #             `FBA空运定价/FBM定价_修正`<= 1.5 and `FBA海运定价/FBM定价_修正`<= 1.5 and `空海同价利率差_修正`<= 0.01 and `空海同价利率差_修正`> 0 and a.`站点` not in ('德国', '日本','新加坡','阿联酋','沙特','英国'), '6;9',
        #
        #             (`FBA铁路定价/FBM定价_修正`> 1.5 or (`适合铁路连续天数`< 7 and `FBA铁路定价/FBM定价_修正`<= 1.5 and `空铁同价利率差_修正`> 0.01)) and a.`站点` in ('德国'), '0',
        #             `FBA铁路定价/FBM定价_修正`<= 1.5 and `空铁同价利率差_修正`> 0.1 and `适合铁路连续天数`>= 7 and a.`站点` in ('德国'), '4',
        #             `FBA铁路定价/FBM定价_修正`<= 1.5 and `空铁同价利率差_修正`<= 0.1 and `空铁同价利率差_修正`> 0.01 and `适合铁路连续天数`>= 7 and a.`站点` in ('德国'), '4;6;9',
        #             `FBA空运定价/FBM定价_修正`<= 1.5 and `FBA铁路定价/FBM定价_修正`<= 1.5 and `空铁同价利率差_修正`<= 0.01 and `空铁同价利率差_修正`> 0 and a.`站点` in ('德国'), '6;9',
        #
        #             (`FBA卡航定价/FBM定价_修正`> 1.5 or (`适合卡航连续天数`< 7 and `FBA卡航定价/FBM定价_修正`<= 1.5 and `空卡同价利率差_修正`> 0.01)) and a.`站点` in ('英国'), '0',
        #             `FBA卡航定价/FBM定价_修正`<= 1.5 and `空卡同价利率差_修正`> 0.1 and `适合卡航连续天数`>= 7 and a.`站点` in ('英国'), '5',
        #             `FBA卡航定价/FBM定价_修正`<= 1.5 and `空卡同价利率差_修正`<= 0.1 and `空卡同价利率差_修正`> 0.01 and `适合卡航连续天数`>= 7 and a.`站点` in ('英国'), '5;6;9',
        #             `FBA空运定价/FBM定价_修正`<= 1.5 and `FBA卡航定价/FBM定价_修正`<= 1.5 and `空卡同价利率差_修正`<= 0.01 and `空卡同价利率差_修正`> 0 and a.`站点` in ('英国'), '6;9',
        #             '-1') as `建议物流方式`
        #     from (
        #         select sku,`站点`,`FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
        #            `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
        #            `FBA空运定价_修正`, `FBA海运定价_修正`, `FBA铁路定价_修正`, `FBA卡航定价_修正`,`空海同价利率差_修正`, `空铁同价利率差_修正`,`空卡同价利率差_修正`,
        #            if(`站点` in ('阿联酋','新加坡','沙特'), 2, 1.5) as `limit_day`,
        #            if(`FBA空运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合空运连续天数`,
        #            multiIf(`FBA海运定价/FBM定价_修正` is null, 0, `FBA海运定价/FBM定价_修正`<=limit_day, 1, 0) as `适合海运连续天数`,
        #            multiIf(`FBA铁路定价/FBM定价_修正` is null, 0, `FBA铁路定价/FBM定价_修正`<=limit_day, 1, 0) as `适合铁路连续天数`,
        #            multiIf(`FBA卡航定价/FBM定价_修正` is null, 0, `FBA卡航定价/FBM定价_修正`<=limit_day, 1, 0) as `适合卡航连续天数`
        #         from (
        #             select a.sku as sku,a.`站点` as `站点`,
        #             a.`FBA空运定价/FBM定价` as `FBA空运定价/FBM定价`,a.`FBA卡航定价/FBM定价` as `FBA卡航定价/FBM定价`,
        #             a.`FBA海运定价/FBM定价` as `FBA海运定价/FBM定价`,a.`FBA铁路定价/FBM定价` as `FBA铁路定价/FBM定价`,
        #             (a.`成本`+a.`头程费空运`+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA空运定价_修正`,
        #             (a.`成本`+a.`运费`)/(1-c.`FBM平台抽成比例`-0.03-a.`FBM目标毛利润率`-a.`FBM税率`)/a.`汇率` as `FBM定价_修正`,
        #             `FBA空运定价_修正`/`FBM定价_修正` as `FBA空运定价/FBM定价_修正`,
        #             (a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA海运定价_修正`,
        #             `FBA海运定价_修正`/`FBM定价_修正` as `FBA海运定价/FBM定价_修正`,
        #             (a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA铁路定价_修正`,
        #             `FBA铁路定价_修正`/`FBM定价_修正` as `FBA铁路定价/FBM定价_修正`,
        #             (a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA卡航定价_修正`,
        #             `FBA卡航定价_修正`/`FBM定价_修正` as `FBA卡航定价/FBM定价_修正`,
        #             1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空海利润率反算_修正`,
        #             `空海利润率反算_修正`-a.`FBA目标毛利润率` as `空海同价利率差_修正`,
        #             1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空铁利润率反算_修正`,
        #             `空铁利润率反算_修正`-a.`FBA目标毛利润率` as `空铁同价利率差_修正`,
        #             1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空卡利润率反算_修正`,
        #             `空卡利润率反算_修正`-a.`FBA目标毛利润率` as `空卡同价利率差_修正`
        #             from (
        #                 select * except({col_list_str6},{col_list_str7}),
        #                     {col_list_str8},
        #                     `站点` as `FBA站点`,replace(`站点`, '-泛美', '') as `FBM站点`
        #                 from (
        #                     select a.* except ({col_list_str3}),
        #                         {col_list_str4},
        #                         {col_list_str5}
        #                     from (select * except (id, date, update_time) from support_document.{table_now} where `站点`='{site}') a
        #                     left join (
        #                         select sku, `站点`,{col_list_str2}
        #                         from (
        #                             {sql_str}
        #                         )
        #                         group by sku, `站点`
        #                     ) b
        #                     on a.sku=b.sku and a.`站点`=b.`站点`
        #                 )
        #             ) a
        #             left join (
        #                 select site `FBA站点`, platform_percentage `FBA平台抽成比例`
        #                 from domestic_warehouse_clear.price_calculate_parameters
        #                 where mode = 'AMAZON-FBA'
        #             ) b
        #             on a.`FBA站点`=b.`FBA站点`
        #             left join (
        #                 select site `FBM站点`, platform_percentage `FBM平台抽成比例`
        #                 from domestic_warehouse_clear.price_calculate_parameters
        #                 where mode = 'AMAZON-FBM'
        #             ) c
        #             on a.`FBM站点`=c.`FBM站点`
        #         )
        #     ) a
        #     left join monitor_table b
        #     on a.sku=b.sku and a.`站点`=b.`站点`
        # )
        # """
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
                    `适合快运连续天数` >= 7 and `FBA快运定价波幅`>= 0.1, '快运定价比修正后数值连续一周低于2且FBA快运定价为主要原因，建议可发快运',
                    `适合慢运连续天数` >= 7 and `FBA慢运定价波幅`>= 0.1, '慢运定价比修正后数值连续一周低于2且FBA慢运定价为主要原因，建议可发慢运',
                '无建议') as `发运建议`,
                multiIf(
                    (`FBA慢运定价/FBM定价_修正`>2 or (`适合慢运连续天数`<7 and `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`> 0.01)) and a.`站点` in ('阿联酋'), '0', 
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`>0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('阿联酋'), '12',
                    `FBA慢运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`>0.01 and `适合慢运连续天数`>=7 and a.`站点` in ('阿联酋'), '12;6',
                    `FBA快运定价/FBM定价_修正`<= 2 and `FBA快运定价/FBM定价_修正`<=2 and `快慢同价利率差_修正`<= 0.01 and a.`站点` in ('阿联酋'), '6',
                    `FBA快运定价/FBM定价_修正`>2 and a.`站点` in ('日本','新加坡','沙特'), '0',
                    `FBA快运定价/FBM定价_修正`<=2 and a.`站点` in ('日本','新加坡','沙特'), '6',
                    ((`FBA慢运定价/FBM定价_修正`> 1.5) or (`适合慢运连续天数`< 7 and `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.01)) and a.`站点` not in ('日本','新加坡','阿联酋','沙特'), '0',         
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('美国'), '11',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚','英国'), '12',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`> 0.1 and `适合慢运连续天数`>= 7 and a.`站点` in ('德国'), '13',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.01 and `适合慢运连续天数`>= 7 and a.`站点` in ('美国'), '11;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.01 and `适合慢运连续天数`>= 7 and a.`站点` in ('加拿大', '墨西哥', '澳大利亚'), '12;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.01 and `适合慢运连续天数`>= 7 and a.`站点` in ('德国'), '13;15', 
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.1 and `快慢同价利率差_修正`> 0.01 and `适合慢运连续天数`>= 7 and a.`站点` in ('英国'), '15;12',    
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.01 and a.`站点` in ('美国','加拿大', '墨西哥', '澳大利亚'), '6',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.01 and a.`站点` in ('德国','英国') and `FBA空运定价/FBM定价_修正` <= 1.5, '15;6',
                    `FBA慢运定价/FBM定价_修正`<= 1.5 and `快慢同价利率差_修正`<= 0.01 and a.`站点` in ('德国','英国') and `FBA空运定价/FBM定价_修正` > 1.5, '15',
                    '-1') as `建议物流方式`
            from (
                select sku,`站点`,`FBA空运定价/FBM定价`, `FBA空运定价/FBM定价_修正`,`FBA海运定价/FBM定价`, `FBA海运定价/FBM定价_修正`,
                   `FBA铁路定价/FBM定价`, `FBA铁路定价/FBM定价_修正`,`FBA卡航定价/FBM定价`, `FBA卡航定价/FBM定价_修正`,
                   `FBA快运定价/FBM定价`, `FBA快运定价/FBM定价_修正`,`FBA慢运定价/FBM定价`, `FBA慢运定价/FBM定价_修正`,
                   `FBA空运定价_修正`, `FBA海运定价_修正`, `FBA铁路定价_修正`, `FBA卡航定价_修正`,`FBA快运定价_修正`,`FBA慢运定价_修正`,
                   `空海同价利率差_修正`, `空铁同价利率差_修正`,`空卡同价利率差_修正`,`快慢同价利率差_修正`,
                   if(`站点` in ('阿联酋','新加坡','沙特','日本'), 2, 1.5) as `limit_day`,
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
                    (a.`成本`+a.`头程费空运`+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA空运定价_修正`,
                    (a.`成本`+a.`运费`)/(1-c.`FBM平台抽成比例`-0.03-a.`FBM目标毛利润率`-a.`FBM税率`)/a.`汇率` as `FBM定价_修正`,
                    `FBA空运定价_修正`/`FBM定价_修正` as `FBA空运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA海运定价_修正`,
                    `FBA海运定价_修正`/`FBM定价_修正` as `FBA海运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA铁路定价_修正`,
                    `FBA铁路定价_修正`/`FBM定价_修正` as `FBA铁路定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA卡航定价_修正`,
                    `FBA卡航定价_修正`/`FBM定价_修正` as `FBA卡航定价/FBM定价_修正`,            
                    (a.`成本`+if(a.`头程费快运`<0, null, a.`头程费快运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA快运定价_修正`,
                    `FBA快运定价_修正`/`FBM定价_修正` as `FBA快运定价/FBM定价_修正`,
                    (a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(1-b.`FBA平台抽成比例`-0.03-a.`FBA目标毛利润率`-a.`FBA税率`)/a.`汇率` as `FBA慢运定价_修正`,
                    `FBA慢运定价_修正`/`FBM定价_修正` as `FBA慢运定价/FBM定价_修正`,           
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费海运`<0, null, a.`头程费海运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空海利润率反算_修正`,
                    `空海利润率反算_修正`-a.`FBA目标毛利润率` as `空海同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费铁路`<0, null, a.`头程费铁路`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空铁利润率反算_修正`,
                    `空铁利润率反算_修正`-a.`FBA目标毛利润率` as `空铁同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费卡航`<0, null, a.`头程费卡航`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA空运定价_修正`*a.`汇率`) as `空卡利润率反算_修正`,
                    `空卡利润率反算_修正`-a.`FBA目标毛利润率` as `空卡同价利率差_修正`,
                    1-b.`FBA平台抽成比例`-0.03-a.`FBA税率`-(a.`成本`+if(a.`头程费慢运`<0, null, a.`头程费慢运`)+a.`调拨费`+2+a.fba_fees*a.`汇率`)/(`FBA快运定价_修正`*a.`汇率`) as `快慢利润率反算_修正`,
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

        ratio_limit = 1.5
        # c1 = (df_result['site'].isin(['英国', '德国', '法国', '西班牙', '意大利'])) & \
        #      (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.01)
        c101 = (df_result['site'].isin(['英国', '德国', '法国', '西班牙', '意大利'])) & \
               (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.01) & \
               (df_result['air_price_ratio'] <= ratio_limit)
        c102 = (df_result['site'].isin(['英国', '德国', '法国', '西班牙', '意大利'])) & \
               (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.01) & \
               (df_result['air_price_ratio'] > ratio_limit)
        c2 = (df_result['site'].isin(['美国', '加拿大', '墨西哥', '澳大利亚'])) & \
             (df_result['slow_price_ratio'] <= ratio_limit) & (df_result['fast_and_slow_rate_diff'] <= 0.01)
        c3 = (df_result['site'].isin(['阿联酋', '波兰'])) & \
             (df_result['slow_price_ratio'] <= 2) & (df_result['fast_and_slow_rate_diff'] <= 0.01)
        c4 = (df_result['site'].isin(['日本', '新加坡', '沙特'])) & (df_result['fast_price_ratio'] <= 2)
        # c5 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] <= 2)

        c5 = (df_result['site'].isin(['英国'])) & (df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.01) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c6 = (df_result['site'].isin(['德国', '法国', '西班牙', '意大利'])) & (
                    df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.01) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c7 = (df_result['site'].isin(['美国'])) & (df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.01) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c8 = (df_result['site'].isin(['加拿大', '墨西哥', '澳大利亚'])) & (
                    df_result['slow_price_ratio'] <= ratio_limit) & \
             (df_result['fast_and_slow_rate_diff'] > 0.01) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c9 = (df_result['site'].isin(['阿联酋'])) & (df_result['slow_price_ratio'] <= 2) & \
             (df_result['fast_and_slow_rate_diff'] > 0.01) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
             (df_result['适合慢运连续天数'] >= 7)
        c901 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] <= 2) & \
               (df_result['fast_and_slow_rate_diff'] > 0.01) & (df_result['fast_and_slow_rate_diff'] <= 0.1) & \
               (df_result['适合慢运连续天数'] >= 7)

        c10 = (df_result['site'].isin(['德国', '法国', '西班牙', '意大利'])) & (
                df_result['slow_price_ratio'] <= ratio_limit) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 7)
        c11 = (df_result['site'].isin(['美国'])) & (df_result['slow_price_ratio'] <= ratio_limit) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 7)
        c12 = (df_result['site'].isin(['英国', '加拿大', '墨西哥', '澳大利亚'])) & (
                    df_result['slow_price_ratio'] <= ratio_limit) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 7)
        c13 = (df_result['site'].isin(['阿联酋'])) & (df_result['slow_price_ratio'] <= 2) & \
              (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 7)
        c131 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] <= 2) & \
               (df_result['fast_and_slow_rate_diff'] > 0.1) & (df_result['适合慢运连续天数'] >= 7)

        col = ['英国', '德国', '法国', '西班牙', '意大利', '美国', '加拿大', '墨西哥', '澳大利亚']
        c14 = ((df_result['site'].isin(col)) & (df_result['slow_price_ratio'] > ratio_limit)) | (
                df_result['适合慢运连续天数'] < 7)
        c15 = ((df_result['site'].isin(['阿联酋'])) & (df_result['slow_price_ratio'] > 2)) | (
                    df_result['适合慢运连续天数'] < 7)
        c16 = (df_result['site'].isin(['日本', '新加坡', '沙特'])) & (df_result['fast_price_ratio'] > 2)
        c17 = (df_result['site'].isin(['波兰'])) & (df_result['slow_price_ratio'] > 2)

        df_result['proposed_transport'] = np.select(
            [c101, c102, c2, c3, c4, c5, c6, c7, c8, c9, c901, c10, c11, c12, c13, c131, c14, c15, c16, c17],
            ['15;6', '15', '6', '6', '6', '15;12', '13;15', '11;6', '12;6', '12;6', '13;6', '13', '11', '12', '12',
             '13', '0', '0', '0', '0'])

        #
        df_result['date'] = time.strftime('%Y-%m-%d')
        col = ['account_id', 'asin', 'seller_sku', 'fbm_seller', 'sku', 'site', '头程费快运', '头程费慢运', 'fbm运费',
               'fba_fast_price_rmb','fba_slow_price_rmb', 'fbm_price_rmb', 'air_price_ratio', 'fast_price_ratio',
               'slow_price_ratio', 'fast_and_slow_rate_diff',
               '适合快运连续天数', '适合慢运连续天数', 'proposed_transport', 'date']

        # df_result_all = pd.concat([df_result, df_result_all])
        #
        # print(df_result[col].info())
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
    write_to_sql(df_2, 'ads_pricing_ratio')
    # 测试
    time.sleep(600)

##
if __name__ == "__main__":
    # yunfei_check()
    # pricing_ratio()
    # pricing_ratio_new()
    # pricing_ratio_monitor_new()
    # pricing_ratio_mx()
    pricing_ratio_mx_monitor()
    # upload_pricing_ratio()
    # pricing_ratio_listing()
    # upload_listing_pricing_ratio()
