"""
TEMU海外仓链接屏蔽逻辑：
1、取当前链接的主体账号，及其应该归属的责任团队
2、sku+目的国下，公共仓的库存、预计可售天数、库龄数据
3、责任团队，sku+目的国的日销数据
4、根据屏蔽逻辑判断哪些链接需要屏蔽

屏蔽逻辑20241010：
1. 公共仓库存总可售天数<60天
2. 超90天库龄库存<=3 pcs
3. 以责任团队7天日均销量预估库存可售天数<90天。（包含公共仓和独享仓总库存的预计可售天数）
4. 在上述基础上，如果利润率都大于20%，可以暂时不屏蔽。（近7天利润率、最低活动价利润率、申报价利润率）
"""
##
import pandas as pd
import numpy as np
import time, datetime
import re
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_temu_price import get_temu_listing, get_temu_account, \
    get_freight_subsidy,get_promotion_listing, get_rate,extract_correct_string,warehouse_mark,get_line,get_main_resp
from utils.utils import  save_df, make_path
from utils import utils
import os
warnings.filterwarnings("ignore")
##
def get_cons(df):
    print('获取代销仓SKU信息...')
    sql = """
        SELECT 
            sku, latest_quotation, 1 as cons_label,
            case
                when site = 1 then 'US'
                when site = 2 then 'UK'
                when site = 3 then 'FR'
                when site = 4 then 'IT'
                when site = 5 then 'ES'
                when site = 6 then 'DE'
                when site = 7 then 'AU'
                when site = 9 then 'CA'
            else '其他' end as column_to_split
        FROM yibai_prod_base_sync.yibai_prod_sku_consignment_inventory
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_cons = conn_ck.ck_select_to_df(sql)
    df_cons = df_cons.drop_duplicates(subset='sku')
    df = pd.merge(df, df_cons[['sku','cons_label']], how='left', on=['sku'])
    return df

##
def get_listing_order():
    date_today = time.strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    date_15 = (datetime.date.today() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
    date_7 = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}' and paytime < '{date_today}'
            and platform_code = 'TEMU'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    order_base = conn_ck.ck_select_to_df(sql)
    temu_account = get_temu_account()
    temu_account = temu_account.rename(columns={'main_name':'主体账号'})
    order_base = pd.merge(order_base,
                            temu_account[['account_id',  '主体账号']],  how='left', on=['account_id'])

    # 公共仓7天销量
    df_order_info = order_base[order_base['paytime'] >= date_7]
    order1 = df_order_info[~df_order_info['warehouse_name'].str.contains('独享')]
    order2 = df_order_info[df_order_info['warehouse_name'].str.contains('独享')]
    order1_group = order1.groupby(['主体账号','sku','warehouse']).agg(
        {'total_price':'sum','quantity':'sum','true_profit_new1':'sum'}).reset_index()
    order2_group = order2.groupby(['主体账号', 'sku', 'warehouse']).agg({'quantity': 'sum'}).reset_index()
    order1_group['7days_sales_avg'] = (order1_group['quantity']/7).round(4)
    order2_group['7days_sales_avg'] = (order2_group['quantity']/7).round(4)
    order1_group['责任方7天出单利润率'] = order1_group['true_profit_new1'] / order1_group['total_price']

    # 链接维度近30天和近7天销量
    order_30 = order_base.groupby(['account_id','seller_sku']).agg(
        {'total_price':'sum','quantity':'sum'}).reset_index().rename(
        columns={'total_price': '30_sales','quantity':'30_num'})
    order_30['30_sales_avg'] = order_30['30_sales']/30
    order_temp = order_base[order_base['paytime'] >= date_7]
    order_7 = order_temp.groupby(['account_id', 'seller_sku']).agg(
        {'total_price':'sum','quantity':'sum','true_profit_new1':'sum'}).reset_index().rename(
        columns={'total_price': '7_sales','quantity':'7_num','true_profit_new1':'7_profit'})
    mode_unit_price = order_temp.groupby(
        ['account_id', 'seller_sku'])['total_price'].idxmax().map(order_temp['total_price']).reset_index()
    order_7 = pd.merge(order_7, mode_unit_price, how='left', on=['account_id','seller_sku'])
    order_7 = order_7.rename(columns={'total_price':'链接近7天出单最多售价'})
    order_7['链接近7天出单利润率'] = order_7['7_profit'] / order_7['7_sales']
    order_info = pd.merge(order_30, order_7, how='left', on=['account_id','seller_sku'])
    order_info = order_info.fillna(0)

    return order1_group, order2_group, order_info

def get_temu_sku():
    #  获取库存、库龄、日销数据
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT 
            sku nb_sku, warehouse, available_stock, on_way_stock, age_90_plus, day_sales, estimated_sales_days
        FROM over_sea.dwm_sku_info_temu
        WHERE 
            date_id = '{date_today}' and warehouse in ('美国仓','德国仓','法国仓','英国仓','澳洲仓','加拿大仓')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock_info = conn.read_sql(sql)
    return df_stock_info
## 仓标

def get_main_resp_temp(temu_listing):
    """
    匹配链接的责任归属。当前版本：第九版责任账户
    输入：listing:new_sku、主体账号
         责任人明细表
    输出：第九版责任账号、is_same
    """
    #  获取第九版责任人
    print('获取责任人明细表...')
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_resp = pd.read_excel('F://Desktop//TEMU责任人明细.xlsx', dtype={'sku': str})
    #
    df_resp = df_resp.rename(columns={'主体账号':'第九版责任账号','sku':'new_sku'})
    df_resp['new_sku'] = df_resp['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    temu_listing_0 = pd.merge(temu_listing, df_resp[['new_sku','站点','第九版责任账号']], how='left', on=['new_sku','站点'])
    #
    df_resp_line = pd.read_excel('F://Desktop//TEMU责任人明细_类目.xlsx')
    df_resp_line = df_resp_line.rename(columns={'一级类目':'一级产品线','二级级类目':'二级产品线','主体账号':'责任账号'})
    line1 = df_resp_line[df_resp_line['二级产品线'] == '全']
    line2 = df_resp_line[df_resp_line['二级产品线'] != '全']
    temu_listing_0 = pd.merge(temu_listing_0, line1[['一级产品线','责任账号']], how='left', on=['一级产品线'])
    temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['责任账号'], temu_listing_0['第九版责任账号'])
    temu_listing_0.drop(['责任账号'], axis=1, inplace=True)
    #
    temu_listing_0 = pd.merge(temu_listing_0, line2[['一级产品线','二级产品线','责任账号']], how='left', on=['一级产品线','二级产品线'])
    temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['责任账号'], temu_listing_0['第九版责任账号'])
    temu_listing_0.drop(['责任账号'], axis=1, inplace=True)
    # 剩下未归属成功的，按链接初始的主体账号
    # temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['主体账号'], temu_listing_0['第九版责任账号'])
    # temu_listing_0['is_same'] = np.where(temu_listing_0['主体账号'] == temu_listing_0['第九版责任账号'], 1, 0)
    # sku带仓标和不带仓标，归属不同责任人时，取归属相同的情况
    # temu_listing_0 = temu_listing_0.sort_values(by='is_same', ascending=False).drop_duplicates(subset=['account_id','product_sku_id'])

    return temu_listing_0
##
# df_temp = pd.read_excel('F:\Desktop\匹配Temu责任人.xlsx', dtype={'sku':str})
# ##
# df_temp = df_temp.rename(columns={'sku':'new_sku'})
# df_temp = get_line(df_temp)
# ##
# df_temp['站点'] = df_temp['area'].replace({'澳洲':'AU','美国':'US','英国':'UK','欧洲':'DE','加拿大':'CA'})
# ##
# df_temp = get_main_resp_temp(df_temp)
# ##
# df_temp.to_excel('df_temp.xlsx', index=0)
##
def main():
    """
    temu需屏蔽链接输出
    """
    order1_group, order2_group, order_info = get_listing_order()

    temu_listing = get_temu_listing()
    temu_listing = temu_listing.rename(columns={'main_name':'主体账号'})

    # temu_listing = temu_listing.sample(10000)
    # 筛选有效链接
    temu_listing = temu_listing[
        (temu_listing['online_status'] == '已发布到站点') & (~temu_listing['sku'].isna()) & (temu_listing['sku'] != '')]
    temu_listing.drop(['account_status','account_operation_mode','lazada_account_operation_mode',
                    'create_time','date_id','freight_subsidy'], axis=1, inplace=True)

    # sku拆分捆绑、仓标
    temu_listing['nb_sku'] = temu_listing['sku'].map(extract_correct_string)
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    temu_listing['new_sku'] = temu_listing['nb_sku']
    temu_listing['new_sku'] = temu_listing['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    #
    # 获取产品线和代销sku标签
    temu_listing = get_line(temu_listing)
    temu_listing = get_cons(temu_listing)
    # #  获取第九版责任人
    # print('获取责任人明细表...')
    # df_resp = pd.read_excel('F://Desktop//TEMU责任人明细.xlsx', dtype={'sku': str})
    # #
    # df_resp = df_resp.rename(columns={'主体账号':'第九版责任账号','sku':'new_sku'})
    # df_resp['new_sku'] = df_resp['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    # temu_listing_0 = pd.merge(temu_listing, df_resp[['new_sku','站点','第九版责任账号']], how='left', on=['new_sku','站点'])
    # #
    # df_resp_line = pd.read_excel('F://Desktop//TEMU责任人明细_类目.xlsx')
    # df_resp_line = df_resp_line.rename(columns={'一级类目':'一级产品线','二级级类目':'二级产品线','主体账号':'责任账号'})
    # line1 = df_resp_line[df_resp_line['二级产品线'] == '全']
    # line2 = df_resp_line[df_resp_line['二级产品线'] != '全']
    # temu_listing_0 = pd.merge(temu_listing_0, line1[['一级产品线','责任账号']], how='left', on=['一级产品线'])
    # temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['责任账号'], temu_listing_0['第九版责任账号'])
    # temu_listing_0.drop(['责任账号'], axis=1, inplace=True)
    # #
    # temu_listing_0 = pd.merge(temu_listing_0, line2[['一级产品线','二级产品线','责任账号']], how='left', on=['一级产品线','二级产品线'])
    # temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['责任账号'], temu_listing_0['第九版责任账号'])
    # temu_listing_0.drop(['责任账号'], axis=1, inplace=True)
    # # 剩下未归属成功的，按链接初始的主体账号
    # temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['主体账号'], temu_listing_0['第九版责任账号'])
    # temu_listing_0['is_same'] = np.where(temu_listing_0['主体账号'] == temu_listing_0['第九版责任账号'], 1, 0)
    # # sku带仓标和不带仓标，归属不同责任人时，取归属相同的情况
    # temu_listing_0 = temu_listing_0.sort_values(by='is_same', ascending=False).drop_duplicates(subset=['account_id','product_sku_id'])
    temu_listing_0 = get_main_resp(temu_listing)
    # sku带仓标和不带仓标，归属不同责任人时，取归属相同的情况
    temu_listing_0 = temu_listing_0.sort_values(by='is_same', ascending=False).drop_duplicates(subset=['account_id','product_sku_id'])
    # 筛选目标链接
    # col = ['易佰泛品：李金强','易佰泛品：毛竹temu','易佰泛品：冉育雪temu','易佰泛品：小雪糕temu','易佰泛品：王俊玲temu','精铺大部：杨静temu','精铺大部：李航temu']
    # temu_listing_0 = temu_listing_0[temu_listing_0['主体账号'].isin(col)]
    print(f'链接责任划分完成，共{len(temu_listing_0)}条')

    #  获取库存、库龄、日销数据
    df_stock_info = get_temu_sku()
    #
    temu_listing_0 = pd.merge(temu_listing_0, df_stock_info, how='left', on=['nb_sku','warehouse'])

    # 匹配各团队的7天日销
    order1_group = order1_group.rename(columns={'主体账号':'第九版责任账号','7days_sales_avg':'责任账号公共仓近7天销量'})
    order2_group = order2_group.rename(columns={'主体账号':'第九版责任账号','7days_sales_avg':'责任账号独享仓近7天销量'})
    temu_listing_0 = pd.merge(temu_listing_0, order1_group[['第九版责任账号','sku','warehouse','责任账号公共仓近7天销量','责任方7天出单利润率']],
                              how='left', on=['第九版责任账号','sku','warehouse'])
    temu_listing_0 = pd.merge(temu_listing_0, order2_group[['第九版责任账号','sku','warehouse','责任账号独享仓近7天销量']],
                              how='left', on=['第九版责任账号','sku','warehouse'])

    #
    col = ['责任账号公共仓近7天销量','责任账号独享仓近7天销量','available_stock','age_90_plus','estimated_sales_days']
    temu_listing_0[col] = temu_listing_0[col].fillna(0).astype(float)
    c1 = (temu_listing_0['available_stock'] == 0) | (temu_listing_0['available_stock'] == '')
    temu_listing_0['责任团队_预估可售天数'] = np.where(
        c1, 0, (temu_listing_0['available_stock']/temu_listing_0['责任账号公共仓近7天销量']).replace(np.inf, 9999))
    #

    c2 = (temu_listing_0['is_same']==0) & (temu_listing_0['estimated_sales_days']<60) & \
         (temu_listing_0['age_90_plus']<=3) & (temu_listing_0['责任团队_预估可售天数']<90)
    temu_listing_0['is_shield'] = np.where(c2, 1, 0)
    temu_listing_0['is_shield'] = np.where(c1, 0, temu_listing_0['is_shield'])

    #
    order_info = order_info.rename(columns={'seller_sku':'product_sku_id'})
    temu_listing_0 = pd.merge(temu_listing_0, order_info, how='left', on=['account_id','product_sku_id'])
    temu_listing_0.drop(['7_profit'], axis=1, inplace=True)

    dic = {'is_same':'当前归属是否与责任归属一致', 'is_shield':'是否屏蔽','cons_label':'是否代销品','available_stock':'大仓可用库存',
           'on_way_stock': '大仓在途库存','age_90_plus':'超90天库龄库存','day_sales':'大仓日销','estimated_sales_days':'大仓预计可售天数',
           '30_sales':'链接近30天销售额','7_sales':'链接近7天销售额','7_num':'链接近7天销量',}

    return temu_listing_0
# temu_listing_0 = main()
## 独享仓库存
def get_duxiang_stock():

    date_today = time.strftime('%Y%m%d')
    sql = f"""
    
        SELECT
            ps.sku sku, toString(toDate(toString(date_id))) date_id, yw.ebay_category_id AS category_id, yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
            available_stock, allot_on_way_count AS on_way_stock
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        WHERE 
            ps.date_id = '{date_today}'          -- 根据需要取时间
            and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            and ps.available_stock > 0
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and yw.warehouse_other_type = 2   -- 筛选公告仓（非子仓）
            and ywc.name in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓','法国仓','俄罗斯仓','乌拉圭仓')
            and yw.warehouse_name like '%独享%'
        ORDER BY date_id DESC
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_duxiang_stock = conn_ck.ck_select_to_df(sql)

    c1 = df_duxiang_stock['warehouse_name'].str.contains('1009')
    c2 = df_duxiang_stock['warehouse_name'].str.contains('1010')
    c3 = df_duxiang_stock['warehouse_name'].str.contains('1818')
    c4 = df_duxiang_stock['warehouse_name'].str.contains('1008')
    df_duxiang_stock['主体账号'] = np.select([c1, c2, c3, c4],
                                             ['易佰泛品：王俊玲temu', '易佰泛品：李金强', '易佰泛品：冉育雪temu',
                                              '易佰泛品：毛竹temu'])
    return df_duxiang_stock

# 补充促销价对应毛利率
def get_cost(df):
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2024-10-08'
    sql = f"""
        SELECT account_id, product_sku_id, new_price, total_cost
        FROM yibai_oversea.oversea_temu_listing_all
        WHERE date_id = '{date_today}'
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_listing = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_listing, how='left', on=['account_id','product_sku_id'])


    return df

##
# 活动价的运费补贴计算
def get_promtion_profit():
    """
    汇总信息，计算活动价的利润率
    """
    temu_listing = main()
    promotion_listing = get_promotion_listing()

    # 筛选链接的最低活动价
    df_prom_info = pd.merge(promotion_listing, temu_listing[[
        'account_id','product_skc_id','主体账号','第九版责任账号', 'sku','warehouse']], how='left', on=['account_id','product_skc_id'])
    df_prom_info['activity_price'] = df_prom_info['activity_price'].astype(float)
    idx = df_prom_info.groupby(['account_id','product_skc_id'])['activity_price'].idxmin()
    # 使用这些索引来选择记录
    listing_prom = df_prom_info.loc[idx].rename(columns={'activity_price':'链接最低活动价'})
    listing_prom = listing_prom[['account_id','product_skc_id','activity_stock','链接最低活动价']]

    sku_prom = df_prom_info.groupby(['第九版责任账号','sku','warehouse']).agg(
        {'activity_price':'min'}).reset_index().rename(columns={'activity_price':'责任人最低活动价'})

    df = pd.merge(temu_listing, listing_prom, how='left', on=['account_id','product_skc_id'])
    df_result = pd.merge(df, sku_prom, how='left', on=['第九版责任账号','sku','warehouse'])

    df_result = get_cost(df_result)

    df_rate0 = get_rate()
    df_rate = df_rate0[df_rate0['country'] == 'US']
    # 运费补贴
    df_result = get_freight_subsidy(df_result)
    df_result[['链接最低活动价','责任人最低活动价']] = df_result[['链接最低活动价','责任人最低活动价']].astype(float)
    df_result['运费补贴_链接维度'] = np.where(
        (df_result['链接最低活动价'] * df_rate.iloc[0, 2] / df_result['rate']) < df_result['limit_price'],
        df_result['freight_subsidy'], 0)
    df_result['运费补贴_责任人维度'] = np.where(
        (df_result['责任人最低活动价'] * df_rate.iloc[0, 2] / df_result['rate']) < df_result['limit_price'],
        df_result['freight_subsidy'], 0)
    df_result.drop(['rate', 'limit_price'], axis=1, inplace=True)
    #
    df_result['rate'] = df_rate.iloc[0, 2]
    #
    df_result['链接申报价的利润率'] = 1 - 0.04 -  (
            df_result['new_price'] + df_result['total_cost']) / df_result['rate'] / df_result['supplier_price'] + (
                                                 df_result['freight_subsidy'] / df_result['rate'] / df_result['supplier_price'])
    df_result['链接最低活动价的利润率'] = 1 - 0.04 -  (
            df_result['new_price'] + df_result['total_cost']) / df_result['rate'] / df_result['链接最低活动价'] + (
                                                 df_result['运费补贴_链接维度'] / df_result['rate'] / df_result['链接最低活动价'])
    #
    df_result['责任人最低活动价的利润率'] = 1 - 0.04 -  (
            df_result['new_price'] + df_result['total_cost']) / df_result['rate'] / df_result['责任人最低活动价'] + (
                                                 df_result['运费补贴_责任人维度'] / df_result['rate'] / df_result['责任人最低活动价'])

    # 补充屏蔽条件
    c1 = (df_result['is_shield']==1) & ((df_result['链接最低活动价的利润率'] < 0.25) | (df_result['链接申报价的利润率']<0.25))
    c2 = (df_result['is_shield']==1)
    df_result['is_shield'] = np.where(c1, 1, np.where(c2,0,df_result['is_shield']))

    # # 第二种思路
    # c3 = (df_result['is_shield']==1) & ((df_result['链接近7天出单利润率'] >= 0.20) & (df_result['链接最低活动价的利润率'] >= 0.20))
    # c4 = (df_result['is_shield'] == 1) & (
    #             (df_result['链接近7天出单利润率'] >= 0.20) & (df_result['链接最低活动价的利润率'].isna()))
    # c5 = (df_result['is_shield'] == 1) & (
    #             (df_result['链接近7天出单利润率'].isna()) & (df_result['链接最低活动价的利润率'] >= 0.20))
    # c6 = (df_result['is_shield'] == 1) & (
    #             (df_result['链接近7天出单利润率'].isna()) & (df_result['链接最低活动价的利润率'].isna()) & (df_result['链接申报价的利润率'] >= 0.20))
    # df_result['is_shield'] = np.where(c3 | c4 | c5 | c6, 0, df_result['is_shield'])

    return df_result

##
def temp():
    """
    temu需屏蔽链接输出
    """
    order1_group, order2_group, order_info = get_listing_order()

    temu_listing = get_temu_listing()
    temu_listing = temu_listing.rename(columns={'main_name':'主体账号'})
    # 筛选有效链接
    temu_listing = temu_listing[
        (temu_listing['online_status'] == '已发布到站点') & (~temu_listing['sku'].isna()) & (temu_listing['sku'] != '')]
    temu_listing.drop(['account_status','account_operation_mode','lazada_account_operation_mode',
                    'create_time','date_id'], axis=1, inplace=True)

    # sku拆分捆绑、仓标
    temu_listing['nb_sku'] = temu_listing['sku'].map(extract_correct_string)
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    temu_listing['new_sku'] = temu_listing['nb_sku']
    temu_listing['new_sku'] = temu_listing['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    #
    # 获取产品线和代销sku标签
    temu_listing = get_line(temu_listing)
    temu_listing = get_cons(temu_listing)
    #  获取第九版责任人
    temu_listing_0 = get_main_resp(temu_listing)
    # sku带仓标和不带仓标，归属不同责任人时，取归属相同的情况
    temu_listing_0 = temu_listing_0.sort_values(by='is_same', ascending=False).drop_duplicates(subset=['account_id','product_sku_id'])
    # 筛选目标链接
    # col = ['易佰泛品：李金强','易佰泛品：毛竹temu','易佰泛品：冉育雪temu','易佰泛品：小雪糕temu','易佰泛品：王俊玲temu','精铺大部：杨静temu','精铺大部：李航temu']
    # temu_listing_0 = temu_listing_0[temu_listing_0['主体账号'].isin(col)]
    print(f'链接责任划分完成，共{len(temu_listing_0)}条')
    col = ['account_id','product_sku_id','total_cost','第九版责任账号','freight_subsidy']
    temu_listing_0 = temu_listing_0[col]

    return temu_listing_0

def split_excel_by_account(input_path, output_dir, t='屏蔽'):
    """
    将一个Excel文件根据账号列拆分为多个Excel文件。

    参数:
    input_excel_path: 字符串，输入Excel文件的路径。
    output_directory: 字符串，输出Excel文件的目录。
    """
    date = time.strftime('%Y%m%d')
    # 读取Excel文件
    df = pd.read_excel(input_path)
    print(df.info())
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    col_name = '主体账号'
    if t == '清仓':
        col_name = '责任人'
    # 获取账号列的唯一值
    unique_accounts = df[col_name].unique()
    # 遍历每个账号，创建单独的Excel文件
    for account in unique_accounts:
        # 筛选出当前账号的数据
        account_df = df[df[col_name] == account]

        # 定义输出文件名
        name = account.split('：')[-1]
        name = name.replace('temu','')
        if t == '屏蔽':
            output_file_name = f"Temu责任方库存不足非责任方强制屏蔽-{name}{date}.xlsx"
        elif t == '达标':
            output_file_name = f"TEMU链接利润率达标情况-{name}{date}.xlsx"
        elif t == '清仓':
            output_file_name = f"TEMU海外仓清仓定价-{name}{date}.xlsx"
        output_file_path = os.path.join(output_dir, output_file_name)

        # 保存为Excel文件
        account_df.to_excel(output_file_path, index=False)

    print(f"Excel文件已成功拆分到目录：{output_dir}")


##
##
if __name__ == '__main__':
    # utils.program_name = 'TEMU链接屏蔽逻辑'
    # make_path()
    #
    # df_result = get_promtion_profit()
    # save_df(df_result, 'TEMU链接屏蔽逻辑', file_type='xlsx')
    #
    # temu_listing = temp()
    # save_df(temu_listing, 'TEMU链接利润率达标情况', file_type='xlsx')

    # 拆分表格
    # input_path = 'F:\Desktop\日常任务\TEMU定价需求\TEMU屏蔽链接拆分\达标数据拆分\TEMU订单利润率达标情况1009V3.xlsx'
    # # output_dir = 'F:\Desktop\日常任务\TEMU定价需求\TEMU屏蔽链接拆分\达标数据拆分'
    # input_path = 'F:\Desktop\日常任务\TEMU定价需求\TEMU屏蔽链接拆分\屏蔽链接拆分\TEMU链接屏蔽逻辑1022_屏蔽链接.xlsx'
    # output_dir = 'F:\Desktop\日常任务\TEMU定价需求\TEMU屏蔽链接拆分\屏蔽链接拆分'
    input_path = 'F:\Desktop\日常任务\TEMU定价需求\TEMU屏蔽链接拆分\清仓定价拆分\TEMU海外仓清仓定价1218.xlsx'
    output_dir = 'F:\Desktop\日常任务\TEMU定价需求\TEMU屏蔽链接拆分\清仓定价拆分'
    split_excel_by_account(input_path, output_dir, t='清仓')