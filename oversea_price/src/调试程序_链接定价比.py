"""
计算当前FBA链接的定价比
判断当前FBA链接是否适合继续做FBA，以及合适的头程运输方式
1、获取当前FBA链接
2、获取价格计算的参数
3、针对不同头程方式：空运、海运、铁路，带入到定价公式，计算FBA的价格
4、计算FBM的价格
5、计算定价比，及建议物流方式
"""
##
import pandas as pd
import numpy as np
import datetime, time
from tqdm import tqdm
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from pulic_func.base_api.all_freight_interface import *
from pulic_func.base_api.adjust_price_function_amazon import shuilv,fanou_fanmei
from pulic_func.price_adjust_web_service.make_price import amazon_fba_para
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang,jisuanfenlei,sku_and_num_split,mobanxiuzheng
from pulic_func.adjust_price_base_api.FBA_fee import tou_cheng_dingjiabi
from pulic_func.price_adjust_web_service.AMAZON_FBA_dingjia import fba_ding_jia_biao, aut
from all_auto_task.pricing_ratio_monitor import FBA_jisuan,FBM_jisuan, get_diff, get_cost_range, \
    merge_four_dim_diff,merge_first_product_line,bendiyunfei
from all_auto_task.scripts_ck_client import CkClient
##
# import importlib
# import sys
# importlib.reload(sys.modules['all_auto_task.pricing_ratio_monitor'])
##
def get_listing(site='泛欧'):
    # date_today = time.strftime('%Y%m%d')
    date_today = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    # 获取FBA全量链接信息
    # 由于数量较大，采用分批取数的方式
    if site == '泛欧':
        sql = f"""
            SELECT distinct account_id
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
            WHERE site2 = '泛欧'
        """
    if site == '非泛欧':
        sql = f"""
            SELECT distinct account_id
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
            WHERE site2 != '泛欧'
        """
    df_account = conn_ck.ck_select_to_df(sql)
    print(f'店铺总数量：{len(df_account)}')
    # sql = f"""
    #     SELECT count()
    #     FROM yibai_product_kd_sync.yibai_amazon_listing_alls
    #     WHERE fulfillment_channel = 'AMA'
    # """
    # df_cnt = conn_ck.ck_select_to_df(sql)
    # print(df_cnt)
    #
    step = 100
    account_list = [df_account[i:i + step] for i in range(0, len(df_account), step)]
    #
    df_listing = pd.DataFrame()
    time1 = time.time()
    for a in account_list:
        account_tuple = tuple(a['account_id'].unique())
        sql = f"""
            SELECT 
                f.account_id, f.asin, f.seller_sku as seller_sku, a.seller_sku as fbm_seller, e.sku, f.site, f.site2, 
                case when r.fba_commission_rate is Null then 0.15 else r.fba_commission_rate end fba_commission_rate, 
                case when r2.fbm_commission_rate is Null then fba_commission_rate else r2.fbm_commission_rate end fbm_commission_rate,
                f.FBM_difference,f.FBM_profit, f.FBA_profit,
                cost, first_trip_source, first_trip_fee_rmb, toucheng_kongyun,toucheng_haiyun,toucheng_tl,
                toucheng_kh,e.fba_fees_rmb, m.available,
                p.inv_age_0_to_90_days, p.inv_age_91_to_180_days, p.inv_age_181_to_270_days,p.inv_age_271_to_365_days,p.inv_age_365_plus_days
            FROM (
                SELECT 
                    distinct account_id, asin1 as asin, seller_sku, sku, c.site1 site,
                    case 
                        when c.site1 in ('德国', '法国', '西班牙','意大利','瑞典','荷兰','波兰','比利时','土耳其') then '泛欧'
                        else c.site1
                    end as site2, FBM_difference , FBM_profit, FBA_profit
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
            ) r2
            ON f.account_id = r2.account_id and a.seller_sku = r2.seller_sku
            LEFT JOIN (
                SELECT account_id, sku, afn_fulfillable_quantity as available
                FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end 
                WHERE toInt32(account_id) in {account_tuple}
            ) m
            ON toInt32(f.account_id) = toInt32(m.account_id) and f.seller_sku = m.sku
            LEFT JOIN (
                SELECT account_id, sku, inv_age_0_to_90_days, inv_age_91_to_180_days, inv_age_181_to_270_days,inv_age_271_to_365_days,inv_age_365_plus_days
                FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_aged_planning_data 
                WHERE toInt32(account_id) in {account_tuple}
            ) p
            ON toInt32(f.account_id) = toInt32(p.account_id) and f.seller_sku = p.sku
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        df_listing = pd.concat([df_temp, df_listing])
    df_listing.columns = [i.split('.')[-1] for i in df_listing.columns]
    time2 = time.time()
    t = time2 - time1
    print(f"FBA链接信息获取完成. 共耗时{'%.0f' % t}s")
    return df_listing
#
def get_linelist(df):
    sql = """select distinct a.sku as sku,c.category_name as `一级产品线`
                from yibai_prod_base_sync.yibai_prod_sku a 
                left join yibai_prod_base_sync.yibai_prod_category b 
                on a.product_category_id=b.id
                left join yibai_prod_base_sync.yibai_prod_category c 
                on b.category_id_level_1=c.id"""
    conn = pd_to_ck(database='fba_ag_over_180_days_price_adjustment', data_sys='调价明细历史数据')
    linlist = conn.ck_select_to_df(sql)
    df['sku'] = df['sku'].astype(str)
    linlist['sku'] = linlist['sku'].astype(str)

    df = df.merge(linlist, on=['sku'], how='left')
    df = df.fillna({'一级产品线': 'other'})
    return df

def get_4_difference(df, type1='FBA'):
    df.rename(columns={'cost': '成本'}, inplace=True)
    df.rename(columns={'site': '站点'}, inplace=True)
    # type: 'FBA','FBM','ALL'
    sql = """
    SELECT case when a.shipping_type =1 THEN 'FBM' WHEN 3 THEN 'FBA' END AS `渠道`,
    case when a.site = 'other' then 'other' else c.site1  end as `站点`,
    case when b.category_name ='' then 'other' else b.category_name end as `一级产品线`,a.cost_range `成本段`,
    toFloat64(net_profit2) `毛净利差值`,toFloat64(true_refund_profit) `真实退款率`
    FROM yibai_sale_center_listing_sync.yibai_listing_profit_config a
    left join yibai_prod_base_sync.yibai_prod_category b on a.first_product_line = b.id
    left join domestic_warehouse_clear.site_table c on a.site = UPPER(c.site) 
    where a.platform_code ='AMAZON' AND a.shipping_type in (1,3)  and a.is_del = 0 and a.status = 1
    HAVING `站点` <> ''
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    cha = conn_mx.ck_select_to_df(sql)
    cha['毛净利差值'] = cha['毛净利差值'] / 100
    cha['真实退款率'] = cha['真实退款率'] / 100
    cha = cha[cha['渠道'] == type1]
    cha = cha.drop_duplicates(['站点', '一级产品线', '成本段'])
    cha.drop(['渠道'], axis=1, inplace=True)

    cha1 = cha.loc[(cha['成本段'] == 'other'), ['站点', '一级产品线', '毛净利差值', '真实退款率']].drop_duplicates(
        ['站点', '一级产品线'])
    cha1.rename(columns={'毛净利差值': '毛净利差值1', '真实退款率': '真实退款率1'}, inplace=True)

    cha2 = cha.loc[(cha['一级产品线'] == 'other') & (cha['成本段'] == 'other'), ['站点', '毛净利差值',
                                                                                 '真实退款率']].drop_duplicates(
        ['站点'])
    cha2.rename(columns={'毛净利差值': '毛净利差值2', '真实退款率': '真实退款率2'}, inplace=True)
    if type1 == 'FBA':
        df['成本段'] = 'other'
        df.loc[(df['成本'] <= 20) & (df['成本'] > 0), '成本段'] = '0,20'
        df.loc[(df['成本'] <= 100) & (df['成本'] > 20), '成本段'] = '20,100'
        df.loc[df['成本'] > 100, '成本段'] = '100,100000'
    elif type1 == 'FBM':
        df['成本段'] = 'other'
        df.loc[(df['成本'] <= 100) & (df['成本'] > 0), '成本段'] = '0,100'
        df.loc[(df['成本'] <= 300) & (df['成本'] > 100), '成本段'] = '100,300'
        df.loc[df['成本'] > 300, '成本段'] = '300,100000'
    df = df.merge(cha, on=['站点', '一级产品线', '成本段'], how='left')
    df = df.merge(cha1, on=['一级产品线', '站点'], how='left')
    df.loc[df['毛净利差值'].isnull(), '毛净利差值'] = df['毛净利差值1']
    df.loc[df['真实退款率'].isnull(), '真实退款率'] = df['真实退款率1']
    df = df.merge(cha2, on=['站点'], how='left')
    df.loc[df['毛净利差值'].isnull(), '毛净利差值'] = df['毛净利差值2']
    df.loc[df['真实退款率'].isnull(), '真实退款率'] = df['真实退款率2']
    df.drop(['毛净利差值1', '毛净利差值2', '真实退款率1', '真实退款率2'], axis=1, inplace=True)
    return df

#
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

#
def cut_bins(df):
    df['ky价差分段'] = pd.cut(df['ky定价价差'], bins=[-3000,-1000, -10, 0, 5, 10, 20,30,40,50,60,100,1000],
                                labels=['A:(,-100]', 'B:(-100,-10]', 'C:(-10,0]', 'D:(0,5]', 'E:(5,10]','F:(10,20]','G:(20,30]','H:(30,40]','I:(40,50]','J:(50,60]','K:(60,100]','L:(100,+]'])
    df['ky价差分段'] = np.where(df['ky定价价差'] > 100, 'L:(100,+]', df['ky价差分段'])
    df['ky价差分段'] = np.where(df['ky定价价差'] < -100, 'A:(,-100]', df['ky价差分段'])

    df['ky定价比值分段'] = pd.cut(df['ky定价比值'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8,3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]','e:(0.8, 1]','f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]','j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]', 'm:(2.4, 2.6]','n:(2.6, 2.8]','o:(2.8, 3]', 'p:(3, +)'])
    df['ky定价比值分段'] = np.where(df['ky定价比值'] > 5, 'p:(3, +)', df['ky定价比值分段'])
    df['hy定价比值分段'] = pd.cut(df['hy定价比值'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8,3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]','e:(0.8, 1]','f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]','j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]', 'm:(2.4, 2.6]','n:(2.6, 2.8]','o:(2.8, 3]', 'p:(3, +)'])
    df['hy定价比值分段'] = np.where(df['hy定价比值'] > 5, 'p:(3, +)', df['hy定价比值分段'])
    df['hy定价比值分段'] = np.where(df['hy定价比值'].isna(), 'Others', df['hy定价比值分段'])
    df['tl定价比值分段'] = pd.cut(df['tl定价比值'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8,3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]','e:(0.8, 1]','f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]','j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]', 'm:(2.4, 2.6]','n:(2.6, 2.8]','o:(2.8, 3]', 'p:(3, +)'])
    df['tl定价比值分段'] = np.where(df['tl定价比值'] > 5, 'p:(3, +)', df['tl定价比值分段'])
    df['tl定价比值分段'] = np.where(df['tl定价比值'].isna(), 'Others', df['tl定价比值分段'])
    # 修改
    df['kh定价比值分段'] = pd.cut(df['kh定价比值'],
                                  bins=[0, 0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8, 3, 5],
                                  labels=['a:(0, 0.2]', 'b:(0.2, 0.4]', 'c:(0.4, 0.6]', 'd:(0.6, 0.8]', 'e:(0.8, 1]',
                                          'f:(1, 1.2]', 'g:(1.2, 1.4]',
                                          'h:(1.4, 1.6]', 'i:(1.6, 1.8]', 'j:(1.8, 2]', 'k:(2, 2.2]', 'l:(2.2, 2.4]',
                                          'm:(2.4, 2.6]', 'n:(2.6, 2.8]', 'o:(2.8, 3]', 'p:(3, +)'])
    df['kh定价比值分段'] = np.where(df['kh定价比值'] > 5, 'p:(3, +)', df['kh定价比值分段'])
    df['kh定价比值分段'] = np.where(df['kh定价比值'].isna(), 'Others', df['kh定价比值分段'])
    df['运费比分段'] = pd.cut(df[f'运费比'],
                                  bins=[0, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65,
                                        0.7, 0.75, 0.8, 1, 2, 9],
                                  labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                          'e:(0.2, 0.25]', 'f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]',
                                          'i:(0.4, 0.45]',
                                          'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]',
                                          'n:(0.65, 0.7]',
                                          'o:(0.7, 0.75]', 'p:(0.75, 0.8)', 'q:(0.8, 1)', 'r:(1, 2)', 's:(2, +)'])
    df['运费比分段'] = np.where(df[f'运费比'] >= 2, 's:(2, +)', df['运费比分段'])
    col_list = ['总运费比', '空运总运费比', '海运总运费比', '铁路总运费比', '卡航总运费比']
    for i in col_list:
        df[f'{i}分段'] = pd.cut(df[f'{i}'],
                                bins=[0, 0.05, 0.1, 0.15, 0.20, 0.25, 0.30, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65,
                                      0.7, 0.75, 0.8, 1, 2, 9],
                                labels=['a:(0, 0.05]', 'b:(0.05, 0.1]', 'c:(0.1, 0.15]', 'd:(0.15, 0.20]',
                                        'e:(0.2, 0.25]', 'f:(0.25, 0.3]', 'g:(0.3, 0.35]', 'h:(0.35, 0.4]',
                                        'i:(0.4, 0.45]',
                                        'j:(0.45, 0.5]', 'k:(0.5, 0.55]', 'l:(0.55, 0.6]', 'm:(0.6, 0.65]',
                                        'n:(0.65, 0.7]',
                                        'o:(0.7, 0.75]', 'p:(0.75, 0.8)', 'q:(0.8, 1)', 'r:(1, 2)', 's:(2, +)'])
    # bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2, 1.3,
    #         1.4, 1.5, 1.6, 2, 4, 20],
    # labels = ['a:(0, 0.1]', 'b:(0.1, 0.2]', 'c:(0.2, 0.3]', 'd:(0.3, 0.4]',
    #           'e:(0.4, 0.5]', 'f:(0.5, 0.6]', 'g:(0.6, 0.7]', 'h:(0.7, 0.8]',
    #           'i:(0.8, 0.9]',
    #           'j:(0.9, 1]', 'k:(1, 1.1]', 'l:(1.1, 1.2]', 'm:(1.2, 1.3]',
    #           'n:(1.3, 1.4]',
    #           'o:(1.4, 1.5]', 'p:(1.5, 1.6)', 'q:(1.6, 2)', 'r:(2, 4)', 's:(4, +)'])
    df[f'{i}分段'] = np.where(df[f'{i}'] >= 2, 's:(2, +)', df[f'{i}分段'])
    return df
#
def dwm_data(df_listing):
    """
    计算定价比
    """
    #
    # (product_cost)+(fba_fees)*rate+(first_trip_fee_rmb)+2)/(1-commission_rate-0.03-FBA_difference-FBA_profit-tax_rate)
    print(df_listing.info())
    df_listing = get_linelist(df_listing)
    df_listing = get_4_difference(df_listing, type1='FBA')
    df_listing = shuilv(df_listing, fb_type='FBA')
    df_jie_kou = freight_interface_fu(df_listing, table_name='freight_interface_amazon')
    print('FBM运费数据获取完成.')
    df_listing = df_listing.merge(df_jie_kou, on=['站点', 'sku'], how='left')
    df_listing = df_listing.rename(columns={'运费': 'fbm_fees'})

    df_listing = get_price(df_listing)
    print('价格计算完成.')
    # 库存数据处理
    df_stock = df_listing[['account_id', 'asin', 'seller_sku', '站点', 'site2', 'available', 'inv_age_0_to_90_days',
                           'inv_age_91_to_180_days',
                           'inv_age_181_to_270_days', 'inv_age_271_to_365_days', 'inv_age_365_plus_days']]
    cols = ['available', 'inv_age_0_to_90_days', 'inv_age_91_to_180_days', 'inv_age_181_to_270_days',
            'inv_age_271_to_365_days', 'inv_age_365_plus_days']
    df_stock[cols] = df_stock[cols].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
    df_stock = fanou_fanmei(df_stock)
    df_stock['排序'] = 2
    df_stock.loc[df_stock['站点'] == '美国', '排序'] = 1
    # 按【seller_sku + 标识】去重。优先选有库龄的数据
    df_stock['a_col'] = df_stock['inv_age_0_to_90_days'] + df_stock['inv_age_91_to_180_days'] + df_stock[
        'inv_age_181_to_270_days'] + df_stock['inv_age_271_to_365_days'] + df_stock['inv_age_365_plus_days']
    df_stock = df_stock.sort_values(['排序', 'available', 'a_col'], ascending=[True, False, False])
    df_stock = df_stock.drop_duplicates(['标识', 'seller_sku'], 'first')
    df_stock.drop(['排序', '站点', 'a_col'], axis=1, inplace=True)
    df_stock_asin = df_stock.groupby(['asin', 'site2']).agg({'available': 'sum', 'inv_age_0_to_90_days': 'sum',
                                                             'inv_age_91_to_180_days': 'sum',
                                                             'inv_age_181_to_270_days': 'sum',
                                                             'inv_age_271_to_365_days': 'sum',
                                                             'inv_age_365_plus_days': 'sum'}).reset_index()
    # 取订单数据
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    # 订单数据统计
    date_today = datetime.datetime.today().strftime('%Y-%m-%d')
    date_start = (datetime.datetime.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    sql = f"""
        SELECT account_id, seller_sku, sum(`sku数量`) as `销量`, sum(`净利润`) as `净利润`, sum(`销售额`) as `销售额`
        FROM domestic_warehouse_clear.monitor_fba_order
        WHERE created_time >= '{date_start}' and created_time < '{date_today}'
        GROUP BY account_id, seller_sku
    """
    df_sales = conn_ck.ck_select_to_df(sql)
    #
    # df_listing.drop(['销量','销售额'], axis=1, inplace=True)
    df_sales_final = pd.merge(df_listing, df_sales, how='left', on=['account_id', 'seller_sku'])

    # 销量处理
    print('按asin+站点聚合信息...')
    column_list = ['account_id', 'seller_sku', 'asin', 'site2', '销量', '销售额', '净利润']
    df_sales_final = df_sales_final[column_list].drop_duplicates(subset=['account_id', 'seller_sku'])
    column_list_2 = ['销量', '销售额', '净利润']
    df_sales_final[column_list_2] = df_sales_final[column_list_2].fillna(0)
    df_sales_final = df_sales_final.groupby(['asin', 'site2']).agg(
        {'销量': 'sum', '销售额': 'sum', '净利润': 'sum'}).reset_index()
    df_listing = df_listing[(~df_listing['fbm_price_rmb'].isna())]
    df_listing['ky定价价差'] = (df_listing['fba_ky_price_rmb'] - df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['ky定价比值'] = (df_listing['fba_ky_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['hy定价比值'] = (df_listing['fba_hy_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['tl定价比值'] = (df_listing['fba_tl_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)
    df_listing['kh定价比值'] = (df_listing['fba_kh_price_rmb'] / df_listing['fbm_price_rmb']).astype(float).round(2)

    # 链接处理
    col_list = ['asin', 'seller_sku', 'fbm_seller', 'sku', 'site2', '成本',
                'first_trip_source', 'fba_fees_rmb', 'first_trip_fee_rmb', 'toucheng_kongyun', 'toucheng_haiyun',
                'toucheng_tl', 'toucheng_kh', 'fbm_fees',
                'fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb', 'fba_kh_price_rmb', 'fbm_price_rmb']
    df_listing_final = df_listing[col_list]
    df_listing_final = df_listing_final[~df_listing_final['fbm_price_rmb'].isna()].drop_duplicates(
        subset=['asin', 'site2'])
    df_listing_price = df_listing.groupby(['asin', 'site2']).agg(
        {'ky定价价差': 'mean', 'ky定价比值': 'mean', 'hy定价比值': 'mean', 'tl定价比值': 'mean', 'kh定价比值': 'mean'})
    # 按asin + site维度聚合销售数据和库存数据
    df_listing_final = pd.merge(df_listing_final, df_listing_price, how='left', on=['asin', 'site2'])
    df_listing_final = pd.merge(df_listing_final, df_stock_asin, how='left', on=['asin', 'site2'])
    df_listing_final = pd.merge(df_listing_final, df_sales_final, how='left', on=['asin', 'site2'])
    column_list_3 = ['available', 'inv_age_0_to_90_days', 'inv_age_91_to_180_days', 'inv_age_181_to_270_days',
                     'inv_age_271_to_365_days', 'inv_age_365_plus_days']
    #
    df_listing_final[column_list_3] = df_listing_final[column_list_3].apply(pd.to_numeric, errors='coerce').fillna(
        0).astype(int)
    # 分段处理
    df_listing_final['运费比'] = df_listing_final['first_trip_fee_rmb'] / df_listing_final['fbm_fees']
    df_listing_final['总运费比'] = (df_listing_final['first_trip_fee_rmb'] + df_listing_final['fba_fees_rmb']) / \
                                   df_listing_final['fbm_fees']
    # 修改总运费比为：fba头程/fbm总运费
    df_listing_final['空运总运费比'] = (df_listing_final['toucheng_kongyun']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final['海运总运费比'] = (df_listing_final['toucheng_haiyun']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final['铁路总运费比'] = (df_listing_final['toucheng_tl']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final['卡航总运费比'] = (df_listing_final['toucheng_kh']) / \
                                       df_listing_final['fbm_fees']
    df_listing_final = cut_bins(df_listing_final)
    #
    df_listing_final['销量'] = df_listing_final['销量'].fillna(0)
    df_listing_final['日均销量'] = (df_listing_final['销量'] / 30).astype(float)

    df_listing_final['日均销量分段'] = pd.cut(df_listing_final['日均销量'],
                                              bins=[-1, 0, 0.1, 0.3, 0.6, 1, 3, 5, 10, 20, 50],
                                              labels=['J:0', 'I:(0,0.1]', 'H:(0.1,0.3]', 'G:(0.3,0.6]', 'F:(0.6,1]',
                                                      'E:(1,3]', 'D:(3,5]', 'C:(5,10]', 'B:(10,20]', 'A:(20,+]'])
    df_listing_final['日均销量分段'] = np.where(df_listing_final['日均销量'] >= 50, 'A:(20,+]',
                                                df_listing_final['日均销量分段'])

    # df_listing_final.to_excel('df_listing_final.xlsx',index=0)

    return df_listing_final

def write_to_sql(df):
    """
    将中间表数据写入mysql
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = datetime.datetime.now().strftime('%Y%m%d')
    df = df.reset_index(drop=True)
    site_list = df['site2'].unique()
    for i in site_list:
        df_temp = df[df['site2'] == i]
        table_name = f'price_ratio_analysis_{date_today}_{i}'
        conn.to_sql(df_temp, table_name, if_exists='replace')
        print(f'{i}价差分析数据存表完成, 表名{table_name}.')
    # sql = f"""
    # delete from {table_name}
    # """
    # conn.execute(sql)
    conn.close()

def main():
    t1 = time.time()
    df1 = dwm_data(get_listing('泛欧'))
    t2 = time.time()
    print(df1.info())
    print(f"数据计算完成！共耗时{'%.0f' % (t2-t1)}s")
    write_to_sql(df1)
    t3 = time.time()
    print(f"数据存储完成！共耗时{'%.0f' % (t3-t2)}s")

    t1 = time.time()
    df2 = dwm_data(get_listing('非泛欧'))
    t2 = time.time()
    print(df2.info())
    print(f"数据计算完成！共耗时{'%.0f' % (t2-t1)}s")
    write_to_sql(df2)
    t3 = time.time()
    print(f"数据存储完成！共耗时{'%.0f' % (t3-t2)}s")

##
# 链接维度定价比计算
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
##
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
    ##
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
    ##
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
        col = ['sku','site','站点','country_code','cost','fba头程属性']
        df_listing = df_listing[col]
        df_listing = df_listing.drop_duplicates(subset=['sku','site'], keep='first')
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
        df_fast = df_fast[['sku','site','头程费_人民币']]
        df_fast.columns = ['sku','site','头程费快运']
        #
        df_slow = tou_cheng_dingjiabi(df_listing, conn, dingjia_type='慢运')
        df_slow = df_slow[['sku','site','头程费_人民币']]
        df_slow.columns = ['sku','site','头程费慢运']
        #
        # FBM运费
        df_fbm = df_listing.copy()
        # df_fbm['站点'] = df_fbm['站点FBM']
        df_fbm['发货仓库'] = None
        df_fbm['物流类型'] = None
        df_jie_kou = freight_interface_fu(df_fbm, table_name='freight_interface_amazon')
        df_jie_kou = df_jie_kou.sort_values(by=['sku','站点','warehouse_id','ship_name'], ascending=[True,True,True,True])
        df_jie_kou = df_jie_kou.drop_duplicates(subset=['sku','站点'], keep='first')
        df_jie_kou = df_jie_kou.rename(columns={'运费':'fbm运费'})
        #
        df_listing_temp.rename(columns={"sku1": "sku"}, inplace=True)
        df_result = pd.merge(df_listing_temp, df_fast, how='left', on=['sku','site'])
        df_result = pd.merge(df_result, df_slow, how='left', on=['sku','site'])
        df_result = pd.merge(df_result, df_jie_kou[['sku','站点', 'fbm运费']], how='left', on=['sku','站点'])
        #
        # 差值
        df_diff = get_diff()
        #
        df_diff_fba = df_diff[(df_diff['shipping_type'] == 3)][
            ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
        df_diff_fba = df_diff_fba.rename(columns={'net_profit2':'fba_diff','net_interest_rate_target':'FBA_profit'})
        df_diff_fba[['fba_diff','FBA_profit']] = (df_diff_fba[['fba_diff','FBA_profit']]/100).astype(float)
        df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
            ['site', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
        df_diff_fbm = df_diff_fbm.rename(columns={'net_profit2':'fbm_diff','net_interest_rate_target':'FBM_profit'})
        df_diff_fbm[['fbm_diff','FBM_profit']] = (df_diff_fbm[['fbm_diff','FBM_profit']]/100).astype(float)
        #
        df_result = merge_first_product_line(df_result)
        df_result = get_cost_range(df_result, df_diff_fba)
        df_result = merge_four_dim_diff(df_result, df_diff_fba, ['site', 'first_product_line', 'cost_range'])
        #
        df_result.drop('cost_range', axis=1, inplace=True)
        df_result = get_cost_range(df_result, df_diff_fbm)
        df_result = merge_four_dim_diff(df_result, df_diff_fbm, ['site', 'first_product_line', 'cost_range'])

        #
        df_result.drop('cost_range', axis=1, inplace=True)
        col = ['fba_diff','FBA_profit','fbm_diff','FBM_profit']
        df_result[col] = df_result[col].astype(float)
        #
        df_result = shuilv(df_result, fb_type='FBA')
        df_result = df_result.rename(columns={'税率':'FBA税率'})
        df_result = shuilv(df_result, fb_type='FBM')
        df_result = df_result.rename(columns={'税率':'FBM税率'})
        #
        # 计算定价
        print('开始计算定价比...')
        df_result = get_price(df_result)
        #
        ratio_limit = 1.5
        col = ['英国','德国','法国','西班牙','意大利','美国','加拿大','墨西哥','澳大利亚']
        c1 = (df_result['site'].isin(col)) & (df_result['fast_price_ratio'] <= ratio_limit)
        c2 = (df_result['site'].isin(['波兰','日本','阿联酋','沙特','新加坡'])) & (df_result['fast_price_ratio'] <= 2)
        df_result['适合快运连续天数'] = np.select([c1, c2],[1, 1], 0)
        #
        c1 = (df_result['site'].isin(col)) & (df_result['slow_price_ratio'] <= ratio_limit)
        c2 = (df_result['site'].isin(['波兰','日本','阿联酋','沙特','新加坡'])) & (df_result['slow_price_ratio'] <= 2)
        df_result['适合慢运连续天数'] = np.select([c1, c2],[1, 1], 0)
        #
        # 建议物流方式
        ratio_limit = 1.5
        c1 = (df_result['site'].isin(['英国','德国','法国','西班牙','意大利'])) & (df_result['fast_price_ratio']<=ratio_limit) & \
             (df_result['slow_price_ratio']<=ratio_limit) & (df_result['fast_and_slow_rate_diff']<=0.01)
        c2 = (df_result['site'].isin(['美国','加拿大','墨西哥','澳大利亚'])) & (df_result['fast_price_ratio']<=ratio_limit) & \
             (df_result['slow_price_ratio']<=ratio_limit) & (df_result['fast_and_slow_rate_diff']<=0.01)
        c3 = (df_result['site'].isin(['阿联酋'])) & (df_result['fast_price_ratio']<=2) & \
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

        df_result['proposed_transport'] = np.select([c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17],
                                                    ['15','6','6','6','13','16;15','11;6','12,6','12;6','16','11','12','12','0','0','0','0'])
        #
        df_result['date'] = time.strftime('%Y-%m-%d')
        # 取前一天的定价比数据表
        sql = f"""
            SELECT account_id, seller_sku, sku,  `适合快运连续天数` `适合快运连续天数_前一日`, `适合慢运连续天数` `适合慢运连续天数_前一日`
            FROM support_document.{df_table.iloc[0, 0]}
            WHERE account_id in {tuple(df_account_all['account_id'].unique())} and site in {site}
        """
        df_result_y = conn_mx2.ck_select_to_df(sql)
        df_result = pd.merge(df_result, df_result_y, how='left', on=['account_id', 'seller_sku', 'sku'])
        df_result[['适合快运连续天数_前一日', '适合慢运连续天数_前一日']] = df_result[
            ['适合快运连续天数_前一日', '适合慢运连续天数_前一日']].fillna(0).astype(int)
        df_result['适合快运连续天数'] = df_result['适合快运连续天数'] + df_result['适合快运连续天数'] * \
                                          df_result['适合快运连续天数_前一日']
        df_result['适合慢运连续天数'] = df_result['适合慢运连续天数'] + df_result['适合慢运连续天数'] * \
                                          df_result['适合慢运连续天数_前一日']
        # df_result.drop(['适合快运连续天数_前一日', '适合慢运连续天数_前一日'], axis=1, inplace=True)
        col = ['account_id', 'asin', 'seller_sku','fbm_seller','sku','site','头程费快运','头程费慢运','fbm运费','fba_fast_price_rmb',
               'fba_slow_price_rmb','fbm_price_rmb','fast_price_ratio','slow_price_ratio','fast_and_slow_rate_diff',
               '适合快运连续天数','适合慢运连续天数','proposed_transport', 'date']

        # df_result_all = pd.concat([df_result, df_result_all])
        #
        conn_mx2.write_to_ck_json_type(df_result[col], table_name)
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='support_document')
        # ck_client.write_to_ck_json_type(table_name, df_result[col])

##
sql = f"""
    SELECT account_id, seller_sku, sku,  `适合快运连续天数` `适合快运连续天数_前一日`, `适合慢运连续天数` `适合慢运连续天数_前一日`
    FROM support_document.pricing_ratio_listing_20240305
    WHERE  site in '土耳其'
    LIMIT 10
"""
conn_mx2 = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
df_result_y = conn_mx2.ck_select_to_df(sql)
##
if len(df_result_y) == 0:
    df_result_y = pd.DataFrame(columns=['account_id', 'seller_sku', 'sku'])
##
df_temp = pd.DataFrame(columns=['account_id', 'seller_sku'], data=[[1,2],[2,3]])
##
df_temp_0 = pd.merge(df_temp, df_result_y, how='left', on=['account_id'])
##
sql = """
    SELECT *
    FROM yibai_sale_center_system.yibai_system_account
    LIMIT 10
    
"""
conn_mx2 = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
df_cd_account = conn_mx2.ck_select_to_df(sql)
##
sql = """
    SELECT x.account_id account_id, a.delivery_country delivery_country
    FROM yibai_sale_center_system.yibai_system_account x
    INNER JOIN yibai_sale_center_system.yibai_system_auth_account a
    ON a.account_id = x.id
    WHERE platform_code  = 'CDISCOUNT' and delivery_country !='CN'
"""
conn_mx2 = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
df_cd_account_0 = conn_mx2.ck_select_to_df(sql)
##
df_cd_account_0.to_excel('df_cd_account.xlsx', index=0)
##
if __name__ == '__main__':
    # main()
    pricing_ratio_listing()
    print('done.')
    # df_result.to_excel('df_result.xlsx', index=0)