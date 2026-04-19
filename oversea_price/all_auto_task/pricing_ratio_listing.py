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
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from pulic_func.base_api.all_freight_interface import *
from pulic_func.base_api.adjust_price_function_amazon import shuilv,fanou_fanmei

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
                LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon c
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
                LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon c
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
                case when r.commission_rate is Null then 0.15 else r.commission_rate end commission_rate, f.FBM_difference,f.FBM_profit, f.FBA_profit,
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
                SELECT account_id, seller_sku, `佣金率` commission_rate
                FROM yibai_fba.yibai_amazon_referral_fee
                WHERE account_id in {account_tuple} 
            ) r
            ON f.account_id = r.account_id and f.seller_sku = r.seller_sku
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
##
def get_linelist(df):
    sql = """select distinct a.sku as sku,c.category_name as `一级产品线`
                from yibai_prod_base_sync.yibai_prod_sku a 
                left join yibai_prod_base_sync.yibai_prod_category b 
                on a.product_category_id=b.id
                left join yibai_prod_base_sync.yibai_prod_category c 
                on b.category_id_level_1=c.id"""
    conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
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
    left join domestic_warehouse_clear.yibai_site_table_amazon c on a.site = UPPER(c.site) 
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

##
def get_price(df):
    df['fba_ky_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_kongyun'] + 2
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBA_profit'] - df[
        '税率'] - 0.04)
    df['fba_hy_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_haiyun'] + 2
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBA_profit'] - df[
        '税率'] - 0.04)
    df['fba_tl_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_tl'] + 2
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBA_profit'] - df[
        '税率'] - 0.04)
    df['fba_kh_price_rmb'] = (df['成本'] + df['fba_fees_rmb'] + df['toucheng_kh'] + 2
                              ) / (1 - df['commission_rate'] - df['毛净利差值'] - df['FBA_profit'] - df[
        '税率'] - 0.04)
    df['fbm_price_rmb'] = (df['成本'] + df['fbm_fees']) / (
                1 - 0.15 - 0.04 - df['毛净利差值'] - df['FBM_profit'] - df['税率'])
    df[['fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb', 'fba_kh_price_rmb','fbm_price_rmb']] = df[
        ['fba_ky_price_rmb', 'fba_hy_price_rmb', 'fba_tl_price_rmb','fba_kh_price_rmb', 'fbm_price_rmb']].astype(float).round(2)
    return df

##
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
##
def dwm_data(df_listing):
    """
    计算定价比
    """
    #
    # (product_cost)+(fba_fees)*rate+(first_trip_fee_rmb)+2)/(1-commission_rate-0.04-FBA_difference-FBA_profit-tax_rate)
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
        FROM domestic_warehouse_clear.yibai_monitor_fba_order
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
def get_listing_data(site='泛欧'):
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
                LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon c
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
                LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon c
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
    account_list = [df_account[i:i + step] for i in range(0, int(len(df_account)/100), step)]
    #
    df_listing = pd.DataFrame()
    time1 = time.time()
    for a in account_list:
        account_tuple = tuple(a['account_id'].unique())
        sql = f"""
            SELECT 
                f.account_id, f.asin, f.seller_sku as seller_sku, a.seller_sku as fbm_seller, e.sku, f.site, f.site2, 
                case when r.commission_rate is Null then 0.15 else r.commission_rate end commission_rate, f.FBM_difference,f.FBM_profit, f.FBA_profit,
                cost, first_trip_source, first_trip_fee_rmb, toucheng_kongyun,toucheng_haiyun,toucheng_tl,
                toucheng_kh,e.fba_fees_rmb
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
                LEFT JOIN domestic_warehouse_clear.yibai_site_table_amazon c
                ON b.site = c.site
                WHERE a.fulfillment_channel = 'AMA' and a.account_id in {account_tuple}
            ) f
            LEFT JOIN (
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
                SELECT account_id, seller_sku, `佣金率` commission_rate
                FROM yibai_fba.yibai_amazon_referral_fee
                WHERE account_id in {account_tuple} 
            ) r
            ON f.account_id = r.account_id and f.seller_sku = r.seller_sku
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        df_listing = pd.concat([df_temp, df_listing])
    df_listing.columns = [i.split('.')[-1] for i in df_listing.columns]
    time2 = time.time()
    t = time2 - time1
    print(f"FBA链接信息获取完成. 共耗时{'%.0f' % t}s")
    return df_listing
# ##
# df_listing_temp = get_listing_data('泛欧')
# ##
# # 未匹配到运费数据的链接情况。（是否匹配定价比程序的数据？）
# df_no_fee = df_listing_temp[(df_listing_temp['sku']!='') & (df_listing_temp['first_trip_source']=='')]
# print(f'有SKU信息，但未匹配到FBA运费的链接共{len(df_no_fee)}条')
# ##
# df_listing_temp.to_excel('df_listing_temp.xlsx',index=0)
##
if __name__ == '__main__':
    main()