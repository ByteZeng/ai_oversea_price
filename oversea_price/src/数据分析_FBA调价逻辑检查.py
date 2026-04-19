import warnings

import pandas as pd
from tqdm import tqdm

from pulic_func.base_api.mysql_connect import pd_to_ck
from utils import utils
from utils.utils import make_path, save_df, read_df
import datetime,time
import os

warnings.filterwarnings('ignore')


def main():
    """
    support_document.yibai_amazon_listings_all_raw2_unmatched 表中FBA抽查最近调价情况
    """
    utils.program_name = '数据分析_FBA调价逻辑检查'
    make_path()
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')

    sql = f"""
        SELECT x.* FROM support_document.yibai_amazon_listings_all_raw2_unmatched x
        WHERE fulfillment_channel ='AMA'
        limit 10
    """
    df = conn_mx.ck_select_to_df(sql)

    print('main done!')

    return df


def check(df):
    """
    FBA 抽查最近调价记录
    """
    utils.program_name = '数据分析_FBA调价逻辑检查'
    make_path()
    utils.program_name = '数据分析_FBA调价逻辑检查/抽查/'
    make_path()

    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='抽查'):
        account_id = row['account_id']
        seller_sku = row['seller_sku']
        utils.program_name = f'数据分析_FBA调价逻辑检查/抽查/{account_id}'
        make_path()

        # 存档库
        conn_mx2 = pd_to_ck(database='support_document', data_sys='ck存档库')
        sql = f"""
            select distinct table from system.parts
            where database = 'fba_ag_over_180_days_price_adjustment'
            and table like 'fba_price_adjustment_fba_%'
            order by table desc
            limit 30
        """
        df_table = conn_mx2.ck_select_to_df(sql)

        df1 = pd.DataFrame()
        for index, row in tqdm(df_table.iterrows(), total=df_table.shape[0], desc='存档库'):
            sql = f"""
                SELECT account_id, seller_sku, your_price, Results_price `目标售价`, lowest_price `销毁价` , 
                Current_net_rate `当前售价净利润率`, result_profit_net_rate `目标售价净利润率`, 
                Price_adjustment_priority `调价优先级`, meet_criteria `符合条件`,
                adjustment_priority `之前的销售状态`, Average_daily_sales `日均销量`
                FROM fba_ag_over_180_days_price_adjustment.{row['table']}
                WHERE seller_sku = \'{seller_sku}\'
                and account_id = {account_id}
            """
            try:
                df_temp = conn_mx2.ck_select_to_df(sql)
                df_temp['date'] = pd.to_datetime(row['table'].split('_')[-1])
                df1 = df_temp if df1.empty else pd.concat([df1, df_temp], ignore_index=True)
            except:
                pass

        # 近段时间
        conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
        sql = f"""
            select distinct table from system.parts
            where database = 'fba_ag_over_180_days_price_adjustment'
            and table like 'fba_price_adjustment_fba_%'
            order by table
        """
        df_table = conn_mx.ck_select_to_df(sql)

        df2 = pd.DataFrame()
        for index, row in tqdm(df_table.iterrows(), total=df_table.shape[0], desc='近段时间'):
            sql = f"""
                SELECT account_id, seller_sku, your_price, Results_price `目标售价`, lowest_price `销毁价` , 
                Current_net_rate `当前售价净利润率`, result_profit_net_rate `目标售价净利润率`, 
                Price_adjustment_priority `调价优先级`, meet_criteria `符合条件`,
                adjustment_priority `之前的销售状态`, Average_daily_sales `日均销量`
                FROM fba_ag_over_180_days_price_adjustment.{row['table']}
                WHERE seller_sku = \'{seller_sku}\'
                and account_id = {account_id}
            """
            try:
                df_temp = conn_mx.ck_select_to_df(sql)
                df_temp['date'] = pd.to_datetime(row['table'].split('_')[-1])
                df2 = df_temp if df2.empty else pd.concat([df2, df_temp], ignore_index=True)
            except:
                pass

        df3 = pd.concat([df1, df2], ignore_index=True).sort_values(['date']).reset_index(drop=True)
        save_df(df3, f'/抽查_{seller_sku}', file_type='xlsx')
    print('done')


def select_by_account_id(account_id):
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')

    sql = f"""
        SELECT x.* FROM support_document.yibai_amazon_listings_all_raw2_unmatched x
        WHERE fulfillment_channel ='AMA'
        and account_id = {account_id}
    """
    df = conn_mx.ck_select_to_df(sql)

    print('main done!')

    return df


def select_by_listing(account_id, seller_sku):
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')

    sql = f"""
        SELECT x.* FROM support_document.yibai_amazon_listings_all_raw2_unmatched x
        WHERE fulfillment_channel ='AMA'
        and account_id = {account_id} and seller_sku = \'{seller_sku}\'
    """
    df = conn_mx.ck_select_to_df(sql)

    print('main done!')

    return df


def select_by_account_id_filtered(account_id, site):
    utils.program_name = '数据分析_FBA调价逻辑检查'
    make_path()
    utils.program_name = '数据分析_FBA调价逻辑检查/抽查/'
    make_path()
    utils.program_name = f'数据分析_FBA调价逻辑检查/抽查/{site}'
    make_path()
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date = datetime.date.today() - pd.Timedelta(days=1)
    date1 = date.strftime('%Y%m%d')

    sql = f"""
        SELECT a.account_id account_id, a.seller_sku seller_sku, a.sku sku,
        a.asin asin, a.your_price your_price, a.fba_fees fba_fees,
        a.`当前售价利润率-净利润率` `当前售价利润率-净利润率`, a.`价格状态` `价格状态`, 
        a.`销售状态` `销售状态`, a.`不调价原因` `不调价原因`, 
        b.site site, c.min_adjustment min_adjustment, 
        b.`meet_criteria` meet_criteria, b.`tar_priority` tar_priority, 
        b.your_price `调价前价格`, b.`Target_profit_price` Target_profit_price, b.`Results_price` Results_price, 
        b.`Current_net_rate` Current_net_rate, 
        b.`tar_profit` - fba_diff  tar_net_profit, b.`result_profit_net_rate` result_profit_net_rate, 
        b.`fba_fees` fba_fees0, b.`fba_fees1` fba_fees1
        FROM (
            select * from support_document.yibai_amazon_listings_all_raw2_unmatched
            WHERE fulfillment_channel ='AMA'
            and account_id = {account_id}
            and `不调价原因` = ''
        ) a 
        left join (
            select * from fba_ag_over_180_days_price_adjustment.fba_price_adjustment_fba_{date1}
            WHERE account_id = {account_id}
        ) b
        on a.account_id=b.account_id and a.seller_sku=b.seller_sku
        left join (
            select * from domestic_warehouse_clear.site_table
        ) c
        on b.site=c.site1
"""
    df = conn_mx.ck_select_to_df(sql)

    if df.empty:
        pass
    else:
        df['低费率阈值'] = 0
        df.loc[df['site'] == '美国', '低费率阈值'] = 10
        df.loc[df['site'] == '英国', '低费率阈值'] = 10
        df.loc[df['site'] == '德国', '低费率阈值'] = 11
        df.loc[df['site'] == '法国', '低费率阈值'] = 12
        df.loc[df['site'] == '意大利', '低费率阈值'] = 12
        df.loc[df['site'] == '西班牙', '低费率阈值'] = 12
        df.loc[df['site'] == '荷兰', '低费率阈值'] = 12
        df.loc[df['site'] == '瑞典', '低费率阈值'] = 140
        df.loc[df['site'] == '波兰', '低费率阈值'] = 55
        df = df[df['your_price'] > df['低费率阈值']]

        df['result_profit_net_rate'] = df['result_profit_net_rate'].round(2)
        df['min_adjustment'] = df['min_adjustment'].round(2)

        df['raw2价格和调价前价格是否匹配'] = (df['your_price'] == df['调价前价格']).replace({True: '是', False: '否'})
        df['是否调价'] = (df['调价前价格'] != df['Results_price']).replace({True: '是', False: '否'})
        df['调价价差是否大于站点最低调价幅度'] = (
                (df['Results_price'] - df['调价前价格']).abs() >= df['min_adjustment'] - 0.00000001).replace(
            {True: '是', False: '否'})
        df['调价是否修正'] = (
                ((df['销售状态'] == '负利润加快动销') & (df['当前售价利润率-净利润率'] > 0) & (
                        df['result_profit_net_rate'] < 0)) |
                (((df['销售状态'] == '正常') | (df['销售状态'] == '正利润加快动销')) & (
                        df['当前售价利润率-净利润率'] < 0) & (
                         df['result_profit_net_rate'] >= 0))
        ).replace({True: '是', False: '否'})
        df['调价两次使用fba_fees是否一致'] = (df['fba_fees0'] == df['fba_fees1']).replace({True: '是', False: '否'})

        df = df[df['调价是否修正'] == '否']
        df = df[df['调价价差是否大于站点最低调价幅度'] == '否']
        if not df.empty:
            save_df(df, f'/抽查_{account_id}', file_type='xlsx')
        else:
            pass


if __name__ == '__main__':
    # df = main()
    # df = select_by_listing(1240, 'THU-GS07762THUvKkW-17-FBA')

    utils.program_name = '数据分析_FBA调价逻辑检查'
    time_today = time.strftime('%Y-%m-%d')
    f_path = os.path.join('F:\\yibai-price-strategy\\data', time_today)
    file_name = os.path.join(f_path, f'数据分析_FBA账号调价成功率统计\\数据分析_FBA账号调价成功率统计20231027.xlsx')
    df = pd.read_excel(file_name)
    # df = read_df('数据分析_FBA账号调价成功率统计20231027', file_type='xlsx')
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        select_by_account_id_filtered(row['account_id'], row['site'])
    print('done')

    # check(df)
