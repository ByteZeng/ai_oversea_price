import warnings
import datetime
import pandas as pd

from pulic_func.base_api.mysql_connect import pd_to_ck
from utils import utils
from utils.utils import make_path, save_df, read_df

warnings.filterwarnings('ignore')


def get_listing_by_age():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据2')
    sql = f"""
        select distinct table from system.parts
        where database = 'fba_ag_over_180_days_price_adjustment'
        and table like 'wfs_price_adjust_%'
        order by table desc
        limit 1
    """
    df_table = conn_mx.ck_select_to_df(sql)
    table = df_table['table'].values[0]
    sql = f"""
    select 
        account_id, seller_sku, sku, ats_0_90_days as `不超90天库龄库存`,
        `超270天库存` as `超270天库龄库存`, 
        `ats_365_days` `超365天库龄库存`
    from fba_ag_over_180_days_price_adjustment.{table}
    """
    df = conn_mx.ck_select_to_df(sql)
    df = df[(df['不超90天库龄库存'] == 0) & (df['超270天库龄库存'] > 0)]
    return df, table


def get_listing_by_destroy_time(table):
    date = datetime.date.today() - pd.Timedelta(days=6)
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据2')
    sql = f"""
        select 
            a.account_id account_id, a.seller_sku seller_sku, a.start_time `抵达销毁价开始时间`,
            b.`在库库存可售天数` `在库库存可售天数`, b.`是否停售` `是否停售`, b.`成本` `成本`,
            b.`目标售价` `目标售价`, b.`销毁价` `销毁价`
            from 
            (
                select * from domestic_warehouse_clear.wfs_clear_destroy_time
                where start_time <= \'{date}\'
            ) a
            left join (
                select * from fba_ag_over_180_days_price_adjustment.{table}
            ) b
            on a.account_id=b.account_id and a.seller_sku=b.seller_sku
        """
    df = conn_mx.ck_select_to_df(sql)
    df = df[(df['在库库存可售天数'] > 30) & (df['是否停售'] == '否')]
    df['已达销毁价天数'] = (pd.to_datetime(datetime.date.today()) - pd.to_datetime(df['抵达销毁价开始时间'])).dt.days + 1
    return df


def main():
    """
    经济型销毁_Walmart
    筛选条件：
    1、不存在不超90天库龄库存
    2、存在超270天库龄库存
    3、已达到销毁价15天
    4、在库库存预计可售天数大于30天
    5、链接可售
    # 6、不包含冬季产品
    """
    utils.program_name = '经济型销毁_Walmart'
    make_path()
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')

    date = datetime.date.today()
    df1, table = get_listing_by_age()
    df2 = get_listing_by_destroy_time(table)
    df3 = df2.merge(df1, on=['account_id', 'seller_sku'], how='inner')

    # 筛选非冬季 - 冬季feature_val=4
    sql = f"""
        SELECT
            sku
        FROM yibai_prod_base_sync.yibai_prod_sku
        WHERE feature_val like \'%4%\'
    """
    # df4 = conn_mx.ck_select_to_df(sql)
    # df4['是否冬季'] = '是'
    # df3 = df3.merge(df4, on=['sku'], how='left')
    # df3['是否冬季'] = df3['是否冬季'].fillna('否')
    # df5 = df3[df3['是否冬季'] == '否']

    df5 = df3.copy()
    df5['销毁金额'] = df5['成本'] * df5['超270天库龄库存']

    print(f'{date.year:04d}-{date.month:02d}-{date.day:02d} Walmart经济型销毁数据统计：')
    print(f"共计{df5.shape[0]}个sku，{df5['超270天库龄库存'].sum()}PCS，"
          f"销毁金额{round(df5['销毁金额'].sum() / 10000, 2)}W")
    print("""
    筛选条件：
    1、不存在不超90天库龄库存
    2、存在超270天库龄库存
    3、已达到销毁价15天
    4、在库库存预计可售天数大于30天
    5、链接可售
    6、不包含冬季产品
    """)

    save_df(df5, f'{utils.program_name}{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')

    # last_destroy_file_name = '经济型销毁_Walmart_raw20230907'
    # list_col_names = ['account_id', 'seller_sku', '超270天库龄库存', '销毁金额']
    # df6 = read_df(last_destroy_file_name, file_type='xlsx')
    # df7 = df6[list_col_names].merge(df5[list_col_names], on=['account_id', 'seller_sku'], how='left',
    #                                 suffixes=('_上次', '_本次'))
    # df7['上次是否完成销毁'] = pd.isna(df7['销毁金额_本次']).replace({True: '是', False: '否'})
    # save_df(df7, f'上次销毁情况统计_Walmart{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')
    #
    # print(f"{date.year:04d}-{date.month:02d}-{date.day:02d} Walmart上次销毁情况统计："
    #       f"\n上次销毁数据统计时间：{last_destroy_file_name.split('raw')[1]}，"
    #       f"本次销毁统计时间：{date.year:04d}{date.month:02d}{date.day:02d}"
    #       f"\n上次应该销毁 {df6.shape[0]} 个sku，{df6['超270天库龄库存'].sum()} PCS，"
    #       f"销毁金额 {round(df6['销毁金额'].sum() / 10000, 2)} W，"
    #       f"实际销毁 {df7[df7['上次是否完成销毁'] == '是'].shape[0]} 个sku，"
    #       f"\n剩余仍待销毁 {df7[df7['上次是否完成销毁'] == '否'].shape[0]} 个sku，"
    #       f"{df7[df7['上次是否完成销毁'] == '否']['超270天库龄库存_本次'].astype('int').sum()} PCS，"
    #       f"销毁金额 {round(df7[df7['上次是否完成销毁'] == '否']['销毁金额_本次'].sum() / 10000, 2)} W")

    print('done!')


if __name__ == '__main__':
    main()
