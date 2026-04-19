import time
import warnings
import datetime
import pandas as pd

from pulic_func.base_api.mysql_connect import pd_to_ck
from utils import utils
from utils.utils import make_path, save_df, read_df

warnings.filterwarnings('ignore')


def get_listing_by_age():
    sql = """
    select 
        account_id, itemid, seller_sku, sku1 sku, age_1_30 + age_30_60 + age_60_90 as `不超90天库龄库存`,
        age_270_300 + age_300_330 + age_330_360 + age_360_plus as `超270天库龄库存`, 
        age_360_plus `超360天库龄库存`
    from domestic_warehouse_clear.fbc_age1
    where record_date = (
        select max(record_date) from domestic_warehouse_clear.fbc_age1
    )
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df = conn_mx.ck_select_to_df(sql)
    df = df[(df['不超90天库龄库存'] == 0) & (df['超360天库龄库存'] > 0) & (df['超270天库龄库存'] > 0)]
    return df


def get_listing_by_destroy_time():
    date = datetime.date.today() - pd.Timedelta(days=14)
    conn_mx = pd_to_ck(database='fba_ag_over_180_days_price_adjustment', data_sys='调价明细历史数据')
    # sql = f"""
    #     select distinct table from system.parts
    #     where database = 'fba_ag_over_180_days_price_adjustment'
    #     and table like 'fbc_price_adjust_%'
    #     order by table desc
    #     limit 1
    # """
    # df_table = conn_mx.ck_select_to_df(sql)
    # print(df_table.info())
    # table = df_table['table'].values[0]
    date_today = time.strftime('%Y%m%d')
    date_today = '20240927'
    table = f'fbc_price_adjust_{date_today}'
    sql = f"""
    select 
        a.account_id account_id, a.seller_sku seller_sku, a.itemid itemid, a.start_time `抵达销毁价开始时间`,
        b.`在库库存可售天数` `在库库存可售天数`, b.`是否停售` `是否停售`, b.`成本` `成本`,
        b.`目标售价` `目标售价`, b.`销毁价` `销毁价`
        from 
        (
            select * from yibai_fba.cd_fbc_clear_destroy_time
            where start_time <= \'{date}\'
        ) a
        left join (
            select * from fba_ag_over_180_days_price_adjustment.{table}
        ) b
        on a.account_id=b.account_id and a.seller_sku=b.seller_sku and a.itemid=b.itemid
    """
    df = conn_mx.ck_select_to_df(sql)
    print(df.info())
    df = df[(df['在库库存可售天数'] > 30) & (df['是否停售'] == '否')]
    df['已达销毁价天数'] = (pd.to_datetime(datetime.date.today()) - pd.to_datetime(
        df['抵达销毁价开始时间'])).dt.days + 1

    return df


def main():
    """
    经济型销毁_CD
    筛选条件：
    1、不存在不超90天库龄库存
    2、存在超270天库龄库存
    3、已达到销毁价15天
    4、在库库存预计可售天数大于30天
    5、链接可售
    # 6、不包含冬季产品

    2023-10-11
    发现问题：销售对应该销毁的商品涨价的行为会使得通过 domestic_warehouse_clear.cd_fbc_clear_destroy_time 表去获取到达销毁价信息不准确
    已反馈给 李航 经理规避这种情况的发生。
    """
    utils.program_name = '经济型销毁_CD'
    make_path()
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')

    date = datetime.date.today()
    df1 = get_listing_by_age()
    df2 = get_listing_by_destroy_time()
    df3 = df2.merge(df1, on=['account_id', 'seller_sku', 'itemid'], how='inner')

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

    print(f'{date.year:04d}-{date.month:02d}-{date.day:02d} CD经济型销毁数据统计：')
    print(f"共计 {df5.shape[0]} 个sku，{df5['超270天库龄库存'].sum()} PCS，"
          f"销毁金额 {round(df5['销毁金额'].sum() / 10000, 2)} W")
    print("""
    筛选条件：
    1、不存在不超90天库龄库存
    2、存在超270天库龄库存
    3、已达到销毁价15天
    4、在库库存预计可售天数大于30天
    5、链接可售
    # 6、不包含冬季产品
    """)

    save_df(df5, f'{utils.program_name}{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')

    # last_destroy_file_name = '经济型销毁raw_CD20230907'
    # list_col_names = ['account_id', 'seller_sku', 'itemid', '超270天库龄库存', '销毁金额']
    # df6 = read_df(last_destroy_file_name, file_type='xlsx')
    # df7 = df6[list_col_names].merge(df5[list_col_names], on=['account_id', 'seller_sku', 'itemid'], how='left',
    #                                 suffixes=('_上次', '_本次'))
    # df7['上次是否完成销毁'] = pd.isna(df7['销毁金额_本次']).replace({True: '是', False: '否'})
    # save_df(df7, f'上次销毁情况统计_CD{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')
    #
    # print(f"上次销毁情况统计："
    #       f"\n上次销毁数据统计时间：{last_destroy_file_name.split('D')[1]}，"
    #       f"本次销毁统计时间：{date.year:04d}{date.month:02d}{date.day:02d}"
    #       f"\n上次应该销毁 {df6.shape[0]} 个sku，{df6['超270天库龄库存'].sum()} PCS，"
    #       f"销毁金额 {round(df6['销毁金额'].sum() / 10000, 2)} W，"
    #       f"实际销毁 {df7[df7['上次是否完成销毁']=='是'].shape[0]} 个sku，"
    #       f"\n剩余仍待销毁 {df7[df7['上次是否完成销毁']=='否'].shape[0]} 个sku，"
    #       f"{df7[df7['上次是否完成销毁']=='否']['超270天库龄库存_本次'].astype('int').sum()} PCS，"
    #       f"销毁金额 {round(df7[df7['上次是否完成销毁']=='否']['销毁金额_本次'].sum() / 10000, 2)} W")

    print('done!')


if __name__ == '__main__':
    main()
