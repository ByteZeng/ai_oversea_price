import warnings

import pandas as pd
from tqdm import tqdm

from pulic_func.base_api.base_function import mysql_escape
from pulic_func.base_api.mysql_connect import pd_to_ck
from utils import utils
from utils.utils import make_path, save_df
import datetime

warnings.filterwarnings('ignore')


def main():
    """

    """
    utils.program_name = '数据分析_FBA账号调价成功率统计'
    make_path()
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date = datetime.date.today()
    date1 = (date - pd.Timedelta(days=1)).strftime('%Y%m%d')

    sql = """
        select a.id as account_id,b.site1 as `站点`
        from yibai_system_kd_sync.yibai_amazon_account a 
        left join domestic_warehouse_clear.site_table b 
        on a.site =b.site 
        inner join (
            SELECT distinct account_id
            from yibai_product_kd_sync.yibai_amazon_listings_all_raw2
            WHERE status = '1'
        ) c 
        on toInt32(a.id) = c.account_id
        order by `站点`, account_id
    """
    df_site = conn_mx.ck_select_to_df(sql)
    df = pd.DataFrame()
    for key, group in tqdm(df_site.groupby(['站点'])):
        print(f'站点：{key}')
        id_list = mysql_escape(group, 'account_id', 1)
        sql = f"""
        select account_id, site, sum(a.`成功`) `成功数量`, sum(a.`失败`) `失败数量`, count(a.`成功`)-sum(a.`成功`)-sum(a.`失败`) `其他数量`
        from (
            select 
                aa.account_id account_id, aa.seller_sku seller_sku, aa.site site,
                case when aa.`调价后价格`=bb.`最新价格`  then 1 else 0 end as `成功`,
                case when aa.`调价前价格`=bb.`最新价格`  then 1 else 0 end as `失败`
            from (
                select account_id, seller_sku, site, toFloat64(your_price) `调价前价格`, toFloat64(Results_price) `调价后价格`
                from fba_ag_over_180_days_price_adjustment.fba_price_adjustment_fba_{date1}
                where toFloat64(your_price) != toFloat64(Results_price)
                and toInt64(account_id) in ({id_list}) 
            ) aa 
            inner join (
                select account_id, seller_sku, toFloat64(price) `最新价格`
                from yibai_product_kd_sync.yibai_amazon_listings_all_raw2
                where toInt64(account_id) in ({id_list}) 
            ) bb on aa.account_id = bb.account_id and aa.seller_sku = bb.seller_sku 
        ) a
        group by account_id, site
        """
        df_temp = conn_mx.ck_select_to_df(sql)
        if df.empty:
            df = df_temp
        else:
            df = pd.concat([df, df_temp], ignore_index=True)
    df['成功数量是否为0'] = (df['成功数量'] == 0).replace({True: '是', False: '否'})
    save_df(df, f'{utils.program_name}{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')

    print('done!')


if __name__ == '__main__':
    main()
