import time
import warnings

import pandas as pd
from tqdm import tqdm
from pulic_func.base_api.base_function import mysql_escape
from pulic_func.base_api.mysql_connect import pd_to_ck
import datetime

warnings.filterwarnings('ignore')


def yibai_amazon_listings_all_raw2_unmatched():
    """
    盘一下 raw2 表的链接  看下 当前售价与销售状态不符合的数据
    售价 正净利 -销售状态非正常/正利润加快动销
    售价 负净利--销售状态正常/正利润加快动销

    1、筛选链接状态可售的
    2、添加不调价信息 yibai_product_kd_sync.yibai_amazon_adjustprice_filter_sku
    """
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')

    # date
    sql = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'no_adjust_reason_listing_%'
        order by table desc
        limit 1
    """
    df_table = conn_mx.ck_select_to_df(sql)
    table = df_table['table'].values[0]
    date1 = table.split('_')[-1]
    date = pd.to_datetime(date1)

    # 结果表准备
    table_name = f'yibai_amazon_listings_all_raw2_unmatched'
    sql = f"""
        DROP TABLE IF EXISTS support_document.{table_name}
    """
    conn_mx.ck_execute_sql(sql)
    print('结果表删除成功！')
    sql = f"""
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
                `id` Int64,
        `account_id` Int32,
        `seller_sku` String,
        `status` Int32,
        `your_price` Decimal(15, 2),
        `fulfillment_channel` String,
        `sku` String,
        `deliver_mode` Int32,
        `asin` String,
        `税率` Decimal(8, 4),
        `your_price_point` Decimal(8, 2),
        `promotion_percent` Decimal(8, 2),
        `promotion_amount` Int32,
        `percentage_off` Decimal(8, 2),
        `money_off` Int32,
        `Coupon_handling_fee` Decimal(8, 2),
        `成本` Nullable(Decimal(12, 2)),
        `rate` Decimal(10, 6),
        `fba_fees` Nullable(Decimal(8, 2)),
        `头程费_人民币` Nullable(Decimal(8, 2)),
        `运费` Nullable(Decimal(8, 2)),
        `毛净利率差值` Decimal(8, 4),
        `佣金率` Decimal(8, 4),
        `当前售价利润率-毛利润率` Nullable(Decimal(8, 4)),
        `当前售价利润率-净利润率` Nullable(Decimal(8, 4)),
        `价格状态` String,
        `销售状态` String,
        `不调价原因` String COMMENT '不调价原因',
        `create_date` String,
        `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (account_id, `seller_sku`)
        SETTINGS index_granularity = 8192
    """
    conn_mx.ck_create_table(sql)
    print('结果表建表成功！')

    sql = """
        select a.id as account_id,b.site1 as `站点`
        from yibai_system_kd_sync.yibai_amazon_account a 
        left join domestic_warehouse_clear.yibai_site_table_amazon b 
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
    for key, group in tqdm(df_site.groupby(['站点'])):
        print(f'站点：{key}')
        id_list = mysql_escape(group, 'account_id', 1)
        sql = f"""
            select a.*, b.reason `不调价原因`
            from (
                select * from yibai_domestic.yibai_amazon_listing_profit
                where account_id in ({id_list}) and status = 1
                -- and create_date = \'{date.strftime('%Y-%m-%d')}\'
                and (
                    (
                        (ceil(`当前售价利润率-净利润率`, 2) > 0) and (`销售状态` == '负利润加快动销')
                    ) 
                    or (
                        (ceil(`当前售价利润率-净利润率`, 2) < 0) and (
                            (`销售状态` == '正常') or (`销售状态` == '正利润加快动销'))
                    )
                )
            ) a
            left join (
                SELECT arrayStringConcat(groupArray(concat(item, reason)), '|') reason, 
                account_id , seller_sku  FROM support_document.no_adjust_reason_listing_{date1}
                where account_id in ({id_list})
                group by account_id , seller_sku 
            ) b
            on a.account_id = b.account_id and a.seller_sku = b.seller_sku
        """
        df = conn_mx.ck_select_to_df(sql)
        if df.empty:
            pass
        else:
            conn_mx.ck_insert(df, table_name, if_exist='append')
    print('done!')


def check_raw2():
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')

    date = datetime.date.today()
    date1 = date.strftime('%Y%m%d')
    date = datetime.date.today() - pd.Timedelta(days=1)
    date2 = date.strftime('%Y%m%d')
    sleep_time = 3600
    is_try = True

    # 检查前置任务raw2是否顺利同步
    while is_try:
        try:
            sql = f"""
                SELECT trans_records
                from  yibai_sync_log.yibai_datax_trans_log 
                where date_id = \'{date1}\'
                and d_table_name = 'yibai_amazon_listings_all_raw2'
                order by update_time desc
                limit 1
                """
            df1 = conn_mx.ck_select_to_df(sql)
            sql = f"""
                SELECT trans_records
                from  yibai_sync_log.yibai_datax_trans_log 
                where date_id = \'{date2}\'
                and d_table_name = 'yibai_amazon_listings_all_raw2'
                order by update_time desc
                limit 1
                """
            df2 = conn_mx.ck_select_to_df(sql)

            diff_ratio = abs(df1['trans_records'].values[0] - df2['trans_records'].values[0]) / \
                         df2['trans_records'].values[0]
            if diff_ratio >= 0.02:
                print(f'raw2同步误差今昨日超过2%，1小时后重新检查！')
                time.sleep(sleep_time)
            else:
                is_try = False
        except:
            print(f'ck同步不成功，1小时后重新检查！')
            time.sleep(sleep_time)


if __name__ == '__main__':
    check_raw2()
    yibai_amazon_listings_all_raw2_unmatched()
