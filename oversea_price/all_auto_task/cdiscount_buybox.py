import warnings

import pandas as pd
from tqdm import tqdm
from pulic_func.base_api.base_function import mysql_escape
from pulic_func.base_api.mysql_connect import pd_to_ck
import datetime

warnings.filterwarnings('ignore')


def cdiscount_buybox():
    """
    cd 平台购物车监控
    基于 yibai_product_kd_sync.yibai_cdiscount_listing
    """
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')

    sql = f"""
        DROP TABLE IF EXISTS support_document.cdiscount_buybox
    """
    conn_mx.ck_execute_sql(sql)
    sql = f"""
    CREATE TABLE support_document.cdiscount_buybox
    (
        `account_id` Int64,
        `seller_sku` String,
        `itemid` String,
        `listing同步时间` String COMMENT 'listing同步时间',
        `update_time` String COMMENT '更新时间'
    )
    ENGINE = MergeTree
    ORDER BY account_id
    SETTINGS index_granularity = 8192
    """
    conn_mx.ck_execute_sql(sql)

    sql = """
    INSERT INTO support_document.cdiscount_buybox
    SELECT account_id , seller_sku , product_id itemid, toDateTime(last_update_time , 'UTC') `listing同步时间`,
    toDate(now()) AS `update_time`
    FROM yibai_sale_center_listing_sync.yibai_cdiscount_listing
    WHERE is_best_offer =1 and offer_state=1 and used_status=1 
    """
    conn_mx.ck_execute_sql(sql)
    print('done!')


if __name__ == '__main__':
    cdiscount_buybox()
