import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import traceback
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import pd_to_ck
from pulic_func.base_api.mysql_connect import connect_to_sql


def get_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    return out_date


def sync_to_over_sea():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #
    sql = f"""
        select * except(sync_time) from yb_datacenter.yb_oversea_sku_age   
        where date = '{get_date()}' and status in (0, 1) and warehouse_stock > 0
    """
    conn_mx = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_mx.ck_select_to_df(sql)
    df['charge_total_price'] = df['charge_total_price'].astype(float)

    sql2 = 'select distinct from_currency_code as charge_currency,rate from domestic_warehouse_clear.erp_rate'
    rate = conn.read_sql(sql2)
    df = df.merge(rate, on=['charge_currency'], how='left')
    df['charge_total_price_rmb'] = df['charge_total_price'] * df['rate']
    df['charge_total_price_rmb'] = df['charge_total_price_rmb'].round(4)
    df['site'] = df['country']
    df.loc[df['site'].isin(['CS', 'CZ']), 'site'] = 'DE'
    df.loc[df['site'].isin(['UK', 'GB']), 'site'] = 'UK'
    df.loc[df['site'].isin(['ES', 'SP']), 'site'] = 'ES'
    sql2 = 'select distinct country as site,rate as rate1 from domestic_warehouse_clear.erp_rate'
    rate2 = conn.read_sql(sql2)
    df = df.merge(rate2, on=['site'], how='left')
    df.loc[df['charge_total_price_rmb'].isnull(), 'charge_total_price_rmb'] = df['charge_total_price'] * df['rate1']
    df.drop(columns=['rate', 'rate1', 'site'], inplace=True)
    df.drop_duplicates(inplace=True)
    df['sync_date'] = datetime.date.today()
    #
    conn.to_sql(df, table='yb_oversea_sku_age', if_exists="append")
    #
    conn.close()


if __name__ == "__main__":
    sync_to_over_sea()
