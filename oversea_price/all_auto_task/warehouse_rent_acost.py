import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import traceback
from all_auto_task.dingding import send_msg
from pulic_func.base_api.mysql_connect import pd_to_ck
from pulic_func.base_api.mysql_connect import connect_to_sql


def get_date(a):
    d = datetime.date.today() - datetime.timedelta(days=2 + a)
    d = d.isoformat()
    return d


def charge_total_price(a, conn):
    sql = f"""
        select sku,charge_currency,toFloat64(sum(charge_total_price)) as `仓租`,warehouse,country 
        from (
            SELECT sku,charge_currency,
            case 
                when charge_total_price is null then 0 
                else charge_total_price  
            end as charge_total_price,
            country,
            CASE WHEN country='US' THEN '美国仓'
                WHEN country IN ('UK','GB') THEN '英国仓'
                WHEN country IN ('CZ','CS','DE') THEN '德国仓'
                WHEN country='FR' THEN '法国仓'
                WHEN country='IT' THEN '意大利仓'
                WHEN country='AU' THEN '澳洲仓'
                WHEN country IN ('ES','SP') THEN '西班牙仓'
                WHEN country='CA' THEN '加拿大仓'
                WHEN country='JP' THEN '日本仓'
                WHEN country='PL' THEN '德国仓'
                ELSE '美国仓' 
            END AS `warehouse`
            FROM yb_datacenter.yb_oversea_sku_age
            WHERE date= '{get_date(a)}' and status in (0,1)  and oversea_type<>'4PX'
        ) 
        group by sku,warehouse,charge_currency,country   
    """
    conn_mx = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    zu = conn_mx.ck_select_to_df(sql)

    sql2 = 'select distinct from_currency_code as charge_currency,rate from domestic_warehouse_clear.erp_rate'
    rate = conn.read_sql(sql2)

    print(rate.head())
    zu.loc[zu['country'] == 'GB', 'country'] = 'UK'
    zu = zu.merge(rate, on=['charge_currency'], how='left')
    zu['仓租'] = zu['仓租'] * zu['rate']
    zu = zu[['sku', 'warehouse', '仓租']]
    zu.drop_duplicates(inplace=True)

    return zu


def total_price(df, a, conn):
    sql1 = f"""SELECT sku,warehouse,sum(total_price) as order_amt
    ,sum(true_profit_new1) as total_profit,sum(real_profit) as total_net_profit
     FROM `dashbord_new` where sales_status='总计' and created_time='{get_date(a)}'
            group by sku,warehouse"""
    df1 = conn.read_sql(sql1)
    df = df.merge(df1, on=['sku', 'warehouse'], how='left')
    print(df.head())
    df['仓租'].fillna(0, inplace=True)
    df['order_amt'].fillna(0, inplace=True)
    df['acost'] = 999
    df.loc[df['order_amt'] != 0, 'acost'] = df['仓租'] / df['order_amt']

    # 分段
    df['acost分段'] = None
    df.loc[(df['acost'] >= 0) & (df['acost'] < 5), 'acost分段'] = '[0,5)'
    df.loc[(df['acost'] >= 5) & (df['acost'] < 10), 'acost分段'] = '[5,10)'
    df.loc[(df['acost'] >= 10) & (df['acost'] < 15), 'acost分段'] = '[10,15)'
    df.loc[(df['acost'] >= 15) & (df['acost'] < 20), 'acost分段'] = '[15,20)'
    df.loc[(df['acost'] >= 20) & (df['acost'] < 25), 'acost分段'] = '[20,25)'
    df.loc[(df['acost'] >= 25) & (df['acost'] < 30), 'acost分段'] = '[25,30)'
    df.loc[(df['acost'] >= 30) & (df['acost'] < 40), 'acost分段'] = '[30,40)'
    df.loc[(df['acost'] >= 40) & (df['acost'] < 60), 'acost分段'] = '[40,60)'
    df.loc[(df['acost'] >= 60) & (df['acost'] < 80), 'acost分段'] = '[60,80)'
    df.loc[(df['acost'] >= 80) & (df['acost'] <= 100), 'acost分段'] = '[80,100]'
    df.loc[(df['acost'] > 100), 'acost分段'] = '(100,∞)'
    df['date'] = f'{get_date(a)}'
    print(df.info())
    df['acost'] = df['acost'].apply(lambda x: round(x, 4))
    df['仓租'] = df['仓租'].apply(lambda x: round(x, 6))
    df['order_amt'] = df['order_amt'].apply(lambda x: round(x, 4))
    df.rename(columns={'acost分段': 'acost_section', '仓租': 'charge_total_price_rmb'}, inplace=True)

    conn.to_sql(df, 'proportion_of_warehouse_rent', if_exists='append')
    print(f"{get_date(a)}写入完成")

    return df


def delete_data(conn):
    sql = f"""delete from proportion_of_warehouse_rent where date>='{get_date(a=9)}' """
    conn.execute(sql)


def main():
    conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
    #
    delete_data(conn)
    print('已删除近10天数据')
    for i in range(10):
        df = charge_total_price(i, conn)
        total_price(df, i, conn)
    conn.close()


def main_run():
    try:
        main()
        send_msg('动销组定时任务推送', '海外仓acost',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}计算海外仓租acost完成,请到proportion_of_warehouse_rent查看数据!",
                 is_all=False)
    except Exception as e:
        send_msg('动销组定时任务推送', '海外仓acost',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}计算海外仓租acost出现问题,请及时排查,失败原因详情请查看airflow",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'],
                 is_all=False, status='失败')
        raise Exception(traceback.format_exc())


if __name__ == "__main__":
    main_run()
