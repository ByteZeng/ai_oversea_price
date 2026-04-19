import datetime
import traceback
import pandas as pd
import time
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_sql, pd_to_ck
warnings.filterwarnings("ignore")


def get_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    return out_date


def delete_data(conn):
    sql = f"delete from yibai_cdiscount_oversea_listing_price where DATE='{datetime.date.today().isoformat()}'"
    conn.execute(sql)


def get_sku_tuple(conn):
    sql = f"""select distinct sku from dwm_sku_temp_info WHERE date_id>='{get_date()}' and available_stock > 0 """
    df = conn.read_sql(sql)
    sku_tuple = tuple(df["sku"])
    return sku_tuple


def get_fee(conn):
    sql = """
        select site,pay_fee,paypal_fee,vat_fee,extra_fee,platform_zero,platform_must_percent 
        from yibai_platform_fee
        where platform='CDISCOUNT'
    """
    df_fee = conn.read_sql(sql)
    return df_fee


def write_data(conn, conn_ck):
    sku_tuple = get_sku_tuple(conn)
    x, y = divmod(len(sku_tuple), 200)
    if y != 0:
        x += 1
    i = 0
    df_all = pd.DataFrame()

    while i < x:
        per_tuple = sku_tuple[200 * i:200 * (i + 1)]
        if len(per_tuple) == 1:
            sql = f"""
            select 
                a.account_id as account_id, b.short_name as account_name, 'CDISCOUNT' as platform, 'FR' AS site,
                a.sku, a.product_id, a.seller_sku,
                best_shipping_charges, best_shipping_charges + price as online_price, offer_state
            FROM yibai_sale_center_listing_sync.yibai_cdiscount_listing a 
            inner join (
                SELECT x.id account_id, a.delivery_country delivery_country, x.short_name
                FROM yibai_sale_center_system_sync.yibai_system_account x
                INNER JOIN yibai_sale_center_system_sync.yibai_system_auth_account a
                ON a.account_id = x.id
                WHERE platform_code  = 'CDISCOUNT' and delivery_country !='CN' and x.account_id in (
                    select account_id
                    from yibai_account_manage_sync.yibai_cdiscount_account
                    where account_type = 1 and is_yibai =1
                )
            ) b 
            on a.account_id=b.account_id
            WHERE 
                offer_state = 1 and used_status=1 
                and product_id not in (
                    select distinct sku from yibai_sale_center_listing_sync.yibai_cdiscount_fbc_stock_detail 
                    where create_date >= toString(today()-10)
                )
                and sku = '{per_tuple[0]}' 
            """
            # sql = f"""SELECT
            #       a.account_id,
            #       b.short_name AS 'account_name',
            #       'CDISCOUNT'as platform,
            #       'FR' AS site,
            #       a.erp_sku as sku,
            #       a.seller_sku,
            #       a.product_id,
            #       best_shipping_charges,
            #       a.price + best_shipping_charges AS 'online_price',
            #       a.offer_state
            #     FROM
            #       yibai_cdiscount_listing a
            #       LEFT JOIN yibai_system.yibai_cdiscount_account b ON a.account_id = b.id
            #     WHERE
            #       a.offer_state = 'Active'
            #         and a.used_status = 1
            #         and b.token_status=1
            #       AND warehouse <> '中国'  and erp_sku = '{per_tuple[0]}'
            #       AND isfbc = 0 and used_status <> 2"""
        else:
            sql = f"""
            select 
                a.account_id as account_id, b.short_name as account_name, 'CDISCOUNT'as platform, 'FR' AS site,
                a.sku, a.product_id, a.seller_sku,
                best_shipping_charges, best_shipping_charges + price as online_price, offer_state
            FROM yibai_sale_center_listing_sync.yibai_cdiscount_listing a 
            inner join (
                SELECT x.id account_id, a.delivery_country delivery_country, x.short_name
                FROM yibai_sale_center_system_sync.yibai_system_account x
                INNER JOIN yibai_sale_center_system_sync.yibai_system_auth_account a
                ON a.account_id = x.id
                WHERE platform_code  = 'CDISCOUNT' and delivery_country !='CN'
            ) b 
            on a.account_id=b.account_id
            WHERE 
                offer_state = 1 and used_status=1 
                and product_id not in (
                    select distinct sku from yibai_sale_center_listing_sync.yibai_cdiscount_fbc_stock_detail 
                    where create_date >= toString(today()-10)
                )
                and sku in {per_tuple} 
            """
            # sql = f"""SELECT
            #       a.account_id,
            #       b.short_name AS 'account_name',
            #       'CDISCOUNT'as platform,
            #       'FR' AS site,
            #       a.erp_sku as sku,
            #       a.seller_sku,
            #       a.product_id,
            #       best_shipping_charges,
            #       a.price + best_shipping_charges AS 'online_price',
            #       a.offer_state
            #     FROM
            #       yibai_cdiscount_listing a
            #       LEFT JOIN yibai_system.yibai_cdiscount_account b ON a.account_id = b.id
            #     WHERE
            #       a.offer_state = 'Active'
            #         and a.used_status = 1
            #         and b.token_status=1
            #       AND warehouse <> '中国'  and erp_sku in {per_tuple}
            #       AND isfbc = 0 and used_status <> 2"""

        # df = conn_kd.read_sql(sql)
        # 切换取数数据源
        df = conn_ck.ck_select_to_df(sql)
        df["DATE"] = datetime.date.today().isoformat()
        if len(df) > 0:
            df_all = df_all.append(df)
        i += 1
        print(i)
    print(df_all.info())
    # 2023-09-18 CD剔除平台仓链接，按product_id剔除
    sql = """
        SELECT distinct toString(sku) as product_id
        FROM yibai_sale_center_listing_sync.yibai_cdiscount_fbc_stock_detail
        where create_date >= toString(today()-30) and platform_stock > 0
    """
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    # conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    print('CD平台仓链接数量共{}条.'.format(len(df_temp)))
    df_all = df_all[~df_all['product_id'].isin(df_temp['product_id'].to_list())]
    print("剔除平台仓链接后共{}条数据.".format(len(df_all)))
    # df_all.drop('product_id', axis=1, inplace=True)
    conn.to_sql(df_all, table='yibai_cdiscount_oversea_listing_price', if_exists='append')
    return len(df_all)


def get_base_data(conn):
    sql = f"""
        select distinct * from yibai_cdiscount_oversea_listing_price 
        where DATE='{datetime.date.today().isoformat()}'
    """
    df = conn.read_sql(sql)
    df.drop(["platform"], axis=1, inplace=True)
    print(len(df))
    return df


def get_transport_fee(conn):
    sql = """
        select distinct sku,warehouseId,warehouseName,shipName,totalCost,shippingCost,firstCarrierCost,new_price,country 
        from oversea_transport_fee_useful 
        where platform='CDISCOUNT'
    """
    df = conn.read_sql(sql)
    print(len(df))
    df_data = get_base_data(conn)
    df = df_data.merge(df, left_on=["sku", "site"], right_on=["sku", "country"])
    return df


def get_kucun():
    sql = """select sku,warehouse,available_stock as oversea_stock, warehouse_name, warehouse_id
            from yb_datacenter.v_oversea_stock    
            where oversea_stock > 0
            """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_1 = ck_client.ck_select_to_df(sql)
    return df_1


def get_rate(conn):
    sql = '''select country,rate from domestic_warehouse_clear.erp_rate'''
    df_rate = conn.read_sql(sql)
    df = get_transport_fee(conn)
    df = df.merge(df_rate, on="country")
    df.drop_duplicates(inplace=True)
    df_name = get_kucun()
    df = df.merge(df_name, left_on=["sku", "warehouseId"], right_on=["sku", "warehouse_id"])
    df.drop(["warehouse_id"], axis=1, inplace=True)
    return df


def insert_cd_listing_price_detail():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        sql = f"""
            SELECT sku,warehouse,sale_status,start_time,end_time
            FROM oversea_sale_status
            order by start_time desc
        """
        df = conn.read_sql(sql)
        print(df)
        df = df.drop_duplicates(['sku', 'warehouse'], 'first')
        df["end_time"] = df["end_time"].fillna(0)
        df["sale_status_new"] = '正常'
        df.loc[df["end_time"] == 0, "sale_status_new"] = df["sale_status"]
        df.drop(["sale_status", "start_time", "end_time"], axis=1, inplace=True)
        df_data = get_rate(conn)
        df = df_data.merge(df, on=["sku", "warehouse"], how="left")
        df["sale_status_new"] = df["sale_status_new"].fillna('正常')
        df["online_price"] = df["online_price"].astype(float)
        df_new = df[df["online_price"] <= 0]
        df_new2 = df[df["online_price"] > 0]
        df_fee = get_fee(conn)
        df_new2 = df_new2.merge(df_fee, on="site")
        df_new2["lpr_rate"] = (1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2[
            'extra_fee']) - (df_new2["totalCost"] + df_new2["new_price"]) / df_new2["rate"] / df_new2["online_price"]
        # df_new2.loc[df_new2["site"]=='UK','lpr_rate'] = df_new2["lpr_rate"]-0.2
        df_new2["lpr_rate"] = df_new2["lpr_rate"].astype(float)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].round(4)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].apply(lambda x: '%.2f%%' % (x * 100))
        df_new2["must_price"] = round((df_new2["totalCost"] + df_new2['new_price']) / (
                1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2['extra_fee'] - df_new2[
            'platform_zero'] - df_new2['platform_must_percent']) / df_new2["rate"], 2)
        # df_new2.loc[df_new2["site"] == 'UK', 'must_price'] = round((df_new2["totalCost"] + df_new2['new_price']) /(1-0.4-0.1-0.15-0.04)/df_new2["rate"], 2)
        df_new2["must_price"] = df_new2["must_price"].astype('str')
        df_new2["must_price"] = df_new2["must_price"].apply(lambda x: new_round(x))
        df_new2.loc[df_new2["must_price"] == "na9", "must_price"] = ''
        df_new2.drop(["pay_fee", "paypal_fee", "vat_fee", "extra_fee", "platform_zero", "platform_must_percent"],
                     axis=1, inplace=True)
        df = df_new2.append(df_new)
        df = df.sort_values(by=["totalCost"], ascending=True)
        df = df.drop_duplicates(['account_id', 'seller_sku'], 'first')
        df.drop_duplicates(inplace=True)
        # df = df[df['warehouseId']==340]

        # df.to_excel(os.path.join(r'C:\Users\Administrator\Desktop\2', "cd.xlsx"))
        conn.to_sql(df, table='cd_listing_price_detail', if_exists='append')
        #
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓cd数据插入',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓cd数据插入到cd_listing_price_detail表处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def new_round(data):
    data = str(data)
    if data != 'nan':
        k = data.split(".")[1]
        if len(k) == 1:
            new_data = data + '9'
        else:
            if data[-1] != '9':
                new_data = data[:-1] + '9'
            else:
                new_data = data
        return new_data


def select_yesterday_number(conn):
    today_time = time.strftime('%Y-%m-%d')
    dt = datetime.datetime.strptime(today_time, "%Y-%m-%d")
    yesterday_str = (dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    sql = f"""select count(1) from yibai_cdiscount_oversea_listing_price where DATE='{yesterday_str}'"""
    df = conn.read_sql(sql)
    yesterday_number = df.loc[0][0]
    return yesterday_number


def cd_listing():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        # conn_kd = connect_to_sql(database='yibai_product', data_sys='小平台刊登库2')
        conn_kd = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
        print(1)
        delete_data(conn)
        print(2)
        all_data = write_data(conn, conn_kd)
        yesterday_number = select_yesterday_number(conn)
        print(all_data, yesterday_number)
        a = abs((all_data - yesterday_number) / yesterday_number)
        # df = get_status()
        # all_data = len(df)
        # print(all_data)
        #
        conn.close()
        # conn_kd.close()
    except:
        send_msg('动销组定时任务推送', '海外仓cd全量定价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓cd平台listing表数据数据处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if a < 0.03:
            send_msg('动销组定时任务推送', '海外仓cd全量定价',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓cd平台listing表数据数据条数为{all_data}, 数据量正常",
                     is_all=False)
        else:
            send_message = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓cd平台listing表数据数据条数为{all_data}, 数据量较昨天数据量{yesterday_number}, 大于3%,异常,请检查"
            send_msg('动销组定时任务推送', '海外仓cd全量定价', send_message,
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
            raise Exception(send_message)


if __name__ == "__main__":
    # cd_listing()
    # select_yesterday_number()
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    all_data = write_data(conn, conn_ck)
