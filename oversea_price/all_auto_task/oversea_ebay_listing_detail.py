import datetime
import traceback
import pandas as pd
import time
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.dingding import send_msg
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_ck
from pulic_func.base_api.base_function import mysql_escape
from tqdm import tqdm

warnings.filterwarnings("ignore")


def get_date(n):
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=n)).strftime("%Y-%m-%d")
    return out_date


def insert_sku_to_sku_messige(conn2):
    sql = f"TRUNCATE sku_messige"
    conn2.execute(sql)
    # 2023-06-28：去除90天库龄以下的SKU，即eBay仅对90天以上库龄SKU进行调价
    sql = f"""
        select distinct sku 
        from dwm_sku_temp_info 
        WHERE (age_90_plus > 0 or available_stock <= 0) and date_id>=(SELECT max(date_id) FROM dwm_sku_temp_info)
        """
    # 20250407 ebay不调价测试：部分账号开发调价
    sql = f"""
        select distinct sku 
        from dwm_sku_temp_info 
        WHERE date_id>=(SELECT max(date_id) FROM dwm_sku_temp_info)
        """
    df = conn2.read_sql(sql)
    conn2.to_sql(df, table='sku_messige', if_exists='append')
    return df


def write_data(conn2):
    df_f = insert_sku_to_sku_messige(conn2)
    df_all = pd.DataFrame()

    df_f = df_f.reset_index(drop=True)
    df_f['index'] = df_f.index
    df_f['index'] = df_f['index'].apply(lambda m: int(m / 5000))

    # conn = connect_to_sql(database='yibai_product', data_sys='ebay刊登库')
    for key, group in tqdm(df_f.groupby(['index'])):
        sku_list = mysql_escape(group, 'sku')
        sql = f"""
            with  account_list as (
                select distinct id,a.account_id account_id
                from yibai_sale_center_system_sync.yibai_system_account a
                inner join (
                    select account_id
                    from yibai_account_manage_sync.yibai_ebay_account
                    where account_type = 1 and is_yibai =1
                ) b
                on a.account_id=b.account_id
                where platform_code='EB' and status=1 and is_del=0
            )
            SELECT 
                a.item_id, a.sku, '' system_sku, b.sell_sku, b.siteid as site, c.site_code country,
                CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
                b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
                d.account_id, a.start_price,
                f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price
            FROM (
                SELECT item_id,sku,start_price 
                FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku 
                WHERE sku in ({sku_list})
            ) a 
            INNER JOIN (
                SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
                from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
                where  warehouse_category_id !=1 and listing_status = 1 
                and account_id in (select distinct id from account_list)
            ) b  ON a.item_id=b.item_id
            LEFT JOIN (
                SELECT item_id, shipping_service_cost
                FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
                WHERE shipping_status=1 and shipping_service_priority=1
            ) f  ON a.item_id = f.item_id
            LEFT JOIN (
                SELECT site_id,site,site1 AS `站点`,is_open,site_code 
                FROM domestic_warehouse_clear.yibai_site_table_ebay 
                where is_open='是'
            ) c  ON b.siteid = c.site_id
            LEFT JOIN account_list d on b.account_id = d.id
            LEFT JOIN (
                SELECT id, warehouse
                FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
                WHERE warehouse_type_id in (2, 3)
            ) e ON b.warehouse_category_id = e.id

        """
        conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
        df_ebay_listing = conn_ck.ck_select_to_df(sql)
        df_ebay_listing.columns = [i.split('.')[-1] for i in df_ebay_listing.columns]
        df_ebay_listing['DATE'] = time.strftime('%Y-%m-%d')
        df_all = pd.concat([df_ebay_listing, df_all])
    # 20241216 剔除虚拟仓链接、FBA仓链接
    df_all = df_all[~df_all['name'].str.contains('虚拟')]
    df_all = df_all[~df_all['name'].str.contains('FBA')]
    # 20220629剔除折扣链接
    date_today = time.strftime('%Y-%m-%d')
    sql_discount = f"""
        SELECT item_id 
        FROM yibai_sale_center_listing_sync.yibai_ebay_discount_status_analysis 
        where ((status=1 and formatDateTime(toDateTime(sale_start_time), '%Y-%m-%d') <= '{date_today}'
        and formatDateTime(toDateTime(sale_end_time), '%Y-%m-%d') >= '{date_today}')
        or (status=2)
        or ((status=1 and need_task=10 and formatDateTime(toDateTime(sale_end_time), '%Y-%m-%d')<='{date_today}')
        or(status=2 and next_cycle_time>0)))
     """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_ebay_discount = conn_ck.ck_select_to_df(sql_discount)
    df_ebay_discount['折扣'] = 1
    df_all['item_id'] = df_all['item_id'].astype('str')
    df_ebay_discount['item_id'] = df_ebay_discount['item_id'].astype('str')
    df_all = df_all.merge(df_ebay_discount, on=['item_id'], how='left')
    df_all = df_all[df_all['折扣'] != 1]
    del df_all['折扣']

    # 20250411 eba剔除锁价表链接
    print(f'剔除锁价表链接前有{len(df_all)}条数据。')
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT item_id, sku, 1 as is_lock_listing
        FROM yibai_sale_center_listing_sync.yibai_ebay_price_adjustment_filter_sku
        WHERE end_time > '{date_today}' and is_del = 0
    """
    df_lock = conn_ck.ck_select_to_df(sql)
    df_lock['item_id'] = df_lock['item_id'].astype(str)
    df_all = df_all.merge(df_lock, on=['item_id','sku'], how='left')
    df_all = df_all[df_all['is_lock_listing'] != 1]
    print(f'剔除锁价表链接后剩余{len(df_all)}条数据。')
    del df_all['is_lock_listing']

    conn2.to_sql(df_all, table='yibai_ebay_oversea_listing_price', if_exists='append')
    return len(df_all)


def get_base_data(conn):
    sql = f"""select distinct * from yibai_ebay_oversea_listing_price where DATE='{datetime.date.today().isoformat()}'"""
    df = conn.read_sql(sql)
    return df


def get_transport_fee(conn):
    sql = """
        select distinct sku,warehouseId,warehouseName,shipName,totalCost,shippingCost,firstCarrierCost,
        new_price,platform,country 
        from oversea_transport_fee_useful 
        where platform='EB'
    """
    df = conn.read_sql(sql)
    df_data = get_base_data(conn)
    df = df_data.merge(df, on=["sku", "country"])
    print(len(df))
    return df


def get_kucun():
    sql = """
        select sku,warehouse,available_stock as oversea_stock, warehouse_name, warehouse_id
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


def get_fee(conn):
    sql = """
        select site,pay_fee,paypal_fee,vat_fee,extra_fee,platform_zero,platform_must_percent 
        from yibai_platform_fee 
        where platform='EB'
    """
    df_fee = conn.read_sql(sql)
    df_fee = df_fee.rename(columns={"site": "country"})
    return df_fee


def get_old_data(conn2):
    sql = f"""select date, count(1) as cnt 
    from yibai_ebay_oversea_listing_price where date >= '{get_date(7)}' and date < '{get_date(0)}' group by date order by date desc limit 1 """
    df_old_data = conn2.read_sql(sql)
    return df_old_data.loc[0, 'date'], df_old_data.loc[0, 'cnt']


def insert_ebay_listing_price_detail():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        sql = f"""SELECT sku, warehouse, sale_status, start_time, end_time FROM oversea_sale_status order by start_time desc"""
        df = conn.read_sql(sql)
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
        df_new2 = df_new2.merge(df_fee, on="country")
        df_new2["lpr_rate"] = (1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2[
            'extra_fee']) - (df_new2["totalCost"] + df_new2["new_price"]) / df_new2[
                                  "rate"] / df_new2["online_price"]
        df_new2.loc[df_new2["site"] == 'UK', 'lpr_rate'] = (1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2[
            'vat_fee'] - df_new2['extra_fee']) - (
                                                                   df_new2["totalCost"] + df_new2["new_price"]) / \
                                                           df_new2[
                                                               "rate"] / (df_new2["online_price"] * 1.2)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].astype(float)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].round(4)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].apply(lambda x: '%.2f%%' % (x * 100))
        df_new2["must_price"] = round(
            (df_new2["totalCost"] + df_new2['new_price']) / (
                    1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2['extra_fee'] -
                    df_new2['platform_zero'] - df_new2['platform_must_percent']) / df_new2[
                "rate"], 2)
        # df_new2.loc[df_new2["site"].isin(['US', 'CA', 'MX']), "must_price"] = round(
        #     (df_new2["totalCost"] + df_new2['new_price']) / (1 - 0.116 - 0.09 - 0.11 - 0.02 - 0.01 - 0.045) / df_new2[
        #         "rate"], 2)
        df_new2.loc[df_new2["site"] == 'UK', 'must_price'] = df_new2["must_price"] * 1.2
        df_new2["must_price"] = df_new2["must_price"].astype('str')
        df_new2["must_price"] = df_new2["must_price"].apply(lambda x: new_round(x))
        df_new2.loc[df_new2["must_price"] == "na9", "must_price"] = ''
        df_new2.drop(["pay_fee", "paypal_fee", "vat_fee", "extra_fee", "platform_zero", "platform_must_percent"],
                     axis=1,
                     inplace=True)
        df = df_new2.append(df_new)
        df = df.sort_values(by=["totalCost"], ascending=True)
        df.drop_duplicates(inplace=True)
        # df = df[df['warehouseId']==444]
        # df.to_excel(os.path.join(r'C:\Users\Administrator\Desktop\2', "ebay.xlsx"))
        sql = f"""delete from ebay_listing_price_detail WHERE date='{get_date(0)}'"""
        conn.execute(sql)
        conn.to_sql(df, table='ebay_listing_price_detail', if_exists='append')
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓ebay数据插入',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓ebay数据插入到ebay_listing_price_detail表处理出现问题,请及时排查,失败原因详情请查看airflow日志",
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


def ebay_listing():
    try:
        conn2 = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        print(1)
        sql = f"delete from yibai_ebay_oversea_listing_price where DATE='{datetime.date.today().isoformat()}'"
        conn2.execute(sql)
        print(2)
        today_data = write_data(conn2)
        # print(3)
        # df = get_status()
        # today_data = len(df)
        last_day_date, last_day_data = get_old_data(conn2)
        a = abs((today_data - last_day_data) / last_day_data)
        b = '%.2f%%' % (a * 100)
        #
        conn2.close()
    except:
        send_msg('动销组定时任务推送', '海外仓ebay全量定价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ebay平台listing表数据数据处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if a > 0.05:
            send_meassage = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ebay平台listing表数据数据条数为{today_data},数据量较{last_day_date}数据量{last_day_data}差异{b}, 大于5个百分点,异常,请检查!"
            send_msg('动销组定时任务推送', '海外仓ebay全量定价', send_meassage,
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
            raise Exception(send_meassage)
        else:
            send_msg('动销组定时任务推送', '海外仓ebay全量定价',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ebay平台listing表数据数据条数为{today_data}, 数据量正常",
                     is_all=False)


if __name__ == '__main__':
    ebay_listing()
