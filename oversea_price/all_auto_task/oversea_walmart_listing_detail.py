import datetime
import traceback
import pandas as pd
import time
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import pd_to_ck
from pulic_func.base_api.base_function import mysql_escape
from pulic_func.base_api.mysql_connect import connect_to_sql
warnings.filterwarnings("ignore")


def delete_data(conn):
    sql = f"delete from yibai_walmart_oversea_listing_price where DATE='{datetime.date.today().isoformat()}'"
    conn.execute(sql)


def get_sku_tuple(conn):
    D_T = (datetime.date.today() - datetime.timedelta(7)).isoformat()
    sql = f"""
    select distinct sku from dwm_sku_temp_info 
    WHERE warehouse in ('美国仓', '加拿大仓') and available_stock>0
    AND date_id>='{D_T}'
    """
    df = conn.read_sql(sql)
    # sku_tuple = tuple(df["sku"])
    return df


def write_data(conn):
    df = get_sku_tuple(conn)
    df_all = pd.DataFrame()
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m/1000))
    conn_mx = pd_to_ck(database='yibai_sale_center_system_sync', data_sys='调价明细历史数据')
    # 20241008 取消检查
    # 等待当天链接同步完成
    # while 1:
    #     sql = """
    #     select trans_records,error_records from yibai_sync_log.yibai_datax_trans_log
    #     where dag_name='yibai_other_listing_sync' and d_table_name='yibai_walmart_report'
    #     and workflow_time >= toString(today())
    #     order by workflow_time desc limit 3
    #     """
    #     df_listing = conn_mx.ck_select_to_df(sql)
    #     if df_listing.shape[0] > 0:
    #         if df_listing['error_records'][0] == 0 and df_listing['trans_records'][0] > 0:
    #             break
    #         else:
    #             print('数据存在异常')
    #             time.sleep(30)
    #     else:
    #         print('walmart链接未同步')
    #         time.sleep(60)
    #
    #     # 限制循环时间
    #     now = datetime.datetime.now()
    #     if now.hour == 9:
    #         raise IOError('链接未同步')
    # 20240417 切换walmart账号表
    for key, group in df.groupby(['index']):
        sku_list = mysql_escape(group, 'sku')
        sql = f"""
            SELECT 
                a.erp_id as account_id,b.short_name as short_name,
                case 
                    when b.site='us_dsv' then 'us' 
                    else b.site 
                end as site, 
                a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price
            FROM (
                select * from yibai_sale_center_listing_sync.yibai_walmart_report 
                where (upper(publish_status)='PUBLISHED') and sku in ({sku_list}) 
                order by updated_unix desc limit 1 by erp_id,seller_sku
            ) a
            LEFT JOIN (
                SELECT erp_id, short_name, lower(site_code) as site, status
                FROM yibai_sale_center_system_sync.yibai_system_account
                WHERE platform_code = 'WALMART' and account_id in (
                    select account_id
                    from yibai_account_manage_sync.yibai_walmart_account
                    where account_type = 1 and is_yibai =1
                )
            ) b
            ON a.erp_id = b.erp_id
            WHERE b.status=1 and b.site<>'us_dsv'
        """
        df = conn_mx.ck_select_to_df(sql)
        df["DATE"] = datetime.date.today().isoformat()
        df_all = df_all.append(df)
    print('抓链接完成')
    print(df_all.info())
    # 2023-11-30 筛选条件移到最后统一处理
    # 2023-06-15 剔除平台仓链接
    # 2023-07-28 更新剔除方式，按item_id剔除
    # sql = """
    #     SELECT distinct toString(item_id) item_id
    #     FROM yibai_sale_center_listing_sync.yibai_walmart_health_report
    # """
    # df_temp = conn_mx.ck_select_to_df(sql)
    # df_all =pd.concat([df_all,df_temp,df_temp]).drop_duplicates(subset=['item_id'],keep=False)
    # # df_all.drop('item_id', axis=1, inplace=True)
    conn.to_sql(df_all, 'yibai_walmart_oversea_listing_price', if_exists='append')
    print('抓链接完成1')
    print(len(df_all), len(df_all.drop_duplicates()))
    return len(df_all)


def get_rate(conn):
    sql = '''select country,rate from domestic_warehouse_clear.erp_rate'''
    df_rate = conn.read_sql(sql)
    # sql2和sql3一起是获取交通费
    sql2 = """
            select distinct sku,warehouseId, warehouseName, shipName, totalCost, shippingCost, 
                firstCarrierCost, new_price, platform, country 
            from oversea_transport_fee_useful 
            where platform='WALMART'
        """
    df = conn.read_sql(sql2)
    print(len(df))
    df = df[df["platform"] == 'WALMART']
    # 获取基础数据
    sql3 = f"""select distinct * from yibai_walmart_oversea_listing_price where DATE='{datetime.date.today().isoformat()}'"""
    df_data = conn.read_sql(sql3)
    print(len(df_data))
    df_data["site"] = df_data["site"].str.upper()
    df = df_data.merge(df, left_on=["sku", "site"], right_on=["sku", "country"])
    df = df.merge(df_rate, on="country")
    df.drop_duplicates(inplace=True)

    # 获取仓库数据
    sql4 = """
            select sku,warehouse,available_stock as oversea_stock, warehouse_name, warehouse_id
                from yb_datacenter.v_oversea_stock    
            where oversea_stock > 0
        """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_name = ck_client.ck_select_to_df(sql4)
    df = df.merge(df_name, left_on=["sku", "warehouseId"], right_on=["sku", "warehouse_id"])
    df.drop(["warehouse_id"], axis=1, inplace=True)
    return df


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


def insert_walmart_listing_price_detail():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        sql = f"""
            SELECT sku, warehouse, sale_status, start_time, end_time
            FROM oversea_sale_status
            order by start_time desc
        """
        df = conn.read_sql(sql)
        df = df.drop_duplicates(['sku', 'warehouse'], 'first')
        df["end_time"] = df["end_time"].fillna(0)
        df["sale_status_new"] = '正常'
        df.loc[df["end_time"] == 0, "sale_status_new"] = df["sale_status"]
        df.drop(["sale_status", "start_time", "end_time"], axis=1, inplace=True)
        df_data = get_rate(conn)
        df = df_data.merge(df, on=["sku", "warehouse"], how="left")
        df["sale_status_new"] = df["sale_status_new"].fillna('正常')
        df["price"] = df["price"].astype(float)
        df_new = df[df["price"] <= 0]
        df_new2 = df[df["price"] > 0]

        # 获取费用
        sql2 = """
            select site, pay_fee, paypal_fee, vat_fee, extra_fee, platform_zero, platform_must_percent 
            from yibai_platform_fee
            where platform='WALMART'
        """
        df_fee = conn.read_sql(sql2)
        print(len(df_fee))
        df_new2 = df_new2.merge(df_fee, on="site")
        df_new2["lpr_rate"] = (1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2[
            'extra_fee']) - (df_new2["totalCost"] + df_new2["new_price"]) / df_new2["rate"] / df_new2["price"]
        # df_new2.loc[df_new2["site"]=='UK','lpr_rate'] = df_new2["lpr_rate"]-0.2
        df_new2["lpr_rate"] = df_new2["lpr_rate"].astype(float)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].round(4)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].apply(lambda x: '%.2f%%' % (x * 100))
        df_new2["must_price"] = round((df_new2["totalCost"] + df_new2['new_price']) / (
                1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2['extra_fee'] - df_new2[
            'platform_zero'] - df_new2['platform_must_percent']) / df_new2["rate"], 2)
        # df_new2.loc[df_new2["site"] == 'UK', 'must_price'] = round((df_new2["totalCost"] + df_new2['new_price']) /(1-0.4-0.1-0.15-0.02-0.01)/df_new2["rate"], 2)
        df_new2["must_price"] = df_new2["must_price"].astype('str')
        df_new2["must_price"] = df_new2["must_price"].apply(lambda x: new_round(x))
        df_new2.loc[df_new2["must_price"] == "na9", "must_price"] = ''
        df_new2.drop(["pay_fee", "paypal_fee", "vat_fee", "extra_fee", "platform_zero", "platform_must_percent"],
                     axis=1, inplace=True)
        df = df_new2.append(df_new)
        print(len(df))
        df = df.sort_values(by=["totalCost"], ascending=True)
        df.drop_duplicates(inplace=True)
        conn.to_sql(df, 'walmart_listing_price_detail', if_exists='append')
        try:
            df_data2 = df.drop_duplicates(['account_id', 'seller_sku'], 'first')
            print(df_data2)
        except:
            pass
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓walmart数据插入',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓walmart数据插入到walmart_listing_price_detail表处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def select_yesterday_number(conn):
    today_time = time.strftime('%Y-%m-%d')
    dt = datetime.datetime.strptime(today_time, "%Y-%m-%d")
    yesterday_str = (dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    sql = f"""select count(1) from yibai_walmart_oversea_listing_price where DATE='{yesterday_str}'"""
    df = conn.read_sql(sql)
    yesterday_number = df.loc[0][0]
    return yesterday_number


def walmart_listing():
    try:
        conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
        #
        print(1)
        delete_data(conn)
        print(2)
        all_data = write_data(conn)
        print(3)
        # df = get_status()
        # all_data = len(df)
        yesterday_number = select_yesterday_number(conn)
        print(all_data, yesterday_number)
        a = abs((all_data - yesterday_number) / yesterday_number)
        #
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓walmart全量定价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓walmart平台listing表数据数据条数为处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if a < 0.03:
            send_msg('动销组定时任务推送', '海外仓walmart全量定价',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓walmart平台listing表数据数据条数为{all_data}，数据量正常",
                     is_all=False)
        else:
            send_message = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓walmart平台listing表数据数据条数为{all_data},数据量较昨天数据量{yesterday_number}, 大于3%,异常,请检查"
            send_msg('动销组定时任务推送', '海外仓walmart全量定价', send_message,
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
            raise Exception(send_message)


if __name__ == '__main__':
    select_yesterday_number()
