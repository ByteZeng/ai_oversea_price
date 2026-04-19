import datetime
import traceback
import pandas as pd
import time
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api import adjust_price_function_amazon
from urllib import parse
from pulic_func.base_api.mysql_connect import connect_to_sql
warnings.filterwarnings("ignore")


def get_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=15)).strftime("%Y-%m-%d")
    return out_date


def delete_data(conn):
    # 重跑时手动需手动删除以下两表当天的数据
    sql = f"delete from yibai_amazon_oversea_listing_price where DATE='{datetime.date.today().isoformat()}'"
    conn.execute(sql)


def get_sku_df(conn):
    sql = f"""select distinct sku from dwm_sku_temp_info WHERE date_id>='{get_date()}'"""
    df = conn.read_sql(sql)
    return df


def write_data(conn):
    sku_df = get_sku_df(conn)
    # 从ck库里面获取listing数据
    # adjust_price_function_amazon.amazon_ck_zhukushujuyanzheng()

    print('开始')

    # df_all = pd.DataFrame()
    # for i in range(7):
    #     print(i)
    #     sql = f"""
    #         select
    #             b.account_id as account_id, b.account_name as account_name,
    #             group_name, short_name, 'AMAZON' as platfrom,
    #             case
    #                 when b.site ='sp' then 'es'
    #                 else b.site
    #                 end as site,
    #             status, e.sku as sku, a.seller_sku as seller_sku,
    #             open_date,
    #             if(trim(a.asin1) <> '', a.asin1, t.asin1) as asin,
    #             a.price AS your_price, fulfillment_channel, f.sale_price as sale_price, a.price as online_price
    #             from
    #                 (
    #                  select account_id, asin1, seller_sku, price, status,
    #                         fulfillment_channel, open_date, create_time
    #                   from yibai_product_kd_sync.yibai_amazon_listings_all_raw2
    #                   where mod(account_id,7) = {i}
    #                     and fulfillment_channel='DEF'
    #                 )a
    #                 inner join
    #                 (
    #                   select account_id, sku, seller_sku
    #                     from yibai_product_kd_sync.yibai_amazon_sku_map
    #                    where deliver_mode=2
    #                      and mod(account_id,7) = {i}
    #                 ) e
    #                 on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
    #                 left join
    #                 (
    #                   select account_id, seller_sku, asin1
    #                   from yibai_product_kd_sync.yibai_amazon_listing_alls
    #                   where mod(account_id,7) = {i}
    #                 ) t
    #                 on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
    #                 join
    #                 (
    #                   select cast(b.id as Int32) as account_id, account_name, group_id, short_name, site
    #                   from yibai_system_kd_sync.yibai_amazon_account b
    #                   where account_num not in ('Gaorgas','Wocilnia','Heixwaio')
    #                     and group_id <> 163
    #                 ) b
    #                 on (a.account_id= b.account_id)
    #                 join
    #                 (
    #                   select group_id, group_name
    #                     from yibai_system_kd_sync.yibai_amazon_group
    #                    where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
    #                      and not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
    #                    ) c
    #                 on (b.group_id=c.group_id)
    #                 left join
    #                 (
    #                     select
    #                            ListingPrice as sale_price, account_id, seller_sku
    #                  from
    #                         yibai_product_kd_sync.yibai_amazon_listing_price
    #                     where mod(account_id,7) = {i}
    #                 ) f
    #                 on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
    #                 order by a.create_time desc limit 1
    #                 by a.account_id, a.seller_sku
    #                settings max_memory_usage = 20000000000
    #     """
    #     ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                          db_name='yibai_oversea')
    #     df = ck_client.ck_select_to_df(sql)
    #     df_all = df_all.append(df)

    # # 2023-06-07 补充yxh的在线链接
    # sql = """select distinct sku from over_sea_age_new_date WHERE title like "%%跃星辉%%" """
    # df_yxh_sku = sql_to_pd(database='over_sea', sql=sql)
    # yxh_sku_list = tuple(list(df_yxh_sku['sku'].apply(str).unique()))
    # sql = """
    # SELECT 
    #     yl.*, a.account_name, c.group_name, a.short_name, if(a.site ='sp', 'es', a.site) as site,
    #     if(trim(yala.asin) != '', yala.asin, ya.asin) asin, open_date, status, 
    #     if(trim(yala.price) != '', yala.price, ya.price) online_price
    # FROM (
    #     SELECT sku, seller_sku, account_id, deliver_mode
    #     FROM yibai_product.yibai_amazon_sku_map
    #     WHERE sku in {}  AND deliver_mode=2
    # ) yl
    # LEFT JOIN (
    #     SELECT account_id, seller_sku, asin1 as asin, create_time, price
    #     FROM yibai_product.yibai_amazon_listings_all_raw2
    #     WHERE fulfillment_channel='DEF' and seller_sku in (SELECT seller_sku FROM yibai_amazon_sku_map WHERE sku in {})
    # ) yala
    # ON yl.account_id = yala.account_id and yl.seller_sku = yala.seller_sku
    # LEFT JOIN (
    #     SELECT account_id, seller_sku, asin1 as asin, open_date, status, price
    #     FROM yibai_product.yibai_amazon_listing_alls
    #     WHERE fulfillment_channel='DEF' and seller_sku in (SELECT seller_sku FROM yibai_amazon_sku_map WHERE sku in {})
    # ) ya
    # ON yl.account_id = ya.account_id and yl.seller_sku = ya.seller_sku 
    # INNER JOIN (
    #     select id as account_id, account_name, group_id, short_name, site
    #     from yibai_system.yibai_amazon_account  
    # ) a
    # on a.account_id= yl.account_id
    # INNER JOIN (
    #     select group_id, group_name
    #     from yibai_system.yibai_amazon_group 
    #     ) c 
    # ON a.group_id = c.group_id
    # """.format(yxh_sku_list, yxh_sku_list, yxh_sku_list)
    # # engine = create_engine("mysql+pymysql://sgtjuser:FDJ6hhkl452@124.71.81.224:3306/yibai_product?charset=utf8")
    # print('TGF!')
    # engine = create_engine("mysql+pymysql://sgtjuser:FDJ6hhkl452@124.71.81.224:3306/yibai_product?charset=utf8")
    # conn = engine.connect()  # 创建连接
    # df_yxh_listing = pd.read_sql(sql, conn)
    # conn.close()
    # print('yxh在线链接获取完成')
    
    sql = """ 
    with listing_table as (
        select distinct account_id, seller_sku 
        from yibai_product_kd_sync.yibai_amazon_sku_map 
        where deliver_mode=2
    )
    
    select b.account_id as account_id, b.account_name as account_name, 
        group_name, short_name, 'AMAZON' as platfrom,
        if(b.site ='sp', 'es', b.site) as site,
        status, e.sku as sku, a.seller_sku as seller_sku,  
        open_date,
        if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
        a.price AS your_price, fulfillment_channel, f.sale_price as sale_price, a.price as online_price
    from (
        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
        from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) a 
    inner join ( 
        select account_id, sku, seller_sku 
        from yibai_product_kd_sync.yibai_amazon_sku_map 
        where deliver_mode=2 
    ) e
    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
    left join (
        select account_id, seller_sku, asin1 
        from yibai_product_kd_sync.yibai_amazon_listing_alls 
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) t
    on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
    inner join (
        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from yibai_system_kd_sync.yibai_amazon_account b
        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
        and account_id in (
            select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
            where account_type = 1 and is_yibai =1 )
    ) b
    on (a.account_id= b.account_id)
    inner join (
        select group_id, group_name
        from yibai_system_kd_sync.yibai_amazon_group 
        where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
    ) c 
    on (b.group_id=c.group_id)
    left join (
        select account_id, seller_sku, ListingPrice as sale_price
        from yibai_product_kd_sync.yibai_amazon_listing_price 
        where  (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) f
    on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
    order by a.create_time desc limit 1
    by a.account_id, a.seller_sku
    settings max_memory_usage = 20000000000
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                                                  db_name='yibai_oversea')
    df_all = ck_client.ck_select_to_df(sql)
    # 20230626 处理map表中SKU字段带*的问题
    # 20240116 捆绑sku单独计算定价。
    # df_all['sku'] = df_all['sku'].str.split('*').str[0]

    # 备份，方便其他任务取数
    conn.to_sql(df_all, table='yibai_amazon_oversea_listing_price_all', if_exists='replace')

    df_all = df_all.merge(sku_df, on=['sku'])
    
    # 合并yxh在线listing
    # df_all = pd.concat([df_all, df_yxh_listing])

    df_all["DATE"] = datetime.date.today().isoformat()
    conn.to_sql(df_all, table='yibai_amazon_oversea_listing_price', if_exists='append')
    return df_all


def get_rate(conn):
    sql = '''select country, rate from domestic_warehouse_clear.erp_rate'''
    df_rate = conn.read_sql(sql)
    print(df_rate.info())
    # 获取运送费
    sql2 = """
        select distinct 
            sku, warehouseId, warehouseName, shipName, totalCost, shippingCost, 
            firstCarrierCost, new_price, platform, country 
        from oversea_transport_fee_useful 
        where platform='AMAZON'
    """
    df = conn.read_sql(sql2)
    print(len(df))

    # 获取基础数据
    sql3 = f"""
        select distinct 
            account_id, group_name, short_name, upper(site) as site, status, 
            sku, seller_sku, open_date, asin, online_price, DATE 
        from yibai_amazon_oversea_listing_price 
        where DATE='{datetime.date.today().isoformat()}'
        """
    df_data = conn.read_sql(sql3)
    print(len(df_data))
    df = df_data.merge(df, left_on=["sku", "site"], right_on=["sku", "country"])
    df = df.merge(df_rate, on="country")
    df.drop_duplicates(inplace=True)

    # 获取仓库值
    print('获取仓库值')
    sql4 = """
        select sku,warehouse,available_stock as oversea_stock, warehouse_name, warehouse_id
        from yb_datacenter.v_oversea_stock    
        where oversea_stock > 0
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_name = ck_client.ck_select_to_df(sql4)
    # try:
    #     df_1 = sql_to_pd2(sql4)
    # except:
    #     df_1 = sql_to_pd2(sql4)
    # df_name = df_1.query("oversea_stock>0")

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


def insert_amazon_listing_price_detail():
    try:
        conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
        #
        sql = """
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
        df["online_price"] = df["online_price"].astype(float)
        df_new = df[df["online_price"] <= 0]
        df_new2 = df[df["online_price"] > 0]
        # 获取费用
        sql2 = """
                select site, pay_fee, paypal_fee, vat_fee, extra_fee, platform_zero, platform_must_percent 
                from yibai_platform_fee
                where platform='AMAZON'
            """
        df_fee = conn.read_sql(sql2)
        df_new2 = df_new2.merge(df_fee, on="site")
        df_new2["lpr_rate"] = (1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2[
            'extra_fee']) - (df_new2["totalCost"] + df_new2["new_price"]) / df_new2["rate"] / df_new2["online_price"]

        df_new2["lpr_rate"] = df_new2["lpr_rate"].astype(float)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].round(4)
        df_new2["lpr_rate"] = df_new2["lpr_rate"].apply(lambda x: '%.2f%%' % (x * 100))
        df_new2["must_price"] = round((df_new2["totalCost"] + df_new2['new_price']) / (
                1 - df_new2['pay_fee'] - df_new2['paypal_fee'] - df_new2['vat_fee'] - df_new2['extra_fee'] - df_new2[
            'platform_zero'] - df_new2['platform_must_percent']) / df_new2["rate"], 2)
        # df_new2.loc[df_new2["site"].isin(['UK','AU','US','CA']),"must_price"] = round(
        #     (df_new2["totalCost"] + df_new2['new_price']) / (1 - 0.1913-0.08 - 0.15 - 0.02 - 0.01) / df_new2["rate"], 2)
        # df_new2.loc[df_new2["site"] == 'UK', 'must_price'] = round((df_new2["totalCost"] + df_new2['new_price']) /(1-0.1913-0.04-0.15-0.02-0.01-0.167)/df_new2["rate"], 2)
        df_new2["must_price"] = df_new2["must_price"].astype('str')
        df_new2["must_price"] = df_new2["must_price"].apply(lambda x: new_round(x))
        df_new2.loc[df_new2["must_price"] == "na9", "must_price"] = ''
        df_new2.drop(["pay_fee", "paypal_fee", "vat_fee", "extra_fee", "platform_zero", "platform_must_percent"],
                     axis=1,
                     inplace=True)
        df = df_new2.append(df_new)
        df = df.sort_values(by=["totalCost"], ascending=True)
        # 20211231删除英国仓发往其他站点的数据(为了保证和log出的数据一样
        df = df[~((df['warehouse'] == '英国仓') & (~(df['site'].isin(['UK', 'GB', 'uk', 'gb']))))]
        df = df[~((df['warehouse'] != '英国仓') & ((df['site'].isin(['UK', 'GB', 'uk', 'gb']))))]
        df = df.drop_duplicates(['account_id', 'seller_sku'], 'first')
        df.drop_duplicates(inplace=True)
        conn.to_sql(df, table='amazon_listing_price_detail', if_exists='append')
        #
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓amazon数据插入',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓amazon数据插入到amazon_listing_price_detail表处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def select_yesterday_number(conn):
    today_time = time.strftime('%Y-%m-%d')
    dt = datetime.datetime.strptime(today_time, "%Y-%m-%d")
    yesterday_str = (dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    sql = f"""select count(1) as num from yibai_amazon_oversea_listing_price where DATE='{yesterday_str}'"""
    df = conn.read_sql(sql)
    yesterday_number = df['num'][0]
    return yesterday_number


def amazon_listing():
    try:
        conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
        #
        print(1)
        delete_data(conn)
        print(2)
        df = write_data(conn)
        all_data = len(df)
        yesterday_number = select_yesterday_number(conn)
        print(all_data, yesterday_number)
        a = abs((all_data - yesterday_number) / yesterday_number)
        #
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓amazon全量定价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓amazon平台listing表数据处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if a < 0.03:
            send_msg('动销组定时任务推送', '海外仓amazon全量定价',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓amazon平台listing表数据数据条数为{all_data}, 数据量正常",
                     is_all=False)
        else:
            send_message = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓amazon平台listing表数据数据条数为{all_data},数据量较昨天数据量{yesterday_number}, 大于3%,异常,请检查"
            send_msg('动销组定时任务推送', '海外仓amazon全量定价', send_message,
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
            raise Exception(send_message)


if __name__ == '__main__':
    amazon_listing()
