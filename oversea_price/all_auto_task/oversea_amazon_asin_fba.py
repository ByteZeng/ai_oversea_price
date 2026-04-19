import datetime
import traceback
import pandas as pd
import time
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from pulic_func.base_api import adjust_price_function_amazon
from pulic_func.base_api. mysql_connect import pd_to_ck
from pulic_func.base_api.mysql_connect import connect_to_sql
warnings.filterwarnings("ignore")


def get_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    return out_date

def write_to_ck(df, table_name):
    """
    将中间表数据写入ck
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    sql = f"""
    ALTER TABLE yibai_oversea.{table_name} DELETE where date_id = '{date_id}'
    """
    conn_ck.ck_execute_sql(sql)
    # 确认当天日期数据已删除
    n = 1
    while n < 5:
        print(f'删除当前表里当日的数据，第{n}次测试...')
        sql = f"""
            SELECT count()
            FROM yibai_oversea.{table_name}
            where date_id = '{date_id}'
        """
        df_cnt = conn_ck.ck_select_to_df(sql)
        if df_cnt.iloc[0,0] == 0:
            print('结果表删除成功！')
            conn_ck.ck_insert(df, table_name, if_exist='append')
            print('最新数据存储完成！')
            break
        else:
            n += 1
            time.sleep(60)
    if n == 5:
        print('备份CK失败，当天数据未删除完成，CK未备份')

    # 删除360天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 360, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)



def write_data():
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
    select account_id from yibai_oversea.yibai_ads_oversea_amazon_listing 
    where date_id >='{datetime.date.today() - datetime.timedelta(days=2)}'
    and deliver_mode = 2 
    group by account_id
    """
    # df1 = conn.read_sql(sql)
    df1 = conn_ck.ck_select_to_df(sql)
    print(df1.head())
    account_id_tuple = tuple(set(df1['account_id']))
    print(account_id_tuple[:5])
    account_tuple = tuple(df1['account_id'])
    x, y = divmod(len(account_tuple), 20)
    if y != 0:
        x += 1
    i = 0

    # while i < x:
    #     per_tuple = account_tuple[100 * i:100 * (i + 1)]
    #     if len(per_tuple) == 1:
    df_all = pd.DataFrame()
    j = 0
    for i in range(x):
        a = account_tuple[i * 20:(i + 1) * 20]
        if len(a) > 1:
            sql = f"""
                    select
                        a.account_id as account_id, e.sku as sku, a.seller_sku as seller_sku,
                        case when notEmpty(a.asin1) then a.asin1
                            when empty(a.asin1) and a.product_id_type=1 then a.product_id
                            else s.asin1 end as asin,
                        a.status as status
                    from
                        (
                            select account_id, seller_sku, asin1, product_id_type, product_id, status
                            from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 t1
                            join (select cast(id as UInt32) as account_id from yibai_system_kd_sync.yibai_amazon_account) t2
                            on (t1.account_id = t2.account_id)
                            where t1.account_id in {a}
                            and fulfillment_channel ='AMA'
                        )  a 
                    inner join 
                        (select account_id, seller_sku, sku from yibai_product_kd_sync.yibai_amazon_sku_map where account_id in {a}) e
                        on (a.account_id = e.account_id and  a.seller_sku = e.seller_sku)
                    inner join 
                    ( 
                        select account_id, seller_sku, asin1 from yibai_product_kd_sync.yibai_amazon_listing_alls where account_id in {a}
                    ) s on (a.account_id=s.account_id and a.seller_sku=s.seller_sku)
                    inner join 
                        yibai_system_kd_sync.yibai_amazon_account b ON a.account_id=cast(b.id as Int32)
                    inner join
                        yibai_system_kd_sync.yibai_amazon_group c ON b.group_id=c.group_id and 
                        c.group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组', '新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
                """
        else:
            sql = f"""
                    select
                        a.account_id as account_id, e.sku as sku, a.seller_sku as seller_sku,
                        case when notEmpty(a.asin1) then a.asin1
                            when empty(a.asin1) and a.product_id_type=1 then a.product_id
                            else s.asin1 end as asin,
                        a.status as status
                    from
                        (
                            select account_id, seller_sku, asin1, product_id_type, product_id, status
                            from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 t1
                            join (select cast(id as UInt32) as account_id from yibai_system_kd_sync.yibai_amazon_account) t2
                            on (t1.account_id = t2.account_id)
                            where t1.account_id = {a[0]}
                            and fulfillment_channel ='AMA'
                        )  a 
                    inner join 
                        (select account_id, seller_sku, sku from yibai_product_kd_sync.yibai_amazon_sku_map where account_id = {a[0]}) e
                        on (a.account_id = e.account_id and  a.seller_sku = e.seller_sku)
                    inner join 
                    ( 
                        select account_id, seller_sku, asin1 from yibai_product_kd_sync.yibai_amazon_listing_alls where account_id = {a[0]}
                    ) s on (a.account_id=s.account_id and a.seller_sku=s.seller_sku)
                    inner join 
                        yibai_system_kd_sync.yibai_amazon_account b ON a.account_id=cast(b.id as Int32)
                    inner join
                        yibai_system_kd_sync.yibai_amazon_group c ON b.group_id=c.group_id and 
                        c.group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组', '新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
                """

        df = conn_ck.ck_select_to_df(sql)
        df_all = df_all.append(df)
        j += 1
        print(j)
    print(df_all.columns)
    df_all = df_all[df_all["asin"] != '']
    df_all["date_id"] = datetime.date.today().isoformat()
    df_all.drop_duplicates(inplace=True)
    #
    write_to_ck(df_all, 'yibai_oversea_amazon_asin_fba')
    # conn_ck.ck_insert(df_all, 'yibai_oversea_amazon_asin_fba', if_exist='replace')
    # conn.to_sql(df_all, 'yibai_oversea_amazon_asin_fba', if_exists='append')
    print('传输完成')
    all_data = len(df_all)
    return all_data


def amazon_fba_listing():
    try:
        # 从ck库里面获取listing数据
        # adjust_price_function_amazon.amazon_ck_zhukushujuyanzheng()
        # 删除当天的数据
        all_data = write_data()

    except:
        send_msg('动销组定时任务推送', 'amazon同asin fba',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}amazon同asin fba全量抽取处理出现问题,会影响调价数据(同asin下有fba链接的去除)"
                 f"请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-13922822326'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if all_data < 600000:
            send_msg('动销组定时任务推送', 'amazon同asin fba',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S')}amazon同asin fba全量抽取处理完成(用于去除调价数据中同asin下有fba链接去除),总数量为{all_data},请检查!",
                     mobiles=['+86-13922822326'], is_all=False)
            raise Exception(f'amazon同asin fba全量抽取处理完成,总数量为{all_data},数据量异常,会影响调价数据(同asin下有fba链接的去除),请检查!')
        else:
            send_msg('动销组定时任务推送', 'amazon同asin fba',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}amazon同asin fba全量抽取处理完成(用于去除调价数据中同asin下有fba链接去除),总数量为{all_data}",
                     is_all=False)


if __name__ == '__main__':
    # amazon_fba_listing()
    write_data()
