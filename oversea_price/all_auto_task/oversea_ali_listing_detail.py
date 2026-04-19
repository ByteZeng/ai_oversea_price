import datetime
import json
import time
import traceback
import pandas as pd
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from tqdm import tqdm
from all_auto_task.clickhouse_con import send_ck_yibai_data_temp
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import pd_to_ck
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_sql
warnings.filterwarnings("ignore")



def delete_data(conn):
    sql = f"delete from yibai_ali_oversea_listing_price where DATE='{datetime.date.today().isoformat()}'"
    conn.execute(sql)


def get_sku_tuple(conn):
    sql = f"""select distinct sku from over_sea_age_new """
    df = conn.read_sql(sql)
    sku_tuple = tuple(df["sku"])
    return sku_tuple


def get_active_id(conn_kd):
    sql = """SELECT DISTINCT id as account_id FROM yibai_system.yibai_aliexpress_account_qimen WHERE status=1"""
    df = conn_kd.read_sql(sql)
    account_id_tuple = tuple(df["account_id"])
    return account_id_tuple


def write_data(conn, conn_kd):
    sku_tuple = get_sku_tuple(conn)
    account_id_tuple = get_active_id(conn_kd)
    print(len(sku_tuple))
    x, y = divmod(len(sku_tuple), 1000)
    if y != 0: x += 1
    i = 0
    df_all = pd.DataFrame()

    while i < x:
        per_tuple = sku_tuple[1000 * i:1000 * (i + 1)]
        sql = f"""
                    SELECT a.account_id,a.product_id,a.sku 
                    FROM yibai_aliexpress_sku_list a 
                    LEFT JOIN yibai_product_aliexpress_list b 
                    ON a.product_id=b.product_id 
                    WHERE a.sku IN {per_tuple} AND b.product_status_type in ('onSelling', 'OnSelling') 
                    AND a.account_id IN {account_id_tuple}"""
        df = conn_kd.read_sql(sql)
        df_all = df_all.append(df)
        i += 1
        print(i)
    product_id_tuple = tuple(set(list(df_all['product_id'])))
    print(len(product_id_tuple))
    return product_id_tuple, df_all


def write_data2(product_id_tuple, df1, conn_kd, conn):
    product_id_tuple = tuple(set(product_id_tuple))
    print(len(product_id_tuple))
    x, y = divmod(len(product_id_tuple), 1000)
    if y != 0:
        x += 1
    i = 0
    df2 = pd.DataFrame()
    while i < x:
        per_tuple = product_id_tuple[1000 * i:1000 * (i + 1)]
        sql2 = f"""
            SELECT account_id,product_id,aeop_ae_product_skus,group_ids,freight_template_id 
            FROM yibai_product_aliexpress_list_detail WHERE product_id IN {per_tuple}
            """
        df = conn_kd.read_sql(sql2)
        df2 = df2.append(df)
        i += 1

    df2_1 = df2[['account_id', 'product_id', 'aeop_ae_product_skus']].drop_duplicates()
    df2_1 = df2_1.reset_index(drop=True)
    print(1)

    df_sku = pd.DataFrame()
    list1 = []
    for i in tqdm(range(len(df2_1['product_id'])), desc='ali4'):
        df_m = pd.DataFrame(json.loads(df2_1['aeop_ae_product_skus'][i]))
        df_m['product_id'] = df2_1['product_id'][i]
        df_m['account_id'] = df2_1['account_id'][i]
        list1.append(df_m)
    df_sku = df_sku.append(list1)
    df_sku = df_sku[['account_id', 'sku_code', 'sku_price', 'sku_stock', 'product_id', 'id']]
    df_sku.columns = ['account_id', 'ali_sku', 'sku_price', 'sku_stock', 'product_id', 'sku_attribute_id']

    ali_sku = df_sku[['ali_sku']].drop_duplicates()
    ali_sku = ali_sku.reset_index(drop=True)
    ali_sku['index'] = ali_sku.index
    ali_sku['index'] = ali_sku['index'].apply(lambda m: int(m / 1000))
    print(2)
    df3 = pd.DataFrame()
    a_list = []
    for key, group in tqdm(ali_sku.groupby(['index']), desc='ali5'):
        group = group[['ali_sku']].drop_duplicates()
        ali_sku_list = ','.join(list(map(lambda m: f'"{m}"', list(group['ali_sku']))))

        sql3 = f"""
            SELECT sku,ali_sku FROM yibai_aliexpress_sku_map 
            WHERE ali_sku IN ({ali_sku_list})
        """
        df10 = conn_kd.read_sql(sql3)
        a_list.append(df10)
    df3 = df3.append(a_list)
    print(3)
    df_sku = df_sku.merge(df3, on=['ali_sku'], how='left')
    df_sku.loc[df_sku['sku'].isnull(), 'sku'] = df_sku.loc[df_sku['sku'].isnull(), 'ali_sku']
    # 分拆捆绑的sku
    df_sku = df_sku.reset_index(drop=True)
    df_sku['sku'] = df_sku['sku'].astype(str)
    df_sku = df_sku[~df_sku['sku'].str.contains('\+')]  # 排除多个erpsku一起卖的情况
    df_sku['解绑前sku'] = df_sku['sku']
    # df_sku = df_sku.drop(['sku'], axis=1).join(df_sku['sku'].str.split('+', expand=True).stack().reset_index(level=1, drop=True).rename('sku'))   #  多个erpsku一起卖的情况
    df_sku = df_sku.drop(['sku'], axis=1).join(
        df_sku['sku'].str.rsplit('*', n=1, expand=True).rename(columns={0: 'sku', 1: '捆绑数量'}))  # 一个sku多个卖

    # 获取每个sku的捆绑数量
    def zhuanhuan(m):
        try:
            a = int(m)
        except:
            a = 1
        return a

    try:
        df_sku['捆绑数量'].fillna('1', inplace=True)
        df_sku['捆绑数量'] = df_sku['捆绑数量'].str.lower()
        df_sku['捆绑数量'] = df_sku['捆绑数量'].replace('pcs', '', regex=True)
        df_sku.loc[~df_sku['捆绑数量'].str.isnumeric(), '捆绑数量'] = '1'
        # df_sku['捆绑数量'] = df_sku['捆绑数量'].astype(int)
        df_sku['捆绑数量'] = df_sku['捆绑数量'].apply(lambda m: zhuanhuan(m))
    except:
        df_sku['捆绑数量'] = 1

    # 存在一个productid对应一个erpsku多次的情况，只保留捆绑个数不同的，个数相同的剔除
    df_sku = df_sku.drop_duplicates(['account_id', 'product_id', 'sku', '捆绑数量'], 'first')
    # 和yibai_aliexpress_sku_list表匹配对应
    df1 = df1.merge(df_sku, on=['account_id', 'product_id', 'sku'], how='left')

    # 获取每个product_id的折扣
    time_now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    df_id = df1[['product_id']].drop_duplicates()
    df_id = df_id.reset_index(drop=True)
    df_id['index'] = df_id.index
    df_id['index'] = df_id['index'].apply(lambda m: int(m / 1000))
    print(4)
    df_zhekou = pd.DataFrame()

    a_list = []
    for key, group in tqdm(df_id.groupby(['index']), desc='ali6'):
        group = group[['product_id']].drop_duplicates()
        id_list_str1 = ','.join(list(map(lambda m: f'{m}', list(group['product_id']))))

        sql = f"""
                SELECT a.account_id,a.product_id,a.all_discount AS 折扣,b.promotion_name as 折扣分组 
                FROM yibai_aliexpress_product_promotion_new a 
                left join yibai_aliexpress_promotion b 
                ON a.account_id=b.account_id AND a.promotion_id=b.promotion_id 
                WHERE b.status='Ongoing' AND b.end_time>='{time_now}'  
                AND a.product_id IN({id_list_str1})
                """
        df10 = conn_kd.read_sql(sql)
        a_list.append(df10)
    df_zhekou = df_zhekou.append(a_list)
    del a_list
    if df_zhekou.shape[0] == 0:
        df_zhekou = pd.DataFrame(columns=['account_id', 'product_id', '折扣', '折扣分组'])

    df_zhekou.loc[(df_zhekou['折扣'] < 0) | (df_zhekou['折扣'] > 100), '折扣'] = 0
    df_zhekou['折扣'] = df_zhekou['折扣'] / 100
    df1 = df1.merge(df_zhekou, on=['account_id', 'product_id'], how='left')
    df1['折扣'].fillna(0, inplace=True)
    df1['sku_price'] = df1['sku_price'].astype(float)
    df1 = df1[df1['sku_price'].notnull()]
    df1['当前实际售价'] = df1['sku_price'] * (1 - df1['折扣'])
    print(len(df1))
    df1["DATE"] = datetime.date.today().isoformat()

    conn.to_sql(df1, 'yibai_ali_oversea_listing_price', if_exists='append')
    return len(df1)


def get_kucun():
    sql = """select sku,warehouse,available_stock as oversea_stock, warehouse_name, warehouse_id
                from yb_datacenter.v_oversea_stock    
            where oversea_stock > 0"""
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_1 = ck_client.ck_select_to_df(sql)
    return df_1


def get_fashuodi(df_all, product_id_tuple, conn_mx):
    x, y = divmod(len(product_id_tuple), 100)
    if y != 0: x += 1
    i = 0
    df2 = pd.DataFrame()

    while i < x:
        per_tuple = product_id_tuple[100 * i:100 * (i + 1)]
        sql = f"""
            SELECT distinct product_id,property_value_name_en as `发货地` 
            FROM yibai_product_kd_sync.yibai_product_aliexpress_attribute_list 
            WHERE sku_property_id=200007763 AND product_id IN {per_tuple}
        """
        df = conn_mx.ck_select_to_df(sql)

        df2 = df2.append(df)
        df2.drop_duplicates(inplace=True)
        print(i)
        i += 1

    df2['site'] = 'China'
    df2.loc[df2['发货地'] == 'Czech Republic', 'site'] = 'DE'
    df2.loc[df2['发货地'] == 'United States', 'site'] = 'US'
    df2.loc[df2['发货地'] == 'Germany', 'site'] = 'DE'
    df2.loc[df2['发货地'] == 'Australia', 'site'] = 'AU'
    df2.loc[df2['发货地'] == 'Poland', 'site'] = 'PL'
    df2.loc[df2['发货地'] == 'France', 'site'] = 'FR'
    df2.loc[df2['发货地'] == 'Spain', 'site'] = 'ES'
    df2.loc[df2['发货地'] == 'United Kingdom', 'site'] = 'UK'
    df2.loc[df2['发货地'] == 'Russian Federation', 'site'] = 'RU'
    df2.loc[df2['发货地'] == 'France', 'site'] = 'FR'
    df2.loc[df2['发货地'] == 'CZ', 'site'] = 'CZ'
    df2.loc[df2['发货地'] == 'CN', 'site'] = 'CN'
    df2 = df2[df2['site'] != 'China']
    df2 = df_all.merge(df2, on='product_id')

    return df2


def get_base_data(conn):
    sql = f"""
        select distinct * from yibai_ali_oversea_listing_price 
        where DATE='{datetime.date.today().isoformat()}'
    """
    df = conn.read_sql(sql)
    print(len(df))
    return df


def get_transport_fee(conn):
    sql = """
        select distinct sku,warehouseId,warehouseName,shipName,totalCost,shippingCost,
        firstCarrierCost,new_price,country 
        from oversea_transport_fee_useful 
        where platform='ALI'
    """
    df = conn.read_sql(sql)
    print(len(df))
    df_data = get_base_data(conn)
    df = df_data.merge(df, left_on=["sku", "site"], right_on=["sku", "country"])
    return df


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


def insert_ali_listing_price_detail():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        sql = f"""
            SELECT sku,warehouse,sale_status,start_time,end_time
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
        conn.to_sql(df, 'ali_listing_price_detail', if_exists='append')
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓ali数据插入',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓ali数据插入到ali_listing_price_detail表处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def ali_listing():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        conn_kd = connect_to_sql(database='yibai_product', data_sys='小平台刊登库2')
        conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
        #
        print(1)
        delete_data(conn)
        print(2)
        product_id_tuple, df_all = write_data(conn, conn_kd)
        print(3)
        df2 = get_fashuodi(df_all, product_id_tuple, conn_mx)
        print(4)
        all_data = write_data2(product_id_tuple, df2, conn_kd, conn)
        # print(5)
        # df = get_status()
        # all_data = len(df)
        # print(all_data)
        conn.close()
        conn_kd.close()
    except:
        send_msg('动销组定时任务推送', '海外仓ali全量定价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ali平台listing表数据数据处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if all_data:
            send_msg('动销组定时任务推送', '海外仓ali全量定价',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ali平台listing表数据数据条数为{all_data}",
                     is_all=False)
        else:
            send_msg('动销组定时任务推送', '海外仓ali全量定价',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓ali平台listing表数据数据条数为0,异常,请检查!",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'],
                     is_all=False)
            raise Exception(f'海外仓ali平台listing表数据数据条数为0,异常,请检查!')


if __name__ == '__main__':
    ali_listing()
