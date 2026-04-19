import datetime
import traceback

import pandas as pd
from sqlalchemy import create_engine

from all_auto_task.scripts_ck_client import CkClient
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_ck


def connect_to_cql(database="", host="", port="", username='', password=""):
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(username, password, host, port, database, 'utf8'))
    conn = engine.connect()  # 创建连接
    return conn


# 数据部服务器
def sql_to_pd2(database, host, port, username, password, sql):
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(username, password, host, port, database, 'utf8'))
    conn = engine.connect()  # 创建连接
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


# 国内仓运费接口获取token
def get_token():
    r = requests.get(
        'http://tmsservice.yibainetwork.com:92/ordersys/services/ServiceAuthAccount/getAccessToken?type=java')
    if r.status_code == 200:
        token = r.json().get("data")
        return token
    raise ValueError("fetch token error")


class get_trip_fee(object):
    def __init__(self, platform, shipCountry, table_name):
        # platform_location = {'AMAZON': 1,
        #                      "EB": 'shenzhen',
        #                      'CDISCOUNT': '',
        #                      "WALMART": '',
        #                      "DAR": ''}
        self.platform = platform
        self.shipCountry = shipCountry
        self.table_name = table_name
        # self.warehouse = warehouse  # "1,319,478,481"
        # self.shipType = shipType
        # self.location = platform_location[platform]
        self.ship_list = []

    def batch_df_order_fu(self, group):
        # headers = {'Content-Type': 'application/json;charset=UTF-8'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        url0 = 'http://rest.java.yibainetwork.com/logistics/orderShippingFee/getProductBestLogistics?access_token='
        url = url0 + get_token(2)
        #
        for sku in list(group['sku']):
            data = {
                'cpath': '1->8->9801',
                # 'warehouseId': "58,38,53,35,87,49,50,90,62,42,54,36,47,86,88,325,340,333,349,352,102,339,46,59,353,444,443,434,89,529,59,577,680",
                # 'warehouseId':"49,50,88,325,340,352,529,653,769",
                'warehouseId': '769,352,653,340,325,818,529,88,50,49',
                # 'warehouseId':'88,340,325,50',
                # 'warehouseId':'646,648',
                'shipCountry': self.shipCountry,
                'platformCode': self.platform,
                # 'platformCode': "SHOPPE",
                'sku': sku,
                'skuNum': '1',
                'responseLevel': 3,
                'feeType': 255,
                'checkWarehouse': 0
            }
            # print(data)
            try:
                response = requests.post(url=url, data=data)
                # print(response.json())
                r = json.loads(response.text)
                if r.get('data') != [] or r.get('data') is not None:
                    temp = pd.DataFrame(r.get('data'))
                    try:
                        temp.drop(['feeResult'], axis=1, inplace=True)
                    except:
                        # print(1)
                        pass
                    try:
                        temp.drop(['effDate'], axis=1, inplace=True)
                    except:
                        # print(2)
                        pass
                    temp['platformCode'] = self.shipCountry
                    temp['shipCountry'] = self.platform
                    temp['sku'] = sku
                    self.ship_list.append(temp)
            except:
                print(traceback.format_exc())

    def batch_df_order(self, df):
        df = df.reset_index(drop=True)
        # print(1)
        df['index'] = df.index
        df['index'] = df['index'].apply(lambda m: int(m / 200))
        # print(2)
        threadPool = ThreadPoolExecutor(max_workers=40)
        thread_list = []
        for key, group in df.groupby(['index']):
            group = group.reset_index(drop=True)
            future = threadPool.submit(self.batch_df_order_fu, group)
            thread_list.append(future)

        with tqdm(total=len(thread_list), desc=f'{self.platform}-{self.shipCountry}接口获取运费') as pbar:
            for future in as_completed(thread_list):
                data = future.result()
                pbar.update(1)
        threadPool.shutdown(wait=True)

    def get_date(self):
        df_ship = pd.DataFrame()
        df_ship = df_ship.append(self.ship_list)
        df_ship = df_ship.reset_index(drop=True)
        return df_ship


def xuanpin_yunfei(df, platform, shipCountry, table_name, df_new_price, conn_ck):
    trip = get_trip_fee(platform=platform, shipCountry=shipCountry, table_name=table_name)
    print(1)
    trip.batch_df_order(df)
    print(2)
    df_ship = trip.get_date()
    print(3)
    del trip
    print(4)
    if df_ship.shape[0] > 0:
        ##空运头程
        df_ship['firstCarrierCost1'] = None
        df_ship.loc[df_ship['warehouseName'].str.contains('谷仓英国'), 'firstCarrierCost1'] = df_ship['weight'] / 1000 * 52
        df_ship.loc[df_ship['warehouseName'].str.contains('谷仓法国'), 'firstCarrierCost1'] = df_ship['weight'] / 1000 * 44
        df_ship.loc[df_ship['warehouseName'].str.contains('谷仓捷克'), 'firstCarrierCost1'] = df_ship['weight'] / 1000 * 42
        df_ship.loc[df_ship['warehouseName'].str.contains('谷仓美国东'), 'firstCarrierCost1'] = df_ship['weight'] / 1000 * 45
        df_ship['totalCost1'] = df_ship['totalCost'] - df_ship['firstCarrierCost'] + df_ship['firstCarrierCost1']
        print(df_ship.shape)
        print(5)
        #
        df_ship = df_ship.merge(df_new_price, on=['sku'], how='left')
        df_ship['new_price'] = df_ship['new_price'].fillna(0)
        df_ship['new_price'] = df_ship['new_price'].astype('float64').round(2)
        #
        df_ship.fillna("0", inplace=True)
        for col in list(df_ship.columns):
            df_ship[col] = df_ship[col].astype('str')
        df_ship.rename(columns={'platformCode': 'platform', 'shipCountry': 'country'}, inplace=True)
        print(6)
        df_ship = df_ship.reset_index(drop=True)
        df_ship['index'] = df_ship.index
        df_ship['index'] = df_ship['index'].apply(lambda m: int(m/10000))
        for key, group in df_ship.groupby(['index']):
            group.drop(['index'], axis=1, inplace=True)
            conn_ck.write_to_ck_json_type(group, sheet_name=table_name)
        print(7)


def yun_sku():
    d = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        select distinct SKU as sku from over_sea_age_new
        where date='{d}'
    """
    df = conn.read_sql(sql)
    conn.close()

    # 写到数据库
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_clear_table('air_transport_sku_base')
    conn_ck.write_to_ck_json_type(df, 'air_transport_sku_base')


def main_yun():
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    table_name = f"oversea_run_transport_air_{time.strftime('%Y%m%d')}"
    conn_ck.ck_drop_table(table_name)

    sql = f""" CREATE TABLE yibai_oversea.{table_name} (
                   `warehouseId` String,
                   `warehouseName` String,
                   `logisticsId` String,
                   `shipCode` String,
                   `shipName` String,
                   `shippingCost` String,
                   `residencePrice` String,
                   `remoteExtraFee` String,
                   `extraSizeFee` String,
                   `packTypeFee` String,
                   `overseasFee` String,
                   `packFee` String ,
                   `taxationFee` String,
                   `totalCost` String COMMENT '总运费',
                   `drawPrice` String ,
                   `firstCarrierCost` String COMMENT '海运头程',
                   `dutyCost` String,
                   `antidumpFee` String,
                   `vatTax` String,
                   `overseaPackageFee` String,
                   `weight` String,
                   `country` String,
                   `platform` String,
                   `sku` String,
                   `firstCarrierCost1` String COMMENT '海外仓空运头程',
                   `totalCost1` String COMMENT '海外仓空运头程总运费',
                   `new_price` String COMMENT 'prod_sku表的成本',
                   `companyShipCode` String,
                   `isPlatformLogistics` String,
                   `useStatus` String,
                   `carrierShipCost` String,
                   `carrierShipFee` String,
                   `shipFuelCost` String,
                   `suFreightResult` String,
                   `ruleId` String,
                   `abnormal` String,
                   `shipNameAlias` String,
                   `shipType` String,
                   `origin` String
                  ) 
           ENGINE = MergeTree
           ORDER BY (sku,platform,country)
           SETTINGS index_granularity = 8192"""
    conn_ck.ck_execute_sql(sql=sql)
    sql = """
    select distinct sku from yibai_oversea.air_transport_sku_base
    """
    df = conn_ck.ck_select_to_df(sql)

    sql = """
        select sku,new_price from yibai_prod_base_sync.yibai_prod_sku
        where sku in (select distinct sku from yibai_oversea.air_transport_sku_base)
        """
    df_new_price = conn_ck.ck_select_to_df(ck_sql=sql)

    if len(df) > 0:
        for platform in ['AMAZON']:
            for shipCountry in ['US', 'UK', 'DE', 'FR', 'ES', 'IT']:
                xuanpin_yunfei(df, platform, shipCountry, table_name, df_new_price, conn_ck)


if __name__ == "__main__":
    # yun_sku()
    main_yun()