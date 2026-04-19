import datetime
import shutil
import traceback
from pathlib import Path
from urllib.request import urlretrieve
import pandas as pd
import numpy as np
import time
import os
import requests
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.scripts_ck_client import CkClient
from all_auto_task.yunfei_ckeck import yunfei_data_check
from all_auto_task.dingding import send_msg
# from all_auto_task.oversea_ali_listing_detail import ali_listing
# from all_auto_task.oversea_wish_listing_detail import wish_listing
# from all_auto_task.oversea_walmart_listing_detail import walmart_listing
# from all_auto_task.oversea_cd_listing_detail import cd_listing
# from all_auto_task.oversea_ebay_listing_detail import ebay_listing
# from all_auto_task.oversea_amazon_listing_detail import amazon_listing
# from all_auto_task.oversea_allegro_listing_detail import get_listing_all_site, get_lazada_listing
from all_auto_task.nacos_api import get_user
from pulic_func.dag_manage.dag_status import dag_status_of_price_two, get_user_password
from pulic_func.mongo.mongo_con import MongoDb
from all_auto_task.compare_transport_fee import transport_fee_compare_upload
from all_auto_task.oversea_amazon_asin_fba import amazon_fba_listing
import subprocess
from all_auto_task.oversea_add_logic import get_target_sku_fee
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck,get_ck_conf_data
from pulic_func.price_adjust_web_service.daingjia_public import  sku_and_num_split
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
warnings.filterwarnings("ignore")



def delete_data():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = "truncate oversea_transport_fee_useful"
    conn.execute(sql)


def delete_data2():
    sql = f"drop table if exists oversea_transport_fee_all_{get_date()}"
    conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
    conn.execute(sql)
    conn.close()


def get_date():
    qu_shu_date_str = datetime.date.today().isoformat()
    date_list = qu_shu_date_str.split("-")
    date_new = date_list[0] + date_list[1] + date_list[2]
    return date_new


def get_token():
    r = requests.get(
        'http://tmsservice.yibainetwork.com:92/ordersys/services/ServiceAuthAccount/getAccessToken?type=java')
    if r.status_code == 200:
        token = r.json().get("data")
        return token
    raise ValueError("fetch token error")


def site(platform_type=1, group='YB'):
    """
    platform_type: 1 非temu平台、 非1 temu平台
    group：YB 泛品、 JP 精品
    """
    df_site = pd.DataFrame()
    # df_amazon = pd.DataFrame(
    #     {'shipCountry': ["US", "CA", "MX", "JP", "AU", 'GB', 'DE', 'FR', 'ES', 'IT',
    #                      'NL', 'SE', 'PL', 'TR', 'BE','BR','SK','CZ','DK','FI','HU','PT'], })
    # df_amazon['platform'] = "AMAZON"
    # df_amazon['location'] = '1'
    # df_ebay = pd.DataFrame({'shipCountry': ["US", "CA", "AU", 'GB', 'DE', 'FR', 'ES', 'IT'], })
    # df_ebay['platform'] = "EB"
    # df_ebay['location'] = 'shenzhen'
    # df_wish = pd.DataFrame({'shipCountry': ['AT', 'BE', 'CZ', 'LU', 'NL', 'AU', 'DK', 'FI', 'FR', 'DE', 'HU', 'IE',
    #                                         'IT', 'LV', 'PL', 'PT', 'SK', 'SI', 'ES', 'GB', 'US', 'BG', 'EE', 'SE',
    #                                         'GR', 'HR', 'LT', 'RO', 'CH', 'NO', 'MC'], })
    # df_wish['platform'] = "WISH"
    # df_ali = pd.DataFrame({'shipCountry': ["US", 'DE', 'FR', 'ES', 'IT'], })
    # df_ali['platform'] = "ALI"
    # df_walmart = pd.DataFrame({'shipCountry': ["US", "CA"], })
    # df_walmart['platform'] = "WALMART"
    # df_cd = pd.DataFrame({'shipCountry': ["FR"], })
    # df_cd['platform'] = "CDISCOUNT"
    # df_ozon = pd.DataFrame({'shipCountry': ["RU"], })
    # df_ozon['platform'] = "OZON"
    # df_ozon2 = pd.DataFrame({'shipCountry': ["RU"], })
    # df_ozon2['platform'] = "DMSOzon"
    # df_allegro = pd.DataFrame({'shipCountry': ["PL"], })
    # df_allegro['platform'] = "ALLEGRO"
    sql = f"""
        SELECT platform, shipCountry, location
        FROM over_sea.oversea_fee_site
        WHERE group_code = '{group}' and is_del = 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_platform = conn.read_sql(sql)
    # TEMU
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
        select distinct upper(site3) as shipCountry,'TEMU' as platform,'' as location
        from domestic_warehouse_clear.yibai_site_table_temu
        """
    df_temu = conn_mx.ck_select_to_df(sql)
    #
    if platform_type == 1:
        df_site = df_site.append([df_platform])
    else:
        df_site = df_site.append([df_temu])
    df_site['location'].fillna('', inplace=True)
    df_site = df_site.reset_index(drop=True)
    return df_site


def download_shipping_fee_csv(base_dir, platform_type):
    token = get_token()
    url4 = r'http://logistics-local.yibainetwork.com:33357/shippingFeeTaskOversea/exportToCsv?platform={}&country={}&isDetail=true&access_token=' + token
    url5 = r'http://logistics-local.yibainetwork.com:33357/shippingFeeTaskOversea/viewExportTask?platform={}&country={}&isDetail=true&access_token=' + token
    url6 = r'http://logistics-local.yibainetwork.com:33357/shippingFeeTaskOversea/download?platform={}&country={}&isDetail=true&access_token=' + token
    url9 = r'http://logistics-local.yibainetwork.com:33357/shippingFeeTaskOversea/finishStatus?access_token=' + token

    df = site(platform_type=platform_type, group='YB')
    # df = pd.DataFrame(columns=['platform', 'shipCountry', 'location'], data=[['AMAZON', 'US', '1']])
    df['item'] = df['platform'] + '-' + df['shipCountry']

    now = datetime.datetime.now()
    # 设置目标时间为当前日期的中午12点
    target_time = now.replace(hour=12, minute=0, second=0, microsecond=0)
    # 如果当前时间已经超过今天早上10点，则将目标时间设置为明天的早上10点
    if now >= target_time:
        target_time += datetime.timedelta(days=1)

    while True:
        current_time = datetime.datetime.now()
        if current_time >= target_time:
            print("已经到达12点，跳出循环。")
            break

        while True:
            current_time = datetime.datetime.now()
            if current_time >= target_time:
                print("已经到达12点，跳出循环。")
                break
            try:
                res = requests.get(url=url9).json()['data']
                print(res)
                break
            except:
                pass
        i = 0
        for item in list(df['item']):
            if res[item] == 2:
                i = i + 1
            else:
                print(item + '运费未完成')
        if i == len(df['item']):
            break
        time.sleep(60)
    print(df)
    print('运费计算完成，开始导出下载')
    # 加入下载导出列表，添加导出任务
    i = 0
    while i < len(df['shipCountry']):
        current_time = datetime.datetime.now()
        if current_time >= target_time:
            print("已经到达12点，跳出循环。")
            break
        try:
            r = requests.get(url4.format(df['platform'][i], df['shipCountry'][i])).json()
            print(df['platform'][i], df['shipCountry'][i], r["msg"])
            i = i + 1
        except:
            pass
    print('等待')
    # time.sleep(900)
    i = 0
    while i < len(df['shipCountry']):
        current_time = datetime.datetime.now()
        if current_time >= target_time:
            print("已经到达12点，跳出循环。")
            break
        try:
            # 查看导出进度，任务状态为2的表示已经导出成功，可以下载
            r = requests.get(url5.format(df['platform'][i], df['shipCountry'][i])).json()
            print(r)
            if r["data"]['status'] == 2:
                # 下载文件
                url = url6.format(df['platform'][i], df['shipCountry'][i])
                file_name = os.path.join(base_dir, f'{df["platform"][i]}_{df["shipCountry"][i]}.csv')
                # 通过响应头查看csv文件大小
                r = requests.head(url)
                size_csv = int(r.headers['Content-Length'])
                # 下载csv,当csv文件比响应头中提示的大小要小时，重新下载
                while True:
                    urlretrieve(url, file_name)
                    size = os.stat(file_name).st_size
                    if size >= size_csv:
                        break
                i = i + 1
        except:
            pass


def read_file(root):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务期')
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".csv"):
                file_path = os.path.join(dirpath, filename)
                print(file_path)
                df = pd.read_csv(file_path, encoding="utf-8")
                df_list = [i for i in df.columns]
                if len(df) == 0 or len(df_list) < 2:
                    print(df_list)
                    raise Exception(f'{filename}文件没有数据')
                df["platform"] = filename.split(".")[0].split("_")[0]
                if filename.split(".")[0].split("_")[1] == 'GB':
                    df["country"] = "UK"
                else:
                    df["country"] = filename.split(".")[0].split("_")[1]
                try:
                    df.drop(["weight"], axis=1, inplace=True)
                except:
                    pass
                conn.to_sql(df, table=f'oversea_transport_fee_all_{get_date()}', if_exists='append')
    conn.close()


def get_data(conn):
    sql = f"""SELECT * FROM oversea_transport_fee_all_{get_date()} order by totalCost"""
    df = conn.read_sql(sql)
    df = df[~((df["platform"].isin(["AMAZON", "EB", "WALMART"])) & (
        df["shipName"].isin(['谷东-UPS_一票多箱_Multi', '谷东_FEDEX_OVERNIGHT[Fedex_标准次日达]', '谷东_FEDEX_2DAY',
                             '谷西_FEDEX_2DAY', '谷西_FEDEX_OVERNIGHT[Fedex_标准次日达]'])))]
    df = df.drop_duplicates(['sku', "platform", "country", "warehouseId"], 'first')
    return df


def get_product_type():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #
    sql = """
        SELECT 
            a.sku, a.last_price, a.new_price,
        CASE
            WHEN a.product_status = 0 THEN '审核不通过'
            WHEN a.product_status = 1  THEN '刚开发'
            WHEN a.product_status = 2  THEN '编辑中'
            WHEN a.product_status = 3  THEN '预上线'
            WHEN a.product_status = 4  THEN '在售中'
            WHEN a.product_status = 5  THEN '已滞销'
            WHEN a.product_status = 6  THEN '待清仓'
            WHEN a.product_status = 7  THEN '已停售'
            WHEN a.product_status = 8  THEN '待买样'
            WHEN a.product_status = 9  THEN '待品检'
            WHEN a.product_status = 10 THEN '拍摄中'
            WHEN a.product_status = 11 THEN '产品信息确认'
            WHEN a.product_status = 12 THEN '修图中'
            WHEN a.product_status = 14 THEN '设计审核中'
            WHEN a.product_status = 15 THEN '文案审核中'
            WHEN a.product_status = 16 THEN '文案主管终审中'
            WHEN a.product_status = 17 THEN '试卖编辑中'
            WHEN a.product_status = 18 THEN '试卖在售中'
            WHEN a.product_status = 19 THEN '试卖文案终审中'
            WHEN a.product_status = 20 THEN '预上线拍摄中'
            WHEN a.product_status = 21 THEN '物流审核中'
            WHEN a.product_status = 22 THEN '缺货中'
            WHEN a.product_status = 27 THEN '作图审核中'
            WHEN a.product_status = 28 THEN '关务审核中'
            WHEN a.product_status = 29 THEN '开发审核中'
            WHEN a.product_status = 30 THEN '拍摄中、编辑中'
            when a.product_status = 31 then '编辑中,拍摄中'
            when a.product_status = 32 then '已编辑，拍摄中'
            when a.product_status = 33 then '编辑中,已拍摄'
            when a.product_status = 35 then '新系统开发中'
            ELSE '未知'  
            END AS '产品状态'
        from 
            yibai_product a
    """
    df_data = sql_to_pd(database='yibai_product', sql=sql, data_sys='ERP')
    print('df_data', len(df_data), df_data.columns)
    df_data["成本"] = df_data["new_price"]
    df_data.loc[df_data["产品状态"] == '已停售', "成本"] = df_data["last_price"]
    df_data.drop(["new_price", "last_price", "产品状态"], axis=1, inplace=True)
    df_data = df_data.rename(columns={"成本": "new_price"})
    df_data = df_data[df_data["new_price"] > 0]
    print('df_data', len(df_data), df_data.columns)
    df_transport_fee = get_data(conn)
    print('df_transport_fee', len(df_transport_fee), df_transport_fee.columns)
    # 获取退款率等数据
    platform_fee_sql = f'''SELECT site as country, platform, refound_fee, pay_fee, paypal_fee, extra_fee, vat_fee FROM yibai_platform_fee'''
    df_platform_fee = conn.read_sql(platform_fee_sql)

    df = df_transport_fee.merge(df_platform_fee, how='left', on=['country', 'platform'])
    print('df', len(df), df.columns)

    df.eval('lowest_price = shippingCost / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee)',
            inplace=True)
    df.loc[df['platform'] == 'EB', 'lowest_price'] = df['lowest_price'] * 1.2
    df['lowest_price'] = df['lowest_price'].round(4)
    df.drop(['refound_fee', 'pay_fee', 'paypal_fee', 'extra_fee', 'vat_fee'], axis=1, inplace=True)
    print('df', len(df), df.columns)
    df = df.merge(df_data, on="sku")
    print('df', len(df), df.columns)

    sql3 = """
        select sku, price_cny as limit_price_rmb 
        from domestic_warehouse_clear.sku_limit_price 
        where platform='ALL'
    """
    df_2 = conn.read_sql(sql3)
    print('df_2', len(df_2), df_2.columns)
    df = df.merge(df_2, on="sku", how="left")
    print('df', len(df), df.columns)

    df["limit_price_rmb"] = df["limit_price_rmb"].fillna(0)
    df_other = df[df["limit_price_rmb"] == 0]
    df_other.drop(["limit_price_rmb"], axis=1, inplace=True)
    df_base = df[df["limit_price_rmb"] != 0]
    print('df_base', len(df_base), df_base.columns)

    sql4 = """
        select sku, platform, price_cny as limit_price_rmb 
        from domestic_warehouse_clear.sku_limit_price 
        where EN is null  and platform !='ALL'
    """
    df_3 = conn.read_sql(sql4)
    print('df_3', len(df_3), df_3.columns)

    df_other = df_other.merge(df_3, on=["sku", "platform"], how="left")
    print('df_other', len(df_other), df_other.columns)

    df_other["limit_price_rmb"] = df_other["limit_price_rmb"].fillna(0)
    df_other1 = df_other[df_other["limit_price_rmb"] == 0]
    df_other1.drop(["limit_price_rmb"], axis=1, inplace=True)
    df_base1 = df_other[df_other["limit_price_rmb"] != 0]
    df_base = df_base.append(df_base1)
    print('df_base', len(df_base))

    sql5 = """
        select sku,platform,EN as country,price_cny as limit_price_rmb 
        from domestic_warehouse_clear.sku_limit_price 
        where EN is not null and platform !='ALL'
    """
    df_4 = conn.read_sql(sql5)
    print('df_4', len(df_4), df_4.columns)

    df_other1 = df_other1.merge(df_4, on=["sku", "platform", "country"], how="left")
    print('df_other1', len(df_other1), df_other1.columns)

    df_other1["limit_price_rmb"] = df_other1["limit_price_rmb"].fillna(0)
    df_other2 = df_other1[df_other1["limit_price_rmb"] == 0]
    df_base2 = df_other1[df_other1["limit_price_rmb"] != 0]
    df_new = df_base.append(df_base2)
    df_new = df_new.append(df_other2)
    df_new["limit_price_rmb"] = df_new["limit_price_rmb"].fillna(0)
    # pd_to_sql(df_new, database='over_sea', table='oversea_transport_fee_useful_ceshi', if_exists='replace')
    df_new = df_new.reset_index(drop=True)

    df_new["zero_percent"] = None
    df_new["five_percent"] = None
    print('df_new', len(df_new), df_new.columns)

    df_stock = get_stock()
    print('df_stock', len(df_stock), df_stock.columns)
    df_new = df_new.merge(df_stock, on=["sku", "warehouseId"])
    print('df_new', len(df_new), df_new.columns)

    df_new["son_sku"] = df_new["sku"].apply(lambda x: split_sku(x))

    # df_new["totalCost"] = df_new["totalCost"]-df_new["firstCarrierCost"]+df_new["new_firstCarrierCost"]
    delete_data()
    df_new = df_new.sort_values('totalCost')

    # 加一列大仓
    sql6 = """select a.id as warehouseId,b.name as warehouse
            from yibai_warehouse a
            left join yibai_warehouse_category b on a.ebay_category_id=b.id"""
    df6_w = conn.read_sql(sql6)
    df_new = df_new.merge(df6_w, on=['warehouseId'], how='left')
    # 把波兰仓命名为德国仓
    # df_new.loc[df_new['warehouse'] == '波兰仓', 'warehouse'] = '德国仓'
    conn.to_sql(df_new, table='oversea_transport_fee_useful', if_exists='append')
    # pd_to_sql(df_new, database='over_sea', table='oversea_transport_fee_useful_bak')
    all_data = len(df_new)
    #
    conn.close()
    return all_data


def split_sku(sku):
    sku = str(sku)
    sku = sku.strip()
    if sku.startswith(('GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-')):
        sku = sku.split('-', 1)[1]
    elif sku.startswith(('GB', 'DE')):
        sku = sku[2:]
    elif sku.endswith(('GB', 'DE')):
        sku = sku[:-2]
    return sku


def get_package_size():
    print('开始计算体积')
    sql = """
        select 
            ps.sku, ps.warehouse_id, ps.warehouse_code, 
            p.pack_product_width, p.pack_product_height, p.pack_product_length
        from
            v_oversea_stock ps 
        left join 
            yb_product p on ps.sku = p.sku 
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df = ck_client.ck_select_to_df(sql)
    df["pack_product_width"] = df["pack_product_width"].fillna(0)
    df["pack_product_height"] = df["pack_product_height"].fillna(0)
    df["pack_product_length"] = df["pack_product_length"].fillna(0)
    sql = """
        select sku, warehouse_code, instock_length, instock_wide, instock_high 
        from yibai_zk_oversea_sku_diff_list
    """
    df_data = sql_to_pd(database='yibai_product', sql=sql, data_sys='ERP')
    df_data["instock_length"] = df_data["instock_length"].fillna(0)
    df_data["instock_wide"] = df_data["instock_wide"].fillna(0)
    df_data["instock_high"] = df_data["instock_high"].fillna(0)
    df = df.merge(df_data, on=["sku", "warehouse_code"])
    df["pack_product_width"] = df["pack_product_width"].astype(float)
    df["pack_product_height"] = df["pack_product_height"].astype(float)
    df["pack_product_length"] = df["pack_product_length"].astype(float)
    df["instock_length"] = df["instock_length"].astype(float)
    df["instock_wide"] = df["instock_wide"].astype(float)
    df["instock_high"] = df["instock_high"].astype(float)
    df["size"] = df["instock_length"] * df["instock_wide"] * df["instock_high"]
    df.loc[(df['size'] == 0) | (df['size'].isnull()), 'size'] = df["pack_product_width"] * df["pack_product_height"] * \
                                                                df["pack_product_length"]
    df['size'] = df['size'] / 1000000
    return df


def get_stock():
    sql = """
        select sku, warehouse_id as warehouseId, available_stock
            from yb_datacenter.v_oversea_stock 
    """
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    return df


def oversea_files_download(platform_type=1):
    try:
        if not os.path.exists('/data/yunfei'):
            os.makedirs('/data/yunfei')
        base_dir1 = Path('/data/yunfei')
        items = os.listdir(base_dir1)
        subfolders = [item for item in items if os.path.isdir(os.path.join(base_dir1, item)) and 'yunfei' in item]
        for folder in subfolders:
            folder_item = os.path.join(base_dir1, folder)
            if os.path.exists(folder_item):
                shutil.rmtree(folder_item)
        #
        if platform_type == 1:
            base_dir = Path(f'/data/yunfei/yunfei{get_date()}')
        else:
            base_dir = Path(f'/data/yunfei/yunfei{get_date()}_1')
        if base_dir.exists():
            shutil.rmtree(base_dir)
        os.makedirs(base_dir)
        download_shipping_fee_csv(base_dir, platform_type)

        # 20250403 补充下载精品运费
        # jp_oversea_files_download()
        send_msg('动销组定时任务推送', '海外仓运费下载',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓运费下载完成",
                 is_all=False)
    except:
        send_msg('动销组定时任务推送', '海外仓运费下载',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} <海外仓运费下载>出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())

def jp_oversea_files_download(platform_type=1):
    try:
        print('取消此任务，精铺子仓运费在：yibai_oversea.oversea_transport_fee_daily')

        # 切换为下载三仓同时存在的运费下载
        get_target_sku_fee()

        # if platform_type == 1:
        #     base_dir = Path(f'/data/yunfei/jp_yunfei{get_date()}')
        #     print('1')
        # else:
        #     base_dir = Path(f'/data/yunfei/jp_yunfei{get_date()}_1')
        # if base_dir.exists():
        #     shutil.rmtree(base_dir)
        # print(base_dir)
        # os.makedirs(base_dir)
        # jp_download_shipping_fee_csv(base_dir, platform_type)
        # print('精品海外仓运费下载完成...')
    except:
        print('三仓运费数据下载失败！')
        raise Exception(traceback.format_exc())

def jp_download_shipping_fee_csv(base_dir, platform_type):
    token = get_token()
    # http://logistics-localjp.yibainetwork.com:33357/
    url4 = r'http://logistics-localjp.yibainetwork.com:33357/shippingFeeTaskOversea/exportToCsv?platform={}&country={}&isDetail=true&access_token=' + token
    url5 = r'http://logistics-localjp.yibainetwork.com:33357/shippingFeeTaskOversea/viewExportTask?platform={}&country={}&isDetail=true&access_token=' + token
    url6 = r'http://logistics-localjp.yibainetwork.com:33357/shippingFeeTaskOversea/download?platform={}&country={}&isDetail=true&access_token=' + token
    url9 = r'http://logistics-localjp.yibainetwork.com:33357/shippingFeeTaskOversea/finishStatus?access_token=' + token

    df = site(platform_type=platform_type, group='JP')
    # df = df[(df['platform']=='AMAZON') & (df['shipCountry'].isin(['US']))]
    # df['platform'] = 'DMSAmazon'
    df['item'] = df['platform'] + '-' + df['shipCountry']

    # df.to_excel('F://Desktop//df_country.xlsx', index=0)
    now = datetime.datetime.now()
    # 设置目标时间为当前日期的中午12点
    target_time = now.replace(hour=12, minute=0, second=0, microsecond=0)
    # 如果当前时间已经超过今天早上10点，则将目标时间设置为明天的早上10点
    if now >= target_time:
        target_time += datetime.timedelta(days=1)

    while True:
        current_time = datetime.datetime.now()
        if current_time >= target_time:
            print("已经到达12点，跳出循环。")
            break

        while True:
            current_time = datetime.datetime.now()
            if current_time >= target_time:
                print("已经到达12点，跳出循环。")
                break
            try:
                res = requests.get(url=url9).json()['data']
                print('接口返回结果：')
                print(res)
                break
            except:
                pass
        i = 0
        for item in list(df['item']):
            if res[item] == 2:
                i = i + 1
            else:
                print(item + '运费未完成')
        if i == len(df['item']):
            break
        time.sleep(60)
    print(df)
    print('运费计算完成，开始导出下载')
    # 加入下载导出列表，添加导出任务
    i = 0
    while i < len(df['shipCountry']):
        current_time = datetime.datetime.now()
        if current_time >= target_time:
            print("已经到达12点，跳出循环。")
            break
        try:
            r = requests.get(url4.format(df['platform'][i], df['shipCountry'][i])).json()
            print(df['platform'][i], df['shipCountry'][i], r["msg"])
            i = i + 1
        except:
            pass
    print('等待')
    # time.sleep(900)
    i = 0
    while i < len(df['shipCountry']):
        current_time = datetime.datetime.now()
        if current_time >= target_time:
            print("已经到达12点，跳出循环。")
            break
        try:
            # 查看导出进度，任务状态为2的表示已经导出成功，可以下载
            r = requests.get(url5.format(df['platform'][i], df['shipCountry'][i])).json()
            print(r)
            if r["data"]['status'] == 2:
                # 下载文件
                url = url6.format(df['platform'][i], df['shipCountry'][i])
                file_name = os.path.join(base_dir, f'{df["platform"][i]}_{df["shipCountry"][i]}.csv')
                # 通过响应头查看csv文件大小
                r = requests.head(url)
                size_csv = int(r.headers['Content-Length'])
                # 下载csv,当csv文件比响应头中提示的大小要小时，重新下载
                while True:
                    urlretrieve(url, file_name)
                    size = os.stat(file_name).st_size
                    if size >= size_csv:
                        break
                i = i + 1
        except:
            pass

def get_oversea_sku_age_trans_msg():
    sql = """with 
                groupArray((date, sku_cnt)) as sku_arr,
                toString(today()-2) as this_day,
                toString(today()-3) as last_day,
                ['WYT', 'GC', '4PX'] as warehouse_e_name,
                ['万邑通', '谷仓', '递四方'] as warehouse_cn_name,
                concat('【同步', transform(oversea_type,warehouse_e_name,warehouse_cn_name,oversea_type), '库龄】') as title
            select
                this_day,
                last_day,
                oversea_type, 
                arrayFirst(x->x.1==this_day, sku_arr).2 as this_day_sku,
                arrayFirst(x->x.1==last_day, sku_arr).2 as last_day_sku,
                abs(if(last_day_sku==0, 100, intDivOrZero((this_day_sku-last_day_sku)*10000, last_day_sku)/100)) as pctg,
                if(pctg <= 10, '正常', '异常') as status,
                concat(title, '\n', '状态：', status, '\n', this_day, '的sku数量：', toString(this_day_sku), '\n', 
                        last_day, '的sku数量：', toString(last_day_sku), '\n', '差异：', concat(toString(pctg), '%'), '\n',
                        '通知时间：', toString(now())) as msg
              from 
                (
                    select date, oversea_type, count(distinct sku) as sku_cnt
                    from  yb_datacenter.yb_oversea_sku_age 
                    where date in (toString(today()-2), toString(today()-3))
                    and sku not like 'RM%%'
                    group by date,oversea_type
                )
            group by 
              oversea_type"""
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    df_age = ck_client.ck_select_to_df(sql)
    for tup in zip(df_age['status'], df_age['msg']):
        print(tup[1])

    sql = """with
                groupArray((date_id, record_cnt, sum_available_stock, sum_on_way_stock )) as data_arr,
                toYYYYMMDD(today()) as this_day,
                toYYYYMMDD(today()-1) as last_day
                select
                    this_day,
                    last_day,
                    arrayFirst(x->x.1==this_day, data_arr) as this_record,
                    arrayFirst(x->x.1==last_day, data_arr) as last_record,
                    abs(if(last_record.2==0, 100, intDivOrZero((this_record.2-last_record.2)*10000, last_record.2)/100)) as pctg,
                    if(pctg <= 10, '正常', '异常') as status,
                    concat('【同步海外仓',toString(this_day), '日库存： ', status, ' 】', '\n',
                            toString(last_day), '的库存记录数：', toString(last_record.2), '\n',
                            toString(this_day), '的库存记录数：', toString(this_record.2), '\n',
                            toString(last_day), '的总库存数：', toString(last_record.3), '\n',
                            toString(this_day), '的总库存数：', toString(this_record.3), '\n',
                            toString(last_day), '的总在途库存数：', toString(last_record.4), '\n',
                            toString(this_day), '的总在途库存数：', toString(this_record.4), '\n',
                            '记录数差异：', concat(toString(pctg), '%'), '\n',
                            '通知时间：', toString(now())) as msg
                  from
                    (
                        select date_id,
                               count() as record_cnt,
                               sum(available_stock) as sum_available_stock,
                               sum(allot_on_way_count) as sum_on_way_stock
                        from  yb_datacenter.yb_stock
                        where date_id in (toYYYYMMDD(today()), toYYYYMMDD(today()-1))
                          and toInt64(warehouse_id) in (select id from yibai_logistics_tms_sync.yibai_warehouse where warehouse_type in (2,3,8))
                        group by date_id
                    )"""
    df = ck_client.ck_select_to_df(sql)
    for tup in zip(df['status'], df['msg']):
        print(tup[1])
        if tup[0] == '异常':
            send_msg('动销组定时任务推送', '海外仓库存同步', tup[1])
            raise Exception("海外仓库存yb_stock不完整")

    for tup in zip(df_age['status'], df_age['msg']):
        if tup[0] == '异常':
            send_msg('动销组定时任务推送', '海外仓库龄同步', tup[1])
            raise Exception("库龄库存sku_age数量不正确")


def refresh_mongo_warehouse():
    try:
        send_fba_mongo = MongoDb('send_fba', 'yibai_oversea_move_warehouse')
        send_fba_mongo.connect = "mongodb://oversea_user:oversea_useR391802122120@172.16.50.217:27017/?authSource=send_fba"
        list1 = send_fba_mongo.mongo_select()
        assert len(list1) == 1
        warehouse_array = dict(list1[0])['oversea_warehouse']
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='yb_datacenter')
        conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
        conn_ck.ck_execute_sql(sql='truncate table yibai_oversea_move_warehouse')
        sql = f"""insert into yibai_oversea_move_warehouse(oversea_warehouse) values({warehouse_array})"""
        conn_ck.ck_execute_sql(sql)
    except:
        raise Exception(traceback.format_exc())


def get_oversea_yunfei_all_number():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        import_data()
        # refresh_mongo_warehouse()
        delete_data()
        all_data = write_transport_fee_data()
        print(all_data)
        conn.close()

    except:
        # send_msg('动销组定时任务推送', '海外仓运费计算',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓运费处理出现问题,请及时排查,失败原因详情请查看airflow日志",
        #          mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
        #          status='失败')
        raise Exception(traceback.format_exc())
    else:
        if all_data == 0:
            send_msg('动销组定时任务推送', '海外仓运费计算',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓运费总数量为{all_data},数据量异常,请检查!",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
            raise Exception('海外仓运费总数量为0,数据量异常,请检查!')
        if all_data > 2000000:
            send_msg('动销组定时任务推送', '海外仓运费计算',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓运费处理完成,总量为:{all_data}",
                     is_all=False)
        # else:
        #     send_msg('动销组定时任务推送', '海外仓运费计算',
        #              f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓运费总数量为{all_data},数据量异常,请检查!",
        #              mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
        #     raise Exception(f'海外仓运费总数量为{all_data},数据量异常,请检查!')


def yunfei_data_ckeck_update():
    yunfei_data_check()


# def walmart_listing_detail_update():
#     walmart_listing()
#
#
# def cd_listing_detail_update():
#     cd_listing()
#
#
# def ebay_listing_detail_update():
#     ebay_listing()


def wait_amazon_listing_data_to_ck():
    user, password = get_user_password()
    try:
        now_time = time.strftime("%Y-%m-%d")
        while True:
            kd_main_sync_status = dag_status_of_price_two("yibai_product_kd_main_sync", f'{time.strftime("%Y-%m-%d")}',
                                                          user, password)
            kd_slave_sync_status = dag_status_of_price_two("yibai_product_kd_slave_sync",
                                                           f'{time.strftime("%Y-%m-%d")}', user, password)
            if kd_main_sync_status == 'success' or kd_slave_sync_status == 'success':
                break
            time.sleep(20)
            now_time2 = time.strftime("%Y-%m-%d")
            if now_time != now_time2:
                raise Exception("amazon平台将数据同步到ck失败，现在已经是第二天了")
    except:
        send_msg('动销组定时任务推送', '等待运费获取',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}等待amazon—listing数据同步程序出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


# def amazon_listing_detail_update():
#     amazon_listing()
#
#
# def wish_listing_detail_update():
#     wish_listing()
#
#
# def ali_listing_detail_update():
#     ali_listing()


def import_data(platform_type=1):
    if platform_type == 1:
        base_dir = f'/data/yunfei/yunfei{get_date()}'
    else:
        base_dir = f'/data/yunfei/yunfei{get_date()}_1'
    print(base_dir)
    print(os.listdir(base_dir))
    for file in os.listdir(base_dir):
        print(file)
        df = pd.read_csv(os.path.join(base_dir, file))
        print(df.shape)
        print(df.columns)
        if df.shape[0] == 0:
            os.remove(os.path.join(base_dir, file))
    if platform_type == 1:
        # shell_cmd = f'cd {base_dir} && ' + """
        # dos2unix *.csv && awk 'BEGIN{{OFS=","}}{{if(FNR > 1){{print $0, substr(FILENAME, 1, index(FILENAME,"_")-1), substr(FILENAME, index(FILENAME,"_")+1, 2) }}}}' *.csv \
        # | clickhouse-client -u datax --password=dadg_T0Gtadffx --host=172.16.51.44 --port=9000 -m -n \
        # --connect_timeout=300 --send_timeout=600 --receive_timeout=600 \
        # --max_insert_block_size=50000 \
        # --settings "send_progress_in_http_headers=0" \
        # --query="alter table yibai_oversea.oversea_transport_fee_daily drop partition tuple(toYYYYMMDD(today()));\
        # insert into yibai_oversea.oversea_transport_fee_daily(sku, warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost, \
        # remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, \
        # newPrice, createTime, platform, country) select sku, warehouseId, warehouseName, logisticsId, shipCode, shipName, \
        # totalCost, shippingCost, remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, firstCarrierCost, \
        # dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, platform, if(country=='GB', 'UK', country) from \
        # input('sku String,warehouseId Nullable(Int32),warehouseName Nullable(String),logisticsId Nullable(Int32),shipCode Nullable(String),shipName Nullable(String),\
        # totalCost Nullable(String),shippingCost Nullable(String),remoteExtraFee Nullable(String),extraSizeFee Nullable(String),packTypeFee Nullable(String),\
        # overseasFee Nullable(String),packFee Nullable(String),taxationFee Nullable(String),drawPrice Nullable(String),firstCarrierCost Nullable(String),\
        # dutyCost Nullable(String),antidumpFee Nullable(String),overseaPackageFee Nullable(String),newPrice Nullable(String),weight Nullable(String), \
        # createTime Nullable(String),platform Nullable(String),country Nullable(String)') format CSV"
        # """
        # print(shell_cmd)
        # if subprocess.call(shell_cmd, shell=True) == 0:
        #     print("ok")
        # else:
        #     raise Exception('oversea_transport_fee_daily运费数据入库失败')
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        sql = """
         alter table yibai_oversea.oversea_transport_fee_daily drop partition tuple(toYYYYMMDD(today()))
         """
        conn_ck.ck_execute_sql(sql)
        for file in os.listdir(base_dir):
            print(file)
            df = pd.read_csv(os.path.join(base_dir, file))
            df.drop(['weight'], axis=1, inplace=True)
            df['platform'] = file.split('_')[0]
            df['country'] = file.split('_')[1].split('.')[0]
            col = ['warehouseId','logisticsId']
            for i in col:
                df[i] = pd.to_numeric(df[i], errors='coerce')
                df[i] = df[i].fillna(0).astype(int)
            # df[col] = df[col].fillna(0).astype(int)
            print(df.shape)
            conn_ck.write_to_ck_json_type(df, 'oversea_transport_fee_daily', l_type = 'python')
            # conn_ck.ck_insert(df, 'oversea_transport_fee_daily')
    else:
        conn_mx = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        sql = """
        alter table yibai_oversea.oversea_transport_fee_daily_1 drop partition tuple(toYYYYMMDD(today()))
        """
        conn_mx.ck_execute_sql(sql)
        for file in os.listdir(base_dir):
            print(file)
            df = pd.read_csv(os.path.join(base_dir, file))
            df.drop(['weight'], axis=1, inplace=True)
            df['platform'] = file.split('_')[0]
            df['country'] = file.split('_')[1].split('.')[0]
            print(df.shape)
            conn_mx.write_to_ck_json_type(df, 'oversea_transport_fee_daily_1')
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

def jp_import_data():

    base_dir = f'/data/yunfei/jp_yunfei{get_date()}'

    print('本任务已取消！')
    # print(base_dir)
    # print(os.listdir(base_dir))
    # for file in os.listdir(base_dir):
    #     print(file)
    #     df = pd.read_csv(os.path.join(base_dir, file))
    #     print(df.shape)
    #     print(df.columns)
    #     if df.shape[0] == 0:
    #         os.remove(os.path.join(base_dir, file))
    #
    # conn_mx = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # sql = """
    # alter table yibai_oversea.oversea_transport_fee_daily_jp drop partition tuple(toYYYYMMDD(today()))
    # """
    # conn_mx.ck_execute_sql(sql)
    # for file in os.listdir(base_dir):
    #     print(file)
    #     df = pd.read_csv(os.path.join(base_dir, file))
    #     df.drop(['weight'], axis=1, inplace=True)
    #     df['platform'] = file.split('_')[0]
    #     df['country'] = file.split('_')[1].split('.')[0]
    #     print(df.shape)
    #     conn_mx.write_to_ck_json_type(df, 'oversea_transport_fee_daily_jp')
    # if os.path.exists(base_dir):
    #     shutil.rmtree(base_dir)

def get_interf_fee(df):
    """ 调用接口获取sku的运费 """
    df = df[['sku', 'country','best_warehouse_id','best_warehouse_name', 'warehouse']].drop_duplicates()
    df = df.rename(columns={'best_warehouse_id':'warehouse_id'})
    #
    df['数量'] = 1
    df = df.reset_index(drop=True).reset_index()
    # dic = {'US': '47,49,50,58', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
    #        'AU': '353,769','CZ': '325','PL': '325'}
    # df['warehouse_id'] = df['country'].replace(dic)

    # df_bundle = df_bundle.sample(1000)
    df_result = sku_and_num_split(df)
    df_result = interf_fuc(df_result)
    df_result = df_result[['sku', 'shipCountry', 'warehouseId','warehouseName',  'shipName', 'totalCost','shippingCost','firstCarrierCost',
             'dutyCost','overseasFee','extraSizeFee','remoteExtraFee']]
    df_result = df_result.rename(columns={'warehouseId':'best_warehouse_id','warehouseName':'best_warehouse_name'})
    #
    # 大仓库名补充
    df = df.rename(columns={'warehouse_id':'best_warehouse_id'})
    df_result = pd.merge(df_result, df[['best_warehouse_id', 'warehouse']].drop_duplicates(), how='left', on='best_warehouse_id')
    # 存表
    df_result['date_id'] = time.strftime('%Y-%m-%d')
    print(df_result.head(4))
    write_to_sql(df_result, 'fine_sku_fee_useful')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'fine_sku_fee_useful', if_exists='replace')
    return df
def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = time.strftime('%Y-%m-%d')

    sql = f"""
    delete from over_sea.{table_name} where date_id='{date_id}'
    """
    print(sql)
    conn.execute(sql)
    conn.to_sql(df, table_name, if_exists='append')

    conn.close()

def interf_fuc(df):
    """
    捆绑SKU的运费数据。
    调用运费接口获取
    """
    df_result = pd.DataFrame()
    # key2 = '49;769;325;88;50;353;47;680;58;1139'
    for (key1, key2), group in df.groupby(['country', 'warehouse_id']):
        print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea('AMAZON', key1, key2, '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost','dutyCost', 'overseasFee', 'extraSizeFee', 'remoteExtraFee']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost','dutyCost', 'overseasFee', 'extraSizeFee', 'remoteExtraFee']]
        group3 = group3.sort_values(['totalCost'], ascending=True).drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])
    # , 'dutyCost', 'overseasFee', 'extraSizeFee', 'remoteExtraFee'
    return df_result

def fine_sku_fee():
    """ 精铺转泛品运费数据 """
    # date_today = time.strftime('%Y-%m-%d')
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=10)
    sql = f"""
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id >= '{date_start}' and best_warehouse_name like '%%精铺%%'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    print(df['warehouse'].unique())
    dic = {'美国仓':'US', '德国仓':'DE', '英国仓':'UK','澳洲仓':'AU','加拿大仓':'CA'}
    df['country'] = df['warehouse'].replace(dic)

    df_fee = get_interf_fee(df)


    return None

def write_transport_fee_data():
    """
                round(toDecimal64(1.2,4)*toDecimal64((if(a.country='CA', toDecimal64(1.25,4)*toDecimal64OrZero(a.shippingCost,4),
            toDecimal64OrZero(a.shippingCost,4)),4)+toDecimal64OrZero(a.extraSizeFee,4))
                    / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price,
    """
    n = 0
    sql = f"""   select 
            a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost_new shippingCost, 
            remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
            firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
            a.country as country,
            round(1.2*(ifNull(a.totalCost,0) - ifNull(toFloat64(a.firstCarrierCost) + toFloat64(a.dutyCost),0))
                    / toFloat64(1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee-toDecimal64(promotion_fee, 4)), 4) as lowest_price,
            if(isNull(b.new_price),bb.new_price,b.new_price) as new_price,
            if (isNull(b.pur_weight_pack), bb.pur_weight_pack, b.pur_weight_pack) as pur_weight_pack,
            toFloat64( if (isNull(b.pur_length_pack), bb.pur_length_pack, b.pur_length_pack)) *
            toFloat64( if (isNull(b.pur_width_pack), bb.pur_width_pack, b.pur_width_pack)) *
            toFloat64( if (isNull(b.pur_height_pack), bb.pur_height_pack, b.pur_height_pack)) as weight_volume,
            0 limit_price_rmb,
            null as zero_percent,
            null as five_percent,
            available_stock,
            w.warehouse as warehouse,
            son_sku
            from (
                SELECT *
                FROM (
                     select 
                        sku, cast(warehouseId as Int64) warehouseId,  warehouseName, cast(logisticsId as Int64) logisticsId, shipCode, shipName,shippingCost,
                        CAST(remoteExtraFee AS Float64) AS remoteExtraFee, CAST(extraSizeFee AS Float64) AS extraSizeFee,
                        CAST(packTypeFee AS Float64) AS packTypeFee,
                        CAST(overseasFee AS Float64) AS overseasFee,
                        CAST(packFee AS Float64) AS packFee,
                        CAST(taxationFee AS Float64) AS taxationFee,
                        CAST(drawPrice AS Float64) AS drawPrice,
                        CAST(firstCarrierCost AS Float64) AS firstCarrierCost,
                        CAST(dutyCost AS Float64) AS dutyCost,
                        CAST(antidumpFee AS Float64) AS antidumpFee,
                        CAST(overseaPackageFee AS Float64) AS overseaPackageFee,
                        CAST(newPrice AS Float64) AS newPrice,
                        createTime, platform,
                        case when country = 'GB' then 'UK' else country end as country,
                        case when subString(sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                             subString(sku, 4)
                        when subString(sku, 1, 2) in ['DE', 'GB'] then 
                             subString(sku, 3)
                        when subString(sku, -2) in ['DE', 'GB'] then 
                             subString(sku, 1, -2)
                        else sku
                        end as son_sku,
                        -- 20250121 澳洲fastway尾程上浮50%，上浮后再取最优渠道
                        -- 20250822 美国仓fedex部分超长sku，尾程临时上浮
                        case 
                            when shipCode in ('GCHWC_220218003','GCHWC_220218004','WYT_20190924','WYT_20190807')
                            then toFloat64(totalCost) - toFloat64(shippingCost) + 1.5 * toFloat64(shippingCost)
                            when country in ('UK','GB')
                            then toFloat64(totalCost) - toFloat64(shippingCost) + 1.2 * toFloat64(shippingCost)
                            else toFloat64(totalCost)
                        end as totalCost,
                        case 
                            when shipCode in ('GCHWC_220218003','GCHWC_220218004','WYT_20190924','WYT_20190807')
                            then 1.5 * toFloat64(shippingCost)
                            when country in ('UK','GB')
                            then 1.1* toFloat64(shippingCost)
                        else toFloat64(shippingCost)
                        end as shippingCost_new
                    from yibai_oversea.oversea_transport_fee_daily a
                    where 1 = 1
                    -- AND date_id = 20260327
                     AND date_id = (
                        SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily
                        WHERE date_id > toYYYYMMDD(subtractDays(today(), 15))
                     )
                    AND (NOT (warehouseName like '%英国%' and country not in ('UK','GB')))
                    AND (NOT (warehouseName not like '%英国%' and country in ('UK','GB')))
                    AND shipName not like '%GOFO%'
                    AND shipName not like '%%自提UNIUNI%%'
                    AND shipName not like '%%VC%%'
                    -- AND shipName not in ('CK1-美东2仓UNI本地派送','CK1-美西仓UNI本地派送')
                    AND shipCode not in ('AMAZON_250804003_COPY17616329176207', 'Chukou1_250304011', 'Chukou1_250415004')
                    AND (NOT (toFloat64(shippingCost) < 0.1 and platform not in ('SHOPEE', 'LAZADA'))) 
                    order by toDate(createTime) desc,toDecimal64(totalCost,4) asc 
                    limit 1 by sku, platform, country, toInt64(warehouseId)
                    
                    UNION ALL
                    select
                        sku, warehouseId, warehouseName, 0 logisticsId, shipCode, shipName,shippingCost,
                        0.0 remoteExtraFee, 0.0 extraSizeFee, 0.0 packTypeFee, 0.0 overseasFee, 0.0 packFee, 0.0 taxationFee,
                        0.0 drawPrice, firstCarrierCost, dutyCost, 0.0 antidumpFee, 0.0 overseaPackageFee, 0.0 newPrice,
                        '' createTime, platform,
                        case when shipCountry = 'GB' then 'UK' else shipCountry end as country, '' son_sku, totalCost,
                        shippingCost as shippingCost_new
                    from yibai_oversea.oversea_transport_fee_three_area a
                    where 1 = 1
                    -- AND date_id = '2026-01-01'
                    AND date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_three_area)
                    AND platform != 'TEMU'
                    AND shipName not like '%GOFO%'
                    AND shipName not like '%%自提UNIUNI%%'
                    AND shipName not like '%%VC%%'
                    AND shipName not in ('Amazon线上-CK1美东2仓本地派送（VC-DF）','CK1-美东2仓UNI本地派送','CK1-美西仓UNI本地派送')
                    AND shipCode not in ('AMAZON_250804003_COPY17616329176207', 'Chukou1_250304011', 'Chukou1_250415004')
                    AND (NOT (shippingCost < 0.1 and platform not in ('SHOPEE', 'LAZADA')))
                    order by totalCost asc
                    limit 1 by sku, platform, country, toInt64(warehouseId)
                ) a
                order by toDecimal64(totalCost,4) asc 
                limit 1 by sku, platform, country, toInt64(warehouseId)
            ) a
            LEFT JOIN (
                SELECT
                    id as warehouse_id, warehouse_name, warehouse_code,warehouse_type type, country,
                    b.name warehouse
                FROM yibai_logistics_tms_sync.yibai_warehouse a
                LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b ON a.ebay_category_id = b.id
                WHERE warehouse_type IN (2,3,8)
            ) w ON toInt64OrNull(toString(a.warehouseId)) = w.warehouse_id
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) b
            on (a.sku = b.sku)
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) bb
            on (a.son_sku = bb.sku)
            join 
            (
                select sku, warehouse_id as warehouseId, available_stock
                  from yb_datacenter.v_oversea_stock 
            ) g
            on (a.sku = g.sku and a.warehouseId = g.warehouseId)
            left join 
            (
              select site as country, 
              case when platform='Wildberries' then 'OZON' else platform end as platform, 
              pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee,
              case when platform in ('AMAZON','ALLEGRO') then 0.05 when platform in ('SHOPEE') then 0
              else 0.03 end as promotion_fee
              from yibai_wish.yibai_platform_fee
              UNION ALL
              select site as country, 
              'DMSOzon' as platform, 
              pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee,
              case when platform in ('AMAZON','ALLEGRO') then 0.05 when platform in ('SHOPEE') then 0
              else 0.03 end as promotion_fee
              from (SELECT * FROM yibai_wish.yibai_platform_fee WHERE platform = 'Wildberries') a
            ) c
            on (a.country = c.country and a.platform = c.platform)
        WHERE 
            (NOT (warehouse = '英国仓' and country not in ('UK','GB')))
            AND (NOT (warehouse != '英国仓' and country in ('UK','GB')))
            AND (NOT (warehouse != '美国仓' and country in ('US')))
            -- AND (NOT (warehouse = '美国仓' and country != 'US'))
            -- and sku = 'YM19365GM811'
        """
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = ck_client.ck_select_to_df(sql)
    df = df.drop_duplicates(subset=['sku', 'warehouseId', 'platform', 'country'])
    df['country'] = df['country'].replace('GB','UK')
    print(f'运费数据获取完成，共{len(df)}条.')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = get_toucheng_price_new(df)

    df = df.drop_duplicates(subset=['sku', 'warehouseId', 'platform', 'country'])
    print(df.info())
    conn.to_sql(df, table='oversea_transport_fee_useful', if_exists='append')
    print('mysql数据存储完成.')
    # 20231228 同步一份数据到ck
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    table_name = 'oversea_transport_fee_useful'
    sql = f'TRUNCATE TABLE IF EXISTS yibai_oversea.{table_name}'
    conn_ck.ck_execute_sql(sql)
    print('CK当日数据删除完成.')

    # 20241226 temu运费处理
    write_transport_fee_data_temu()
    return len(df)

def get_toucheng_price(df):
    """
    获取头程单价：
    1、优先用子仓匹配，筛选主要渠道的单价（普货）
    2、其次用大仓匹配，筛选主要渠道的单价
    3、最后用分摊头程的头程
    """

    # sql = """
    #     SELECT *
    #     FROM over_sea.oversea_transport_fee_useful
    #     WHERE available_stock > 0 and platform = 'AMAZON'
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)

    # 取头程单价
    sql = """
        SELECT DISTINCT warehouse_id warehouseId, warehouse_name warehouseName, `计费方式` weight_method,
        `头程计泡系数` dim_weight, `是否包税` include_tax, `普货单价` price, warehouse
        FROM yibai_oversea.oversea_fees_parameter
        WHERE `是否主要渠道` = 1
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_tc = conn_ck.ck_select_to_df(sql)

    col = ['warehouseId', 'price', 'weight_method', 'dim_weight', 'include_tax']
    df = pd.merge(df, df_tc[col], how='left', on=['warehouseId'])
    df_tc_2 = df_tc.sort_values(by='price', ascending=True).drop_duplicates(subset=['warehouse'])
    dic = {'price':'price_2', 'weight_method':'weight_method_2', 'include_tax':'include_tax_2', 'dim_weight':'dim_weight_2'}
    df_tc_2 = df_tc_2.rename(columns=dic)
    col = ['warehouse', 'price_2', 'weight_method_2', 'dim_weight_2', 'include_tax_2']
    df = pd.merge(df, df_tc_2[col], how='left', on=['warehouse'])
    df['price'] = np.where(df['price'].isna(), df['price_2'], df['price'])
    df['weight_method'] = np.where(df['weight_method'].isna(), df['weight_method_2'], df['weight_method'])
    df['include_tax'] = np.where(df['weight_method'].isna(), df['include_tax_2'], df['include_tax'])
    df['dim_weight'] = np.where(df['dim_weight'].isna(), df['dim_weight_2'], df['dim_weight'])
    df.drop(['price_2','weight_method_2','include_tax_2','dim_weight_2'], axis=1, inplace=True)

    col = ['weight_volume', 'pur_weight_pack','price']
    df[col] = df[col].fillna(0).astype(float)
    df['dim_weight'] = df['dim_weight'].fillna(6000).astype(int)
    df['计费重'] = np.where(df['weight_volume']/df['dim_weight'] > df['pur_weight_pack']/1000,
                           df['weight_volume']/df['dim_weight'], df['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']=='计费重',
                                          df['计费重'] * df['price'], df['weight_volume'] / df['dim_weight'] * df['price'])
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,1057,847]),
                                          df['firstCarrierCost'], df['new_firstCarrierCost'])
    # 20241022未匹配到头程报价的子仓，头程取分摊
    df['new_firstCarrierCost'] = np.where(df['new_firstCarrierCost']==0, df['firstCarrierCost'], df['new_firstCarrierCost'])
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost']
    df[col] = df[col].astype(float)
    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    # 20241010 保留分摊头程的总运费
    df['totalCost_origin'] = df['totalCost']
    df['totalCost'] = np.where(df['include_tax']==1,
                               df['totalCost'] - df['firstCarrierCost']-df['dutyCost'] + df['new_firstCarrierCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])
    col = ['price', 'weight_volume', '计费重', 'pur_weight_pack','weight_method','include_tax','dim_weight']
    df.drop(columns=col, inplace=True, axis=1)
    # df.to_excel('F://Desktop//df_fee.xlsx', index=0)

    return df

def get_toucheng_price_new(df):
    """
    获取头程单价：
    1、优先用子仓匹配，筛选主要渠道的单价（普货）
    2、其次用分摊头程的头程
    """
    # 取子仓
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = """
        SELECT distinct warehouse_id warehouseId, area
        FROM yibai_oversea.yibai_warehouse_oversea_temp
    """
    df_area = conn_ck.ck_select_to_df(sql)

    # 取头程单价
    sql = """
        SELECT DISTINCT area, `计费方式` weight_method,
        `头程计泡系数` dim_weight, `是否包税` include_tax, `普货单价` price, warehouse
        FROM yibai_oversea.oversea_fees_parameter_new
        WHERE `是否主要渠道` = 1
    """
    df_tc = conn_ck.ck_select_to_df(sql)
    df_tc = pd.merge(df_area, df_tc, how='left', on='area')
    df_tc = df_tc[~df_tc['price'].isna()]
    # df_tc = df_tc.drop_duplicates(subset=['warehouseId'])
    # df_tc.to_excel('F://Desktop//df_tc_temp.xlsx', index=0)
    col = ['warehouseId', 'price', 'weight_method', 'dim_weight', 'include_tax']
    df_tc['warehouseId'] = df_tc['warehouseId'].fillna(0).astype(int)
    df = pd.merge(df, df_tc[col], how='left', on=['warehouseId'])
    print(df.info())
    col = ['weight_volume', 'pur_weight_pack','price']
    df[col] = df[col].fillna(0).astype(float)
    df['dim_weight'] = df['dim_weight'].fillna(6000).astype(int)
    df['计费重'] = np.where(df['weight_volume']/df['dim_weight'] > df['pur_weight_pack']/1000,
                           df['weight_volume']/df['dim_weight'], df['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']=='计费重',
                                          df['计费重'] * df['price'], df['weight_volume'] / df['dim_weight'] * df['price'])
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,1057,847]),
                                          df['firstCarrierCost'], df['new_firstCarrierCost'])
    # 20241022未匹配到头程报价的子仓，头程取分摊
    df['new_firstCarrierCost'] = np.where(df['new_firstCarrierCost']==0, df['firstCarrierCost'], df['new_firstCarrierCost'])
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost']
    df[col] = df[col].astype(float)
    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    # 20241010 保留分摊头程的总运费
    df['totalCost_origin'] = df['totalCost']
    df['totalCost'] = np.where(df['include_tax']==1,
                               df['totalCost'] - df['firstCarrierCost']-df['dutyCost'] + df['new_firstCarrierCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])
    col = ['price', 'weight_volume', '计费重', 'pur_weight_pack','weight_method','include_tax','dim_weight']
    df.drop(columns=col, inplace=True, axis=1)


    return df


def write_transport_fee_data_temu():
    """ temu平台useful运费 """
    # 检查当日temu运费是否下载完成
    sql = """
        select 
            count(1)
        from yibai_oversea.oversea_transport_fee_daily_1
        where date_id =  toYYYYMMDD(today())
    """
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = ck_client.ck_select_to_df(sql)
    n = 0
    if df.iloc[0,0] == 0:
        print('今日temu运费数据没有同步完成，暂取最近日期运费！')
        # raise Exception('今日temu运费数据没有同步完成，暂取最近日期运费！')
    # yunfei_data_check(platform='TEMU')
    # 取运费
    sql = f"""   select 
            a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost_new shippingCost, 
            remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
            firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
            a.country as country,
            0 as lowest_price,
            if(isNull(b.new_price),bb.new_price,b.new_price) as new_price,
            if (isNull(b.pur_weight_pack), bb.pur_weight_pack, b.pur_weight_pack) as pur_weight_pack,
            toFloat64( if (isNull(b.pur_length_pack), bb.pur_length_pack, b.pur_length_pack)) *
            toFloat64( if (isNull(b.pur_width_pack), bb.pur_width_pack, b.pur_width_pack)) *
            toFloat64( if (isNull(b.pur_height_pack), bb.pur_height_pack, b.pur_height_pack)) as weight_volume,
            0 limit_price_rmb,
            null as zero_percent,
            null as five_percent,
            available_stock,
            w.warehouse as warehouse,
            son_sku
            from (
                SELECT *
                FROM (
                     select 
                        sku, cast(warehouseId as Int64) warehouseId,  warehouseName, cast(logisticsId as Int64) logisticsId, shipCode, shipName,shippingCost,
                        CAST(remoteExtraFee AS Float64) AS remoteExtraFee, CAST(extraSizeFee AS Float64) AS extraSizeFee,
                        CAST(packTypeFee AS Float64) AS packTypeFee,
                        CAST(overseasFee AS Float64) AS overseasFee,
                        CAST(packFee AS Float64) AS packFee,
                        CAST(taxationFee AS Float64) AS taxationFee,
                        CAST(drawPrice AS Float64) AS drawPrice,
                        CAST(firstCarrierCost AS Float64) AS firstCarrierCost,
                        CAST(dutyCost AS Float64) AS dutyCost,
                        CAST(antidumpFee AS Float64) AS antidumpFee,
                        CAST(overseaPackageFee AS Float64) AS overseaPackageFee,
                        CAST(newPrice AS Float64) AS newPrice,
                        createTime, platform,
                        case when country = 'GB' then 'UK' else country end as country,
                        case when subString(sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                             subString(sku, 4)
                        when subString(sku, 1, 2) in ['DE', 'GB'] then 
                             subString(sku, 3)
                        when subString(sku, -2) in ['DE', 'GB'] then 
                             subString(sku, 1, -2)
                        else sku
                        end as son_sku,
                        -- 20250121 澳洲fastway尾程上浮50%，上浮后再取最优渠道
                        -- 20250822 美国仓fedex部分超长sku，尾程临时上浮
                        case 
                            when shipCode in ('GCHWC_220218003','GCHWC_220218004','WYT_20190924','WYT_20190807')
                            then toFloat64(totalCost) - toFloat64(shippingCost) + 1.5 * toFloat64(shippingCost)
                            when country in ('UK','GB')
                            then toFloat64(totalCost) - toFloat64(shippingCost) + 1.2 * toFloat64(shippingCost)
                            when country = 'US'
                            then toFloat64(totalCost) - toFloat64(shippingCost) + 0.85 * toFloat64(shippingCost)
                        else toFloat64(totalCost)
                        end as totalCost,
                        case 
                            when shipCode in ('GCHWC_220218003','GCHWC_220218004','WYT_20190924','WYT_20190807')
                            then 1.5 * toFloat64(shippingCost)
                            when country in ('UK','GB')
                            then 1.1* toFloat64(shippingCost)
                            when country = 'US'
                            then 0.85 * toFloat64(shippingCost)
                        else toFloat64(shippingCost)
                        end as shippingCost_new
                    from yibai_oversea.oversea_transport_fee_daily_1 a
                    where 1 = 1
                    AND date_id = 20260414
                    -- AND date_id = (
                    --     SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily_1
                    --    WHERE date_id > toYYYYMMDD(subtractDays(today(), 15))
                    -- )
                    AND (NOT (warehouseName like '%英国%' and country not in ('UK','GB')))
                    AND (NOT (warehouseName not like '%英国%' and country in ('UK','GB')))
                    AND shippingCost > 2
                    AND shipName not like '%GOFO%'
                    AND shipName not like '%%自提UNIUNI%%'
                    AND shipName not like '%BFE%'
                    AND shipName not like '%UPS Ground Saver%'
                    AND shipName not like '%%VC%%'
                    AND shipName not in ('CK1-美东2仓UNI本地派送','CK1-美西仓UNI本地派送')
                    AND shipCode not in ('AMAZON_250804003_COPY17616329176207', 'Chukou1_250304011', 'Chukou1_250415004')
                    AND (NOT (toFloat64(shippingCost) < 0.1 and platform not in ('SHOPEE', 'LAZADA'))) 
                    order by toDate(createTime) desc,toDecimal64(totalCost,4) asc 
                    limit 1 by sku, platform, country, toInt64(warehouseId)
                    
                    UNION ALL
                    select
                        sku, warehouseId, warehouseName, 0 logisticsId, shipCode, shipName,shippingCost,
                        0.0 remoteExtraFee, 0.0 extraSizeFee, 0.0 packTypeFee, 0.0 overseasFee, 0.0 packFee, 0.0 taxationFee,
                        0.0 drawPrice, firstCarrierCost, dutyCost, 0.0 antidumpFee, 0.0 overseaPackageFee, 0.0 newPrice,
                        '' createTime, platform,
                        case when shipCountry = 'GB' then 'UK' else shipCountry end as country, '' son_sku, totalCost,
                        shippingCost as shippingCost_new
                    from yibai_oversea.oversea_transport_fee_three_area a
                    where 1 = 1
                    -- AND date_id = '2026-01-01'
                    AND date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_three_area)
                    AND platform = 'TEMU'
                    AND shipName not like '%%VC%%'
                    AND shipName not like '%GOFO%'
                    AND shipName not like '%%自提UNIUNI%%'
                    AND shipName not like '%BFE%'
                    AND shipName not like '%UPS Ground Saver%'
                    AND shipName not in ('CK1-美东2仓UNI本地派送','CK1-美西仓UNI本地派送')
                    AND shipCode not in ('AMAZON_250804003_COPY17616329176207', 'Chukou1_250304011', 'Chukou1_250415004')
                    AND (NOT (shippingCost < 2 and platform not in ('SHOPEE', 'LAZADA')))
                    order by totalCost asc
                    limit 1 by sku, platform, country, toInt64(warehouseId)
                ) a
                order by toDecimal64(totalCost,4) asc 
                limit 1 by sku, platform, country, toInt64(warehouseId)
            ) a
            LEFT JOIN (
                SELECT
                    id as warehouse_id, warehouse_name, warehouse_code,warehouse_type type, country,
                    b.name warehouse
                FROM yibai_logistics_tms_sync.yibai_warehouse a
                LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b ON a.ebay_category_id = b.id
                WHERE warehouse_type IN (2,3,8)
            ) w ON toInt64(a.warehouseId) = toInt64(w.warehouse_id)
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) b
            on (a.sku = b.sku)
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) bb
            on (a.son_sku = bb.sku)
            join 
            (
                select sku, warehouse_id as warehouseId, available_stock
                  from yb_datacenter.v_oversea_stock 
            ) g
            on (a.sku = g.sku and toInt64(a.warehouseId) = toInt64(g.warehouseId))
            left join 
            (
              select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee,
              case when platform in ('AMAZON','ALLEGRO') then 0.05 when platform in ('SHOPEE') then 0
              else 0.03 end as promotion_fee
              from yibai_wish.yibai_platform_fee
            ) c
            on (a.country = c.country and a.platform = c.platform)
        -- WHERE 
        --     sku = '00EOT00708'
        """
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = ck_client.ck_select_to_df(sql)
    print(df.info())
    df['country'] = df['country'].replace('GB','UK')
    df = df.drop_duplicates(subset=['sku', 'warehouseId', 'platform', 'country'])
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    # 自算头程替换函数
    df = get_toucheng_price_new(df)
    # df[df['available_stock']>0].to_excel('F://Desktop//df_useful_temu.xlsx', index=0)
    #
    conn.to_sql(df, table='oversea_transport_fee_useful_temu', if_exists='replace')
    print('mysql数据存储完成.')
    # 20231228 同步一份数据到ck
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    table_name = 'oversea_transport_fee_useful_temu'
    sql = f'TRUNCATE TABLE IF EXISTS yibai_oversea.{table_name}'
    conn_ck.ck_execute_sql(sql)
    print('CK当日数据删除完成.')

    return len(df)


def write_transport_fee_data_shopee():
    """
    shopee平台useful运费
    2025-05-08：临时处理
    """
    # 取运费
    sql = """
        SELECT * 
        FROM over_sea.oversea_shopee_fee_daily
        -- WHERE date
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    df = df.rename(columns={'shipCountry':'country'})
    df['country'] = df['country'].replace('GB','UK')
    df = df.drop_duplicates(subset=['sku', 'warehouseId', 'platform', 'country'])

    # 取产品体积和重量
    def get_product(df):
        sql = f"""
            SELECT
                sku, pur_weight_pack, 
                pur_length_pack*pur_width_pack*pur_height_pack weight_volume
            FROM yibai_prod_base_sync.yibai_prod_sku
        """
        conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
        df_prod = conn_ck.ck_select_to_df(sql)

        df = pd.merge(df, df_prod, how='left', on='sku')

        return df

    df = get_product(df)
    # 20240603取最新头程
    sql = """
        SELECT warehouse_id as warehouseId,price, weight_method, include_tax
        FROM `yibai_toucheng_new`
        """
    df_toucheng = conn.read_sql(sql)
    df_toucheng['warehouseId'] = df_toucheng['warehouseId'].astype(int)
    df['warehouseId'] = df['warehouseId'].astype(int)
    df = pd.merge(df, df_toucheng, how='left', on=['warehouseId'])
    # df = df.merge(df_toucheng, on=['warehouseId'])
    col = ['weight_volume', 'pur_weight_pack','price']
    df[col] = df[col].fillna(0).astype(float)
    df['计费重'] = np.where(df['weight_volume']/6000 > df['pur_weight_pack']/1000,
                           df['weight_volume']/6000, df['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']==1,
                                          df['计费重'] * df['price'], df['weight_volume'] / 1000000 * df['price'])
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,1057,847]),
                                          df['firstCarrierCost'], df['new_firstCarrierCost'])
    # 20241022未匹配到头程报价的子仓，头程取分摊
    df['new_firstCarrierCost'] = np.where(df['new_firstCarrierCost']==0, df['firstCarrierCost'], df['new_firstCarrierCost'])
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost']
    df[col] = df[col].astype(float)
    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    # 20241010 保留分摊头程的总运费
    df['totalCost_origin'] = df['totalCost']
    df['totalCost'] = np.where(df['include_tax']==1,
                               df['totalCost'] - df['firstCarrierCost']-df['dutyCost'] + df['new_firstCarrierCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])
    col = ['price', 'weight_volume', '计费重', 'pur_weight_pack','weight_method','include_tax']
    df.drop(columns=col, inplace=True, axis=1)

    # 匹配大仓
    sql = """
        SELECT warehouse_id warehouseId, warehouse
        FROM over_sea.yibai_warehouse_oversea_temp
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_warehouse = conn.read_sql(sql)
    df = pd.merge(df, df_warehouse, how='left', on=['warehouseId'])
    # dic = {'VN':'越南仓', 'TH':'泰国仓', 'PH':'菲律宾仓', 'MY':'马来西亚仓', 'ID':'印尼仓'}
    print(df.info())

    conn.to_sql(df, table='oversea_transport_fee_useful_shopee', if_exists='replace')
    print('mysql数据存储完成.')


    return len(df)


def get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """
    sql = """
    SELECT 
        platform, site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df

def allegro_transport_fee(df):
    """ allegro 平台运费处理 """
    # 20250305 ALLEGRO平台运费临时处理
    df_alle = df[(df['platform']=='AMAZON') & (df['warehouse'].isin(['德国仓','法国仓'])) &
                (df['country'].isin(['PL','CZ','SK','HU']))]
    df_alle['platform'] = 'ALLEGRO'
    # 20260306 带电sku在CZ运费渠道只能用：YM186-捷克DHL FREIGHT本地卡派
    df_ele_sku = get_sku_attr()   # 获取带电sku
    sku_list = df_ele_sku['sku'].unique()
    c1 = (df_alle['sku'].isin(sku_list)) & (df_alle['country'] == 'CZ') & (df_alle['warehouse_id']==325)
    df_alle = df_alle[~c1]
    c2 = (df_alle['sku'].isin(sku_list)) & (df_alle['country'] == 'PL') & (df_alle['warehouse_id']==325)
    df_2 = df_alle[c2]
    df_2['country'] = 'CZ'
    c3 = (df_2['shippingCost']<227.3)
    df_2['ship_name'] = np.where(c3, 'YM186-捷克DHL FREIGHT本地卡派', df_2['ship_name'])
    df_2['total_cost'] = np.where(c3, df_2['total_cost'] - df_2['shippingCost'] + 227.23, df_2['total_cost'])
    df_2['totalCost_origin'] = np.where(c3, df_2['totalCost_origin'] - df_2['shippingCost'] + 227.23, df_2['totalCost_origin'])
    df_2['ship_fee'] = np.where(c3, df_2['ship_fee'] - df_2['shippingCost']+227.23, df_2['ship_fee'])
    df_2['shippingCost'] = np.where(c3, 227.23, df_2['shippingCost'])
    df_alle = pd.concat([df_alle, df_2])

    df_plat = get_platform_fee()
    df_plat = df_plat[df_plat['platform'] == 'ALLEGRO']
    df_alle = pd.merge(df_alle, df_plat[['country','ppve','refound_fee']], how='left', on=['country'])
    df_alle['lowest_price'] = 1.2 * (df_alle['totalCost_origin']-df_alle['firstCarrierCost']-df_alle['dutyCost'])/(
            1-df_alle['ppve']-df_alle['refound_fee']-0.05)
    df_alle['lowest_price'] = df_alle['lowest_price'].fillna(0).astype(float).round(2)
    df_alle['total_cost'] = df_alle['totalCost_origin']
    df_alle.drop(['ppve','refound_fee'], axis=1, inplace=True)

    return df_alle


# 不可删除，有airflow测试任务
def write_transport_fee_test():
    sql = """
                 SELECT *
                FROM (
                     select 
                        sku, cast(warehouseId as Int64) warehouseId,  warehouseName, cast(logisticsId as Int64) logisticsId, shipCode, shipName,shippingCost,
                        CAST(remoteExtraFee AS Float64) AS remoteExtraFee, CAST(extraSizeFee AS Float64) AS extraSizeFee,
                        CAST(packTypeFee AS Float64) AS packTypeFee,
                        CAST(overseasFee AS Float64) AS overseasFee,
                        CAST(packFee AS Float64) AS packFee,
                        CAST(taxationFee AS Float64) AS taxationFee,
                        CAST(drawPrice AS Float64) AS drawPrice,
                        CAST(firstCarrierCost AS Float64) AS firstCarrierCost,
                        CAST(dutyCost AS Float64) AS dutyCost,
                        CAST(antidumpFee AS Float64) AS antidumpFee,
                        CAST(overseaPackageFee AS Float64) AS overseaPackageFee,
                        CAST(newPrice AS Float64) AS newPrice,
                        createTime, platform,
                        case when country = 'GB' then 'UK' else country end as country,
                        case when subString(sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                             subString(sku, 4)
                        when subString(sku, 1, 2) in ['DE', 'GB'] then 
                             subString(sku, 3)
                        when subString(sku, -2) in ['DE', 'GB'] then 
                             subString(sku, 1, -2)
                        else sku
                        end as son_sku,
                        -- 20250121 澳洲fastway尾程上浮50%，上浮后再取最优渠道
                        -- 20250822 美国仓fedex部分超长sku，尾程临时上浮
                        case 
                            when shipCode in ('GCHWC_220218003','GCHWC_220218004','WYT_20190924','WYT_20190807')
                            then toFloat64(totalCost) - toFloat64(shippingCost) + 1.5 * toFloat64(shippingCost)
                            when country in ('UK','GB')
                            then toFloat64(totalCost) - toFloat64(shippingCost) + 1.2 * toFloat64(shippingCost)
                            else toFloat64(totalCost)
                        end as totalCost,
                        case 
                            when shipCode in ('GCHWC_220218003','GCHWC_220218004','WYT_20190924','WYT_20190807')
                            then 1.5 * toFloat64(shippingCost)
                            when country in ('UK','GB')
                            then 1.2* toFloat64(shippingCost)
                        else toFloat64(shippingCost)
                        end as shippingCost_new
                    from yibai_oversea.oversea_transport_fee_daily a
                    where 1 = 1
                    -- AND date_id = 20260130
                    AND date_id = (
                        SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily
                        WHERE date_id > toYYYYMMDD(subtractDays(today(), 15))
                    )
                    AND (NOT (warehouseName like '%英国%' and country not in ('UK','GB')))
                    AND (NOT (warehouseName not like '%英国%' and country in ('UK','GB')))
                    AND shipName not like '%GOFO%'
                    AND shipName not like '%%自提UNIUNI%%'
                    AND shipName not like '%%VC%%'
                    AND shipName not in ('Amazon线上-CK1美东2仓本地派送（VC-DF）','CK1-美东2仓UNI本地派送','CK1-美西仓UNI本地派送')
                    AND shipCode not in ('AMAZON_250804003_COPY17616329176207', 'Chukou1_250304011', 'Chukou1_250415004')
                    AND (NOT (toFloat64(shippingCost) < 0.1 and platform not in ('SHOPEE', 'LAZADA'))) 
                    order by toDate(createTime) desc,toDecimal64(totalCost,4) asc 
                    limit 1 by sku, platform, country, toInt64(warehouseId)

                    UNION ALL
                    select
                        sku, warehouseId, warehouseName, 0 logisticsId, shipCode, shipName,shippingCost,
                        0.0 remoteExtraFee, 0.0 extraSizeFee, 0.0 packTypeFee, 0.0 overseasFee, 0.0 packFee, 0.0 taxationFee,
                        0.0 drawPrice, firstCarrierCost, dutyCost, 0.0 antidumpFee, 0.0 overseaPackageFee, 0.0 newPrice,
                        '' createTime, platform,
                        case when shipCountry = 'GB' then 'UK' else shipCountry end as country, '' son_sku, totalCost,
                        shippingCost as shippingCost_new
                    from yibai_oversea.oversea_transport_fee_three_area a
                    where 1 = 1
                    AND date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_three_area)
                    AND platform != 'TEMU'
                    AND shipName not like '%GOFO%'
                    AND shipName not like '%%自提UNIUNI%%'
                    AND shipName not like '%%VC%%'
                    AND shipName not in ('Amazon线上-CK1美东2仓本地派送（VC-DF）','CK1-美东2仓UNI本地派送','CK1-美西仓UNI本地派送')
                    AND shipCode not in ('AMAZON_250804003_COPY17616329176207', 'Chukou1_250304011', 'Chukou1_250415004')
                    AND (NOT (shippingCost < 0.1 and platform not in ('SHOPEE', 'LAZADA')))
                    order by totalCost asc
                    limit 1 by sku, platform, country, toInt64(warehouseId)
                ) a
                WHERE sku = 'YM19365GM811'
                order by toDecimal64(totalCost,4) asc 
                limit 1 by sku, platform, country, toInt64(warehouseId)

    """
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = ck_client.ck_select_to_df(sql)
    df.to_excel('F://Desktop//df_useful_fee.xlsx', index=0)
    print(df.info())

# 运费数据获取
def get_transport_fee():
    """
    获取当前最新运费数据
    """
    """    
    UNION ALL

    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, totalCost_origin,
        firstCarrierCost, dutyCost, 0 available_stock,
        (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name, 0 lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful_shopee
    """
    sql = """
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost_origin,
        CASE 
            WHEN platform IN ('SHOPEE', 'LAZADA') THEN GREATEST(totalCost, totalCost_origin)
            ELSE totalCost 
        END as total_cost,
        CASE 
            WHEN platform IN ('SHOPEE', 'LAZADA') THEN GREATEST(firstCarrierCost, new_firstCarrierCost)
            ELSE firstCarrierCost 
        END as firstCarrierCost,
        dutyCost,  available_stock, shippingCost,
        (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name,lowest_price,
        case when platform = 'OZON' then 'Wildberries' 
        when platform = 'DMSOzon' then 'Wildberries' else platform end as platform, country, warehouse
    FROM oversea_transport_fee_useful
    WHERE platform not in ('WISH')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    df = df.sort_values(by='lowest_price', ascending=False).drop_duplicates(subset=['sku','warehouse_id','platform','country'])

    # allegro运费处理
    df_alle = allegro_transport_fee(df)
    # df_alle[df_alle['country'] == 'CZ'].to_excel('F://Desktop//df_cz.xlsx', index=0)

    # print(df.columns, df_alle.columns)
    df = pd.concat([df, df_alle])

    # 20260202 运费取大仓下有库存子仓最便宜的渠道
    df = df[~df['warehouse_name'].str.contains('精品')]
    df_1 = df[df['available_stock'] > 0]
    df_1 = df_1.sort_values(by='ship_fee', ascending=True)
    df_1 = df_1.drop_duplicates(subset=['sku','warehouse','platform','country'], keep='first')
    df_2 = df[df['available_stock'] <= 0]
    df_2 = df_2.sort_values(by='ship_fee', ascending=True)
    df_2 = df_2.drop_duplicates(subset=['sku','warehouse','platform','country'], keep='first')
    df = pd.concat([df_1, df_2])
    df = df.sort_values(by='available_stock', ascending=False).drop_duplicates(
        subset=['sku','warehouse','platform','country'], keep='first')
    # df.drop(['totalCost_origin','firstCarrierCost','dutyCost','available_stock'], axis=1, inplace=True)
    df.drop(['available_stock','shippingCost'], axis=1, inplace=True)
    df = df.rename(columns={'warehouse_name':'best_fee_warehouse','warehouse_id':'best_fee_id'})
    # # 合并精铺产品运费
    # df_fine_fee = get_fine_fee()
    # df = pd.concat([df, df_fine_fee])
    df['best_fee_id'] = df['best_fee_id'].astype(int)
    print(df.info())
    # 销毁价最低1美金
    df['lowest_price'] = df['lowest_price'].fillna(0).astype(float)
    df['lowest_price'] = np.where(df['lowest_price']<7, 7, df['lowest_price'])

    return df

def get_sku_attr():
    """带电、材质"""

    dict={1:'产品基础属性',2:'产品特殊属性',5:'产品电池属性',25:'产品插头属性',32:'产品违禁属性',
          40:'产品形态属性',66:'订单属性',142:'产品电压属性',168:'产品包装属性'}
    attr_list=pd.DataFrame()
    for i in dict.keys():
        sql=f"""
        SELECT
            a.sku sku, 
            CASE
                WHEN b.parent_id = {i} THEN
                b.attribute_name 
            END AS  `{dict[i]}`
        FROM yibai_prod_base_sync.yibai_prod_sku_select_attr a
        INNER JOIN (
            SELECT sku, warehouse, available_stock
            FROM yibai_oversea.dwm_sku_temp_info
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_sku_temp_info)
            and warehouse = '德国仓'
            -- and available_stock > 0
        ) c ON a.sku = c.sku
        LEFT JOIN yibai_prod_base_sync.yibai_prod_attributes_tms b ON a.attr_value_id = b.id 
        WHERE
            b.parent_id = {i} and a.attr_type_id=1
        """
        conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
        df=df.groupby('sku').apply(lambda x:';'.join(set(x['{}'.format(dict[i])]))).rename('{}'.format(dict[i])).reset_index()
        df=df.drop_duplicates()
        # 属性_df.head(1)
        attr_list = pd.concat([df, attr_list])

    col = ['sku', '产品形态属性', '产品违禁属性','产品电池属性']
    attr_list = attr_list[col]

    # 筛选带电sku
    # attr_list = attr_list[~df['产品形态属性'].str.contains('液体|膏体', na=False)]
    attr_list = attr_list[attr_list['产品电池属性'].isin(['内置电池','配套电池','纯电池及移动电源'])]


    # print(attr_list.info())

    # attr_list.to_excel('F://Desktop//df_attr.xlsx', index=0)

    return attr_list

def get_electric_useful():
    """ 带电sku的运费数据 """
    # 获取sku
    df_sku = get_sku_attr()
    df_sku = df_sku[['sku']]
    df_sku['site3'] = 'CZ'
    df_sku['数量'] = 1
    df_sku['ship_zip'] = 47002
    df_sku['best_warehouse_id'] = 325

    # w_list1 = get_oversea_ship_type_list()
    w_list1 = '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30'
    df_result = pd.DataFrame()
    for (key1, key2), group in df_sku.groupby(['site3', 'best_warehouse_id']):
        print(key1, key2, group.shape)
        group1 = group[['sku', '数量', 'ship_zip']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea('ALLEGRO', key1, key2, w_list1, '')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost',
             'shippingCost', 'firstCarrierCost']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[
            ['sku', '数量','ship_zip', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost',
             'shippingCost', 'firstCarrierCost']]
        # group3 = group3.sort_values(['totalCost'], ascending=True)
        # group3 = group3.drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])

    df_result.to_excel('F://Desktop//df_ele_useful.xlsx', index=0)

    return df_result


if __name__ == "__main__":

    # delete_data()
    # write_transport_fee_data()
    # oversea_files_download(platform_type=2)
    # oversea_files_download(platform_type=1)

    write_transport_fee_data_temu()


    # get_toucheng_price()
    # get_transport_fee()


