# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import pandas as pd
from concurrent.futures._base import as_completed
import json
import os
import re
import base64
import datetime, time
import hashlib
import hmac
from typing import List, Optional
from urllib.parse import quote_plus
import requests
from sqlalchemy import create_engine
from all_auto_task.scripts_ck_client import CkClient
import warnings
warnings.filterwarnings('ignore')

# 数据部服务器

def get_date():
    qu_shu_date = datetime.date.today()
    qu_shu_date_str = qu_shu_date.isoformat()
    date_lsit = qu_shu_date_str.split("-")
    date_new = date_lsit[0] + date_lsit[1] + date_lsit[2]
    return date_new

def ebay_Importprice(group, item):
    url = 'http://ebayapi.yibainetwork.com/services/ebay/ebayapi/modifypriceapproval'
    headers = {'Content-Type': 'application/json', }

    data_post0 = []
    # group.to_excel('rslj.xlsx')
    for i in range(len(group['sku'])):
        data_dict = {
            'sku': str(group['sku'][i]),
            'itemid': int(group['item_id'][i]),
            'start_price_target': float(group['销售价'][i]),
            'target_profit': float(group['目标利润率'][i]),
            'shipping_service_cost': str(group['运费(国内第一运费)'][i]),
            'shipping_service_additional_cost': str(group['额外每件加收运费'][i]),
            'work_number': '209313',
            'sale_status': str(group['sale_status'][i]),
            'warehouse_ids': str(group['warehouse_id'][i])

        }

        # print(data_dict)
        data_post0.append(data_dict)
    data_post = {'data': data_post0}
    # print(data_post)

    while True:
        try:
            res = requests.post(url, json=data_post, headers=headers, timeout=(180, 120)).json()
            print(res)
            if res['code'] == 200:
                break
            else:
                print(f'eaby{item}:', res['message'])
        except:
            print(f'eaby{item}:接口失败，重新上传')
            time.sleep(30)


def ebay_price_post(f_path):
    # file_name=r"C:\Users\Administrator\Desktop\eb上传.xls"
    # file_name = os.path.join(f_path, '调价分表-EB', f'EB价格上传-20220621.xlsx')
    time_today = time.strftime('%Y-%m-%d')
    file_name = os.path.join(f_path, f'EB调价数据检查_{time_today}.xlsx')
    print(file_name)
    df_data = pd.read_excel(file_name)
    # 匹配warehouse_id
    sql = """
        SELECT distinct warehouse_id, warehouse_name as best_warehouse_name, warehouse
        FROM yb_datacenter.v_oversea_stock
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_warehouse_id = ck_client.ck_select_to_df(sql)
    df_data = pd.merge(df_data, df_warehouse_id[['warehouse_id', 'best_warehouse_name']], how='left', on='best_warehouse_name')
    print(df_data.head(5))
    print("ebay调价数目", len(df_data))

    # #把当天的调价数目上传
    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='EB' """
    # conn.execute(delete_sql)
    # insert_sql = f"""insert into yibai_oversea_adjust_number
    # values ('{datetime.date.today().isoformat()}','EB',{len(df_data)})"""
    # conn.execute(insert_sql)
    # print('eb调价数据上传成功')

    df = df_data[["sku", "item_id", "shipping_fee", "price", "gross_profit_rate", "sales_status", 'warehouse_id']]
    df['额外每件加收运费'] = ""
    df["销售价"] = df["price"]
    df["目标利润率"] = df["gross_profit_rate"] * 100
    df["目标利润率"] = df["目标利润率"].round(2)
    df['运费(国内第一运费)'] = df["shipping_fee"]
    df['sale_status'] = df["sales_status"]
    df = df.reset_index(drop=True)
    # 每次传10000个item_id
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 50000))
    df['warehouse_id'] = df['warehouse_id'].astype('int')
    print(df.info())

    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(ebay_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='ebay') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)

def ebay_price_post_test():
    # file_name=r"C:\Users\Administrator\Desktop\eb上传.xls"
    # file_name = os.path.join(f_path, '调价分表-EB', f'EB价格上传-20220621.xlsx')
    time_today = time.strftime('%Y-%m-%d')
    file_name = os.path.join('/data', 'ebay正常品测试数据1019.xlsx')
    print(file_name)
    df_data = pd.read_excel(file_name)
    print("ebay测试调价数目", len(df_data))
    df_data = df_data.rename(columns={'target_price':'price'})
    df_data['price'] = df_data['price'].round(2)
    df = df_data[["sku", "item_id", "shipping_fee", "price", "gross_profit_rate", "sales_status", 'warehouse_id']]
    df['额外每件加收运费'] = ""
    df["销售价"] = df["price"]
    df["目标利润率"] = df["gross_profit_rate"] * 100
    df["目标利润率"] = df["目标利润率"].round(2)
    df['运费(国内第一运费)'] = df["shipping_fee"]
    df['sale_status'] = df["sales_status"]
    df = df.reset_index(drop=True)
    # 每次传10000个item_id
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 50000))
    df['warehouse_id'] = df['warehouse_id'].astype('int')
    print(df.info())

    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(ebay_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='ebay') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)
# #####################################################################################################################
def amazon_Importprice(df1, item):
    # 正式环境
    url0 = 'http://amazon.yibainetwork.com/services/products/Amazonlistingpriceadjustment/Importprice?token='
    # 测试环境
    # url0 = 'http://dp.yibai-it.com:10026/services/products/Amazonlistingpriceadjustment/Importprice?token='
    headers = {'Content-Type': 'application/json', }

    data0 = []
    for i in range(len(df1.index)):
        data1 = {'account_id': int(df1['account_id'][i]),
                 'seller_sku': df1['seller_sku'][i],
                 'price': df1['price'][i],
                 'sale_price': df1['sale_price'][i],
                 'business_price': df1['price'][i],
                 'lowest_price': df1['lowest_price'][i],
                 'start_time': df1['start_time'][i],
                 'end_time': df1['end_time'][i],
                 'reason': df1['sales_status'][i], }
        data0.append(data1)
    data_post = {'user_id': '205532',
                 'price_data': data0}
    data_post0 = json.dumps(data_post)
    # print(data_post0)
    token_str = data_post0 + 'saagdfaz'
    token = hashlib.md5(token_str.encode()).hexdigest()
    # print(token)
    url = url0 + token
    while True:
        try:
            res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600)).json()
            if res['status'] == 1:
                break
            else:
                print(f'amazon{item}:', res['msg'])
        except:
            print(f'amazon{item}:接口失败，重新上传')
            time.sleep(30)

# yxh调价接口
def amazon_importprice_yxh(df1, item):
    # 正式环境
    url0 = 'http://yuexinghamazon.yibainetwork.com/services/products/Amazonlistingpriceadjustment/Importprice?token='
    # 测试环境
    # url0 = 'http://dp.yibai-it.com:10026/services/products/Amazonlistingpriceadjustment/Importprice?token='
    headers = {'Content-Type': 'application/json', }


    data0 = []
    for i in range(len(df1.index)):
        data1 = {'account_id': int(df1['account_id'][i]),
                 'seller_sku': str(df1['SellerSKU'][i]),
                 'price': float(df1['your price'][i]),
                 'sale_price': df1['sale price'][i],
                 'business_price': float(df1['Business Price'][i]),
                 'lowest_price': df1['lowest price'][i],
                 'start_time': df1['start time'][i],
                 'end_time': df1['end time'][i],
                 'reason': df1['调价原因'][i], }
        data0.append(data1)
    data_post = {'user_id':'Y202655',
                 'price_data': data0}
    data_post0 = json.dumps(data_post)
    # print(data_post0)
    token_str = data_post0 + 'saagdfaz'
    token = hashlib.md5(token_str.encode()).hexdigest()
    # print(token)
    url = url0 + token
    while True:
        try:
            res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600)).json()
            # print(res)
            # res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600))
            # print(res.content.decode("utf-8"))
            if res['status'] == 1:
                break
            else:
                print(f'amazon{item}:', res['msg'])
        except:
            print(f'amazon{item}接口失败，重新上传')
            time.sleep(30)

def amazon_price_post(f_path):
    # file_name=os.path.join(f_path, '调价分表-AMAZON', f'AMAZON价格上传-20220621.xlsx')
    time_today = time.strftime('%Y-%m-%d')
    file_name = os.path.join(f_path, f'AMAZON调价数据检查_{time_today}.xlsx')
    # file_name = os.path.join(f_path, '调价分表-AMAZON', f'AMAZON价格上传-{get_date()}.xlsx')
    if os.path.exists(file_name):
        print(file_name)
        df = pd.read_excel(file_name)
    else:
        file_name = file_name.replace('.xlsx', '.csv')
        print(file_name)
        df = pd.read_csv(file_name)
    print('AMAZON上传数量为:', len(df))

    # 上传amazon调价数目
    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='AMAZON' """
    # conn.execute(delete_sql)
    # insert_sql = f"""insert into yibai_oversea_adjust_number (date,platform,number)
    #    values ('{datetime.date.today()}','AMAZON',{len(df)})"""
    # conn.execute(insert_sql)
    # print('AMAZON调价数目统计成功')
    df = df[["account_id", "seller_sku", "price", "sales_status"]]
    df.loc[df['sales_status'].isin(['回调', '正常']), 'price'] = df['price'] + 0.5
    df['sale_price'] = ''
    df['lowest_price'] = ''
    df['start_time'] = ''
    df['end_time'] = ''
    df = df.drop_duplicates()
    df = df.sort_values(['account_id'], ascending=True)

    df.fillna('', inplace=True)
    df = df.reset_index(drop=True)
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 1000))

    # 单线程测试使用
    # for key, group in tqdm(df.groupby(['index'])):
    #     group = group.reset_index(drop=True)
    #     amazon_Importprice(group, key)
    # exit()

    threadPool = ThreadPoolExecutor(max_workers=20)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(amazon_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='AMAZON') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)
    send_msg("海外仓及精品组日常数据处理", '动销组定时任务推送', 'AMAZON调价已上传,请检查数据')

    # # 上传yxh调价数据
    # file_name = os.path.join(f_path, '调价分表-AMAZON', f'AMAZON价格上传-{get_date()}-yxh.xlsx')
    # if os.path.exists(file_name):
    #     print(file_name)
    #     df_yxh = pd.read_excel(file_name)
    # else:
    #     file_name = file_name.replace('.xlsx', '.csv')
    #     print(file_name)
    #     df_yxh = pd.read_csv(file_name)
    # print('yxh_AMAZON上传数量为:', len(df_yxh))
    # df_yxh = df_yxh[["account_id", "seller_sku", "price", "adjust_recent_clean"]]
    # df_yxh.loc[df_yxh['adjust_recent_clean'].isin(['回调', '正常']), 'price'] = df['price'] + 0.5
    # df_yxh['sale_price'] = ''
    # df_yxh['lowest_price'] = ''
    # df_yxh['start_time'] = ''
    # df_yxh['end_time'] = ''
    # df_yxh = df_yxh.drop_duplicates()
    # df_yxh = df_yxh.sort_values(['account_id'], ascending=True)
    #
    # df_yxh.fillna('', inplace=True)
    # df_yxh = df_yxh.reset_index(drop=True)
    # df_yxh['index'] = df_yxh.index
    # df_yxh['index'] = df_yxh['index'].apply(lambda m_data: int(m_data / 1000))
    #
    # threadPool = ThreadPoolExecutor(max_workers=20)
    # thread_list = []
    # for key, group in df_yxh.groupby(['index']):
    #     group = group.reset_index(drop=True)
    #     future = threadPool.submit(amazon_importprice_yxh, group, key)
    #     thread_list.append(future)
    #
    # with tqdm(total=len(thread_list), desc='AMAZON') as pbar:
    #     for future in as_completed(thread_list):
    #         data = future.result()
    #         pbar.update(1)
    # threadPool.shutdown(wait=True)
def amazon_price_post_test():
    # file_name=os.path.join(f_path, '调价分表-AMAZON', f'AMAZON价格上传-20220621.xlsx')
    time_today = time.strftime('%Y-%m-%d')
    file_name = os.path.join('/data', 'AMAZON补充调价数据20231219.xlsx')
    if os.path.exists(file_name):
        print(file_name)
        df = pd.read_excel(file_name)
    else:
        file_name = file_name.replace('.xlsx', '.csv')
        print(file_name)
        df = pd.read_csv(file_name)
    print('AMAZON测试上传数量为:', len(df))
    # df.drop('price', axis=1, inplace=True)
    df = df.rename(columns={'target_price':'price'})
    df['price'] = df['price'].round(2)
    df = df[["account_id", "seller_sku", "price", "sales_status"]]
    df.loc[df['sales_status'].isin(['回调', '正常']), 'price'] = df['price'] + 0.5
    df['sale_price'] = ''
    df['lowest_price'] = ''
    df['start_time'] = ''
    df['end_time'] = ''
    df = df.drop_duplicates()
    df = df.sort_values(['account_id'], ascending=True)

    df.fillna('', inplace=True)
    df = df.reset_index(drop=True)
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 1000))

    # 单线程测试使用
    # for key, group in tqdm(df.groupby(['index'])):
    #     group = group.reset_index(drop=True)
    #     amazon_Importprice(group, key)
    # exit()

    threadPool = ThreadPoolExecutor(max_workers=20)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(amazon_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='AMAZON') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)
    # send_msg("海外仓及精品组日常数据处理", '动销组定时任务推送', 'AMAZON调价已上传,请检查数据')

def url(ding_type):
    secret = ''
    access_token = ''
    if ding_type == '动销组定时任务推送':
        secret = 'SEC2dbc10f676641297b6fd934a80d2041efa54bfe4ff1621b93aa0b6da9a72d905'
        access_token = 'c3f16626d9c431a60dc7b00154fd7c5f0ce47e76aa5a3bebc4f91250bd1c0d98'
    elif ding_type == 'daraz调价':
        secret = 'SECa16e30b19585e6a2e3583eb6a2140422595727b579a07192e197722dcb9564ba'
        access_token = '15c1293e4da188b17904478c4cba806f10bf6333cbb1d47f174025eb9aa4ec31'
    elif ding_type == 'lazada平台-erp调价':
        secret = 'SECbbe93a58a6d0e147c34e09b8184e40361e1ed1b0b8c4528ad8bb91fe4067f268'
        access_token = '939f6e2574249bf5a5be9cebbbcefa6c482030fcebe1cc296bbae287f32e0f76'

    elif ding_type == 'shopee平台-erp调价':
        secret = 'SEC897a406db159d7d4c23ff64a08d36f2635ad817655742ba91c99637e7bd4ee75'
        access_token = '8fb417e555d1c3051296e4d017c6239913fd8ce70986b356c4b381c280ed03d3'

    elif ding_type == '亚马逊调价成功率':
        secret = 'SEC64f8a376f8099fc457599955c53ccc0b1c276176df60faf03f09a715db9b7cab'
        access_token = 'd007ec0dc8ce2762959b914135070efb151d48f850542965d2123bec88717bb7'

    elif ding_type == 'walmart平台调价':
        secret = 'SEC832cbeb9efeac511f55106b49cecf654f4125fbe0f6cdbdc75ac17230ed09e1f'
        access_token = 'cef404c06828d9a631bb009941a8dabc848bfb9a7c687b5f8500d3779104602c'
    elif ding_type == 'EBAY调价LISTING问题沟通群':
        secret = 'SEC168a2eb35d58dbcec639124216299ca74c8fe13d2b27079cc1aeb224004edb57'
        access_token = '36bf0e59ba08c6f76659afc0ec680ec187f9a4a91dcda9faf231fbec6403631c'

    elif ding_type == 'CD-调价群':
        secret = 'SEC8fcb4aa051537e949ff3f3572042406ab14938d17a7afccae9720dfe0548e988'
        access_token = '388f264b2b2a0ea307b732237abed77c132d9ae2b2fb4beef5e9b3ec15fca43b'

    elif ding_type == "ebay海外仓调价群":
        secret = "SECfb65f1e919aced91956e8648b00b5fb1562eaef9d7cbfddf009c525029a51996"
        access_token = "c926786ee1ea0335fe7c6fcca5930b00e68e3aa157522358aabeb5ea88363215"


    elif ding_type == '定价与订单物流运费获取一致性':
        secret = 'SEC8190f5a1b93aae39b7852cd407b7e91cffac4b378d838d1462058c3cc39a15e9'
        access_token = '044761aa27153fb1c51929857633f2ba018efa5900e9ae56aee0364975db1b1f'

    elif ding_type == 'FBC调价跟进':
        secret = 'SEC458d57d140032c7e0df63de4db6c348582a89329aec6bb1082357e31b8f0d631'
        access_token = '52638fa774e929e243c355dafcb4a80fa62b17009d1a49163dcdac316d9d8b7f'

    elif ding_type == '速卖通-平台调价':
        secret = 'SECb25bcac46e8f7438afeedde3b5f0dfdf99420bb3f69a80ee646367fe5bb44863'
        access_token = '5a5f18ce5c6dbe8ce3f067d8239f7af65de3fa4a1822fbdccc784ba5e1bce300'

    elif ding_type == '全平台调价数据分享群':
        secret = 'SEC8ce5d783308e65c1a69b8cd3ebd531e799b6665905f29d6ed58ecba16014358e'
        access_token = '347347b57eeb7a978074b828be05273279612a46895e0ea8ba0b25c4125d4b59'

    elif ding_type == '海外仓及精品组日常数据处理':
        secret = 'SECcc44563c593e67724f6fc688ff8737755639b25fcc0679cf5ea1d9be5199feed'
        access_token = 'a02560731053a18bf07eae2ed04bc8f3a188c1c6d59c1bbc54f274f29a4eee6f'

    elif ding_type == 'wish平台调价':
        secret = 'SEC8dd2575c7541d38b9336464c159bb8f64d21079ebb62d476b18d3288c2c4cba0'
        access_token = '580e558b5dc7f8dec84ddddd0dea9de74adce5c8e2b8f51dbc3329525847a9f7'

    timestamp = round(time.time() * 1000)
    secret_enc = secret.encode("utf-8")
    string_to_sign = "{}\n{}".format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode("utf-8")
    hmac_code = hmac.new(
        secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
    ).digest()
    sign = quote_plus(base64.b64encode(hmac_code))
    url = f"https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}"
    return url

def send_msg(ding_type: str = None,
             task_type: str = None,
             text: str = None,
             mobiles: Optional[List[str]] = None,
             is_all: bool = True,
             status: str = '成功', ):
    res = requests.post(url=url(ding_type),
                        headers={"Content-Type": "application/json"},
                        json={
                            "msgtype": "text",
                            "at": {
                                "atMobiles": mobiles or [],
                                "isAtAll": is_all,
                            },
                            "text": {"content": text},
                        },
                        )
    result = res.json()
    print(result)
    if result.get("errcode") == 0:
        print("发送成功！")

    if ding_type == '动销组定时任务推送':
        send_action(url(ding_type),
                    title="动销任务状态",
                    text='动销任务状态',
                    btn_s=[
                        {
                            'title': '查看详细进度',
                            'actionURL': f'http://sjfxapi.yibainetwork.com:5050/api/price/task/{datetime.date.today().strftime("%Y%m%d")}'
                        }
                    ]
                    )


def send_action(url, title: str = None, text: str = None, btn_s: List = None):
    res = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={
            "actionCard": {
                "title": title,
                "text": text,
                "btnOrientation": "1",
                "btns": btn_s
            },
            "msgtype": "actionCard"
        },
    )
    result = res.json()
    print(result)
    if result.get("errcode") == 0:
        print("发送成功！")


# #####################################################################################################################
def cd_Importprice(group, item):
    url = 'http://smallplatform.yibainetwork.com/services/api/cdiscount/editprice'
    headers = {'Content-Type': 'application/json', }

    data_post0 = []
    for i in range(len(group.index)):
        data_dict = {
            'short_name': group['店铺简称'][i],
            'seller_sku': group['刊登SKU'][i],
            'adjust': group['调整后的价格(支持百分比)'][i],
            'std_fee': float(group['STD运费'][i]),
            'std_add': float(group['STD每件加收'][i]),
            'trk_fee': float(group['TRK运费'][i]),
            'trk_add': float(group['TRK每件加收'][i]),
            'reg_fee': float(group['REG运费'][i]),
            'reg_add': float(group['REG每件加收'][i]),
            'fst_fee': float(group['FST运费'][i]),
            'fst_add': float(group['FST每件加收'][i]),
        }
        data_post0.append(data_dict)
    data_post = {'data': data_post0,
                 'work_number': '205532',
                 'create_time': str(datetime.datetime.now()).split('.')[0]}
    while True:
        try:
            # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
            while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
                time.sleep(30)
            res = requests.post(url, json=data_post, headers=headers, timeout=(180, 120)).json()
            if res['result'] == "成功":
                break
            else:
                print(f'cd{item}:', res['message'])
        except:
            print(f'cd{item}:接口失败，重新上传')
            time.sleep(30)


def cd_price_post(f_path):
    # file_name=r"C:\Users\Administrator\Desktop\cd上传.xlsx"
    # file_name=r"E:\调价任务\海外仓清仓加快动销log上传(王杰交接)\调价分表-CD\CD价格上传前60万条-20211118.xlsx"
    # file_name = os.path.join(f_path, '调价分表-CD', f'CD价格上传-20220621.xlsx')
    # file_name = os.path.join(f_path, '调价分表-CD', f'CD价格上传-{get_date()}.xlsx')
    time_today = time.strftime('%Y-%m-%d')
    file_name = os.path.join(f_path, f'CDISCOUNT调价数据检查_{time_today}.xlsx')
    # 如果写出来的是xlsx就读取xlsx的内容，否则读取csv的内容
    if os.path.exists(file_name):
        print(file_name)
        df = pd.read_excel(file_name)
    else:
        file_name = file_name.replace('.xlsx', '.csv')
        print(file_name)
        df = pd.read_csv(file_name)

    df_data = df.copy()

    # df_data=df_data[df_data['is_up']=='涨价']
    print('CD上传数量为:', len(df_data))
    # 删除CD目前的调价数据
    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='CD' """
    # conn.execute(delete_sql)
    #
    # # 上传CD调价数目
    # insert_sql = f"""insert into yibai_oversea_adjust_number (date,platform,number)
    #    values ('{datetime.date.today().isoformat()}','CD',{len(df)})"""
    # conn.execute(insert_sql)
    # print('CD调价数目统计成功')

    df = df_data[["account_name", "seller_sku", "price"]]
    df = df.sort_values(['account_name'], ascending=True)
    df["调整后的价格(支持百分比)"] = df["price"]
    df["STD运费"] = 0
    df["STD每件加收"] = 0
    df["TRK运费"] = 0
    df["TRK每件加收"] = 0
    df["REG运费"] = 12.99
    df["REG每件加收"] = 12.99
    df["FST运费"] = 999
    df["FST每件加收"] = 999
    df = df.rename(columns={"account_name": "店铺简称", "seller_sku": "刊登SKU"})

    # 接口原因，价格需要传字符串
    df['调整后的价格(支持百分比)'] = df['调整后的价格(支持百分比)'].apply(lambda m: round(m, 2))
    df['调整后的价格(支持百分比)'] = df['调整后的价格(支持百分比)'].astype(str)
    #
    df = df.reset_index(drop=True)
    # 每次传10000
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m / 1000))

    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(cd_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='cd') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)


# #####################################################################################################################
def time_and_sign(WALMART_API_KEY):
    # 验证
    # 1.time
    time0 = int(time.time())
    # 2.sign
    time1 = json.dumps({'time': time0}).replace(' ', '')
    s = f'{time1}{WALMART_API_KEY}'
    sign = hashlib.md5(s.encode('utf8')).hexdigest()
    return time0, sign


def data_walmart_zhekou(group):
    # 20221109从13小时1分钟改为14小时,1124改为20小时
    start_time = (datetime.datetime.today() + datetime.timedelta(hours=20, minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    end_time = (datetime.datetime.today() + datetime.timedelta(days=60)).strftime('%Y-%m-%d %H:%M:%S')
    data_post0 = []
    for i in range(len(group.index)):
        data_dict = {
            'account_id': int(group['account_id'][i]),
            'seller_sku': group['线上SKU'][i],
            'current_price': float(group['价格'][i]),
            'comparison_price': float(group['原价'][i]),
            'current_price_type': group['促销类型'][i],
            'effective_date': start_time,
            'expiration_date': end_time,
            'process_mode': group['processMode值'][i],
        }
        data_post0.append(data_dict)
    data_post = {'data': data_post0,
                 'user_id': '8988', }
    return data_post


def data_walmart_price(group):
    data_post0 = []
    for i in range(len(group.index)):
        data_dict = {
            'account_id': int(group['account_id'][i]),
            'seller_sku': group['线上SKU'][i],
            'price': float(group['价格'][i]),
        }
        data_post0.append(data_dict)
    data_post = {'data': data_post0,
                 'user_id': '8988', }
    return data_post


def walmart_Importprice(group, item):
    # 线上
    url0 = 'http://smallplatformapi.yibainetwork.com/services/walmart/api/mutipromotions'
    url1 = 'http://smallplatformapi.yibainetwork.com/services/walmart/api/mutiprice'
    # url1 = 'http://124.71.63.83/services/walmart/api/mutiprice'
    WALMART_API_KEY = 'YThlM2Y1NjI0ODQyYTQzMGI2MDllZWUzYmQ5NjVjYzY='
    # 测试
    # url0 = 'http://dp.yibai-it.com:30016/services/walmart/api/mutipromotions'
    # url1 = 'http://dp.yibai-it.com:30016/services/walmart/api/mutiprice'
    # WALMART_API_KEY = 'Njk4YzY3MmYzYzc3YjI3MzM2NDc1NjhjZGIyNDMyYzY='
    #
    headers = {'Content-Type': 'application/json', }

    # 修改价格
    while True:
        try:
            # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
            while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
                time.sleep(30)
            #
            time0, sign = time_and_sign(WALMART_API_KEY)
            #
            url = f'{url1}?sign={sign}&time={time0}'

            data_post = data_walmart_price(group)

            #
            res = requests.post(url, json=data_post, headers=headers, timeout=(3600, 3600)).json()
            print(res)
            if res['ack'] == "success" and len(res['error_data']) == 0:
                break
            else:
                print(f'walmart价格{item}:', res['error_data'])
                print(res)
        except:
            print(f'walmart价格{item}:接口失败，重新上传')
            time.sleep(30)

    # 修改折扣
    while True:
        try:
            # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
            while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
                time.sleep(30)
            #
            time0, sign = time_and_sign(WALMART_API_KEY)
            #
            url = f'{url0}?sign={sign}&time={time0}'
            data_post = data_walmart_zhekou(group)
            #
            res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600)).json()
            print(res)
            if res['ack'] == "success" and len(res['error_data']) == 0:
                break
            else:
                print(f'walmart折扣{item}:', res['error_data'])
        except:
            print(f'walmart折扣{item}:接口失败，重新上传')
            time.sleep(30)

def walmart_price_post(f_path):
    # file_name=os.path.join(f_path, '调价分表-WALMART', f'WALMART价格上传-20220621.xlsx')
    # file_name = os.path.join(f_path, '调价分表-WALMART', f'WALMART价格上传-{get_date()}.xlsx')
    time_today = time.strftime('%Y-%m-%d')
    file_name = os.path.join(f_path, f'WALMART调价数据检查_{time_today}.xlsx')
    print(file_name)
    df_data = pd.read_excel(file_name)

    # walmart 账户 状态有问题的不调价
    engine_59 = create_engine("mysql+pymysql://songzhanzhao:szzyibai%^&8@124.71.220.143:3306/yibai_system?charset=utf8mb4")
    conn_59 = engine_59.connect()
    sql_account_status = '''
    SELECT id as account_id, status FROM yibai_system.yibai_walmart_account
    '''
    df_status = pd.read_sql(sql_account_status, conn_59)
    df_data = df_data.merge(df_status)
    df_data = df_data[df_data['status'] == 1]
    df_data.drop('status', axis=1, inplace=True)

    # df_data=df_data[df_data['is_up']=='涨价']
    df_data = df_data.drop_duplicates()
    print('walmart', df_data.head())
    print("walmart调价数量:", len(df_data))

    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='WALMART' """
    # conn.execute(delete_sql)
    #
    # # 上传调价数目
    # insert_sql = f"""insert into yibai_oversea_adjust_number (date,platform,number)
    #    values ('{datetime.date.today().isoformat()}','WALMART',{len(df_data)})"""
    # conn.execute(insert_sql)
    # print('walmart调价数目统计成功')

    df = df_data[["account_id", "seller_sku", "price"]]
    df = df.rename(columns={"price": "价格", "seller_sku": "线上SKU"})
    df["原价"] = df["价格"] + 5
    df["促销类型"] = "REDUCED"
    df["processMode值"] = "UPSERT"
    df = df.reset_index(drop=True)
    df = df.sort_values(['account_id'], ascending=True)
    df = df.reset_index(drop=True)
    # 每次传100
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m / 100))

    # 单线程测试使用(walmart接口实现方式要求只能单线程)
    for key, group in tqdm(df.groupby(['index'])):
        group = group.reset_index(drop=True)
        walmart_Importprice(group, key)


# #####################################################################################################################

# def ali(f_path):
#     # from price_post_api import price_post_api
#
#     # file_name=r"C:\Users\Administrator\Desktop\ali上传.xlsx"
#     file_name = os.path.join(f_path, '调价分表-ALI', f'ALI价格上传-{get_date()}.xlsx')
#     print(file_name)
#     df = pd.read_excel(file_name)
#     print("ali调价数目:", len(df))
#
#     #
#     delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='ALI' """
#     conn.execute(delete_sql)
#
#     # 上传调价数目
#     insert_sql = f"""insert into yibai_oversea_adjust_number (date,platform,number)
#        values ('{datetime.date.today().isoformat()}','ALI',{len(df)})"""
#     conn.execute(insert_sql)
#     print('ali调价数目统计成功')
#
#     # 接口传数据
#     price_post_api().ali_price_post(df)


# #####################################################################################################################
# #####################################################################################################################



def main():
    time_today = time.strftime('%Y-%m-%d')
    # # f_path = os.path.dirname(os.path.abspath(__file__))
    f_path = os.path.join('/data', time_today)
    print(f_path)
    ebay_price_post(f_path)
    amazon_price_post(f_path)
    # ali(f_path)
    cd_price_post(f_path)
    walmart_price_post(f_path)

    # print('AMAZON补充链接上传')
    # # ebay_price_post_test()
    # amazon_price_post_test()
    print('done!')

    # p = Poo
    # l(8)
    # for platform in ['amazon', 'ebay', 'cd', 'walmart']:  # , 'walmart', 'ali', 'cd'
    #     p.apply_async(func=platform_fu, args=(platform, f_path))
    # p.close()
    # p.join()


if __name__ == "__main__":
    # record_txt = os.getcwd() + "/record.txt"
    # try:
    main()
    print('运行成功')
# except Exception as err:
#     print('运行失败')
#     with open(record_txt, 'w', encoding='utf-8') as f:
#         f.write(u"运行失败"+'\n'+"失败原因：" + traceback.format_exc())
