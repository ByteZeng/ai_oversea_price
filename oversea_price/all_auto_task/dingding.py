import base64
import datetime
import hashlib
import hmac
from typing import List, Optional
from urllib.parse import quote_plus
from pulic_func.base_api.mysql_connect import connect_to_sql
import time
import pandas as pd
import requests
from sqlalchemy import create_engine


def connect_to_cql(database="", username='', password="", host="", port=""):
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(username, password, host, port, database, 'utf8'))
    conn = engine.connect()  # 创建连接
    return conn


def url(ding_type):
    secret = ''
    access_token=''
    if ding_type=='动销组定时任务推送':
        secret = 'SEC2dbc10f676641297b6fd934a80d2041efa54bfe4ff1621b93aa0b6da9a72d905'
        access_token = 'c3f16626d9c431a60dc7b00154fd7c5f0ce47e76aa5a3bebc4f91250bd1c0d98'
    elif ding_type=='daraz调价':
        secret = 'SECa16e30b19585e6a2e3583eb6a2140422595727b579a07192e197722dcb9564ba'
        access_token = '15c1293e4da188b17904478c4cba806f10bf6333cbb1d47f174025eb9aa4ec31'
    elif ding_type=='daraz调价-new':
        secret = 'SEC26cf6d16a7cb4ffb8af98dcb34b11a8703740b8cb52e29e25daed168f6277707'
        access_token = '72be94a5c9ecc8a3b8f624e14220a10adaa410b5be6b93fa037f06c64dd53c26'
    elif ding_type=='lazada平台-erp调价':
        secret = 'SECbbe93a58a6d0e147c34e09b8184e40361e1ed1b0b8c4528ad8bb91fe4067f268'
        access_token = '939f6e2574249bf5a5be9cebbbcefa6c482030fcebe1cc296bbae287f32e0f76'

    elif ding_type=='shopee平台-erp调价':
        secret = 'SEC897a406db159d7d4c23ff64a08d36f2635ad817655742ba91c99637e7bd4ee75'
        access_token = '8fb417e555d1c3051296e4d017c6239913fd8ce70986b356c4b381c280ed03d3'

    elif ding_type=='亚马逊调价成功率':
        secret = 'SEC64f8a376f8099fc457599955c53ccc0b1c276176df60faf03f09a715db9b7cab'
        access_token = 'd007ec0dc8ce2762959b914135070efb151d48f850542965d2123bec88717bb7'

    elif ding_type=='walmart平台调价':
        secret = 'SEC832cbeb9efeac511f55106b49cecf654f4125fbe0f6cdbdc75ac17230ed09e1f'
        access_token = 'cef404c06828d9a631bb009941a8dabc848bfb9a7c687b5f8500d3779104602c'
    elif ding_type=='EBAY调价LISTING问题沟通群':
        secret = 'SEC168a2eb35d58dbcec639124216299ca74c8fe13d2b27079cc1aeb224004edb57'
        access_token = '36bf0e59ba08c6f76659afc0ec680ec187f9a4a91dcda9faf231fbec6403631c'

    elif ding_type=='CD-调价群':
        secret = 'SEC8fcb4aa051537e949ff3f3572042406ab14938d17a7afccae9720dfe0548e988'
        access_token = '388f264b2b2a0ea307b732237abed77c132d9ae2b2fb4beef5e9b3ec15fca43b'

    elif ding_type=="ebay海外仓调价群":
        secret="SECfb65f1e919aced91956e8648b00b5fb1562eaef9d7cbfddf009c525029a51996"
        access_token="c926786ee1ea0335fe7c6fcca5930b00e68e3aa157522358aabeb5ea88363215"
    elif ding_type == 'EBAY调价LISTING问题沟通群':
        secret = 'SEC168a2eb35d58dbcec639124216299ca74c8fe13d2b27079cc1aeb224004edb57'
        access_token = '36bf0e59ba08c6f76659afc0ec680ec187f9a4a91dcda9faf231fbec6403631c'


    elif ding_type=='定价与订单物流运费获取一致性':
        secret = 'SEC8190f5a1b93aae39b7852cd407b7e91cffac4b378d838d1462058c3cc39a15e9'
        access_token = '044761aa27153fb1c51929857633f2ba018efa5900e9ae56aee0364975db1b1f'

    elif ding_type=='FBC调价跟进':
        secret = 'SEC458d57d140032c7e0df63de4db6c348582a89329aec6bb1082357e31b8f0d631'
        access_token = '52638fa774e929e243c355dafcb4a80fa62b17009d1a49163dcdac316d9d8b7f'

    elif ding_type=='速卖通-平台调价':
        secret = 'SECb25bcac46e8f7438afeedde3b5f0dfdf99420bb3f69a80ee646367fe5bb44863'
        access_token = '5a5f18ce5c6dbe8ce3f067d8239f7af65de3fa4a1822fbdccc784ba5e1bce300'

    elif ding_type=='全平台调价数据分享群':
        secret = 'SEC8ce5d783308e65c1a69b8cd3ebd531e799b6665905f29d6ed58ecba16014358e'
        access_token = '347347b57eeb7a978074b828be05273279612a46895e0ea8ba0b25c4125d4b59'

    elif ding_type == 'amazon调价通知':
        secret = "SECa619b328bff78aec056f4ebdeab57e95ddaf8c86de6ce3c3ff3157d08df3ecd0"
        access_token = "0724b1c29a67520f951082f37534578a35c9f7c7881a90521f06062c8566cff0"


    elif ding_type == 'wish平台调价':
        secret = 'SEC8dd2575c7541d38b9336464c159bb8f64d21079ebb62d476b18d3288c2c4cba0'
        access_token = '580e558b5dc7f8dec84ddddd0dea9de74adce5c8e2b8f51dbc3329525847a9f7'

    elif ding_type == 'ALLEGRO平台调价通知':
        secret = 'SECa56664d4dd8a6b3d7e0eae85d7485c660b566fb751725332901155f94c235509'
        access_token = '446914361cb0a337e2cd5a6a1f52e93d1a87eea0dc31f5cc3e87c068b6950d55'
    
    elif ding_type == 'OZON平台调价通知':
        secret = 'SECef065b50d2d5f260fd8305580b1e055c8d4a16ad5d66ea4ee51d9a3347ee86e4'
        access_token = '173fdc75f600783a9ce21efae3955ce5d0b3d9a216be8e89a94863654d812c1d'

    timestamp = round(time.time() * 1000)
    secret_enc = secret.encode("utf-8")
    string_to_sign = "{}\n{}".format(timestamp,secret)
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

    write_data(task_type, status, text)
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


def write_data(task_type, status, text):
    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""insert into timed_task_record values {now_time, task_type, status, text}"""
    print(sql)
    try:
        conn.execute(sql)
    except:
        pass
    conn.close()


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


