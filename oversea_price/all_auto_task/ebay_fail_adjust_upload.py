# -*- coding: utf-8 -*-


import traceback
import shutil
import os
from all_auto_task.dingding import *

from pulic_func.base_api.upload_zip import *

import json
import os
import math
import zipfile
import base64
import datetime
import hashlib
import hmac
from typing import List, Optional
from urllib.parse import quote_plus

import time

import requests

from pulic_func.base_api import  mysql_connect as pbm
from all_auto_task.FBA_newest_stock_upload import fba_newest_stock_to_distribute_system





f_path = os.path.dirname(os.path.abspath(__file__))  # 文件当前路径
f_path1 = os.path.join(f_path, 'ebay_fail_upload')



def time_path():
    # 处理时间
    qu_shu_date = datetime.date.today()
    qu_shu_date_str = qu_shu_date.isoformat()
    create_dir()
    return qu_shu_date_str, f_path1


def create_dir():
    if not os.path.exists(f_path1):
        os.makedirs(f_path1)


def ebay_log_data_to_distribute_system():
    try:
        qu_shu_date_str, f_path1 = time_path()

        # f_path2 = os.path.join(f_path1, f'ebay{qu_shu_date_str}')
        # if not os.path.exists(f_path2):
        #     os.makedirs(f_path2)


        ming="ebay近次调价失败数据"
        if os.path.exists(os.path.join(f_path1,'EB')):
            shutil.rmtree(os.path.join(f_path1,'EB'))
        f_path_ZIP = os.path.join(f_path1,'EB', f'{time.strftime("%Y%m%d")}', ming)
        if not os.path.exists(f_path_ZIP):
            os.makedirs(f_path_ZIP)

        # 调价明细
        # data_sys = '数据部服务器'

        f_path = os.path.dirname(os.path.abspath(__file__))
        # file_name=r"C:\Users\Administrator\Desktop\ebay调价失败数据20220318~20220323.xlsx"
        # sql="""select distinct date from yibai_oversea_eb_upload_log
        # order by date desc limit 1"""
        # df=pbm.sql_to_pd(sql=sql,database='over_sea',data_sys='数据部服务器')
        #最近一次上传的日期
        d1=datetime.date.today()
        d = d1 - datetime.timedelta(days=1)
        sql1=f"""select * from yibai_oversea_eb_upload_log where date='{d}'"""
        df=pbm.sql_to_pd(sql=sql1,database='over_sea',data_sys='数据部服务器')
        if len(df)>0:
            # try:
            #     del df['Unnamed: 0']
            # except:
            #     pass
            print(df.info())
            sql1 = f"""  select 
                    distinct sku, itemid as item_id,auditor_msg
                from 
                    yibai_ebay_modify_price_approval
                where 
                    auditor_time>'{d} 09:00:00' and auditor_time<='{d1} 09:00:00' and create_user_id ='8988' and price_modify_status =3"""
            df1 = pbm.sql_to_pd(sql=sql1, database="yibai_product", data_sys="ebay刊登库")
            df['item_id']=df['item_id'].astype('int')
            df1['item_id']=df1['item_id'].astype('int')
            df = df.merge(df1, on=['item_id', 'sku'],how='left')

            df = df[df['auditor_msg'].isnull()]
            del df['auditor_msg']
            df.drop_duplicates(inplace=True)
            print("ebay昨日调价失败的数目",len(df))
            excel_name = os.path.join(f_path_ZIP, f'ebay调价失败给销售调价明细-{qu_shu_date_str}.xlsx')
            df.to_excel(excel_name, sheet_name=f'ebay昨日调价失败明细', index=False)
            # exit()


            # 压缩上传到销售工作台
            # 生成压缩文件
            upload_zip(f_path1, f'EB', userCode='208254')
            # 发送钉钉消息
            send_msg('EBAY调价LISTING问题沟通群', '动销组定时任务推送', 'ebay近次调价失败发给销售自行调价的数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请核查数据后上传')
        else:
            print('昨天没有调价')

    except:
        raise Exception(traceback.format_exc())


def url(ding_type):
    secret = ''
    access_token=''
    if ding_type=='动销组定时任务推送':
        secret = 'SEC2dbc10f676641297b6fd934a80d2041efa54bfe4ff1621b93aa0b6da9a72d905'
        access_token = 'c3f16626d9c431a60dc7b00154fd7c5f0ce47e76aa5a3bebc4f91250bd1c0d98'
    elif ding_type=='daraz调价':
        secret = 'SECa16e30b19585e6a2e3583eb6a2140422595727b579a07192e197722dcb9564ba'
        access_token = '15c1293e4da188b17904478c4cba806f10bf6333cbb1d47f174025eb9aa4ec31'
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

    elif ding_type == '海外仓及精品组日常数据处理':
        secret = 'SECcc44563c593e67724f6fc688ff8737755639b25fcc0679cf5ea1d9be5199feed'
        access_token = 'a02560731053a18bf07eae2ed04bc8f3a188c1c6d59c1bbc54f274f29a4eee6f'

    elif ding_type == 'wish平台调价':
        secret = 'SEC8dd2575c7541d38b9336464c159bb8f64d21079ebb62d476b18d3288c2c4cba0'
        access_token = '580e558b5dc7f8dec84ddddd0dea9de74adce5c8e2b8f51dbc3329525847a9f7'


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




def upload_zip(f_path, plat, userCode='208254'):
    os.chdir(f_path)
    zip_file = zipfile.ZipFile(os.path.join(f_path, f'{plat}.zip'), 'w', compression=zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(plat, topdown=False):
        for name in files:
            # zip_file.write(''.join((root, '\\', name)))
            zip_file.write(os.path.join(root, name))
    zip_file.close()

    # 通过接口传文件到服务器
    dir1 = os.path.join(f_path, f'{plat}.zip')
    uploadBig(dir1, userCode=userCode, plat=plat)


def uploadBig(fileDir, userCode: str = "208254", plat=None):
    UPLOAD_ZIP_URL = "http://salesworkbench.yibainetwork.com:11408/api/v2/file/center/upload/big"
    UPLOAD_ZIP_ID_URL = "http://salesworkbench.yibainetwork.com:11408/api/v2/file/center/getUploadId"
    UPLOAD_ZIP_SIGN = "c03922e1906d65a0e0dda34eebd55fc7"
    FIXED_TIMESTAMP = 1609835146114

    getUploadIdUrl = f"{UPLOAD_ZIP_ID_URL}?user_code={userCode}&sign={UPLOAD_ZIP_SIGN}&timestamp={FIXED_TIMESTAMP}"
    try:
        r = requests.get(getUploadIdUrl)
        response = json.loads(r.text)
        if response['status'] == 0:
            uploadId = response['data']
            print("文件上传成功")
        else:
            print(plat, '文件上传失败1')
            return False
        params = {
            "filename": uploadId,
        }
        uploadUrl = f"{UPLOAD_ZIP_URL}?user_code={userCode}&sign={UPLOAD_ZIP_SIGN}&timestamp={FIXED_TIMESTAMP}"
        with open(fileDir, 'rb') as f:
            size = os.path.getsize(fileDir)
            total_blob_num = math.ceil(size / 1024000)
            current_blob_num = 1
            while 1:
                content = f.read(1024000)
                if len(content) < 1:
                    break
                files = {"file": content}
                params.update({"total_blob_num": total_blob_num, "blob_num": current_blob_num})
                r = requests.post(uploadUrl, files=files, data=params)
                response = json.loads(r.text)
                # print(response)
                current_blob_num += 1
                if response['code'] != 200:
                    print(plat, '文件上传失败2')
                    return False
            return True
    except:
        print(plat, '文件上传失败3')
        return False




if  __name__=="__main__":

    ebay_log_data_to_distribute_system()
    # send_msg('EBAY调价LISTING问题沟通群', '动销组定时任务推送', 'ebay近次调价失败发给销售自行调价的数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请核查数据后上传')
