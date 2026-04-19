import shutil
import traceback
import os
from all_auto_task.dingding import *
from all_auto_task.nacos_api import get_user
from pulic_func.base_api.upload_zip import *
from all_auto_task.scripts_ck_client import CkClient

from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api import  mysql_connect as pbm

f_path = os.path.dirname(os.path.abspath(__file__))  # 文件当前路径
f_path1 = os.path.join(f_path, 'competitive_offers')

def time_path():
    # 处理时间
    qu_shu_date = datetime.date.today()
    qu_shu_date_str = qu_shu_date.isoformat()
    create_dir()
    return qu_shu_date_str, f_path1


def create_dir():
    if not os.path.exists(f_path1):
        os.makedirs(f_path1)





def competitive_base_data():
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    sql=f"""select * FROM yibai_order.yibai_amazon_competitivepricing_summary_offers
    where create_at >='{datetime.date.today()-datetime.timedelta(days=1)}'
    """
    df=ck_client.ck_select_to_df(ck_sql=sql)
    print(1)
    print(df.head())
    sql="""select a.id as account_id,a.short_name,a.account_name,a.site,a.account_num,b.group_name
        FROM `yibai_amazon_account` a
        left join yibai_amazon_group b on a.group_id=b.group_id
        where status=1 and b.group_name not like "%%深圳精品%%" and b.group_name not in ("武汉JK组","0组","500组","50组","0")"""
    df_account=pbm.sql_to_pd(sql=sql,database='yibai_system',data_sys='AMAZON刊登库')
    df=df.merge(df_account,on=['account_id'])
    print(df.head())
    return df

def competitive_data_to_distribute_system():
    df=competitive_base_data()

    try:
        qu_shu_date_str, f_path1 = time_path()
        f_path2 = os.path.join(f_path1, f'amazon{qu_shu_date_str}')
        if not os.path.exists(f_path2):
            os.makedirs(f_path2)

        wenjianjia = '亚马逊购物车competitive_offers数据'
        today = qu_shu_date_str.replace('-', '')
        if os.path.exists(os.path.join(f_path1,'AMAZON')):
            shutil.rmtree(os.path.join(f_path1,'AMAZON'))

        f_path_ZIP = os.path.join(f_path1, 'AMAZON', today, wenjianjia)
        if not os.path.exists(f_path_ZIP):
            os.makedirs(f_path_ZIP)
        for item, group in df.groupby(['group_name']):
            excel_name = os.path.join(f_path_ZIP, f'amazon平台competitive购物车数据-{qu_shu_date_str}-整体{item}.xlsx')
            # group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            if group.shape[0] < 1048576:
                group.to_excel(excel_name, sheet_name=f'{item}', index=False)
                print(item)
            else:
                group = group.reset_index(drop=True)
                group['index'] = group.index
                group['index'] = group['index'].apply(lambda m: int(m / 1000000))

                writer = pd.ExcelWriter(excel_name)
                for key, sub_group in group.groupby(['index']):
                    sub_group.drop(['index'], axis=1, inplace=True)
                    sub_group.to_excel(writer, sheet_name=f'{item}{key}', index=False)
                writer.save()
                writer.close()
        # 接口上传模版
        # file_name = os.path.join(f_path2, f'amazon平台海外仓调价上传表(接口上传){qu_shu_date_str}.csv')
        # df.to_csv(file_name, index=False)
        print(6)
        upload_zip_new(f_path1, 'AMAZON')
        # 发送钉钉消息
        # send_msg('海外仓及精品组日常数据处理', '动销组定时任务推送', '海外仓清仓及加快动销与正常品提价数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请注意查收')
    except:
        # send_msg('动销组定时任务推送', '海外仓及精品组日常数据处理',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓amazon数据推送至分发系统出现问题,请及时排查,失败原因详情请查看airflow日志",
        #          mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
        #          status='失败')
        raise Exception(traceback.format_exc())

def upload_zip_new(f_path, plat, userCode='205756'):
    os.chdir(f_path)
    zip_file = zipfile.ZipFile(os.path.join(f_path, f'{plat}.zip'), 'w', compression=zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(plat, topdown=False):
        for name in files:
            # zip_file.write(''.join((root, '\\', name)))
            zip_file.write(os.path.join(root, name))
    zip_file.close()

    # 通过接口传文件到服务器
    dir1 = os.path.join(f_path, f'{plat}.zip')
    uploadBig_new(dir1, userCode=userCode, plat=plat)


def uploadBig_new(fileDir, userCode: str = "205756", plat=None):
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


if __name__=="__main__":
    competitive_data_to_distribute_system()








