import traceback
import shutil
import os
from all_auto_task.dingding import *
from all_auto_task.nacos_api import get_user
from pulic_func.base_api.upload_zip import *
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_sql, sql_to_pd

f_path = os.path.dirname(os.path.abspath(__file__))  # 文件当前路径
f_path1 = os.path.join(f_path, 'over_sea')

from pulic_func.base_api import  mysql_connect as pbm


def get_now_weekday():
    day_of_week = datetime.datetime.now().isoweekday()
    return day_of_week


def time_path():
    # 处理时间
    qu_shu_date = datetime.date.today()
    qu_shu_date_str = qu_shu_date.isoformat()
    create_dir()
    return qu_shu_date_str, f_path1


def create_dir():
    if not os.path.exists(f_path1):
        os.makedirs(f_path1)



def upload_zip_adjust(f_path, plat, userCode='208254'):
    os.chdir(f_path)
    zip_file = zipfile.ZipFile(os.path.join(f_path, f'{plat}.zip'), 'w', compression=zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(plat, topdown=False):
        for name in files:
            # zip_file.write(''.join((root, '\\', name)))
            zip_file.write(os.path.join(root, name))
    zip_file.close()

    # 通过接口传文件到服务器
    dir1 = os.path.join(f_path, f'{plat}.zip')
    uploadBig_adjsut(dir1, userCode=userCode, plat=plat)


def uploadBig_adjsut(fileDir, userCode: str = "208254", plat=None):
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


def ali_log_data_to_distribute_system():
    try:
        qu_shu_date_str, f_path1 = time_path()

        f_path2 = os.path.join(f_path1, f'ali{qu_shu_date_str}')
        if not os.path.exists(f_path2):
            os.makedirs(f_path2)

        wenjianjia = '海外仓清仓+加快动销调价明细(清仓、加快动销、回调)-ALI'
        if os.path.exists(os.path.join(f_path1, 'ALI')):
            shutil.rmtree(os.path.join(f_path1, 'ALI'))
        today = qu_shu_date_str.replace('-', '')
        f_path_ZIP = os.path.join(f_path1, 'ALI', today, wenjianjia)
        if not os.path.exists(f_path_ZIP):
            os.makedirs(f_path_ZIP)

        # data_sys='海外仓_王杰'
        print(get_now_weekday())
        w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='数据部服务器')['qingcang_weekday'].astype('int').tolist()

        if get_now_weekday() in w_df:
            sql1 = f"""SELECT * FROM yibai_oversea_ali_adjust_log where DATE='{time.strftime('%Y-%m-%d')}'"""
        else:
            sql1 = f"""SELECT * FROM yibai_oversea_ali_adjust_log where DATE='{time.strftime('%Y-%m-%d')}' and IS_UP='涨价'
                   """
        df = sql_to_pd(sql=sql1, database='over_sea', data_sys='数据部服务器')
        print('ALI剔除前的数量', len(df))
        df = tichu_all(df)
        print('剔除后的数量', len(df))


        # 万邑通有库存的情况
        # wyt_stock=wyt_stocks()
        # wyt_stock['库存情况'] = '万邑通有库存'  ###对于2.14降价的sku只降不长
        # df3 = reduce_price_sku()
        # df3 = df3[df3['platform'] == "ALI"]
        #
        # df3 = df3.merge(wyt_stock, on=['sku', 'warehouse'])
        # df3 = df3[['sku', 'warehouse', 'price', "净利润率", "totalCost"]]
        # df3.columns = ['sku', 'warehouse', '调整价', "净利润率", "totalCost"]
        # df3.sort_values(by=['totalCost'], inplace=True)
        # df3.drop_duplicates(subset=['sku', 'warehouse'], inplace=True)
        # df = df.merge(df3, on=['sku', 'warehouse'], how="left")
        # df['参与调价整价之前的价格'] = df['price']
        # df['price'] = df.apply(lambda x: min(x['price'], x['调整价']), axis=1)
        # df = df[(df['price'] - df['当前实际售价'] >= 0.3) | (df['price'] - df['当前实际售价'] <= 0.3)]
        # df['is_up'] = "涨价"
        # df.loc[df['price'] - df['当前实际售价'] < 0, "is_up"] = "降价"

        # 20220708仅谷仓有库存的链接最低净利不能低于-18%
        # 20220720 改成 -0.2
        a=-0.2
        df_gc=gc_zhangjia(a=-0.2)

        df_gc_w = df_gc[df_gc['platform'] == 'ALI']
        print('ALI仅谷仓数量', len(df_gc_w))
        df_gc_w = df_gc_w[['sku', 'warehouse', f'谷仓{a}净利定价', 'totalCost']]
        df_gc_w.sort_values(by=['totalCost'], inplace=True)
        df_gc_w.drop_duplicates(subset=['sku', 'warehouse'], inplace=True)
        df_gc_w.columns = ['sku', 'warehouse', f'谷仓{a}净利定价', '运费']

        df = df.merge(df_gc_w, on=['sku', 'warehouse'], how='left')
        df.loc[df['price'] < df[f'谷仓{a}净利定价'], 'price'] = df[f'谷仓{a}净利定价']

        df = df[(df['price'] - df['当前实际售价']).abs() >= 0.3]
        # #剔除利润率1%以内波动的数据
        df['涨幅率']=(df['price']-df['当前实际售价'])/df['当前实际售价']
        df = df.drop(['涨幅率'],axis=1)


        # data_sys='小平台刊登库2'
        sql2 = f'SELECT id AS account_id,short_name as 账号简称 from yibai_aliexpress_account_qimen'
        account = sql_to_pd(database='yibai_system', data_sys='小平台刊登库2', sql=sql2)
        df = df.merge(account, on=['account_id'], how='left')
        #
        df = df.reset_index(drop=True)
        df['index'] = df.index
        df['index'] = df['index'].apply(lambda m: int(m / 500000))
        for item, group in df.groupby(['index']):
            group.drop(['index'], axis=1, inplace=True)
            excel_name = os.path.join(f_path_ZIP, f'ali平台海外仓调价明细-{qu_shu_date_str}-整体{item}.xlsx')
            group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            del group
        df.drop(['index'], axis=1, inplace=True)

        df = df.drop_duplicates()
        file_name = os.path.join(f_path2, f'ali平台海外仓调价上传表{qu_shu_date_str}.csv')
        df.to_csv(file_name, index=False)

        # 压缩上传到销售工作台
        # 生成压缩文件
        upload_zip_adjust(f_path1, 'ALI', userCode='208254')
        # 发送钉钉消息
        send_msg('速卖通-平台调价', '动销组定时任务推送', '海外仓清仓及加快动销与正常品提价数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请注意查收')
    except:
        send_msg('动销组定时任务推送', '速卖通-平台调价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓ali数据推送至分发系统出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())

def ebay_price_listing_config(df):
    sql = """
    SELECT item_id,sku_all as sku,source_type 
    FROM yibai_ebay_modify_price_listing_config 
    WHERE status=1
    """
    df1 = sql_to_pd(database='yibai_product',sql=sql,data_sys='ebay刊登库')
    df1['source_type'].fillna('', inplace=True)
    df1 = df1[df1['source_type'].str.contains('-1|2')]
    df1 = df1[['item_id','sku']].drop_duplicates()
    df1['需要不调价'] = '是'

    df['item_id'] = df['item_id'].apply(lambda m: int(m))
    df1['item_id'] = df1['item_id'].apply(lambda m: int(m))
    #分成两类
    #如果sku有多个，则通过item_id去除
    df2=df1[df1['sku'].str.contains(',')]
    df2=df2[['item_id','需要不调价']].drop_duplicates()
    df = df.merge(df2, on=['item_id'], how='left')
    df = df[df['需要不调价'] != '是']
    df.drop(['需要不调价'], axis=1, inplace=True)
    #其他德则通过item_id和sku进行去除
    df=df.merge(df1,on=['item_id','sku'],how='left')
    df = df[df['需要不调价'] != '是']
    df.drop(['需要不调价'], axis=1, inplace=True)
    return df


def tichu1(df):
    df1=sql_to_pd(sql="""select * from yibai_amazon_oversea_no_adjust_sku""", database='over_sea', data_sys='数据部服务器')
    df1.drop_duplicates(inplace=True)
    df1['不调价']=1
    df1['sku']=df1['sku'].astype('str')
    df['sku']=df['sku'].astype('str')
    df=df.merge(df1,on=['sku','warehouse'],how='left')
    for i in df.columns:
        if str(i)=='is_up' or str(i)=='IS_UP':
            df.drop(index=df[(df['不调价']==1)&(df[i]=='降价')].index,inplace=True)
    del df['不调价']
    df.drop_duplicates(inplace=True)
    return df

def tichu_all(df):
    df1=sql_to_pd(sql="""select * from yibai_oversea_no_adjust_sku""", database='over_sea', data_sys='数据部服务器')
    df1.drop_duplicates(inplace=True)
    df1.columns = ['sku', 'warehouse']
    df1['不调价']=1
    df1['sku']=df1['sku'].astype('str')
    df['sku']=df['sku'].astype('str')
    df=df.merge(df1,on=['sku','warehouse'],how='left')
    df=df[df['不调价']!=1]
    del df['不调价']
    df.drop_duplicates(inplace=True)
    return df


# 2022/03/04仅谷仓有库存的要求净利不能低于-0.2,在3月7号改至-0.18,3月9号改为-15%，20220708改为-18%
def gc_zhangjia(a=-0.2):
    sql1 = f"""select A.sku,A.warehouse,B.warehouse_id,B.best_warehouse
        from (select gc.sku,gc.warehouse,   
            case when wyt.warehosue_stock is null then '仅谷仓有库存' else '万邑通有库存'  end as is_gc
            from (select sku,warehouse,sum(warehouse_stock) as warehosue_stock 
            from warehouse_stock_charges
            where date="{datetime.date.today()}" and oversea_type="GC" and warehouse_stock>0
            group by sku,warehouse) gc 
            left join  
            (select sku,warehouse,sum(warehouse_stock) as warehosue_stock    
            from warehouse_stock_charges   
            where date="{datetime.date.today()}" and oversea_type="WYT" and warehouse_stock>0

            group by sku,warehouse)wyt on gc.sku=wyt.sku and gc.warehouse=wyt.warehouse
            having is_gc='仅谷仓有库存') A
			inner join (select SKU as sku,warehouse,warehouse_id,best_warehouse
			from over_sea_age_new where date='{datetime.date.today()}' and available_stock>0) B on A.sku=B.sku and A.warehouse=B.warehouse """
    df1 = sql_to_pd(sql=sql1, database='over_sea', data_sys='数据部服务器')
    print("谷仓有库存的sku数目为:", len(df1))
    sql2 = """select  a.sku,a.warehouse,a.warehouseId as warehouse_id,
            a.platform,a.country,totalCost,newPrice,pay_fee,paypal_fee,vat_fee,extra_fee,platform_zero,r.rate
            from oversea_transport_fee_useful a
            inner join yibai_platform_fee b on a.platform=b.platform and a.country=b.site
            inner join domestic_warehouse_clear.erp_rate r on a.country=r.country COLLATE utf8mb4_general_ci
            where a.platform<>'AMAZON'"""
    df2 = sql_to_pd(sql=sql2, database='over_sea', data_sys='数据部服务器')
    df2.sort_values(by=['totalCost'], inplace=True)
    df2.drop_duplicates(subset=['sku', 'warehouse', 'country', 'platform', 'warehouse_id'], inplace=True)
    df3 = df1.merge(df2, on=['sku', 'warehouse', 'warehouse_id'], how='left')
    df3.loc[df3['platform'] == 'ALI', 'rate'] = 6
    df3[f'谷仓{a}净利定价'] = (df3['newPrice'] + df3['totalCost']) / (
                1 - df3['pay_fee'] - df3['paypal_fee'] - df3['vat_fee'] - df3['extra_fee']
                - df3['platform_zero'] -a) / df3['rate']
    df3[f'谷仓{a}净利定价'] = df3[f'谷仓{a}净利定价'].round(1) - 0.01
    df3['毛利润率'] = df3['platform_zero'] - a
    return df3


###针对万邑通超300天库龄库存占总万邑通库存30%以上，且预计可售天数>15天
###OR超万邑通360天库龄库存占总万邑通库存5%以上。则一次MAX(调价至平台要求净利润率价格与兜底价格的一半，目标净利润率定价一半）作为上限价格
# 20220616改为只要万邑通有库存改为平台要净利率价格与销毁价一半
def reduce_price_sku():
    # df=wyt_jiangjia()
    # df1=df[((df['300天库存占比是否超30%']=="是")&(df['可售天数大于15天']=="是"))|((df['360天库存占比是否超5%']=="是"))]
    # sku_list=tuple(set(df1['sku']))
    sql1 = f"""select A.*,case 
            when platform='ALI' and (A.platform_must_price+A.lowest_price)/2>A.platform_must_price/2 
            then round((A.platform_must_price+A.lowest_price)/2/6,1)-0.01
            when platform='ALI' and (A.platform_must_price+A.lowest_price)/2<=A.platform_must_price/2
            then round(A.platform_must_price/2,1)/6-0.01 
            when (A.platform_must_price+A.lowest_price)/2>A.platform_must_price/2 
            then round((A.platform_must_price+A.lowest_price)/2/A.rate,1)-0.01
            else round(A.platform_must_price/2,1)/A.rate-0.01
            end as price          
            from (select a.sku,a.warehouse,a.new_price,a.best_warehouse,a.warehouse_id,b.platform,b.country,b.totalCost
            ,b.lowest_price,r.rate,c.pay_fee,c.paypal_fee,c.extra_fee,c.vat_fee,c.platform_zero,c.platform_must_percent, 
            (a.new_price+b.totalCost)/(1-c.pay_fee-c.paypal_fee-c.extra_fee-c.vat_fee-c.platform_zero-c.platform_must_percent) as platform_must_price,
            (a.new_price+b.totalCost)/(1-c.pay_fee-c.paypal_fee-c.extra_fee-c.vat_fee-c.platform_zero) as platform_zero_price
            from over_sea_age_new a
            left join oversea_transport_fee_useful b on a.sku=b.sku and a.warehouse_id=b.warehouseId
            inner join  yibai_platform_fee c on b.country=c.site and b.platform=c.platform
            left join domestic_warehouse_clear.erp_rate r  on b.country=r.country COLLATE utf8mb4_general_ci
            where a.date='{datetime.date.today()}' and a.available_stock>0 and best_warehouse like '%%万邑通%%'
            order by b.totalCost)A
        """
    # 20220616改为只要万邑通有库存改为平台要净利率价格与销毁价1/4
    # 20220621改为仅万邑通有库存的sku负利润直接调到销毁价，正利润加快动销的澳洲仓不调，其他仓也一律调到销毁价
    sql2 = f"""select A.*,case 
            when platform='ALI' then round(A.lowest_price/6,1)-0.01


            else round(A.lowest_price/A.rate,1)-0.01

            end as price          
            from (select a.sku,a.warehouse,a.new_price,a.best_warehouse,a.warehouse_id,b.platform,b.country,b.totalCost
            ,b.lowest_price,r.rate,c.pay_fee,c.paypal_fee,c.extra_fee,c.vat_fee,c.platform_zero,c.platform_must_percent, 
            (a.new_price+b.totalCost)/(1-c.pay_fee-c.paypal_fee-c.extra_fee-c.vat_fee-c.platform_zero-c.platform_must_percent) as platform_must_price,
            (a.new_price+b.totalCost)/(1-c.pay_fee-c.paypal_fee-c.extra_fee-c.vat_fee-c.platform_zero) as platform_zero_price
            from over_sea_age_new a
            left join oversea_transport_fee_useful b on a.sku=b.sku and a.warehouse_id=b.warehouseId
            inner join  yibai_platform_fee c on b.country=c.site and b.platform=c.platform
            left join domestic_warehouse_clear.erp_rate r  on b.country=r.country COLLATE utf8mb4_general_ci
            where a.date='{datetime.date.today()}' and a.available_stock>0 and best_warehouse like '%%万邑通%%'
            order by b.totalCost)A
        """
    df2 = sql_to_pd(sql=sql2, database='over_sea', data_sys='数据部服务器')
    df2.drop_duplicates(subset=['sku', 'warehouse', 'country', 'warehouse_id', 'platform'], inplace=True)
    # df3=df1.merge(df2,on=['sku','warehouse'])
    df3 = df2.copy()
    df3["净利润率"] = (df3['price'] * df3['rate'] - df3['totalCost'] - df3['new_price']) / (df3['price'] * df3['rate']) - \
                  df3['pay_fee'] - \
                  df3['paypal_fee'] - df3['vat_fee'] - df3['extra_fee'] - df3['platform_zero']
    df3.loc[df3['platform'] == 'ALI', '净利润率'] = (df3['price'] * 6 - df3['totalCost'] - df3['new_price']) / (
                df3['price'] * 6) - df3['pay_fee'] - \
                                                df3['paypal_fee'] - df3['vat_fee'] - df3['extra_fee'] - df3[
                                                    'platform_zero']

    df3 = df3[df3['platform'] != "WISH"]
    df3['净利润率'] = df3['净利润率'].round(2)
    df3['调价时间'] = f"{datetime.date.today()}"
    # pd_to_sql(df3,table="over_age_300_plus_reduce_price_detail",database="over_sea",data_sys="海外仓_王杰",if_exists="append")
    return df3

def wyt_stocks():
    # 万邑通有库存的情况
    sql1 = """select sku,warehouse,sum(available_stock) as warehouse_stock
                              from yb_datacenter.v_oversea_stock 
                              where warehouse_name like '%万邑通%' and available_stock>0
                              group by sku,warehouse
                             """

    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    wyt_stock = ck_client.ck_select_to_df(sql1)
    return wyt_stock





def amazon_log_data_to_distribute_system():
    try:
        qu_shu_date_str, f_path1 = time_path()
        f_path2 = os.path.join(f_path1, f'amazon{qu_shu_date_str}')
        if not os.path.exists(f_path2):
            os.makedirs(f_path2)

        wenjianjia = '海外仓清仓+加快动销调价明细(清仓、加快动销、回调)-amazon'
        if os.path.exists(os.path.join(f_path1, 'AMAZON')):
            shutil.rmtree(os.path.join(f_path1, 'AMAZON'))
        today = qu_shu_date_str.replace('-', '')
        f_path_ZIP = os.path.join(f_path1, 'AMAZON', today, wenjianjia)
        if not os.path.exists(f_path_ZIP):
            os.makedirs(f_path_ZIP)

        w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='数据部服务器')['qingcang_weekday'].astype('int').tolist()
        print(get_now_weekday())
        if get_now_weekday() in w_df:
            sql = f"""SELECT * FROM yibai_oversea_amazon_adjust_log where DATE='{time.strftime('%Y-%m-%d')}'"""
        else:
            sql = f"""SELECT * FROM yibai_oversea_amazon_adjust_log where DATE='{time.strftime('%Y-%m-%d')}' and IS_UP='涨价'
                   """
        df = sql_to_pd(sql=sql, database='over_sea', data_sys='数据部服务器')
        # 因为3RG账号限制，暂时不调3RG这个账号
        df = df[df['short_name'] != '3RG']

        print('AMAZON剔除前的数量', len(df))
        # df = tichu_all(df)
        # print('剔除第一种的数量',len(df))

        sql1 = f"""select sku as sku,warehouse,after_profit from oversea_age_{time.strftime("%Y%m%d")}_dtl"""
        df_dtl = sql_to_pd(sql=sql1, database='over_sea', data_sys='数据部服务器')
        df = df.merge(df_dtl, on=['sku', 'warehouse'], how='left')
        df = tichu1(df)
        print('剔除第二种的数量', len(df))
        print(df.head())
        print('降价的数据', df[df['is_up'] == '降价'])

        #20220708取消仅万邑通有库存的调整价设置
        # # 仅万邑通有库存的情况
        # sql1 = """select *
        #         from (select sku,warehouse,sum(available_stock) as warehouse_stock
        #          from yb_datacenter.v_oversea_stock
        #          where warehouse_name like '%万邑通%' and available_stock>0 group by sku,warehouse) a
        #          where concat(sku,warehouse)
        #          not in
        #          (select distinct concat(sku,warehouse)
        #          from yb_datacenter.v_oversea_stock
        #          where warehouse_name not like '%万邑通%' and available_stock>0)
        #          """
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='yb_datacenter')
        # wyt_stock = ck_client.ck_select_to_df(ck_sql=sql1)
        # wyt_stock['库存情况'] = '仅万邑通有库存'
        # print(len(wyt_stock))
        #
        #
        # ###对于2.14降价的sku只降不长
        # df3 = reduce_price_sku()
        # # 只调整万邑通有库存的sku
        # df3 = df3.merge(wyt_stock, on=['sku', 'warehouse'])
        # df3 = df3[df3['platform'] == "AMAZON"]
        # df3 = df3[['sku', 'warehouse', 'country', 'price']]
        # df3.columns = ['sku', 'warehouse', 'site', '调整价']
        # df3['site'] = df3['site'].str.lower()
        # df3.drop_duplicates(inplace=True)
        # df = df.merge(df3, on=['sku', 'warehouse', 'site'], how="left")
        # print("匹配参考价之后的数量", len(df))
        # df.to_excel('zsdsdf.xlsx')
        # df['参与调整价之前的调后价格'] = df['price']
        # ###((df['亚马逊市场价'] + (df['money_off'] + df['coupon_handling_fee'] + df['promotion_amount'])) / (
        # ###  1 - df['percentage_off'] - df['your_price_point'] - df['promotion_percent'])).round(1) - 0.01
        # print(df.info())
        # if len(df[df['调整价'].notnull()]) > 0:
        #     df['调整价1'] = ((df['调整价'] + (df['money_off'] + df['coupon_handling_fee'] + df['promotion_amount'])) / (
        #             1 - df['percentage_off'] - df['your_price_point'] - df['promotion_percent'])).round(1) - 0.01
        #     # 20220621澳洲仓仅万邑通有库存负利润加快动销sku直接调到销毁价，其他仓仅万邑通有库存正负利润一律调到销毁价
        #     df.loc[(df['price'] > df['调整价1']) & (df['adjust_recent_clean'].isin(['负利润加快动销', '正利润加快动销']))
        #            & (df['warehouse'] != '澳洲仓'), 'price'] = df['调整价1']
        #     df.loc[(df['price'] > df['调整价1']) & (df['adjust_recent_clean'] == '负利润加快动销')
        #            & (df['warehouse'] == '澳洲仓'), 'price'] = df['调整价1']
        #
        # df.loc[(df['price'] - df['online_price'] >= 0), 'is_up'] = '涨价'
        # df.loc[(df['price'] - df['online_price'] < 0), 'is_up'] = '降价'
        # df = df[(df['price'] - df['online_price']).abs() >= 0.3]
        #
        # df.drop_duplicates(subset=['account_id', 'seller_sku'], inplace=True)

        # sql1 = f"""select a.sku,a.`大仓` as warehouse,a.site,u.totalCost,
        #             case when a.`亚马逊市场价`>=round(u.lowest_price*1.2/r.rate,1)-0.01
        #              then a.`亚马逊市场价`
        #              else round(u.lowest_price*1.2/r.rate,1)-0.01 end as "亚马逊市场价"
        #             from ebay_walmart_amazon_market_value a
        #             left join over_sea_age_new b on a.sku=b.SKU and a.`大仓`=b.warehouse and b.date='{datetime.date.today()}'
        #             inner join oversea_transport_fee_useful u on a.sku=u.sku and b.warehouse_id=u.warehouseId
        #             and a.site=u.country and u.platform="AMAZON"
        #             left join domestic_warehouse_clear.erp_rate r on r.country=u.country  COLLATE utf8mb4_general_ci
        #             where a.`亚马逊市场价` is not null
        #             order by u.totalCost"""
        # df_old = sql_to_pd2(sql=sql1)
        # df1 = sql_to_pd2(sql="""select sku,site,`大仓` as warehouse,`复核结果` from amazon_newest_market_value""")
        # df_old.sort_values(by=['totalCost'], inplace=True)
        # df_old.drop_duplicates(subset=['sku', 'warehouse', 'site'], inplace=True)
        # df2 = df1.merge(df_old, on=['sku', 'warehouse', 'site'], how='outer')
        # del df2['totalCost']
        #
        # df2.columns = ['sku', 'site', 'warehouse', '复核结果', '原亚马逊市场价']
        # df2['亚马逊市场价'] = df2['复核结果']
        # df2.loc[df2['复核结果'].isnull(), '亚马逊市场价'] = df2['原亚马逊市场价']
        #
        # df1 = df2.copy()
        #
        # df1.drop_duplicates(subset=['sku', 'warehouse', 'site'], inplace=True)
        # df1 = df1.merge(wyt_stock, on=['sku', 'warehouse'])
        # df1['site'] = df1['site'].str.lower()
        # df = df.merge(df1, on=['sku', 'warehouse', 'site'], how='left')
        #
        #
        # df.loc[(df['adjust_recent_clean'] == '负利润加快动销') & (df['no_coupon_price'] > df['亚马逊市场价']) & (
        #     df['亚马逊市场价'].notnull()), 'price'] \
        #     = ((df['亚马逊市场价'] + (df['money_off'] + df['coupon_handling_fee'] + df['promotion_amount'])) / (
        #             1 - df['percentage_off'] - df['your_price_point'] - df['promotion_percent'])).round(1) - 0.01
        # df.loc[(df['adjust_recent_clean'] == '负利润加快动销') & (df['price'] - df['online_price'] < 0), 'is_up'] = '降价'
        # df.loc[(df['adjust_recent_clean'] == '负利润加快动销') & (df['price'] - df['online_price'] >= 0), 'is_up'] = '涨价'

        # 亚马逊阶梯定价
        df.sort_values(by=['sku', 'warehouse', 'site', 'price'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['price'] = df['price'] + df.index.values % 30 * 0.01
        df = df[(df['price'] - df['online_price']).abs() >= 0.3]

        # #剔除利润率1%以内波动的数据
        df['涨降幅度'] = 999
        df.loc[df['online_price'] > 0, '涨降幅度'] = (df['price'] - df['online_price']) / df['online_price']
        df = df.loc[(df['涨降幅度'] >= 0.01) | (df['涨降幅度'] <= -0.01) ]
        df = df.drop(['涨降幅度'],axis=1)
        
        #Amazon账号异常不调1027
        df = df.loc[(df['account_id'] != 228) & (df['account_id'] != 265) & (df['account_id'] != 881) & (df['account_id'] != 1349)
        & (df['account_id'] != 6594) & (df['account_id'] != 6614) & (df['account_id'] != 6615)& (df['account_id'] != 930)& (df['account_id'] != 6851)]

        # 海外仓冲业绩不调账号（2023-03-30）
    # ONS
    # SRS
    # AOS
    # FOD
    # TOS
    # SAP
    # HGP
    # GOB
    # CUL
    # VIS
    # CAD
    # GIN
    # CTT
    # AGL
    # DLF
    # FHS
    # FFS
    # LOS
    # MSS
    # TJS
    # DPF
    # CAB
    # BIN
    # ISP
    # LVE
        # HLF
        df = df.loc[
        (df['account_id'] != 2347)
        & (df['account_id'] != 1026)
        & (df['account_id'] != 75)
        & (df['account_id'] != 6488)
        & (df['account_id'] != 1013)
        & (df['account_id'] != 6700)
        & (df['account_id'] != 7074)
        & (df['account_id'] != 55786)
        & (df['account_id'] != 39680)
        & (df['account_id'] != 301)
        & (df['account_id'] != 7976)
        & (df['account_id'] != 3912)
        & (df['account_id'] != 56709)
        & (df['account_id'] != 39650)
        & (df['account_id'] != 35804)
        & (df['account_id'] != 5764)
        & (df['account_id'] != 8818)
        & (df['account_id'] != 929)
        & (df['account_id'] != 1636)
        & (df['account_id'] != 4850)
        & (df['account_id'] != 4065)
        & (df['account_id'] != 27592)
        & (df['account_id'] != 53475)
        & (df['account_id'] != 6612)
        & (df['account_id'] != 3446)
        & (df['account_id'] != 7089)
        ]

        for item, group in df.groupby(['group_name']):
            excel_name = os.path.join(f_path_ZIP, f'amazon平台海外仓调价明细-{qu_shu_date_str}-整体{item}.xlsx')
            # group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            if group.shape[0] < 1048576:
                group.to_excel(excel_name, sheet_name=f'{item}', index=False)
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

        #核价程序上传
        he_df=sql_to_pd(sql=f"select * from amazon_listing_price_detail where DATE='{datetime.date.today()}'",
                        database='over_sea', data_sys='数据部服务器')
        for item, group in he_df.groupby(['group_name']):
            excel_name = os.path.join(f_path_ZIP, f'amazon平台海外仓核价明细-{qu_shu_date_str}-整体{item}.xlsx')
            # group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            if group.shape[0] < 1048576:
                group.to_excel(excel_name, sheet_name=f'{item}', index=False)
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
        file_name = os.path.join(f_path2, f'amazon平台海外仓调价上传表(接口上传){qu_shu_date_str}.csv')
        df.to_csv(file_name, index=False)

        upload_zip_adjust(f_path1, 'AMAZON', userCode='208254')
        # 发送钉钉消息
        send_msg('amazon调价通知', '动销组定时任务推送', '海外仓清仓及加快动销与正常品提价数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请注意查收')
    except:
        send_msg('动销组定时任务推送', '海外仓及精品组日常数据处理',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓amazon数据推送至分发系统出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def cd_log_data_to_distribute_system():
    try:
        qu_shu_date_str, f_path1 = time_path()

        f_path2 = os.path.join(f_path1, f'cd{qu_shu_date_str}')
        if not os.path.exists(f_path2):
            os.makedirs(f_path2)

        wenjianjia = '海外仓清仓+加快动销调价明细(清仓、加快动销、回调)-CDISCOUNT'
        if os.path.exists(os.path.join(f_path1, 'CDISCOUNT')):
            shutil.rmtree(os.path.join(f_path1, 'CDISCOUNT'))
        today = qu_shu_date_str.replace('-', '')
        f_path_ZIP = os.path.join(f_path1, 'CDISCOUNT', today, wenjianjia)
        if not os.path.exists(f_path_ZIP):
            os.makedirs(f_path_ZIP)

        # data_sys = '海外仓_王杰'
        print(get_now_weekday())
        w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='数据部服务器')['qingcang_weekday'].astype('int').tolist()
        if get_now_weekday() in w_df:
            sql = f"""SELECT * FROM yibai_oversea_cd_adjust_log where DATE='{time.strftime('%Y-%m-%d')}' 
                """
        else:
            sql = f"""SELECT * FROM yibai_oversea_cd_adjust_log where DATE='{time.strftime('%Y-%m-%d')}' and IS_UP='涨价'
                """
        df = sql_to_pd(sql=sql, database='over_sea', data_sys='数据部服务器')
        print('CD剔除前的数量', len(df))
        # df = tichu_all(df)

        # # 仅万邑通有库存的情况
        # sql1 = """select *
        #         from (select sku,warehouse,sum(available_stock) as warehouse_stock
        #          from yb_datacenter.v_oversea_stock
        #          where warehouse_name like '%万邑通%' and available_stock>0 group by sku,warehouse) a
        #          where concat(sku,warehouse)
        #          not in
        #          (select distinct concat(sku,warehouse)
        #          from yb_datacenter.v_oversea_stock
        #          where warehouse_name not like '%万邑通%' and available_stock>0)
        #          """
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='yb_datacenter')
        # wyt_stock = ck_client.ck_select_to_df(ck_sql=sql1)
        # wyt_stock['库存情况'] = '仅万邑通有库存'
        #
        # ###对于2.14降价的sku只降不长
        # df3 = reduce_price_sku()
        # df3 = df3.merge(wyt_stock, on=['sku', 'warehouse'])
        # df3 = df3[df3['platform'] == "CDISCOUNT"]
        # df3 = df3[['sku', 'warehouse', 'country', 'price']]
        # df3.columns = ['sku', 'warehouse', 'site', '调整价']
        # df3.drop_duplicates(inplace=True)
        # df = df.merge(df3, on=['sku', 'warehouse', 'site'], how="left")
        # df['参与调价整价之前的价格'] = df['price']
        # # 20220621澳洲仓仅万邑通有库存负利润加快动销sku直接调到销毁价，其他仓仅万邑通有库存正负利润一律调到销毁价
        #
        # df.loc[
        #     (df['price'] > df['调整价']) & (df['adjust_recent_clean'].isin(['负利润加快动销', '正利润加快动销'])) & (df['调整价'].notnull())
        #     & (df['warehouse'] != '澳洲仓'), 'price'] = df['调整价']
        #
        # df.loc[(df['price'] > df['调整价']) & (df['adjust_recent_clean'] == '负利润加快动销') & (df['调整价'].notnull())
        #        & (df['warehouse'] == '澳洲仓'), 'price'] = df['调整价']

        # 仅谷仓有库存的链接最低净利不能低于-15%，20220708改为不能低于-18%
        a = -0.2
        df_gc = gc_zhangjia(a=-0.2)

        df_gc_w = df_gc[df_gc['platform'] == 'CDISCOUNT']
        df_gc_w = df_gc_w[['sku', 'warehouse', 'country', f'谷仓{a}净利定价', 'totalCost']]
        df_gc_w.columns = ['sku', 'warehouse', 'site', f'谷仓{a}净利定价', '运费']
        df = df.merge(df_gc_w, on=['sku', 'warehouse', 'site'], how='left')
        df.loc[df['price'] < df[f'谷仓{a}净利定价'], 'price'] = df[f'谷仓{a}净利定价']

        df = df[(df['price'] - df['online_price']).abs() >= 0.3]
        # #剔除利润率1%以内波动的数据
        df = df[df['warehouse']!='英国仓']
        df['涨幅率']=(df['post_price']-df['online_price'])/df['online_price']
        df = df.loc[(df['涨幅率'] >= 0.01) | (df['涨幅率'] <= -0.01) ]
        df = df.drop(['涨幅率'],axis=1)
        
        #1124只执行负利润加快动销和负利润加快动销的回调以及正利润加快动销的回调
        df = df.loc[(df['adjust_recent_clean'] != '正常')&(df['adjust_recent_clean'] != '正利润加快动销')]


        # 海外仓冲业绩不调账号（2023-03-30）
        # SUP
        # BIN
        df = df.loc[
            (df['account_id'] != 271)
            & (df['account_id'] != 313)
        ]

        df = df.reset_index(drop=True)
        df['index'] = df.index
        df['index'] = df['index'].apply(lambda m: int(m / 500000))
        for item, group in df.groupby(['index']):
            group.drop(['index'], axis=1, inplace=True)
            excel_name = os.path.join(f_path_ZIP, f'cd平台海外调价明细-{qu_shu_date_str}-整体{item}.xlsx')
            group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            del group
        df.drop(['index'], axis=1, inplace=True)

        df = df.drop_duplicates()
        df = df.reset_index(drop=True)
        file_name = os.path.join(f_path2, f'cd平台海外仓调价上传表{qu_shu_date_str}.csv')
        df.to_csv(file_name, index=False)

        # 提取英仓的数据
        t_uk = time.strftime('%Y%m%d')
        sql_uk = f"""
                                   select

                A.*,

                case

                    when A.platform = 'EB'

                    and A.country in ('UK', 'DE', 'FR', 'ES', 'IT') then round(((1-0.15-0.015-0.167-0.04)* RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    when A.platform = 'EB'

                    and A.country not in ('UK', 'DE', 'FR', 'ES', 'IT') then round((0.805 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    ELSE round((0.82 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                END AS '实际毛利润率',

                case

                    when A.platform = 'AMAZON' then round((0.62 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    when A.platform = 'ALI' then round((0.79 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    when A.platform = 'WISH' then round((0.62 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    when A.platform in ('CDISCOUNT', 'WALMART') then round((0.67 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    when A.platform = 'EB'

                    and A.country in ('UK', 'DE', 'FR', 'ES', 'IT') then round((0.482 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    when A.platform = 'EB' then round((0.652 * RMB定价-totalCost-new_price)/ RMB定价,

                    2)

                    ELSE NULL

                END AS '实际净利润率',

                CASE

                    WHEN A.platform = 'EB' THEN round((A.RMB定价 + 0.8)/ B.rate + 0.35,

                    1)-0.01

                    WHEN A.platform IN ('AMAZON', 'WALMART', 'CDISCOUNT') THEN round(A.RMB定价 / B.rate,

                    1)-0.01

                    WHEN A.platform = 'ALI' THEN round(A.RMB定价 / 6,

                    1)-0.01

                    WHEN A.platform = 'WISH' THEN A.RMB定价-0.01

                    ELSE NULL

                END AS '本币定价',

                CASE

                    WHEN A.platform IN ('AMAZON', 'EB', 'WALMART', 'CDISCOUNT') THEN round(A.净利润百分之五RMB定价_AMAZON / B.rate,

                    1)-0.01

                    ELSE NULL

                END AS 'Amazon5%%净利润本币定价',

                CASE

                    WHEN A.platform IN ('AMAZON', 'EB', 'WALMART', 'CDISCOUNT') THEN round(A.净利润百分之十RMB定价_AMAZON / B.rate,

                    1)-0.01

                    ELSE NULL

                END AS 'Amazon10%%净利润本币定价',

                case

                    when A.RMB定价-2 <= ROUND(lowest_price,

                    1)-0.01 then '已达到销毁价格'

                    else '未达到销毁价格'

                end as '是否达到销毁价格'

            from

                (

                SELECT

                    A.*,

                    B.totalCost,

                    B.lowest_price,

                    B.platform,

                    B.country,

                    B.shipName,

                    GREATEST(

                    (case

                        when A.adjust_recent_clean IN ('清仓', '负利润加快动销', '正利润加快动销')

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(GREATEST(A.after_profit,0),

                        0))>= B.lowest_price THEN ROUND((A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(GREATEST(A.after_profit,0),

                        0)),

                        1)-0.01

                        when A.adjust_recent_clean IN ('清仓', '负利润加快动销', '正利润加快动销')

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-IFNULL(GREATEST(A.after_profit,0),

                        0))<B.lowest_price THEN ROUND(B.lowest_price,

                        1)-0.01

                        when A.adjust_recent_clean in ('回调', '正常')

                        AND is_chargo = '是'

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent + 0.02)<B.lowest_price THEN ROUND(B.lowest_price,

                        1)-0.01

                        when A.adjust_recent_clean in ('回调', '正常')

                        AND is_chargo = '是'

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent + 0.02)>= B.lowest_price THEN ROUND((A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent + 0.02),

                        1)-0.01

                        when A.adjust_recent_clean in ('回调', '正常')

                        AND is_chargo = '否'

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)<B.lowest_price THEN ROUND(B.lowest_price,

                        1)-0.01

                        when A.adjust_recent_clean in ('回调', '正常')

                        AND is_chargo = '否'

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent)>= B.lowest_price THEN ROUND((A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent),

                        1)-0.01

                        ELSE NULL

                    END ),

                    B.limit_price_rmb) AS RMB定价,

                    case

                        when B.platform = 'AMAZON'

                        and A.adjust_recent_clean in ('回调', '正常')

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05)<B.lowest_price THEN ROUND(B.lowest_price,

                        1)-0.01

                        when B.platform = 'AMAZON'

                        and A.adjust_recent_clean in ('回调', '正常')

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05)>= B.lowest_price THEN ROUND((A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.05),

                        1)-0.01

                        ELSE NULL

                    END AS '净利润百分之五RMB定价_AMAZON',

                    case

                        when B.platform = 'AMAZON'

                        and A.adjust_recent_clean in ('回调', '正常')

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1)<B.lowest_price THEN ROUND(B.lowest_price,

                        1)-0.01

                        when B.platform = 'AMAZON'

                        and A.adjust_recent_clean in ('回调', '正常')

                        AND (A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1)>= B.lowest_price THEN ROUND((A.new_price + totalCost)/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-0.1),

                        1)-0.01

                        ELSE NULL

                    END AS '净利润百分之十RMB定价_AMAZON'

                FROM

                    oversea_age_{t_uk}_dtl A

                INNER JOIN oversea_transport_fee_useful B ON

                    A.SKU = B.SKU

                    AND A.warehouse_id = B.warehouseId

                    and platform = 'CDISCOUNT'

                    and warehouseName LIKE '%%英%%'

                INNER join yibai_platform_fee C on

                    B.platform = C.platform

                    AND B.country = C.site)A

            left join domestic_warehouse_clear.erp_rate B on

                A.country = B.country COLLATE utf8mb4_general_ci
            """
        df_uk = sql_to_pd(sql=sql_uk, database='over_sea', data_sys='数据部服务器')
        #不调价剔除
        df_uk.rename(columns={'SKU':'sku'},inplace=True)
        df_uk=tichu_all(df_uk)

        df_uk.to_excel(os.path.join(f_path_ZIP, f'cd平台海外调价明细-{t_uk}-英国仓.xlsx'))

        # 压缩上传到销售工作台
        # 生成压缩文件
        upload_zip_adjust(f_path1, 'CDISCOUNT', userCode='208254')
        # 发送钉钉消息
        send_msg('CD-调价群', '动销组定时任务推送', '海外仓清仓及加快动销与正常品提价数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请注意查收')
    except:
        send_msg('动销组定时任务推送', 'CD-调价群',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓cd数据推送至分发系统出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def ebay_log_data_to_distribute_system():
    try:
        qu_shu_date_str, f_path1 = time_path()

        f_path2 = os.path.join(f_path1, f'ebay{qu_shu_date_str}')
        if not os.path.exists(f_path2):
            os.makedirs(f_path2)

        wenjianjia = '海外仓清仓+加快动销调价明细(清仓、加快动销、回调)-EB'

        if os.path.exists(os.path.join(f_path1,'EB')):
            shutil.rmtree(os.path.join(f_path1,'EB'))

        today = qu_shu_date_str.replace('-', '')
        f_path_ZIP = os.path.join(f_path1, 'EB', today, wenjianjia)
        if not os.path.exists(f_path_ZIP):
            os.makedirs(f_path_ZIP)

        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

        # 调价明细
        # data_sys = '数据部服务器'
        w_df = conn.read_sql("""select * from qingcang_weekday""")['qingcang_weekday'].astype('int').tolist()
        print(get_now_weekday())
        if get_now_weekday() in w_df:
            sql = f"""SELECT * FROM yibai_oversea_eb_adjust_log where DATE='{time.strftime('%Y-%m-%d')}'"""
        else:
            sql = f"""SELECT * FROM yibai_oversea_eb_adjust_log where DATE='{time.strftime('%Y-%m-%d')}' and IS_UP='涨价'
                """
        df = conn.read_sql(sql)
        df.rename(columns={"name": 'warehouse'}, inplace=True)

        print('EBAY剔除前的数量', len(df))

        # df = tichu_all(df)
        # print('剔除第一种的数量', len(df))
        # df = ebay_price_listing_config(df)
        # print('剔除第二种的数量', len(df))
        # 20211221当前定价（售价）>150 且属于法国仓，暂不进行涨价操作
        df = df.loc[~((df['online_price'] > 150) & (df['warehouse'] == '法国仓')), :]
        # # 仅万邑通有库存的情况
        # sql1 = """select *
        #                        from (select sku,warehouse,sum(available_stock) as warehouse_stock
        #                         from yb_datacenter.v_oversea_stock
        #                         where warehouse_name like '%万邑通%' and available_stock>0 group by sku,warehouse) a
        #                         where concat(sku,warehouse)
        #                         not in
        #                         (select distinct concat(sku,warehouse)
        #                         from yb_datacenter.v_oversea_stock
        #                         where warehouse_name not like '%万邑通%' and available_stock>0)
        #                         """
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='yb_datacenter')
        # wyt_stock = ck_client.ck_select_to_df(ck_sql=sql1)
        # wyt_stock['库存情况'] = '仅万邑通有库存'
        #
        # ###对于2.14降价的sku只降不长
        # ###对于2.14降价的sku只降不长
        # df3 = reduce_price_sku()
        # df3 = df3[df3['platform'] == "EB"]
        # df3 = df3.merge(wyt_stock, on=['sku', 'warehouse'])
        # df3 = df3[['sku', 'warehouse', 'country', 'price', "净利润率", "totalCost", "platform_zero"]]
        # df3.columns = ['sku', 'warehouse', 'site', '调整价', "净利润率", "totalCost", "platform_zero"]
        # df3.drop_duplicates(inplace=True)
        # df = df.merge(df3, on=['sku', 'warehouse', 'site'], how='left')
        # df['参与调价整价之前的价格'] = df['post_price']
        #
        # # 20220621澳洲仓仅万邑通有库存负利润加快动销sku直接调到销毁价，其他仓仅万邑通有库存正负利润一律调到销毁价
        #
        # df.loc[(df['post_price'] > (df['调整价'] - df['shipping_fee'])) & (
        #     df['adjust_recent_clean'].isin(['负利润加快动销', '正利润加快动销']))
        #        & (df['调整价'].notnull()) & (df['warehouse'] != '澳洲仓')
        # , "acc_profit"] = df['净利润率'] + df['platform_zero']
        #
        # df.loc[(df['post_price'] > (df['调整价'] - df['shipping_fee'])) & (df['adjust_recent_clean'] == '负利润加快动销')
        #        & (df['调整价'].notnull()) & (df['warehouse'] == '澳洲仓')
        # , "acc_profit"] = df['净利润率'] + df['platform_zero']
        #
        # print(df.info())
        # df['调整价'] = df['调整价'].astype('float')
        # df.loc[(df['post_price'] > (df['调整价'] - df['shipping_fee'])) &
        #        (df['adjust_recent_clean'].isin(['负利润加快动销', '正利润加快动销'])) & (df['调整价'].notnull())
        #        & (df['warehouse'] != '澳洲仓'), "post_price"] = (df['调整价'] - df['shipping_fee']).round(1) - 0.01
        #
        # df.loc[(df['post_price'] > (df['调整价'] - df['shipping_fee'])) & (df['adjust_recent_clean'] == '负利润加快动销')
        #        & (df['调整价'].notnull()) & (df['warehouse'] == '澳洲仓')
        # , "post_price"] = (df['调整价'] - df['shipping_fee']).round(1) - 0.01
        #
        # df.loc[(df['post_price'] - df['online_price'] + df['shipping_fee'] >= 0)
        # , "IS_UP"] = "涨价"
        # df.loc[(df['post_price'] - df['online_price'] + df['shipping_fee'] < 0)
        # , "IS_UP"] = "降价"
        #
        # # 正负利润当前价格高于市场价的降回市场价,低于市场价只降不涨
        # sql1 = """
        #            select a.sku,a.`大仓` as warehouse,a.site as country,
        #
        #        case when a.`ebay市场价`>=round(u.lowest_price*1.2/r.rate,1)-0.01
        #        then a.`ebay市场价`
        #        else round(u.lowest_price*1.2/r.rate,1)-0.01 end as "ebay市场价"
        #        ,
        #        case when a.`ebay市场价`>=round(u.lowest_price*1.2/r.rate,1)-0.01
        #        then round((a.`ebay市场价`*r.rate-u.newPrice-u.totalCost)/(a.`ebay市场价`*r.rate)-f.paypal_fee-f.pay_fee-f.vat_fee-f.extra_fee,4)
        #        else round((u.lowest_price*1.2-b.new_price-u.totalCost)/(u.lowest_price*1.2) -f.paypal_fee-f.pay_fee-f.vat_fee-f.extra_fee,4)
        #        end as "ebay市场价毛利率"
        #        from ebay_walmart_amazon_market_value a
        #        inner join over_sea_age_new b on a.sku=b.SKU and a.`大仓`=b.warehouse
        #        inner join oversea_transport_fee_useful u on a.sku=u.sku and b.warehouse_id=u.warehouseId
        #        and a.site=u.country and u.platform="EB"
        #        inner join yibai_platform_fee f on f.site=u.country and f.platform="EB"
        #        inner join domestic_warehouse_clear.erp_rate r on r.country=u.country  COLLATE utf8mb4_general_ci
        #        where a.`ebay市场价` is not null
        #        order by u.totalCost
        #            """
        # df1 = sql_to_pd2(sql=sql1)
        # df1.drop_duplicates(subset=['sku', 'warehouse', 'country'], inplace=True)
        # df1 = df1.merge(wyt_stock, on=['sku', 'warehouse'])
        # df1 = df1[df1['ebay市场价毛利率'].notnull()]
        # df1['ebay市场价'] = df1['ebay市场价'].astype('float')
        # df1['ebay市场价毛利率'] = df1['ebay市场价毛利率'].astype('float64')
        # df = df.merge(df1, on=['sku', 'warehouse', 'country'], how='left')
        #
        # # 对于有市场价的只降不涨
        # df.loc[(df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) & (df['ebay市场价'].notnull())
        #        & (df['online_price'] <= df['ebay市场价']) & (df['IS_UP'] == '涨价'), '剔除'] = 1
        # # 剔除掉低于市场价且涨价的
        # df = df[df['剔除'] != 1]
        # del df['剔除']
        # df.loc[(df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) &
        #        (df['online_price'] > df['ebay市场价']) & (df['ebay市场价'] - df['shipping_fee'] <= 1), 'post_price'] = 1
        #
        # df.loc[(df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) & (df['online_price'] > df['ebay市场价'])
        # , 'acc_profit'] = df['ebay市场价毛利率'].round(4)
        # df.loc[(df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) &
        #        (df['online_price'] > df['ebay市场价']) & (df['ebay市场价'] - df['shipping_fee'] > 1)
        # , 'post_price'] = (df['ebay市场价'] - df['shipping_fee']).round(1) - 0.01
        #
        # df.loc[(df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) & (df['online_price'] > df['ebay市场价'])
        # , 'IS_UP'] = '降价'

        # df['调后价格'] = df['post_price']
        # df['调前价格'] = df['start_price']
        # df['post_price'] = df['调前价格']
        # df['start_price'] = df['调后价格']
        # df.drop(columns=['调后价格', '调前价格'], inplace=True)
        # 仅谷仓有库存的链接最低净利不能低于-15%
        a=-0.2
        df_gc=gc_zhangjia(a=-0.2)
        df_gc_w = df_gc[df_gc['platform'] == 'EB']
        print('EB仅谷仓数量', len(df_gc_w))
        df_gc_w = df_gc_w[['sku', 'warehouse', 'country', f'谷仓{a}净利定价', 'totalCost', '毛利润率']]

        df = df.merge(df_gc_w, on=['sku', 'warehouse', 'country'], how='left')
        df.loc[df['post_price'] < df[f'谷仓{a}净利定价'], 'acc_profit'] = df['毛利润率']
        df.loc[df['post_price'] < df[f'谷仓{a}净利定价'], 'post_price'] = df[f'谷仓{a}净利定价']
        
        # 20221009-ebay英国BGS,GFL,GF0;德国AUP,DSP,GAO最低净利不能低于-10%进行兜底测试(广告费率加到10%)
        sql2 = f"""select  a.sku,a.warehouse,a.warehouseId as warehouse_id,
                        a.platform,a.country,totalCost,newPrice,pay_fee,paypal_fee,vat_fee,extra_fee,platform_zero,r.rate
                        from oversea_transport_fee_useful a
                        inner join yibai_platform_fee b on a.platform=b.platform and a.country=b.site
                        inner join domestic_warehouse_clear.erp_rate r on a.country=r.country COLLATE utf8mb4_general_ci
                        where a.platform<>'AMAZON'"""
        df2 = conn.read_sql(sql2)

        df4 = df.loc[(df['country'] == 'UK')]
        df4 = df4.loc[(df4['account_id'] == 81) | (df4['account_id'] == 85) | (df4['account_id'] == 80)]
        df5 = df.loc[(df['country'] == 'DE')]
        df5 = df5.loc[(df5['account_id'] == 331) | (df5['account_id'] == 278) | (df5['account_id'] == 446)]
        df_te = pd.concat([df4, df5])
        df_te = df_te[['sku','account_id','country','warehouse', 'warehouse_id', 'best_warehouse']]
        df_te = df_te.merge(df2, on=['sku', 'warehouse', 'warehouse_id','country'], how='left')
        df_te = df_te[df_te['platform'] == 'EB']
        df_te['-10%净利定价'] = (df_te['newPrice'] + df_te['totalCost']) / (
            1 - df_te['pay_fee'] - df_te['paypal_fee'] - df_te['vat_fee'] - df_te['extra_fee']
            - df_te['platform_zero'] +0.1) / df_te['rate']
        df_te['-10%净利定价'] = df_te['-10%净利定价'].round(1) - 0.01
        df_te['te毛利润率'] = df_te['platform_zero'] - 0.1
        print('EB净利-10%数量', len(df_te))
        df_te = df_te[['sku', 'account_id','warehouse', 'country', '-10%净利定价', 'te毛利润率']]
        df = df.merge(df_te, on=['sku','account_id','warehouse', 'country'], how='left')
        df.loc[df['post_price'] < df['-10%净利定价'], 'acc_profit'] = df['te毛利润率']
        df.loc[df['post_price'] < df['-10%净利定价'], 'post_price'] = df['-10%净利定价']-df['shipping_fee']
        df = df.drop(['-10%净利定价','te毛利润率'],axis=1)
        
        
        # 20221009-ebay英国TPG,GHB;德国EGG,MOL,进行利润率33%测试(广告费率加到10%)
        df6 = df.loc[(df['country'] == 'UK')]
        df6 = df6.loc[(df6['account_id'] == 171) | (df6['account_id'] == 181)| (df6['account_id'] == 905)| (df6['account_id'] == 774) ]
        df7 = df.loc[(df['country'] == 'DE')]
        df7 = df7.loc[(df7['account_id'] == 889) | (df7['account_id'] == 671)| (df7['account_id'] == 448)| (df7['account_id'] == 824)]
        df_te2 = pd.concat([df6, df7])
        df_te2 = df_te2[['sku','account_id','country','warehouse', 'warehouse_id', 'best_warehouse']]
        df_te2 = df_te2.merge(df2, on=['sku', 'warehouse', 'warehouse_id','country'], how='left')
        df_te2 = df_te2[df_te2['platform'] == 'EB']

        df_te2['净利率']=0.33-df_te2['platform_zero']
        df_te2['对应净利定价'] = (df_te2['newPrice'] + df_te2['totalCost']) / (
            1 - df_te2['pay_fee'] - df_te2['paypal_fee'] - df_te2['vat_fee'] - df_te2['extra_fee']
            - df_te2['platform_zero'] - df_te2['净利率']) / df_te2['rate']
        df_te2['对应净利定价'] = df_te2['对应净利定价'].round(1) - 0.01
        df_te2['33%毛利润率'] = 0.33
        print('EB毛利33%数量', len(df_te2))
        
        df_te2 = df_te2[['sku', 'account_id','warehouse', 'country','净利率','对应净利定价','33%毛利润率']]
        df = df.merge(df_te2, on=['sku','account_id','warehouse', 'country'], how='left')
        df = df.drop_duplicates()
        df.loc[df['33%毛利润率'].isna()==False, 'acc_profit'] = df['33%毛利润率']
        df.loc[df['对应净利定价'].isna()==False, 'post_price'] = df['对应净利定价']-df['shipping_fee']
        df = df.drop(['净利率','对应净利定价','33%毛利润率'],axis=1)
        df.loc[(df['post_price'] - df['online_price'] + df['shipping_fee'] >= 0), "IS_UP"] = "涨价"
        df.loc[(df['post_price'] - df['online_price'] + df['shipping_fee'] < 0), "IS_UP"] = "降价"
        
        #20220317 GY6账号暂时不调价
        df=df.loc[df['account_id'] != 978]
        # 20220921 HSD/VAG/JUS/DIF/FOA/DRO/STE账号暂时不调价，澳仓提利润率测试高推广费效果，利润率都调至28%以上
        
        df = df.loc[(df['account_id'] != 923) & (df['account_id'] != 855) & (df['account_id'] != 393) & (df['account_id'] != 710)
        & (df['account_id'] != 357) & (df['account_id'] != 599) & (df['account_id'] != 602)] 

        # 海外仓冲业绩不调账号（2023-03-30）FFS
        # CAB
        # DLF
        # HLF
        # CTT
        # HWK
        # FHS
        # HGP
        # SAP
        # TJS
        # FOD
        # QHN
        # LOS
        # SRS
        # BUU
        # FER
        # NC3
        # SH3
        # SUP
        # DPF
        # KKO
        # CAO
        # SUH
        # LVE
        # BIN
        # AM9
        # UNP
        # MSS
        # ISP
        # GOB
        # ONS
        # AOS
        # HSA
        # SGH
        # TOS
        # NDB
        # SN1
        # CUL
        # SSH
        # VIS
        # GIN
        # BJY
        # AGL
        # HOA
        # CAD
        df = df.loc[
        (df['account_id'] != 2347)
        & (df['account_id'] != 1026)
        & (df['account_id'] != 75)
        & (df['account_id'] != 6488)
        & (df['account_id'] != 1013)
        & (df['account_id'] != 6700)
        & (df['account_id'] != 7074)
        & (df['account_id'] != 55786)
        & (df['account_id'] != 39680)
        & (df['account_id'] != 301)
        & (df['account_id'] != 7976)
        & (df['account_id'] != 3912)
        & (df['account_id'] != 56709)
        & (df['account_id'] != 39650)
        & (df['account_id'] != 35804)
        & (df['account_id'] != 5764)
        & (df['account_id'] != 8818)
        & (df['account_id'] != 929)
        & (df['account_id'] != 1636)
        & (df['account_id'] != 4850)
        & (df['account_id'] != 4065)
        & (df['account_id'] != 27592)
        & (df['account_id'] != 53475)
        & (df['account_id'] != 6612)
        & (df['account_id'] != 3446)
        & (df['account_id'] != 7089)
        ]

        # #剔除利润率1%以内波动的数据
        df['涨幅率']=(df['post_price']-df['online_price']+df['shipping_fee'])/df['online_price']
        df = df.loc[(df['涨幅率'] >= 0.01) | (df['涨幅率'] <= -0.01) ]
        df = df.drop(['涨幅率'],axis=1)
        df = df[(df['post_price'] - df['online_price'] + df['shipping_fee']).abs() >= 0.3]


        # 去重
        df.drop_duplicates(subset=['item_id', 'sku'], inplace=True)
        print('ebay', len(df), df.columns)
        df_chaxun = df.copy()

        df_chaxun = df_chaxun.reset_index(drop=True)
        df_chaxun['index'] = df_chaxun.index
        df_chaxun['index'] = df_chaxun['index'].apply(lambda m: int(m / 500000))
        for item, group in df_chaxun.groupby(['index']):
            group.drop(['index'], axis=1, inplace=True)
            excel_name = os.path.join(f_path_ZIP, f'ebay平台海外仓调价明细-{qu_shu_date_str}-整体{item}.xlsx')
            group.to_excel(excel_name, sheet_name=f'所有链接明细{item}', index=False)
            del group
        del df_chaxun

        #
        sql1 = """select a.sku,a.warehouse,a.lowest_price/b.rate as '最低销毁价格本币' from  oversea_adjust_platform_dtl_temp a
                left join domestic_warehouse_clear.erp_rate b on a.country=b.country  COLLATE utf8mb4_general_ci
                where platform='EB'
                and  ((warehouse='英国仓' and a.country  ='UK')
                OR (warehouse='德国仓' and a.country='DE')
                OR (warehouse='法国仓' and a.country='FR')
                OR (warehouse='西班牙仓' and a.country='ES')
                OR (warehouse='意大利仓' and a.country='IT')
                OR (warehouse='澳洲仓' and a.country='AU')
                OR (warehouse='美国仓' and a.country='US')
                OR (warehouse='日本仓' and a.country='JP'))               
                AND adjust_recent_clean='负利润加快动销'"""
        df_xiaohui = conn.read_sql(sql1)
        df_xiaohui.to_excel(os.path.join(f_path_ZIP, f'ebay平台{qu_shu_date_str}海外仓销毁价明细.xlsx'))

        file_name = os.path.join(f_path2, f'ebay平台海外仓调价上传表{qu_shu_date_str}.csv')
        df.to_csv(file_name, index=False)
        conn.close()
        # 压缩上传到销售工作台
        # 生成压缩文件
        upload_zip_adjust(f_path1, 'EB', userCode='208254')
        # 发送钉钉消息
        send_msg('EBAY调价LISTING问题沟通群', '动销组定时任务推送', '海外仓清仓及加快动销与正常品提价数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请注意查收')
    except:
        send_msg('动销组定时任务推送', 'ebay海外仓调价群',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓ebay数据推送至分发系统出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def walmart_log_data_to_distribute_system():
    try:
        qu_shu_date_str, f_path1 = time_path()

        f_path2 = os.path.join(f_path1, f'walmart{qu_shu_date_str}')
        if not os.path.exists(f_path2):
            os.makedirs(f_path2)

        wenjianjia = '海外仓清仓+加快动销调价明细(清仓、加快动销、回调)-WALMART'
        if os.path.exists(os.path.join(f_path1,'WALMART')):
            shutil.rmtree(os.path.join(f_path1,'WALMART'))

        today = qu_shu_date_str.replace('-', '')
        f_path_ZIP = os.path.join(f_path1, 'WALMART', today, wenjianjia)
        if not os.path.exists(f_path_ZIP):
            os.makedirs(f_path_ZIP)

        # data_sys='数据部服务器'
        w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='数据部服务器')['qingcang_weekday'].astype('int').tolist()
        print(get_now_weekday())
        if get_now_weekday() in w_df:
            sql = f"""SELECT * FROM yibai_oversea_walmart_adjust_log where DATE='{time.strftime('%Y-%m-%d')}' and site!='ca' """
        else:
            sql = f"""SELECT * FROM yibai_oversea_walmart_adjust_log where DATE='{time.strftime('%Y-%m-%d')}' and IS_UP='涨价' and site!='ca' """
        df = sql_to_pd(sql=sql, database='over_sea', data_sys='数据部服务器')

        print('WALMART剔除前的数量', len(df))
        # df = tichu_all(df)
        # # 仅万邑通有库存的情况
        # sql1 = """select *
        #                 from (select sku,warehouse,sum(available_stock) as warehouse_stock
        #                  from yb_datacenter.v_oversea_stock
        #                  where warehouse_name like '%万邑通%' and available_stock>0 group by sku,warehouse) a
        #                  where concat(sku,warehouse)
        #                  not in
        #                  (select distinct concat(sku,warehouse)
        #                  from yb_datacenter.v_oversea_stock
        #                  where warehouse_name not like '%万邑通%' and available_stock>0)
        #                  """
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='yb_datacenter')
        # wyt_stock = ck_client.ck_select_to_df(ck_sql=sql1)
        # wyt_stock['库存情况'] = '仅万邑通有库存'
        #
        # ###对于2.14降价的sku只降不长
        # ###对于2.14降价的sku只降不长
        # df3 = reduce_price_sku()
        # df3 = df3[df3['platform'] == "WALMART"]
        # df3 = df3.merge(wyt_stock, on=['sku', 'warehouse'])
        # df3 = df3[['sku', 'warehouse', 'country', 'price']]
        # df3.columns = ['sku', 'warehouse', 'site', '调整价']
        # df3['site'] = df3['site'].str.lower()
        # df3.drop_duplicates(inplace=True)
        # df = df.merge(df3, on=['sku', 'warehouse', 'site'], how="left")
        # print("匹配参考价之后的数据量", len(df))
        # df['参与调价整价之前的价格'] = df['上传调价']
        # # 20220621澳洲仓仅万邑通有库存负利润加快动销sku直接调到销毁价，其他仓仅万邑通有库存正负利润一律调到销毁价
        # df.loc[(df['上传调价'] > df['调整价']) & df['adjust_recent_clean'].isin(['负利润加快动销', '正利润加快动销'])
        #        & (df['调整价'].notnull()) & (df['warehouse'] != '澳洲仓'), "上传调价"] = df['调整价']
        # df.loc[(df['上传调价'] > df['调整价']) & (df['adjust_recent_clean'] == '负利润加快动销')
        #        & (df['调整价'].notnull()) & (df['warehouse'] == '澳洲仓'), "上传调价"] = df['调整价']
        #
        # df.loc[(df['上传调价'] - df['price'] >= 0), 'is_up'] = '涨价'
        # df.loc[(df['上传调价'] - df['price'] < 0), 'is_up'] = '降价'
        # df = df[(df['上传调价'] - df['price']).abs() >= 0.3]
        # df.drop_duplicates(subset=['account_id', 'seller_sku'], inplace=True)
        # # 正负利润当前价格高于市场价的降回市场价,低于市场价只降不涨
        # sql1 = """
        #            select a.sku,a.`大仓` as warehouse,a.site,
        #            case when a.`沃尔玛市场价`>=round(u.lowest_price*1.2/r.rate,1)-0.01
        #             then a.`沃尔玛市场价`
        #             else round(u.lowest_price*1.2/r.rate,1)-0.01 end as "沃尔玛市场价"
        #            from ebay_walmart_amazon_market_value a
        #            left join over_sea_age_new b on a.sku=b.SKU and a.`大仓`=b.warehouse
        #            inner join oversea_transport_fee_useful u on a.sku=u.sku and b.warehouse_id=u.warehouseId
        #            and a.site=u.country and u.platform="WALMART"
        #            left join domestic_warehouse_clear.erp_rate r on r.country=u.country  COLLATE utf8mb4_general_ci
        #            where a.`沃尔玛市场价` is not null
        #            order by u.totalCost"""
        # df1 = sql_to_pd2(sql=sql1)
        # df1 = df1.merge(wyt_stock, on=['sku', 'warehouse'])
        # df1.drop_duplicates(subset=['sku', 'warehouse', 'site'], inplace=True)
        # df1['site'] = df1['site'].str.lower()
        # df1 = df1[(df1['沃尔玛市场价'].notnull())]
        # df1 = df1[~df1['沃尔玛市场价'].str.contains("无同款")]
        # df1['沃尔玛市场价'] = df1['沃尔玛市场价'].astype('float')
        # df1['沃尔玛市场价'] = df1['沃尔玛市场价'].round(1) - 0.01
        # df = df.merge(df1, on=['sku', 'warehouse', 'site'], how='left')
        # print("匹配市场价后的数据量", len(df))
        # # 对于有市场价的只降不涨
        # df.loc[(df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) & (df['沃尔玛市场价'].notnull()) & (
        #             df['price'] <= df['沃尔玛市场价']) & (
        #                df['is_up'] == '涨价'), '剔除'] = 1
        # 剔除掉低于市场价且涨价的
        # df = df[df['剔除'] != 1]
        # del df['剔除']
        # df.loc[
        #     (df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) & (df['沃尔玛市场价'].notnull()) & (
        #                 df['price'] > df['沃尔玛市场价']), '上传调价'] = \
        #     df['沃尔玛市场价']
        # df.loc[
        #     (df['adjust_recent_clean'].isin(['正利润加快动销', '负利润加快动销'])) & (df['price'] > df['沃尔玛市场价']), 'is_up'] = \
        #     '降价'


        # 仅谷仓有库存的链接最低净利不能低于-15%
        a=-0.2
        df_gc=gc_zhangjia(a=-0.2)
        df_gc_w = df_gc[df_gc['platform'] == 'WALMART']
        print('WALMART仅谷仓数量', len(df_gc_w))
        df_gc_w = df_gc_w[['sku', 'warehouse', 'country', f'谷仓{a}净利定价', 'totalCost']]
        df_gc_w.columns = ['sku', 'warehouse', 'site', f'谷仓{a}净利定价', '运费']
        # walmart站点是小写的
        df_gc_w['site'] = df_gc_w['site'].str.lower()
        df = df.merge(df_gc_w, on=['sku', 'warehouse', 'site'], how='left')
        df.loc[df['上传调价'] < df[f'谷仓{a}净利定价'], '上传调价'] = df[f'谷仓{a}净利定价']

        # walmart阶梯定价
        df.sort_values(by=['sku', 'warehouse', 'site', '上传调价'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['上传调价'] = df['上传调价'] + df.index.values % 30 * 0.01
        
        #20221117 US-YHX律所问题不调价
        df = df.loc[(df['account_id'] != 99)]

        #US-FX、US-ZLR运费测试，不调价截止时间 2023.4.1
        df = df.loc[(df['account_id'] != 162) & (df['account_id'] != 206)]
        
        #2022/12/ 1US-ZHL US-RXS US-ZXL US-YCZ US-GJM US-LKD US-AYV截止2023.4.1不调价; US-JFLR US-GFD 截止2023.12.31不调价
        df = df.loc[(df['account_id'] != 390) & (df['account_id'] != 391) & (df['account_id'] != 392) & (df['account_id'] != 393)
        & (df['account_id'] != 394) & (df['account_id'] != 398) & (df['account_id'] != 409)]
        
        df = df.loc[(df['account_id'] != 369) & (df['account_id'] != 378)]
        
        # US-CBY(453) US-FHT(452) US-LPS(454) US-ZHL US-RXS US-ZXL US-YCZ US-GJM US-LK，walmart这些账号专门做跟卖，跟卖别人公司的，不调价，截止2023.6.1
        # walmart   US-QZJ(406) 没说是为什么不调价(暂认为是运费原因) 不调价   截止时间2月28
        # walmart  US-XLGL(171) US-YHX(99) US-PSS(100) 不调价， 截止23年12月31日 原因是律所案件多，自己涨价，准备放弃链接
        # 投入2个账号做售价+运费的方式定价， US-WYH(159)  US-DJR(367) 不调价，截至2023.6.1
        # Walmart US-RDD(479) 做跟卖的账号，不调价 23.6.1
        # Walmart US-FXX(174) US-YNX(468) US-YCF(165) US-LSK(471) US-YCH(89) 运费 不调价，截至2023.6.1
        # US-GLY(405) US-LRA(110) & CA-CPYK(195) & CA-MYM(144) 运费 不调价，截至2023.6.1
        # US-MYM(133) 新投入做加运费，不调价，截至2023.6.1(可以调)
        # walmart US-WZHK(164) US-SDKJ(207) US-AID(315) 这3个账号要做加运费的，不调价截止到6.1（6.1当日开始调价）
        # US-QZJ(406)  walmart 这个账号要做加运费的，不调价截止到6.1
        # walmart US-DBJ(480) US-MTS(475) US-FTJ(486) US-JXYH(163)这4个账号要做售价+运费的，不调价截止到6.1
        # 2023-02-27更新，运费原因不调价的的账户只改为这些， US-MWK US-FHT US-MYM US-YBF US-YCH(89) US-LSK(471) US-GFD US-YNX， 截止到6.1
        # walmart US-HGY(436) US-SYK(489) 跟卖不调价 截止到6.1
        # walmart 474 账号终止了（2023年3月3日）
        df = df.loc[(df['account_id'] != 453) & (df['account_id'] != 454)
        & (df['account_id'] != 99) & (df['account_id'] != 100) & (df['account_id'] != 171)
        & (df['account_id'] != 468)
        & (df['account_id'] != 471) & (df['account_id'] != 89)
        & (df['account_id'] != 170)
        & (df['account_id'] != 378)
        & (df['account_id'] != 436)
        & (df['account_id'] != 489)
        & (df['account_id'] != 472)
        & (df['account_id'] != 380)
        & (df['account_id'] != 474)
        ]



        df = df[(df['上传调价'] - df['price']).abs() >= 0.3]
        # #剔除利润率1%以内波动的数据
        df['涨幅率']=(df['上传调价']-df['price'])/df['price']
        df = df.loc[(df['涨幅率'] >= 0.01) | (df['涨幅率'] <= -0.01) ]
        df = df.drop(['涨幅率'],axis=1)

        print('walmart', len(df))
        df = df.reset_index(drop=True)
        df['index'] = df.index
        df['index'] = df['index'].apply(lambda m: int(m / 500000))
        for item, group in df.groupby(['index']):
            group.drop(['index'], axis=1, inplace=True)
            excel_name = os.path.join(f_path_ZIP, f'walmart平台海外仓调价明细-{qu_shu_date_str}-整体{item}.xlsx')
            group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            del group
        file_name = os.path.join(f_path_ZIP, f'oversea_clear_status.xlsx')

        df.drop(['index'], axis=1, inplace=True)

        df = df.drop_duplicates()
        df.to_csv(os.path.join(f_path2, f'walmart平台海外仓调价上传表{qu_shu_date_str}.csv'), index=False)

        # 压缩上传到销售工作台
        # 生成压缩文件
        upload_zip_adjust(f_path1, 'WALMART', userCode='208254')
        # 发送钉钉消息
        send_msg('walmart平台调价', '动销组定时任务推送', '海外仓清仓及加快动销与正常品提价数据已上传至销售工作台\n明细数据可在【销售中台-->文件分发管理-->文件下载中心】下载\n请注意查收')
    except:
        send_msg('动销组定时任务推送', 'walmart平台调价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓walmart数据推送至分发系统出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


if __name__ == "__main__":
    cd_log_data_to_distribute_system()
