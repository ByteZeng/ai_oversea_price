import re

from pulic_func.adjust_price_base_api.public_function import *


def chuli_data(df_all):
    df = pd.read_csv("15号定价日志刊登的运费.csv")
    df['warehouse_id'] = df['content'].str.split(",").apply(lambda x: x[0].split('"warehouseId":')[-1])
    print(df['warehouse_id'])
    df['totalCost'] = df['content'].str.split('"totalCost":').apply(lambda x: str(x[-1]).split(",")[0]).str.strip("}")
    df['刊登头程']=df['content'].str.split('"firstCarrierCost":').apply(lambda x: str(x[-1]).split(",")[0]).str.strip("}")
    print(df['totalCost'])
    print(df.info())
    # 只取整数或者小数的totalCost
    df1 = df[df['warehouse_id'].str.isdigit()]
    df1['warehouse_id'] = df1['warehouse_id'].astype("int")
    df1['刊登totalCost'] = df1['totalCost'].astype("float")
    del df1['totalCost']
    df_all=df_all.merge(df1,on=['account_id','sku'])
    return df_all

def sku_map(df):
    seller_sku_list = tuple(set(df['seller_sku']))
    sql = f"""select account_id,seller_sku,sku,deliver_mode from yibai_amazon_sku_map 
        where seller_sku in {seller_sku_list}"""
    df1 = sql_to_pd(sql=sql, database="yibai_product", data_sys="外网")
    print(df.head())
    df=df.merge(df1,on=['account_id','seller_sku'])
    return df

###亚马逊运费
def amazon_yunfei(df):
    do=df[(df['fulfillment_channel']=='DEF')&(df['deliver_mode']==1)]
    print("国内仓链接数据",len(do))
    sku_list=tuple(set(do['sku']))
    print("sku_list数目",len(sku_list))
    conn = connect_to_sql(database="temp_database_hxx", data_sys="数据部服务器")
    r = conn.execute("show tables;").fetchall()
    r1 = [i[0] for i in r if i[0].__contains__("old_freight_interface_amazon")]
    print(r1)
    df1=pd.DataFrame()
    for i in r1:
        print(i)
        sql1=f"""select sku,warehouse_id,site as "站点",total_cost from {i}
                where sku in {sku_list}
                """
        # df11=ck_select_to_df(ck_sql=sql1,database='yibai_data_temp',host='139.9.67.175',port='9003')
        df11=conn.read_sql(sql1)
        print(df11.head())
        df11.sort_values(by=['total_cost'],inplace=True)
        df11.drop_duplicates(['sku',"站点"],"first",inplace=True)
        df1=df1.append(df11)
    conn.close()
    do=do.merge(df1,on=['sku',"站点"],how='left')
    do.rename(columns={'total_cost':"运费"},inplace=True)
    # df=df[df['运费'].notnull()]
    return do

def amazon_oversea_yunfei(df):
    ov=df[(df['fulfillment_channel']=='DEF')&(df['deliver_mode']==2)]
    print("ov的数据长度为",len(ov))
    ov1=ov[ov['sku'].notnull()]
    print("ov1的数据长度为",len(ov1))
    sku_list=tuple(set(ov1['sku']))
    sql=f"""select sku,warehouseId,country as site,totalCost as "运费" from oversea_transport_fee_useful
            where   platform='AMAZON' and sku in {sku_list}"""
    df1=sql_to_pd(sql=sql,data_sys='数据部服务器',database='over_sea')
    df1.sort_values(by="运费",inplace=True)
    df1['运费']=df1['运费'].astype('float')
    df1['site']=df1['site'].str.lower()
    ov=ov.merge(df1,on=['sku','site'],how="left")

    print(1)
    print(ov.info())
    ov=platform_fee(ov,platform="AMAZON")
    return ov

# 计算0净利和目标净利
def platform_fee(df, platform='AMAZON'):
    sql4 = f"""SELECT a.site,a.pay_fee as '平台佣金',a.paypal_fee,a.vat_fee as '税率',a.extra_fee as '库存折旧'
    ,a.platform_zero as 'FBM毛净利差值'
        FROM `yibai_platform_fee` a
        where a.platform='{platform}'"""
    df4 = sql_to_pd(sql=sql4, database='over_sea', data_sys='数据部服务器')
    df4['site']=df4['site'].str.lower()
    df = df.merge(df4, on='site', how='left')
    # df['目标毛利润率'] = df['FBM毛净利差值'] + 0.02
    df['毛利润']=(df['price']*df['rate']-df['成本']-df['运费']-df['price']*df['rate']*
               (df['平台佣金'] + df['paypal_fee'] + df['税率']+ df['库存折旧']))
    print(df.info())

    # df['2%净利率定价_本币'] = (df['成本'] + df['运费']) / (1 - df['平台佣金'] - df['paypal_fee'] - df['税率'] - df['库存折旧']
    #                                           - df['目标毛利润率']) / df['rate']

    return df




def pi_totalcost(df):
    do=df[(df['fulfillment_channel']=="DEF")&(df['deliver_mode']==1)]
    do=amazon_yunfei(do)
    fba=df[df['fu']]

def zhandian(df):
    sql="""select id as account_id,site from yibai_amazon_account"""
    df1=sql_to_pd(sql=sql,database="yibai_system",data_sys="AMAZON刊登库")
    sql1="""select country as site,site as "站点",rate from erp_rate 
    where rate is not null and country is not null and country <>''"""
    df2=sql_to_pd(sql=sql1,database="domestic_warehouse_clear",data_sys="数据部服务器")
    df2['site']=df2['site'].str.lower()
    df2.loc[df2['site'].isin(['es', 'sp']), 'site'] = "es"
    df2.loc[df2['site'].isin(['gb', 'uk']), 'site'] = "uk"
    df1.loc[df1['site'].isin(['es', 'sp']), 'site'] = "es"
    df1.loc[df1['site'].isin(['gb', 'uk']), 'site'] = "uk"
    df3=df1.merge(df2,on=['site'],how='left')
    df=df.merge(df3,on=['account_id'],how='left')
    return df

def new_price(df,sku_list):
    # sku_list=tuple(set(df[df['sku'].notnull()]['sku']))
    sql=f"""select sku,case when product_status=7 then last_price else new_price end new_price from  yibai_product where sku in {sku_list}"""
    df1=sql_to_pd(sql=sql,database="yibai_product",data_sys="AMAZON刊登库")
    df1.columns=['sku','成本']
    df=df.merge(df1,on=['sku'])
    return df


def amazon_dingjia(df):
    do=amazon_yunfei(df)
    # 税率
    do = shuilv(do, fb_type='FBM')
    # 国内仓退款率、毛净利差值
    do = amazon_fbm_gross_net_profit(do)
    do["平台佣金"]=0.15
    do['库存折旧']=0.04
    # do['目标毛利润率'] =do['FBM毛净利差值'] + 0.02
    print(do.info())
    do['毛利润']=(do['price']*do['rate']-(do['运费'] + do['成本'])-do['price']*do['rate']*(( 0.15 +0.04+do['税率'])))
    # do['2%净利率定价_本币'] = (do['运费'] + do['成本']) / (1 - 0.15 - 0.04 - do['目标毛利润率'] - do['税率']) / do['rate']
    # do.loc[do['2%净利率定价_本币'].notnull(), "2%净利率定价_本币"] =do['2%净利率定价_本币'].astype('float').round(1) - 0.01
    ov=amazon_oversea_yunfei(df)
    fba=Fba_dingjia(df)
    df=pd.concat([do,ov,fba])

    return df

def Fba_fees(df):
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    sku_list=tuple(set(df['seller_sku']))
    sql = f"""
    SELECT account_id,seller_sku,destruction_price,cost,rate,fba_fees,first_trip_fee_rmb,size_segment 
    FROM yibai_fba.fba_fees 
    WHERE fba_fee_source!='无法计算：尺寸重量超限' and first_trip_fee_rmb is not null
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df1 = conn_mx.ck_select_to_df(sql)
    df1 = df1[['account_id', 'seller_sku', 'destruction_price', 'cost', 'fba_fees', 'first_trip_fee_rmb']]



    df = df.merge(df1, on=['account_id', 'seller_sku'])
    # FBA毛净利差值
    FBA_difference = sql_to_pd(database='domestic_warehouse_clear', data_sys='数据部服务器',
                               sql='SELECT site1 as 站点,FBA_difference as FBA毛净利率差值,FBA_profit FROM site_table where platform="亚马逊"')
    df = df.merge(FBA_difference, on=['站点'])
    return df


def amazon_coupon_fu(df):
    today = datetime.date.today().isoformat()
    sql = f"""
    SELECT account_id,seller_sku,asin,coupon_type,percentage_off,money_off,start_date 
    FROM yibai_amazon_coupon 
    where coupon_status=8 and start_date<='{today} 00:00:00' and end_date>='{today} 23:59:59' 
    and is_delete=0 and disable in (0, 5)
    """
    df0 = sql_to_pd(database='yibai_product', sql=sql, data_sys='销售运营分析系统')
    df0 = df0.sort_values(['start_date'], ascending=False)
    df0 = df0.drop_duplicates(['account_id', 'asin'], 'first')
    df0.drop(['start_date'], axis=1, inplace=True)
    # percentage_off
    df1 = df0[df0['coupon_type'] == 1]
    df1 = df1[['account_id', 'asin', 'percentage_off']]
    df1['percentage_off'] = df1['percentage_off'].apply(lambda m: float(m) / 100)
    df1 = df1.sort_values(['percentage_off'], ascending=False)
    df1 = df1.drop_duplicates(['account_id', 'asin'], 'first')
    df = df.merge(df1, on=['account_id', 'asin'], how='left')
    df['percentage_off'].fillna(0, inplace=True)
    # money_off
    df2 = df0[df0['coupon_type'] == 2]
    df2 = df2[['account_id', 'asin', 'money_off']]
    df2['money_off'] = df2['money_off'].apply(lambda m: float(m))
    df2 = df2.sort_values(['money_off'], ascending=False)
    df2 = df2.drop_duplicates(['account_id', 'asin'], 'first')
    df = df.merge(df2, on=['account_id', 'asin'], how='left')
    df['money_off'].fillna(0, inplace=True)
    # 优惠券手续费
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    select `站点`,Coupon_handling_fee from domestic_warehouse_clear.amazon_coupon_handling_fee
    """
    coupon_shouxufei = conn_mx.ck_select_to_df(sql)
    df = df.merge(coupon_shouxufei, on=['站点'], how='left')
    df['Coupon_handling_fee'].fillna(0, inplace=True)
    df.loc[(df['percentage_off'] == 0) & (df['money_off'] == 0), 'Coupon_handling_fee'] = 0
    return df

def amazon_promotion_sku(n=1):
    df = get_promotion_sku()
    # df = df.merge(df1, on=['account_id', "seller_sku"], how='left')
    df['promotion_source'] = df['promotion_source'].fillna(4)
    df['promotion_source'] = df['promotion_source'].astype(int)
    df['promotion_percent'] = df['promotion_percent'].fillna(0)
    df['promotion_amount'] = df['promotion_amount'].fillna(0)

    if n == 2:
        df.drop(['promotion_source'], axis=1, inplace=True)
    df=df[(df['promotion_amount']>0)|(df['promotion_percent']>0)]
    pd_to_sql(df,table='yibai_amazon_promotion',if_exists='replace',database='over_sea',data_sys='数据部服务器')

    return df


def jp_integral():
    """
    日本站点积分
    :param df:
    :return:
    """
    url = 'http://rest.java.yibainetwork.com/data/yibaiFeedPriceConfig/feePriceConfiList'
    columns_dict = {
        'accountId': 'account_id',
        'yourPrice': 'your_price_point',
        # 'salePrice': 'sale_price_point',
    }
    df0 = jiekou_qushu(url, columns_dict)
    df0['account_id'] = df0['account_id'].astype(int)
    df0['your_price_point'] = df0['your_price_point'].fillna(0)
    df0.loc[df0['your_price_point'] == '', 'your_price_point'] = 0
    df0['your_price_point'] = df0['your_price_point'].astype(float)
    df0['your_price_point'] = df0['your_price_point'] / 100
    #
    # df = df.merge(df0, on=['account_id'], how='left')
    # df['your_price_point'] = df['your_price_point'].fillna(0)
    pd_to_sql(df0,table='your_price_point',database='over_sea',data_sys='数据部服务器')
    return df0


def get_promotion_sku(n=1):
    d_t = datetime.date.today().isoformat()
    """
        人工促销商品列表：`yibai_product_cloud`.`yibai_amazon_promotion_sku`
        调价促销商品列表：`yibai_product_cloud`.`yibai_amazon_promotion_sku_adj`
        先code在`yibai_product_cloud`.`yibai_amazon_promotion` 找到source是1还是2
        是1就去：`yibai_product_cloud`.`yibai_amazon_promotion_sku`
        是2就去：`yibai_product_cloud`.`yibai_amazon_promotion_sku_adj`      
    """
    sql = f"""
    select a.`code`,a.account_id,a.method,a.b2b_price,a.source,b.seller_sku 
    from yibai_amazon_promotion a 
    left join yibai_amazon_promotion_sku b 
    on a.`code` = b.`code`
    where a.status in (5, 11, 12) and a.start_date<='{d_t}' and a.end_date>='{d_t}' 
    and a.type=1 and a.source=1
    union all 
    select a.`code`,a.account_id,a.method,a.b2b_price,a.source,b.seller_sku 
    from yibai_amazon_promotion a 
    left join yibai_amazon_promotion_sku_adj b 
    on a.`code` = b.`code`
    where a.status in (5, 11, 12) and a.start_date<='{d_t}' and a.end_date>='{d_t}' 
    and a.type=1 and a.source=2
    """
    sql1="""select * from yibai_amazon_promotion where end_time>=date_sub(curdate(),interval 60 day)"""
    df1_sql = sql_to_pd(database='yibai_product_cloud', sql=sql, data_sys='AMAZON刊登库')
    pd_to_sql(df1_sql,database='over_sea',data_sys='数据部服务器',table='yibai_amazon_promotion_all',if_exists='replace')
    df = sql_to_pd(database='yibai_product_cloud', sql=sql, data_sys='AMAZON刊登库')
    df = df.drop_duplicates()
    #
    a_list = []
    for key, group in df.groupby(['b2b_price']):
        key = json.loads(key)
        df0 = pd.DataFrame(key)
        df0['qlb'] = df0['qlb'].apply(lambda m: int(m))
        # 只看买一个时的折扣
        df0 = df0[df0['qlb'] == 1]
        if df0.shape[0] > 0:
            df0 = df0.reset_index(drop=True)
            group['b2b_price_qp'] = df0['qp'][0]
            a_list.append(group)
    df = pd.DataFrame()
    df = df.append(a_list)
    df.drop(['b2b_price'], axis=1, inplace=True)
    df['b2b_price_qp'] = df['b2b_price_qp'].astype(float)
    if n == 2:
        df = df[(df['method'] == 1) & (df['source'] == 2)]
        df = df[['code', 'account_id', "seller_sku", 'b2b_price_qp']]
        df['b2b_price_qp'] = df['b2b_price_qp'] / 100
        df = df[df['b2b_price_qp'].isin([0.01, 0.02, 0.04, 0.07, 0.1, 0.13, 0.16, 0.19, 0.22, 0.25])]
        df.rename(columns={'b2b_price_qp': 'promotion_percent1'}, inplace=True)
        df = df.reset_index(drop=True)
        return df
    # 存在多条促销时，折扣累加，不改折扣
    df_count = df[['account_id', "seller_sku"]]
    df_count['行数'] = 1
    df_count = df_count.groupby(['account_id', "seller_sku"])[['行数']].sum().reset_index()
    df = df.merge(df_count, on=['account_id', "seller_sku"])
    df.loc[df['行数'] > 1, 'source'] = 3

    df0 = df[['account_id', "seller_sku", 'source']].drop_duplicates()
    # 百分比
    df1 = df[df['method'] == 1]
    df1 = df1[['account_id', "seller_sku", 'b2b_price_qp']]
    df1.columns = ['account_id', "seller_sku", 'b2b_price_percent']
    df1['b2b_price_percent'] = df1['b2b_price_percent'] / 100
    df1 = df1.groupby(['account_id', "seller_sku"])[['b2b_price_percent']].sum().reset_index()
    # 折扣
    df2 = df[df['method'] == 2]
    df2 = df2[['account_id', "seller_sku", 'b2b_price_qp']]
    df2.columns = ['account_id', "seller_sku", 'b2b_price_amount']
    df2 = df2.groupby(['account_id', "seller_sku"])[['b2b_price_amount']].sum().reset_index()

    df = df0.merge(df1, on=['account_id', "seller_sku"], how='left')
    df = df.merge(df2, on=['account_id', "seller_sku"], how='left')
    df['b2b_price_percent'] = df['b2b_price_percent'].fillna(0)
    df['b2b_price_amount'] = df['b2b_price_amount'].fillna(0)
    df = df[['account_id', "seller_sku", "source", "b2b_price_percent", "b2b_price_amount"]]
    df.columns = ['account_id', "seller_sku", "promotion_source", "promotion_percent", "promotion_amount"]
    df = df.reset_index(drop=True)
    return df


def fba_data():
    df=amazon_promotion_sku(n=2)
    jp_integral()



#最近一次跑得
def Fba_dingjia_last(df):
    df=df[(df['fulfillment_channel']=='AMA')]
    df=Fba_fees(df)
    # 税率
    df = shuilv(df, fb_type='FBA')
    # 日本站点积分
    df = jp_integral(df)
    # df['目标毛利润率']=df['FBA毛净利率差值']+0.02
    print(df.columns())
    df=amazon_coupon_fu(df)
    print(df.info())
    df=amazon_promotion_sku(df)
    print(df.info())

    df['cost']=df['cost'].astype('float')
    df['promotion_percent']=0
    df['promotion_amount']=0
    df['price']=df['price'].astype('float')
    df['当前售价利润率-净利率'] = 1 - 0.15 - 0.04 - df['税率'] - \
                        (df['cost'] + 2 + df['first_trip_fee_rmb'] + df['fba_fees'] * df['rate']) / \
                        (df['price'] * (1 - df['your_price_point'] - df['percentage_off'] - df['promotion_percent']) -
                         df['money_off'] - df['Coupon_handling_fee'] - df['promotion_amount']) / df['rate'] - df[
                            'FBA毛净利率差值']

    # df['毛利润']=(df['price']*df['rate']-(df['cost'] + 2 + df['first_trip_fee_rmb'] + df['fba_fees'] * df['rate'])-df['price']*df['rate']*(0.15 +0.04 + df['税率']))
    df=df[df['当前售价利润率-净利率']>=0.1]
    return df


def Fba_profit(df):
    df=df[(df['fulfillment_channel']=='AMA')]
    df=Fba_fees(df)
    # 税率
    df = shuilv(df, fb_type='FBA')
    # 日本站点积分
    df = jp_integral(df)
    # df['目标毛利润率']=df['FBA毛净利率差值']+0.02
    df=amazon_coupon_fu(df)
    df=amazon_promotion_sku(df)
    print(df.info())
    df['cost']=df['cost'].astype('float')
    df['promotion_percent']=0
    df['promotion_amount']=0
    df['price']=df['price'].astype('float')
    df['当前售价利润率-净利率'] = 1 - 0.15 - 0.04 - df['税率'] - \
                        (df['cost'] + 2 + df['first_trip_fee_rmb'] + df['fba_fees'] * df['rate']) / \
                        (df['price'] * (1 - df['your_price_point'] - df['percentage_off'] - df['promotion_percent']) -
                         df['money_off'] - df['Coupon_handling_fee'] - df['promotion_amount']) / df['rate'] - df[
                            'FBA毛净利率差值']
    #高于目标净利部分
    df=df[df['当前售价利润率-净利率']>df['FBA_profit']]

    # df['毛利润']=(df['price']*df['rate']-(df['cost'] + 2 + df['first_trip_fee_rmb'] + df['fba_fees'] * df['rate'])-df['price']*df['rate']*(0.15 +0.04 + df['税率']))

    return df


def amazon_zijin_appeal():
    """
    amazon资金冻结记录
    :return:
    """
    url = 'http://rest.java.yibainetwork.com/data/yibaiAmazonAccountAppeal/amazonAccountAppealList'
    columns_dict = {'id': 'id',
                    'part': 'part',
                    'groupId': 'group_id',
                    'browser': 'browser',
                    'site': 'site',
                    'accountPerson': 'account_person',
                    'accountId': 'account_id',
                    'shortName': 'short_name',
                    'accountBalance': 'account_balance',
                    'appealType': 'appeal_type',
                    'issueType': 'issue_type',
                    'appealPerson': 'appeal_person',
                    'frozenTime': 'frozen_time',
                    'remark': 'remark',
                    'asin': 'asin',
                    'recoverTime': 'recover_time',
                    'appealStatus': 'appeal_status',
                    'appealProgress': 'appeal_progress',
                    'operaStatus': 'opera_status',
                    'auditUser': 'audit_user',
                    'accountSource': 'account_source',
                    'frozenDays': 'frozen_days',
                    'modifyUser': 'modify_user',
                    'modifyTime': 'modify_time',
                    'createUser': 'create_user',
                    'createTime': 'create_time',
                    'prompt': 'prompt',
                    'isDel': 'is_del',
                    }
    df = jiekou_qushu(url, columns_dict)
    print(df.head())
    print(df.info())
    # df=df[['account_id',]]
    # df = df[(df['appeal_type'] == 4) & (df['appeal_status'].isin([1, 2, 3, 5])) & (df['is_del'] == 0)]


    df=df[['account_id','appeal_type','appeal_status','is_del','frozen_time','asin','recover_time'
           ,'opera_status','audit_user','frozen_days','modify_time','modify_user','create_user','create_time']]


    pd_to_sql(df, table='account_appeal', database='over_sea', data_sys='数据部服务器')

    return df


# 通过接口获取数据库数据
def jiekou_qushu(url, columns_dict):
    token = java_get_token()

    url = f'{url}?access_token=' + token
    data = {"pageNumber": 1, "pageSize": 1}
    while 1:
        try:
            total = requests.post(url, json=data, timeout=(30, 30)).json()['data']['total']
            break
        except:
            print(f'{url} 获取数量接口失败，重新链接')
    pageSize = 100000
    n = total / pageSize
    if int(n) != n:
        n = int(n) + 1

    dict1 = {}
    dict_list = []
    for key in columns_dict:
        v = columns_dict[key]
        #
        dict1[v] = []
        dict_list.append(v)

    df = pd.DataFrame(dict1)
    if total > 0:
        for i in range(1, n + 1):
            data = {
                "pageNumber": i,
                "pageSize": pageSize
            }
            while 1:
                try:
                    data_list = requests.post(url=url, json=data, timeout=(30, 30)).json()["data"]["records"]
                    break
                except:
                    print(f'{url} 获取数据接口失败，重新链接')
            if len(data_list) == 0:
                break
            df_i = pd.DataFrame(data_list)
            df_i.rename(columns=columns_dict, inplace=True)

            df_i = df_i[dict_list]
            df = df.append(df_i)
    df = df.drop_duplicates()
    return df


# 获取token（BI提供的数据表）
def java_get_token():
    site_redis = redis_conn()
    java_token = 'java_get_token'
    A = site_redis.get(java_token)
    if A:
        A = json.loads(A)
    else:
        A = {'expires_in': 0, }
    if A['expires_in'] >= int(time.time()):
        token = A['access_token']
    else:
        # 获取token
        url = 'http://oauth.java.yibainetwork.com/oauth/token?grant_type=client_credentials'
        while 1:
            try:
                resp = requests.post(url, auth=HTTPBasicAuth('prod_data_mgn', 'mgnkk7cytdsD'), timeout=(30, 30))
                break
            except:
                print('获取token接口失败，重新链接')
        token = json.loads(resp.text)['access_token']
        expires_in = json.loads(resp.text)['expires_in'] + int(time.time())
        A = json.dumps({'access_token': token, 'expires_in': expires_in})
        site_redis.set(java_token, A)
    return token

# def Fbm_dingjia(df):


# def price(df):
#     df_p=pd.DataFrame()
#     seller_sku_list=tuple(df['seller_sku'])
#     a,b=divmod(len(seller_sku_list),5000)
#     conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
#     for i in range(a+1):
#         sql=f"""select account_id,seller_sku,price,fulfillment_channel,status
#         from yibai_product_kd_sync.yibai_amazon_listings_all_raw2
#         where seller_sku in {seller_sku_list[i*5000:(i+1)*5000]}"""
#         t=conn_ck.ck_select_to_df(sql)
#         df_p=pd.concat([df_p,t])
#     df['account_id']=df['account_id'].astype('int')
#     df_p['account_id']=df_p['account_id'].astype('int')
#     df=df.merge(df_p,on=['account_id','seller_sku'],how='left')
#
#     return df


# 匹配日均销量
def ri_jun(df_m):
    dt = datetime.date.today().isoformat()
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = f"""
        select count() as `数量` 
        from yibai_domestic.sales_amazon 
        where dt='{dt}'
        """
    c = conn_ck.ck_select_to_df(sql)
    if c['数量'][0] == 0:
        amazon_sales.amazon_sales()

    #
    sql = """
        SELECT account_id,seller_sku,asin,fulfillment_channel,
        3days_sales,7days_sales,15days_sales,30days_sales,order_num 
        from yibai_domestic.sales_amazon
        """
    order = conn_ck.ck_select_to_df(sql)
    order = order.drop_duplicates(['account_id', 'seller_sku'], 'first')

    fba = order[order['fulfillment_channel'] != 'DEF']
    fba = fba[
        ['account_id', 'seller_sku', 'asin', '3days_sales', '7days_sales', '15days_sales', '30days_sales', 'order_num']]

    FBM = order[order['fulfillment_channel'] == 'DEF']
    FBM = FBM[['account_id', 'asin', '3days_sales', '7days_sales', '15days_sales', '30days_sales']]
    FBM.columns = ['account_id', 'asin', '3days_sales1', '7days_sales1', '15days_sales1', '30days_sales1']
    FBM = FBM.groupby(['account_id', 'asin'])[
        ['3days_sales1', '7days_sales1', '15days_sales1', '30days_sales1']].sum().reset_index()
    fba = fba.merge(FBM, on=['account_id', 'asin'], how='left')
    for col in ['3days_sales', '7days_sales', '15days_sales', '30days_sales']:
        col1 = col + '1'
        fba[col1] = fba[col1].fillna(0)
        fba[col] = fba[col] + fba[col1]
        fba.drop([col1], axis=1, inplace=True)
    fba['日均销量'] = fba['7days_sales'] / 7 * 0.7 + fba['15days_sales'] / 15 * 0.2 + fba['30days_sales'] / 30 * 0.1
    #
    col_list = ['日均销量', '30days_sales', 'order_num', '7days_sales', '15days_sales']
    col_list1 = ['account_id', 'seller_sku'] + col_list
    fba = fba[col_list1]
    #
    df_m = df_m.merge(fba, on=['account_id', 'seller_sku'], how='left')
    for col in col_list:
        df_m[col] = df_m[col].fillna(0)

    # 考虑跟卖关系
    df_m = fba_genmai(df_m)
    df_g = df_m[['to_account_id', 'to_seller_sku', '日均销量']]
    df_g = df_g.groupby(['to_account_id', 'to_seller_sku'])[['日均销量']].sum().reset_index()
    df_g.rename(columns={'日均销量': '日均销量-跟卖'}, inplace=True)
    df_m = df_m.merge(df_g, on=['to_account_id', 'to_seller_sku'], how='left')
    df_m['日均销量-跟卖'] = df_m['日均销量-跟卖'].fillna(0)

    # 泛欧销量处理
    col_list.append('日均销量-跟卖')
    col_list2 = ['标识', 'seller_sku', 'fnsku'] + col_list
    df1 = df_m[col_list2]
    df1 = df1.groupby(['标识', 'seller_sku', 'fnsku']).sum().reset_index()
    df_m.drop(col_list, axis=1, inplace=True)
    df_m = df_m.merge(df1, on=['标识', 'seller_sku', 'fnsku'], how='left')
    for col in col_list:
        df_m[col] = df_m[col].fillna(0)

    df_m['两周销量对比'] = df_m['7days_sales'] / df_m['15days_sales']
    df_m.loc[df_m['15days_sales'] == 0, '两周销量对比'] = 0
    df_m.drop(['15days_sales'], axis=1, inplace=True)
    #
    df_m = df_m.drop_duplicates(['account_id', 'seller_sku'], 'first')

    # 正常状态链接的销量逻辑，只考虑FBA链接的销量()
    sql = """
            SELECT account_id,asin,7days_sales1,15days_sales1,30days_sales1,90days_sales1 
            from yibai_domestic.sales_amazon 
            where fulfillment_channel = 'AMA'
            """
    order = conn_ck.ck_select_to_df(sql)
    order = order.groupby(['account_id', 'asin'])[
        ['7days_sales1', '15days_sales1', '30days_sales1', '90days_sales1']].sum().reset_index()
    df_m = df_m.merge(order, on=['account_id', 'asin'], how='left')
    for col in ['7days_sales1', '15days_sales1', '30days_sales1', '90days_sales1']:
        df_m[col] = df_m[col].fillna(0)
    df1 = df_m.groupby(['标识', 'seller_sku', 'fnsku'])[
        ['7days_sales1', '15days_sales1', '30days_sales1', '90days_sales1']].sum().reset_index()
    df1 = df1[['标识', 'seller_sku', 'fnsku', '7days_sales1', '15days_sales1', '30days_sales1', '90days_sales1']]
    df1.columns = ['标识', 'seller_sku', 'fnsku', '7days_sales2', '15days_sales2', '30days_sales2', '90days_sales2']
    df_m = df_m.merge(df1, on=['标识', 'seller_sku', 'fnsku'], how='left')
    df_m['日均销量1'] = df_m['7days_sales2'] / 7 * 0.7 + df_m['15days_sales2'] / 15 * 0.2 + df_m['30days_sales2'] / 30 * 0.1
    return df_m


def base_data():
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    sql = """SELECT account_id,seller_sku,asin,fulfillment_channel,
    7days_sales/7*0.7+15days_sales/15*0.2+30days_sales/30*0.1  as day_sales
    from sales
    where 7days_sales/7*0.7+15days_sales/15*0.2+30days_sales/30*0.1>0.1"""
    df = sql_to_pd(sql=sql, database='monitor_process_data', data_sys='数据部服务器')
    # 销量状态为正常的
    sql = """SELECT * FROM `fba_clear_new`
    where end_time is null"""
    df1 = sql_to_pd(sql=sql, database='domestic_warehouse_clear', data_sys='数据部服务器')
    sql = """select * FROM yibai_product_kd_sync.yibai_amazon_listings_all_raw2
    where fulfillment_channel ='AMA'"""
    df3 = conn_ck.ck_select_to_df(ck_sql=sql)
    df3.rename(columns={'asin1':'asin'},inplace=True)
    # 销售状态为正常的sku
    # 不在销售状态表里的是属于正常状态的销售状态
    df4 = df1[['account_id', 'seller_sku']]
    df4['清仓'] = 1
    df5 = df3.merge(df4, on=['account_id', 'seller_sku'], how='left')
    df5 = df5[df5['清仓'] != 1]
    # 销量低于0.1
    df6 = df[['account_id', 'seller_sku']]
    df6['日销大于0.1'] = 1
    df7 = df5.merge(df6, on=['account_id', 'seller_sku'], how='left')
    df7 = df7[df7['日销大于0.1'] != 1]
    return df7


# def domestic_base_data():
#     conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
#     sql = """SELECT account_id,seller_sku,asin,fulfillment_channel,
#     7days_sales/7*0.7+15days_sales/15*0.2+30days_sales/30*0.1  as day_sales
#     from sales
#     where 7days_sales/7*0.7+15days_sales/15*0.2+30days_sales/30*0.1>0.1"""
#     df = sql_to_pd(sql=sql, database='monitor_process_data', data_sys='数据部服务器')
#     # 销量状态为正常的
#     sql = """SELECT * FROM `fba_clear_new`
#     where end_time is null"""
#     df1 = sql_to_pd(sql=sql, database='domestic_warehouse_clear', data_sys='数据部服务器')
#     sql = """select * FROM yibai_product_kd_sync.yibai_amazon_listings_all_raw2
#     where fulfillment_channel !='AMA'"""
#     df3 = conn_ck.ck_select_to_df(ck_sql=sql)
#     df3.rename(columns={'asin1':'asin'},inplace=True)
#     # 销售状态为正常的sku
#     # 不在销售状态表里的是属于正常状态的销售状态
#     df4 = df1[['account_id', 'seller_sku']]
#     df4['清仓'] = 1
#     df5 = df3.merge(df4, on=['account_id', 'seller_sku'], how='left')
#     df5 = df5[df5['清仓'] != 1]
#     # 销量低于0.1
#     df6 = df[['account_id', 'seller_sku']]
#     df6['日销大于0.1'] = 1
#     df7 = df5.merge(df6, on=['account_id', 'seller_sku'], how='left')
#     df7 = df7[df7['日销大于0.1'] != 1]
#     return df7
def fba_data_1():
    ###################################################################################
    df = base_data()
    df.drop_duplicates(subset=['account_id', 'seller_sku', 'price'], inplace=True)
    # pd_to_sql(df=df,table='fba_kd_sync_202202131',database='over_sea',data_sys='数据部服务器')
    # sql="""select * from fba_kd_sync"""
    # df=sql_to_pd(sql=sql,database='over_sea',data_sys='海外仓_王杰')
    print(df.head())

    df = zhandian(df)
    df = Fba_dingjia(df)
    t1=datetime.date.today().isoformat().replace("-","")
    pd_to_sql(df, table=f'FBa_test_{t1}', database='over_sea', data_sys='数据部服务器')
    #####################################################################################

#20220517最近一次调价
def fba_data_last():
    conn_ck=pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    sql1 = """select a.id as account_id,a.short_name,a.site,a.status as 账号状态,b.group_name
     from yibai_amazon_account a
     left join yibai_amazon_group b on a.group_id=b.group_id
     where b.group_name in ('武汉26组',
     '武汉34组',
     '武汉36组',
     '武汉5组')
     """
    df = sql_to_pd(sql=sql1, data_sys='AMAZON刊登库', database='yibai_system')
    account_id_list = tuple(set(df['account_id']))
    sql1 = f"""SELECT a.account_id,case  when a.asin1 !='' then a.asin1 else c.asin1 end asin ,a.seller_sku,a.price,a.fulfillment_channel 
               FROM (
               SELECT account_id,seller_sku,asin1,price,fulfillment_channel 
               FROM yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
               WHERE fulfillment_channel='AMA' and account_id in {account_id_list}
               ) a 
               left join (
               SELECT account_id,seller_sku,asin1  
               FROM yibai_product_kd_sync.yibai_amazon_listing_alls 
               WHERE fulfillment_channel='AMA'
               ) c 
               on a.account_id=c.account_id and a.seller_sku=c.seller_sku 
               settings max_memory_usage = 20000000000 """
    df1=conn_ck.ck_select_to_df(ck_sql=sql1)
    df=df.merge(df1,on=['account_id'])
    #是正常品的链接
    sql_sale="""            
         select account_id,seller_sku,adjustment_priority 
        from domestic_warehouse_clear.yibai_fba_clear_new
        where end_time is null"""
    df_sale=conn_ck.ck_select_to_df(ck_sql=sql_sale)
    df=df.merge(df_sale,on=['account_id','seller_sku'],how='left')
    df=df[df['adjustment_priority'].isnull()]
    df = zhandian(df)
    df = Fba_profit(df)
    # seller_sku_list=tuple(set)

    #匹配销量
    seller_sku_list = df[['seller_sku']].drop_duplicates()
    sql = "select * from yibai_fba.fba_fnsku"
    df_fnsku = conn_ck.ck_select_to_df(ck_sql=sql)
    df_fnsku = df_fnsku.merge(seller_sku_list, on=['seller_sku'])



    sql = """                 
         SELECT account_id,seller_sku,7days_sales1,15days_sales1,30days_sales1,90days_sales1 
            from yibai_domestic.sales_amazon 
            where fulfillment_channel = 'AMA'
            """
    df_order=conn_ck.ck_select_to_df(ck_sql=sql)
    df_order=df_order.merge(df_fnsku,on=['account_id','seller_sku'],how='left')
    df_order=fanou_fanmei(df_order)
    df_order1=df_order[['account_id','seller_sku','标识','fnsku']].drop_duplicates()
    df_order = df_order.groupby(['标识', 'seller_sku', 'fnsku'])[
        ['7days_sales1', '15days_sales1', '30days_sales1', '90days_sales1']].sum().reset_index()

    df_order1=df_order1.merge(df_order,on=['标识', 'seller_sku', 'fnsku'],how='left')
    df=df.merge(df_order1,on=['account_id','seller_sku'],how='left')
    df['日均销量'] = df['7days_sales1'] / 7 * 0.7 + df['15days_sales1'] / 15 * 0.2 + df['30days_sales1'] / 30 * 0.1
    df['日均销量'].fillna(0,inplace=True)
    pd_to_sql(df, table=f'FBa_test_newest', database='over_sea', data_sys='数据部服务器')










def domestic_zhandian():
    sql="""select id as account_id,site from yibai_amazon_account where status=1"""
    df1=sql_to_pd(sql=sql,database="yibai_system",data_sys="AMAZON刊登库")
    sql1="""select country as site,site as "站点",rate from erp_rate 
    where rate is not null and country is not null and country <>''"""
    df2=sql_to_pd(sql=sql1,database="domestic_warehouse_clear",data_sys="数据部服务器")
    df2['site']=df2['site'].str.lower()
    df2.loc[df2['site'].isin(['es', 'sp']), 'site'] = "es"
    df2.loc[df2['site'].isin(['gb', 'uk']), 'site'] = "uk"
    df1.loc[df1['site'].isin(['es', 'sp']), 'site'] = "es"
    df1.loc[df1['site'].isin(['gb', 'uk']), 'site'] = "uk"
    df3=df1.merge(df2,on=['site'],how='left')
    return df3



zd=domestic_zhandian()

def domestic_base_data():
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    sql1 = """SELECT a.sku,a.available_qty,b.warehouse_name,c.name as warehouse FROM `yibai_warehouse_sku_stock_ods` a
            left join yibai_logistics.yibai_warehouse b on a.warehouse_code=b.warehouse_code
            left join yibai_logistics.yibai_warehouse_category c 
            on b.ebay_category_id=c.id
            where available_qty>0 and c.name='中国仓'"""
    df1 = sql_to_pd(sql=sql1, database='yibai_plan_stock', data_sys='数据管理部同步服务器')

    # 销量状态为正常的
    sql = """SELECT sku, adjustment_priority, current_net_profit, start_time, end_time, status, status1
    FROM domestic_warehouse_clear.yibai_domestic_warehouse_clear_status
    where end_time is null"""
    df3=conn_ck.ck_select_to_df(ck_sql=sql)

    df5 = df3[['sku']]
    df5.drop_duplicates(inplace=True)
    df5['清仓'] = 1
    df1 = df1.merge(df5, on=['sku'], how='left')
    df1 = df1[df1['清仓'] != 1]
    del df1['清仓']
    return df1

def domestic_sales_new():
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')

    sql = """SELECT account_id,seller_sku,fulfillment_channel,
        7days_sales/7*0.7+15days_sales/15*0.2+30days_sales/30*0.1  as day_sales
        from yibai_domestic.sales_amazon
        where fulfillment_channel<>'AMA' """
    df2 = conn_ck.ck_select_to_df(ck_sql=sql)



    return df2

def qushu_date():
    to=datetime.date.today().isoformat()
    to=to.replace("-","")
    return to

conn_ck=pd_to_ck(database='yibai_temp_hxx')
def domestic_yun(sku):
    conn_ck = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器')
    print("此次匹配运费的sku个数为:",len(sku))

    #运费
    # sku=tuple(set(df['sku']))
    if len(sku)>1:
        sql1=f"""SELECT sku,site,total_cost
        FROM yibai_temp_hxx.old_freight_interface_amazon_{qushu_date()}
        where sku in {sku}
        order by total_cost"""
    elif len(sku)==1:
        sql1=f"""SELECT sku,site,total_cost
        FROM yibai_temp_hxx.old_freight_interface_amazon_{qushu_date()}
        where sku = '{sku[0]}'
        order by total_cost"""
    else:
        pass
    df_y=conn_ck.ck_select_to_df(sql1)
    df_y.drop_duplicates(subset=['sku','site'],inplace=True)
    df_y.columns=['sku','站点','运费']
    # df=df.merge(df_y,on=['sku','站点'],how='left')
    return df_y


def domestic_profit_1():
    #基础数据,有库存且在国内仓销售状态为正常的sku
    sql1 = """select a.id as account_id,a.short_name,a.site as country,a.status as 账号状态,b.group_name
        from yibai_amazon_account a
        left join yibai_amazon_group b on a.group_id=b.group_id
        where b.group_name in ('武汉26组',
        '武汉34组',
        '武汉36组',
        '武汉5组')
        """
    df = sql_to_pd(sql=sql1, data_sys='AMAZON刊登库', database='yibai_system')
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df1=domestic_base_data()

    #匹配销量
    df2=domestic_sales_new()
    id_list = tuple(set(df['account_id']))
    a, b = divmod(len(id_list), 10)

    for i in tqdm(range(a+1)):
        df_5 = pd.DataFrame()

        sql = f""" SELECT a.account_id AS account_id,a.seller_sku AS seller_sku,b.sku AS sku,
            case  when a.asin1 !='' then a.asin1 else c.asin1 end as  asin,
            a.price  as price
            FROM (
            SELECT account_id,seller_sku,asin1,price  
            FROM yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
            WHERE account_id in {id_list[i*10:(i+1)*10]} AND fulfillment_channel='DEF' and toFloat64(price) > 0  
            ) a 
            INNER join (
            SELECT account_id,seller_sku,sku   
            FROM yibai_product_kd_sync.yibai_amazon_sku_map 
            WHERE account_id in {id_list[i*10:(i+1)*10]} AND deliver_mode=1 
            ) b 
            on a.account_id=b.account_id and a.seller_sku=b.seller_sku 
            left join (
            SELECT account_id,seller_sku,asin1  
            FROM yibai_product_kd_sync.yibai_amazon_listing_alls 
            WHERE account_id in {id_list[i*10:(i+1)*10]} AND fulfillment_channel='DEF' 
            ) c 
            on a.account_id=c.account_id and a.seller_sku=c.seller_sku  """

        df_5 = conn_ck.ck_select_to_df(ck_sql=sql)
        #匹配大部
        df_5=df_5.merge(df,on=['account_id'])
        #正常品
        df_5=df_5.merge(df1,on=['sku'])
        #匹配销量情况
        df_5=df_5.merge(df2,on=['account_id','seller_sku'],how='left')
        df_5['day_sales'].fillna(0,inplace=True)



        df_5 = df_5.merge(zd, on=['account_id'], how='left')
        print(df_5.columns)



        # 匹配运费和成本
        sku_list1=tuple(set(df_5[df_5['sku'].notnull()]['sku']))
        df_yun=pd.DataFrame()
        if len(sku_list1)>1 and len(sku_list1)<=5000:

            df_yun = domestic_yun(sku_list1)
        else:
            a,b=divmod(len(sku_list1),5000)
            for n in range(a+1):
                sku_test=sku_list1[n*5000:(n+1)*5000]

                df_yun1=domestic_yun(sku_test)
                df_yun=df_yun.append(df_yun1)
        df_5=df_5.merge(df_yun,on=['sku','站点'],how='left')

        #匹配上成本
        df_5 = new_price(df_5, sku_list1)
        ###匹配上税率
        df_5 = shuilv(df_5, fb_type='FBM')
        ###匹配上毛净利差值
        df_5 = amazon_fbm_gross_net_profit_1(df_5)
        df_5=jp_integral(df_5)
        # 当前售价利润率
        df_5 = amazon_coupon_fu(df_5)
        # promotion
        df_5= amazon_promotion_sku(df_5, n=2)
        print(df_5.info())
        df_5['price'] = df_5['price'].astype('float')
        print(df_5.info())
        df_5['当前售价净利润率'] = 1 - 0.18 - df_5['税率'] - (df_5['运费'] + df_5['成本']) / \
                         ((df_5['price']) * (
                                     1 - df_5['your_price_point'] - df_5['percentage_off'] - df_5['promotion_percent']) -
                          (df_5['money_off'] + df_5['Coupon_handling_fee'] + df_5['promotion_amount'])) / df_5['rate']-df_5['FBM毛净利差值']
        df_5 = df_5[df_5['当前售价净利润率'] > df_5['FBM_profit']]

        #看这部分链接的执行情况
        # df_5=domesti_zhixinglv(df_5)
        pd_to_sql(df_5,table=f'domestic_zcp_dx_listing_{qushu_date()}',if_exists='append',database='over_sea')

def amazon_fbm_gross_net_profit_1(df):
    # FBM毛净利差值/FBM退款率
    df0 = sql_to_pd(database='domestic_warehouse_clear', data_sys='数据部服务器',
                    sql='SELECT site1 as 站点,FBM_difference as FBM毛净利差值,FBM_refund_rate AS FBM退款率,FBM_profit FROM site_table')
    df = df.merge(df0, on=['站点'])
    return df


def domestic_profit1():
    #基础数据,有库存且在国内仓销售状态为正常的sku
    df1=domestic_base_data()
    df_s=domestic_sales_new()
    sku_list = tuple(set(df1['sku']))
    a, b = divmod(len(sku_list), 100)

    for i in tqdm(range(a+1)):
        df_5 = pd.DataFrame()

        sql = f""" SELECT a.account_id,a.seller_sku,b.sku,a.price as price,
                        case  when a.asin1 !='' then a.asin1 else c.asin1 end asin,
                        d.ListingPrice as sale_price,d.Shipping,d.updated_at 
                        FROM yibai_amazon_listings_all_raw2 a 
                        INNER JOIN yibai_amazon_sku_map b FORCE INDEX ( idx_sku ) 
                        ON a.account_id=b.account_id AND a.seller_sku=b.seller_sku 
                        INNER JOIN yibai_amazon_listing_alls c 
                        ON a.account_id=c.account_id AND a.seller_sku=c.seller_sku 
                        LEFT JOIN yibai_amazon_listing_price d 
                        ON a.account_id=d.account_id AND a.seller_sku COLLATE utf8_unicode_ci = d.seller_sku 
                        WHERE   b.deliver_mode=1 
                        AND a.fulfillment_channel='DEF' and b.sku in {sku_list[i*100:(i+1)*100]} """

        df_5 = sql_to_pd(sql=sql, database='yibai_product', data_sys='AMAZON刊登库')


        df_5 = df_5.merge(zd, on=['account_id'], how='left')
        # 剔除掉日销大于0.1的链接
        df_5=df_5.merge(df_s,on=['account_id','seller_sku'],how='left')

        # 匹配运费和成本
        sku_list1=tuple(set(df_5[df_5['sku'].notnull()]['sku']))
        if len(sku_list1)>1:
            df_5 = new_price(df_5,sku_list1)
            df_5 = domestic_yun(df_5,sku_list1)
            ###匹配上税率
            df_5 = shuilv(df_5, fb_type='FBM')
            ###匹配上毛净利差值
            df_5 = amazon_fbm_gross_net_profit_1(df_5)
            df_5=jp_integral(df_5)
            # 当前售价利润率
            df_5 = amazon_coupon_fu(df_5)
            # promotion
            df_5= amazon_promotion_sku(df_5, n=2)
            df_5['当前售价净利润率'] = 1 - 0.18 - df_5['税率'] - (df_5['运费'] + df_5['成本']) / \
                             ((df_5['price'] + df_5['Shipping']) * (
                                         1 - df_5['your_price_point'] - df_5['percentage_off'] - df_5['promotion_percent']) -
                              (df_5['money_off'] + df_5['Coupon_handling_fee'] + df_5['promotion_amount'])) / df_5['rate']-df_5['FBM毛净利差值']
            df_5 = df_5[df_5['当前售价净利润率'] >= df_5['FBM_profit']]
        else:
            pass
        #看这部分链接的执行情况
        # df_5=domesti_zhixinglv(df_5)
        pd_to_sql(df_5,table=f'domestic_zcp_dx_listing_{qushu_date()}',if_exists='append',database='over_sea')

def domesti_zhixinglv(df):
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    sku_list=tuple(set(df['sku']))
    sql=f""" select account_id,seller_sku,Current_price,Current_price_profit,rate as tj_rate,Target_profit,
     Target_profit_price,fbm_Results_price,ship_cost,adjust_reason
     FROM yibai_price_dom.yibai_domestic_warehouse_increase_fbm_{qushu_date()}
     where sku in {sku_list}"""
    df1=conn_mx.ck_select_to_df(sql)
    df=df.merge(df1,on=['account_id','seller_sku'],how='left')
    df2=df[df['adjust_reason'].isnull()]
    df2.drop(columns=['Current_price','Current_price_profit','tj_rate','Target_profit',
     'Target_profit_price','fbm_Results_price','ship_cost','adjust_reason'])
    sku_list_1=tuple(set())
    sql1=f"""select account_id,seller_sku,Current_price,Current_price_gross_profit,rate as tj_rate,
    Target_gross_profit,Target_profit_price,FBM_Results_price,adjust_reason
    FROM yibai_price_fba.yibai_fba_increase_fbm_{qushu_date()}
    where sku in {sku_list_1}"""
    df3=conn_mx.ck_select_to_df(sql1)
    df2=df2.merge(df3,on=['account_id','seller_sku'],how='left')
    df5=df[df['adjust_reason'].notnull()]
    df6=pd.concat([df2,df5])
    return df6


def domestic_profit():
    df = amazon_zijin_appeal()


if __name__=="__main__":
    fba_data()
    domestic_profit()













