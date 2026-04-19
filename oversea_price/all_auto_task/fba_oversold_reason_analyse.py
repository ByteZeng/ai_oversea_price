import pandas as pd

import traceback
from all_auto_task.scripts_ck_client import CkClient
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import datetime
from tqdm import tqdm
from all_auto_task.dingding import send_msg
import time
from pulic_func.base_api.mysql_connect import pd_to_ck, connect_to_sql, sql_to_pd

# 亚马逊各站点参数
def amazon_zhandian():
    # df = sql_to_pd(sql='SELECT * FROM site_table WHERE platform="亚马逊"',database='domestic_warehouse_clear',
    #                    data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='数据部服务器')
    df = conn_ck.ck_select_to_df('SELECT * FROM domestic_warehouse_clear.yibai_site_table_amazon')
    # amazon平台的站点
    amazon_site_dict = {'site': list(df['site']),
                        '站点': list(df['site1'])}
    # amazon平台的泛欧站点
    df1 = df[df['area'] == '泛欧']
    amazon_site_eu = list(df1['site1'])
    # amazon平台的泛美站点
    df1 = df[df['area'] == '泛美']
    amazon_site_us = list(df1['site1'])

    # 定价是保留小数站点
    df1 = df[df['pricing_digits'] == 2]
    amazon_site_decimal = list(df1['site'])
    return amazon_site_dict, amazon_site_eu, amazon_site_us, amazon_site_decimal

def fanou_fanmei(df, conn_ck):
    """
    判断是否泛欧泛美，增加标识列
    :param df:
    :return:
    """
    #
    amazon_site_dict, amazon_site_eu, amazon_site_us, amazon_site_decimal = amazon_zhandian()
    seller_sku_df=df[df['seller_sku'].notnull()]
    seller_sku_list=tuple(set(seller_sku_df['seller_sku']))
    if 'fnsku' not in df.columns:
        sql = "select * from yibai_fba.fba_fnsku"
        df_fnsku = conn_ck.ck_select_to_df(sql)
        df=df.merge(df_fnsku,on=['account_id','seller_sku'],how='left')

    sql = """
    SELECT id as account_id,account_num as 账号,site from yibai_amazon_account
    """
    account = sql_to_pd(database='yibai_system', sql=sql, data_sys='AMAZON刊登库')
    df1 = pd.DataFrame(amazon_site_dict)
    account = account.merge(df1, on=['site'], how='left')
    account['账号'] = account['账号'].str.lower()
    account = account[['account_id', '站点', '账号']]
    account = account.drop_duplicates(['account_id'], 'first')
    if '站点' in list(df.columns):
        A = '是'
        account = account[['account_id', '账号']]
    else:
        A = '否'
        account = account[['account_id', '站点', '账号']]

    df = df.merge(account, on=['account_id'], how='left')

    # 标识
    df['标识'] = df['账号'] + df['站点']
    # 泛欧站点fnsku相同的就算泛欧
    df.loc[df['站点'].isin(amazon_site_eu), '标识'] = df['账号'] + '欧洲'
    # 泛美站点fnsku相同的不一定算泛美，需要其他条件判断
    fanmei = fanmei_listing()
    fanmei['是否泛美'] = '是'
    df = df.merge(fanmei, on=['account_id', 'seller_sku'], how='left')
    df.loc[(df['站点'].isin(amazon_site_us)) & (df['是否泛美'] == '是'), '标识'] = df['账号'] + '北美'
    #
    df.drop(['账号', '是否泛美'], axis=1, inplace=True)
    # if A == '否':
    #     df.drop(['站点'], axis=1, inplace=True)
    return df


def fanmei_listing():
    #
    sql = """
        SELECT distinct account_id,seller_sku from yibai_fba.fba_fanmei
        """
    conn_mx = pd_to_ck(database='yibai_fba', data_sys='调价明细历史数据')
    fanmei = conn_mx.ck_select_to_df(sql)
    return fanmei

def FBA_month(df3, conn_ck):
    amazon_site_dict, amazon_site_eu, amazon_site_us, amazon_site_decimal = amazon_zhandian()
    #
    sql1=f"""SELECT account_id,sku as seller_sku,fnsku as fnsku1,
        afn_fulfillable_quantity,afn_reserved_quantity,
        afn_inbound_working_quantity+afn_inbound_shipped_quantity+afn_inbound_receiving_quantity as `在途库存数量`
        FROM yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end
        WHERE month='{datetime.date.today()}' AND `condition`='New'"""

    df2 = conn_ck.ck_select_to_df(sql1)
    print(1)
    df2 = df2.drop_duplicates(['account_id', 'seller_sku'], 'first')
    df3 = df3.merge(df2, on=['account_id', 'seller_sku'], how='left')
    df3.loc[df3['fnsku'].isnull(), 'fnsku'] = df3.loc[df3['fnsku'].isnull(), 'fnsku1']
    df3.drop(['fnsku1'], axis=1, inplace=True)
    # 判断是否泛欧泛美
    print(2)
    df = fanou_fanmei(df3, conn_ck)


    print(df.columns)
    # 泛欧的库存通用
    df1 = df[['标识', 'seller_sku', 'fnsku', 'afn_fulfillable_quantity', 'afn_reserved_quantity', '在途库存数量', '站点']]
    df1.columns = ['标识', 'seller_sku', 'fnsku', 'afn_fulfillable_quantity1', 'afn_reserved_quantity1', '在途库存数量1', '站点']
    df1 = df1[df1['afn_fulfillable_quantity1'].notnull()]
    df1['排序'] = 2
    df1.loc[df1['站点'] == '美国', '排序'] = 1
    df1 = df1.sort_values(['排序', 'afn_fulfillable_quantity1'], ascending=[True, False])
    df1 = df1.drop_duplicates(['标识', 'seller_sku', 'fnsku'], 'first')
    df1.drop(['排序', '站点'], axis=1, inplace=True)
    df = df.merge(df1, on=['标识', 'seller_sku', 'fnsku'], how='left')
    del df1
    for col in ['afn_fulfillable_quantity', 'afn_reserved_quantity', '在途库存数量']:
        col1 = col + '1'
        df.loc[df[col].isnull(), col] = df.loc[df[col].isnull(), col1]
        df.loc[(df[col1].notnull()) & (df['站点'].isin(['墨西哥', '加拿大'])), col] = \
            df.loc[(df[col1].notnull()) & (df['站点'].isin(['墨西哥', '加拿大'])), col1]
        df.loc[(df[col1].notnull()) & (df['站点'].isin(amazon_site_eu)), col] = \
            df.loc[(df[col1].notnull()) & (df['站点'].isin(amazon_site_eu)), col1]
        df.drop([col1], axis=1, inplace=True)
        df[col] = df[col].fillna(-9)
        df[col] = df[col].astype(int)

    # 转运预留库存


    sql = """
       SELECT account_id,sku as seller_sku,
       `reserved_fc-transfers` AS transfers,`reserved_fc-processing` AS processing
       FROM yibai_product_kd_sync.yibai_amazon_reserved_inventory
       """
    df_reserved = conn_ck.ck_select_to_df(sql)
    df_reserved = df_reserved.drop_duplicates()
        # df_reserved.to_csv(file_name, index=False)
    df = df.merge(df_reserved, on=['account_id', 'seller_sku'], how='left')
    del df_reserved
    df1 = df[['标识', 'seller_sku', 'fnsku', 'transfers', 'processing', '站点']]
    df1.columns = ['标识', 'seller_sku', 'fnsku', 'transfers1', 'processing1', '站点']
    df1['排序'] = 2
    df1.loc[df1['站点'] == '美国', '排序'] = 1
    df1 = df1.sort_values(['排序', 'transfers1'], ascending=[True, False])
    df1 = df1.drop_duplicates(['标识', 'seller_sku', 'fnsku'], 'first')
    df1.drop(['站点', '排序'], axis=1, inplace=True)
    df = df.merge(df1, on=['标识', 'seller_sku', 'fnsku'], how='left')
    del df1
    for col in ['transfers', 'processing']:
        col1 = col + '1'
        df.loc[df[col].isnull(), col] = df.loc[df[col].isnull(), col1]
        df.loc[(df[col1].notnull()) & (df['站点'].isin(['墨西哥', '加拿大'])), col] = \
            df.loc[(df[col1].notnull()) & (df['站点'].isin(['墨西哥', '加拿大'])), col1]
        df.loc[(df[col1].notnull()) & (df['站点'].isin(amazon_site_eu)), col] = \
            df.loc[(df[col1].notnull()) & (df['站点'].isin(amazon_site_eu)), col1]
        df.drop([col1], axis=1, inplace=True)
        df[col] = df[col].fillna(0)
        df[col] = df[col].astype(int)
    # reserved_customerorders表示客户已下单锁定的库存，transfers转运中的库存，processing重新测量尺寸重量的库存
    #
    df['在库库存数量'] = df['afn_fulfillable_quantity'] + df['transfers'] + df['processing']
    df.loc[(df['afn_reserved_quantity'] == 0) | (df['transfers'] + df['processing'] == 0), '在库库存数量'] = \
        df.loc[
            (df['afn_reserved_quantity'] == 0) | (df['transfers'] + df['processing'] == 0), 'afn_fulfillable_quantity']
    df.drop(['afn_fulfillable_quantity', 'afn_reserved_quantity', 'transfers', 'processing'], axis=1, inplace=True)
    df = df.drop_duplicates(['account_id', 'seller_sku'], 'first')
    df=df[(df['在库库存数量']==0)|(df['在库库存数量']<0)]
    return df

def base_data(conn):
    sql="""select a.account_id account_id ,a.seller_sku as seller_sku,a.adjustment_priority  as sale_status,a.start_time start_time ,b.short_name as short_name,b.site as site
    ,b.account_num account_num,c.group_name as group_name,
    if(empty(e.fnsku),fulfillment_channel_sku,e.fnsku) fnsku
    from
    domestic_warehouse_clear.yibai_fba_clear_new a
    join yibai_system_kd_sync.yibai_amazon_account b on toInt64(a.account_id)=toInt64(b.id)
    join yibai_system_kd_sync.yibai_amazon_group c on b.group_id =c.group_id and c.group_name not like '%深圳精品%'
    left join yibai_product_kd_sync.yibai_amazon_fba_inventory_month_end e on a.account_id=e.account_id
    and e.sku=a.seller_sku
    left join yibai_product_kd_sync.yibai_afn_inventory_data i on a.account_id =i.account_id and i.seller_sku=a.seller_sku
    where end_time is null and adjustment_priority  in ('负利润加快动销','正利润加快动销')
    """
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df=conn_ck.ck_select_to_df(sql)
    print(df.info())



    df1=df[(df['fnsku']=='')]
    del df1['fnsku']
    seller_sku=tuple(set(df1['seller_sku']))
    df=df[df['fnsku']!='']



    # sql = f"""SELECT account_id,seller_sku,fnsku FROM fba_fnsku
    #    where seller_sku in {seller_sku}
    #                 """
    # df_fn = pbm.sql_to_pd(sql=sql, database='monitor_process_data', data_sys='数据部服务器')

    sql = "select * from yibai_fba.fba_fnsku"
    df_fn = conn_ck.ck_select_to_df(sql)

    df1=df1.merge(df_fn,on=['account_id','seller_sku'],how='left')
    df3=pd.concat([df,df1])
    print(df3.info())


    #匹配账号状态
    account="""select a.id as account_id,a.status as "账号状态",a.short_name ,a.site,b.group_name
    from yibai_system_kd_sync.yibai_amazon_account a 
    left join yibai_system_kd_sync.yibai_amazon_group b on a.group_id =b.group_id  """
    df_account=conn_ck.ck_select_to_df(account)
    df3=df3.merge(df_account,on=['account_id'],how='left')


    df=FBA_month(df3, conn_ck)
    print(5)
    #只要raw2表有的
    sql="""select account_id,seller_sku,case when status='1' then 'Active' else 'Inactive' end as listing_status
    ,case when a.asin1<>'' then a.asin1 else b.asin1 end as asin
    ,open_date,price
    from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 a
    left join (select account_id ,seller_sku ,asin1 from yibai_product_kd_sync.yibai_amazon_listing_alls
    where fulfillment_channel ='AMA') b on a.account_id =b.account_id 
    and a.seller_sku =b.seller_sku  
    where fulfillment_channel ='AMA'"""
    df_raw=conn_ck.ck_select_to_df(sql)
    df_raw.drop_duplicates(subset=['account_id','seller_sku'],inplace=True)
    df_raw['raw2']=1
    df=df.merge(df_raw,on=['account_id','seller_sku'],how='left')
    df['原因']=None
    df.loc[df['raw2']!=1,'原因']='raw2表获取不到，可能已下架'
    df['price']=df['price'].astype('float64')
    df.loc[(df['raw2']==1)&(df['price']==0),'原因']='raw2表价格为0'
    df.loc[(df['在库库存数量']<0),'在库库存数量']='month_end抓取不到数据'
    df.loc[(df['在途库存数量']<0),"在途库存数量"]='month_end抓取不到数据'
    #是否在sku_map表查询不到
    print(df.info())

    #看今天是否调价

    # no_reason_listing = conn_ck.ck_execute_sql("show tables like 'no_adjust_reason_listing%'")[0]
    table_list = conn_ck.ck_show_tables(db_name='support_document')
    no_reason_listing = [i for i in table_list if 'no_adjust_reason_listing' in i]
    if len(no_reason_listing)>0:
        no_reason_number = [int(i[-8:]) for i in no_reason_listing]
        d = max(no_reason_number)
        sql=f"""select account_id,seller_sku,reason
        FROM support_document.no_adjust_reason_listing_{d}
        where item ='FBA调价';"""
        sql1=f"""select distinct account_id,reason as reason1
        FROM support_document.no_adjust_reason_account_{d}
        where item ='FBA调价'"""
        df_no_account=conn_ck.ck_select_to_df(sql1, columns=['account_id', 'reason1'])
        df_no=conn_ck.ck_select_to_df(sql, columns=['account_id', 'seller_sku', 'reason'])
        df_no.drop_duplicates(subset=['account_id','seller_sku'],inplace=True)
        df=df.merge(df_no,on=['account_id','seller_sku'],how='left')
        df=df.merge(df_no_account,on=['account_id'],how='left')
        df.loc[df['reason1'].notnull(),'reason']=df['reason1']
        del df['reason1']

    # fba_price=conn_ck1.ck_execute_sql("show tables like 'fba_price_adjustment_fba%'")[0]
        sql=f"""select account_id,seller_sku,meet_criteria,tar_priority, Current_price,Current_gross_rate, Current_net_rate
        , tar_net_profit, tar_profit, Target_profit_price, Results_price,result_profit_rate
        from yibai_price_fba.yibai_fba_price_adjustment_fba_{d}"""
        df_log=conn_ck.ck_select_to_df(sql)
        df_log['log']=1
        df_log.drop_duplicates(subset=['account_id','seller_sku'],inplace=True)
        df=df.merge(df_log,on=['account_id','seller_sku'],how='left')
        df.loc[df['reason'].notnull(),'原因']=df['reason']
        df.loc[df['log']==1,'原因']="已执行调价,调至"+df['tar_priority']
        print(6)
        print(df.info())

    # 去重
    df.drop_duplicates(inplace=True)
    #查询剩下原因是否因为sku_map表没有捆绑
    df_s=df[df['原因'].isnull()]
    seller_sku_s=tuple(set(df['seller_sku']))
    df_map=pd.DataFrame()
    a,b=divmod(len(seller_sku_s),5000)
    for i in tqdm(range(a+1)):
        sql=f"""select account_id,seller_sku,sku,deliver_mode 
        from yibai_product_kd_sync.yibai_amazon_sku_map
        where seller_sku in {seller_sku_s[i*5000:(i+1)*5000]}"""
        df_map1=conn_ck.ck_select_to_df(sql)
        df_map=df_map.append(df_map1)
    df_map1=fanou_fanmei(df_map, conn_ck)
    print("map1表",df_map1.info())
    print(df_map1.head())
    df_raw11=df_raw[['account_id','seller_sku','asin']]
    df_map1=df_map1.merge(df_raw11,on=['account_id','seller_sku'])
    df_map1=df_map1[['标识','asin','sku']].drop_duplicates()
    df_map.columns=['account_id','seller_sku','sku1','deliver_mode1']
    df=df.merge(df_map,on=['account_id','seller_sku'],how='left')
    df.loc[(df['原因'].isnull())&(df['sku1'].isnull()),"原因"]="没有捆绑，yibai_amazon_sku_map查询不到"
    #看看是否泛欧泛美+asin维度有捆绑
    print(df_map1.info())
    df_map1=df_map1[['标识','asin','sku']].drop_duplicates()
    df=df.merge(df_map1,on=['标识','asin'],how='left')
    df.loc[(df['原因']=='没有捆绑，yibai_amazon_sku_map查询不到')&(df['sku'].notnull()),'原因']='链接维度yibai_amazon_sku_map查询不到，但按照泛美泛欧+asin可以在前面捆绑表搜查得到'
    #sku_map查询不到是否在back_up表有
    sql=f"""SELECT account_id,seller_sku FROM `yibai_amazon_listing_alls_delete_backup`
    where seller_sku in {seller_sku_s}"""
    df_back_up= sql_to_pd(sql=sql,database='yibai_product',data_sys='AMAZON刊登库')
    df_back_up['back_up']=1
    df=df.merge(df_back_up,on=['account_id','seller_sku'],how='left')
    df.loc[(df['原因']=='没有捆绑，yibai_amazon_sku_map查询不到')&(df['back_up']==1),"原因"]='没有捆绑，yibai_amazon_sku_map查询不到，在backup表'



    #查询下month_end当天没有抓到数据，不调价原因下，afn表库存状态
    sql="""select account_id ,seller_sku ,sum(case when warehouse_condition_code ='SELLABLE'
    then toInt64(quantity_available) else 0 end) as sellable_stock,
    sum(case when warehouse_condition_code ='UNSELLABLE'
    then toInt64(quantity_available) else 0 end ) as unsellable_stock
    from yibai_product_kd_sync.yibai_afn_inventory_data 
    group by account_id ,seller_sku"""
    df1=conn_ck.ck_select_to_df(sql)

    df1.drop_duplicates(subset=['account_id','seller_sku'],inplace=True)
    print(df1.info())

    df22=df[df['原因']=='month_end当天没有抓到的数据，不调价']
    df2=df1.merge(df22,on=['account_id','seller_sku'])
    df = df.merge(df1, on=['account_id', 'seller_sku'], how='left')
    print(df2.info())
    df2.drop_duplicates(['seller_sku', 'fnsku', '标识'], inplace=True)
    df2=df2[['seller_sku','fnsku','标识','sellable_stock','unsellable_stock']]

    df2.columns=['seller_sku','fnsku','标识','sellable_stock1','unsellable_stock1']
    df=df.merge(df2,on=['seller_sku','fnsku','标识'],how='left')
    for i in ['sellable_stock','unsellable_stock']:
        df.loc[df[f"{i}"].isnull(),f'{i}']=df[f'{i}1']
    df['afn表数据状态']=None
    df.loc[df['sellable_stock']>0,'afn表数据状态']='afn表库存大于0'
    df.loc[df['sellable_stock']==0,'afn表数据状态']='afn表库存为0'
    df.loc[df['sellable_stock'].isnull(),'afn表数据状态']="afn表抓取不到数据"
    return df


def delete_data(conn):
    sql = f"delete from over_sea.fba_oversold_reasons  where date='{datetime.date.today().isoformat()}'"
    conn.execute(sql)

def main():
    try:
        conn_ck = pd_to_ck(database='over_sea', data_sys='调价存档数据1')
        conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
        #
        d_t = datetime.date.today().isoformat()
        sql = f"""
        alter table over_sea.fba_oversold_reasons delete where date='{d_t}'
        """
        conn_ck.ck_execute_sql(sql)
        # sql = f"""
        # optimize table over_sea.fba_oversold_reasons final
        # """
        # conn_ck.ck_execute_sql(sql)
        i = 0
        while i<15:
            i = i + 1
            sql = f"""
            select count() as num from over_sea.fba_oversold_reasons 
            where date='{d_t}'
            """
            df = conn_ck.ck_select_to_df(sql)
            if df['num'][0] == 0:
                break
            else:
                time.sleep(60)
        #
        # delete_data(conn)
        df = base_data(conn)
        df['date'] = d_t
        df.rename(columns={'账号状态':'account_status','在途库存数量':"on_way_stock"
                           ,"站点":"account_site","标识":"baioshi","在库库存数量":"available_stock"
                           ,"原因":"reasons_about_no_callback","afn表数据状态":"status_of_table_afn_data"}
                  ,inplace=True)
        df.drop(['sync_time'], axis=1, inplace=True)
        print(df.columns)
        print(df.dtypes)
        for col in ['account_id', 'account_status']:
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype(int)
        conn_ck.write_to_ck_json_type(df, 'fba_oversold_reasons')
        # conn.to_sql(df, table='fba_oversold_reasons', if_exists='append')
        # send_msg('动销组定时任务推送', 'fba超卖数据',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}fba超卖数据已计算完成请在over_sea.fba_oversold_reasons"
        #          f"查收数据！", is_all=True)
        conn.close()
    except Exception as e:
        # send_msg('动销组定时任务推送', 'fba超卖数据',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}fba超卖数据计算出现问题,请及时排查,失败原因详情请查看airflow",
        #          is_all=True,
        #          status='失败')
        raise Exception(traceback.format_exc())

if __name__=="__main__":
    main()







