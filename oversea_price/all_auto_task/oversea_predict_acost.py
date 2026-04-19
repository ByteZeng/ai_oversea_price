import datetime
import traceback
from all_auto_task.dingding import send_msg
import time
import pandas as pd
from sqlalchemy import create_engine
import pymysql
# from all_auto_task.scripts_ck_client import CkClient
import re
from pulic_func.base_api import mysql_connect as pbm
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql


def r_cycle(i):
    return int(''.join(re.findall('([0-9])', i)))


def table_list(start, end, conn):
    table1 = conn.show_table_list('over_sea')
    table = [i for i in table1 if re.findall("(^oversea_age_).*([0-9])(_dtl$)", i)]
    table_need = [i for i in table if start <= r_cycle(i) <= end]
    return table_need


def short_date(a):
    d = datetime.date.today() - datetime.timedelta(days=a)

    d1 = d.isoformat().replace("-", '')
    d1 = int(d1)
    return d1


def yb_oversea_sku_age1(d1, d2):
    sql = f"""select distinct *
    from 

    (select CASE 

    WHEN a.origin_sku LIKE 'GB-%%' THEN REPLACE(a.origin_sku,'GB-','') 

    WHEN a.origin_sku LIKE 'DE-%%' THEN REPLACE(a.origin_sku,'DE-','') 

    WHEN a.origin_sku LIKE 'FR-%%' THEN REPLACE(a.origin_sku,'FR-','') 

    WHEN a.origin_sku LIKE 'ES-%%' THEN REPLACE(a.origin_sku,'ES-','') 

    WHEN a.origin_sku LIKE 'IT-%%' THEN REPLACE(a.origin_sku,'IT-','') 

    WHEN a.origin_sku LIKE 'AU-%%' THEN REPLACE(a.origin_sku,'AU-','') 

    WHEN a.origin_sku LIKE 'CA-%%' THEN REPLACE(a.origin_sku,'CA-','') 

    WHEN a.origin_sku LIKE 'JP-%%' THEN REPLACE(a.origin_sku,'JP-','') 

    WHEN a.origin_sku LIKE 'US-%%' THEN REPLACE(a.origin_sku,'US-','') 

    WHEN a.origin_sku LIKE '%%DE' THEN REPLACE(a.origin_sku,'DE','')

    ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    from yb_oversea_sku_age a
    where date>='{d1}'
    and date<'{d2}'
    and status in (0,1)
    union ALL 
    select  CASE 

    WHEN  a.country in ('GB','UK') and a.origin_sku not LIKE 'GB-%%' THEN concat('GB-',a.origin_sku)
    WHEN  a.country IN ('CZ','CS','DE') and a.origin_sku not LIKE '%DE%%'   THEN concat('DE-',a.origin_sku)

    when a.country='FR' and a.origin_sku not like 'FR-%%' then concat('FR-',a.origin_sku)

    when a.country in ('ES','SP')  and a.origin_sku not like 'ES-%%' then concat('ES-',a.origin_sku)

    when a.country='IT' and a.origin_sku not like 'IT-%%' then concat('IT-',a.origin_sku)

    when a.country='AU' and a.origin_sku not like '%%AU%%' then concat('AU-',a.origin_sku)

    when a.country='CA' and a.origin_sku not like 'CA-%%' then concat('CA-',a.origin_sku)

    when a.country='JP' and a.origin_sku not like 'JP-%%' then concat('JP-',a.origin_sku)

    when a.country='US' and a.origin_sku not like 'US-%%' then concat('US-',a.origin_sku)

    ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    from yb_oversea_sku_age a
    where date>='{d1}'
    and date<'{d2}'
    and status in (0,1)) asku"""
    return sql


def real_cang_zu(data, a, conn):
    # 匹配真实仓租
    sql = f""" select a.sku ,a.charge_currency,sum(a.charge_total_price) as "仓租_本币",a.warehouse,a.country,a.warehouse_id,a.warehouse_name
        from (SELECT A.sku as sku,A.charge_currency,
        case when A.charge_total_price is null then 0 else A.charge_total_price  end as charge_total_price,A.country,b.id as warehouse_id,b.warehouse_name,
                 

        CASE 

        WHEN A.country='US' THEN '美国仓'

        WHEN A.country IN ('UK','GB') THEN '英国仓'

        WHEN A.country IN ('CZ','CS','DE') THEN '德国仓'

        WHEN A.country='FR' THEN '法国仓'

        WHEN A.country='IT' THEN '意大利仓'

        WHEN A.country='AU' THEN '澳洲仓'

        WHEN A.country IN ('ES','SP') THEN '西班牙仓'

        WHEN A.country='CA' THEN '加拿大仓'

        WHEN A.country='JP' THEN '日本仓'

        WHEN A.country='PL' THEN '波兰仓'

        ELSE '美国仓' END AS warehouse


        FROM ({yb_oversea_sku_age1(d1=f'{datetime.date.today() - datetime.timedelta(a + 2)}', d2=f'{datetime.date.today() - datetime.timedelta(a - 5)}')}) A
				left join yb_warehouse_erp b on A.warehouse_code=b.warehouse_code

        WHERE 
        status in (0,1)) a
        group by a.sku,a.warehouse,a.charge_currency,a.warehouse_id,a.country,warehouse_name"""
    print(sql)
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    zu = ck_client.ck_select_to_df(ck_sql=sql)
    print(zu.head())

    sql2 = 'select distinct from_currency_code as charge_currency,rate from domestic_warehouse_clear.erp_rate'
    rate = conn.read_sql(sql2)

    print(rate.head())
    zu = zu.merge(rate, on=['charge_currency'], how='left')
    zu['仓租_本币'] = zu['仓租_本币'].astype('float')

    zu['仓租'] = zu['仓租_本币'] * zu['rate']
    ###20220215添加useful里面的头程和关税成本及库存金额
    sku_list = tuple(set(zu["sku"]))
    sql = f"""select distinct sku,warehouseId as warehouse_id,firstCarrierCost*available_stock as firstCarrierCost
       ,dutyCost*available_stock as dutyCost,available_stock*newPrice  as available_stock_money_test
       from oversea_transport_fee_useful
       where sku in {sku_list}"""
    use_df = conn.read_sql(sql)
    zu = zu.merge(use_df, on=['sku', 'warehouse_id'], how='left')
    zu.drop_duplicates(inplace=True)
    zu = zu.groupby(['sku', 'warehouse'])[
        '仓租', 'firstCarrierCost', 'dutyCost', 'available_stock_money_test'].sum().reset_index()

    # zu.to_excel('仓租.xlsx')
    zu = zu[['sku', 'warehouse', '仓租', 'firstCarrierCost', 'dutyCost', 'available_stock_money_test']]
    zu.drop_duplicates(inplace=True)
    # 开始匹配仓租费用
    data = data.merge(zu, on=['sku', 'warehouse'], how='left')
    data['仓租'].fillna(0, inplace=True)
    # data.to_excel(f"仓租费用{datetime.date.today() - datetime.timedelta(days=a)}.xlsx")
    return data


def dingjia_profit(data, a, conn):
    df_dtl = pd.DataFrame()
    for i in table_list(short_date(a + 30), short_date(a), conn):
        d = str(r_cycle(i))
        d = d[:4] + "-" + d[4:6] + "-" + d[6:]
        d1 = datetime.datetime.strptime(d, "%Y-%m-%d")
        w = d1.isoweekday()
        if w == 1 or w == 4:
            sql_dtl = f"""select sku,warehouse,after_profit,{r_cycle(i)} as date
                       from {i}"""
            df_dtl1 = conn.read_sql(sql_dtl)
            df_dtl = df_dtl.append(df_dtl1)
    df_dtl.sort_values(by='date', axis=0, ascending=False, inplace=True)
    df_dtl.drop_duplicates(subset=['sku', 'warehouse'], inplace=True)
    del df_dtl['date']
    data = data.merge(df_dtl, on=['sku', 'warehouse'], how='left')
    return data


# 取订单数据
def order_data(conn):
    # 匹配上今天的库存,库存金额，日销，销库金额
    to = datetime.date.today()
    while True:
        sql_to = f"""
            select distinct sku,warehouse,available_stock,available_stock_money,day_sales*new_price as '销库金额',
            `inv_age_0_to_30_days`,`inv_age_30_to_60_days`,`inv_age_60_to_90_days`, `inv_age_90_to_120_days`
            ,`inv_age_120_to_150_days`,`inv_age_150_to_180_days`,`inv_age_180_to_210_days`,`inv_age_210_plus_days`
            ,`inv_age_0_to_40_days`,`inv_age_40_to_70_days`,`inv_age_70_plus_days`
            from over_sea_age_new where date='{to}' and available_stock>0
           """
        df_to = conn.read_sql(sql_to)
        if len(df_to) > 0:
            break
        else:
            to = to - datetime.timedelta(days=1)
        # 本周真实仓租
    df_to = real_cang_zu(df_to, 8, conn)
    df_to.rename(columns={'仓租': 'this_week_warehouse_rent'}, inplace=True)
    ##'firstCarrierCost','dutyCost','available_stock_money'
    df_to.rename(columns={"firstCarrierCost": "今日总头程费用", "dutyCost": "今日总关税成本",
                          "available_stock_money_test": "今日库存金额"},
                 inplace=True)
    df_to = real_cang_zu(df_to, 15, conn)
    df_to.rename(columns={"仓租": 'last_week_warehouse_rent'}, inplace=True)
    df_to.drop(columns=['firstCarrierCost', 'dutyCost', 'available_stock_money_test'], inplace=True)

    sql1 = """select sku,warehouse,sum(total_price) as this_week_order_amt,sum(quantity) as this_week_quantity
            ,sum(true_profit_new1) as this_week_profit
            ,sum(real_profit) as this_week_net_profit
            ,sum(real_profit)/sum(total_price) as this_week_net_profit_rate
            from dashbord_new_data
            where created_time>=DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 8 day),'%%Y-%%m-%%d') 
            and created_time<DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 day),'%%Y-%%m-%%d') and sales_status='总计'
            group by sku,warehouse"""

    df1 = conn.read_sql(sql1)

    # 定价要往出单前推一周则
    df_to = dingjia_profit(df_to, 9, conn)
    df_to.rename(columns={'after_profit': 'this_week_ago_adjust_net_profit'}, inplace=True)
    print(df1.head())

    # 跑出上周的定价净利
    # df_dtl=pd.DataFrame()
    # for i in table_list(short_date(a=14),short_date(a=8)):
    #     sql_dtl=f"""select sku,warehouse,after_profit,{r_cycle(i)} as date
    #             from {i}"""
    #     df_dtl1=sql_to_pd(sql=sql_dtl,database='over_sea')
    #     df_dtl=df_dtl.append(df_dtl1)
    # df_dtl.sort_values(by='date',axis=0,ascending=False)
    # df_dtl.drop_duplicates(subset=['sku','warehouse'],inplace=True)

    # 跑上周订单明细及定价净利
    sql2 = """select sku,warehouse,sum(total_price) as last_week_order_amt,sum(quantity) as last_week_quantity
            ,sum(true_profit_new1) as last_week_profit
            ,sum(real_profit) as last_week_net_profit
            ,sum(real_profit)/sum(total_price) as last_week_net_profit_rate
            from dashbord_new_data
            where created_time>=DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 15 day),'%%Y-%%m-%%d') 
            and created_time<DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 8 day),'%%Y-%%m-%%d') and sales_status='总计'
            group by sku,warehouse"""
    df = conn.read_sql(sql2)

    df_to = dingjia_profit(df_to, 16, conn)
    df_to.rename(columns={"after_profit": 'last_week_ago_adjust_net_profit'}, inplace=True)
    print(df.head())
    df_all = df_to.merge(df, on=['sku', 'warehouse'], how='left')
    df_all = df_all.merge(df1, on=['sku', 'warehouse'], how='left')

    # 匹配上本周仓租
    # df1=real_cang_zu(df1,a=7)
    # df1.rename(columns={'仓租':"this_week_warehouse_rent"},inplace=True)
    df_all['this_week_order_amt'].fillna(0, inplace=True)
    df_all['this_week_quantity'].fillna(0, inplace=True)
    df_all['this_week_profit'].fillna(-999999, inplace=True)
    df_all['this_week_net_profit'].fillna(-999999, inplace=True)
    df_all.loc[df_all['this_week_order_amt'] == 0, 'this_week_net_profit_rate'] = -999999
    # 计算本周仓租acost

    df_all.loc[(df_all['this_week_order_amt'] != 0), 'this_week_acost'] = df_all['this_week_warehouse_rent'] / df_all[
        'this_week_order_amt']
    df_all.loc[
        (df_all['this_week_order_amt'] == 0) & (df_all['this_week_warehouse_rent'] != 0), 'this_week_acost'] = 999999
    df_all.loc[(df_all['this_week_order_amt'] == 0) & (df_all['this_week_warehouse_rent'] == 0), 'this_week_acost'] = 0

    # 匹配上上周的仓租
    # df=real_cang_zu(df, a=14)
    df_all['last_week_order_amt'].fillna(0, inplace=True)
    df_all['last_week_quantity'].fillna(0, inplace=True)
    df_all['last_week_profit'].fillna(-999999, inplace=True)
    df_all['last_week_net_profit'].fillna(-999999, inplace=True)
    df_all.loc[df_all['last_week_order_amt'] == 0, 'last_week_net_profit_rate'] = -999999
    # 计算上周仓租acost

    df_all.loc[(df_all['last_week_order_amt'] != 0), 'last_week_acost'] = df_all['last_week_warehouse_rent'] / df_all[
        'last_week_order_amt']
    df_all.loc[
        (df_all['last_week_order_amt'] == 0) & (df_all['last_week_warehouse_rent'] != 0), 'last_week_acost'] = 999999
    df_all.loc[(df_all['last_week_order_amt'] == 0) & (df_all['last_week_warehouse_rent'] == 0), 'last_week_acost'] = 0

    return df_all


# 预测卖完的仓租
def predict_warehouse_rent(df, a, conn):
    sql = f""" select sku,warehouse, sum(`0_to_30_predict_amt_rmb` +`30_to_60_predict_amt_rmb` +`60_to_90_predict_amt_rmb` 
            +`90_to_120_predict_amt_rmb` +`120_to_180_predict_amt_rmb` +`180_to_270_predict_amt_rmb` +`270_to_360_predict_amt_rmb` 
            +`360_plus_predict_amt_rmb` ) as '预测卖完仓租'
            from warehouse_stock_charges
            where date='{datetime.date.today() - datetime.timedelta(days=a)}'
            group by sku,warehouse"""
    df1 = conn.read_sql(sql)
    df = df.merge(df1, on=['sku', 'warehouse'], how='left')
    return df


# 开始预测
def predict_data(data, conn):
    # 计算昨天的库存，可售天数，日销并算出卖完订单金额，卖完净利,卖完所需仓租
    this_week_date = datetime.date.today() - datetime.timedelta(days=1)
    while True:
        sql1 = f"""
            select sku,warehouse,new_price,available_stock as '昨日库存',available_stock_money as '昨日库存金额',
            day_sales as '昨日日销',estimated_sales_days as '昨日可售天数'
            from over_sea_age_new  
            where date='{this_week_date}'
        """
        df = conn.read_sql(sql1)
        if len(df) > 0:
            break
        else:
            this_week_date -= datetime.timedelta(days=1)

    data = data.merge(df, on=['sku', 'warehouse'], how='left')
    # 计算卖完订单金额
    data['predict_amt_after_this_week'] = data['this_week_order_amt'] / 7 * data['昨日可售天数']
    # 匹配预计卖完仓租(昨天到卖完的仓租)
    data = predict_warehouse_rent(data, 1, conn)
    data.loc[data['昨日库存'] == 0, '预测卖完仓租'] = 0
    # 计算本周卖完仓租acsot

    data['predict_acost_after_this_week'] = 999
    data.loc[(data["predict_amt_after_this_week"] != 0), 'predict_acost_after_this_week'] = data['预测卖完仓租'] / data[
        'predict_amt_after_this_week']

    data.rename(columns={'预测卖完仓租': 'predict_warehouse_rent_after_this_week'}, inplace=True)
    # 计算卖完净利
    ### 本周预测净利润=(本周净利润+本周销售金额*0.05)/7*可售天数-本周预测仓租(其中本周销售金额*0.05是因为净毛利润率差值里面有5%是仓租花费)
    data['predict_net_profit_after_this_week'] = (data['this_week_net_profit'] + data[
        'this_week_order_amt'] * 0.05) / 7 * data['昨日可售天数'] - data['predict_warehouse_rent_after_this_week']

    # 计算上周最后一天的库存，可售天数，日销并计算出卖完订单金额，卖完净利，卖完仓租
    last_week_date = datetime.date.today() - datetime.timedelta(days=8)
    while True:
        sql2 = f"""select sku,warehouse,available_stock as '7天前库存',available_stock_money as '7天前库存金额'
        ,day_sales as '7天前日销',estimated_sales_days as '7天前可售天数'
        from over_sea_age_new  where date='{last_week_date}'"""
        df2 = conn.read_sql(sql2)
        if len(df2) > 0:
            break
        else:
            last_week_date -= datetime.timedelta(days=1)
    data = data.merge(df2, on=['sku', 'warehouse'], how='left')
    # 计算卖完订单金额
    print("是否有7天前可售天数", data.columns)
    data['predict_amt_after_last_week'] = data['last_week_order_amt'] / 7 * data['7天前可售天数']
    # 匹配7天前卖完仓租
    data = predict_warehouse_rent(data, 8, conn)
    data.loc[data['7天前库存'] == 0, '预测卖完仓租'] = 0
    data['predict_acost_after_last_week'] = 999
    data.loc[data["predict_amt_after_last_week"] != 0, 'predict_acost_after_last_week'] = data['预测卖完仓租'] / data[
        'predict_amt_after_last_week']

    data.rename(columns={'预测卖完仓租': 'predict_warehouse_rent_after_last_week'}, inplace=True)
    # 计算卖完净利
    data['predict_net_profit_after_last_week'] \
        = (data['last_week_net_profit'] + data['last_week_order_amt'] * 0.05) / 7 * data['7天前可售天数'] - data[
        'predict_warehouse_rent_after_last_week']

    # 2022/1/17按要求分档
    acost_fendang(data, col='predict_acost_after_this_week', c='predict_acost_after_this_week_section')
    acost_fendang(data, col='predict_acost_after_last_week', c='predict_acost_after_last_week_section')
    day_sales_fendang(data, col='昨日日销', c='昨日日销分段')
    day_sales_fendang(data, col='7天前日销', c='7天前日销分段')
    estimated_fendang(data, '昨日可售天数', c='昨日可售天数分段')
    estimated_fendang(data, '7天前可售天数', c='7天前可售天数分段')

    # 计算净利增益,预测这周卖完净利-上周预测卖完净利
    ###20220215净利增益公式从本周卖完净利-上周预测卖完净利改为本周预测卖完净利-上周预测卖完净利-本周真实仓租+本周真实净利润
    data['净利增益'] = data['predict_net_profit_after_this_week'] - data['predict_net_profit_after_last_week'] \
                       - data['this_week_warehouse_rent'] + data["this_week_net_profit"]
    # data['定价调整净利']=data['this_week_ago_adjust_net_profit']-data['last_week_ago_adjust_net_profit']

    print(data.head())
    # data.to_excel('测试数据.xlsx',index=False)
    data['date'] = datetime.date.today()
    # 销毁价
    df_des = destruction_price(conn)
    data = data.merge(df_des, on=['sku', 'warehouse'], how='left')
    ##20220215继续销毁带来的收益=-(头程+关税+销毁费用+库存金额)
    data['直接销毁带来的收益'] = -(
                data['今日总头程费用'] + data['今日总关税成本'] + data['今日库存金额'] + data["destruction_price"])
    data['是否销毁'] = "否"
    data.loc[data['predict_net_profit_after_this_week'] < data['直接销毁带来的收益'], "是否销毁"] = "是"
    data.rename(columns={'今日总头程费用': "firstCarrierCost_today", "今日总关税成本": "dutyCost_today",
                         "今日库存金额": "available_stock_money_today"
        , "直接销毁带来的收益": "destructionCost", "是否销毁": "is_desctruction"}, inplace=True)
    print(data.columns)

    # 分类化档
    data = data[["sku", "warehouse", "available_stock", "available_stock_money", "销库金额", "inv_age_0_to_30_days",
                 "inv_age_30_to_60_days",
                 "inv_age_60_to_90_days", "inv_age_90_to_120_days", "inv_age_120_to_150_days",
                 "inv_age_150_to_180_days",
                 "inv_age_180_to_210_days", "inv_age_210_plus_days", "inv_age_0_to_40_days", "inv_age_40_to_70_days",
                 "inv_age_70_plus_days", "this_week_warehouse_rent", "last_week_warehouse_rent",
                 "this_week_ago_adjust_net_profit",
                 "last_week_ago_adjust_net_profit", "last_week_order_amt", "last_week_quantity", "last_week_profit",
                 "last_week_net_profit",
                 "last_week_net_profit_rate", "this_week_order_amt", "this_week_quantity", "this_week_profit",
                 "this_week_net_profit",
                 "this_week_net_profit_rate", "this_week_acost", "last_week_acost", "new_price", "昨日库存",
                 "昨日库存金额",
                 "昨日日销", "昨日日销分段", "昨日可售天数", "昨日可售天数分段", "predict_amt_after_this_week",
                 "predict_warehouse_rent_after_this_week",
                 "predict_acost_after_this_week", "predict_acost_after_this_week_section",
                 "predict_net_profit_after_this_week",
                 "7天前库存", "7天前库存金额", "7天前日销", "7天前日销分段", "7天前可售天数", "7天前可售天数分段",
                 "predict_warehouse_rent_after_last_week",
                 "predict_amt_after_last_week", "predict_acost_after_last_week",
                 "predict_acost_after_last_week_section",
                 "predict_net_profit_after_last_week", "净利增益", "destruction_price", "date", "firstCarrierCost_today"
        , "dutyCost_today", "available_stock_money_today", "destructionCost", "is_desctruction"]]
    data.drop_duplicates(inplace=True)
    conn.to_sql(data, 'oversea_predict_acost', if_exists='append')
    return data


def yb_oversea_sku_age(a):
    sql = f"""select distinct *
    from 

    (select CASE 

    WHEN a.origin_sku LIKE 'GB-%%' THEN REPLACE(a.origin_sku,'GB-','') 

    WHEN a.origin_sku LIKE 'DE-%%' THEN REPLACE(a.origin_sku,'DE-','') 

    WHEN a.origin_sku LIKE 'FR-%%' THEN REPLACE(a.origin_sku,'FR-','') 

    WHEN a.origin_sku LIKE 'ES-%%' THEN REPLACE(a.origin_sku,'ES-','') 

    WHEN a.origin_sku LIKE 'IT-%%' THEN REPLACE(a.origin_sku,'IT-','') 

    WHEN a.origin_sku LIKE 'AU-%%' THEN REPLACE(a.origin_sku,'AU-','') 

    WHEN a.origin_sku LIKE 'CA-%%' THEN REPLACE(a.origin_sku,'CA-','') 

    WHEN a.origin_sku LIKE 'JP-%%' THEN REPLACE(a.origin_sku,'JP-','') 

    WHEN a.origin_sku LIKE 'US-%%' THEN REPLACE(a.origin_sku,'US-','') 

    WHEN a.origin_sku LIKE '%%DE' THEN REPLACE(a.origin_sku,'DE','')

    ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    from yb_oversea_sku_age a
    where date='{datetime.date.today() - datetime.timedelta(days=a + 2)}'
    and status in (0,1)
    union ALL 
    select  CASE 

    WHEN  a.country in ('GB','UK') and a.origin_sku not LIKE 'GB-%%' THEN concat('GB-',a.origin_sku)
    WHEN  a.country IN ('CZ','CS','DE') and a.origin_sku not LIKE '%DE%%'   THEN concat('DE-',a.origin_sku)

    when a.country='FR' and a.origin_sku not like 'FR-%%' then concat('FR-',a.origin_sku)

    when a.country in ('ES','SP')  and a.origin_sku not like 'ES-%%' then concat('ES-',a.origin_sku)

    when a.country='IT' and a.origin_sku not like 'IT-%%' then concat('IT-',a.origin_sku)

    when a.country='AU' and a.origin_sku not like '%%AU%%' then concat('AU-',a.origin_sku)

    when a.country='CA' and a.origin_sku not like 'CA-%%' then concat('CA-',a.origin_sku)

    when a.country='JP' and a.origin_sku not like 'JP-%%' then concat('JP-',a.origin_sku)

    when a.country='US' and a.origin_sku not like 'US-%%' then concat('US-',a.origin_sku)

    ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    from yb_oversea_sku_age a
    where date='{datetime.date.today() - datetime.timedelta(days=a + 2)}'
    and status in (0,1)) asku"""
    return sql


def destruction_price(conn):
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')

    # 计算海外仓销毁价
    sql = f"""select a.sku as sku,a.warehouse_code,a.oversea_type,a.charge_currency,b.warehouse_name,CASE 
    
        WHEN a.country='US' THEN '美国仓'
    
        WHEN a.country IN ('UK','GB') THEN '英国仓'
    
        WHEN a.country IN ('CZ','CS','DE') THEN '德国仓'
    
        WHEN a.country='FR' THEN '法国仓'
    
        WHEN a.country='IT' THEN '意大利仓'
    
        WHEN a.country='AU' THEN '澳洲仓'
    
        WHEN a.country IN ('ES','SP') THEN '西班牙仓'
    
        WHEN a.country='CA' THEN '加拿大仓'
    
        WHEN a.country='JP' THEN '日本仓'
    
        WHEN a.country='PL' THEN '波兰仓'
    
        ELSE '美国仓' END AS warehouse,sum(a.warehouse_stock) as warehouse_stock from ({yb_oversea_sku_age(a=0)}) a
    left join yb_warehouse_erp b on a.warehouse_code=b.warehouse_code
    
    where a.date='{datetime.date.today() - datetime.timedelta(days=2)}' and a.status in (0,1)
    group by a.sku,a.warehouse_code,a.oversea_type,a.charge_currency,b.warehouse_name,warehouse"""
    df = ck_client.ck_select_to_df(ck_sql=sql)

    # 计算销毁价
    sku = tuple(set(df['sku']))
    sql1 = f"""select sku,toFloat64(pur_length_pack) *toFloat64(pur_weight_pack) *toFloat64(pur_width_pack) 
               as "体积重",case when toFloat64(weight_out_storage)=0 
               then toFloat64(product_weight_gross) else toFloat64(weight_out_storage) end as "实际重"
               from yibai_prod_base_sync.yibai_prod_sku 
               order by create_time desc 
               """
    df1 = ck_client.ck_select_to_df(ck_sql=sql1)
    df1["体积重"] = df1["体积重"] / 1000000
    df1["实际重"] = df1["实际重"] / 1000
    df = df.merge(df1, on=['sku'], how='left')
    df['site'] = df['warehouse'].apply(lambda x: str(x)[:-1])
    df.loc[df['site'] == '澳洲', 'site'] = '澳大利亚'

    sql2 = """select site,rate from domestic_warehouse_clear.erp_rate"""
    df2 = conn.read_sql(sql2)
    df = df.merge(df2, on=['site'], how='left')

    # 计算万邑通的销毁价
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['实际重'] > 0) & (
                df['实际重'] <= 0.5), "单价"] = 0.13
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['实际重'] > 0.5) & (
                df['实际重'] <= 1), "单价"] = 0.18
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['实际重'] > 1) & (
                df['实际重'] <= 2), "单价"] = 0.21
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['实际重'] > 2) & (
                df['实际重'] <= 10), "单价"] = 0.35
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['实际重'] > 10) & (
                df['实际重'] <= 20), "单价"] = 0.49
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['实际重'] > 20) & (
                df['实际重'] <= 30), "单价"] = 0.64
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['实际重'] > 30), "单价"] = 0.64 + (
                df['实际重'] - 30) / 10 * 0.33
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '美国仓') & (df['单价'] > 3.24), '单价'] = 3.24

    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['实际重'] > 0) & (
                df['实际重'] <= 0.5), "单价"] = 0.1
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['实际重'] > 0.5) & (
                df['实际重'] <= 1), "单价"] = 0.13
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['实际重'] > 1) & (
                df['实际重'] <= 2), "单价"] = 0.17
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['实际重'] > 2) & (
                df['实际重'] <= 10), "单价"] = 0.26
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['实际重'] > 10) & (
                df['实际重'] <= 20), "单价"] = 0.38
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['实际重'] > 20) & (
                df['实际重'] <= 30), "单价"] = 0.48
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['实际重'] > 30), "单价"] = 0.48 + (
                df['实际重'] - 30) / 10 * 0.25
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '英国仓') & (df['单价'] > 2.42), '单价'] = 2.42

    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['实际重'] > 0) & (
                df['实际重'] <= 0.5), "单价"] = 0.13
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['实际重'] > 0.5) & (
                df['实际重'] <= 1), "单价"] = 0.17
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['实际重'] > 1) & (
                df['实际重'] <= 2), "单价"] = 0.21
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['实际重'] > 2) & (
                df['实际重'] <= 10), "单价"] = 0.34
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['实际重'] > 10) & (
                df['实际重'] <= 20), "单价"] = 0.49
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['实际重'] > 20) & (
                df['实际重'] <= 30), "单价"] = 0.61
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['实际重'] > 30), "单价"] = 0.61 + (
                df['实际重'] - 30) / 10 * 0.31
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '德国仓') & (df['单价'] > 3.09), '单价'] = 3.09

    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse_name'].str.contains('波兰')) & (df['实际重'] > 0) & (
                df['实际重'] <= 0.5), "单价"] = 0.13
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse_name'].str.contains('波兰')) & (df['实际重'] > 0.5) & (
                df['实际重'] <= 1), "单价"] = 0.13
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse_name'].str.contains('波兰')) & (df['实际重'] > 1) & (
                df['实际重'] <= 2), "单价"] = 0.26
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse_name'].str.contains('波兰')) & (df['实际重'] > 2) & (
                df['实际重'] <= 10), "单价"] = 1.29
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse_name'].str.contains('波兰')) & (df['实际重'] > 10) & (
                df['实际重'] <= 20), "单价"] = 2.57
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse_name'].str.contains('波兰')) & (df['实际重'] > 20) & (
                df['实际重'] <= 30), "单价"] = 3.86
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse_name'].str.contains('波兰')) & (
                df['实际重'] > 30), "单价"] = 3.86 + (df['实际重'] - 30) / 10 * 1.29

    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['实际重'] > 0) & (
                df['实际重'] <= 0.5), "单价"] = 0.15
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['实际重'] > 0.5) & (
                df['实际重'] <= 1), "单价"] = 0.19
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['实际重'] > 1) & (
                df['实际重'] <= 2), "单价"] = 0.24
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['实际重'] > 2) & (
                df['实际重'] <= 10), "单价"] = 0.41
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['实际重'] > 10) & (
                df['实际重'] <= 20), "单价"] = 0.58
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['实际重'] > 20) & (
                df['实际重'] <= 30), "单价"] = 0.75
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['实际重'] > 30), "单价"] = 0.75 + (
                df['实际重'] - 30) / 10 * 0.38
    df.loc[(df['oversea_type'] == 'WYT') & (df['warehouse'] == '澳洲仓') & (df['单价'] > 3.77), '单价'] = 3.77

    # 计算谷仓销毁价
    df.loc[(df['oversea_type'] == 'GC') & (df['warehouse'] == '美国仓'), '单价'] = 0.5
    df.loc[(df['oversea_type'] == 'GC') & (df['warehouse'] == '英国仓'), '单价'] = 0.5
    df.loc[(df['oversea_type'] == 'GC') & (df['warehouse'].str.contains('法|德|西|意')), '单价'] = 0.6
    df.loc[(df['oversea_type'] == 'GC') & (df['warehouse_name'].str.contains('捷克')), '单价'] = 0.5
    df.loc[(df['oversea_type'] == 'GC') & (df['warehouse'] == '澳洲仓'), '单价'] = 0.8

    df.loc[(df['oversea_type'] == '4PX'), '单价'] = 3 / df['rate'] * df['实际重']
    df['销毁价'] = df['单价'] * df['warehouse_stock'] * df['rate']

    df.drop_duplicates(inplace=True)

    df = df.groupby(['sku', 'warehouse'])['销毁价'].sum().reset_index()
    df['销毁价'] = df['销毁价'].apply(lambda x: round(x, 2))
    df.rename(columns={'销毁价': 'destruction_price'}, inplace=True)
    return df


def acost_fendang(data, col: str = "", c: str = "acost分段"):
    """
    :param data: dataFrame
    :param col: 预测仓租acost列
    :param c: 分档后的新列(自己取名字)
    """
    """
        oversea_predict_acost中当前仓租ACOT和上个礼拜仓租ACOST需要划挡
        A.0
        B.0~0.05
        C.0.05~0.1
        D.0.1~0.2
        E.0.2~0.5
        F.0.5~1
        G.1~2
        H.2~5
        I.5~10
        J.10~100000
        """
    data[c] = None
    data.loc[data[col] == 0, c] = "A.0"
    data.loc[(data[col] > 0) & (data[col] <= 0.05), c] = "B.0~0.05"
    data.loc[(data[col] > 0.05) & (data[col] <= 0.1), c] = "C.0.05~0.1"
    data.loc[(data[col] > 0.1) & (data[col] <= 0.2), c] = "D.0.1~0.2"
    data.loc[(data[col] > 0.2) & (data[col] <= 0.5), c] = "E.0.2~0.5"
    data.loc[(data[col] > 0.5) & (data[col] <= 1), c] = "F.0.5~1"
    data.loc[(data[col] > 1) & (data[col] <= 2), c] = "G.1~2"
    data.loc[(data[col] > 2) & (data[col] <= 5), c] = "H.2~5"
    data.loc[(data[col] > 5) & (data[col] <= 10), c] = "I.5~10"
    data.loc[(data[col] > 10), c] = "J.10~∞"
    return data


def day_sales_fendang(data, col, c):
    """

    :param data: 数据集
    :param col: 传进去进行分档的列
    :param c: 分档后新列的名字
    :return:

    当前日销以及上周日销划档
    A.日均销量，S=0
    B.日均销量，S∈(0,0.1]
    C.日均销量，S∈(0.1,0.3]
    D.日均销量，S∈(0.3,0.6]
    E.日均销量，S∈(0.6,1]
    F.日均销量，S∈(1,3]
    G.日均销量，S∈(3,5]
    H.日均销量，S∈(5,∞)
    """
    data.loc[data[col] == 0, c] = "A.日均销量，S=0"
    data.loc[(data[col] > 0) & (data[col] <= 0.1), c] = "B.日均销量，S∈(0,0.1]"
    data.loc[(data[col] > 0.1) & (data[col] <= 0.3), c] = "C.日均销量，S∈(0.1,0.3]"
    data.loc[(data[col] > 0.3) & (data[col] <= 0.6), c] = "D.日均销量，S∈(0.3,0.6]"
    data.loc[(data[col] > 0.6) & (data[col] <= 1), c] = "E.日均销量，S∈(0.6,1]"
    data.loc[(data[col] > 1) & (data[col] <= 3), c] = "F.日均销量，S∈(1,3]"
    data.loc[(data[col] > 3) & (data[col] <= 5), c] = "G.日均销量，S∈(3,5]"
    data.loc[(data[col] > 5), c] = "H.日均销量，S∈(5,∞)"
    return data


def estimated_fendang(data, col, c):
    """
    :param data: 数据集
    :param col: 需要传进去的列
    :param c: 需要分档的列
    :return:
    A.可售天数0~30
    B.可售天数30~60
    C.可售天数60~90
    D.可售天数90~180
    E.可售天数180~360
    F.可售天数360以上
    """
    data.loc[(data[col] > 0) & (data[col] <= 30), c] = "A.可售天数0~30"
    data.loc[(data[col] > 30) & (data[col] <= 60), c] = "B.可售天数30~60"
    data.loc[(data[col] > 60) & (data[col] <= 90), c] = "C.可售天数60~90"
    data.loc[(data[col] > 90) & (data[col] <= 180), c] = "D.可售天数90~180"
    data.loc[(data[col] > 180) & (data[col] <= 360), c] = "E.可售天数180~360"
    data.loc[(data[col] > 360), c] = "F.可售天数360以上"
    return data


def delete_data(conn):
    sql1 = f"""delete from oversea_predict_acost where date='{datetime.date.today()}'"""
    conn.execute(sql1)


def acost_main():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        print(datetime.date.today())
        delete_data(conn)
        df = order_data(conn)
        print(df.info())
        data = predict_data(df, conn)
        d = datetime.date.today() - datetime.timedelta(days=1)
        while True:
            sql = f"""
            select * from  over_sea_age_new
            where date='{d}'and available_stock>0
            """
            test = conn.read_sql(sql)
            if len(test) > 0:
                break
            else:
                d = d - datetime.timedelta(days=1)
        t1 = datetime.datetime.strptime((test['DATE'][0]), "%Y-%m-%d")
        t2 = datetime.datetime.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d")
        t = (t2 - t1).days
        d = ((len(data) - len(test)).__abs__()) / t

        if t <= 360:
            send_msg('动销组定时任务推送', '海外仓预测仓租acost',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓预测仓租acost运行成功,总数据量为{len(data)},与昨日数据差异量为{d},数据量正常"
                     f",请到over_sea.oversea_predict_acost查收数据!",
                     mobiles=['+86-13419546972'], is_all=False)
        else:
            send_msg('动销组定时任务推送', '海外仓预测仓租acost',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓预测仓租acost总数据量为{len(data)},与昨日数据差异量>360,数据量异常,请检查"
                     f",请到over_sea.oversea_predict_acost查收数据!",
                     mobiles=['+86-13419546972'], is_all=False)
            raise Exception(
                f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓预测仓租acost总数据量为{len(data)},与昨日数据差异量>360,数据量异常,请检查!")
        conn.close()
    except Exception as e:
        send_msg('动销组定时任务推送', '海外仓预测仓租acost',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}计算海外仓预测仓租acost出现问题"
                 f",请到over_sea.oversea_predict_acost检查数据!",
                 mobiles=['+86-13419546972'], is_all=False)
        raise Exception(traceback.format_exc())


if __name__ == "__main__":
    acost_main()
