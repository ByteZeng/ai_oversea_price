# -*- coding: utf-8 -*-
"""
Created on Tue Jul 22 20:19:02 2025

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
每周汇报清仓框定数据剩余
6.26-7.10-7.17-7.28
"""


import sys
sys.path.append(r"C:\Users\Administrator.YB-202108021116\yibai-price-strategy")
import datetime
import time 
from pulic_func.base_api.mysql_connect import pd_to_sql,sql_to_pd,pd_to_ck,pd
import numpy as  np
from pulic_func.base_api.adjust_price_function_amazon import fanou_fanmei
conn_mx = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
conn_mx2 = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据2')
conn_tt = pd_to_ck(database='tt_oms_sync', data_sys='通拓MRP')
sql = "SELECT DISTINCT  sku  from yibai_domestic.yibai_out_of_system_sku  where  start_time  = '2025-06-18' "
conn_mx = pd_to_ck(database='tt_oms_sync', data_sys='调价明细历史数据')
sku = conn_mx.ck_select_to_df(sql)
sku['sku'] = sku['sku'].astype(str)
dom_fbm = pd.DataFrame()
for date1 in ['2025-08-05','2025-08-12','2025-08-19']:
    print(date1)
    date11 = date1.replace('-','')
    sql = f"""
    SELECT '{date1}' `日期`,sku,cost,Total_inventory_quantity `总库存`,(cost*Total_inventory_quantity) `库存金额` ,Total_inventory_quantity-inventory_of_age_over_180_days `0-180天库存`,
    inventory_of_age_over_180_days- inventory_of_age_over_270_days `180-270天库存`,inventory_of_age_over_270_days-inventory_of_age_over_360_days `270-360天库存`,inventory_of_age_over_360_days `超360天库存`  
    from support_document.domestic_warehouse_clear_sku_{date11} where Total_inventory_quantity > 0
    """
    if date1> '2025-07-28':
        fbm = conn_mx.ck_select_to_df(sql)
    else :
        # fbm = conn_mx2.ck_select_to_df(sql)
        pass
    fbm['sku'] = fbm['sku'].astype(str)
    fbm = fbm[fbm['sku'].isin(sku['sku'])]
    fbm['0-180库存金额'] = fbm['cost'] * fbm['0-180天库存']  
    fbm['180-270天库存金额'] = fbm['cost'] * fbm['180-270天库存']  
    fbm['270-360天库存金额'] = fbm['cost'] * fbm['270-360天库存']  
    fbm['超360库存金额'] = fbm['cost'] * fbm['超360天库存']  
    dom_fbm = dom_fbm.append(fbm,ignore_index=True)

dom_fbm['sku计数'] = 1

dom_fbm= dom_fbm.groupby(['日期'])['sku计数','库存金额','0-180库存金额', '180-270天库存金额', '270-360天库存金额', '超360库存金额'].sum().reset_index()
dom_fbm['超360占比'] = dom_fbm['超360库存金额'] / dom_fbm['库存金额']

#-----------------销库
sql = """
with sku1 as (SELECT DISTINCT  sku  from yibai_domestic.yibai_out_of_system_sku  where  start_time  = '2025-06-18' )
SELECT *,toYearWeek(toDate(  created_time),1 ) `周` from domestic_warehouse_clear.monitor_dom_order where created_time > '2025-06-08' and sku in sku1
"""
order = conn_mx.ck_select_to_df(sql)

sql = """
SELECT *,toYearWeek(toDate(  created_time),1 ) `周` from domestic_warehouse_clear.monitor_dom_order where created_time > '2025-06-08' 
"""
order1 = conn_tt.ck_select_to_df(sql)
order1['sku'] = order1['sku'].astype(str)
order1 = order1[order1['sku'].isin(sku['sku'])]
order = order.append(order1,ignore_index=True)

order1 = order.groupby('order_id')['sellersku_saleprice'].sum().reset_index()
order1.columns = ['order_id','sellersku_saleprice1']
order = order.merge(order1,on='order_id')
order['销售额'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['销售额']
order['毛利润'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['毛利润']
order['净利润'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['净利润']
order['最小时间'] = order['created_time'].copy()
order['最大时间'] = order['created_time'].copy()
order1 = order.groupby('周').agg({'销售额':sum,'净利润':sum,'毛利润':sum,'销库金额':sum,'最小时间':min,'最大时间':max}).reset_index()
order1['净利率'] = order1['净利润']/order1['销售额']

dom_order = order1.copy()
# =============================================================================
# fba-yb
# =============================================================================
sql = """
WITH table1 as (SELECT DISTINCT account_id ,seller_sku  from yibai_fba.yibai_out_of_system_listing  where start_time = '2025-07-08' )
select account_id ,seller_sku,cost,sku from yibai_fba.fba_fees where (account_id ,seller_sku) in table1
"""
fba_fees = conn_mx.ck_select_to_df(sql)
fba = pd.DataFrame()
# '2025-06-26','2025-07-10','2025-07-17','2025-07-28','2025-08-05','2025-08-12'
for date1 in ['2025-08-05','2025-08-12','2025-08-19']:
    date11 = date1.replace('-','')
    sql = f"""
    SELECT '{date1}' `日期`,account_id ,seller_sku ,`站点`,`标识`,area `区域`,afn_fulfillable_quantity `可售库存`,inv_age_0_to_90_days,`超90天库龄`,`超180天库龄`,`超270天库龄`,`超365天库龄` 
    FROM fba_inventory.amazon_fba_inventory_age_self_calculated_{date11} WHERE (afn_fulfillable_quantity > 0 or `超365天库龄`>0) and  (`标识` not like '%北美%' or `站点`= '美国') and account_id in {tuple(fba_fees['account_id'].drop_duplicates())}
    """
    print(date1)
    
    if date1> '2025-07-28':
        temp = conn_mx.ck_select_to_df(sql)
    else :
        temp = conn_mx2.ck_select_to_df(sql)
    temp = temp.drop_duplicates(['account_id','seller_sku'])

    fba = fba.append(temp,ignore_index=True)
fba = fba.merge(fba_fees,on=['account_id','seller_sku'])

fba['sku'] = fba['sku'].astype(str)

fba1 = fba.drop_duplicates(['日期','标识','seller_sku'])
fba1['可售库存金额'] = fba1['可售库存'] * fba1['cost']
fba1['0-90天库存金额'] = fba1['inv_age_0_to_90_days'] * fba1['cost']
fba1['90-180天库存金额'] = (fba1['超90天库龄']-fba1['超180天库龄']) * fba1['cost']
fba1['180-270天库存金额'] = (fba1['超180天库龄']-fba1['超270天库龄'])  * fba1['cost']
fba1['270-365天库存金额'] = (fba1['超270天库龄']-fba1['超365天库龄'])  * fba1['cost']
fba1['超365天库存金额'] = fba1['超365天库龄'] * fba1['cost']
fba1['链接数'] = 1
fba2 = fba1.groupby(['日期'])['链接数','可售库存','可售库存金额','0-90天库存金额', '90-180天库存金额', '180-270天库存金额', '270-365天库存金额','超365天库存金额'].sum().reset_index()
fba2['超365占比'] = (fba2['超365天库存金额']) /(fba2['0-90天库存金额']+fba2['90-180天库存金额']+fba2['180-270天库存金额']+fba2['270-365天库存金额']+fba2['超365天库存金额'])
fba_yb_stock = fba2.copy()

sql = """
with sel as (SELECT account_id ,seller_sku  from yibai_fba.yibai_out_of_system_listing where start_time ='2025-07-08' )
SELECT *,toYearWeek(toDate(  created_time),1 ) `周` from domestic_warehouse_clear.yibai_monitor_fba_order where (account_id ,seller_sku) in sel and created_time > '2025-06-08'
"""
order = conn_mx.ck_select_to_df(sql)

order1 = order.groupby('order_id')['sellersku_saleprice'].sum().reset_index()
order1.columns = ['order_id','sellersku_saleprice1']
order = order.merge(order1,on='order_id')
order['销售额'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['销售额']
order['毛利润'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['毛利润']
order['净利润'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['净利润']
order['最小时间'] = order['created_time'].copy()
order['最大时间'] = order['created_time'].copy()
order1 = order.groupby('周').agg({'销售额':sum,'净利润':sum,'毛利润':sum,'销库金额':sum,'最小时间':min,'最大时间':max}).reset_index()
order1['净利率'] = order1['净利润']/order1['销售额']

fba_yb_order = order1.copy()

# =============================================================================
# fba-TT
# =============================================================================
sql = """
WITH table1 as (SELECT DISTINCT account_id ,seller_sku  from tt_fba.tt_out_of_system_listing  where start_time = '2025-07-14' )
select account_id ,seller_sku,cost,sku from tt_fba.fba_fees where (account_id ,seller_sku) in table1
"""
fba_fees = conn_tt.ck_select_to_df(sql)
fba = pd.DataFrame()
for date1 in ['2025-06-26','2025-07-10','2025-07-17','2025-07-28','2025-08-05','2025-08-12','2025-08-19']:
    date11 = date1.replace('-','')
    sql = f"""
    SELECT '{date1}' `日期`,account_id ,seller_sku ,`站点`,`标识`,area `区域`,afn_fulfillable_quantity `可售库存`,inv_age_0_to_90_days,`超90天库龄`,`超180天库龄`,`超270天库龄`,`超365天库龄` 
    FROM tt_price_fba.tt_amazon_fba_inventory_age_self_calculated_{date11} WHERE afn_fulfillable_quantity > 0 and ( `标识` not like '%北美%' or `站点`= '美国')
    """
    sql1 = f"""
    select '{date1}' `日期`,account_id ,seller_sku ,`站点`,`标识`,area `区域`,afn_fulfillable_quantity `可售库存`,inv_age_0_to_90_days,`超90天库龄`,`超180天库龄`,`超270天库龄`,`超365天库龄` 
    from file('./external_files/clickhouse_parquet_bak/ck_51_179/tt_price_fba/{date11}/tt_price_fba.tt_amazon_fba_inventory_age_self_calculated_{date11}.parquet', Parquet) WHERE afn_fulfillable_quantity > 0 and  `标识` not like '%北美%' or `站点`= '美国'
    """
    print(date1)
    if date1> '2025-08-11':
        temp = conn_tt.ck_select_to_df(sql)
    else :
        temp = conn_tt.ck_select_to_df(sql1)
    fba = fba.append(temp,ignore_index=True)
fba = fba.merge(fba_fees,on=['account_id','seller_sku'])

fba['sku'] = fba['sku'].astype(str)

fba1 = fba.drop_duplicates(['日期','标识','seller_sku'])
fba1['可售库存金额'] = fba1['可售库存'] * fba1['cost']
fba1['0-90天库存金额'] = fba1['inv_age_0_to_90_days'] * fba1['cost']
fba1['90-180天库存金额'] = (fba1['超90天库龄']-fba1['超180天库龄']) * fba1['cost']
fba1['180-270天库存金额'] = (fba1['超180天库龄']-fba1['超270天库龄'])  * fba1['cost']
fba1['270-365天库存金额'] = (fba1['超270天库龄']-fba1['超365天库龄'])  * fba1['cost']
fba1['超365天库存金额'] = fba1['超365天库龄'] * fba1['cost']
fba1['链接数'] = 1
fba2 = fba1.groupby(['日期'])['链接数','可售库存','可售库存金额','0-90天库存金额', '90-180天库存金额', '180-270天库存金额', '270-365天库存金额','超365天库存金额'].sum().reset_index()
fba2['超365占比'] = (fba2['超365天库存金额']) /(fba2['0-90天库存金额']+fba2['90-180天库存金额']+fba2['180-270天库存金额']+fba2['270-365天库存金额']+fba2['超365天库存金额'])
fba_tt_stock = fba2.copy()

sql = """
with sel as (SELECT account_id ,seller_sku  from tt_fba.tt_out_of_system_listing where start_time ='2025-07-14' )
SELECT *,toYearWeek(toDate(  created_time),1 ) `周` from domestic_warehouse_clear.tt_monitor_fba_order where (account_id ,seller_sku) in sel and created_time > '2025-06-08'
"""
order = conn_tt.ck_select_to_df(sql)

order1 = order.groupby('order_id')['sellersku_saleprice'].sum().reset_index()
order1.columns = ['order_id','sellersku_saleprice1']
order = order.merge(order1,on='order_id')
order['销售额'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['销售额']
order['毛利润'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['毛利润']
order['净利润'] = order['sellersku_saleprice']/order['sellersku_saleprice1'] * order['净利润']
order['最小时间'] = order['created_time'].copy()
order['最大时间'] = order['created_time'].copy()
order1 = order.groupby('周').agg({'销售额':sum,'净利润':sum,'毛利润':sum,'销库金额':sum,'最小时间':min,'最大时间':max}).reset_index()
order1['净利率'] = order1['净利润']/order1['销售额']

fba_tt_order = order1.copy()


# =============================================================================
# 海外仓
# =============================================================================
sql = "SELECT *  FROM over_sea.oversea_flash_clearout_sku WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)"
sku = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
sku = sku[~sku['type'].str.contains('转泛品')]
sku = sku[~sku['type'].str.contains('转VC')]

sku = sku[['sku','warehouse']]
# sql = "SELECT sku,warehouse  FROM over_sea.oversea_flash_clearout_sku "
# sku = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
sku['sku'] = sku['sku'].astype(str)
sku = sku[~sku['warehouse'].isin(['印度尼西亚仓','马来西亚仓','菲律宾仓','泰国仓'])]
sku = sku.drop_duplicates()
df = pd.DataFrame()
for date1 in ['2025-06-26','2025-07-10','2025-07-17','2025-07-28','2025-08-05','2025-08-12','2025-08-19']:
    date11 = date1.replace('-','')
    print(date1)
    sql = f"""
    SELECT
        a.*,
        IF(b.sale_status IS NULL ,'正常',b.sale_status) as `销售状态`,'{date1}' `日期`
    FROM (
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date1}'
        and (available_stock > 0)
    ) a
    LEFT JOIN (
        SELECT *
        FROM over_sea.oversea_sale_status
        WHERE end_time IS NULL
    ) b ON a.sku=b.sku AND a.warehouse=b.warehouse
    """
    temp = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
    temp['sku'] = temp['sku'].astype(str)
    temp = temp.merge(sku,on=['sku','warehouse'])
    df = df.append(temp,ignore_index=True)
df['new_price'] = df['new_price'].astype(float)
df['可售库存金额'] = df['available_stock'] * df['new_price']
df['库龄表库存金额'] = df['warehouse_stock'] * df['new_price']
df['90-180库存金额'] = (df['age_90_plus'] - df['age_180_plus']) * df['new_price']
df['180-270库存金额'] = (df['age_180_plus'] - df['age_270_plus']) * df['new_price']
df['270-360库存金额'] = (df['age_270_plus'] - df['age_360_plus']) * df['new_price']
df['超360库存金额'] = df['age_360_plus'] * df['new_price']


df1 = df.groupby('日期')['available_stock','可售库存金额','库龄表库存金额', '90-180库存金额','180-270库存金额', '270-360库存金额', '超360库存金额'].sum().reset_index()
df1 = df1.rename(columns={'warehouse':'大仓','available_stock':'可售库存'})
df1['超180占比'] = (df1['180-270库存金额']+ df1['270-360库存金额']+df1['超360库存金额'])/df1['库龄表库存金额']
del df1['库龄表库存金额']
hw_stock = df1.copy()

#-------------------销库
sql = """SELECT order_id ,sku,warehouse,created_time,toYearWeek(toDate(  created_time),1 ) `周`,
release_money `销库金额`,total_price `销售额`,true_profit_new1 `毛利润`,real_profit  `净利润`
FROM over_sea.dashbord_new_data1 x
WHERE created_time >'2025-06-08' and sales_status <> '总计' and total_price > 0 """
conn_mx = pd_to_ck(database='yibai_oms_sync', data_sys='数据部服务器')
df = conn_mx.ck_select_to_df(sql)
df['组织'] = '易佰'
sql = """SELECT order_id ,sku,warehouse,payment_time created_time,toYearWeek(toDate(  payment_time),1 ) `周`,
release_money `销库金额`,total_price `销售额`,true_profit_new1 `毛利润`,real_profit  `净利润`
FROM yibai_oversea.tt_dashbord_new_data1 
WHERE payment_time >'2025-06-08' and sales_status <> '总计' and total_price > 0 """
conn_mx = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
df1 = conn_mx.ck_select_to_df(sql)
df1['组织'] = '通拓'
df = df.append(df1,ignore_index=True)
df['sku'] = df['sku'].astype(str)

df1 = df.groupby('order_id')['销库金额'].sum().reset_index()
df1.columns=['order_id','销库金额1']
df = df.merge(df1,on='order_id')
df['销售额'] = df['销库金额']/df['销库金额1'] * df['销售额']
df['毛利润'] = df['销库金额']/df['销库金额1'] * df['毛利润']
df['净利润'] = df['销库金额']/df['销库金额1'] * df['净利润']

df = df.merge(sku,on=['sku','warehouse'])

df['最小时间'] = df['created_time'].copy()
df['最大时间'] = df['created_time'].copy()
df1 = df.groupby('周').agg({'销售额':sum,'净利润':sum,'毛利润':sum,'销库金额':sum,'最小时间':min,'最大时间':max}).reset_index()
df1['净利率'] = df1['净利润']/df1['销售额']

hw_order = df1.copy()


writer = pd.ExcelWriter(fr'D:\数据分析2024\1展昭\限时清仓\20250819.xlsx')
dom_fbm.to_excel(excel_writer=writer,sheet_name='国内仓库存',index=False)
hw_stock.to_excel(excel_writer=writer,sheet_name='海外仓库存',index=False)
fba_yb_stock.to_excel(excel_writer=writer,sheet_name='yb_fba_stock',index=False)
fba_tt_stock.to_excel(excel_writer=writer,sheet_name='tt_fba_stock',index=False)
dom_order.to_excel(excel_writer=writer,sheet_name='国内仓订单',index=False)
hw_order.to_excel(excel_writer=writer,sheet_name='海外仓订单',index=False)
fba_yb_order.to_excel(excel_writer=writer,sheet_name='yb_fba_order',index=False)
fba_tt_order.to_excel(excel_writer=writer,sheet_name='tt_fba_order',index=False)
writer.save()
writer.close()