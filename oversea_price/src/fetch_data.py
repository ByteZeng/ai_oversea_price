# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 18:06:40 2023

@author: Administrator
"""

from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import pymysql
import time
import pandas as pd

## 连接CK数据库
def fetch_ck(sql, data_sys=78, database="yb_datacenter"):
    '''
    连接 ClickHouse 数据库
    data_sys: 121.37.248.212-->数据管理部
              121.37.30.78-->调价明细历史数据
              139.159.244.34  --> 多组织订单
    ''' 
    if data_sys == 78:
        connection = 'clickhouse://zhangyilan:zhangyilan2109221544@121.37.30.78:8123/{}'.format(database)
        # connection = 'clickhouse://wangjie:wangjiE301503122120@121.37.30.78:8123/{}'.format(database)
        # connection = 'clickhouse://songzhanzhao:songzhanZhaomrp01@121.37.30.78:8123/{}'.format(database)
        engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    # elif data_sys == 212:
    #     connection = 'clickhouse://songzhanzhao:songzHanzhao0103220490@121.37.248.212:8123/{}'.format(database)
    #     engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    elif data_sys == 190:
        connection = 'clickhouse://zengzhijie:ze65nG_zHij5ie@139.9.0.190:8123/{}'.format(database)
        engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    elif data_sys == 212:
        connection = 'clickhouse://gaoyuzhou:3gao3Yu45ZhO3u@121.37.248.212:8123/{}'.format(database)
        engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    elif data_sys == 140:
        connection = 'clickhouse://datax:datax#07231226@172.16.51.140:9001/{}'.format(database)
        engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    elif data_sys == 34:
        connection = 'clickhouse://zengzhijie:ze65nG_zHij5ie@139.159.244.34:8123/{}'.format(database)
        engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    else:
        print("未指定数据库服务器！")
        return None
    session = make_session(engine)
    print('Fetch CK data...')
    t1 = time.time()
    cursor = session.execute(sql)
    try:
        fields = cursor._metadata.keys
        df = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
    finally:
        cursor.close()
        session.close()
    print('Done! Time passed {:.4f}'.format(time.time() - t1))    
    return df


## 连接MYSQL数据库
def fetch_mysql(sql, data_sys=212, database='monitor_process_data'):
    '''
    连接 MYSQL 数据库
    data_sys: 121.37.248.212  -->数据管理部
              (124.71.5.174)
              (121.37.239.131  110950)
              139.9.206.7     -->数仓基础库
              124.71.220.143  -->销售运营分析系统
              124.71.81.224   -->yxh刊登库 yibai_product
              124.71.85.224   -->亿迈订单从库 yibai_dcm_order
              124.71.63.61    -->云仓从库：库龄数据 yb_stock_center  vlEFa0WsjD
              121.37.223.56   --> 新212服务器
              116.205.232.15  --> cj_本地库
              124.71.6.250    --> cj_刊登从库
              121.37.228.71   --> fba可转寄库存
    '''

    if data_sys == 212:
        # conn = pymysql.connect(db=database, user="209313", password="vlEFa0WsjD",
        #                        host="124.71.5.174", port=3306, charset='utf8')
        conn = pymysql.connect(db=database, user="209313", password="vlEFa0WsjD",
                               host="121.37.239.131", port=11905, charset='utf8')
    # if data_sys == 212:
    #     conn = pymysql.connect(db=database, user="panxx", password="panxx#Mrp01",
    #                            host="121.37.248.212", port=3306, charset='utf8')    
    elif data_sys == 7:
        conn = pymysql.connect(db=database, user="songzhanzhao", password="szzyibai%^&8",
                               host="139.9.206.7", port=3306, charset='utf8')
        # conn = pymysql.connect(db=database, user="wangjie", password="WjY&^*534(j",
        #                host="139.9.206.7", port=3306, charset='utf8')
        
    elif data_sys == 143:
        conn = pymysql.connect(db=database, user="songzhanzhao", password="szzyibai%^&8",
                               host="124.71.220.143", port=3306, charset='utf8') 
    elif data_sys == 186:
        conn = pymysql.connect(db=database, user="209313", password="vlEFa0WsjD",
                       host="124.71.73.186", port=3306, charset='utf8')
    elif data_sys == 81:
        conn = pymysql.connect(db=database, user="202655", password="WHJ6DXAywF",
                               host="124.71.81.224", port=3306, charset='utf8')
    elif data_sys == 85:
        conn = pymysql.connect(db=database, user="202655", password="WHJ6DXAywF",
                               host="124.71.85.224", port=3306, charset='utf8')    
    elif data_sys == 177:
        conn = pymysql.connect(db=database, user="DMuser", password="pgp2GT4qeX",
                               host="172.16.50.177", port=3306, charset='utf8')  
    elif data_sys == 61:
        conn = pymysql.connect(db=database, user="209313", password="vlEFa0WsjD",
                           host="124.71.63.61", port=3306, charset='utf8') 
    elif data_sys == 244:
        conn = pymysql.connect(db=database, user="209313", password="vlEFa0WsjD",
                           host="121.37.197.244", port=3306, charset='utf8') 
    elif data_sys == 56:
        conn = pymysql.connect(db=database, user="209313", password="HSdACLDewz",
                           host="121.37.223.56", port=3306, charset='utf8')  
    elif data_sys == 15:
        conn = pymysql.connect(db=database, user="202655", password="SADF#@AS#@%GH99!",
                           host="116.205.232.15", port=3306, charset='utf8')
    elif data_sys == 250:
        conn = pymysql.connect(db=database, user="202655", password="WHJ6DXAywF",
                           host="124.71.6.250", port=3306, charset='utf8')
    elif data_sys == 71:
        conn = pymysql.connect(db=database, user="yibai209313", password="PZwog4vZ48",
                               host="121.37.228.71", port=9030, charset='utf8')
    else:
        print("未指定数据库服务器！")
        return None
    
    cursor=conn.cursor()
    t1 = time.time()
    print('Fetch Mysql data...')
    cursor.execute(sql)
    df = pd.DataFrame(list(cursor.fetchall()))
    if len(df) == 0: 
        print('取数为空')
        pass
    else: 
        cols = []
        for x in cursor.description:
            cols.append(x[0])
        df.columns = cols
    print('Done! Time passed {:.4f}'.format(time.time() - t1))
    conn.commit();cursor.close();conn.close()
    return df

def update_mysql(sql, data_sys=212, database='monitor_process_data'):
    '''
    更新、删除操作
    '''
    if data_sys == 212:
        conn = pymysql.connect(db=database, user="zhenghongwei", password="Zhenghongwei#mrp01",
                               host="121.37.248.212", port=3306, charset='utf8')
    
    cursor=conn.cursor()
    print('Update Mysql data...')
    cursor.execute(sql)
    print('Done!')
    