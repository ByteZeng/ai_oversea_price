import datetime

from pulic_func.adjust_price_base_api.public_function import *


# from dateutil.relativedelta import relativedelta
# from amazon_others.adjust_for_refund.basic_func import *


def get_order(order_date):
    # sql = f"""
    # select a.order_id,a.account_id,
    # case when a.warehouse_id=323 then 'FBA' when d.warehouse_type=1 then '国内仓' else '海外仓' end 仓位,
    # c.sku,c.sku_old seller_sku,c.item_id,c.total_price 链接金额,c.sale_price 链接售价,c.ship_price 链接运费,c.quantity_old 销量,
    # b.currency_rate 订单汇率,b.total_price 订单金额,shipping_price 订单运费金额,b.adjust_amount 订单调整金额,
    # b.final_value_fee 订单佣金,b.profit_new1 订单利润1,b.true_profit_new1 订单利润,
    # b.packing_cost+b.package_cost 订单包装包材成本,b.stock_price+b.exchange_price 订单库存汇损成本,
    # b.purchase_cost_new1 订单产品成本,b.shipping_cost 订单运费成本1,b.true_shipping_fee 订单运费成本,
    # b.first_carrier_cost 订单头程,b.duty_cost 订单头程关税,b.return_taxation 物流税金
    # from
    # (select date(created_time),date(paytime),order_id,account_id,warehouse_id
    # from yibai_order.yibai_order_amazon
    # where paytime>='{order_date} 00:00:00' and paytime<='{order_date} 23:59:59' and total_price!=0) a
    # left join yibai_order.yibai_order_profit b on a.order_id=b.order_id
    # left join yibai_order.yibai_order_amazon_detail c on a.order_id=c.order_id
    # left join yibai_warehouse.yibai_warehouse d on a.warehouse_id=d.id
    # where (a.warehouse_id=323 or d.warehouse_type=1)
    # """
    # df = sql_to_pd(database='yibai_order', sql=sql, data_sys='ERP')
    sql = f"""
        with order_id_list as (
        select distinct order_id 
        from  yibai_oms_sync.yibai_oms_order
        where payment_time>='{order_date} 00:00:00' and payment_time<='{order_date} 23:59:59' 
        and total_price!=0 and platform_code='AMAZON'
        and (warehouse_id=323 or warehouse_id in (select distinct id as warehouse_id from yibai_logistics_tms_sync.yibai_warehouse where warehouse_type=1))
    )
    
    select 
        a.order_id AS order_id,
        a.account_id AS account_id,
        case 
            when a.warehouse_id=323 then 'FBA'
            when e.warehouse_type=1 then '国内仓' 
            else '海外仓' 
        end as `仓位`,
        d.sku as sku,
        c.seller_sku as seller_sku,
        c.item_id as item_id,
        toFloat64(c.total_price) as  `链接金额`,
        toFloat64(c.sale_price) as  `链接售价`,
        toFloat64(c.ship_price) as `链接运费`,
        c.quantity as `销量`,
        toFloat64(b.currency_rate) as `订单汇率`,
        toFloat64(b.total_price) as  `订单金额`,
        toFloat64(b.shipping_price) as `订单运费金额`,
        toFloat64(b.adjust_amount) as `订单调整金额`,
        toFloat64(b.commission_fees) as `订单佣金`,
        toFloat64(b.profit_new1) as `订单利润1`,
        toFloat64(b.true_profit_new1) as  `订单利润`,
        toFloat64(b.packing_cost)+toFloat64(b.package_cost) as `订单包装包材成本`,
        toFloat64(b.stock_price)+toFloat64(b.exchange_price)  as `订单库存汇损成本`,
        toFloat64(b.purchase_cost_new1) as `订单产品成本`,
        toFloat64(b.shipping_cost) as `订单运费成本1`,
        toFloat64(b.true_shipping_fee) as `订单运费成本`,
        toFloat64(b.first_carrier_cost) as `订单头程`,
        toFloat64(b.duty_cost) as `订单头程关税`,
        toFloat64(b.return_taxation) as `物流税金` 
    from (
        select date(purchase_time) AS created_time,date(payment_time) AS paytime,order_id,account_id,warehouse_id 
        from  yibai_oms_sync.yibai_oms_order
        where order_id in (select order_id from order_id_list)
    ) a
    left join (
        select order_id,
        toFloat64(currency_rate) as currency_rate,
        toFloat64(total_price) as total_price,
        toFloat64(shipping_price) as shipping_price,
        toFloat64(adjust_amount) as adjust_amount,
        toFloat64(commission_fees) as commission_fees,
        toFloat64(profit_new1) as profit_new1,
        toFloat64(true_profit_new1) as true_profit_new1,
        toFloat64(packing_cost) as packing_cost,
        toFloat64(package_cost) as package_cost,
        toFloat64(stock_price) as stock_price,
        toFloat64(exchange_price) as exchange_price,
        toFloat64(purchase_cost_new1) as purchase_cost_new1,
        toFloat64(shipping_cost) as shipping_cost,
        toFloat64(true_shipping_fee) as true_shipping_fee,
        toFloat64(first_carrier_cost) as first_carrier_cost,
        toFloat64(duty_cost) as duty_cost,
        toFloat64(return_taxation) as return_taxation 
        from yibai_oms_sync.yibai_oms_order_profit
        where order_id in (select order_id from order_id_list)
    ) b 
    on a.order_id=b.order_id 
    left join (
        select * from yibai_oms_sync.yibai_oms_order_detail
        where order_id in (select order_id from order_id_list)
    ) c 
    on a.order_id=c.order_id 
    left join (
        select * from yibai_oms_sync.yibai_oms_order_sku
        where order_id in (select order_id from order_id_list)
    ) d 
    on c.order_id=d.order_id and c.id=d.order_detail_id 
    left join yibai_logistics_tms_sync.yibai_warehouse e 
    on a.warehouse_id=e.id 
    """
    # print(sql)
    conn = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn.ck_select_to_df(sql)

    sql = "select id as account_id,site,short_name as `账号简称`,account_num as `账号` from yibai_system_kd_sync.yibai_amazon_account"
    df_account = conn.ck_select_to_df(sql)
    df = df.merge(df_account, on=['account_id'])
    print(df.shape)
    # 剔除一单多品订单
    df.drop_duplicates(['order_id', 'seller_sku', 'account_id', 'item_id', '销量'], inplace=True)
    df1 = df.groupby(['order_id', 'seller_sku', 'account_id'])['site'].count().reset_index()
    df1.columns = ['order_id', 'seller_sku', 'account_id', '次数']
    df = df.merge(df1, on=['order_id', 'seller_sku', 'account_id'], how='left')
    df = df[df['次数'] == 1]
    df.drop(['次数'], axis=1, inplace=True)
    # 剔除订单利润缺失/异常订单
    df = df[df['订单金额'] > 0]
    print(df.shape)
    df.loc[df['订单利润'] == 0, '订单运费成本'] = df.loc[df['订单利润'] == 0, '订单运费成本1']
    df.loc[df['订单利润'] == 0, '订单利润'] = df.loc[df['订单利润'] == 0, '订单利润1']
    df.drop(['订单运费成本1', '订单利润1'], axis=1, inplace=True)
    df = df[df['订单运费成本'] > 0]
    print(df.shape)
    # 匹配税费及促销折扣金额
    # conn = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df1 = pd.DataFrame()

    df_order = df[['order_id']].drop_duplicates()
    df_order = df_order.reset_index(drop=True)
    df_order['index'] = df_order.index
    df_order['index'] = df_order['index'].apply(lambda m: int(m/3000))

    for key, group in tqdm(df_order.groupby(['index'])):
        order_is_list = mysql_escape(group, 'order_id')
        sql = f"""
        select distinct order_id,seller_sku,item_id,
        (shipping_discount_amount+promotion_discount_amount) as  `销售折扣`,
        item_tax_amount+shipping_tax_amount as  `消费税金` 
        from yibai_oms_sync.yibai_oms_order_amazon_item 
        where order_id in ({order_is_list})
        """
        df0 = conn.ck_select_to_df(sql)
        df1 = df1.append(df0)
    df1['销售折扣'] = df1['销售折扣'].astype(float)
    df1['消费税金'] = df1['消费税金'].astype(float)
    df1 = df1.groupby(['order_id'])['销售折扣', '消费税金'].sum().reset_index()

    df = df.merge(df1, on=['order_id'])
    df['销售折扣'] = df['销售折扣'] * df['订单汇率']
    df['销售折扣'] = df['销售折扣'].apply(lambda x: round(x, 2))
    df['消费税金'] = df['消费税金'] * df['订单汇率']
    df['消费税金'] = df['消费税金'].apply(lambda x: round(x, 2))
    df['订单毛利率'] = df['订单利润'] / df['订单金额']
    df['订单毛利率'] = df['订单毛利率'].apply(lambda x: round(x, 4))
    print(df.shape)
    df.loc[df['site'].isin(['in', 'au', 'tr']), '消费税金'] = df.loc[df['site'].isin(['in', 'au', 'tr']), '物流税金']
    df.loc[df['site'].isin(['us', 'ca', 'jp']), '消费税金'] = 0
    df.drop('物流税金', axis=1, inplace=True)
    return df


def get_price_adjust(order_date, df):
    accountid_list = mysql_escape(df, 'account_id', d_type=1)

    df_sellersku = df[['seller_sku']].drop_duplicates()
    df_sellersku = df_sellersku.reset_index(drop=True)
    df_sellersku['index'] = df_sellersku.index
    df_sellersku['index'] = df_sellersku['index'].apply(lambda m: int(m/3000))

    today = datetime.datetime.strptime(order_date, '%Y-%m-%d').date()

    conn1 = pd_to_ck(database='domestic_warehouse_clear_adjustment', data_sys='调价明细历史数据')
    # conn2 = pd_to_ck(database='domestic_warehouse_clear_adjustment', data_sys='调价存档数据')

    table_list1 = conn1.ck_show_tables(db_name='yibai_price_dom')
    table_list2 = conn1.ck_show_tables(db_name='yibai_price_fba')

    # table_list3 = conn2.ck_show_tables(db_name='yibai_price_dom')
    # table_list4 = conn2.ck_show_tables(db_name='yibai_price_fba')

    df1 = pd.DataFrame(columns=['account_id', 'seller_sku', '站点', 'Current_price', 'Shipping', '定价汇率', '定价产品成本',
                                '定价运费成本', 'percentage_off', 'money_off', 'Coupon_handling_fee', 'your_price_point', '定价'])
    # df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    for i in list(range(1, 15)):
        date_ori = (today - datetime.timedelta(i)).isoformat()
        date1 = date_ori.replace('-', '')

        table1 = 'yibai_domestic_warehouse_increase_fbm_' + date1
        # table2 = 'fba_ag_over_180_days_price_adjustment_fbm_' + date1
        table3 = 'yibai_fba_price_adjustment_fbm_' + date1

        for key, group in df_sellersku.groupby(['index']):
            sellersku_list = mysql_escape(group, 'seller_sku')
            sql1 = f"""
            select account_id,seller_sku,site `站点`,Current_price,0 Shipping,rate `定价汇率`,product_cost `定价产品成本`,
            ship_cost `定价运费成本`,percentage_off+promotion_percent as percentage_off,
            money_off+promotion_amount as money_off,Coupon_handling_fee,your_price_point,fbm_Results_price `定价` 
            from yibai_price_dom.{table1} 
            where account_id in ({accountid_list}) and seller_sku in ({sellersku_list})
            """
            # sql2 = f"""
            # select account_id,seller_sku,site `站点`,Current_price,0 Shipping,rate `定价汇率`,cost `定价产品成本`,
            # total_cost `定价运费成本`,percentage_off+promotion_percent as percentage_off,
            # money_off+promotion_amount as money_off,Coupon_handling_fee,your_price_point,FBM_target_price `定价`
            # from fba_ag_over_180_days_price_adjustment.{table2}
            # where account_id in ({accountid_list}) and seller_sku in ({sellersku_list})
            # """

            sql3 = f"""
            select account_id,seller_sku,site `站点`,Current_price,0 Shipping,rate `定价汇率`,cost `定价产品成本`,
            total_cost `定价运费成本`,percentage_off+promotion_percent as percentage_off,
            money_off+promotion_amount as money_off,Coupon_handling_fee,your_price_point,FBM_target_price `定价` 
            from yibai_price_fba.{table3} 
            where account_id in ({accountid_list}) and seller_sku in ({sellersku_list})
            """

            if table1 in table_list1:
                price1 = conn1.ck_select_to_df(sql1)
            # elif table1 in table_list3:
            #     price1 = conn2.ck_select_to_df(sql1)
            else:
                price1 = pd.DataFrame()
            price1['date'] = date_ori
            df1 = df1.append(price1)

            # if table2 in table_list2:
            #     price2 = conn2.ck_select_to_df(sql2)
            #     price2['date'] = date_ori
            #     df2 = df2.append(price2)

            if table3 in table_list2:
                price3 = conn1.ck_select_to_df(sql3)
            # elif table3 in table_list4:
            #     price3 = conn2.ck_select_to_df(sql3)
            else:
                price3 = pd.DataFrame()
            price3['date'] = date_ori
            df3 = df3.append(price3)

    df1 = df1.append(df3)
    df1['fb_type'] = 'FBM'
    df1['date'] = pd.to_datetime(df1['date'], format='%Y-%m-%d')
    df1.sort_values(by=['date'], ascending=False, inplace=True)
    df1.drop_duplicates(['account_id', 'seller_sku'], inplace=True)
    print(df1.shape)
    print(df1.columns)
    df1 = shuilv(df1, fb_type='FBM')
    df1['定价折扣金额'] = (df1['定价'] + df1['Shipping']) * (df1['percentage_off'] + df1['your_price_point']) + df1['money_off']
    df1['定价折扣金额'] = df1['定价折扣金额'] * df1['定价汇率']
    df1['定价收取运费'] = df1['Shipping'] * df1['定价汇率']
    df1['定价税金'] = ((df1['定价'] + df1['Shipping']) * (1 - df1['your_price_point'] - df1['percentage_off']) -
                   (df1['money_off'] + df1['Coupon_handling_fee'])) * df1['定价汇率'] * df1['税率']

    df1['定价毛利率'] = 1 - 0.18 - df1['税率'] - (df1['定价产品成本'] + df1['定价运费成本']) / \
                   ((df1['定价'] + df1['Shipping']) * (1 - df1['your_price_point'] - df1['percentage_off']) -
                    (df1['money_off'] + df1['Coupon_handling_fee'])) / df1['定价汇率']
    df1['定价毛利率'] = df1['定价毛利率'].apply(lambda x: round(x, 4))

    return df1

def idx_max_min(x, type=1):
    x_df = pd.DataFrame()


def compare(df, df1, order_date):
    df1 = df1[['account_id', 'seller_sku', '定价', '定价收取运费', '定价汇率', '定价产品成本', '定价运费成本', '定价税金', '定价折扣金额', '定价毛利率']]
    df = df.merge(df1, on=['account_id', 'seller_sku'], how='left')
    # 仅出单价和定价一致链接纳入统计范围
    df = df[df['链接售价'] == df['定价']]

    df['订单定价毛利率差值'] = df['订单毛利率'] - df['定价毛利率']
    df['订单定价毛利率差值'] = df['订单定价毛利率差值'].apply(lambda x: round(x, 4))

    df['类型'] = '差值在0.05内'
    df.loc[(df['订单定价毛利率差值'] > 0.05), '类型'] = '差值大于0.05'
    df.loc[(df['订单定价毛利率差值'] < -0.05), '类型'] = '差值小于-0.05'

    # 对比差异值   产品成本变动值、运费变动值、收取运费变动值、促销折扣变动值、税费变动值、汇率变动值
    df['产品成本变动值'] = df['订单产品成本'] - df['定价产品成本'] * df['销量']
    df['运费变动值'] = df['订单运费成本'] - df['定价运费成本'] * df['销量']
    df['收取运费变动值'] = df['订单运费金额'] - df['定价收取运费']
    df['促销折扣变动值'] = df['销售折扣'] - df['定价折扣金额'] * df['销量']
    df['税费变动值'] = df['消费税金'] - df['定价税金'] * df['销量']

    df['产品成本变动率'] = df['产品成本变动值'] / df['订单金额']
    df['产品成本变动率'] = df['产品成本变动率'].apply(lambda x: round(x, 4))
    df['运费变动率'] = df['运费变动值'] / df['订单金额']
    df['运费变动率'] = df['运费变动率'].apply(lambda x: round(x, 4))
    df['收取运费变动率'] = (-1) * df['收取运费变动值'] / df['订单金额']
    df['收取运费变动率'] = df['收取运费变动率'].apply(lambda x: round(x, 4))
    df['促销折扣变动率'] = df['促销折扣变动值'] / df['订单金额']
    df['促销折扣变动率'] = df['促销折扣变动率'].apply(lambda x: round(x, 4))
    df['税费变动率'] = df['税费变动值'] / df['订单金额']
    df['税费变动率'] = df['税费变动率'].apply(lambda x: round(x, 4))
    df['礼品包装变动率'] = df['订单调整金额'] / df['订单金额']
    df['礼品包装变动率'] = df['礼品包装变动率'].apply(lambda x: round(x, 4))
    df['佣金变动率'] = df['订单佣金'] / df['订单金额'] - 0.15
    df['佣金变动率'] = df['佣金变动率'].apply(lambda x: round(x, 4))
    print(df.shape)
    # 原因归类
    # df['最大变动率']=df.apply(lambda x:max(x['产品成本变动率'],x['运费变动率'],x['收取运费变动率'],x['促销折扣变动率'],x['税费变动率'],x['礼品包装变动率'],x['佣金变动率']))
    # df['最小变动率'] = df.apply(lambda x: min(x['产品成本变动率'],x['运费变动率'],x['收取运费变动率'],x['促销折扣变动率'],x['税费变动率'],x['礼品包装变动率'],x['佣金变动率']))
    if df.shape[0] > 0:
        df['最大变动率'] = df.apply(
            lambda x: x[['产品成本变动率', '运费变动率', '收取运费变动率', '促销折扣变动率', '税费变动率', '礼品包装变动率', '佣金变动率']].astype(float).max(),
            axis=1)
        df['最小变动率'] = df.apply(
            lambda x: x[['产品成本变动率', '运费变动率', '收取运费变动率', '促销折扣变动率', '税费变动率', '礼品包装变动率', '佣金变动率']].astype(float).min(),
            axis=1)
    else:
        df['最大变动率'] = 0
        df['最小变动率'] = 0

    # df['最大列'] = df.apply(
    #     lambda x: x[['产品成本变动率', '运费变动率', '收取运费变动率', '促销折扣变动率', '税费变动率', '礼品包装变动率', '佣金变动率']].astype(float).idxmax(1),
    #     axis=1)
    df['最大列'] = df[['产品成本变动率', '运费变动率', '收取运费变动率', '促销折扣变动率', '税费变动率', '礼品包装变动率', '佣金变动率']].astype(float).idxmax(1)
    # df['最小列'] = df.apply(
    #     lambda x: x[['产品成本变动率', '运费变动率', '收取运费变动率', '促销折扣变动率', '税费变动率', '礼品包装变动率', '佣金变动率']].astype(float).idxmin(1),
    #     axis=1)
    df['最小列'] = df[['产品成本变动率', '运费变动率', '收取运费变动率', '促销折扣变动率', '税费变动率', '礼品包装变动率', '佣金变动率']].astype(float).idxmin(1)
    df['最大列'] = df['最大列'].apply(lambda x: x[:-1])
    df['最小列'] = df['最小列'].apply(lambda x: x[:-1])

    df_ana0 = df.groupby(['类型'])['order_id'].count().reset_index()
    df_ana0['订单量占比'] = df_ana0['order_id'] / df.shape[0]
    df_ana1 = df[df['类型'] == '差值大于0.05'].groupby(['最小列'])['order_id'].count().reset_index()
    df_ana1['订单量占比'] = df_ana1['order_id'] / df[df['类型'] == '差值大于0.05'].shape[0]
    df_ana2 = df[df['类型'] == '差值小于-0.05'].groupby(['最大列'])['order_id'].count().reset_index()
    df_ana2['订单量占比'] = df_ana2['order_id'] / df[df['类型'] == '差值小于-0.05'].shape[0]

    df_ana0.columns = ['type', 'order_num', 'order_rate']
    df_ana0['order_rate'] = df_ana0['order_rate'].apply(lambda x: round(x, 4))
    df_ana1.columns = ['reason', 'order_num', 'order_rate']
    df_ana1['order_rate'] = df_ana1['order_rate'].apply(lambda x: round(x, 4))
    df_ana1['type'] = '差值大于0.05'
    df_ana2.columns = ['reason', 'order_num', 'order_rate']
    df_ana2['order_rate'] = df_ana2['order_rate'].apply(lambda x: round(x, 4))
    df_ana2['type'] = '差值小于-0.05'
    df_ana0['date'] = order_date
    df_ana1['date'] = order_date
    df_ana2['date'] = order_date

    return df, df_ana0, df_ana1, df_ana2


def main():
    # 计算时间（前一天）
    order_date = (datetime.date.today() - datetime.timedelta(1)).isoformat()

    #
    df = get_order(order_date)
    print(df.shape)
    df1 = get_price_adjust(order_date, df)
    df2, df_ana0, df_ana1, df_ana2 = compare(df, df1, order_date)

    if os.path.exists('/data/setting'):
        file_temporary = '/data/temporary'
    else:
        file_temporary = os.getcwd()
    if os.path.exists(file_temporary):
        shutil.rmtree(file_temporary)
    if not os.path.exists(file_temporary):
        os.makedirs(file_temporary)

    f_path = os.path.join(file_temporary, 'domestic_accuracy')
    if not os.path.exists(f_path):
        os.makedirs(f_path)
    file_name = os.path.join(f_path, f'调价准确率{order_date}.xlsx')
    df2.to_excel(file_name, index=False)
    # df_ana0.to_excel(f'1.xlsx', index=False)
    # df_ana1.to_excel(f'2.xlsx', index=False)
    # df_ana2.to_excel(f'3.xlsx', index=False)
    # exit()

    obs_putFile(item='domestic_accuracy', file_name=file_name, open_dir=None, isall=False)
    # shutil.rmtree(f_path)
    os.remove(file_name)

    df_ana1 = df_ana1.append(df_ana2)

    # conn = connect_to_sql(database='temp_database_wh', data_sys='数据部服务器')
    # sql = f"delete from domestic_accuracy where `date`='{order_date}'"
    # conn.execute(sql)
    # conn.to_sql(df_ana0, table='domestic_accuracy', if_exists='append')
    #
    # sql = f"delete from domestic_accuracy_detail where `date`='{order_date}'"
    # conn.execute(sql)
    # conn.to_sql(df_ana1, table='domestic_accuracy_detail', if_exists='append')
    # conn.close()
    conn_mx = pd_to_ck(database='temp_database_wh', data_sys='调价明细历史数据')
    sql = f"""
    alter table temp_database_wh.domestic_accuracy delete 
    where `date` = '{order_date}'
    """
    conn_mx.ck_execute_sql(sql)
    while 1:
        sql = f"""
        select count() as num from temp_database_wh.domestic_accuracy
        where `date` = '{order_date}'
        """
        df = conn_mx.ck_select_to_df(sql)
        if df['num'][0] == 0:
            break
        else:
            time.sleep(10)
    conn_mx.write_to_ck_json_type(df_ana0, 'domestic_accuracy')
    #
    sql = f"""
        alter table temp_database_wh.domestic_accuracy_detail delete 
        where `date` = '{order_date}'
        """
    conn_mx.ck_execute_sql(sql)
    while 1:
        sql = f"""
            select count() as num from temp_database_wh.domestic_accuracy_detail
            where `date` = '{order_date}'
            """
        df = conn_mx.ck_select_to_df(sql)
        if df['num'][0] == 0:
            break
        else:
            time.sleep(10)
    conn_mx.write_to_ck_json_type(df_ana1, 'domestic_accuracy_detail')


def fba_freeze_seller_sku():
    # 20220909 欧洲连续5次调价都失败的链接
    conn_ck = pd_to_ck(database='yibai_price_fba', data_sys='调价明细历史数据')
    sql = """
    select distinct adjust_date from yibai_price_fba.yibai_fba_price_adjustment_fba_merge
    union all 
    select distinct adjust_date from yibai_fba.amazon_price_adjust_log
    where adjust_date < toString(today()-60)
    """
    df_dt = conn_ck.ck_select_to_df(sql)
    for d_t in list(df_dt['adjust_date']):
        sql = f"""
        alter table yibai_fba.amazon_price_adjust_log drop partition '{d_t}' 
        """
        conn_ck.ck_execute_sql(sql)
    sql = """
    select distinct adjust_date from yibai_price_fba.yibai_fba_price_adjustment_fba_merge
    """
    df_dt = conn_ck.ck_select_to_df(sql)
    dt_list = mysql_escape(df_dt, 'adjust_date')


    sql = """
    select distinct account_id from yibai_price_fba.yibai_fba_price_adjustment_fba_merge
    where site in (
        select distinct site1 from domestic_warehouse_clear.yibai_site_table_amazon 
        where area = '泛欧'
    )
    """
    df_id = conn_ck.ck_select_to_df(sql)
    df_id['index'] = df_id.index
    df_id['index'] = df_id['index'].apply(lambda m: int(m / 10))
    for key, group in tqdm(df_id.groupby(['index'])):
        id_list = mysql_escape(group, 'account_id', 1)
        sql = f"""
        insert into yibai_fba.amazon_price_adjust_log
        select a.account_id,a.seller_sku,adjust_date,
            if(abs(a.your_price-toFloat64(b.your_price)) <= 0.001, '否', '是') as `price_changed`
        from (
            select distinct account_id,seller_sku,your_price,adjust_date
            from yibai_price_fba.yibai_fba_price_adjustment_fba_merge 
            where account_id in ({id_list}) and your_price != Results_price 
        ) a 
        left join (
            select distinct account_id,seller_sku,your_price,
                toString(date_add(day, -1, toDate(create_date))) as adjust_date 
            from (
                select account_id,seller_sku,your_price,arrayJoin(new_date) as create_date
                from (
                select distinct account_id,seller_sku,your_price,create_date as start_date,
                    if(last_date='' or last_date is null, toString(today()+1), last_date) as last_date,
                    arrayFilter(x->x>=start_date and x<last_date, arrayMap(x->toString(today()-x), range(31))) as new_date
                from (
                    select distinct account_id,seller_sku,your_price,create_date,
                            any(create_date) over(partition by account_id,seller_sku order by create_date desc ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as last_date 
                    from yibai_domestic.yibai_amazon_listing_profit_bak 
                    where account_id in ({id_list})
                ))
                where create_date in ({dt_list})
            )  
        ) b 
        on a.account_id=b.account_id and a.seller_sku=b.seller_sku and a.adjust_date=b.adjust_date
        """
        conn_ck.ck_execute_sql(sql)

    #
    sql = """
    truncate table yibai_fba.amazon_price_adjust_log_fba_freeze
    """
    conn_ck.ck_execute_sql(sql)
    sql = """
    select distinct account_id from yibai_fba.amazon_price_adjust_log
    """
    df = conn_ck.ck_select_to_df(sql)
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m/50))
    for key, group in df.groupby(['index']):
        id_list = mysql_escape(group, 'account_id', 1)
        sql = f"""
        insert into yibai_fba.amazon_price_adjust_log_fba_freeze
        with adjust_table as (
            select account_id,seller_sku,adjust_date,price_changed 
            from yibai_fba.amazon_price_adjust_log 
            where account_id in ({id_list})
            order by adjust_date desc limit 5 by account_id,seller_sku
        )
        
        select distinct account_id,seller_sku from adjust_table 
        where (account_id,seller_sku) not in (
            select distinct account_id,seller_sku from adjust_table
            where price_changed='是'
        )
        """
        conn_ck.ck_execute_sql(sql)

if __name__ == '__main__':
    main()
