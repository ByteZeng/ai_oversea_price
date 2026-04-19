"""
海外仓精铺库龄数据更新：
1、将本地excel文件，整理汇总并存储到mysql
"""

import pandas as pd
import os
import numpy as np
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import dwd_warehouse_id_info
warnings.filterwarnings("ignore")


def main():
    # 目标文件夹路径（替换为实际路径）
    folder_path = r"F:\Desktop\日常任务\精铺相关\精铺库龄数据\12月\库龄"

    all_data = pd.DataFrame()  # 存储所有文件的有效数据

    # 定义“文件名关键字 → 服务商”的映射规则
    filename_to_supplier = {
        "隽森": "隽森",
        "谷仓": "谷仓",
        "苏莱美": "苏莱美",
        "西邮": "西邮",
        "亿迈": "亿迈",
        "出口易": "出口易",
        "派速捷": "派速捷",
        "万邑通": "万邑通"
    }

    # 遍历文件夹下的所有文件
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if not os.path.isfile(file_path):
            continue

        # 从文件名提取服务商
        supplier = None
        for keyword, sup in filename_to_supplier.items():
            if keyword in filename:
                supplier = sup
                break
        if supplier is None:
            print(f"文件{filename}未匹配到服务商，已跳过")
            continue  # 若必须匹配服务商，可跳过该文件

        # 根据文件后缀读取数据
        try:
            if filename.endswith((".xlsx", ".xls")):
                df = pd.read_excel(file_path)
            elif filename.endswith(".csv"):
                df = pd.read_csv(file_path, encoding="gbk")  # 编码可根据实际调整
            else:
                continue
            # print(df.info())
        except Exception as e:
            print(f"读取文件{filename}失败：{e}")
            continue


        # 提取关键列
        print(f'开始处理{supplier}数据...')
        df_temp = process_age_data(df, supplier)

        all_data = pd.concat([df_temp, all_data])

    # 补充字段
    all_data['warehouse_stock'] = all_data[["0-30", "30-60", "60-90", "90-120", "120-150", "150-180", "180+"]].sum(axis=1)
    sql = """
        SELECT warehouse_new, warehouse_name, warehouse FROM over_sea.jp_warehouse_name_temp
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_warehouse = conn.read_sql(sql)
    df = pd.merge(all_data, df_warehouse, how='left', on=['warehouse_new'])
    print(df.info())

    # 存储
    df.to_excel('F://Desktop//all_data.xlsx', index=0)

    col = ['0-30', '30-60', '60-90', '90-120', '120-150', '150-180', '180+']
    df[col] = df[col].fillna(0)
    df['age_30_plus'] = df['0-30'] + df['30-60'] + df['60-90'] +df['90-120'] +\
                        df['120-150'] + df['150-180'] + df['180+']
    df['age_60_plus'] = df['30-60'] + df['60-90'] +df['90-120'] +\
                        df['120-150'] + df['150-180'] + df['180+']
    df['age_90_plus'] = df['90-120'] + df['120-150'] + df['150-180'] + df['180+']
    df['age_120_plus'] = df['120-150'] + df['150-180'] + df['180+']
    df['age_150_plus'] = df['150-180'] + df['180+']
    df['age_180_plus'] = df['180+']
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'fine_sku_age', if_exists='replace')

def pivot_df(df):
    """ 数据透视 """
    pivot_df = pd.pivot_table(
        df,
        index=["sku", "warehouse_new", "服务商"],
        columns="库龄分段",
        values="在库库存",  # 新增：指定要聚合的列
        aggfunc="sum",  # 新增：求和（替代计数）
        fill_value=0
    )
    # 调整列顺序
    target_columns = ["0-30", "30-60", "60-90", "90-120", "120-150", "150-180", "180+"]
    pivot_df = pivot_df.reindex(columns=target_columns)

    # 重置索引（将多级索引展开为列，可选：若需要sku和warehouse_new为普通列）
    pivot_df = pivot_df.reset_index()

    return pivot_df


# 不同服务商库龄数据处理
def process_age_data(df, supplier='谷仓'):
    """ 不同服务商库龄字段格式不一样 """
    # required_cols = ["sku", "warehouse_new", "在库库存", "库龄", "服务商", "库龄分段"]
    if supplier == '万邑通':
        df = df[['系统SKU','在库数量','库龄']]
        df.columns = ["sku", "在库库存", "库龄分段"]
        df['warehouse_new'] = '万邑通'
        df["服务商"] = supplier
        df = pivot_df(df)
    elif supplier == '谷仓':
        df = df[['系统SKU','仓库（区域）','在库库存','库龄（天）']]
        df.columns = ["sku", "warehouse_new", "在库库存", "库龄"]
        df["服务商"] = supplier
        df['库龄分段'] = df["库龄"].apply(get_age_segment)
        df = pivot_df(df)
    elif supplier == '苏莱美':
        df = df[['系统SKU','Warehouse/仓库','Qty/数量','Stock age/库龄（天）']]
        df.columns = ["sku", "warehouse_new", "在库库存", "库龄"]
        df["服务商"] = supplier
        df['库龄分段'] = df["库龄"].apply(get_age_segment)
        df = pivot_df(df)
    elif supplier == '西邮':
        df = df[['系统SKU', '仓库/Warehouse', '库存数量/QTY', '库龄/Library Age']]
        df.columns = ["sku", "warehouse_new", "在库库存", "库龄"]
        df["服务商"] = supplier
        df['库龄分段'] = df["库龄"].apply(get_age_segment)
        df = pivot_df(df)
    # 亿迈仓的单独处理
    elif supplier == '亿迈':
        print(df.columns)
        # df = df[['海外仓SKU', '仓库', '在仓数量', '0-30', '30-60','60-90','90-120','120-150','150-180','180-210','210-240',
        #          '240-270','270-300','300-330','330-9999']]
        df['180+'] = df['180-210'] + df['210-240'] + df['240-270'] + df['270-300'] + df['300-330'] + df['330-9999']
        col = ['系统SKU', '仓库', '0-30', '30-60','60-90','90-120','120-150','150-180','180+']
        df = df[col]
        df = df.groupby(['系统SKU','仓库']).sum().reset_index()
        df['服务商'] = supplier
        df = df.rename(columns={'系统SKU':'sku', '仓库':'warehouse_new'})
    elif supplier == '出口易':
        # df = df[['海外仓SKU', '仓库', '在仓数量', '0-30', '30-60','60-90','90-120','120-150','150-180','180-210','210-240',
        #          '240-270','270-300','300-330','330-9999']]
        df['180+'] = df['180-210'] + df['210-240'] + df['240-270'] + df['270-300'] + df['300-330'] + df['330-9999']
        col = ['系统SKU', '仓库', '0-30', '30-60', '60-90', '90-120', '120-150', '150-180', '180+']
        df = df[col]
        df = df.groupby(['系统SKU', '仓库']).sum().reset_index()
        df['服务商'] = supplier
        df = df.rename(columns={'系统SKU': 'sku', '仓库': 'warehouse_new'})
    elif supplier == '派速捷':
        df = df[['系统SKU', '仓库', '可售', '库龄']]
        df.columns = ["sku", "warehouse_new", "在库库存", "库龄"]
        # df['库龄'] = df['库龄'].str.extract(r'(\d+)').astype(int)
        df["服务商"] = supplier
        df['库龄分段'] = df["库龄"].apply(get_age_segment)
        df = pivot_df(df)
    elif supplier == '隽森':
        df = df[['系统SKU', '仓库编码', '数量', '库龄（天）']]
        df.columns = ["sku", "warehouse_new", "在库库存", "库龄"]
        # df['库龄'] = df['库龄'].str.extract(r'(\d+)').astype(int)
        df["服务商"] = supplier
        df['库龄分段'] = df["库龄"].apply(get_age_segment)
        df = pivot_df(df)

    # 非上面的服务商数据，返回空值
    else:
        df = pd.DataFrame()

    return df



# 生成“库龄分段”列（示例规则：≥60→60-90；30≤x<60→30-60；<30→0-30）
def get_age_segment(age):
    if pd.isna(age):  # 处理空值
        return None
    if age >= 180:
        return "180+"
    elif 150 <= age < 180:
        return "150-180"
    elif 120 <= age < 150:
        return "120-150"
    elif 90 <= age < 120:
        return "90-120"
    elif 60 <= age < 90:
        return "60-90"
    elif 30 <= age < 60:
        return "30-60"
    else:  # age < 30
        return "0-30"


def check_stock_age():
    """ 核查当前转泛品sku是否都有库龄数据 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # 1. 取库存
    df_stock = dwd_warehouse_id_info()
    df_stock['type'] = df_stock['type'].fillna(' ')
    df_stock = df_stock[df_stock['type'].str.contains('转泛品|易通兔')]
    df_stock = df_stock[df_stock['available_stock'] > 0]
    # 2. 取库龄
    # 取库龄
    sql = """
        SELECT 
            sku, warehouse_name, '' charge_currency, '' cargo_type, warehouse_stock, 
            90 inventory_age, 0 charge_total_price,
            age_30_plus, age_60_plus,age_90_plus,age_120_plus,
            age_150_plus,age_180_plus, 0 age_270_plus, 0 age_360_plus
        FROM over_sea.fine_sku_age
    """
    df_age = conn.read_sql(sql)
    sql = """
        SELECT a.warehouse_code warehouse_code, a.id as warehouse_id, a.warehouse_name warehouse_name,
        b.name warehouse
        FROM yibai_logistics_tms_sync.yibai_warehouse a
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b ON a.ebay_category_id = b.id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    df_age = pd.merge(df_age, df_warehouse, how='left', on=['warehouse_name'])

    # 3. 合并
    df = pd.merge(df_stock, df_age, how='left', on=['sku', 'warehouse_name'])

    df.to_excel('F://Desktop//df_fine_age.xlsx', index=0)



if __name__ == '__main__':
    # main()

    check_stock_age()