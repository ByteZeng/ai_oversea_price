# author: marmot
import os
from clickhouse_driver import Client
import pandas as pd
import datetime
# from public_function import *


class NewPandaHouse(object):
    def __init__(self, user="default", password="", host="localhost", port=9000, db_name="default", db_sheet=""):
        self.host, self.port = host, port
        self.db_name, self.db_sheet = db_name, db_sheet
        self.client = Client(host=self.host, port=self.port, user=user, password=password, compression=True)

    @staticmethod
    def generate_ck_ddl(insert_df, columns_dict=None):
        """
        :param insert_df: 写入数据
        :param columns_dict: 字段解释映射
        :return:
        """
        ck_ddl = []
        # 常用的三种类型, string, float, int ,time
        for col, d_type in dict(insert_df.dtypes).items():
            if "object" in str(d_type):
                col_type = "String"
                insert_df[col] = insert_df[col].astype(str)
            elif "float" in str(d_type):
                col_type = "Float64"
                insert_df[col] = insert_df[col].astype(float)
            elif "int" in str(d_type):
                col_type = "Int64"
                insert_df[col] = insert_df[col].astype(int)
            else:
                raise Exception(f"{d_type}无法识别")
            # 组装字段类型
            if columns_dict:
                comment = columns_dict.get(col, "")
            else:
                comment = ""
            one_filed = f"`{col}` {col_type}"
            one_filed = one_filed + f" COMMENT '{comment}'" if comment else one_filed
            # 汇总字段
            ck_ddl.append(one_filed)
        return ','.join(ck_ddl), insert_df

    def ck_show_tables(self):
        sql = f'show tables from {self.db_name}'
        answer = self.client.execute(sql)
        return answer

    def ck_show_database(self):
        sql = f'SHOW DATABASES'
        answer = self.client.execute(sql)
        return answer

    def create_database(self, database):
        sql = f"CREATE DATABASE IF NOT EXISTS {database} ENGINE = Ordinary"
        answer = self.client.execute(sql)
        return answer

    def ck_create_table(self, ddl_sql):
        try:
            self.client.execute(ddl_sql)
            return True
        except Exception as create_err:
            raise create_err

    def ck_drop_table(self, sheet_name):
        sql = f'DROP TABLE IF EXISTS {self.db_name}.{sheet_name}'
        try:
            self.client.execute(sql)
            return True
        except Exception as E:
            raise E

    def ck_clear_table(self, sheet_name):
        sql = f'TRUNCATE TABLE IF EXISTS {self.db_name}.{sheet_name}'
        try:
            self.client.execute(sql)
            return True
        except Exception as E:
            raise E

    def ck_get_table_ddl(self, database_name=None, sheet_name=None):
        """
        :param database_name :
        :param sheet_name :
        """
        # 用 df 的列生成 ddl
        database_name = self.db_name if database_name is None else database_name
        sheet_name = self.db_sheet if sheet_name is None else sheet_name
        sql = f'SHOW CREATE table {database_name}.{sheet_name}'
        answer = self.client.execute(sql)
        ck_ddl = answer[0][0]
        return ck_ddl

    def ck_get_table_data_numbers(self, database_name=None, sheet_name=None):
        database_name = self.db_name if database_name is None else database_name
        sheet_name = self.db_sheet if sheet_name is None else sheet_name
        sql = f'SELECT COUNT(id) FROM {database_name}.{sheet_name}'
        answer = self.client.execute(sql)[0][0]
        return answer

    def ck_insert(self, init_insert_df, sheet_name, if_exist="replace", columns_dict=None):
        """
        :param init_insert_df : DataFrame
        :param sheet_name : sheet_name
        :param if_exist ：append or replace, default replace,不等于replace，就是append
        :param columns_dict comment注释对照
        """
        insert_df = init_insert_df.copy()
        if "update_time" not in list(insert_df.columns):
            insert_df['update_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_ddl, insert_df = self.generate_ck_ddl(insert_df, columns_dict)
        if if_exist == "replace":
            # 执行一个删表
            self.ck_drop_table(sheet_name)
        # 执行一个建表
        table_ddl = f'''
        CREATE TABLE IF NOT EXISTS {self.db_name}.{sheet_name} (`id` UInt64, {new_ddl}) 
        ENGINE = MergeTree() ORDER BY (id);
        '''
        self.ck_create_table(table_ddl)
        # 增加id
        if "id" in insert_df.columns:
            del insert_df['id']
        exist_max_id = 0 if if_exist == "replace" else self.ck_get_table_data_numbers(sheet_name=sheet_name)
        insert_df['id'] = [one_num + 1 + exist_max_id for one_num in range(len(insert_df.index))]
        # 数据总是以尽量大的batch进行写入，如每次写入100,000行。
        cols = ','.join(map(lambda m_x: f"`{m_x}`", list(insert_df.columns)))
        data_dict_list = insert_df.to_dict('records')
        data_list = [list(one_item.values()) for one_item in data_dict_list]
        # data_list = data_dict_list
        group_limit = 1000000
        insert_num = int(len(insert_df.index) / group_limit) + 1
        for one_num in range(insert_num):
            now_data_list = data_list[(one_num * group_limit): (one_num * group_limit + group_limit)]
            if now_data_list:
                self.client.execute(f"INSERT INTO {self.db_name}.{sheet_name} ({cols}) VALUES", now_data_list)

    def ck_select(self, ck_sql=None, columns=None):
        """
        :param ck_sql : clickhouse的sql语句
        :param columns : list 返回的df的列名,为空则没有列名
        """
        if ck_sql:
            # 结果是不带列名的df
            # 先查看数据表是否存在
            answer_table_list = self.client.execute(f"show TABLES from {self.db_name};")
            table_list = [one_list[0] for one_list in answer_table_list]
            # 再进行查询
            table_name = ck_sql.split("from")[1].strip()
            table_name = table_name.split(" ")[0]
            if "." in table_name:
                table_name = table_name.split(".")[1]
            if table_name in table_list:
                answer = self.client.execute(ck_sql)
                answer_df = pd.DataFrame(answer)
                if columns:
                    answer_df = answer_df[columns]
                return answer_df

    def ck_select_to_df(self, ck_sql=None, columns=None):
        """
        :param ck_sql : clickhouse的sql语句
        :param columns : list 返回的df的列名,为空则没有列名
        """
        answer = self.client.execute(ck_sql, with_column_types=True)
        data_list = answer[0]
        answer_df = pd.DataFrame(data_list)
        columns_result_list = [one_item[0] for one_item in answer[1]]
        if columns_result_list and data_list:
            answer_df.columns = columns_result_list
            if columns:
                answer_df = answer_df[columns]
        return answer_df


if os.path.exists("/data"):
    send_ck_cls = NewPandaHouse(
        user="zhenghw", password="zhenghw#mrp03", host='127.0.0.1', port=9003, db_name="over_sea"
    )
else:
    send_ck_cls = NewPandaHouse(
        user="zhenghw", password="zhenghw#mrp03", host='121.37.248.212', port=9003, db_name="over_sea"
    )

send_ck_yibai_product = NewPandaHouse(
    user="zhenghw", password="zhenghw#mrp03", host='121.37.248.212', port=9003, db_name="yibai_product"
)

send_ck_yibai_order = NewPandaHouse(
    user="zhenghw", password="zhenghw#mrp03", host='121.37.248.212', port=9003, db_name="yibai_order"
)


send_ck_yibai_data_temp = NewPandaHouse(
    user="tanle", password="tanle202110071441", host='139.9.67.175', port=9003, db_name="yibai_data_temp"
)


# if __name__ == '__main__':
# send_fba_ck_cls.ck_select("select * from yibai_fba_send.yibai_cloud_move_inv_process_records")
# print(send_fba_read_public_ck_cls.read_sql_from_ck(
#     "select * from yibai_fba_send.yibai_cloud_move_inv_process_records")

# )

# test_ck_cls.create_database('cj_adjust_price')
