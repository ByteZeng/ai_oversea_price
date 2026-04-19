# author: marmot
from clickhouse_driver import Client
import pandas as pd
import json
from retry import retry

class CkClient(object):
    def __init__(self, user="", password="", host="", port=9000, db_name=""):
        self.host, self.port = host, port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.client = Client(host=self.host, port=self.port, user=user, password=password, database=self.db_name)



    def ck_execute_sql(self, sql):
        try:
            answer = self.client.execute(sql, with_column_types=True)
            return answer
        except Exception as execute_err:
            self.client = Client(host=self.host, port=self.port, user=self.user, password=self.password, database=self.db_name)
            raise execute_err
    
    def write_to_ck_json_type(self, sheet_name, records_list):
        ''' ck入库：json方式
            record_list pandas数据集
            eg:[{'sku': 'ZM01874', 'warehouse': 'US', 'date': '2021/10/13', 'fee': 0.0}, {...}]
        '''
        sql = f'''insert into {sheet_name} FORMAT JSONEachRow {json.dumps(records_list.to_dict(orient='records'))}'''
        return self.ck_execute_sql(sql)

    def write_to_ck_csv_type(self, sheet_name, records_list):
        """
        ck入库: csv方式
        records_list 中包含类型list字段(仍作为str类型），入库可直接存入Array(...)
        类型的字段中
        eg:[{'sku': 'ZM01874', 'warehouse': 'US', 'date': '2021/10/13', 'fee': 0.0, 'inv_age_list': '[(68, 1),(69,2)]'}
        """
        pd_csv = records_list.to_dict(orient='split')['data']
        data_fmt = '\r\n'.join( [str(line)[1:-1] for line in pd_csv] )
        sql = f'insert into {sheet_name} FORMAT CSV {data_fmt}'
        return self.ck_execute_sql(sql)

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
        sql = f'SHOW CREATE table {database_name}.{sheet_name}'
        try:
            answer = self.client.execute(sql)
            ck_ddl = answer[0][0]
        except Exception as E:
            return ''   
        return ck_ddl

    def ck_get_table_data_numbers(self, sheet_name=None, database_name=None):
        database_name = self.db_name if database_name is None else database_name
        sql = f'SELECT COUNT() FROM {database_name}.{sheet_name}'
        answer = self.client.execute(sql)[0][0]
        return answer

    @retry(tries=3, delay=2, backoff=2)
    def ck_select_to_df(self, ck_sql=None, columns=None):
        """
        :param ck_sql : clickhouse的sql语句
        :param columns : list 返回的df的列名,为空则没有列名
        """
        answer = self.ck_execute_sql(ck_sql)
        data_list = answer[0]
        answer_df = pd.DataFrame(data_list)
        columns_result_list = [one_item[0] for one_item in answer[1]]
        if columns_result_list and data_list:
            answer_df.columns = columns_result_list
            if columns:
                answer_df = answer_df[columns]
        return answer_df


if __name__ == '__main__':
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='139.159.225.47', port='8123',
                         db_name='yibai_database_zhangyilan')
    df1 = ck_client.ck_select_sql(sql='select * from yibai_wish_pivot_result limit 1')
    print(df1)