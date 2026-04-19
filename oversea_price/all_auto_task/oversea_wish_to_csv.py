import datetime
import os
import time

from scripts_ck_client import CkClient
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
import traceback
import multiprocessing

result_dir = r"D:\wish调价"

def make_thread(num):
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='139.159.225.47', port='9001',
                         db_name='yibai_database_zhangyilan')
    pd_account = ck_client.ck_select_to_df("select account_id from yibai_wish_pivot_result group by account_id order by account_id ")
    account_list = pd_account['account_id'].tolist()
    print(account_list)
    # arg_list = [[], [], [], [], [], [], [], [], [], []]
    # for x in account_tuple:
    #     arg_list[x % 10].append(x)
    # 开启多线程获取数据
    executor = ThreadPoolExecutor(max_workers=num)
    all_task = [executor.submit(run, acc_id) for acc_id in account_list ]
    fail_futures = []
    for future in as_completed(all_task):
        _id, account_name = future.result()
        try:
            assert account_name is not None
        except:
            fail_futures.append(_id)
            print(traceback.format_exc())
            print(f"{_id}")

    wait(all_task, return_when=ALL_COMPLETED)

    if fail_futures:
        print(f"以下帐号未正确执行: {sorted(fail_futures)}")
        for acc_id in sorted(fail_futures):
            run_second(acc_id)
    # pool = multiprocessing.Pool(processes=7)
    # for acc_list in arg_list:
    #     pool.apply_async(make_thread, (acc_list,))
    # pool.close()
    # pool.join()


def run(acc_id):
    acc_name = None
    t1 = time.time()
    try:
        ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='139.159.225.47', port='9001',
                             db_name='yibai_database_zhangyilan')
        ck_sql = f"""select account_id, account_name, `Parent Unique ID` as "Parent SKU",\
                    `Local Currency Code`, `Warehouse Name`, column_arr, val_arr from yibai_wish_pivot_result \
                     where account_id = {acc_id} \
                    order by `Parent Unique ID`,`Warehouse Name` """
        df = ck_client.ck_select_to_df(ck_sql)
        part_col = df.loc[0]['column_arr']
        acc_name = df.loc[0, 'account_name']
        df['val_arr'] = df['val_arr'].astype('str')
        df['val_arr'] = df['val_arr'].str.strip('[]')
        df[part_col] = df['val_arr'].str.split(',', expand=True)
        df.drop(columns=['account_id', 'account_name', 'column_arr', 'val_arr'], inplace=True)
        df = df.applymap(lambda x: "" if x.strip() is None or x.strip() == "None" else x)
        df.to_csv(f'{result_dir}\{acc_name}运费调整.csv', index=False, encoding="utf_8_sig")
        print(f"{acc_id}, {acc_name}")
        del df
    except:
        print(f"acc_id:{acc_id}失败, sql={ck_sql}")
        print(traceback.format_exc())
    t2 = time.time()
    print(f"{acc_name}csv生成时长：", (datetime.datetime.fromtimestamp(t2) - datetime.datetime.fromtimestamp(t1)).seconds, "秒")
    return acc_id, acc_name


def run_second(acc_id):
    acc_name = None
    t1 = time.time()
    try:
        ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='139.159.225.47', port='9001',
                             db_name='yibai_database_zhangyilan')
        for i in range(1000):
            ck_sql = f"""select account_id, account_name, `Parent Unique ID` as "Parent SKU",\
                        `Local Currency Code`, `Warehouse Name`, column_arr, val_arr from yibai_wish_pivot_result \
                         where account_id = {acc_id} \
                        order by `Parent Unique ID`,`Warehouse Name`, val_arr limit 10000*{i}+1, 10000 """
            df = ck_client.ck_select_to_df(ck_sql)
            if len(df) < 1 :
                break
            part_col = df.loc[0]['column_arr']
            acc_name = df.loc[0, 'account_name']
            df['val_arr'] = df['val_arr'].astype('str')
            df['val_arr'] = df['val_arr'].str.strip('[]')
            df[part_col] = df['val_arr'].str.split(',', expand=True)
            df.drop(columns=['account_id', 'account_name', 'column_arr', 'val_arr'], inplace=True)
            df = df.applymap(lambda x: "" if x.strip() is None or x.strip() == "None" else x)
            df.to_csv(f'{result_dir}\{acc_name}运费调整.csv', index=False, encoding="utf_8_sig", mode='a', header=True if i==0 else False)

            if len(df) < 10000:
                del df
                break
            del df
    except:
        print(f"acc_id:{acc_id}失败,sql={ck_sql}")
        print(traceback.format_exc())
    t2 = time.time()
    print(f"{acc_name}csv生成时长：", (datetime.datetime.fromtimestamp(t2) - datetime.datetime.fromtimestamp(t1)).seconds, "秒")
    return acc_id, acc_name


def del_file(filepath):
    """
    删除某一目录下的所有文件或文件夹
    :param filepath: 路径
    :return:
    """
    del_list = os.listdir(filepath)
    for f in del_list:
        file_path = os.path.join(filepath, f)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


def gen_wish_csv():
    del_file(result_dir)
    make_thread(5)

if __name__ == "__main__":
    gen_wish_csv()
    # run_second(18)
