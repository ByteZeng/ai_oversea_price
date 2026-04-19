# author:marmot
# descriptions: nacos openapi
import requests

base_url = "http://172.16.50.217:8848"


# 获取鉴权
def get_cfg(tenant, data_id, group):
    auth_usr = base_url + "/nacos/v1/auth/login"
    res = requests.post(auth_usr, data={"username": "admin_user_123", "password": "admin_user_123"})
    if res.status_code == 200:
        res_json = res.json()
        # 获取配置
        access_token = res_json.get("accessToken", "")
        config_url = base_url + f"/nacos/v1/cs/configs?accessToken={access_token}&tenant={tenant}&" \
                                f"dataId={data_id}&group={group}"
        res = requests.get(config_url)
        if res.status_code == 200:
            return res.json()
        else:
            print(res.content.decode())
            return {}
    else:
        res_json = res.content.decode()
        print(res_json)
        return {}


# 初始化相关配置

def get_user():
    databases_cfg = get_cfg(
        "6a8e2216-9036-47be-bbc1-25b945bc3986",
        "com.yibai.data.databases",
        "database"
    )
    from_ip_user_info_dict = databases_cfg.get('172.16.50.221', {}).get("mysql", {})
    from_user = from_ip_user_info_dict["user"]
    from_password = from_ip_user_info_dict["password"]
    return from_user, from_password


def get_user_kd():
    databases_cfg = get_cfg(
        "6a8e2216-9036-47be-bbc1-25b945bc3986",
        "com.yibai.data.databases",
        "database"
    )
    from_ip_user_info_dict = databases_cfg.get('172.16.50.163', {}).get("mysql", {})
    from_user = from_ip_user_info_dict["user"]
    from_password = from_ip_user_info_dict["password"]
    return from_user, from_password


if __name__ == '__main__':
    pass
