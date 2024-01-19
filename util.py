import json, requests
from clickhouse_driver import Client

import logging
logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y/%m/%d-%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# function for a query
def query(query_sql):
    host = '10.202.0.4 8'.replace(' ', '') #10.202.0.4 8
    client = Client(host=host,port='9000',password='12345678',database='defi')
    set_m = 'SET max_memory_usage = 30000000000;'
    client.execute(set_m)
    set_m = 'set max_bytes_before_external_group_by = 30000000000;'
    client.execute(set_m)
    return client.query_dataframe(query_sql)


def setup_clickhouse_client(platform):
    """ insert dataframe needs use_numpy to be True """
    host = '10.202.0. 48'.replace(' ', '')
    _port = '9000'
    if platform == 'AWS':
        host = 'snowball-clickhouse-tcp-d18fab681d702206.elb.ap-southeast-1.amazonaws.com'
    elif platform == 'tz251':
        host = '192.168.101.202'
        _port = '8123'
    client = Client(host=host, port=_port, password='12345678', database='tmp')
    return client


def create_table(platform, table_name = 'defi.all_label_address'):
    query_sql = ""
    if table_name == 'defi.all_label_address':
        query_sql = f"""CREATE TABLE {table_name}
                        (
                            `type` String,
                            `Name` String,
                            `Address` String,
                            `chain` String,
                            `Comment` String,
                            `important` String,
                            `id` String,
                            `is_contract` Int8,
                            `update_time` String,
                            `custom_tags` String,
                            `last_update_time` String
                        )
                        ENGINE = MergeTree
                        PRIMARY KEY Address
                        ORDER BY Address
                        SETTINGS index_granularity = 8192;"""
    
    ch_client = setup_clickhouse_client(platform)
    ch_client.execute(query_sql)


def send_lark_message(channel, msg, at_user = False):
    """
    send message to lark on a given channel
    ref: https://open.feishu.cn/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN
    """
    if at_user:
        msg += '<at id=7202984717992345632></at>'
    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "content": msg,
                        "tag": "lark_md"
                    }
                }
            ]
        }
    }
    if not channel:
        return True
    # send the POST request
    response = requests.post(
        channel,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    trials = 0
    while response.status_code != 200 and trials <= 3:
        logging.error(f"Failed to send lark message, status code: {response.status_code}, message: {response.text}")
        # send the POST request
        response = requests.post(
            channel,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload)
        )
        trials += 1
    return True
