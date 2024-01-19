import json, requests
from clickhouse_driver import Client

import logging
logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y/%m/%d-%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

class Clickhouse:
    def __init__(self, platform):
        if platform == 'AWS':
            self.host = 'snowball-clickhouse-tcp-d18fab681d702206.elb.ap-southeast-1.amazonaws.com'
        elif platform == 'GCP':
            self.host = '10.202.0.4 8'.replace(' ', '') #10.202.0.4 8, leave space to avoid auto decoding
        elif platform == 'tz251':
            self.host = '192.168.101.202'
        self.client = Client(host=self.host,port='9000',password='12345678',database='defi')
        set_m = 'SET max_memory_usage = 30000000000;'
        self.client.execute(set_m)
        set_m = 'set max_bytes_before_external_group_by = 30000000000;'
        self.client.execute(set_m)
    
    def query(self, query_sql):
        return self.client.query_dataframe(query_sql)

    def execute(self, query1, query2):
        self.client.execute(query1, query2)

    def create_table(self, table_name):
        query_sql = ""
        if table_name == 'defi.all_label_address':
            query_sql = f"""CREATE TABLE IF NOT EXISTS {table_name}
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
            
            self.client.execute(query_sql)


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
