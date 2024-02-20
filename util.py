import json, requests, time
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

    def truncate_db(self, table_name):
        query_sql = f"truncate table {table_name}"
        self.client.execute(query_sql)
    
    def listWriteToCh(self, table_name, rows):
        start_time = time.time()
        query_sql = f"INSERT INTO {table_name} VALUES"
        batch_size = 2000
        rows_inserted = 0
        for i in range(0, len(rows), batch_size):
            batch_data = rows[i:i+batch_size]
            self.client.execute(query_sql, batch_data, types_check=True)
            rows_inserted += len(batch_data)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Insert {rows_inserted} rows took {elapsed_time} seconds.")


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
        elif table_name == 'defi.total_marking_address':
            query_sql = f"""
                            CREATE TABLE IF NOT EXISTS defi.total_marking_address
                            (
                                `type` String,
                                `Name` String,
                                `Address` String,
                                `Comment` String,
                                `important` String,
                                `last_update_time` String
                            )
                            ENGINE = MergeTree
                            PRIMARY KEY Address
                            ORDER BY Address
                            SETTINGS index_granularity = 8192;"""
        elif table_name == 'defi.cheating_wallet':
            query_sql = f"""CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                `address` String,
                                `Traded_Tokens` String,
                                `Reason` String,
                                `cnt` String,
                                `c_usd` String,
                                `important` String,
                                `eth_smart` String,
                                `txns` String,
                                `tokens` String,
                                `interval` String,
                                `last_update_time` String
                            )
                            ENGINE = MergeTree
                            PRIMARY KEY address
                            ORDER BY address
                            SETTINGS index_granularity = 8192;"""
        elif table_name == 'defi.cmcv2':
            query_sql = f"""CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                `cmc_rank` Nullable(Float64),
                                `id` Float64,
                                `name` String,
                                `symbol` String,
                                `Ethereum` Nullable(String),
                                `Arbitrum` Nullable(String),
                                `tags` Nullable(String),
                                `market_cap` Nullable(Float64),
                                `price` Nullable(Float64),
                                `total_supply` Nullable(Float64),
                                `circulating_supply` Nullable(Float64),
                                `max_supply` Nullable(Float64),
                                `self_reported_circulating_supply` Nullable(Float64),
                                `self_reported_market_cap` Nullable(Float64),
                                `percent_change_24h` Nullable(Float64),
                                `percent_change_30d` Nullable(Float64),
                                `volume_24h` Nullable(Float64),
                                `volume_7d` Nullable(Float64),
                                `tvl` String,
                                `date_added` String,
                                `last_updated` String,
                                `turnover_24h` String,
                                `turnover_7d` String,
                                `tvl_divide_marketcap` String,
                                `circulating_supply_processed` String,
                                `circulating_market_cap_processed` Nullable(Float64),
                                `last_update_time` String
                            )
                            ENGINE = MergeTree
                            PRIMARY KEY id
                            ORDER BY id
                            SETTINGS index_granularity = 8192;"""
        
        elif table_name == 'defi.cmc_history':
            query_sql = f"""CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                `cmc_rank` Nullable(Float64),
                                `id` Float64,
                                `name` String,
                                `symbol` String,
                                `Ethereum` Nullable(String),
                                `Arbitrum` Nullable(String),
                                `tags` Nullable(String),
                                `market_cap` Nullable(Float64),
                                `price` Nullable(Float64),
                                `total_supply` Nullable(Float64),
                                `circulating_supply` Nullable(Float64),
                                `max_supply` Nullable(Float64),
                                `self_reported_circulating_supply` Nullable(Float64),
                                `self_reported_market_cap` Nullable(Float64),
                                `percent_change_24h` Nullable(Float64),
                                `percent_change_30d` Nullable(Float64),
                                `volume_24h` Nullable(Float64),
                                `volume_7d` Nullable(Float64),
                                `tvl` String,
                                `date_added` String,
                                `last_updated` String,
                                `turnover_24h` String,
                                `turnover_7d` String,
                                `tvl_divide_marketcap` String,
                                `circulating_supply_processed` String,
                                `circulating_market_cap_processed` Nullable(Float64),
                                `last_update_time` String
                            )
                            ENGINE = MergeTree
                            PRIMARY KEY (id, last_updated)
                            ORDER BY (id, last_updated)
                            SETTINGS index_granularity = 8192;"""
        if query_sql != "":
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
