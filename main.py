### 保存 all_label_address
import subprocess, time, json
import pandas as pd
from env import tables, bucket, update_interval, uploading
from util import query, logger, setup_clickhouse_client, create_table

from apscheduler.schedulers.blocking import BlockingScheduler
from pytz import utc

def copy_file_to_gcs(local_file, bucket, uploading = True):
    command = ["gsutil", "cp", local_file, f"gs://{bucket}/{local_file}"]
    if not uploading:
        command = ["gsutil", "cp", f"gs://{bucket}/{local_file}", local_file]
    try:
        result = subprocess.run(command, check=True, capture_output=True)
        logger.info("File copied successfully.")
        logger.info(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        logger.error("An error occurred while copying the file.")
        logger.error(e.stderr.decode())


def upload_to_gcs(table_name, bucket):
    """ load data from a given table name, then upload to the gcs buckeet """
    table_name = 'defi.all_label_address'
    csv_name = table_name + '.csv'
    logger.info(f"created file: {csv_name}")
    query_sql = f""" SELECT *  FROM {table_name} """
    df = query(query_sql)
    df['upload_time'] = pd.to_datetime(time.time(), unit = 's').strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(csv_name)
    copy_file_to_gcs(csv_name, bucket, uploading = True)


def download_from_gcs(table_name, bucket):
    """ downlaod table_name csv file from gcs, read it and save it to clickhouse if not exists, otherwise update it """
    csv_name = table_name + '.csv'
    copy_file_to_gcs(csv_name, bucket, uploading = False)
    df = pd.read_csv(csv_name)
    if table_name == 'defi.all_label_address':
        primary_key = list(query(f"select Address from {table_name} ")['Address'])
        df_new = df[~df['Address'].isin(primary_key)].copy()
        df_existed = df[df['Address'].isin(primary_key)].copy()
        for _, row in df_new.iterrows():
            label_data = [(row['type'], row['Name'], row['Address'], row['Chain'], row['Comment'], row['important'], row['id'], row['is_contract'], row['update_time'], row['upload_time'])]
            try:
                ch_client_251.execute(f'INSERT INTO {table_name} VALUES', label_data)
            except Exception as e:
                logger.error(f"Error during inserting into {table_name}: {e}, data: {label_data}")
        for _, row in df_existed.iterrows():
            update_sql_query = f"""ALTER TABLE {table_name}
                    UPDATE type = '{row["type"]}', Name = '{row["Name"]}', Comment = '{row["Comment"]}',  chain = '{row["chain"]}',
                    id = '{row["id"]}', is_contract = '{row["is_contract"]}', update_time = '{row["update_time"]}', upload_time = '{row["upload_time"]}'
                    WHERE Address = '{row["Address"]}'; """
            try:
                query(update_sql_query)
            except Exception as e:
                logger.error(f"Error during updating {table_name}: {e}, query: {update_sql_query}")
                return []
    # TODO: ADD MORE TABLES

def gcs_transfer_main(upload = True):
    """ process list of tables """
    logger.info(f"need to upload {tables}")
    if upload:
        for table in tables:
            upload_to_gcs(table, bucket)
    else:
        for table in tables:
            create_table('tz251', table)
            download_from_gcs(table, bucket)


if __name__  == '__main__':
    gcs_transfer_main(upload = False)
    ch_client_251 = setup_clickhouse_client('tz251')
    scheduler = BlockingScheduler(timezone = utc)
    if uploading == 1:
        scheduler.add_job(gcs_transfer_main, 'interval', miutes=update_interval, start_date='2023-07-09 08:00:00', args=(True))
    else:
        scheduler.add_job(gcs_transfer_main, 'interval', miutes=update_interval, start_date='2023-07-09 08:00:00', args=(False))
    scheduler.start()

