from time import sleep

# import numpy as np
# from clickhouse_driver import connect
# from influxdb_client import InfluxDBClient, WriteOptions
# from kafka import KafkaConsumer
# from kafka import KafkaProducer
# from pydruid.db import connect as connect_druid
# from pymongo import MongoClient
from sqlalchemy import create_engine


def write_mysql(table_name, df):
    connection_url = "mysql+pymysql://kiaps:Q1w2e3r4t%@192.168.1.252:3306/kiaps?charset=utf8"
    engine = create_engine(connection_url)
    with engine.begin():
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        print(">>> All good. " + table_name)


def write_mysql_dask(table_name, ddf):
    connection_url = "mysql+pymysql://kiaps:Q1w2e3r4t%@192.168.1.252:3306/kiaps?charset=utf8"
    ddf.to_sql(name=table_name, uri=connection_url, if_exists="append", index=False)
    print(">>> All good." + table_name)


# def write_mongodb(table_name, df):
#     client = MongoClient("mongodb://116.123.119.229:27017/kiaps")
#     db = client["kiaps"]
#     collection = db[table_name]
#     df.reset_index(inplace=True)
#     data_dict = df.to_dict("records")
#     collection.insert_many(data_dict)


# def write_influxdb(measurement_name, tag_columns: [], df):
#     bucket = "kiaps"
#     org = "WIZAI"
#     token = "VbKWoqCu6awcds0-jy8jBZkOp6PVG5QGHkdQDDQx21FKtfwFV5wpKbwmAz3gdhQJzu1CicFIj6eF0GT05ti8JA=="
#     url = "116.123.119.229:8086"

#     with InfluxDBClient(url=url, token=token, org=org) as _client:
#         with _client.write_api(write_options=WriteOptions(batch_size=500,
#                                                           flush_interval=10000,
#                                                           jitter_interval=2000,
#                                                           retry_interval=5000,
#                                                           max_retries=5,
#                                                           max_retry_delay=30000,
#                                                           exponential_base=2)) as _write_client:
#             _write_client.write(bucket=bucket, org=org, record=df, data_frame_measurement_name=measurement_name,
#                                 data_frame_tag_columns=tag_columns)


# def write_clickhouse(table_name, df):
#     connection_url = "clickhouse://default:@116.123.119.229:9000/default"
#     conn = connect(connection_url)
#     cursor = conn.cursor()

#     cursor.executemany("INSERT INTO " + table_name + " VALUES", df.to_dict('records'))
#     print(cursor.rowcount, "rows has been inserted.")

#     cursor.close()
#     conn.close()


# def write_druid(table_name, df):
#     conn = connect_druid(host='116.123.119.229', port=8082, path='/druid/v2/sql/', scheme='http')
#     cursor = conn.cursor()

#     cursor.execute("""
#         SELECT *
#           FROM wikipedia
#          LIMIT 10
#     """)
#     for row in cursor:
#         print(row)

#     cursor.close()
#     conn.close()


# def write_kafka(table_name, df):
#     df = np.asarray(df)
#     producer = KafkaProducer(bootstrap_servers=["146.56.168.215:9092"])
#     for row in range(len(df)):
#         send_data = ''
#         for i in range(df.shape[1]):
#             send_data = send_data + str(df[row, i]) + '|'
#         send_data = send_data[:-1]
#         print(send_data)
#         producer.send(table_name, value=send_data.encode())
#         producer.flush()
#         sleep(0.05)


# def read_kafka(table_name):
#     consumer = KafkaConsumer(table_name, bootstrap_servers=["146.56.168.215:9092"], auto_offset_reset="earliest",
#                              enable_auto_commit=True, consumer_timeout_ms=1000)
#     for message in consumer:
#         print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % (
#             message.topic, message.partition, message.offset, message.key, message.value
#         ))
