import argparse
import os
import re
import sys
from datetime import datetime

import pandas as pd

# 2 depth 위 상위 경로 설정(root_path)
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from libs import database
from libs.kiapslogging import KiapsLogging


def db_transfer():
    logger = KiapsLogging()

    root_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))
    if not os.path.exists(os.path.join(root_path, "logs")):
        os.makedirs(os.path.join(root_path, "logs"))
    log_path = "{}/logs/UPLOAD_{}.log".format(root_path, datetime.now().strftime("%Y%m%d"))
    log_dt_format = "%Y-%m-%d %H:%M:%S"

    try:
        with open(args.data, "r") as reader:
            result_list = []

            for i, line in enumerate(reader.readlines()):
                # header skip
                if i < 1:
                    continue
                words = re.split(r"\s+", line.replace("'", "").strip())
                # Merge some list items in a Python List (date + time)
                words[5:7] = [''.join(words[5:7])]
                # reorder fifth element 'datetime' to first element
                order = [5, 0, 1, 2, 3, 4, 6, 7, 8, 9, 10]
                words = [words[i] for i in order]
                result_list.append(words)

            df = pd.DataFrame(result_list)
            df.columns = ["hdr_datetime",
                          "obstype",
                          "hdr_ops_subtype",
                          "hdr_statid",
                          "hdr_lat",
                          "hdr_lon",
                          "hdr_varno",
                          "body_initial_obsvalue",
                          "body_bgvalue",
                          "body_fg_depar",
                          "body_ops_datum_flags_b0"]
            df["hdr_datetime"] = df["hdr_datetime"].astype('datetime64[ns]')
            df["obstype"] = df["obstype"].astype(str)
            df["hdr_ops_subtype"] = df["hdr_ops_subtype"].astype(int)
            df["hdr_statid"] = df["hdr_statid"].astype(str)
            df["hdr_lat"] = df["hdr_lat"].astype(float)
            df["hdr_lon"] = df["hdr_lon"].astype(float)
            df["hdr_varno"] = df["hdr_varno"].astype(int)
            df["body_initial_obsvalue"] = df["body_initial_obsvalue"].replace(to_replace="NULL", value=0)
            df["body_initial_obsvalue"] = df["body_initial_obsvalue"].astype(float)
            df["body_bgvalue"] = df["body_bgvalue"].replace(to_replace="NULL", value=0)
            df["body_bgvalue"] = df["body_bgvalue"].astype(float)
            df["body_fg_depar"] = df["body_fg_depar"].replace(to_replace="NULL", value=0)
            df["body_fg_depar"] = df["body_fg_depar"].astype(float)
            df["body_ops_datum_flags_b0"] = df["body_ops_datum_flags_b0"].astype(int)
            # print(df.dtypes)
            # database.write_mysql(filename, df)
            # database.write_mongodb(filename, df)
            # tag_columns = ["hdr_datetime", "obstype", "hdr_ops_subtype", "hdr_statid", "hdr_lat", "hdr_lon", "hdr_varno"]
            # df.set_index("hdr_datetime", inplace=True)
            # database.write_influxdb(filename, tag_columns, df)
            # database.write_clickhouse(filename, df)
            # database.write_druid(filename, df)
            # database.write_kafka(filename, df)
            database.read_kafka(filename)

            logger.upload_info("Successfully uploaded the file {}".format(filename + "_" + date + ".txt"))
            with open(log_path, "a") as writer:
                writer.write("[INFO] [UPLOAD] [{}] Successfully uploaded the file {}\n".format(
                    datetime.now().strftime(log_dt_format), filename + "_" + date + ".txt"))
    except Exception as e:
        print("File not accessible")

        logger.upload_error(e)
        with open(log_path, "a") as writer:
            writer.write("[ERROR] [UPLOAD] [{}] {}\n".format(datetime.now().strftime(log_dt_format), e))


if __name__ == "__main__":
    # 확장자를 제외한 이 파일 이름 가져오기
    base = os.path.basename(__file__)
    filename = os.path.splitext(base)[0]

    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="D:/project/PythonProjects/KIAPS_BATCH/data/surface/surface_LATE"
                                          "/surface_LATE_2018060100.txt")
    args = parser.parse_args()
    if args.data:
        print("data_path: " + args.data)
        # 확장자를 제외한 data 파일의 날짜 가져오기
        date = args.data[-14:]
        date = date[0:10]

    db_transfer()
