import os
import re
import pandas as pd
from datetime import datetime

import database
from kiaps_log import KiapsLogging

root_path = "C:/Users/user/Downloads"


def db_transfer():
    logger = KiapsLogging()

    if not os.path.exists(root_path + "/logs"):
        os.makedirs(root_path + "/logs")
    log_path = root_path + "/logs/UPLOAD_{}.log".format(datetime.now().strftime("%Y%m%d"))
    log_dt_format = "%Y-%m-%d %H:%M:%S"

    try:
        with open(root_path + '/sonde_2020061600.txt', 'r') as reader:
            result_list = []

            for i, line in enumerate(reader.readlines()):
                # header skip
                if i < 3:
                    continue
                # if i > 200:
                #     continue
                words = re.split(r'\s+', line.strip())
                # hierarchy level 1
                if len(words) < 9:
                    mst_list = []
                    mst_list.extend(words)
                else:
                    result_list.append(mst_list + words)
                    # print(result_list)

            df_dtl = pd.DataFrame(result_list)
            df_dtl.columns = ["nobs", "type", "StnID", "lat", "lon", "StnHgt", "nlev", "ObsTime",
                              "lev", "Pressure", "Height", "GPM", "T", "Td", "RH", "Wd", "Ws", "u", "v", "q", "vsign", "vlon", "vlat", "time"]
            database.write_mysql('sonde', df_dtl)

            logger.upload_info(
                "Successfully uploaded the file {}".format("sonde_2020061600.txt"))
            with open(log_path, "a") as writer:
                writer.write("[INFO] [UPLOAD] [{}] Successfully uploaded the file {}\n".format(datetime.now().strftime(log_dt_format), "sonde_2020061600.txt"))
    except Exception as e:
        print("File not accessible")

        logger.upload_error(e)
        with open(log_path, "a") as writer:
            writer.write("[ERROR] [UPLOAD] [{}] {}\n".format(datetime.now().strftime(log_dt_format), e))


if __name__ == '__main__':
    db_transfer()
