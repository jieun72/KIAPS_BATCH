import re

import pandas as pd

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class SondeXiv(MainBase):
    def __init__(self):
        super().__init__()

    def _init(self):
        pass

    @KiapsLogging.log_decorator
    def db_transfer(self, file_name):
        print(file_name)
        with open(self.config.get("DIRECTORY", "DATA_PATH") +
                  "/sonde/" +
                  self.config.get("GLOBAL", "FILE_DATE") +
                  "/" +
                  self.config.get('SONDE', 'SONDE_XIV') % (self.config.get("GLOBAL", "FILE_DATE")), "r") as reader:
            result_list = []

            for i, line in enumerate(reader.readlines()):
                if i < 3:
                    continue
                words = re.split(r"\s+", line.strip())
                if len(words) < 9:
                    mst_list = [self.config.get("GLOBAL", "FILE_DATE") + "00"]
                    mst_list.extend(words)
                else:
                    result_list.append(mst_list + words)

            df = pd.DataFrame(result_list)
            df.columns = ["datetime", "tobs", "StnID", "obtype", "nlev", "lat", "lon", "StnHgt", "TimeDiff",
                          "lev", "Pressure", "Temp", "Temp_flag", "Temp_OmB", "U-comp", "U-comp_flag", "U-comp_OmB",
                          "V-comp", "V-comp_flag", "V-comp_OmB", "RH", "RH_flag", "RH_OmB", "Q", "Q_flag", "Q_OmB",
                          "Mflag", "avail"]
            df["datetime"] = df["datetime"].astype('datetime64[ns]')
            df["tobs"] = df["tobs"].astype(int)
            df["StnID"] = df["StnID"].astype(int)
            df["obtype"] = df["obtype"].astype(str)
            df["nlev"] = df["nlev"].astype(int)
            df["lat"] = df["lat"].astype(float)
            df["lon"] = df["lon"].astype(float)
            df["StnHgt"] = df["StnHgt"].astype(float)
            df["TimeDiff"] = df["TimeDiff"].astype(float)

            df["lev"] = df["lev"].astype(int)
            df["Pressure"] = df["Pressure"].astype(float)
            df["Temp"] = df["Temp"].astype(float)
            df["Temp_flag"] = df["Temp_flag"].astype(float)
            df["Temp_OmB"] = df["Temp_OmB"].astype(float)
            df["U-comp"] = df["U-comp"].astype(float)
            df["U-comp_flag"] = df["U-comp_flag"].astype(float)
            df["U-comp_OmB"] = df["U-comp_OmB"].astype(float)
            df["V-comp"] = df["V-comp"].astype(float)
            df["V-comp_flag"] = df["V-comp_flag"].astype(float)
            df["V-comp_OmB"] = df["V-comp_OmB"].astype(float)
            df["RH"] = df["RH"].astype(float)
            df["RH_flag"] = df["RH_flag"].astype(float)
            df["RH_OmB"] = df["RH_OmB"].astype(float)
            df["Q"] = df["Q"].astype(float)
            df["Q_flag"] = df["Q_flag"].astype(float)
            df["Q_OmB"] = df["Q_OmB"].astype(float)
            df["Mflag"] = df["Mflag"].astype(float)
            df["avail"] = df["avail"].astype(float)
            database.write_mysql(file_name, df)
