import re

import pandas as pd
import numpy as np

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
                  "/sonde/sonde_" +
                  self.config.get("GLOBAL", "FILE_DATE")[:6] +
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
            df.columns = ["datetime", "nobs", "StnID", "type", "nlev", "lat", "lon", "StnHgt", "ObsTime",
                          "lev", "Pressure", "T", "T_flag", "T_OmB", "U", "U_flag", "U_OmB",
                          "V", "V_flag", "V_OmB", "rh", "rh_flag", "rh_OmB", "q", "q_flag", "q_OmB",
                          "mflag", "avail"]

            df = df.drop([df.columns[9], df.columns[13], df.columns[16], df.columns[19], df.columns[22], df.columns[25]], axis=1)
            df["datetime"] = df["datetime"].astype('datetime64[ns]')
            df["nobs"] = df["nobs"].astype(int)
            df["StnID"] = df["StnID"].astype(int)
            df["type"] = df["type"].astype(str)
            df["nlev"] = df["nlev"].astype(int)
            df["lat"] = df["lat"].astype(float)
            df["lon"] = df["lon"].astype(float)

            df["StnHgt"] = df["StnHgt"].astype(float)
            
            # TODO: TimeDiff 수정예정
            df["ObsTime"] = np.nan

            df["Pressure"] = df["Pressure"].astype(float)

            df["T"] = df["T"].astype(float)
            df["T_flag"] = df["T_flag"].astype(float)

            df["U"] = df["U"].astype(float)
            df["U_flag"] = df["U_flag"].astype(float)

            df["V"] = df["V"].astype(float)
            df["V_flag"] = df["V_flag"].astype(float)
            
            df["rh"] = df["rh"].astype(float)
            df["rh_flag"] = df["rh_flag"].astype(float)
            
            df["q"] = df["q"].astype(float)
            df["q_flag"] = df["q_flag"].astype(float)
            
            df["mflag"] = df["mflag"].astype(float)
            df["avail"] = df["avail"].astype(float)

            database.write_mysql("sonde_grqc", df)
