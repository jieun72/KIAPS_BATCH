import re

import pandas as pd
import numpy as np

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class SondeInnQC(MainBase):
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
                  self.config.get('SONDE', 'SONDE_INNQC') % (self.config.get("GLOBAL", "FILE_DATE")), "r") as reader:
            result_list = []

            for i, line in enumerate(reader.readlines()):
                if i < 3:
                    continue
                words = re.split(r"\s+", line.strip())
                if len(words) < 9:
                    mst_list = []
                    mst_list.extend(words)
                else:
                    result_list.append(mst_list + words)

            df = pd.DataFrame(result_list)
            df.columns = ["nobs", "StnID", "type", "nlev", "lat", "lon", "StnHgt", "ObsTime",
                          "lev", "Pressure", "T", "T_flag", "Wd", "Wd_flag", "Ws", "Ws_flag", "U", "U_flag",
                          "V", "V_flag", "rh", "rh_flag", "q", "q_flag", "z", "z_flag",
                          "mflag", "avail"]

            df = df.drop([df.columns[8]], axis=1)                          
            df.insert(0, "datetime", self.config.get("GLOBAL", "FILE_DATE")+'00')

            df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S")
            df["nobs"] = df["nobs"].astype(int)
            df["StnID"] = df["StnID"].astype(int)
            df["type"] = df["type"].astype(str)
            df["nlev"] = df["nlev"].astype(int)
            df["lat"] = df["lat"].astype(float)
            df["lon"] = df["lon"].astype(float)
            df["StnHgt"] = df["StnHgt"].astype(float)
            df["ObsTime"] = np.nan

            df["Pressure"] = df["Pressure"].astype(float)
            df["T"] = df["T"].astype(float)
            df["T_flag"] = df["T_flag"].astype(float)
            df.insert(12, "T_KPOP_QC", np.nan)
            df.insert(13, "T_AI_QC", np.nan)

            df["Wd"] = df["Wd"].astype(float)
            df["Wd_flag"] = df["Wd_flag"].astype(float)
            df.insert(16, "Wd_KPOP_QC", np.nan)
            df.insert(17, "Wd_AI_QC", np.nan)

            df["Ws"] = df["Ws"].astype(float)
            df["Ws_flag"] = df["Ws_flag"].astype(float)
            df.insert(20, "Ws_KPOP_QC", np.nan)
            df.insert(21, "Ws_AI_QC", np.nan)

            df["U"] = df["U"].astype(float)
            df["U_flag"] = df["U_flag"].astype(float)
            df.insert(24, "U_KPOP_QC", np.nan)
            df.insert(25, "U_AI_QC", np.nan)

            df["V"] = df["V"].astype(float)
            df["V_flag"] = df["V_flag"].astype(float)
            df.insert(28, "V_KPOP_QC", np.nan)
            df.insert(29, "V_AI_QC", np.nan)

            df["rh"] = df["rh"].astype(float)
            df["rh_flag"] = df["rh_flag"].astype(float)
            df.insert(32, "rh_KPOP_QC", np.nan)
            df.insert(33, "rh_AI_QC", np.nan)

            df["q"] = df["q"].astype(float)
            df["q_flag"] = df["q_flag"].astype(float)
            df.insert(36, "q_KPOP_QC", np.nan)
            df.insert(37, "q_AI_QC", np.nan)

            df["z"] = df["z"].astype(float)
            df["z_flag"] = df["z_flag"].astype(float)
            df["mflag"] = df["mflag"].astype(float)
            df["avail"] = df["avail"].astype(float)

            database.write_mysql(file_name, df)
