import re

import pandas as pd

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
            df.columns = ["tobs", "StnID", "obtype", "nlev", "lat", "lon", "StnHgt", "ObsTime",
                          "lev", "Pressure", "Temp", "Temp_flag", "Wd", "Wd_flag", "Ws", "Ws_flag", "U", "U_flag",
                          "V", "V_flag", "Rh", "Rh_flag", "Q", "Q_flag", "z", "z_flag",
                          "Mflag", "avail"]
            df.insert(0, "datetime", self.config.get("GLOBAL", "FILE_DATE"))
            df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S")
            df["tobs"] = df["tobs"].astype(int)
            df["StnID"] = df["StnID"].astype(int)
            df["obtype"] = df["obtype"].astype(str)
            df["nlev"] = df["nlev"].astype(int)
            df["lat"] = df["lat"].astype(float)
            df["lon"] = df["lon"].astype(float)
            df["StnHgt"] = df["StnHgt"].astype(float)
            df["ObsTime"] = df["ObsTime"].astype(float)

            df["lev"] = df["lev"].astype(int)
            df["Pressure"] = df["Pressure"].astype(float)
            df["Temp"] = df["Temp"].astype(float)
            df["Temp_flag"] = df["Temp_flag"].astype(float)
            df["Wd"] = df["Wd"].astype(float)
            df["Wd_flag"] = df["Wd_flag"].astype(float)
            df["Ws"] = df["Ws"].astype(float)
            df["Ws_flag"] = df["Ws_flag"].astype(float)
            df["U"] = df["U"].astype(float)
            df["U_flag"] = df["U_flag"].astype(float)
            df["V"] = df["V"].astype(float)
            df["V_flag"] = df["V_flag"].astype(float)
            df["Rh"] = df["Rh"].astype(float)
            df["Rh_flag"] = df["Rh_flag"].astype(float)
            df["Q"] = df["Q"].astype(float)
            df["Q_flag"] = df["Q_flag"].astype(float)
            df["z"] = df["z"].astype(float)
            df["z_flag"] = df["z_flag"].astype(float)
            df["Mflag"] = df["Mflag"].astype(float)
            df["avail"] = df["avail"].astype(float)
            database.write_mysql(file_name, df)
