import pandas as pd
import numpy as np

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class SurfaceXiv(MainBase):
    def __init__(self):
        super().__init__()

    def _init(self):
        pass

    @KiapsLogging.log_decorator
    def db_transfer(self, file_name):
        print(file_name)
        df = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") +
                           "/surface/" +
                           self.config.get("GLOBAL", "FILE_DATE")[:6] +
                           "/" +
                           self.config.get('SURFACE', 'SURFACE_XIV') % (self.config.get("GLOBAL", "FILE_DATE")),
                           delim_whitespace=True, header=0, dtype={"stnID": object})

        df.columns = ["nobs", "ObsTime", "lat", "lon", "stnID", "stntype", "stnHgt", "Pressure", 
                     "PMSL", "T2m", "Td2m", "RH2m", "WD10m", "Ws10m", "Pressure(B)", "PMSL(B)",
                     "T2m(B)", "Td2m(B)", "RH2m(B)", "U10m(B)", "V10m(B)"]

        df = df.drop([df.columns[14], df.columns[15], df.columns[16], df.columns[17], df.columns[18], df.columns[19], df.columns[20]], axis=1)
        df.insert(0, "datetime", self.config.get("GLOBAL", "FILE_DATE")+'00')
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S")
        df["nobs"] = df["nobs"].astype(int)
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)

        df["ObsTime"] = pd.to_datetime(df["ObsTime"], format="%Y%m%d%H%M%S")
        df["stntype"] = df["stntype"].astype(int)
        df["stnHgt"] = df["stnHgt"].astype(float)

        df["Pressure"] = df["Pressure"].astype(float)
        df["PMSL"] = df["PMSL"].astype(float)
        df["T2m"] = df["T2m"].astype(float)
        df["Td2m"] = df["Td2m"].astype(float)
        df["RH2m"] = df["RH2m"].astype(float)
        df["WD10m"] = df["WD10m"].astype(float)
        df["Ws10m"] = df["Ws10m"].astype(float)

        df["xivFlg"] = 1

        database.write_mysql("surface_grqc", df)
        print("done")
