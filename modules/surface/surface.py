import pandas as pd

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class Surface(MainBase):
    def __init__(self):
        super().__init__()

    def _init(self):
        pass

    @KiapsLogging.log_decorator
    def db_transfer(self, file_name):
        print(file_name)
        df = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") +
                           "/surface/" +
                           self.config.get("GLOBAL", "FILE_DATE") +
                           "/" +
                           self.config.get('SURFACE', 'SURFACE') % (self.config.get("GLOBAL", "FILE_DATE")),
                           delim_whitespace=True, dtype={"stnID": object})
        df.columns = ["nobs", "Date/Time", "lat", "lon", "stnID", "stntype", "stnHgt",  # tag
                      "Flag", "Pressure", "Pmsl", "T2m", "Td2m", "RH2m", "Wd10m", "Ws10m"]  # field
        # df = df.drop(columns=["nobs"])
        df.insert(0, "datetime", self.config.get("GLOBAL", "FILE_DATE"))
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S")
        df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="%Y%m%d%H%M%S")
        database.write_mysql(file_name, df)
        print("done")
