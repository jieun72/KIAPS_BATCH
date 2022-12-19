import pandas as pd

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class SondeAI(MainBase):
    def __init__(self):
        super().__init__()

    def _init(self):
        pass

    @KiapsLogging.log_decorator
    def db_transfer(self, file_name):
        print(file_name)
        
        df = pd.read_csv    (self.config.get("DIRECTORY", "DATA_PATH") +
                           "/sonde/" +
                           self.config.get("GLOBAL", "FILE_DATE") +
                           "/" +
                           self.config.get('SONDE', 'SONDE_AI') % (self.config.get("GLOBAL", "FILE_DATE")))

        df.columns = ["no", "ObsTime", "StnID", "Pressure", "T", "AI_QC_PASS", "lower", "mu", "upper"]

        df = df.drop(columns=["no", "ObsTime"])
        df.insert(0, "datetime", self.config.get("GLOBAL", "FILE_DATE")+'00')
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S")
        
        df["StnID"] = df["StnID"].astype(int)
        df["Pressure"] = df["Pressure"].astype(float)
        df["T"] = df["T"].astype(float)
        df["AI_QC_PASS"] = df["AI_QC_PASS"].astype(int)
        df["lower"] = df["lower"].astype(float)
        df["mu"] = df["mu"].astype(float)
        df["upper"] = df["upper"].astype(float)

        print(df)
        database.write_mysql(file_name, df)
