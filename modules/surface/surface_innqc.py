import pandas as pd

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class SurfaceInnQC(MainBase):
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
                           self.config.get('SURFACE', 'SURFACE_INNQC') % (self.config.get("GLOBAL", "FILE_DATE")),
                           delim_whitespace=True, header=0, dtype={"stnID": object})
        df = df.drop([df.columns[6], df.columns[9], df.columns[10], df.columns[11], df.columns[15], df.columns[16], df.columns[17], 
                      df.columns[21], df.columns[22], df.columns[23], df.columns[27], df.columns[28], df.columns[29], 
                      df.columns[33], df.columns[34], df.columns[35], df.columns[39], df.columns[40], df.columns[41], 
                      df.columns[44], df.columns[45], df.columns[46]], axis=1)

        df.columns = ["nobs", "ObsTime", "lat", "lon", "stnID", "stntype", "FLAG",
                      "Pressure", "Pressure_flag", "Pressure_score", "T2m", "T2m_flag", "T2m_score",
                      "RH2m", "RH2m_flag", "RH2m_score", "Q2m", "Q2m_flag", "Q2m_score",
                      "U10m", "U10m_flag", "U10m_score", "V10m", "V10m_flag", "V10m_score"]
        
        df.insert(0, "datetime", self.config.get("GLOBAL", "FILE_DATE")+'00')
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S")
        df["ObsTime"] = pd.to_datetime(df["ObsTime"], format="%Y%m%d%H%M%S")
        
        database.write_mysql(file_name, df)
        
        print("done")
