import re
import pandas as pd
import numpy as np

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class SurfaceGRQC(MainBase):
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
                           self.config.get('SURFACE', 'SURFACE_coast') % (self.config.get("GLOBAL", "FILE_DATE")),
                           delim_whitespace=True, header=0, dtype={"stnID": object})
        
        df.columns = ["nobs", "ObsTime", "lat", "lon", "stnID", "stntype", "stnHgt", "FLAG", "Pressure", "Pressure_flag",
                     "PMSL", "PMSL_flag", "T2m", "T2m_flag", "Td2m", "Td2m_flag", "RH2m", "RH2m_flag", "Q2m", "Q2m_flag",
                     "U10m", "U10m_flag", "V10m", "V10m_flag", "WD10m", "WD10m_flag", "Ws10m", "Ws10m_flag", "Cldcov", "Cldcov_flag"]
        
        df = df.drop([df.columns[28], df.columns[29]], axis=1)
        df.insert(0, "datetime", self.config.get("GLOBAL", "FILE_DATE")+'00')
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M%S")
        df["nobs"] = df["nobs"].astype(int)
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)
        
        df["ObsTime"] = pd.to_datetime(df["ObsTime"], format="%Y%m%d%H%M%S")
        df["stntype"] = df["stntype"].astype(int)
        df["stnHgt"] = df["stnHgt"].astype(float)
        df["FLAG"] = df["FLAG"].astype(int)

        df["Pressure"] = df["Pressure"].astype(float)
        df["Pressure_flag"] = df["Pressure_flag"].astype(float)
        df["PMSL"] = df["PMSL"].astype(float)
        df["PMSL_flag"] = df["PMSL_flag"].astype(float)
        df["T2m"] = df["T2m"].astype(float)
        df["T2m_flag"] = df["T2m_flag"].astype(float)
        df["RH2m"] = df["RH2m"].astype(float)
        df["RH2m_flag"] = df["RH2m_flag"].astype(float)
        df["Q2m"] = df["Q2m"].astype(float)
        df["Q2m_flag"] = df["Q2m_flag"].astype(float)
        df["U10m"] = df["U10m"].astype(float)
        df["U10m_flag"] = df["U10m_flag"].astype(float)
        df["V10m"] = df["V10m"].astype(float)
        df["V10m_flag"] = df["V10m_flag"].astype(float)
        df["WD10m"] = df["WD10m"].astype(float)
        df["WD10m_flag"] = df["WD10m_flag"].astype(float)
        df["Ws10m"] = df["Ws10m"].astype(float)
        df["Ws10m_flag"] = df["Ws10m_flag"].astype(float)
                
        df["Pressure_GR_QC"] = np.nan
        df["T2m_GR_QC"] = np.nan
        df["RH2m_GR_QC"] = np.nan
        df["U10m_GR_QC"] = np.nan
        df["V10m_GR_QC"] = np.nan
        df["WD10m_GR_QC"] = np.nan
        df["Ws10m_GR_QC"] = np.nan
        df["xivFlg"] = 0

        database.write_mysql(file_name, df)
        print("done")
