import dask.dataframe as dd
import pandas as pd

from libs import database
from libs.kiapslogging import KiapsLogging
from libs.mainbase import MainBase


class Amsua(MainBase):
    def __init__(self):
        super().__init__()

    def _init(self):
        pass

    @KiapsLogging.log_decorator
    def amsua(self, file_name):
        print(file_name)
        df_amsua = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") +
                                 "/amsua/" +
                                 self.config.get("GLOBAL", "FILE_DATE") +
                                 "/" +
                                 self.config.get('AMSUA', 'AMSUA') % (self.config.get("GLOBAL", "FILE_DATE")),
            delim_whitespace=True, skiprows=1, header=None)
        df_amsua.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE")+'00')
        df_amsua["datetime"] = pd.to_datetime(df_amsua["datetime"], format="%Y%m%d%H%M%S")
        df_amsua.columns = ["datetime", "iobs", "lat", "lon", "isat", "obs-time", "ob(01)", "ob(02)", "ob(03)", "ob(04)", "ob(05)", "ob(06)", "ob(07)", "ob(08)", "ob(09)", "ob(10)", "ob(11)", "ob(12)", "ob(13)", "ob(14)", "ob(15)"]
        database.write_mysql(file_name, df_amsua)
        print('done')

    @KiapsLogging.log_decorator
    def amsua_grqc(self, file_name):
        print(file_name)
        ddf_amsua_grqc = dd.read_table(self.config.get("DIRECTORY", "DATA_PATH") +
                                       "/amsua/" +
                                       self.config.get("GLOBAL", "FILE_DATE") +
                                       "/" +
                                       self.config.get('AMSUA', 'AMSUA_GRQC') % (self.config.get("GLOBAL", "FILE_DATE")),
            delim_whitespace=True, header=0)
        ddf_amsua_grqc["datetime"] = pd.to_datetime(self.config.get("GLOBAL", "FILE_DATE"), format="%Y%m%d%H%M%S")
        ddf_amsua_grqc = ddf_amsua_grqc[["datetime", "pixel", "lat", "lon", "irej", "qcflag", "seaice_indx", "scatt_indx", "clw", "ichk(4)", "tb(4)", "ichk(5)", "tb(5)", "ichk(6)", "tb(6)", "ichk(7)", "tb(7)", "ichk(8)", "tb(8)", "ichk(9)", "tb(9)", "ichk(10)", "tb(10)", "ichk(11)", "tb(11)", "ichk(12)", "tb(12)", "ichk(13)", "tb(13)", "ichk(14)", "tb(14)"]]
        database.write_mysql_dask(file_name, ddf_amsua_grqc)
        print('done')

    @KiapsLogging.log_decorator
    def amsua_xiv_before_thin(self, file_name):
        print(file_name)
        ddf_amsua_xiv_before_thin = dd.read_table(self.config.get("DIRECTORY", "DATA_PATH") +
                                                  "/amsua/" +
                                                  self.config.get("GLOBAL", "FILE_DATE") +
                                                  "/" +
                                                  self.config.get('AMSUA', 'AMSUA_XIV_BEFORE_THIN') % (self.config.get("GLOBAL", "FILE_DATE")),
            delim_whitespace=True, header=0)
        ddf_amsua_xiv_before_thin["datetime"] = pd.to_datetime(self.config.get("GLOBAL", "FILE_DATE"), format="%Y%m%d%H%M%S")
        ddf_amsua_xiv_before_thin = ddf_amsua_xiv_before_thin[["datetime", "i", "lat", "lon", "sfctype", "obstdif", "sat_id", "scanpos", "flag", "irej", "ob(4)", "cob(4)", "bk(4)", "ichk(4)", "ob(5)", "cob(5)", "bk(5)", "ichk(5)", "ob(6)", "cob(6)", "bk(6)", "ichk(6)", "ob(7)", "cob(7)", "bk(7)", "ichk(7)", "ob(8)", "cob(8)", "bk(8)", "ichk(8)", "ob(9)", "cob(9)", "bk(9)", "ichk(9)", "ob(10)", "cob(10)", "bk(10)", "ichk(10)", "ob(11)", "cob(11)", "bk(11)", "ichk(11)", "ob(12)", "cob(12)", "bk(12)", "ichk(12)", "ob(13)", "cob(13)", "bk(13)", "ichk(13)", "ob(14)", "cob(14)", "bk(14)", "ichk(14)"]]
        database.write_mysql_dask(file_name, ddf_amsua_xiv_before_thin)
        print('done')

    @KiapsLogging.log_decorator
    def amsua_xiv_ch(self, file_name):
        print(file_name)
        df_amsua_xiv_ch04 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH04') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch04.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch04["datetime"] = pd.to_datetime(df_amsua_xiv_ch04["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch04['kind'] = 4
        database.write_mysql(file_name, df_amsua_xiv_ch04)

        df_amsua_xiv_ch05 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH05') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch05.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch05["datetime"] = pd.to_datetime(df_amsua_xiv_ch05["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch05['kind'] = 5
        database.write_mysql(file_name, df_amsua_xiv_ch05)

        df_amsua_xiv_ch06 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH06') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch06.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch06["datetime"] = pd.to_datetime(df_amsua_xiv_ch06["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch06['kind'] = 6
        database.write_mysql(file_name, df_amsua_xiv_ch06)

        df_amsua_xiv_ch07 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH07') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch07.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch07["datetime"] = pd.to_datetime(df_amsua_xiv_ch07["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch07['kind'] = 7
        database.write_mysql(file_name, df_amsua_xiv_ch07)

        df_amsua_xiv_ch08 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH08') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch08.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch08["datetime"] = pd.to_datetime(df_amsua_xiv_ch08["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch08['kind'] = 8
        database.write_mysql(file_name, df_amsua_xiv_ch08)

        df_amsua_xiv_ch09 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH09') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch09.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch09["datetime"] = pd.to_datetime(df_amsua_xiv_ch09["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch09['kind'] = 9
        database.write_mysql(file_name, df_amsua_xiv_ch09)

        df_amsua_xiv_ch10 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH10') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch10.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch10["datetime"] = pd.to_datetime(df_amsua_xiv_ch10["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch10['kind'] = 10
        database.write_mysql(file_name, df_amsua_xiv_ch10)

        df_amsua_xiv_ch11 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH11') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch11.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch11["datetime"] = pd.to_datetime(df_amsua_xiv_ch11["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch11['kind'] = 11
        database.write_mysql(file_name, df_amsua_xiv_ch11)

        df_amsua_xiv_ch12 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH12') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch12.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch12["datetime"] = pd.to_datetime(df_amsua_xiv_ch12["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch12['kind'] = 12
        database.write_mysql(file_name, df_amsua_xiv_ch12)

        df_amsua_xiv_ch13 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH13') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch13.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch13["datetime"] = pd.to_datetime(df_amsua_xiv_ch13["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch13['kind'] = 13
        database.write_mysql(file_name, df_amsua_xiv_ch13)

        df_amsua_xiv_ch14 = pd.read_table(self.config.get("DIRECTORY", "DATA_PATH") + "/amsua/" + self.config.get("GLOBAL", "FILE_DATE") + "/" + self.config.get('AMSUA', 'AMSUA_XIV_CH14') % (self.config.get("GLOBAL", "FILE_DATE")), delim_whitespace=True, header=0)
        df_amsua_xiv_ch14.insert(0, 'datetime', self.config.get("GLOBAL", "FILE_DATE"))
        df_amsua_xiv_ch14["datetime"] = pd.to_datetime(df_amsua_xiv_ch14["datetime"], format="%Y%m%d%H%M%S")
        df_amsua_xiv_ch14['kind'] = 14
        database.write_mysql(file_name, df_amsua_xiv_ch14)
        print('done')
