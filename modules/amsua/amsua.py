from __future__ import division
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
        ddf_amsua_grqc["datetime"] = pd.to_datetime(self.config.get("GLOBAL", "FILE_DATE")+'00', format="%Y%m%d%H%M%S")
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

    @KiapsLogging.log_decorator
    def amsua_innoqc(self, file_name):
        print(file_name)
        ddf_amsua_innoqc = dd.read_table(self.config.get("DIRECTORY", "DATA_PATH") +
                                          "/amsua/" +
                                         self.config.get("GLOBAL", "FILE_DATE") +
                                          "/" +
                                         self.config.get('AMSUA', 'AMSUA_INNQC') % (self.config.get("GLOBAL", "FILE_DATE")),
            delim_whitespace=True, header=0)
        ddf_amsua_innoqc["datetime"] = pd.to_datetime(self.config.get("GLOBAL", "FILE_DATE")+'00', format="%Y%m%d%H%M%S")

        df1 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'isat', 'bpos', 'irej', 'QCflag', 'sfctype', 'obstdif', 'ob(01)', 'cob(01)', 'bk(01)', 'ck(01)']]
        database.write_mysql_dask('amsua_innoqc_ch1', df1)

        df2 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(02)', 'cob(02)', 'bk(02)', 'ck(02)']]
        database.write_mysql_dask('amsua_innoqc_ch2', df2)

        df3 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(03)', 'cob(03)', 'bk(03)', 'ck(03)']]
        database.write_mysql_dask('amsua_innoqc_ch3', df3)

        df4 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(04)', 'cob(04)', 'bk(04)', 'ck(04)']]
        database.write_mysql_dask('amsua_innoqc_ch4', df4)

        df5 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(05)', 'cob(05)', 'bk(05)', 'ck(05)']]
        database.write_mysql_dask('amsua_innoqc_ch5', df5)

        df6 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(06)', 'cob(06)', 'bk(06)', 'ck(06)']]
        database.write_mysql_dask('amsua_innoqc_ch6', df6)

        df7 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(07)', 'cob(07)', 'bk(07)', 'ck(07)']]
        database.write_mysql_dask('amsua_innoqc_ch7', df7)

        df8 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(08)', 'cob(08)', 'bk(08)', 'ck(08)']]
        database.write_mysql_dask('amsua_innoqc_ch8', df8)

        df9 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(09)', 'cob(09)', 'bk(09)', 'ck(09)']]
        database.write_mysql_dask('amsua_innoqc_ch9', df9)

        df10 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(10)', 'cob(10)', 'bk(10)', 'ck(10)']]
        database.write_mysql_dask('amsua_innoqc_ch10', df10)

        df11 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(11)', 'cob(11)', 'bk(11)', 'ck(11)']]
        database.write_mysql_dask('amsua_innoqc_ch11', df11)

        df12 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(12)', 'cob(12)', 'bk(12)', 'ck(12)']]
        database.write_mysql_dask('amsua_innoqc_ch12', df12)

        df13 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(13)', 'cob(13)', 'bk(13)', 'ck(13)']]
        database.write_mysql_dask('amsua_innoqc_ch13', df13)

        df14 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(14)', 'cob(14)', 'bk(14)', 'ck(14)']]
        database.write_mysql_dask('amsua_innoqc_ch14', df14)

        df15 = ddf_amsua_innoqc[['datetime', 'iobs', 'lat', 'lon', 'irej', 'QCflag', 'ob(15)', 'cob(15)', 'bk(15)', 'ck(15)']]
        database.write_mysql_dask('amsua_innoqc_ch15', df15)

        print("done")