CREATE TABLE `sonde_xiv` (
  `datetime` datetime NOT NULL,
  `seq` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `tobs` int,
  `StnID` int,
  `obtype` varchar(50),
  `nlev` int,
  `lat` double,
  `lon` double,
  `StnHgt` double,
  `TimeDiff` double,
  `lev` int,
  `Pressure` double,
  `Temp` double,
  `Temp_flag` double,
  `Temp_OmB` double,
  `U-comp` double,
  `U-comp_flag` double,
  `U-comp_OmB` double,
  `V-comp` double,
  `V-comp_flag` double,
  `V-comp_OmB` double,
  `RH` double,
  `RH_flag` double,
  `RH_OmB` double,
  `Q` double,
  `Q_flag` double,
  `Q_OmB` double,
  `Mflag` double,
  `avail` double,
  PRIMARY KEY (`datetime`, `seq`),
  KEY `sonde_xiv_datetime_IDX` (`datetime`),
  KEY `sonde_xiv_seq_IDX` (`seq`),
  KEY `sonde_xiv_StnID_IDX` (`StnID`),
  KEY `sonde_xiv_lat_IDX` (`lat`),
  KEY `sonde_xiv_lon_IDX` (`lon`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (YEAR(`datetime`))
SUBPARTITION BY HASH (MONTH(`datetime`))
SUBPARTITIONS 12 (
    PARTITION p1970 VALUES LESS THAN (1971) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1970',
    PARTITION p1971 VALUES LESS THAN (1972) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1971',
    PARTITION p1972 VALUES LESS THAN (1973) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1972',
    PARTITION p1973 VALUES LESS THAN (1974) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1973',
    PARTITION p1974 VALUES LESS THAN (1975) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1974',
    PARTITION p1975 VALUES LESS THAN (1976) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1975',
    PARTITION p1976 VALUES LESS THAN (1977) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1976',
    PARTITION p1977 VALUES LESS THAN (1978) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1977',
    PARTITION p1978 VALUES LESS THAN (1979) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1978',
    PARTITION p1979 VALUES LESS THAN (1980) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1979',
    PARTITION p1980 VALUES LESS THAN (1981) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1980',
    PARTITION p1981 VALUES LESS THAN (1982) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1981',
    PARTITION p1982 VALUES LESS THAN (1983) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1982',
    PARTITION p1983 VALUES LESS THAN (1984) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1983',
    PARTITION p1984 VALUES LESS THAN (1985) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1984',
    PARTITION p1985 VALUES LESS THAN (1986) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1985',
    PARTITION p1986 VALUES LESS THAN (1987) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1986',
    PARTITION p1987 VALUES LESS THAN (1988) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1987',
    PARTITION p1988 VALUES LESS THAN (1989) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1988',
    PARTITION p1989 VALUES LESS THAN (1990) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1989',
    PARTITION p1990 VALUES LESS THAN (1991) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1990',
    PARTITION p1991 VALUES LESS THAN (1992) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1991',
    PARTITION p1992 VALUES LESS THAN (1993) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1992',
    PARTITION p1993 VALUES LESS THAN (1994) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1993',
    PARTITION p1994 VALUES LESS THAN (1995) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1994',
    PARTITION p1995 VALUES LESS THAN (1996) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1995',
    PARTITION p1996 VALUES LESS THAN (1997) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1996',
    PARTITION p1997 VALUES LESS THAN (1998) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1997',
    PARTITION p1998 VALUES LESS THAN (1999) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1998',
    PARTITION p1999 VALUES LESS THAN (2000) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p1999',
    PARTITION p2000 VALUES LESS THAN (2001) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2000',
    PARTITION p2001 VALUES LESS THAN (2002) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2001',
    PARTITION p2002 VALUES LESS THAN (2003) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2002',
    PARTITION p2003 VALUES LESS THAN (2004) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2003',
    PARTITION p2004 VALUES LESS THAN (2005) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2004',
    PARTITION p2005 VALUES LESS THAN (2006) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2005',
    PARTITION p2006 VALUES LESS THAN (2007) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2006',
    PARTITION p2007 VALUES LESS THAN (2008) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2007',
    PARTITION p2008 VALUES LESS THAN (2009) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2008',
    PARTITION p2009 VALUES LESS THAN (2010) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2009',
    PARTITION p2010 VALUES LESS THAN (2011) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2010',
    PARTITION p2011 VALUES LESS THAN (2012) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2011',
    PARTITION p2012 VALUES LESS THAN (2013) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2012',
    PARTITION p2013 VALUES LESS THAN (2014) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2013',
    PARTITION p2014 VALUES LESS THAN (2015) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2014',
    PARTITION p2015 VALUES LESS THAN (2016) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2015',
    PARTITION p2016 VALUES LESS THAN (2017) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2016',
    PARTITION p2017 VALUES LESS THAN (2018) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2017',
    PARTITION p2018 VALUES LESS THAN (2019) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2018',
    PARTITION p2019 VALUES LESS THAN (2020) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2019',
    PARTITION p2020 VALUES LESS THAN (2021) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2020',
    PARTITION p2021 VALUES LESS THAN (2022) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2021',
    PARTITION p2022 VALUES LESS THAN (2023) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2022',
    PARTITION p2023 VALUES LESS THAN (2024) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2023',
    PARTITION p2024 VALUES LESS THAN (2025) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2024',
    PARTITION p2025 VALUES LESS THAN (2026) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2025',
    PARTITION p2026 VALUES LESS THAN (2027) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2026',
    PARTITION p2027 VALUES LESS THAN (2028) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2027',
    PARTITION p2028 VALUES LESS THAN (2029) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2028',
    PARTITION p2029 VALUES LESS THAN (2030) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2029',
    PARTITION p2030 VALUES LESS THAN (2031) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2030',
    PARTITION p2031 VALUES LESS THAN (2032) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2031',
    PARTITION p2032 VALUES LESS THAN (2033) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2032',
    PARTITION p2033 VALUES LESS THAN (2034) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2033',
    PARTITION p2034 VALUES LESS THAN (2035) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2034',
    PARTITION p2035 VALUES LESS THAN (2036) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2035',
    PARTITION p2036 VALUES LESS THAN (2037) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2036',
    PARTITION p2037 VALUES LESS THAN (2038) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2037',
    PARTITION p2038 VALUES LESS THAN (2039) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2038',
    PARTITION p2039 VALUES LESS THAN (2040) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2039',
    PARTITION p2040 VALUES LESS THAN (2041) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2040',
    PARTITION p2041 VALUES LESS THAN (2042) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2041',
    PARTITION p2042 VALUES LESS THAN (2043) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2042',
    PARTITION p2043 VALUES LESS THAN (2044) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2043',
    PARTITION p2044 VALUES LESS THAN (2045) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2044',
    PARTITION p2045 VALUES LESS THAN (2046) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2045',
    PARTITION p2046 VALUES LESS THAN (2047) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2046',
    PARTITION p2047 VALUES LESS THAN (2048) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2047',
    PARTITION p2048 VALUES LESS THAN (2049) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2048',
    PARTITION p2049 VALUES LESS THAN (2050) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p2049',
    PARTITION pmax VALUES LESS THAN MAXVALUE DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/pmax'
);

ALTER TABLE `sonde_xiv`
PARTITION BY RANGE (MONTH(`datetime`))
SUBPARTITION BY HASH (DAY(`datetime`))
SUBPARTITIONS 31 (
    PARTITION p01 VALUES LESS THAN (02) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p01',
    PARTITION p02 VALUES LESS THAN (03) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p02',
    PARTITION p03 VALUES LESS THAN (04) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p03',
    PARTITION p04 VALUES LESS THAN (05) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p04',
    PARTITION p05 VALUES LESS THAN (06) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p05',
    PARTITION p06 VALUES LESS THAN (07) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p06',
    PARTITION p07 VALUES LESS THAN (08) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p07',
    PARTITION p08 VALUES LESS THAN (09) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p08',
    PARTITION p09 VALUES LESS THAN (10) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p09',
    PARTITION p10 VALUES LESS THAN (11) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p10',
    PARTITION p11 VALUES LESS THAN (12) DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p11',
    PARTITION p12 VALUES LESS THAN MAXVALUE DATA DIRECTORY = '/home/kiaps/par/data/sonde_xiv/p12'
);

CREATE TABLE `surface_LATE` (
  `hdr_datetime` datetime NOT NULL,
  `seq` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `obstype` varchar(50),
  `hdr_ops_subtype` int,
  `hdr_statid` varchar(50),
  `hdr_lat` double,
  `hdr_lon` double,
  `hdr_varno` int,
  `body_initial_obsvalue` double,
  `body_bgvalue` double,
  `body_fg_depar` double,
  `body_ops_datum_flags_b0` int,
  PRIMARY KEY (`hdr_datetime`, `seq`),
  KEY `surface_LATE_hdr_datetime_IDX` (`hdr_datetime`),
  KEY `surface_LATE_seq_IDX` (`seq`),
  KEY `surface_LATE_hdr_statid_IDX` (`hdr_statid`),
  KEY `surface_LATE_hdr_lat_IDX` (`hdr_lat`),
  KEY `surface_LATE_hdr_lon_IDX` (`hdr_lon`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (YEAR(`hdr_datetime`))
SUBPARTITION BY HASH (MONTH(`hdr_datetime`))
SUBPARTITIONS 12 (
    PARTITION p1970 VALUES LESS THAN (1971) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1970',
    PARTITION p1971 VALUES LESS THAN (1972) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1971',
    PARTITION p1972 VALUES LESS THAN (1973) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1972',
    PARTITION p1973 VALUES LESS THAN (1974) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1973',
    PARTITION p1974 VALUES LESS THAN (1975) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1974',
    PARTITION p1975 VALUES LESS THAN (1976) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1975',
    PARTITION p1976 VALUES LESS THAN (1977) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1976',
    PARTITION p1977 VALUES LESS THAN (1978) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1977',
    PARTITION p1978 VALUES LESS THAN (1979) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1978',
    PARTITION p1979 VALUES LESS THAN (1980) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1979',
    PARTITION p1980 VALUES LESS THAN (1981) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1980',
    PARTITION p1981 VALUES LESS THAN (1982) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1981',
    PARTITION p1982 VALUES LESS THAN (1983) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1982',
    PARTITION p1983 VALUES LESS THAN (1984) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1983',
    PARTITION p1984 VALUES LESS THAN (1985) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1984',
    PARTITION p1985 VALUES LESS THAN (1986) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1985',
    PARTITION p1986 VALUES LESS THAN (1987) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1986',
    PARTITION p1987 VALUES LESS THAN (1988) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1987',
    PARTITION p1988 VALUES LESS THAN (1989) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1988',
    PARTITION p1989 VALUES LESS THAN (1990) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1989',
    PARTITION p1990 VALUES LESS THAN (1991) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1990',
    PARTITION p1991 VALUES LESS THAN (1992) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1991',
    PARTITION p1992 VALUES LESS THAN (1993) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1992',
    PARTITION p1993 VALUES LESS THAN (1994) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1993',
    PARTITION p1994 VALUES LESS THAN (1995) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1994',
    PARTITION p1995 VALUES LESS THAN (1996) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1995',
    PARTITION p1996 VALUES LESS THAN (1997) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1996',
    PARTITION p1997 VALUES LESS THAN (1998) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1997',
    PARTITION p1998 VALUES LESS THAN (1999) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1998',
    PARTITION p1999 VALUES LESS THAN (2000) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p1999',
    PARTITION p2000 VALUES LESS THAN (2001) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2000',
    PARTITION p2001 VALUES LESS THAN (2002) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2001',
    PARTITION p2002 VALUES LESS THAN (2003) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2002',
    PARTITION p2003 VALUES LESS THAN (2004) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2003',
    PARTITION p2004 VALUES LESS THAN (2005) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2004',
    PARTITION p2005 VALUES LESS THAN (2006) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2005',
    PARTITION p2006 VALUES LESS THAN (2007) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2006',
    PARTITION p2007 VALUES LESS THAN (2008) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2007',
    PARTITION p2008 VALUES LESS THAN (2009) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2008',
    PARTITION p2009 VALUES LESS THAN (2010) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2009',
    PARTITION p2010 VALUES LESS THAN (2011) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2010',
    PARTITION p2011 VALUES LESS THAN (2012) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2011',
    PARTITION p2012 VALUES LESS THAN (2013) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2012',
    PARTITION p2013 VALUES LESS THAN (2014) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2013',
    PARTITION p2014 VALUES LESS THAN (2015) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2014',
    PARTITION p2015 VALUES LESS THAN (2016) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2015',
    PARTITION p2016 VALUES LESS THAN (2017) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2016',
    PARTITION p2017 VALUES LESS THAN (2018) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2017',
    PARTITION p2018 VALUES LESS THAN (2019) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2018',
    PARTITION p2019 VALUES LESS THAN (2020) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2019',
    PARTITION p2020 VALUES LESS THAN (2021) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2020',
    PARTITION p2021 VALUES LESS THAN (2022) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2021',
    PARTITION p2022 VALUES LESS THAN (2023) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2022',
    PARTITION p2023 VALUES LESS THAN (2024) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2023',
    PARTITION p2024 VALUES LESS THAN (2025) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2024',
    PARTITION p2025 VALUES LESS THAN (2026) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2025',
    PARTITION p2026 VALUES LESS THAN (2027) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2026',
    PARTITION p2027 VALUES LESS THAN (2028) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2027',
    PARTITION p2028 VALUES LESS THAN (2029) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2028',
    PARTITION p2029 VALUES LESS THAN (2030) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2029',
    PARTITION p2030 VALUES LESS THAN (2031) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2030',
    PARTITION p2031 VALUES LESS THAN (2032) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2031',
    PARTITION p2032 VALUES LESS THAN (2033) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2032',
    PARTITION p2033 VALUES LESS THAN (2034) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2033',
    PARTITION p2034 VALUES LESS THAN (2035) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2034',
    PARTITION p2035 VALUES LESS THAN (2036) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2035',
    PARTITION p2036 VALUES LESS THAN (2037) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2036',
    PARTITION p2037 VALUES LESS THAN (2038) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2037',
    PARTITION p2038 VALUES LESS THAN (2039) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2038',
    PARTITION p2039 VALUES LESS THAN (2040) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2039',
    PARTITION p2040 VALUES LESS THAN (2041) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2040',
    PARTITION p2041 VALUES LESS THAN (2042) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2041',
    PARTITION p2042 VALUES LESS THAN (2043) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2042',
    PARTITION p2043 VALUES LESS THAN (2044) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2043',
    PARTITION p2044 VALUES LESS THAN (2045) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2044',
    PARTITION p2045 VALUES LESS THAN (2046) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2045',
    PARTITION p2046 VALUES LESS THAN (2047) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2046',
    PARTITION p2047 VALUES LESS THAN (2048) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2047',
    PARTITION p2048 VALUES LESS THAN (2049) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2048',
    PARTITION p2049 VALUES LESS THAN (2050) DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/p2049',
    PARTITION pmax VALUES LESS THAN MAXVALUE DATA DIRECTORY = '/home/kiaps/par/data/surface_LATE/pmax'
);

------------------------------------------------------------------------------------------------------------------------

CREATE TABLE `sonde_xiv` (
  `datetime` datetime,
  `tobs` UInt64,
  `StnID` UInt64,
  `obtype` String,
  `nlev` UInt64,
  `lat` Float64,
  `lon` Float64,
  `StnHgt` Float64,
  `TimeDiff` Float64,
  `lev` UInt64,
  `Pressure` Float64,
  `Temp` Float64,
  `Temp_flag` Float64,
  `Temp_OmB` Float64,
  `U-comp` Float64,
  `U-comp_flag` Float64,
  `U-comp_OmB` Float64,
  `V-comp` Float64,
  `V-comp_flag` Float64,
  `V-comp_OmB` Float64,
  `RH` Float64,
  `RH_flag` Float64,
  `RH_OmB` Float64,
  `Q` Float64,
  `Q_flag` Float64,
  `Q_OmB` Float64,
  `Mflag` Float64,
  `avail` Float64
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(datetime)
ORDER BY (seq, StnID);

CREATE TABLE `surface_LATE` (
  `hdr_datetime` datetime,
  `obstype` String,
  `hdr_ops_subtype` UInt64,
  `hdr_statid` String,
  `hdr_lat` Float64,
  `hdr_lon` Float64,
  `hdr_varno` UInt64,
  `body_initial_obsvalue` Float64,
  `body_bgvalue` Float64,
  `body_fg_depar` Float64,
  `body_ops_datum_flags_b0` UInt64
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(hdr_datetime)
ORDER BY (hdr_statid);