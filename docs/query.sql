SELECT partition_name, subpartition_name, table_rows FROM information_schema.partitions WHERE partition_method = 'RANGE' AND table_name = 'sonde_xiv';
SELECT * FROM sonde_xiv PARTITION (p2020);

SELECT Temp
FROM sonde_xiv
WHERE datetime BETWEEN STR_TO_DATE('2021-03-01 00:00:00', '%Y-%m-%d %H:%i:%s') AND STR_TO_DATE('2021-03-02 18:00:00', '%Y-%m-%d %H:%i:%s');

SELECT sx.datetime, AVG(sx.Temp), STDDEV(sx.Temp)
FROM sonde_xiv sx
WHERE sx.datetime >= STR_TO_DATE('2020-03-01 00:00:00', '%Y-%m-%d %H:%i:%s')
AND sx.datetime < STR_TO_DATE('2020-04-01 00:00:00', '%Y-%m-%d %H:%i:%s')
AND sx.Temp != -999.99
GROUP BY sx.datetime;

SELECT *
FROM sonde_xiv sx, surface_LATE sl
WHERE sx.datetime = sl.hdr_datetime
AND sx.datetime > STR_TO_DATE('2020-03-01 00:00:00', '%Y-%m-%d %H:%i:%s')
AND sl.hdr_datetime < STR_TO_DATE('2020-04-01 00:00:00', '%Y-%m-%d %H:%i:%s');

------------------------------------------------------------------------------------------------------------------------

SELECT ObsTime, AVG(Pressure), stddevPop(Pressure)--, stddevSamp(Pressure)
FROM sonde
GROUP BY ObsTime;

SELECT sx.datetime, AVG(sx.Temp), stddevPop(sx.Temp)--, stddevSamp(sx.Temp)
FROM sonde_xiv sx
WHERE sx.datetime >= toDateTime('2020-03-01 00:00:00')
AND sx.datetime < toDateTime('2020-04-01 00:00:00')
AND sx.Temp != -999.99
GROUP BY sx.datetime;

SELECT *
FROM sonde_xiv sx
JOIN surface_LATE sl
ON sx.datetime = sl.hdr_datetime
WHERE sx.datetime >= toDateTime('2020-03-01 00:00:00')
AND sx.datetime < toDateTime('2020-04-01 00:00:00');