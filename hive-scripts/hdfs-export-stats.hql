WITH log_time AS (SELECT * FROM xg_hbase_logs WHERE hbase_ts >= (unix_timestamp()-${hiveconf:interval})), 
req1 AS (SELECT request, ip FROM log_time WHERE status >= 300 AND status < 400), 
req2 AS (SELECT request, ip FROM log_time WHERE status >= 400 AND status < 500), 
req3 AS (SELECT request, ip FROM log_time WHERE status >= 500), 
req4 AS (SELECT request, ip, size FROM log_time) 
INSERT OVERWRITE DIRECTORY '${hiveconf:exp_path}' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
SELECT COALESCE(req1.ip, req2.ip, req3.ip, req4.ip), count(req4.request), avg(req4.size), 
count(req1.request), count(req2.request), count(req3.request) 
FROM req1 
FULL JOIN req2 ON req1.ip = req2.ip 
FULL JOIN req3 ON req3.ip = COALESCE(req1.ip, req2.ip) 
FULL JOIN req4 ON req4.ip = COALESCE(req1.ip, req2.ip, req3.ip)
GROUP BY req1.ip, req2.ip, req3.ip, req4.ip;
