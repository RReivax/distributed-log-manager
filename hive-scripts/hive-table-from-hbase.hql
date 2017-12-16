CREATE EXTERNAL TABLE xg_hbase_logs(rowkey INT, IP STRING, user_identifier STRING, user_id STRING, ts STRING, 
request STRING, status INT, size INT, hbase_ts TIMESTAMP)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, user:IP, user:user-identifier, user:userid, info:date, 
info:request, info:status, info:size, :timestamp')
TBLPROPERTIES('hbase.table.name' = 'xg_logs');
