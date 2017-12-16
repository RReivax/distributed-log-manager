# $1 HDFS Folder to store the export
time=$(date +%d-%m-%Y_%H-%M)
end="/"
path="$1/archive_$time$end"

hbase org.apache.hadoop.hbase.mapreduce.Export xg_logs $path
hadoop fs -copyToLocal $path "archive_$time"
tar -zcvf "archive_$time.gz" "archive_$time"
hadoop fs -put "archive_$time.gz" "$1/archive_$time.gz"
hadoop fs -rm -r -skipTrash -f $path
rm -r "archive_$time"
rm -r "archive_$time.gz"


