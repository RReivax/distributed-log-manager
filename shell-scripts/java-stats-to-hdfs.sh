# $1 username
# $2 password
# $3 lenght of the interval to build stats on in seconds
#    (from now-$1 to now)
# $4 path in HDFS where to write the stats

hadoop jar `dirname $0`/../XG-Hive/target/XG-Hive-1.0-SNAPSHOT.jar main.ExportStats jdbc:hive2://m3.adaltas.com:10000/default $1 $2 $3 $4
