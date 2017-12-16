# $1 lenght of the interval to build stats on in seconds
#    (from now-$1 to now)
# $2 path in HDFS where to write the stats

time=$(date +%d-%m-%Y_%H-%M)
end="/"
path="$2/stats_$time$end"

hive -hiveconf interval=$1 -hiveconf exp_path=$path -f `dirname $0`/../hive-scripts/hdfs-export-stats.hql
