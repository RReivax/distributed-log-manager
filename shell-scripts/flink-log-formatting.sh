# $1 : Row log topic to read from
# $2 : Formatted log topic to write to

/usr/local/flink/bin/flink run -c main.KafkaLogFormatting `dirname $0`/../XG-Flink/target/XG-Flink-1.0-SNAPSHOT.jar --cTopic $1 --zookeeper.connect m1.adaltas.com:2181 --bootstrap.servers m1.adaltas.com:6667 --cons.group.id flink --pTopic $2
