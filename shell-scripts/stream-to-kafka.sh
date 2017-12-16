# $1 : File to stream
# $2 : Topic

while read p; do
	echo $p ;
	sleep 2  ;
done < $1 | /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh  --topic $2 --broker-list m1.adaltas.com:6667
