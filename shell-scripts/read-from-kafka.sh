# $1 : topic
# $2 (optionnal) : --from-beginning 

/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server m1.adaltas.com:6667 --topic $1 $2
