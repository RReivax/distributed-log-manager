import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaToHbase {
	static int rowkey_index;

	public static Put strLogToHPut(String log){
		Pattern pattern = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\] \"([^\"]*)\" ([^ ]*) ([^ ]*)");
		Matcher matcher = pattern.matcher(log);
		Put out = new Put(Bytes.toBytes(Integer.toString(++rowkey_index)));

		if(matcher.matches()) {
			if(!"-".equals(matcher.group(1))) out.addColumn(Bytes.toBytes("user"), Bytes.toBytes("IP"), Bytes.toBytes(matcher.group(1)));
			if(!"-".equals(matcher.group(2))) out.addColumn(Bytes.toBytes("user"), Bytes.toBytes("user-identifier"), Bytes.toBytes(matcher.group(2)));
			if(!"-".equals(matcher.group(3))) out.addColumn(Bytes.toBytes("user"), Bytes.toBytes("userid"), Bytes.toBytes(matcher.group(3)));
			if(!"-".equals(matcher.group(4))) out.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(matcher.group(4)));
			if(!"-".equals(matcher.group(5))) out.addColumn(Bytes.toBytes("info"), Bytes.toBytes("request"), Bytes.toBytes(matcher.group(5)));
			if(!"-".equals(matcher.group(6))) out.addColumn(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes(matcher.group(6)));
			if(!"-".equals(matcher.group(7))) out.addColumn(Bytes.toBytes("info"), Bytes.toBytes("size"), Bytes.toBytes(matcher.group(7)));
		}
		return out;
	}

    public static void main(String[] args) {
	
        String tableName = "";
		String topicName = "";

		try {
			if(args.length != 2){
				throw new Exception("Incorrect number of arguments\nUsage: hadoop jar [..].jar " +
                        		    "<kafka_topic> <hbase_table>");
        	}	
			topicName = args[0];
			tableName = args[1];
		} catch (Exception e) {
			System.out.print(e);
			System.exit(1);
    	}

		/*
		 * HBase configuration
		 */
		Configuration config = HBaseConfiguration.create();
       	config.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
	    Connection connection = null;
		rowkey_index = 0;

		/*
		 * Kafka configuration
		 */
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "m1.adaltas.com:6667");
		properties.put("zookeeper.connect", "m1.adlatas.com:2181");
		properties.put("key.deserializer", StringDeserializer.class.getName());
		properties.put("group.id", "xg-hbase-consumer");
		properties.put("value.deserializer", StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		consumer.subscribe(Arrays.asList("xg_formatted"));


		try {
			connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf(tableName));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records)
				{
					System.out.printf("offset = %d, key = %s, value = %s\n",
							record.offset(), record.key(), record.value());

					Put mylog = strLogToHPut(record.value());
					if(!mylog.isEmpty()) table.put(mylog);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
