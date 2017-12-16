package main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;


public class KafkaLogFormatting {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Get required parameters
        ParameterTool pTool = ParameterTool.fromArgs(args);
        Properties cProps = new Properties();
        Properties pProps = new Properties();
        cProps.setProperty("zookeeper.connect", pTool.getRequired("zookeeper.connect"));
        cProps.setProperty("bootstrap.servers", pTool.getRequired("bootstrap.servers"));
        pProps.setProperty("bootstrap.servers", pTool.getRequired("bootstrap.servers"));
        cProps.setProperty("group.id", pTool.getRequired("cons.group.id"));

        // Create DataStream from Kafka topic
        DataStream<String> consumer = env.addSource(new FlinkKafkaConsumer010<>(
                pTool.getRequired("cTopic"),
                new SimpleStringSchema(),
                cProps));

        // Map each String into a CommonLog
        DataStream<CommonLog> formatted_log = consumer.map(new MapFunction<String, CommonLog>() {
           @Override
           public CommonLog map(String log) throws Exception{
               return CommonLog.fromString(log);
           }
        });

        // Send the CommonLogs to the second Kafka topic
        formatted_log.addSink(new FlinkKafkaProducer010<>(
                pTool.getRequired("pTopic"),       // target topic
                new CommonLogSchema(),               // serialization schema
                pProps));

        // Execute program
        env.execute("Kafka Log Formatting (Hermand_Leonard)");
    }

    // Used for serialization and deserialization of the logs when writing to/reading from topics
    public static class CommonLogSchema implements DeserializationSchema<CommonLog>, SerializationSchema<CommonLog> {
        private static final long serialVersionUID = 1L;

        public CommonLogSchema() {
        }

        public CommonLog deserialize(byte[] message) {
            return CommonLog.fromString(new String(message));
        }

        public boolean isEndOfStream(CommonLog nextElement) {
            return false;
        }

        public byte[] serialize(CommonLog log) {
            return log.toString().getBytes();
        }

        public TypeInformation<CommonLog> getProducedType() {
            return TypeExtractor.getForClass(CommonLog.class);
        }
    }
}
