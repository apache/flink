package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * 
 * @author zhangjun
 * @date 2018年12月10日 下午3:29:41
 */
public class Kafka2Hdfs {


	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(5000);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("test3", new SimpleStringSchema(),
				properties);
		myConsumer.setStartFromLatest();


		StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path("hdfs://localhost/tmp/flink-data/json"), new SimpleStringEncoder<String>("UTF-8"))
				.build();

		env.addSource(myConsumer).addSink(sink);
		env.execute();
	}

}
