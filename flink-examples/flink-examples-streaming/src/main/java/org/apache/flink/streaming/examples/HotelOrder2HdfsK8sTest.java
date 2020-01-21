package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 酒店的订单数据，从kafka消费，然后放入redis，同时备份hdfs一份
 * {"cardNo":240000000764742642,"checkInDate":"2019-09-02 00:00:00","checkOutDate":"2019-09-03 00:00:00","cityId":"0101","cityName":"北京","createTime":"2019-09-02 14:59:23","mhotelId":"90183840","orderId":590788702,"payStatus":3,"platform":"wx","roomNightsCount":1,"status":"V","sumPrice":170.0,"unionId":"ohmdTt5jJkPsdRbYOL38K4_OCkg0","wechatOpenId":"o498X0VtQ4_TjOHNTo5mDr5iHUCU"} .
 * /home/work/software/flink/bin/flink run  -p 1  -c com.ly.ad.backup.HotelOrder2HdfsK8sTest -d -m yarn-cluster -ynm HotelOrder2HdfsK8sTest   /home/work/zhangjun/git/flink-jobs4bj/flink-stream-jobs/target/flink-stream-jobs-1.0.jar hdfs://hadoopcluster/tmp/zhangjun/hotel_order_tmp_k8stest/
 */
public class HotelOrder2HdfsK8sTest{

	private static final Logger LOGGER = LoggerFactory.getLogger(HotelOrder2HdfsK8sTest.class);
	private static final String kafkaAddress = "kafka-074-010.bigdata.ly:18850,kafka-074-011.bigdata.ly:18850,kafka-074-012.bigdata.ly:18850,kafka-074-013.bigdata.ly:18850,kafka-074-024.bigdata.ly:18850,kafka-074-025.bigdata.ly:18850";
	private static final String kafkaTopic = "OTHER_hotel_order";
	private static final String kafkaGroupId = "HotelOrder2HdfsK8sTest";

	public static void main(String[] args) throws Exception{

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(300000);
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		if (args.length < 1){
			throw new Exception("args length < 1");
		}

		String hdfsDataPath = args[0];

		Properties properties = new Properties();
		properties.setProperty(
			"bootstrap.servers",
			kafkaAddress);
		properties.setProperty("group.id", kafkaGroupId);

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(
			kafkaTopic, new SimpleStringSchema(),
			properties);
		DataStream<String> dataStream = env.addSource(myConsumer, "fromKafka");

		DataStream<JSONObject> jsonStream = dataStream.map(
			(MapFunction<String, JSONObject>) value -> JSONObject.parseObject(value));

		//数据备份到hdfs一份
		BucketingSink<String> hdfsSink = new BucketingSink<>(hdfsDataPath);
		hdfsSink.setBucketer(new DateTimeBucketer<>("yyyyMMddHH", ZoneId.of("UTC+8")));
		hdfsSink.setWriter(new StringWriter<>());
		hdfsSink.setInactiveBucketThreshold(30 * 1000L);

		//write to hdfs
		byte[] b1 = {0x01};
		final String str = new String(b1);
		jsonStream.map(new MapFunction<JSONObject, String>(){

			String[] fields = new String[]{
				"appDeviceId",
				"cardNo",
				"checkInDate",
				"checkOutDate",
				"cityId",
				"cityName",
				"createTime",
				"mhotelId",
				"orderId",
				"payStatus",
				"platform",
				"roomNightsCount",
				"status",
				"sumPrice",
				"unionId",
				"wechatOpenId",
				"innerFrom"
			};

			@Override
			public String map(JSONObject jsonObject) throws Exception{

				List result = new ArrayList();
				for (String field: fields){
					Object v = jsonObject.get(field);
					if (v == null){
						result.add("");
					} else {
						result.add(v);
					}
				}
				String res = StringUtils.join(result, str);
				return res;
			}
		}).addSink(hdfsSink).setParallelism(1).name("writeHdfs");

		env.execute("HotelOrder2HdfsK8sTest");
	}

}
