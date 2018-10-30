package org.apache.flink.streaming.connectors.dynamodbstreams.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

/**
 * Sample command-line program of consuming data from a single DynamoDB stream.
 */
public class ConsumeFromDynamodbStreams {
	private static final String DYNAMODB_STREAM_NAME = "stream";

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		Properties dynamodbStreamsConsumerConfig = new Properties();
		final String streamName = pt.getRequired(DYNAMODB_STREAM_NAME);
		dynamodbStreamsConsumerConfig.setProperty(
				ConsumerConfigConstants.AWS_REGION, pt.getRequired("region"));
		dynamodbStreamsConsumerConfig.setProperty(
				ConsumerConfigConstants.AWS_ACCESS_KEY_ID, pt.getRequired("accesskey"));
		dynamodbStreamsConsumerConfig.setProperty(
				ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, pt.getRequired("secretkey"));

		DataStream<String> dynamodbStreams = see.addSource(new FlinkKinesisConsumer<>(
				streamName,
				new SimpleStringSchema(),
				dynamodbStreamsConsumerConfig));

		dynamodbStreams.print();

		see.execute();
	}

}
