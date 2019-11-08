/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkDynamoDBStreamsConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

/**
 * Sample command-line program of consuming data from a single DynamoDB stream.
 */
public class ConsumeFromDynamoDBStreams {
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

		DataStream<String> dynamodbStreams = see.addSource(new FlinkDynamoDBStreamsConsumer<>(
				streamName,
				new SimpleStringSchema(),
				dynamodbStreamsConsumerConfig));

		dynamodbStreams.print();

		see.execute();
	}

}
