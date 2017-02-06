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

package org.apache.flink.streaming.examples.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Read Strings from Kafka v0.9 and print them to standard out.
 * Note: On a cluster, DataStream.print() will print to the TaskManager's .out file!
 * <p>
 * Please pass the following arguments to run the example:
 * --topic test --bootstrap.servers localhost:9092 --group.id myconsumer
 */
public class ReadFromKafka09 {

	public static void main(String[] args) throws Exception {
		// parse and check input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		if (parameterTool.getNumberOfParameters() < 3) {
			System.out.println("Missing parameters!\n" +
				"Usage: Kafka --topic <topic> " +
				"--bootstrap.servers <kafka brokers> --group.id <some id>");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

		//get topic from parameters
		final String topic = parameterTool.getRequired("topic");
		final DataStreamSource messageStream = env
			.addSource(new FlinkKafkaConsumer09<>(
				topic,
				new SimpleStringSchema(),
				parameterTool.getProperties()));

		// write kafka stream to standard out.
		messageStream.print();

		env.execute("Read from Kafka v0.9 example");
	}
}
