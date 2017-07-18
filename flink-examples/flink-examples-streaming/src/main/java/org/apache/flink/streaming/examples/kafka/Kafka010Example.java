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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


/**
 * An example that shows how to read from and write to Kafka. This will read String messages
 * from the input topic, prefix them by a configured prefix and output to the output topic.
 *
 * <p>Example usage:
 * 	--input-topic test-input --output-topic test-output --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 */
public class Kafka010Example {

	public static void main(String[] args) throws Exception {
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 5) {
			System.out.println("Missing parameters!\n" +
					"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
					"--bootstrap.servers <kafka brokers> " +
					"--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]");
			return;
		}

		String prefix = parameterTool.get("prefix", "PREFIX:");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> input = env
				.addSource(new FlinkKafkaConsumer010<>(
						parameterTool.getRequired("input-topic"),
						new SimpleStringSchema(),
						parameterTool.getProperties()))
				.map(new PrefixingMapper(prefix));

		input.addSink(
				new FlinkKafkaProducer010<>(
						parameterTool.getRequired("output-topic"),
						new SimpleStringSchema(),
						parameterTool.getProperties()));

		env.execute("Kafka 0.10 Example");
	}

	private static class PrefixingMapper implements MapFunction<String, String> {
		private final String prefix;

		public PrefixingMapper(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public String map(String value) throws Exception {
			return prefix + value;
		}
	}
}
