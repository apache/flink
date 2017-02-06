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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Generate a String every 500 ms and write it into a Kafka v0.9 topic
 * <p>
 * Please pass the following arguments to run the example:
 * --topic test --bootstrap.servers localhost:9092
 */
public class WriteIntoKafka09 {

	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		if (parameterTool.getNumberOfParameters() < 2) {
			System.out.println("Missing parameters!\n" +
				"Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

		// very simple data generator
		final DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 6369260445318862378L;
			boolean running = true;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				long i = 0;
				while (this.running) {
					ctx.collect("Element - " + i++);
					Thread.sleep(500);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		//get topic from parameters
		final String topic = parameterTool.getRequired("topic");
		// write data into Kafka
		messageStream.addSink(new FlinkKafkaProducer09<>(
			topic,
			new SimpleStringSchema(),
			parameterTool.getProperties()));

		env.execute("Write into Kafka v0.9 example");
	}
}
