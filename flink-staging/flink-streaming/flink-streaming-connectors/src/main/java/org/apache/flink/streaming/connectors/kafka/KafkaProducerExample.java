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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.util.JavaDefaultStringSchema;
import org.apache.flink.util.Collector;

public class KafkaProducerExample {

	private static String host;
	private static int port;
	private static String topic;

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setDegreeOfParallelism(4);

		@SuppressWarnings({ "unused", "serial" })
		DataStream<String> stream1 = env.addSource(new SourceFunction<String>() {
			@Override
			public void run(Collector<String> collector) throws Exception {
				for (int i = 0; i < 100; i++) {
					collector.collect("message #" + i);
					Thread.sleep(100L);
				}

				collector.collect(new String("q"));
			}

			@Override
			public void cancel() {				
			}
			
			
		}).addSink(
				new KafkaSink<String>(topic, host + ":" + port, new JavaDefaultStringSchema())
		)
		.setParallelism(3);

		env.execute();
	}

	private static boolean parseParameters(String[] args) {
		if (args.length == 3) {
			host = args[0];
			port = Integer.parseInt(args[1]);
			topic = args[2];
			return true;
		} else {
			System.err.println("Usage: KafkaProducerExample <host> <port> <topic>");
			return false;
		}
	}
}
