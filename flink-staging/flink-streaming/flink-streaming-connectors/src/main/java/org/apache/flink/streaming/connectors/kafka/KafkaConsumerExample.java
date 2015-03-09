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
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaCustomOffsetSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.SimpleKafkaSource;
import org.apache.flink.streaming.connectors.util.JavaDefaultStringSchema;

public class KafkaConsumerExample {

	private static String host;
	private static int port;
	private static String topic;

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setDegreeOfParallelism(4);

		@SuppressWarnings("unused")
		DataStream<String> stream1 = env
				.addSource(
//						new KafkaSource<String>(host + ":" + port, topic, new JavaDefaultStringSchema()))
//						new SimpleKafkaSource<String>(topic, host, port, new JavaDefaultStringSchema()))
						new KafkaCustomOffsetSource<String>(topic, host, port, new JavaDefaultStringSchema()))
				.setParallelism(3)
				.print().setParallelism(3);

		env.execute();
	}

	private static boolean parseParameters(String[] args) {
		if (args.length == 3) {
			host = args[0];
			port = Integer.parseInt(args[1]);
			topic = args[2];
			return true;
		} else {
			System.err.println("Usage: KafkaConsumerExample <host> <port> <topic>");
			return false;
		}
	}

}
