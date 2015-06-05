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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;

@SuppressWarnings("serial")
public class KafkaProducerExample {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.err.println("Usage: KafkaProducerExample <host> <port> <topic>");
			return;
		}

		final String host = args[0];
		final int port = Integer.parseInt(args[1]);
		final String topic = args[2];

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(4);
		
		env.addSource(new SourceFunction<String>() {
			
			private volatile boolean running = true;
			
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				for (int i = 0; i < 20 && running; i++) {
					ctx.collect("message #" + i);
					Thread.sleep(100L);
				}

				ctx.collect("q");
			}

			@Override
			public void cancel() {
				running = false;
			}


		})
			.addSink(new KafkaSink<String>(host + ":" + port, topic, new JavaDefaultStringSchema()))
				.setParallelism(3);

		env.execute();
	}
}
