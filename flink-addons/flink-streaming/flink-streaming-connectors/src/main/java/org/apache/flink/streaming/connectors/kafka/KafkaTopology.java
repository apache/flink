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
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopology {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaTopology.class);
	
	public static final class MySource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<String> collector) throws Exception {
			for (int i = 0; i < 10; i++) {
				collector.collect(new String(Integer.toString(i)));
			}
			collector.collect(new String("q"));

		}
	}

	public static final class MyKafkaSource extends KafkaSource<String> {
		private static final long serialVersionUID = 1L;

		public MyKafkaSource(String zkQuorum, String groupId, String topicId, int numThreads) {
			super(zkQuorum, groupId, topicId, numThreads);
		}

		@Override
		public String deserialize(byte[] msg) {
			String s = new String(msg);
			if (s.equals("q")) {
				closeWithoutSend();
			}
			return new String(s);
		}

	}

	public static final class MyKafkaSink extends KafkaSink<String, String> {
		private static final long serialVersionUID = 1L;

		public MyKafkaSink(String topicId, String brokerAddr) {
			super(topicId, brokerAddr);
		}

		@Override
		public String serialize(String tuple) {
			if (tuple.equals("q")) {
				sendAndClose();
			}
			return tuple;
		}

	}
	
	public static final class MyKafkaPrintSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(String value) {
			if (LOG.isInfoEnabled()) {
				LOG.info("String: <{}> arrived from Kafka", value);
			}
		}
	}

	private static final int SOURCE_PARALELISM = 1;

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		@SuppressWarnings("unused")
		DataStream<String> stream1 = env
			.addSource(new MyKafkaSource("localhost:2181", "group", "test", 1), SOURCE_PARALELISM)
			.addSink(new MyKafkaPrintSink());

		@SuppressWarnings("unused")
		DataStream<String> stream2 = env
			.addSource(new MySource())
			.addSink(new MyKafkaSink("test", "localhost:9092"));

		env.execute();
	}
}
