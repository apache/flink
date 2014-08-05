/**
 *
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
 *
 */

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RMQTopology {

	public static final class MyRMQSink extends RMQSink<String> {
		public MyRMQSink(String HOST_NAME, String QUEUE_NAME) {
			super(HOST_NAME, QUEUE_NAME);
		}

		private static final long serialVersionUID = 1L;

		@Override
		public byte[] serialize(String t) {
			if (t.equals("q")) {
				sendAndClose();
			}
			return SerializationUtils.serialize((String) t);
		}

	}

	public static final class MyRMQSource extends RMQSource<String> {

		public MyRMQSource(String HOST_NAME, String QUEUE_NAME) {
			super(HOST_NAME, QUEUE_NAME);
		}

		private static final long serialVersionUID = 1L;

		@Override
		public String deserialize(byte[] t) {
			String s = (String) SerializationUtils.deserialize(t);
			if (s.equals("q")) {
				closeWithoutSend();
			}
			return s;
		}

	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		@SuppressWarnings("unused")
		DataStream<String> dataStream1 = env
			.addSource(new MyRMQSource("localhost", "hello"))
			.print();

		@SuppressWarnings("unused")
		DataStream<String> dataStream2 = env
			.fromElements("one", "two", "three", "four", "five", "q")
			.addSink(new MyRMQSink("localhost", "hello"));

		env.execute();
	}
}
