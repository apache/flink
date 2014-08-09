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

package org.apache.flink.streaming.connectors.flume;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlumeTopology {

	public static class MyFlumeSink extends FlumeSink<String> {
		private static final long serialVersionUID = 1L;

		public MyFlumeSink(String host, int port) {
			super(host, port);
		}

		@Override
		public byte[] serialize(String tuple) {
			if (tuple.equals("q")) {
				try {
					sendAndClose();
				} catch (Exception e) {
					throw new RuntimeException("Error while closing Flume connection with " + port + " at "
							+ host, e);
				}
			}
			return SerializationUtils.serialize(tuple);
		}

	}

	public static class MyFlumeSource extends FlumeSource<String> {
		private static final long serialVersionUID = 1L;

		MyFlumeSource(String host, int port) {
			super(host, port);
		}

		@Override
		public String deserialize(byte[] msg) {
			String s = (String) SerializationUtils.deserialize(msg);
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
			.addSource(new MyFlumeSource("localhost", 41414))
			.print();

		@SuppressWarnings("unused")
		DataStream<String> dataStream2 = env
			.fromElements("one", "two", "three", "four", "five", "q")
			.addSink(new MyFlumeSink("localhost", 42424));

		env.execute();
	}
}
