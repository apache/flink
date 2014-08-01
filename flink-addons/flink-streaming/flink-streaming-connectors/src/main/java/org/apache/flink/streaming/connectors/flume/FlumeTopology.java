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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;

public class FlumeTopology {

	public static class MyFlumeSink extends FlumeSink<Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		public MyFlumeSink(String host, int port) {
			super(host, port);
		}

		@Override
		public byte[] serialize(Tuple1<String> tuple) {
			if (tuple.f0.equals("q")) {
				try {
					sendAndClose();
				} catch (Exception e) {
					new RuntimeException("Error while closing Flume connection with " + port + " at "
							+ host, e);
				}
			}
			return SerializationUtils.serialize((String) tuple.getField(0));
		}

	}

	public static class MyFlumeSource extends FlumeSource<Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		MyFlumeSource(String host, int port) {
			super(host, port);
		}

		@Override
		public Tuple1<String> deserialize(byte[] msg) {
			String s = (String) SerializationUtils.deserialize(msg);
			Tuple1<String> out = new Tuple1<String>();
			out.f0 = s;
			if (s.equals("q")) {
				closeWithoutSend();
			}
			return out;
		}

	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		@SuppressWarnings("unused")
		DataStream<Tuple1<String>> dataStream1 = env
			.addSource(new MyFlumeSource("localhost", 41414))
			.print();

		@SuppressWarnings("unused")
		DataStream<Tuple1<String>> dataStream2 = env
			.fromElements("one", "two", "three", "four", "five", "q")
			.addSink(new MyFlumeSink("localhost", 42424));

		env.execute();
	}
}
