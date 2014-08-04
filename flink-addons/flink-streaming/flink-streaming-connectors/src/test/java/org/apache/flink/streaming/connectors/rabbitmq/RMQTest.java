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

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.junit.Test;

public class RMQTest {

	public static final class MySink implements SinkFunction<Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<String> tuple) {
			result.add(tuple.f0);
		}

	}

	private static Set<String> expected = new HashSet<String>();
	private static Set<String> result = new HashSet<String>();

	@SuppressWarnings("unused")
	private static void fillExpected() {
		expected.add("one");
		expected.add("two");
		expected.add("three");
		expected.add("four");
		expected.add("five");
	}

	@Test
	public void RMQTest1() throws Exception {
		//
		// StreamExecutionEnvironment env = new StreamExecutionEnvironment();
		//
		// DataStream<Tuple1<String>> dataStream1 = env
		// .addSource(new RMQSource("localhost", "hello"), 1)
		// .addSink(new MySink());
		//
		// DataStream<Tuple1<String>> dataStream2 = env
		// .fromElements("one", "two", "three", "four", "five", "q")
		// .addSink(new RMQSink("localhost", "hello"));
		//
		// env.execute();
		//
		// fillExpected();
		//
		// assertEquals(expected, result);
	}
}
