/** Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class CoFlatMapTest implements Serializable {
	private static final long serialVersionUID = 1L;

	private static Set<String> result;
	private static Set<String> expected = new HashSet<String>();

	private final static class EmptySink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(String tuple) {
		}
	}

	private final static class MyCoFlatMap implements CoFlatMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap1(String value, Collector<String> coll) {
			for (int i = 0; i < value.length(); i++) {
				coll.collect(value.substring(i, i + 1));
				result.add(value.substring(i, i + 1));
			}
		}

		@Override
		public void flatMap2(Integer value, Collector<String> coll) {
			coll.collect(value.toString());
			result.add(value.toString());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void multipleInputTest() {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);
		expected.add("a");
		expected.add("b");
		expected.add("c");
		expected.add("d");
		expected.add("e");
		expected.add("f");
		expected.add("g");
		expected.add("h");
		expected.add("e");
		expected.add("1");
		expected.add("2");
		expected.add("3");
		expected.add("4");
		expected.add("5");

		result = new HashSet<String>();

		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Integer> ds1 = env.fromElements(1, 3, 5);
		DataStream<Integer> ds2 = env.fromElements(2, 4).merge(ds1);

		try {
			ds1.forward().merge(ds2);
			fail();
		} catch (RuntimeException e) {
			// good
		}

		@SuppressWarnings({ "unused" })
		DataStream<String> ds4 = env.fromElements("abc", "def", "ghe").connect(ds2)
				.flatMap(new MyCoFlatMap()).addSink(new EmptySink());

		env.executeTest(32);

		Assert.assertEquals(expected, result);
	}
}
