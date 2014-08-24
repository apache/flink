/**
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

package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.util.LogUtils;

import org.junit.Assert;
import org.junit.Test;

public class CoMapTest implements Serializable {
	private static final long serialVersionUID = 1L;

	private static Set<String> result;
	private static Set<String> expected = new HashSet<String>();

	private final static class EmptySink implements SinkFunction<Boolean> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Boolean tuple) {
		}
	}

	private final static class MyCoMap implements CoMapFunction<String, Integer, Boolean> {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean map1(String value) {
			result.add(value);
			return true;
		}

		@Override
		public Boolean map2(Integer value) {
			result.add(value.toString());
			return false;
		}
	}

	@Test
	public void multipleInputTest() {
		
		LogUtils.initializeDefaultTestConsoleLogger();
		
		expected.add("a");
		expected.add("b");
		expected.add("c");
		expected.add("1");
		expected.add("2");
		expected.add("3");
		expected.add("4");

		result = new HashSet<String>();

		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Integer> ds1 = env.fromElements(1, 3);
		@SuppressWarnings("unchecked")
		DataStream<Integer> ds2 = env.fromElements(2, 4).merge(ds1);

		DataStream<String> ds3 = env.fromElements("a", "b");

		@SuppressWarnings({ "unused", "unchecked" })
		DataStream<Boolean> ds4 = env.fromElements("c").merge(ds3).connect(ds2).map(new MyCoMap())
				.addSink(new EmptySink());

		env.executeTest(32);
		Assert.assertEquals(expected, result);
	}
}
