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

package org.apache.flink.streaming.api.invokable.operator.windowing;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class WindowIntegrationTest implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Integer MEMORYSIZE = 32;

	@SuppressWarnings("serial")
	private static class ModKey implements KeySelector<Integer, Integer> {
		private int m;

		public ModKey(int m) {
			this.m = m;
		}

		@Override
		public Integer getKey(Integer value) throws Exception {
			return value % m;
		}
	}

	@SuppressWarnings("serial")
	public static class IdentityWindowMap implements
			WindowMapFunction<Integer, StreamWindow<Integer>> {

		@Override
		public void mapWindow(Iterable<Integer> values, Collector<StreamWindow<Integer>> out)
				throws Exception {

			StreamWindow<Integer> window = new StreamWindow<Integer>();

			for (Integer value : values) {
				window.add(value);
			}
			out.collect(window);
		}

	}

	@Test
	public void test() throws Exception {

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(2);
		inputs.add(2);
		inputs.add(3);
		inputs.add(4);
		inputs.add(5);
		inputs.add(10);
		inputs.add(11);
		inputs.add(11);

		StreamExecutionEnvironment env = new TestStreamEnvironment(2, MEMORYSIZE);

		DataStream<Integer> source = env.fromCollection(inputs);

		source.window(Count.of(2)).every(Count.of(3)).sum(0).getDiscretizedStream()
				.addSink(new CentralSink1());

		source.window(Count.of(4)).groupBy(new ModKey(2)).mapWindow(new IdentityWindowMap())
				.flatten().addSink(new CentralSink2());

		source.groupBy(new ModKey(3)).window(Count.of(2)).sum(0).getDiscretizedStream()
				.addSink(new DistributedSink1());

		source.groupBy(new ModKey(3)).window(Count.of(2)).groupBy(new ModKey(2))
				.mapWindow(new IdentityWindowMap()).flatten().addSink(new DistributedSink2());

		env.execute();

		// sum ( Count of 2 slide 3 )
		List<StreamWindow<Integer>> expected1 = new ArrayList<StreamWindow<Integer>>();
		expected1.add(StreamWindow.fromElements(4));
		expected1.add(StreamWindow.fromElements(9));
		expected1.add(StreamWindow.fromElements(22));

		validateOutput(expected1, CentralSink1.windows);

		// Tumbling Count of 4 grouped by mod 2
		List<StreamWindow<Integer>> expected2 = new ArrayList<StreamWindow<Integer>>();
		expected2.add(StreamWindow.fromElements(2, 2));
		expected2.add(StreamWindow.fromElements(1, 3));
		expected2.add(StreamWindow.fromElements(4, 10));
		expected2.add(StreamWindow.fromElements(5, 11));
		expected2.add(StreamWindow.fromElements(11));

		validateOutput(expected2, CentralSink2.windows);

		// groupby mod 3 sum ( Tumbling Count of 2)
		List<StreamWindow<Integer>> expected3 = new ArrayList<StreamWindow<Integer>>();
		expected3.add(StreamWindow.fromElements(4));
		expected3.add(StreamWindow.fromElements(5));
		expected3.add(StreamWindow.fromElements(16));
		expected3.add(StreamWindow.fromElements(10));
		expected3.add(StreamWindow.fromElements(11));
		expected3.add(StreamWindow.fromElements(3));

		validateOutput(expected3, DistributedSink1.windows);

		// groupby mod3 Tumbling Count of 2 grouped by mod 2
		List<StreamWindow<Integer>> expected4 = new ArrayList<StreamWindow<Integer>>();
		expected4.add(StreamWindow.fromElements(2, 2));
		expected4.add(StreamWindow.fromElements(1));
		expected4.add(StreamWindow.fromElements(4));
		expected4.add(StreamWindow.fromElements(5, 11));
		expected4.add(StreamWindow.fromElements(10));
		expected4.add(StreamWindow.fromElements(11));
		expected4.add(StreamWindow.fromElements(3));

		validateOutput(expected4, DistributedSink2.windows);

	}

	public static <R> void validateOutput(List<R> expected, List<R> actual) {
		assertEquals(new HashSet<R>(expected), new HashSet<R>(actual));
	}

	@SuppressWarnings("serial")
	private static class CentralSink1 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class CentralSink2 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class DistributedSink1 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class DistributedSink2 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}
}
