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

package org.apache.flink.streaming.api.operators.windowing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.FullStream;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class WindowIntegrationTest implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Integer MEMORYSIZE = 32;

	@SuppressWarnings("serial")
	public static class ModKey implements KeySelector<Integer, Integer> {
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

	@SuppressWarnings("serial")
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

		KeySelector<Integer, ?> key = new ModKey(2);

		Timestamp<Integer> ts = new Timestamp<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}
		};

		StreamExecutionEnvironment env = new TestStreamEnvironment(2, MEMORYSIZE);
		env.disableOperatorChaining();

		DataStream<Integer> source = env.fromCollection(inputs);

		source.window(Time.of(3, ts, 1)).every(Time.of(2, ts, 1)).sum(0).getDiscretizedStream()
				.addSink(new TestSink1());

		source.window(Time.of(4, ts, 1)).groupBy(new ModKey(2)).mapWindow(new IdentityWindowMap())
				.flatten().addSink(new TestSink2());

		source.groupBy(key).window(Time.of(4, ts, 1)).sum(0).getDiscretizedStream()
				.addSink(new TestSink4());

		source.groupBy(new ModKey(3)).window(Count.of(2)).groupBy(new ModKey(2))
				.mapWindow(new IdentityWindowMap()).flatten().addSink(new TestSink5());

		source.window(Time.of(2, ts)).every(Time.of(3, ts)).min(0).getDiscretizedStream()
				.addSink(new TestSink3());

		source.groupBy(key).window(Time.of(4, ts, 1)).max(0).getDiscretizedStream()
				.addSink(new TestSink6());

		source.window(Time.of(5, ts, 1)).mapWindow(new IdentityWindowMap()).flatten()
				.addSink(new TestSink7());

		source.window(Time.of(5, ts, 1)).every(Time.of(4, ts, 1)).groupBy(new ModKey(2)).sum(0)
				.getDiscretizedStream().addSink(new TestSink8());

		try {
			source.window(FullStream.window()).every(Count.of(2)).getDiscretizedStream();
			fail();
		} catch (Exception e) {
		}
		try {
			source.window(FullStream.window()).getDiscretizedStream();
			fail();
		} catch (Exception e) {
		}
		try {
			source.every(Count.of(5)).mapWindow(new IdentityWindowMap()).getDiscretizedStream();
			fail();
		} catch (Exception e) {
		}

		source.every(Count.of(4)).sum(0).getDiscretizedStream().addSink(new TestSink11());

		source.window(FullStream.window()).every(Count.of(4)).groupBy(key).sum(0)
				.getDiscretizedStream().addSink(new TestSink12());

		DataStream<Integer> source2 = env.addSource(new ParallelSourceFunction<Integer>() {

			private int i = 1;

			@Override
			public boolean reachedEnd() throws Exception {
				return i > 10;
			}

			@Override
			public Integer next() throws Exception {
				return i++;
			}

		});

		DataStream<Integer> source3 = env.addSource(new RichParallelSourceFunction<Integer>() {
			private int i = 1;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				i = 1 + getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public boolean reachedEnd() throws Exception {
				return i > 11;
			}

			@Override
			public Integer next() throws Exception {
				int result = i;
				i += 2;
				return result;
			}

		});

		source2.window(Time.of(2, ts, 1)).sum(0).getDiscretizedStream().addSink(new TestSink9());

		source3.window(Time.of(5, ts, 1)).groupBy(new ModKey(2)).sum(0).getDiscretizedStream()
				.addSink(new TestSink10());

		source.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return value;
			}
		}).every(Time.of(5, ts, 1)).sum(0).getDiscretizedStream().addSink(new TestSink13());

		env.execute();

		// sum ( Time of 3 slide 2 )
		List<StreamWindow<Integer>> expected1 = new ArrayList<StreamWindow<Integer>>();
		expected1.add(StreamWindow.fromElements(5));
		expected1.add(StreamWindow.fromElements(11));
		expected1.add(StreamWindow.fromElements(9));
		expected1.add(StreamWindow.fromElements(10));
		expected1.add(StreamWindow.fromElements(32));

		validateOutput(expected1, TestSink1.windows);

		// Tumbling Time of 4 grouped by mod 2
		List<StreamWindow<Integer>> expected2 = new ArrayList<StreamWindow<Integer>>();
		expected2.add(StreamWindow.fromElements(2, 2, 4));
		expected2.add(StreamWindow.fromElements(1, 3));
		expected2.add(StreamWindow.fromElements(5));
		expected2.add(StreamWindow.fromElements(10));
		expected2.add(StreamWindow.fromElements(11, 11));

		validateOutput(expected2, TestSink2.windows);

		// groupby mod 2 sum ( Tumbling Time of 4)
		List<StreamWindow<Integer>> expected3 = new ArrayList<StreamWindow<Integer>>();
		expected3.add(StreamWindow.fromElements(4));
		expected3.add(StreamWindow.fromElements(5));
		expected3.add(StreamWindow.fromElements(22));
		expected3.add(StreamWindow.fromElements(8));
		expected3.add(StreamWindow.fromElements(10));

		validateOutput(expected3, TestSink4.windows);

		// groupby mod3 Tumbling Count of 2 grouped by mod 2
		List<StreamWindow<Integer>> expected4 = new ArrayList<StreamWindow<Integer>>();
		expected4.add(StreamWindow.fromElements(2, 2));
		expected4.add(StreamWindow.fromElements(1));
		expected4.add(StreamWindow.fromElements(4));
		expected4.add(StreamWindow.fromElements(5, 11));
		expected4.add(StreamWindow.fromElements(10));
		expected4.add(StreamWindow.fromElements(11));
		expected4.add(StreamWindow.fromElements(3));

		validateOutput(expected4, TestSink5.windows);

		// min ( Time of 2 slide 3 )
		List<StreamWindow<Integer>> expected5 = new ArrayList<StreamWindow<Integer>>();
		expected5.add(StreamWindow.fromElements(1));
		expected5.add(StreamWindow.fromElements(4));
		expected5.add(StreamWindow.fromElements(10));

		validateOutput(expected5, TestSink3.windows);

		// groupby mod 2 max ( Tumbling Time of 4)
		List<StreamWindow<Integer>> expected6 = new ArrayList<StreamWindow<Integer>>();
		expected6.add(StreamWindow.fromElements(3));
		expected6.add(StreamWindow.fromElements(5));
		expected6.add(StreamWindow.fromElements(11));
		expected6.add(StreamWindow.fromElements(4));
		expected6.add(StreamWindow.fromElements(10));

		validateOutput(expected6, TestSink6.windows);

		List<StreamWindow<Integer>> expected7 = new ArrayList<StreamWindow<Integer>>();
		expected7.add(StreamWindow.fromElements(1, 2, 2, 3, 4, 5));
		expected7.add(StreamWindow.fromElements(10));
		expected7.add(StreamWindow.fromElements(10, 11, 11));

		validateOutput(expected7, TestSink7.windows);

		List<StreamWindow<Integer>> expected8 = new ArrayList<StreamWindow<Integer>>();
		expected8.add(StreamWindow.fromElements(4, 8));
		expected8.add(StreamWindow.fromElements(4, 5));
		expected8.add(StreamWindow.fromElements(10, 22));

		for (List<Integer> sw : TestSink8.windows) {
			Collections.sort(sw);
		}

		validateOutput(expected8, TestSink8.windows);

		List<StreamWindow<Integer>> expected9 = new ArrayList<StreamWindow<Integer>>();
		expected9.add(StreamWindow.fromElements(6));
		expected9.add(StreamWindow.fromElements(14));
		expected9.add(StreamWindow.fromElements(22));
		expected9.add(StreamWindow.fromElements(30));
		expected9.add(StreamWindow.fromElements(38));

		validateOutput(expected9, TestSink9.windows);

		List<StreamWindow<Integer>> expected10 = new ArrayList<StreamWindow<Integer>>();
		expected10.add(StreamWindow.fromElements(6, 9));
		expected10.add(StreamWindow.fromElements(16, 24));

		for (List<Integer> sw : TestSink10.windows) {
			Collections.sort(sw);
		}

		validateOutput(expected10, TestSink10.windows);

		List<StreamWindow<Integer>> expected11 = new ArrayList<StreamWindow<Integer>>();
		expected11.add(StreamWindow.fromElements(8));
		expected11.add(StreamWindow.fromElements(38));
		expected11.add(StreamWindow.fromElements(49));

		for (List<Integer> sw : TestSink11.windows) {
			Collections.sort(sw);
		}

		validateOutput(expected11, TestSink11.windows);

		List<StreamWindow<Integer>> expected12 = new ArrayList<StreamWindow<Integer>>();
		expected12.add(StreamWindow.fromElements(4, 4));
		expected12.add(StreamWindow.fromElements(18, 20));
		expected12.add(StreamWindow.fromElements(18, 31));

		for (List<Integer> sw : TestSink12.windows) {
			Collections.sort(sw);
		}

		validateOutput(expected12, TestSink12.windows);

		List<StreamWindow<Integer>> expected13 = new ArrayList<StreamWindow<Integer>>();
		expected13.add(StreamWindow.fromElements(17));
		expected13.add(StreamWindow.fromElements(27));
		expected13.add(StreamWindow.fromElements(49));

		for (List<Integer> sw : TestSink13.windows) {
			Collections.sort(sw);
		}

		validateOutput(expected13, TestSink13.windows);

	}

	public static <R> void validateOutput(List<R> expected, List<R> actual) {
		assertEquals(new HashSet<R>(expected), new HashSet<R>(actual));
	}

	@SuppressWarnings("serial")
	private static class TestSink1 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink2 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink3 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink4 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink5 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink6 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink7 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink8 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink9 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink10 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink11 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink12 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class TestSink13 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

}
