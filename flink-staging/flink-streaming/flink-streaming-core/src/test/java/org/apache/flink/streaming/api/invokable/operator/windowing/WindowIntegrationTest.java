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
import org.apache.flink.streaming.api.function.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.function.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.helper.Count;
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

		DataStream<Integer> source = env.fromCollection(inputs);

		source.window(Time.of(3, ts, 1)).every(Time.of(2, ts, 1)).sum(0).getDiscretizedStream()
				.addSink(new CentralSink1());

		source.window(Time.of(4, ts, 1)).groupBy(new ModKey(2)).mapWindow(new IdentityWindowMap())
				.flatten().addSink(new CentralSink2());

		source.groupBy(key).window(Time.of(4, ts, 1)).sum(0).getDiscretizedStream()
				.addSink(new DistributedSink1());

		source.groupBy(new ModKey(3)).window(Count.of(2)).groupBy(new ModKey(2))
				.mapWindow(new IdentityWindowMap()).flatten().addSink(new DistributedSink2());

		source.window(Time.of(2, ts)).every(Time.of(3, ts)).min(0).getDiscretizedStream()
				.addSink(new CentralSink3());

		source.groupBy(key).window(Time.of(4, ts, 1)).max(0).getDiscretizedStream()
				.addSink(new DistributedSink3());

		source.window(Time.of(5, ts, 1)).mapWindow(new IdentityWindowMap()).flatten()
				.addSink(new DistributedSink4());

		source.window(Time.of(5, ts, 1)).every(Time.of(4, ts, 1)).groupBy(new ModKey(2)).sum(0)
				.getDiscretizedStream().addSink(new DistributedSink5());

		DataStream<Integer> source2 = env.addSource(new ParallelSourceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void run(Collector<Integer> collector) throws Exception {
				for (int i = 1; i <= 10; i++) {
					collector.collect(i);
				}
			}

			@Override
			public void cancel() {
			}
		});

		DataStream<Integer> source3 = env.addSource(new RichParallelSourceFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void run(Collector<Integer> collector) throws Exception {
				for (int i = 1; i <= 11; i++) {
					if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
						collector.collect(i);
					}
				}
			}

			@Override
			public void cancel() {
			}
		});

		source2.window(Time.of(2, ts, 1)).sum(0).getDiscretizedStream()
				.addSink(new DistributedSink6());

		source3.window(Time.of(5, ts, 1)).groupBy(new ModKey(2)).sum(0).getDiscretizedStream()
				.addSink(new DistributedSink7());

		env.execute();

		// sum ( Time of 3 slide 2 )
		List<StreamWindow<Integer>> expected1 = new ArrayList<StreamWindow<Integer>>();
		expected1.add(StreamWindow.fromElements(5));
		expected1.add(StreamWindow.fromElements(11));
		expected1.add(StreamWindow.fromElements(9));
		expected1.add(StreamWindow.fromElements(10));
		expected1.add(StreamWindow.fromElements(32));

		validateOutput(expected1, CentralSink1.windows);

		// Tumbling Time of 4 grouped by mod 2
		List<StreamWindow<Integer>> expected2 = new ArrayList<StreamWindow<Integer>>();
		expected2.add(StreamWindow.fromElements(2, 2, 4));
		expected2.add(StreamWindow.fromElements(1, 3));
		expected2.add(StreamWindow.fromElements(5));
		expected2.add(StreamWindow.fromElements(10));
		expected2.add(StreamWindow.fromElements(11, 11));

		validateOutput(expected2, CentralSink2.windows);

		// groupby mod 2 sum ( Tumbling Time of 4)
		List<StreamWindow<Integer>> expected3 = new ArrayList<StreamWindow<Integer>>();
		expected3.add(StreamWindow.fromElements(4));
		expected3.add(StreamWindow.fromElements(5));
		expected3.add(StreamWindow.fromElements(22));
		expected3.add(StreamWindow.fromElements(8));
		expected3.add(StreamWindow.fromElements(10));

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

		// min ( Time of 2 slide 3 )
		List<StreamWindow<Integer>> expected5 = new ArrayList<StreamWindow<Integer>>();
		expected5.add(StreamWindow.fromElements(1));
		expected5.add(StreamWindow.fromElements(4));
		expected5.add(StreamWindow.fromElements(10));

		validateOutput(expected5, CentralSink3.windows);

		// groupby mod 2 max ( Tumbling Time of 4)
		List<StreamWindow<Integer>> expected6 = new ArrayList<StreamWindow<Integer>>();
		expected6.add(StreamWindow.fromElements(3));
		expected6.add(StreamWindow.fromElements(5));
		expected6.add(StreamWindow.fromElements(11));
		expected6.add(StreamWindow.fromElements(4));
		expected6.add(StreamWindow.fromElements(10));

		validateOutput(expected6, DistributedSink3.windows);

		List<StreamWindow<Integer>> expected7 = new ArrayList<StreamWindow<Integer>>();
		expected7.add(StreamWindow.fromElements(1, 2, 2, 3, 4, 5));
		expected7.add(StreamWindow.fromElements(10));
		expected7.add(StreamWindow.fromElements(10, 11, 11));

		validateOutput(expected7, DistributedSink4.windows);

		List<StreamWindow<Integer>> expected8 = new ArrayList<StreamWindow<Integer>>();
		expected8.add(StreamWindow.fromElements(4, 8));
		expected8.add(StreamWindow.fromElements(4, 5));
		expected8.add(StreamWindow.fromElements(10, 22));

		for (List<Integer> sw : DistributedSink5.windows) {
			Collections.sort(sw);
		}

		validateOutput(expected8, DistributedSink5.windows);

		List<StreamWindow<Integer>> expected9 = new ArrayList<StreamWindow<Integer>>();
		expected9.add(StreamWindow.fromElements(6));
		expected9.add(StreamWindow.fromElements(14));
		expected9.add(StreamWindow.fromElements(22));
		expected9.add(StreamWindow.fromElements(30));
		expected9.add(StreamWindow.fromElements(38));

		validateOutput(expected9, DistributedSink6.windows);

		List<StreamWindow<Integer>> expected10 = new ArrayList<StreamWindow<Integer>>();
		expected10.add(StreamWindow.fromElements(6, 9));
		expected10.add(StreamWindow.fromElements(16, 24));

		for (List<Integer> sw : DistributedSink7.windows) {
			Collections.sort(sw);
		}

		validateOutput(expected10, DistributedSink7.windows);

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
	private static class CentralSink3 implements SinkFunction<StreamWindow<Integer>> {

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

	@SuppressWarnings("serial")
	private static class DistributedSink3 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class DistributedSink4 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class DistributedSink5 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class DistributedSink6 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

	@SuppressWarnings("serial")
	private static class DistributedSink7 implements SinkFunction<StreamWindow<Integer>> {

		public static List<StreamWindow<Integer>> windows = Collections
				.synchronizedList(new ArrayList<StreamWindow<Integer>>());

		@Override
		public void invoke(StreamWindow<Integer> value) throws Exception {
			windows.add(value);
		}

	}

}
