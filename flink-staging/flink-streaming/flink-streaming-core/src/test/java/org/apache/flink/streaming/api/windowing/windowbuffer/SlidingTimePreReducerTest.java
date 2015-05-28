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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestCollector;
import org.junit.Test;

public class SlidingTimePreReducerTest {

	TypeSerializer<Integer> serializer = TypeExtractor.getForObject(1).createSerializer(null);
	TypeInformation<Tuple2<Integer,Integer>> tupleType = TypeInfoParser.parse("Tuple2<Integer,Integer>");

	ReduceFunction<Integer> reducer = new SumReducer();
	ReduceFunction<Tuple2<Integer, Integer>> tupleReducer = new TupleSumReducer();

	@Test
	@SuppressWarnings("unchecked")
	public void testPreReduce1() throws Exception {
		// This ensures that the buffer is properly cleared after a burst of elements by
		// replaying the same sequence of elements with a later timestamp and expecting the same
		// result.

		TestCollector<StreamWindow<Tuple2<Integer, Integer>>> collector = new TestCollector<StreamWindow<Tuple2<Integer, Integer>>>();

		SlidingTimePreReducer<Tuple2<Integer, Integer>> preReducer = new SlidingTimePreReducer<Tuple2<Integer, Integer>>(tupleReducer,
				tupleType.createSerializer(new ExecutionConfig()), 3, 2, new TimestampWrapper<Tuple2<Integer, Integer>>(new Timestamp<Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public long getTimestamp(Tuple2<Integer, Integer> value) {
						return value.f0;
					}
				}, 1));

		int timeOffset = 0;

		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 1, 1));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 2, 2));
		preReducer.emitWindow(collector);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 3, 3));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 4, 4));
		preReducer.evict(1);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 5, 5));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 6, 6));
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 7, 7));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 8, 8));
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 9, 9));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 10, 10));
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 11, 11));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 12, 12));
		preReducer.emitWindow(collector);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 13, 13));

		// ensure that everything is cleared out
		preReducer.evict(100);


		timeOffset = 25; // a little while later...

		// Repeat the same sequence, this should produce the same result
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 1, 1));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 2, 2));
		preReducer.emitWindow(collector);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 3, 3));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 4, 4));
		preReducer.evict(1);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 5, 5));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 6, 6));
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 7, 7));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 8, 8));
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 9, 9));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 10, 10));
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 11, 11));
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 12, 12));
		preReducer.emitWindow(collector);
		preReducer.store(new Tuple2<Integer, Integer>(timeOffset + 13, 13));

		List<StreamWindow<Tuple2<Integer, Integer>>> expected = new ArrayList<StreamWindow<Tuple2<Integer, Integer>>>();
		timeOffset = 0; // rewind ...
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 1, 3)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 2, 9)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 4, 15)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 6, 21)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 8, 27)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 10, 33)));

		timeOffset = 25; // and back to the future ...
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 1, 3)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 2, 9)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 4, 15)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 6, 21)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 8, 27)));
		expected.add(StreamWindow.fromElements(new Tuple2<Integer, Integer>(timeOffset + 10, 33)));


		assertEquals(expected, collector.getCollected());
	}

	@Test
	public void testPreReduce2() throws Exception {
		TestCollector<StreamWindow<Integer>> collector = new TestCollector<StreamWindow<Integer>>();

		SlidingTimePreReducer<Integer> preReducer = new SlidingTimePreReducer<Integer>(reducer,
				serializer, 5, 2, new TimestampWrapper<Integer>(new Timestamp<Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public long getTimestamp(Integer value) {
						return value;
					}
				}, 1));

		preReducer.store(1);
		preReducer.store(2);
		preReducer.emitWindow(collector);
		preReducer.store(3);
		preReducer.store(4);
		preReducer.emitWindow(collector);
		preReducer.store(5);
		preReducer.store(6);
		preReducer.evict(1);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(7);
		preReducer.store(8);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(9);
		preReducer.store(10);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(11);
		preReducer.store(12);
		preReducer.emitWindow(collector);
		preReducer.store(13);

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(3));
		expected.add(StreamWindow.fromElements(10));
		expected.add(StreamWindow.fromElements(20));
		expected.add(StreamWindow.fromElements(30));
		expected.add(StreamWindow.fromElements(40));
		expected.add(StreamWindow.fromElements(50));

		assertEquals(expected, collector.getCollected());
	}

	@Test
	public void testPreReduce3() throws Exception {
		TestCollector<StreamWindow<Integer>> collector = new TestCollector<StreamWindow<Integer>>();

		SlidingTimePreReducer<Integer> preReducer = new SlidingTimePreReducer<Integer>(reducer,
				serializer, 6, 3, new TimestampWrapper<Integer>(new Timestamp<Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public long getTimestamp(Integer value) {
						return value;
					}
				}, 1));

		preReducer.store(1);
		preReducer.store(2);
		preReducer.store(3);
		preReducer.emitWindow(collector);
		preReducer.store(4);
		preReducer.store(5);
		preReducer.store(6);
		preReducer.emitWindow(collector);
		preReducer.evict(3);
		preReducer.store(7);
		preReducer.store(8);
		preReducer.store(9);
		preReducer.emitWindow(collector);
		preReducer.evict(3);
		preReducer.store(10);
		preReducer.store(11);
		preReducer.store(12);
		preReducer.emitWindow(collector);
		preReducer.evict(3);
		preReducer.store(13);

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(6));
		expected.add(StreamWindow.fromElements(21));
		expected.add(StreamWindow.fromElements(39));
		expected.add(StreamWindow.fromElements(57));

		assertEquals(expected, collector.getCollected());
	}

	@Test
	public void testPreReduce4() throws Exception {
		TestCollector<StreamWindow<Integer>> collector = new TestCollector<StreamWindow<Integer>>();

		SlidingTimePreReducer<Integer> preReducer = new SlidingTimePreReducer<Integer>(reducer,
				serializer, 3, 2, new TimestampWrapper<Integer>(new Timestamp<Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public long getTimestamp(Integer value) {
						return value;
					}
				}, 1));

		preReducer.store(1);
		preReducer.store(2);
		preReducer.emitWindow(collector);
		preReducer.store(3);
		preReducer.store(4);
		preReducer.evict(1);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(5);
		preReducer.store(6);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(7);
		preReducer.store(8);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.emitWindow(collector);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(14);
		preReducer.emitWindow(collector);
		preReducer.emitWindow(collector);
		preReducer.evict(1);
		preReducer.emitWindow(collector);
		preReducer.emitWindow(collector);
		preReducer.store(21);
		preReducer.emitWindow(collector);
		preReducer.evict(1);
		preReducer.emitWindow(collector);

		preReducer.store(9);

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(3));
		expected.add(StreamWindow.fromElements(9));
		expected.add(StreamWindow.fromElements(15));
		expected.add(StreamWindow.fromElements(21));
		expected.add(StreamWindow.fromElements(8));
		expected.add(StreamWindow.fromElements(8));
		expected.add(StreamWindow.fromElements(14));
		expected.add(StreamWindow.fromElements(14));
		expected.add(StreamWindow.fromElements(21));

		assertEquals(expected, collector.getCollected());
	}

	private static class SumReducer implements ReduceFunction<Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}

	}

	private static class TupleSumReducer implements ReduceFunction<Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
			return new Tuple2<Integer, Integer>(value1.f0, value1.f1 + value2.f1);
		}

	}

}
