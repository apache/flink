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
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.operators.windowing.WindowingITCase;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestOutput;
import org.junit.Test;

public class SlidingCountGroupedPreReducerTest {

	TypeSerializer<Integer> serializer = TypeExtractor.getForObject(1).createSerializer(null);

	ReduceFunction<Integer> reducer = new SumReducer();

	KeySelector<Integer, ?> key = new WindowingITCase.ModKey(2);

	@Test
	public void testPreReduce1() throws Exception {
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountGroupedPreReducer<Integer> preReducer = new SlidingCountGroupedPreReducer<Integer>(
				reducer, serializer, key, 3, 2, 0);

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
		preReducer.store(9);
		preReducer.store(10);
		preReducer.emitWindow(collector);
		preReducer.evict(2);
		preReducer.store(11);
		preReducer.store(12);
		preReducer.emitWindow(collector);
		preReducer.store(13);

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(1, 2));
		expected.add(StreamWindow.fromElements(3, 6));
		expected.add(StreamWindow.fromElements(5, 10));
		expected.add(StreamWindow.fromElements(7, 14));
		expected.add(StreamWindow.fromElements(9, 18));
		expected.add(StreamWindow.fromElements(11, 22));

		checkResults(expected, collector.getCollected());
	}

	@Test
	public void testPreReduce2() throws Exception {
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountGroupedPreReducer<Integer> preReducer = new SlidingCountGroupedPreReducer<Integer>(
				reducer, serializer, key, 5, 2, 0);

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
		expected.add(StreamWindow.fromElements(1, 2));
		expected.add(StreamWindow.fromElements(4, 6));
		expected.add(StreamWindow.fromElements(12, 8));
		expected.add(StreamWindow.fromElements(18, 12));
		expected.add(StreamWindow.fromElements(24, 16));
		expected.add(StreamWindow.fromElements(30, 20));

		checkResults(expected, collector.getCollected());
	}

	@Test
	public void testPreReduce3() throws Exception {
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountGroupedPreReducer<Integer> preReducer = new SlidingCountGroupedPreReducer<Integer>(
				reducer, serializer, key, 6, 3, 0);

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
		expected.add(StreamWindow.fromElements(2, 4));
		expected.add(StreamWindow.fromElements(9, 12));
		expected.add(StreamWindow.fromElements(21, 18));
		expected.add(StreamWindow.fromElements(30, 27));

		checkResults(expected, collector.getCollected());
	}

	@Test
	public void testPreReduce4() throws Exception {
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountGroupedPreReducer<Integer> preReducer = new SlidingCountGroupedPreReducer<Integer>(
				reducer, serializer, key, 5, 1, 2);

		preReducer.store(1);
		preReducer.evict(1);
		preReducer.store(1);
		preReducer.evict(1);
		preReducer.store(1);
		preReducer.emitWindow(collector);
		preReducer.store(2);
		preReducer.emitWindow(collector);
		preReducer.store(3);
		preReducer.emitWindow(collector);
		preReducer.store(4);
		preReducer.emitWindow(collector);
		preReducer.store(5);
		preReducer.emitWindow(collector);
		preReducer.evict(1);
		preReducer.store(6);
		preReducer.emitWindow(collector);
		preReducer.evict(1);
		preReducer.store(7);
		preReducer.emitWindow(collector);
		preReducer.evict(1);
		preReducer.store(8);
		preReducer.emitWindow(collector);
		preReducer.evict(1);

		List<StreamWindow<Integer>> expected = new ArrayList<StreamWindow<Integer>>();
		expected.add(StreamWindow.fromElements(1));
		expected.add(StreamWindow.fromElements(1, 2));
		expected.add(StreamWindow.fromElements(4, 2));
		expected.add(StreamWindow.fromElements(4, 6));
		expected.add(StreamWindow.fromElements(9, 6));
		expected.add(StreamWindow.fromElements(8, 12));
		expected.add(StreamWindow.fromElements(15, 10));
		expected.add(StreamWindow.fromElements(12, 18));

		checkResults(expected, collector.getCollected());
	}

	private static class SumReducer implements ReduceFunction<Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}

	}


	protected static void checkResults(List<StreamWindow<Integer>> expected,
			List<StreamWindow<Integer>> actual) {

		for (StreamWindow<Integer> sw : expected) {
			Collections.sort(sw);
		}

		for (StreamWindow<Integer> sw : actual) {
			Collections.sort(sw);
		}

		assertEquals(expected, actual);
	}
}
