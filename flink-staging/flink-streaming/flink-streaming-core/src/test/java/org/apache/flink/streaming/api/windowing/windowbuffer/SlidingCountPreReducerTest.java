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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestOutput;
import org.junit.Test;

public class SlidingCountPreReducerTest {

	TypeSerializer<Integer> serializer = TypeExtractor.getForObject(1).createSerializer(null);

	ReduceFunction<Integer> reducer = new SumReducer();

	@Test
	public void testPreReduce1() throws Exception {
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountPreReducer<Integer> preReducer = new SlidingCountPreReducer<Integer>(reducer,
				serializer, 3, 2, 0);

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
		expected.add(StreamWindow.fromElements(3));
		expected.add(StreamWindow.fromElements(9));
		expected.add(StreamWindow.fromElements(15));
		expected.add(StreamWindow.fromElements(21));
		expected.add(StreamWindow.fromElements(27));
		expected.add(StreamWindow.fromElements(33));

		assertEquals(expected, collector.getCollected());
	}

	@Test
	public void testPreReduce2() throws Exception {
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountPreReducer<Integer> preReducer = new SlidingCountPreReducer<Integer>(reducer,
				serializer, 5, 2, 0);

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
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountPreReducer<Integer> preReducer = new SlidingCountPreReducer<Integer>(reducer,
				serializer, 6, 3, 0);

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
		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();

		SlidingCountPreReducer<Integer> preReducer = new SlidingCountPreReducer<Integer>(reducer,
				serializer, 5, 1, 2);

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
		expected.add(StreamWindow.fromElements(3));
		expected.add(StreamWindow.fromElements(6));
		expected.add(StreamWindow.fromElements(10));
		expected.add(StreamWindow.fromElements(15));
		expected.add(StreamWindow.fromElements(20));
		expected.add(StreamWindow.fromElements(25));
		expected.add(StreamWindow.fromElements(30));

		assertEquals(expected, collector.getCollected());
	}

	private static class SumReducer implements ReduceFunction<Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}

	}

}
