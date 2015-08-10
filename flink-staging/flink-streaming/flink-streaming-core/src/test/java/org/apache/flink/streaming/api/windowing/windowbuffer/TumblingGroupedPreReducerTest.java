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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestOutput;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.junit.Test;

public class TumblingGroupedPreReducerTest {

	TypeInformation<Tuple2<Integer, Integer>> type = TypeExtractor
			.getForObject(new Tuple2<Integer, Integer>(1, 1));
	TypeSerializer<Tuple2<Integer, Integer>> serializer = type.createSerializer(null);

	KeySelector<Tuple2<Integer, Integer>, ?> key = KeySelectorUtil.getSelectorForKeys(
			new Keys.ExpressionKeys<Tuple2<Integer, Integer>>(new int[] { 0 }, type), type, null);

	Reducer reducer = new Reducer();

	@SuppressWarnings("unchecked")
	@Test
	public void testEmitWindow() throws Exception {

		List<Tuple2<Integer, Integer>> inputs = new ArrayList<Tuple2<Integer, Integer>>();
		inputs.add(new Tuple2<Integer, Integer>(1, 1));
		inputs.add(new Tuple2<Integer, Integer>(0, 0));
		inputs.add(new Tuple2<Integer, Integer>(1, -1));
		inputs.add(new Tuple2<Integer, Integer>(1, -2));

		TestOutput<StreamWindow<Tuple2<Integer, Integer>>> collector = new TestOutput<StreamWindow<Tuple2<Integer, Integer>>>();
		List<StreamWindow<Tuple2<Integer, Integer>>> collected = collector.getCollected();

		WindowBuffer<Tuple2<Integer, Integer>> wb = new TumblingGroupedPreReducer<Tuple2<Integer, Integer>>(
				reducer, key, serializer);

		wb.store(serializer.copy(inputs.get(0)));
		wb.store(serializer.copy(inputs.get(1)));
		wb.emitWindow(collector);
		wb.evict(2);

		assertEquals(1, collected.size());

		assertSetEquals(StreamWindow.fromElements(new Tuple2<Integer, Integer>(1, 1),
				new Tuple2<Integer, Integer>(0, 0)), collected.get(0));

		wb.store(serializer.copy(inputs.get(0)));
		wb.store(serializer.copy(inputs.get(1)));
		wb.store(serializer.copy(inputs.get(2)));

		wb.store(serializer.copy(inputs.get(3)));

		wb.emitWindow(collector);
		wb.evict(4);

		assertEquals(2, collected.size());

		assertSetEquals(StreamWindow.fromElements(new Tuple2<Integer, Integer>(3, -2),
				new Tuple2<Integer, Integer>(0, 0)), collected.get(1));

		// Test whether function is mutating inputs or not
		assertEquals(2, reducer.allInputs.size());
		assertEquals(reducer.allInputs.get(0), inputs.get(2));
		assertEquals(reducer.allInputs.get(1), inputs.get(3));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEmitWindow2() throws Exception {

		List<Tuple2<Integer, Integer>> inputs = new ArrayList<Tuple2<Integer, Integer>>();
		inputs.add(new Tuple2<Integer, Integer>(1, 1));
		inputs.add(new Tuple2<Integer, Integer>(0, 0));
		inputs.add(new Tuple2<Integer, Integer>(1, -1));
		inputs.add(new Tuple2<Integer, Integer>(1, -2));

		TestOutput<StreamWindow<Tuple2<Integer, Integer>>> collector = new TestOutput<StreamWindow<Tuple2<Integer, Integer>>>();
		List<StreamWindow<Tuple2<Integer, Integer>>> collected = collector.getCollected();

		WindowBuffer<Tuple2<Integer, Integer>> wb = new TumblingGroupedPreReducer<Tuple2<Integer, Integer>>(
				reducer, key, serializer).sequentialID();

		wb.store(serializer.copy(inputs.get(0)));
		wb.store(serializer.copy(inputs.get(1)));
		wb.emitWindow(collector);
		wb.evict(2);
		
		assertSetEquals(StreamWindow.fromElements(inputs.get(0), inputs.get(1)), collected.get(0));
		
		wb.store(serializer.copy(inputs.get(0)));
		wb.store(serializer.copy(inputs.get(1)));
		wb.store(serializer.copy(inputs.get(2)));
		wb.emitWindow(collector);
		wb.evict(3);
		
		assertSetEquals(StreamWindow.fromElements(new Tuple2<Integer, Integer>(2, 0), inputs.get(1)), collected.get(1));

		
	}

	private static <T> void assertSetEquals(Collection<T> first, Collection<T> second) {
		assertEquals(new HashSet<T>(first), new HashSet<T>(second));
	}

	@SuppressWarnings("serial")
	private class Reducer implements ReduceFunction<Tuple2<Integer, Integer>> {

		public List<Tuple2<Integer, Integer>> allInputs = new ArrayList<Tuple2<Integer, Integer>>();

		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1,
				Tuple2<Integer, Integer> value2) throws Exception {
			allInputs.add(value2);
			value1.f0 = value1.f0 + value2.f0;
			value1.f1 = value1.f1 + value2.f1;
			return value1;
		}

	}

}
