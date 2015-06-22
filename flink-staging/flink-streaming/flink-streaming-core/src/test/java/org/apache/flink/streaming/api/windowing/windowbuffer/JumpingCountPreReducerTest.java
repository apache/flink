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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestOutput;
import org.junit.Test;

public class JumpingCountPreReducerTest {

	TypeSerializer<Tuple2<Integer, Integer>> serializer = TypeExtractor.getForObject(
			new Tuple2<Integer, Integer>(1, 1)).createSerializer(null);

	Reducer reducer = new Reducer();

	@SuppressWarnings("unchecked")
	@Test
	public void testEmitWindow() throws Exception {

		List<Tuple2<Integer, Integer>> inputs = new ArrayList<Tuple2<Integer, Integer>>();
		inputs.add(new Tuple2<Integer, Integer>(1, 1));
		inputs.add(new Tuple2<Integer, Integer>(2, 0));
		inputs.add(new Tuple2<Integer, Integer>(3, -1));
		inputs.add(new Tuple2<Integer, Integer>(4, -2));
		inputs.add(new Tuple2<Integer, Integer>(5, -3));

		TestOutput<StreamWindow<Tuple2<Integer, Integer>>> collector = new TestOutput<StreamWindow<Tuple2<Integer, Integer>>>();
		List<StreamWindow<Tuple2<Integer, Integer>>> collected = collector.getCollected();

		WindowBuffer<Tuple2<Integer, Integer>> wb = new JumpingCountPreReducer<Tuple2<Integer, Integer>>(
				reducer, serializer, 2);

		wb.store(serializer.copy(inputs.get(0)));
		wb.store(serializer.copy(inputs.get(1)));
		wb.store(serializer.copy(inputs.get(2)));
		wb.store(serializer.copy(inputs.get(3)));
		wb.store(serializer.copy(inputs.get(4)));

		wb.emitWindow(collector);

		assertEquals(1, collected.size());
		assertEquals(StreamWindow.fromElements(new Tuple2<Integer, Integer>(12, -6)),
				collected.get(0));

		wb.store(serializer.copy(inputs.get(0)));
		wb.store(serializer.copy(inputs.get(1)));
		wb.store(serializer.copy(inputs.get(2)));

		// Nothing should happen here
		wb.evict(3);

		wb.store(serializer.copy(inputs.get(3)));

		wb.emitWindow(collector);

		assertEquals(2, collected.size());
		assertEquals(StreamWindow.fromElements(new Tuple2<Integer, Integer>(7, -3)),
				collected.get(1));

		// Test whether function is mutating inputs or not
		assertEquals(3, reducer.allInputs.size());
		assertEquals(reducer.allInputs.get(0), inputs.get(3));
		assertEquals(reducer.allInputs.get(1), inputs.get(4));
		assertEquals(reducer.allInputs.get(2), inputs.get(3));
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
