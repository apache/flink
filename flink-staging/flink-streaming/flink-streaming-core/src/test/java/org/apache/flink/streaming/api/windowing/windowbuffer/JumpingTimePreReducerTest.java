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

import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestOutput;
import org.junit.Test;

public class JumpingTimePreReducerTest {

	TypeSerializer<Integer> serializer = TypeExtractor.getForObject(1).createSerializer(null);

	ReduceFunction<Integer> reducer = new SumReducer();

	@Test
	public void testEmitWindow() throws Exception {

		TestOutput<StreamWindow<Integer>> collector = new TestOutput<StreamWindow<Integer>>();
		List<StreamWindow<Integer>> collected = collector.getCollected();

		WindowBuffer<Integer> wb = new JumpingTimePreReducer<Integer>(
				reducer, serializer, 3, 2, new TimestampWrapper<Integer>(new Timestamp<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public long getTimestamp(Integer value) {
				return value;
			}
		}, 1));

		wb.store(1);
		wb.store(2);
		wb.store(3);
		wb.evict(1);
		wb.emitWindow(collector);

		assertEquals(1, collected.size());
		assertEquals(StreamWindow.fromElements(5),
				collected.get(0));

		wb.store(4);
		wb.store(5);

		// Nothing should happen here
		wb.evict(2);

		wb.store(6);

		wb.emitWindow(collector);
		wb.evict(2);
		wb.emitWindow(collector);
		wb.store(12);
		wb.emitWindow(collector);

		assertEquals(3, collected.size());
		assertEquals(StreamWindow.fromElements(11),
				collected.get(1));
		assertEquals(StreamWindow.fromElements(12),
				collected.get(2));
	}

	private static class SumReducer implements ReduceFunction<Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}
	}
}
