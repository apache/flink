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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestOutput;
import org.junit.Test;

public class ParallelMergeTest {

	@Test
	public void nonGroupedTest() throws Exception {

		ReduceFunction<Integer> reducer = new ReduceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer a, Integer b) throws Exception {
				return a + b;
			}
		};

		TestOutput<StreamWindow<Integer>> output = new TestOutput<StreamWindow<Integer>>();
		TimestampedCollector<StreamWindow<Integer>> collector = new TimestampedCollector<StreamWindow<Integer>>(output);
		List<StreamWindow<Integer>> result = output.getCollected();

		ParallelMerge<Integer> merger = new ParallelMerge<Integer>(reducer);
		merger.numberOfDiscretizers = 2;

		merger.flatMap1(createTestWindow(1), collector);
		merger.flatMap1(createTestWindow(1), collector);
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), collector);
		assertTrue(result.isEmpty());
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), collector);
		assertEquals(StreamWindow.fromElements(2), result.get(0));

		merger.flatMap2(new Tuple2<Integer, Integer>(2, 2), collector);
		merger.flatMap1(createTestWindow(2), collector);
		merger.flatMap1(createTestWindow(2), collector);
		merger.flatMap2(new Tuple2<Integer, Integer>(2, 1), collector);
		assertEquals(1, result.size());
		merger.flatMap1(createTestWindow(2), collector);
		assertEquals(StreamWindow.fromElements(3), result.get(1));

		// check error handling
		merger.flatMap1(createTestWindow(3), collector);
		merger.flatMap2(new Tuple2<Integer, Integer>(3, 1), collector);
		merger.flatMap2(new Tuple2<Integer, Integer>(3, 1), collector);

		merger.flatMap2(new Tuple2<Integer, Integer>(4, 1), collector);
		merger.flatMap2(new Tuple2<Integer, Integer>(4, 1), collector);
		merger.flatMap1(createTestWindow(4), collector);
		try {
			merger.flatMap1(createTestWindow(4), collector);
			fail();
		} catch (RuntimeException e) {
			// Do nothing
		}

		ParallelMerge<Integer> merger2 = new ParallelMerge<Integer>(reducer);
		merger2.numberOfDiscretizers = 2;
		merger2.flatMap1(createTestWindow(0), collector);
		merger2.flatMap1(createTestWindow(1), collector);
		merger2.flatMap1(createTestWindow(1), collector);
		merger2.flatMap2(new Tuple2<Integer, Integer>(1, 1), collector);
		try {
			merger2.flatMap2(new Tuple2<Integer, Integer>(1, 1), collector);
			fail();
		} catch (RuntimeException e) {
			// Do nothing
		}

	}

	@Test
	public void groupedTest() throws Exception {

		TestOutput<StreamWindow<Integer>> output = new TestOutput<StreamWindow<Integer>>();
		TimestampedCollector<StreamWindow<Integer>> collector = new TimestampedCollector<StreamWindow<Integer>>(output);
		List<StreamWindow<Integer>> result = output.getCollected();

		ParallelMerge<Integer> merger = new ParallelGroupedMerge<Integer>();
		merger.numberOfDiscretizers = 2;

		merger.flatMap1(createTestWindow(1), collector);
		merger.flatMap1(createTestWindow(1), collector);
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), collector);
		assertTrue(result.isEmpty());
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), collector);
		assertEquals(StreamWindow.fromElements(1, 1), result.get(0));
	}

	private StreamWindow<Integer> createTestWindow(Integer id) {
		StreamWindow<Integer> ret = new StreamWindow<Integer>(id);
		ret.add(1);
		return ret;
	}
}
