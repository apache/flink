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
import org.apache.flink.streaming.api.operators.windowing.ParallelGroupedMerge;
import org.apache.flink.streaming.api.operators.windowing.ParallelMerge;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestCollector;
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

		TestCollector<StreamWindow<Integer>> out = new TestCollector<StreamWindow<Integer>>();
		List<StreamWindow<Integer>> output = out.getCollected();

		ParallelMerge<Integer> merger = new ParallelMerge<Integer>(reducer);
		merger.numberOfDiscretizers = 2;

		merger.flatMap1(createTestWindow(1), out);
		merger.flatMap1(createTestWindow(1), out);
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), out);
		assertTrue(output.isEmpty());
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), out);
		assertEquals(StreamWindow.fromElements(2), output.get(0));

		merger.flatMap2(new Tuple2<Integer, Integer>(2, 2), out);
		merger.flatMap1(createTestWindow(2), out);
		merger.flatMap1(createTestWindow(2), out);
		merger.flatMap2(new Tuple2<Integer, Integer>(2, 1), out);
		assertEquals(1, output.size());
		merger.flatMap1(createTestWindow(2), out);
		assertEquals(StreamWindow.fromElements(3), output.get(1));

		// check error handling
		merger.flatMap1(createTestWindow(3), out);
		merger.flatMap2(new Tuple2<Integer, Integer>(3, 1), out);
		merger.flatMap2(new Tuple2<Integer, Integer>(3, 1), out);

		merger.flatMap2(new Tuple2<Integer, Integer>(4, 1), out);
		merger.flatMap2(new Tuple2<Integer, Integer>(4, 1), out);
		merger.flatMap1(createTestWindow(4), out);
		try {
			merger.flatMap1(createTestWindow(4), out);
			fail();
		} catch (RuntimeException e) {
			// Do nothing
		}

		ParallelMerge<Integer> merger2 = new ParallelMerge<Integer>(reducer);
		merger2.numberOfDiscretizers = 2;
		merger2.flatMap1(createTestWindow(0), out);
		merger2.flatMap1(createTestWindow(1), out);
		merger2.flatMap1(createTestWindow(1), out);
		merger2.flatMap2(new Tuple2<Integer, Integer>(1, 1), out);
		try {
			merger2.flatMap2(new Tuple2<Integer, Integer>(1, 1), out);
			fail();
		} catch (RuntimeException e) {
			// Do nothing
		}

	}

	@Test
	public void groupedTest() throws Exception {

		TestCollector<StreamWindow<Integer>> out = new TestCollector<StreamWindow<Integer>>();
		List<StreamWindow<Integer>> output = out.getCollected();

		ParallelMerge<Integer> merger = new ParallelGroupedMerge<Integer>();
		merger.numberOfDiscretizers = 2;

		merger.flatMap1(createTestWindow(1), out);
		merger.flatMap1(createTestWindow(1), out);
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), out);
		assertTrue(output.isEmpty());
		merger.flatMap2(new Tuple2<Integer, Integer>(1, 1), out);
		assertEquals(StreamWindow.fromElements(1, 1), output.get(0));
	}

	private StreamWindow<Integer> createTestWindow(Integer id) {
		StreamWindow<Integer> ret = new StreamWindow<Integer>(id);
		ret.add(1);
		return ret;
	}
}
