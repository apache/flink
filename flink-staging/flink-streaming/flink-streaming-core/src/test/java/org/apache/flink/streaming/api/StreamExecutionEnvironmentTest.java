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

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SplittableIterator;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertTrue;

public class StreamExecutionEnvironmentTest {

	private static final long MEMORYSIZE = 32;
	private static int PARALLELISM = 4;

	@Test
	@SuppressWarnings("unchecked")
	public void testFromCollectionParallelism() {
		TypeInformation<Object> typeInfo = TypeExtractor.getForClass(Object.class);
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		boolean seenExpectedException = false;

		try {
			DataStream<Object> dataStream1 = env.fromCollection(new DummySplittableIterator(), typeInfo)
					.setParallelism(4);
		} catch (IllegalArgumentException e) {
			seenExpectedException = true;
		}

		DataStream<Object> dataStream2 = env.fromParallelCollection(new DummySplittableIterator(), typeInfo)
				.setParallelism(4);

		String plan = env.getExecutionPlan();

		assertTrue("Expected Exception for setting parallelism was not thrown.", seenExpectedException);
		assertTrue("Parallelism for dataStream1 is not right.",
				plan.contains("\"contents\":\"Collection Source\",\"parallelism\":1"));
		assertTrue("Parallelism for dataStream2 is not right.",
				plan.contains("\"contents\":\"Parallel Collection Source\",\"parallelism\":4"));
	}

	@Test
	public void testGenerateSequenceParallelism() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		boolean seenExpectedException = false;

		try {
			DataStream<Long> dataStream1 = env.generateSequence(0,0).setParallelism(4);
		} catch (IllegalArgumentException e) {
			seenExpectedException = true;
		}

		DataStream<Long> dataStream2 = env.generateParallelSequence(0,0).setParallelism(4);

		String plan = env.getExecutionPlan();

		assertTrue("Expected Exception for setting parallelism was not thrown.", seenExpectedException);
		assertTrue("Parallelism for dataStream1 is not right.",
				plan.contains("\"contents\":\"Sequence Source\",\"parallelism\":1"));
		assertTrue("Parallelism for dataStream2 is not right.",
				plan.contains("\"contents\":\"Parallel Sequence Source\",\"parallelism\":4"));
	}

	public static class DummySplittableIterator extends SplittableIterator {
		private static final long serialVersionUID = 1312752876092210499L;

		@Override
		public Iterator[] split(int numPartitions) {
			return new Iterator[0];
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return 0;
		}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Object next() {
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
