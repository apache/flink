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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.streaming.api.functions.source.FromSplittableIteratorFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.SplittableIterator;
import org.junit.Test;

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
			DataStream<Long> dataStream1 = env.generateSequence(0, 0).setParallelism(4);
		} catch (IllegalArgumentException e) {
			seenExpectedException = true;
		}

		DataStream<Long> dataStream2 = env.generateParallelSequence(0, 0).setParallelism(4);

		String plan = env.getExecutionPlan();

		assertTrue("Expected Exception for setting parallelism was not thrown.", seenExpectedException);
		assertTrue("Parallelism for dataStream1 is not right.",
				plan.contains("\"contents\":\"Sequence Source\",\"parallelism\":1"));
		assertTrue("Parallelism for dataStream2 is not right.",
				plan.contains("\"contents\":\"Parallel Sequence Source\",\"parallelism\":4"));
	}

	@Test
	public void testSources() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		SourceFunction<Integer> srcFun = new SourceFunction<Integer>() {

			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		};
		DataStreamSource<Integer> src1 = env.addSource(srcFun);
		assertEquals(srcFun, getFunctionForDataSource(src1));

		List<Long> list = Arrays.asList(0L, 1L, 2L);

		DataStreamSource<Long> src2 = env.generateSequence(0, 2);
		assertTrue(getFunctionForDataSource(src2) instanceof FromIteratorFunction);

		DataStreamSource<Long> src3 = env.fromElements(0L, 1L, 2L);
		assertTrue(getFunctionForDataSource(src3) instanceof FromElementsFunction);

		DataStreamSource<Long> src4 = env.fromCollection(list);
		assertTrue(getFunctionForDataSource(src4) instanceof FromElementsFunction);

		DataStreamSource<Long> src5 = env.generateParallelSequence(0, 2);
		assertTrue(getFunctionForDataSource(src5) instanceof FromSplittableIteratorFunction);
	}

	/////////////////////////////////////////////////////////////
	// Utilities
	/////////////////////////////////////////////////////////////


	private static StreamOperator<?> getOperatorForDataStream(DataStream<?> dataStream) {
		StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
		StreamGraph streamGraph = env.getStreamGraph();
		return streamGraph.getStreamNode(dataStream.getId()).getOperator();
	}

	private static <T> SourceFunction<T> getFunctionForDataSource(DataStreamSource<T> dataStreamSource) {
		AbstractUdfStreamOperator<?, ?> operator =
				(AbstractUdfStreamOperator<?, ?>) getOperatorForDataStream(dataStreamSource);
		return (SourceFunction<T>) operator.getUserFunction();
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
