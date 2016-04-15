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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.util.NoOpSink;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.SplittableIterator;
import org.junit.Test;

public class StreamExecutionEnvironmentTest extends StreamingMultipleProgramsTestBase {

	@Test
	public void fromElementsWithBaseTypeTest1() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(ParentClass.class, new SubClass(1, "Java"), new ParentClass(1, "hello"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void fromElementsWithBaseTypeTest2() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(SubClass.class, new SubClass(1, "Java"), new ParentClass(1, "hello"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFromCollectionParallelism() {
		try {
			TypeInformation<Integer> typeInfo = BasicTypeInfo.INT_TYPE_INFO;
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			DataStreamSource<Integer> dataStream1 = env.fromCollection(new DummySplittableIterator<Integer>(), typeInfo);

			try {
				dataStream1.setParallelism(4);
				fail("should throw an exception");
			}
			catch (IllegalArgumentException e) {
				// expected
			}

			dataStream1.addSink(new NoOpSink<Integer>());
	
			DataStreamSource<Integer> dataStream2 = env.fromParallelCollection(new DummySplittableIterator<Integer>(),
					typeInfo).setParallelism(4);

			dataStream2.addSink(new NoOpSink<Integer>());

			String plan = env.getExecutionPlan();

			assertEquals("Parallelism of collection source must be 1.", 1, env.getStreamGraph().getStreamNode(dataStream1.getId()).getParallelism());
			assertEquals("Parallelism of parallel collection source must be 4.",
					4,
					env.getStreamGraph().getStreamNode(dataStream2.getId()).getParallelism());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSources() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SourceFunction<Integer> srcFun = new SourceFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		};
		DataStreamSource<Integer> src1 = env.addSource(srcFun);
		src1.addSink(new NoOpSink<Integer>());
		assertEquals(srcFun, getFunctionFromDataSource(src1));

		List<Long> list = Arrays.asList(0L, 1L, 2L);

		DataStreamSource<Long> src2 = env.generateSequence(0, 2);
		assertTrue(getFunctionFromDataSource(src2) instanceof StatefulSequenceSource);

		DataStreamSource<Long> src3 = env.fromElements(0L, 1L, 2L);
		assertTrue(getFunctionFromDataSource(src3) instanceof FromElementsFunction);

		DataStreamSource<Long> src4 = env.fromCollection(list);
		assertTrue(getFunctionFromDataSource(src4) instanceof FromElementsFunction);
	}

	/////////////////////////////////////////////////////////////
	// Utilities
	/////////////////////////////////////////////////////////////


	private static StreamOperator<?> getOperatorFromDataStream(DataStream<?> dataStream) {
		StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
		StreamGraph streamGraph = env.getStreamGraph();
		return streamGraph.getStreamNode(dataStream.getId()).getOperator();
	}

	private static <T> SourceFunction<T> getFunctionFromDataSource(DataStreamSource<T> dataStreamSource) {
		dataStreamSource.addSink(new NoOpSink<T>());
		AbstractUdfStreamOperator<?, ?> operator =
				(AbstractUdfStreamOperator<?, ?>) getOperatorFromDataStream(dataStreamSource);
		return (SourceFunction<T>) operator.getUserFunction();
	}

	public static class DummySplittableIterator<T> extends SplittableIterator<T> {
		private static final long serialVersionUID = 1312752876092210499L;

		@SuppressWarnings("unchecked")
		@Override
		public Iterator<T>[] split(int numPartitions) {
			return (Iterator<T>[]) new Iterator<?>[0];
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
		public T next() {
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public static class ParentClass {
		int num;
		String string;
		public ParentClass(int num, String string) {
			this.num = num;
			this.string = string;
		}
	}

	public static class SubClass extends ParentClass{
		public SubClass(int num, String string) {
			super(num, string);
		}
	}
}
