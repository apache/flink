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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.util.NoOpSink;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.SplittableIterator;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamExecutionEnvironmentTest extends StreamingMultipleProgramsTestBase {

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

	@Test
	public void testAutomaticTypeRegistration() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//we have to disable this option so that getRegisteredKryoTypes() does not return a copy of the currently
		//registered types
		env.getConfig().disableForceKryo();

		DeduplicatorAccessor stream = new DeduplicatorAccessor(env);

		//verify registration for sources
		DataStreamSource<DummyCustomClass> input = env.addSource(new SourceFunction<DummyCustomClass>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void run(SourceContext<DummyCustomClass> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		});
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyCustomClass.class));
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		//verify registration for transform calls with typeinfo present
		input.map(new MapFunction<DummyCustomClass, DummyCustomClass>() {
			@Override
			public DummyCustomClass map(DummyCustomClass value) throws Exception {
				return new DummyCustomClass(0);
			}
		});
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyCustomClass.class));
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		//verify registration for transform calls without typeinfo passed via returns()
		input.map(new MapFunctionWithTypeErased()).returns(DummyCustomClass.class);
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyCustomClass.class));

		//verify that classes are not added multiple times
		input.map(new MapFunctionWithTypeErased()).returns(DummyCustomClass.class);
		assertEquals(1, env.getConfig().getRegisteredKryoTypes().size());
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		//verify registartion for KeySelector within KeyedStreams
		input.keyBy(new KeySelector<DummyCustomClass, DummyKeyClass>() {
			@Override
			public DummyKeyClass getKey(DummyCustomClass value) throws Exception {
				return new DummyKeyClass(0);
			}
		});
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyKeyClass.class));
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		//verify registration for KeySelector within JoinedStreams
		JoinedStreams<DummyCustomClass, DummyCustomClass>.Where<DummyKeyClass> where = input.join(input).where(new KeySelector<DummyCustomClass, DummyKeyClass>() {
			@Override
			public DummyKeyClass getKey(DummyCustomClass value) throws Exception {
				return new DummyKeyClass(0);
			}
		});
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyKeyClass.class));
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		JoinedStreams<DummyCustomClass, DummyCustomClass>.Where<DummyKeyClass>.EqualTo equalTo = where.equalTo(new KeySelector<DummyCustomClass, DummyKeyClass>() {
			@Override
			public DummyKeyClass getKey(DummyCustomClass value) throws Exception {
				return new DummyKeyClass(0);
			}
		});
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyKeyClass.class));
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		//verify registration for KeySelector within CoGroupedStreams
		CoGroupedStreams<DummyCustomClass, DummyCustomClass>.Where<DummyKeyClass> whereCG = input.coGroup(input).where(new KeySelector<DummyCustomClass, DummyKeyClass>() {
			@Override
			public DummyKeyClass getKey(DummyCustomClass value) throws Exception {
				return new DummyKeyClass(0);
			}
		});
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyKeyClass.class));
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		CoGroupedStreams<DummyCustomClass, DummyCustomClass>.Where<DummyKeyClass>.EqualTo equalToCG = whereCG.equalTo(new KeySelector<DummyCustomClass, DummyKeyClass>() {
			@Override
			public DummyKeyClass getKey(DummyCustomClass value) throws Exception {
				return new DummyKeyClass(0);
			}
		});
		assertTrue(env.getConfig().getRegisteredKryoTypes().contains(DummyKeyClass.class));
		env.getConfig().getRegisteredKryoTypes().clear();
		stream.clearDuplicates();

		//sanity check to make sure registered type were removed properly
		assertEquals(0, env.getConfig().getRegisteredKryoTypes().size());
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

	/////////////////////////////////////////////////////////////
	// Type Registration Check Utilities
	/////////////////////////////////////////////////////////////

	public static class DeduplicatorAccessor extends DataStream<Object> {
		public DeduplicatorAccessor(StreamExecutionEnvironment environment) {
			super(environment, new DummyTransformation());
		}

		public void clearDuplicates() {
			deduplicator.clear();
		}
	}

	public static class DummyTransformation extends StreamTransformation<Object> {
		public DummyTransformation() {
			super("dummy", getForClass(Object.class), 1);
		}

		@Override
		public void setChainingStrategy(ChainingStrategy strategy) {
		}

		@Override
		public Collection<StreamTransformation<?>> getTransitivePredecessors() {
			return null;
		}
	}

	public static class DummyCustomClass {
		private final int index;

		public DummyCustomClass(int index) {
			this.index = index;
		}
	}

	public static class DummyKeyClass {
		private final int index;

		public DummyKeyClass(int index) {
			this.index = index;
		}
	}

	public static class MapFunctionWithTypeErased<IN, OUT> implements MapFunction<IN, OUT> {
		@Override
		public OUT map(IN value) throws Exception {
			return (OUT) new DummyCustomClass(0);
		}
	}
}
