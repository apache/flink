/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

public class StreamingOperatorsITCase extends StreamingMultipleProgramsTestBase {

	private String resultPath1;
	private String resultPath2;
	private String expected1;
	private String expected2;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception {
		resultPath1 = tempFolder.newFile().toURI().toString();
		resultPath2 = tempFolder.newFile().toURI().toString();
		expected1 = "";
		expected2 = "";
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected1, resultPath1);
		compareResultsByLinesInMemory(expected2, resultPath2);
	}

	/**
	 * Tests the proper functioning of the streaming fold operator. For this purpose, a stream
	 * of Tuple2<Integer, Integer> is created. The stream is grouped according to the first tuple
	 * value. Each group is folded where the second tuple value is summed up.
	 *
	 * @throws Exception
	 */
	@Test
	public void testFoldOperation() throws Exception {
		int numElements = 10;
		int numKeys = 2;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Integer, Integer>> sourceStream = env.addSource(new TupleSource(numElements, numKeys));

		SplitStream<Tuple2<Integer, Integer>> splittedResult = sourceStream
			.keyBy(0)
			.fold(0, new FoldFunction<Tuple2<Integer, Integer>, Integer>() {
				@Override
				public Integer fold(Integer accumulator, Tuple2<Integer, Integer> value) throws Exception {
					return accumulator + value.f1;
				}
			}).map(new RichMapFunction<Integer, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(Integer value) throws Exception {
					return new Tuple2<Integer, Integer>(getRuntimeContext().getIndexOfThisSubtask(), value);
				}
			}).split(new OutputSelector<Tuple2<Integer, Integer>>() {
				@Override
				public Iterable<String> select(Tuple2<Integer, Integer> value) {
					List<String> output = new ArrayList<>();

					output.add(value.f0 + "");

					return output;
				}
			});

		splittedResult.select("0").map(new MapFunction<Tuple2<Integer,Integer>, Integer>() {
			@Override
			public Integer map(Tuple2<Integer, Integer> value) throws Exception {
				return value.f1;
			}
		}).writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

		splittedResult.select("1").map(new MapFunction<Tuple2<Integer, Integer>, Integer>() {
			@Override
			public Integer map(Tuple2<Integer, Integer> value) throws Exception {
				return value.f1;
			}
		}).writeAsText(resultPath2, FileSystem.WriteMode.OVERWRITE);

		StringBuilder builder1 = new StringBuilder();
		StringBuilder builder2 = new StringBuilder();
		int counter1 = 0;
		int counter2 = 0;

		for (int i = 0; i < numElements; i++) {
			if (i % 2 == 0) {
				counter1 += i;
				builder1.append(counter1 + "\n");
			} else {
				counter2 += i;
				builder2.append(counter2 + "\n");
			}
		}

		expected1 = builder1.toString();
		expected2 = builder2.toString();

		env.execute();
	}

	/**
	 * Tests whether the fold operation can also be called with non Java serializable types.
	 */
	@Test
	public void testFoldOperationWithNonJavaSerializableType() throws Exception {
		final int numElements = 10;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Integer, NonSerializable>> input = env.addSource(new NonSerializableTupleSource(numElements));

		input
			.keyBy(0)
			.fold(
				new NonSerializable(42),
				new FoldFunction<Tuple2<Integer, NonSerializable>, NonSerializable>() {
					@Override
					public NonSerializable fold(NonSerializable accumulator, Tuple2<Integer, NonSerializable> value) throws Exception {
						return new NonSerializable(accumulator.value + value.f1.value);
					}
			})
			.map(new MapFunction<NonSerializable, Integer>() {
				@Override
				public Integer map(NonSerializable value) throws Exception {
					return value.value;
				}
			})
			.writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE);

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < numElements; i++) {
			builder.append(42 + i + "\n");
		}

		expected1 = builder.toString();

		env.execute();
	}

	private static class NonSerializable {
		// This makes the type non-serializable
		private final Object obj = new Object();

		private final int value;

		public NonSerializable(int value) {
			this.value = value;
		}
	}

	private static class NonSerializableTupleSource implements SourceFunction<Tuple2<Integer, NonSerializable>> {
		private final int numElements;

		public NonSerializableTupleSource(int numElements) {
			this.numElements = numElements;
		}


		@Override
		public void run(SourceContext<Tuple2<Integer, NonSerializable>> ctx) throws Exception {
			for (int i = 0; i < numElements; i++) {
				ctx.collect(new Tuple2<Integer, NonSerializable>(i, new NonSerializable(i)));
			}
		}

		@Override
		public void cancel() {}
	}

	private static class TupleSource implements SourceFunction<Tuple2<Integer, Integer>> {

		private final int numElements;
		private final int numKeys;

		public TupleSource(int numElements, int numKeys) {
			this.numElements = numElements;
			this.numKeys = numKeys;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			for (int i = 0; i < numElements; i++) {
				Tuple2<Integer, Integer> result = new Tuple2<>(i % numKeys, i);
				ctx.collect(result);
			}
		}

		@Override
		public void cancel() {

		}
	}
}
