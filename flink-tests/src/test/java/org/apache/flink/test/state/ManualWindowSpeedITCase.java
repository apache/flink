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

package org.apache.flink.test.state;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Random;

/**
 * A collection of manual tests that serve to assess the performance of windowed operations. These
 * run in local mode with parallelism 1 with a source that emits data as fast as possible. Thus,
 * these mostly test the performance of the state backend.
 *
 * <p>When doing a release we should manually run theses tests on the version that is to be released
 * and on older version to see if there are performance regressions.
 *
 * <p>When a test is executed it will output how many elements of key {@code "Tuple 0"} have
 * been processed in each window. This gives an estimate of the throughput.
 */
@Ignore
public class ManualWindowSpeedITCase extends AbstractTestBase {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testTumblingIngestionTimeWindowsWithFsBackend() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(1);

		String checkpoints = tempFolder.newFolder().toURI().toString();
		env.setStateBackend(new FsStateBackend(checkpoints));

		env.addSource(new InfiniteTupleSource(1_000))
				.keyBy(0)
				.timeWindow(Time.seconds(3))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				})
				.filter(new FilterFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						return value.f0.startsWith("Tuple 0");
					}
				})
				.print();

		env.execute();
	}

	@Test
	public void testTumblingIngestionTimeWindowsWithFsBackendWithLateness() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(1);

		String checkpoints = tempFolder.newFolder().toURI().toString();
		env.setStateBackend(new FsStateBackend(checkpoints));

		env.addSource(new InfiniteTupleSource(10_000))
				.keyBy(0)
				.timeWindow(Time.seconds(3))
				.allowedLateness(Time.seconds(1))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				})
				.filter(new FilterFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						return value.f0.startsWith("Tuple 0");
					}
				})
				.print();

		env.execute();
	}

	@Test
	public void testTumblingIngestionTimeWindowsWithRocksDBBackend() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(1);

		env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));

		env.addSource(new InfiniteTupleSource(10_000))
				.keyBy(0)
				.timeWindow(Time.seconds(3))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				})
				.filter(new FilterFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						return value.f0.startsWith("Tuple 0");
					}
				})
				.print();

		env.execute();
	}

	@Test
	public void testTumblingIngestionTimeWindowsWithRocksDBBackendWithLateness() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(1);

		env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));

		env.addSource(new InfiniteTupleSource(10_000))
				.keyBy(0)
				.timeWindow(Time.seconds(3))
				.allowedLateness(Time.seconds(1))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				})
				.filter(new FilterFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						return value.f0.startsWith("Tuple 0");
					}
				})
				.print();

		env.execute();
	}

	/**
	 * A source that emits elements with a fixed set of keys as fast as possible. Used for
	 * rough performance estimation.
	 */
	public static class InfiniteTupleSource implements ParallelSourceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private int numKeys;

		private volatile boolean running = true;

		public InfiniteTupleSource(int numKeys) {
			this.numKeys = numKeys;
		}

		@Override
		public void run(SourceContext<Tuple2<String, Integer>> out) throws Exception {
			Random random = new Random(42);
			while (running) {
				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>("Tuple " + (random.nextInt(numKeys)), 1);
				out.collect(tuple);
			}
		}

		@Override
		public void cancel() {
			this.running = false;
		}
	}
}
