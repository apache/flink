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

package org.apache.flink.test.state.operator.restore.keyed;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.test.state.operator.restore.ExecutionMode;
import org.apache.flink.util.Collector;

import org.junit.Assert;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Savepoint generator to create the savepoint used by the {@link AbstractKeyedOperatorRestoreTestBase}.
 * Switch to specific version branches and run this job to create savepoints of different Flink versions.
 *
 * <p>The job should be cancelled manually through the REST API using the cancel-with-savepoint operation.
 */
public class KeyedJob {

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		String savepointsPath = pt.getRequired("savepoint-path");

		Configuration config = new Configuration();
		config.setString(CoreOptions.SAVEPOINT_DIRECTORY, savepointsPath);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(RestartStrategies.noRestart());

		env.setStateBackend(new MemoryStateBackend());

		/**
		 * Source -> keyBy -> C(Window -> StatefulMap1 -> StatefulMap2)
		 */

		SingleOutputStreamOperator<Tuple2<Integer, Integer>> source = createIntegerTupleSource(env, ExecutionMode.GENERATE);

		SingleOutputStreamOperator<Integer> window = createWindowFunction(ExecutionMode.GENERATE, source);

		SingleOutputStreamOperator<Integer> first = createFirstStatefulMap(ExecutionMode.GENERATE, window);

		SingleOutputStreamOperator<Integer> second = createSecondStatefulMap(ExecutionMode.GENERATE, first);

		env.execute("job");
	}

	public static SingleOutputStreamOperator<Tuple2<Integer, Integer>> createIntegerTupleSource(StreamExecutionEnvironment env, ExecutionMode mode) {
		return env.addSource(new IntegerTupleSource(mode));
	}

	public static SingleOutputStreamOperator<Integer> createWindowFunction(ExecutionMode mode, DataStream<Tuple2<Integer, Integer>> input) {
		return input
			.keyBy(0)
			.countWindow(1)
			.apply(new StatefulWindowFunction(mode))
			.setParallelism(4)
			.uid("window");
	}

	public static SingleOutputStreamOperator<Integer> createFirstStatefulMap(ExecutionMode mode, DataStream<Integer> input) {
		SingleOutputStreamOperator<Integer> map = input
			.map(new StatefulStringStoringMap(mode, "first"))
			.setParallelism(4)
			.uid("first");

		return map;
	}

	public static SingleOutputStreamOperator<Integer> createSecondStatefulMap(ExecutionMode mode, DataStream<Integer> input) {
		SingleOutputStreamOperator<Integer> map = input
			.map(new StatefulStringStoringMap(mode, "second"))
			.setParallelism(4)
			.uid("second");

		return map;
	}

	private static final class IntegerTupleSource extends RichSourceFunction<Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1912878510707871659L;
		private final ExecutionMode mode;

		private boolean running = true;

		private IntegerTupleSource(ExecutionMode mode) {
			this.mode = mode;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			for (int x = 0; x < 10; x++) {
				ctx.collect(new Tuple2<>(x, x));
			}

			switch (mode) {
				case GENERATE:
				case MIGRATE:
					synchronized (this) {
						while (running) {
							this.wait();
						}
					}
			}
		}

		@Override
		public void cancel() {
			synchronized (this) {
				running = false;
				this.notifyAll();
			}
		}
	}

	private static final class StatefulWindowFunction extends RichWindowFunction<Tuple2<Integer, Integer>, Integer, Tuple, GlobalWindow> {

		private static final long serialVersionUID = -7236313076792964055L;

		private final ExecutionMode mode;
		private transient ListState<Integer> state;

		private boolean applyCalled = false;

		private StatefulWindowFunction(ExecutionMode mode) {
			this.mode = mode;
		}

		@Override
		public void open(Configuration config) {
			this.state = getRuntimeContext().getListState(new ListStateDescriptor<>("values", Integer.class));
		}

		@Override
		public void apply(Tuple key, GlobalWindow window, Iterable<Tuple2<Integer, Integer>> values, Collector<Integer> out) throws Exception {
			// fail-safe to make sure apply is actually called
			applyCalled = true;
			switch (mode) {
				case GENERATE:
					for (Tuple2<Integer, Integer> value : values) {
						state.add(value.f1);
					}
					break;
				case MIGRATE:
				case RESTORE:
					Iterator<Tuple2<Integer, Integer>> input = values.iterator();
					Iterator<Integer> restored = state.get().iterator();
					while (input.hasNext() && restored.hasNext()) {
						Tuple2<Integer, Integer> value = input.next();
						Integer rValue = restored.next();
						Assert.assertEquals(rValue, value.f1);
					}
					Assert.assertEquals(restored.hasNext(), input.hasNext());
			}
		}

		@Override
		public void close() {
			Assert.assertTrue("Apply was never called.", applyCalled);
		}
	}

	private static class StatefulStringStoringMap extends RichMapFunction<Integer, Integer> implements ListCheckpointed<String> {

		private static final long serialVersionUID = 6092985758425330235L;
		private final ExecutionMode mode;
		private final String valueToStore;

		private StatefulStringStoringMap(ExecutionMode mode, String valueToStore) {
			this.mode = mode;
			this.valueToStore = valueToStore;
		}

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}

		@Override
		public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Arrays.asList(valueToStore + getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public void restoreState(List<String> state) throws Exception {
			switch (mode) {
				case GENERATE:
					break;
				case MIGRATE:
				case RESTORE:
					Assert.assertEquals("Failed for " + valueToStore + getRuntimeContext().getIndexOfThisSubtask(), 1, state.size());
					String value = state.get(0);
					Assert.assertEquals(valueToStore + getRuntimeContext().getIndexOfThisSubtask(), value);
			}
		}
	}
}
