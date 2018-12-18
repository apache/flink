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

package org.apache.flink.test.state.operator.restore.unkeyed;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.state.operator.restore.ExecutionMode;

import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

/**
 * Savepoint generator to create the savepoint used by the {@link AbstractNonKeyedOperatorRestoreTestBase}.
 * Switch to specific version branches and run this job to create savepoints of different Flink versions.
 *
 * <p>The job should be cancelled manually through the REST API using the cancel-with-savepoint operation.
 */
public class NonKeyedJob {

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		String savepointsPath = pt.getRequired("savepoint-path");

		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointsPath);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(RestartStrategies.noRestart());

		env.setStateBackend(new MemoryStateBackend());

		/**
		 * Source -> StatefulMap1 -> CHAIN(StatefulMap2 -> Map -> StatefulMap3)
		 */
		DataStream<Integer> source = createSource(env, ExecutionMode.GENERATE);

		SingleOutputStreamOperator<Integer> first = createFirstStatefulMap(ExecutionMode.GENERATE, source);
		first.startNewChain();

		SingleOutputStreamOperator<Integer> second = createSecondStatefulMap(ExecutionMode.GENERATE, first);
		second.startNewChain();

		SingleOutputStreamOperator<Integer> stateless = createStatelessMap(second);

		SingleOutputStreamOperator<Integer> third = createThirdStatefulMap(ExecutionMode.GENERATE, stateless);

		env.execute("job");
	}

	public static SingleOutputStreamOperator<Integer> createSource(StreamExecutionEnvironment env, ExecutionMode mode) {
		return env.addSource(new IntegerSource(mode))
			.setParallelism(4);
	}

	public static SingleOutputStreamOperator<Integer> createFirstStatefulMap(ExecutionMode mode, DataStream<Integer> input) {
		return input
			.map(new StatefulStringStoringMap(mode, "first"))
			.setParallelism(4)
			.uid("first");
	}

	public static SingleOutputStreamOperator<Integer> createSecondStatefulMap(ExecutionMode mode, DataStream<Integer> input) {
		return input
			.map(new StatefulStringStoringMap(mode, "second"))
			.setParallelism(4)
			.uid("second");
	}

	public static SingleOutputStreamOperator<Integer> createThirdStatefulMap(ExecutionMode mode, DataStream<Integer> input) {
		SingleOutputStreamOperator<Integer> map = input
			.map(new StatefulStringStoringMap(mode, "third"))
			.setParallelism(4)
			.uid("third");

		return map;
	}

	public static SingleOutputStreamOperator<Integer> createStatelessMap(DataStream<Integer> input) {
		return input.map(new NoOpMapFunction())
			.setParallelism(4);
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

	private static class NoOpMapFunction implements MapFunction<Integer, Integer> {

		private static final long serialVersionUID = 6584823409744624276L;

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}
	}

	private static final class IntegerSource extends RichParallelSourceFunction<Integer> {

		private static final long serialVersionUID = 1912878510707871659L;
		private final ExecutionMode mode;

		private volatile boolean running = true;

		private IntegerSource(ExecutionMode mode) {
			this.mode = mode;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			ctx.collect(1);

			switch (mode) {
				case GENERATE:
				case MIGRATE:
					// keep the job running until cancel-with-savepoint was done
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
}
