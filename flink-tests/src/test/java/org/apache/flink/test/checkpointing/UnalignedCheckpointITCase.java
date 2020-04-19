/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test for performing the unaligned checkpoint.
 *
 * <p>This tests checks for completeness and orderness of results after recovery. In particular, the following
 * topology is used:
 * <ol>
 *     <li>A source that generates unique, monotonically increasing, equidistant longs. Given parallelism=4, the first
 *     subtask generates [0, 4, ...], the second subtask [1, 5, ...].</li>
 *     <li>A shuffle that shifts all outputs of partition i to the input of partition i+1 mod n, such that the
 *     order of records are retained.</li>
 *     <li>A failing map that fails during map, snapshotState, initializeState, and close on certain checkpoints /
 *     run attempts (more see below).</li>
 *     <li>A shuffle that fairly distributes the records deterministically, such that duplicates can be detected.</li>
 *     <li>A verifying sink that counts all anomalies and exposes counters for verification in the calling test case.</li>
 * </ol>
 *
 * <p>The tests are executed in a certain degree of parallelism until a given number of checkpoints have been
 * successfully taken. Tests time out to guard against infinite failure loops or if no successful checkpoint has been
 * taken for other reasons.
 *
 * <p>Failures are triggered on certain checkpoints to spread them evenly and run attempts to avoid infinite failure
 * loops. For the tests, the failures are induced in the 1. subtask after m&lt;n successful checkpoints with n the
 * number of successful checkpoints needed to pass the test:
 * <ol>
 *     <li>After {@code m=1/4*n}, map fails.</li>
 *     <li>After {@code m=1/2*n}, snapshotState fails.</li>
 *     <li>After {@code m=3/4*n}, map fails and the corresponding recovery fails once.</li>
 *     <li>At the end, close fails once.</li>
 * </ol>
 *
 * <p>The following verifications are performed.
 * <ul>
 *     <li>The number of outputs should be the number of inputs (no lost records).</li>
 *     <li>No record of source subtask {@code i} can overtake a record of the same subtask (orderness).</li>
 *     <li>No record should arrive at the sink twice (e.g., being emitted or recovered multiple times), which tests
 *     exactly once.</li>
 *     <li>The number of successful checkpoints is indeed {@code >=n}.</li>
 * </ul>
 */
public class UnalignedCheckpointITCase extends TestLogger {
	public static final String NUM_INPUTS = "inputs";
	public static final String NUM_OUTPUTS = "outputs";
	private static final String NUM_OUT_OF_ORDER = "outOfOrder";
	private static final String NUM_DUPLICATES = "duplicates";
	private static final String NUM_LOST = "lost";
	private static final Logger LOG = LoggerFactory.getLogger(UnalignedCheckpointITCase.class);

	@Rule
	public ErrorCollector collector = new ErrorCollector();

	@Rule
	public final TemporaryFolder temp = new TemporaryFolder();

	@Rule
	public final Timeout timeout = Timeout.builder()
			.withTimeout(300, TimeUnit.SECONDS)
			.build();

	@Test
	public void shouldPerformUnalignedCheckpointOnNonParallelLocalChannel() throws Exception {
		execute(1, 1, true);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnParallelLocalChannel() throws Exception {
		execute(5, 5, true);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnNonParallelRemoteChannel() throws Exception {
		execute(1, 1, false);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnParallelRemoteChannel() throws Exception {
		execute(5, 1, false);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnLocalAndRemoteChannel() throws Exception {
		execute(5, 3, true);
	}

	@Test
	public void shouldPerformUnalignedCheckpointMassivelyParallel() throws Exception {
		execute(20, 20, true);
	}

	private void execute(int parallelism, int slotsPerTaskManager, boolean slotSharing) throws Exception {
		StreamExecutionEnvironment env = createEnv(parallelism, slotsPerTaskManager, slotSharing);

		long minCheckpoints = 10;
		createDAG(env, minCheckpoints, slotSharing);
		final JobExecutionResult result = env.execute();

		collector.checkThat(result.<Long>getAccumulatorResult(NUM_OUT_OF_ORDER), equalTo(0L));
		collector.checkThat(result.<Long>getAccumulatorResult(NUM_DUPLICATES), equalTo(0L));
		collector.checkThat(result.<Long>getAccumulatorResult(NUM_LOST), equalTo(0L));

		// at this point, there is no way that #input != #output, but still perform these sanity checks
		Long inputs = result.<Long>getAccumulatorResult(NUM_INPUTS);
		collector.checkThat(inputs, greaterThan(0L));
		collector.checkThat(result.<Long>getAccumulatorResult(NUM_OUTPUTS), equalTo(inputs));
	}

	@Nonnull
	private LocalStreamEnvironment createEnv(int parallelism, int slotsPerTaskManager, boolean slotSharing) throws IOException {
		Configuration conf = new Configuration();
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, .9f);
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
				slotSharing ? (parallelism + slotsPerTaskManager - 1) / slotsPerTaskManager : parallelism * 3);

		conf.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temp.newFolder().toURI().toString());

		final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
		env.enableCheckpointing(100);
		// keep in sync with FailingMapper in #createDAG
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.milliseconds(100)));
		env.getCheckpointConfig().enableUnalignedCheckpoints();
		return env;
	}

	private void createDAG(StreamExecutionEnvironment env, long minCheckpoints, boolean slotSharing) {
		env.addSource(new LongSource(minCheckpoints))
			.slotSharingGroup(slotSharing ? "default" : "source")
			// shifts records from one partition to another evenly to retain order
			.partitionCustom(new ShiftingPartitioner(), l -> l)
			.map(new FailingMapper(state -> state.completedCheckpoints == minCheckpoints / 4 && state.runNumber == 0
					|| state.completedCheckpoints == minCheckpoints * 3 / 4 && state.runNumber == 2,
				state -> state.completedCheckpoints == minCheckpoints / 2 && state.runNumber == 1,
				state -> state.runNumber == 3,
				state -> state.runNumber == 4))
			.slotSharingGroup(slotSharing ? "default" : "map")
			.partitionCustom(new DistributingPartitioner(), l -> l)
			.addSink(new VerifyingSink(minCheckpoints))
			.slotSharingGroup(slotSharing ? "default" : "sink");
	}

	private static class LongSource extends RichParallelSourceFunction<Long> implements CheckpointListener,
			CheckpointedFunction {

		private final long minCheckpoints;
		private volatile boolean running = true;
		private static final ListStateDescriptor<State> STATE_DESCRIPTOR =
				new ListStateDescriptor<>("state", State.class);
		private final LongCounter numInputsCounter = new LongCounter();
		private ListState<State> stateList;
		private State state;

		public LongSource(final long minCheckpoints) {
			this.minCheckpoints = minCheckpoints;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator(NUM_INPUTS, numInputsCounter);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			stateList = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
			state = getOnlyElement(stateList.get(), new State(0, getRuntimeContext().getIndexOfThisSubtask()));
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			stateList.clear();
			stateList.add(state);
			info("Snapshotted next input {}", state.nextNumber);
		}

		private void info(String description, Object... args) {
			UnalignedCheckpointITCase.info(getRuntimeContext(), description, args);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			state.numCompletedCheckpoints++;
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			int increment = getRuntimeContext().getNumberOfParallelSubtasks();
			info("First emitted input {}", state.nextNumber);
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(state.nextNumber);
					state.nextNumber += increment;

					if (state.numCompletedCheckpoints >= minCheckpoints) {
						cancel();
					}
				}
			}

			numInputsCounter.add(state.nextNumber / increment);
			info("Last emitted input {} = {} total emits", state.nextNumber - increment, numInputsCounter.getLocalValue());
		}

		@Override
		public void cancel() {
			running = false;
		}

		private static class State {
			private long numCompletedCheckpoints;
			private long nextNumber;

			private State(long numCompletedCheckpoints, long nextNumber) {
				this.numCompletedCheckpoints = numCompletedCheckpoints;
				this.nextNumber = nextNumber;
			}
		}
	}

	static void info(RuntimeContext runtimeContext, String description, Object[] args) {
		LOG.info(description + " @ {} subtask ({} attempt)",
				ArrayUtils.addAll(args, new Object[]{runtimeContext.getIndexOfThisSubtask(), runtimeContext.getAttemptNumber()}));
	}

	private static class VerifyingSink extends RichSinkFunction<Long> implements CheckpointedFunction {
		private final LongCounter numOutputCounter = new LongCounter();
		private final LongCounter outOfOrderCounter = new LongCounter();
		private final LongCounter lostCounter = new LongCounter();
		private final LongCounter duplicatesCounter = new LongCounter();
		private static final ListStateDescriptor<State> STATE_DESCRIPTOR =
				new ListStateDescriptor<>("state", State.class);
		private ListState<State> stateList;
		private State state;
		private final long minCheckpoints;

		private VerifyingSink(long minCheckpoints) {
			this.minCheckpoints = minCheckpoints;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator(NUM_OUTPUTS, numOutputCounter);
			getRuntimeContext().addAccumulator(NUM_OUT_OF_ORDER, outOfOrderCounter);
			getRuntimeContext().addAccumulator(NUM_DUPLICATES, duplicatesCounter);
			getRuntimeContext().addAccumulator(NUM_LOST, lostCounter);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			stateList = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
			state = getOnlyElement(stateList.get(), new State(getRuntimeContext().getNumberOfParallelSubtasks()));
			info("Initialized last snapshotted records {}", Arrays.asList(state.lastRecordInPartitions));
		}

		private void info(String description, Object... args) {
			UnalignedCheckpointITCase.info(getRuntimeContext(), description, args);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			stateList.clear();
			stateList.add(state);
			info("Last snapshotted records {}", Arrays.asList(state.lastRecordInPartitions));
		}

		@Override
		public void close() throws Exception {
			numOutputCounter.add(state.numOutput);
			outOfOrderCounter.add(state.numOutOfOrderness);
			duplicatesCounter.add(state.numDuplicates);
			lostCounter.add(state.numLostValues);
			info("Last received records {}", Arrays.asList(state.lastRecordInPartitions));
			super.close();
		}

		@Override
		public void invoke(Long value, Context context) throws Exception {
			int parallelism = state.lastRecordInPartitions.length;
			int partition = (int) (value % parallelism);
			long lastRecord = state.lastRecordInPartitions[partition];
			if (value < lastRecord) {
				state.numOutOfOrderness++;
				info("Out of order records current={} and last={}", value, lastRecord);
			} else if (value == lastRecord) {
				state.numDuplicates++;
				info("Duplicate record {}", value);
			} else if (lastRecord != -1) {
				long expectedValue = lastRecord + parallelism * parallelism;
				if (value != expectedValue) {
					state.numLostValues++;
					info("Lost records {}-{}", expectedValue, value);
				}
			}
			state.lastRecordInPartitions[partition] = value;
			state.numOutput++;
		}

		private static class State {
			private long numOutOfOrderness;
			private long numLostValues;
			private long numDuplicates;
			private long numOutput = 0;
			private long[] lastRecordInPartitions;

			private State(int numberOfParallelSubtasks) {
				lastRecordInPartitions = new long[numberOfParallelSubtasks];
				for (int index = 0; index < lastRecordInPartitions.length; index++) {
					lastRecordInPartitions[index] = -1;
				}
			}
		}
	}

	private static class ShiftingPartitioner implements Partitioner<Long> {
		@Override
		public int partition(Long key, int numPartitions) {
			return (int) ((key + 1) % numPartitions);
		}
	}

	private static class DistributingPartitioner implements Partitioner<Long> {
		@Override
		public int partition(Long key, int numPartitions) {
			return (int) ((key / numPartitions) % numPartitions);
		}
	}

	private static class FailingMapperState {
		private long completedCheckpoints;
		private long runNumber;

		private FailingMapperState(long completedCheckpoints, long runNumber) {
			this.completedCheckpoints = completedCheckpoints;
			this.runNumber = runNumber;
		}
	}

	private static class FailingMapper extends RichMapFunction<Long, Long> implements CheckpointedFunction, CheckpointListener {
		private static final ListStateDescriptor<FailingMapperState> FAILING_MAPPER_STATE_DESCRIPTOR =
				new ListStateDescriptor<>("state", FailingMapperState.class);
		private ListState<FailingMapperState> listState;
		private FailingMapperState state;
		private final FilterFunction<FailingMapperState> failDuringMap;
		private final FilterFunction<FailingMapperState> failDuringSnapshot;
		private final FilterFunction<FailingMapperState> failDuringRecovery;
		private final FilterFunction<FailingMapperState> failDuringClose;
		private long lastValue;

		private FailingMapper(
				FilterFunction<FailingMapperState> failDuringMap,
				FilterFunction<FailingMapperState> failDuringSnapshot,
				FilterFunction<FailingMapperState> failDuringRecovery,
				FilterFunction<FailingMapperState> failDuringClose) {
			this.failDuringMap = failDuringMap;
			this.failDuringSnapshot = failDuringSnapshot;
			this.failDuringRecovery = failDuringRecovery;
			this.failDuringClose = failDuringClose;
		}

		@Override
		public Long map(Long value) throws Exception {
			lastValue = value;
			checkFail(failDuringMap, "map");
			return value;
		}

		public void checkFail(FilterFunction<FailingMapperState> failFunction, String description) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0 && failFunction.filter(state)) {
				failMapper(description);
			}
		}

		private void failMapper(String description) throws Exception {
			throw new Exception("Failing " + description + " @ " + state.completedCheckpoints + " (" + state.runNumber + " attempt); last value " + lastValue);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			state.completedCheckpoints++;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			checkFail(failDuringSnapshot, "snapshotState");
			listState.clear();
			listState.add(state);
		}

		@Override
		public void close() throws Exception {
			checkFail(failDuringClose, "close");
			super.close();
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			listState = context.getOperatorStateStore().getListState(FAILING_MAPPER_STATE_DESCRIPTOR);
			state = getOnlyElement(listState.get(), new FailingMapperState(0, 0));
			state.runNumber = getRuntimeContext().getAttemptNumber();
			checkFail(failDuringRecovery, "initializeState");
		}
	}
}
