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
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.equalTo;

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
	private static final String NUM_OUTPUTS = "outputs";
	private static final String NUM_OUT_OF_ORDER = "outOfOrder";
	private static final String NUM_FAILURES = "failures";
	private static final String NUM_DUPLICATES = "duplicates";
	private static final String NUM_LOST = "lost";
	private static final Logger LOG = LoggerFactory.getLogger(UnalignedCheckpointITCase.class);
	// keep in sync with FailingMapper in #createDAG
	private static final int EXPECTED_FAILURES = 5;

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

		long minCheckpoints = 8;
		createDAG(env, minCheckpoints, slotSharing);
		final JobExecutionResult result = env.execute();

		collector.checkThat(result.<Long>getAccumulatorResult(NUM_OUT_OF_ORDER), equalTo(0L));
		collector.checkThat(result.<Long>getAccumulatorResult(NUM_DUPLICATES), equalTo(0L));
		collector.checkThat(result.<Long>getAccumulatorResult(NUM_LOST), equalTo(0L));
		collector.checkThat(result.<Integer>getAccumulatorResult(NUM_FAILURES), equalTo(EXPECTED_FAILURES));
	}

	@Nonnull
	private LocalStreamEnvironment createEnv(int parallelism, int slotsPerTaskManager, boolean slotSharing) throws IOException {
		Configuration conf = new Configuration();
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, .9f);
		conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4kb"));
		final int taskManagers = slotSharing ? (parallelism + slotsPerTaskManager - 1) / slotsPerTaskManager : parallelism * 3;
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, taskManagers);
		final int numBuffers = 3 * slotsPerTaskManager * slotsPerTaskManager * taskManagers * 3;
		conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse(numBuffers * 4 + "kb"));

		conf.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temp.newFolder().toURI().toString());

		final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
		env.enableCheckpointing(100);
		env.setParallelism(parallelism);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(EXPECTED_FAILURES, Time.milliseconds(100)));
		env.getCheckpointConfig().enableUnalignedCheckpoints(true);
		return env;
	}

	private void createDAG(StreamExecutionEnvironment env, long minCheckpoints, boolean slotSharing) {
		env.fromSource(new LongSource(minCheckpoints, env.getParallelism()), WatermarkStrategy.noWatermarks(), "source")
			.slotSharingGroup(slotSharing ? "default" : "source")
			// shifts records from one partition to another evenly to retain order
			.partitionCustom(new ShiftingPartitioner(), l -> l)
			.map(new FailingMapper(state -> state.completedCheckpoints >= minCheckpoints / 4 && state.runNumber == 0
					|| state.completedCheckpoints >= minCheckpoints * 3 / 4 && state.runNumber == 2,
				state -> state.completedCheckpoints >= minCheckpoints / 2 && state.runNumber == 1,
				state -> state.runNumber == 3,
				state -> state.runNumber == 4))
			.slotSharingGroup(slotSharing ? "default" : "map")
			.partitionCustom(new DistributingPartitioner(), l -> l)
			.addSink(new VerifyingSink(minCheckpoints))
			.slotSharingGroup(slotSharing ? "default" : "sink");
	}

	private static class LongSource implements Source<Long, LongSource.LongSplit, List<LongSource.LongSplit>> {
		private final long minCheckpoints;
		private final int numSplits;

		private LongSource(long minCheckpoints, int numSplits) {
			this.minCheckpoints = minCheckpoints;
			this.numSplits = numSplits;
		}

		@Override
		public Boundedness getBoundedness() {
			return Boundedness.CONTINUOUS_UNBOUNDED;
		}

		@Override
		public SourceReader<Long, LongSplit> createReader(SourceReaderContext readerContext) {
			return new LongSourceReader(minCheckpoints);
		}

		@Override
		public SplitEnumerator<LongSplit, List<LongSource.LongSplit>> createEnumerator(SplitEnumeratorContext<LongSplit> enumContext) {
			List<LongSplit> splits = IntStream.range(0, numSplits)
				.mapToObj(i -> new LongSplit(i, numSplits, 0))
				.collect(Collectors.toList());
			return new LongSplitSplitEnumerator(enumContext, splits);
		}

		@Override
		public SplitEnumerator<LongSplit, List<LongSource.LongSplit>> restoreEnumerator(SplitEnumeratorContext<LongSplit> enumContext, List<LongSource.LongSplit> checkpoint) {
			return new LongSplitSplitEnumerator(enumContext, checkpoint);
		}

		@Override
		public SimpleVersionedSerializer<LongSplit> getSplitSerializer() {
			return new SplitVersionedSerializer();
		}

		@Override
		public SimpleVersionedSerializer<List<LongSplit>> getEnumeratorCheckpointSerializer() {
			return new EnumeratorVersionedSerializer();
		}

		private static class LongSourceReader implements SourceReader<Long, LongSplit> {

			private final long minCheckpoints;
			private final LongCounter numInputsCounter = new LongCounter();
			private LongSplit split;

			public LongSourceReader(final long minCheckpoints) {
				// there is currently no way to know when a checkpoint succeeded in sources, so add #expected failures
				this.minCheckpoints = minCheckpoints + EXPECTED_FAILURES;
			}

			private void info(String description, Object... args) {
				LOG.info(description + " @ {} subtask (? attempt)",
					ArrayUtils.addAll(args, split.nextNumber % split.increment));
			}

			@Override
			public void start() {
			}

			@Override
			public InputStatus pollNext(ReaderOutput<Long> output) {
				if (split == null) {
					return InputStatus.NOTHING_AVAILABLE;
				}

				output.collect(split.nextNumber);
				split.nextNumber += split.increment;
				return split.numCompletedCheckpoints >= minCheckpoints ? InputStatus.END_OF_INPUT : InputStatus.MORE_AVAILABLE;
			}

			@Override
			public List<LongSplit> snapshotState() {
				if (split == null) {
					return Collections.emptyList();
				}
				info("Snapshotted next input {}", split.nextNumber);
				split.numCompletedCheckpoints++;
				return singletonList(split);
			}

			@Override
			public CompletableFuture<Void> isAvailable() {
				return FutureUtils.completedVoidFuture();
			}

			@Override
			public void addSplits(List<LongSplit> splits) {
				split = Iterables.getOnlyElement(splits);
			}

			@Override
			public void handleSourceEvents(SourceEvent sourceEvent) {
			}

			@Override
			public void close() throws Exception {
				numInputsCounter.add(split.nextNumber / split.increment);
			}
		}

		private static class LongSplit implements SourceSplit {
			private final int increment;
			private long nextNumber;
			private long numCompletedCheckpoints;

			public LongSplit(long nextNumber, int increment, long numCompletedCheckpoints) {
				this.nextNumber = nextNumber;
				this.increment = increment;
				this.numCompletedCheckpoints = numCompletedCheckpoints;
			}

			@Override
			public String splitId() {
				return String.valueOf(increment);
			}
		}

		private static class LongSplitSplitEnumerator implements SplitEnumerator<LongSplit, List<LongSplit>> {
			private final SplitEnumeratorContext<LongSplit> context;
			private final List<LongSplit> unassignedSplits;

			private LongSplitSplitEnumerator(SplitEnumeratorContext<LongSplit> context, List<LongSplit> unassignedSplits) {
				this.context = context;
				this.unassignedSplits = new ArrayList<>(unassignedSplits);
			}

			@Override
			public void start() {
			}

			@Override
			public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
			}

			@Override
			public void addSplitsBack(List<LongSplit> splits, int subtaskId) {
				unassignedSplits.addAll(splits);
			}

			@Override
			public void addReader(int subtaskId) {
				if (context.registeredReaders().size() == context.currentParallelism()) {
					int numReaders = context.registeredReaders().size();
					Map<Integer, List<LongSplit>> assignment = new HashMap<>();
					for (int i = 0; i < unassignedSplits.size(); i++) {
						assignment
							.computeIfAbsent(i % numReaders, t -> new ArrayList<>())
							.add(unassignedSplits.get(i));
					}
					context.assignSplits(new SplitsAssignment<>(assignment));
					unassignedSplits.clear();
				}
			}

			@Override
			public List<LongSplit> snapshotState() throws Exception {
				return unassignedSplits;
			}

			@Override
			public void close() throws IOException {
			}
		}

		private static class EnumeratorVersionedSerializer implements SimpleVersionedSerializer<List<LongSplit>> {
			@Override
			public int getVersion() {
				return 0;
			}

			@Override
			public byte[] serialize(List<LongSplit> splits) throws IOException {
				final byte[] bytes = new byte[20 * splits.size()];
				for (final LongSplit split : splits) {
					ByteBuffer.wrap(bytes).putLong(split.nextNumber).putInt(split.increment).putLong(split.numCompletedCheckpoints);
				}
				return bytes;
			}

			@Override
			public List<LongSplit> deserialize(int version, byte[] serialized) {
				final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
				final ArrayList<LongSplit> splits = new ArrayList<>();
				while (byteBuffer.hasRemaining()) {
					splits.add(new LongSplit(byteBuffer.getLong(), byteBuffer.getInt(), byteBuffer.getLong()));
				}
				return splits;
			}
		}

		private static class SplitVersionedSerializer implements SimpleVersionedSerializer<LongSplit> {
			@Override
			public int getVersion() {
				return 0;
			}

			@Override
			public byte[] serialize(LongSplit split) {
				final byte[] bytes = new byte[20];
				ByteBuffer.wrap(bytes).putLong(split.nextNumber).putInt(split.increment).putLong(split.numCompletedCheckpoints);
				return bytes;
			}

			@Override
			public LongSplit deserialize(int version, byte[] serialized) {
				final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
				return new LongSplit(byteBuffer.getLong(), byteBuffer.getInt(), byteBuffer.getLong());
			}
		}
	}

	static void info(RuntimeContext runtimeContext, String description, Object[] args) {
		LOG.info(description + " @ {} subtask ({} attempt)",
				ArrayUtils.addAll(args, runtimeContext.getIndexOfThisSubtask(), runtimeContext.getAttemptNumber()));
	}

	private static class VerifyingSink extends RichSinkFunction<Long> implements CheckpointedFunction, CheckpointListener {
		private final LongCounter numOutputCounter = new LongCounter();
		private final LongCounter outOfOrderCounter = new LongCounter();
		private final LongCounter lostCounter = new LongCounter();
		private final LongCounter duplicatesCounter = new LongCounter();
		private final IntCounter numFailures = new IntCounter();
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
			getRuntimeContext().addAccumulator(NUM_FAILURES, numFailures);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			stateList = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
			state = getOnlyElement(stateList.get(), new State(getRuntimeContext().getNumberOfParallelSubtasks()));
			info("Initialized last snapshotted records {}", Arrays.asList(state.lastRecordInPartitions));
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			stateList.clear();
			stateList.add(state);
			info("Last snapshotted records {}", Arrays.asList(state.lastRecordInPartitions));
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			state.completedCheckpoints++;
		}

		@Override
		public void close() throws Exception {
			numOutputCounter.add(state.numOutput);
			outOfOrderCounter.add(state.numOutOfOrderness);
			duplicatesCounter.add(state.numDuplicates);
			lostCounter.add(state.numLostValues);
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				numFailures.add(getRuntimeContext().getAttemptNumber());
			}
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

			if (state.completedCheckpoints < minCheckpoints) {
				// induce heavy backpressure until enough checkpoints have been written
				Thread.sleep(0, 100_000);
			}
			// after all checkpoints have been completed, the remaining data should be flushed out fairly quickly
		}

		private void info(String description, Object... args) {
			UnalignedCheckpointITCase.info(getRuntimeContext(), description, args);
		}

		private static class State {
			private long numOutOfOrderness;
			private long numLostValues;
			private long numDuplicates;
			private long numOutput = 0;
			private final long[] lastRecordInPartitions;
			private long completedCheckpoints;

			private State(int numberOfParallelSubtasks) {
				lastRecordInPartitions = new long[numberOfParallelSubtasks];
				Arrays.fill(lastRecordInPartitions, -1);
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
		public void notifyCheckpointAborted(long checkpointId) {
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
