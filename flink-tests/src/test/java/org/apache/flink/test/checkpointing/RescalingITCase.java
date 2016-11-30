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

package org.apache.flink.test.checkpointing;

import io.netty.util.internal.ConcurrentSet;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TODO : parameterize to test all different state backends!
 */
public class RescalingITCase extends TestLogger {

	private static final int numTaskManagers = 2;
	private static final int slotsPerTaskManager = 2;
	private static final int numSlots = numTaskManagers * slotsPerTaskManager;

	enum OperatorCheckpointMethod {
		NON_PARTITIONED, CHECKPOINTED_FUNCTION, LIST_CHECKPOINTED
	}

	private static TestingCluster cluster;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slotsPerTaskManager);

		final File checkpointDir = temporaryFolder.newFolder();
		final File savepointDir = temporaryFolder.newFolder();

		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, checkpointDir.toURI().toString());
		config.setString(ConfigConstants.SAVEPOINT_DIRECTORY_KEY, savepointDir.toURI().toString());

		cluster = new TestingCluster(config);
		cluster.start();
	}

	@AfterClass
	public static void teardown() {
		if (cluster != null) {
			cluster.shutdown();
			cluster.awaitTermination();
		}
	}

	@Test
	public void testSavepointRescalingInKeyedState() throws Exception {
		testSavepointRescalingKeyedState(false);
	}

	@Test
	public void testSavepointRescalingOutKeyedState() throws Exception {
		testSavepointRescalingKeyedState(true);
	}

	/**
	 * Tests that a a job with purely keyed state can be restarted from a savepoint
	 * with a different parallelism.
	 */
	public void testSavepointRescalingKeyedState(boolean scaleOut) throws Exception {
		final int numberKeys = 42;
		final int numberElements = 1000;
		final int numberElements2 = 500;
		final int parallelism = scaleOut ? numSlots / 2 : numSlots;
		final int parallelism2 = scaleOut ? numSlots : numSlots / 2;
		final int maxParallelism = 13;

		FiniteDuration timeout = new FiniteDuration(3, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = null;
		JobID jobID = null;

		try {
			jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createJobGraphWithKeyedState(parallelism, maxParallelism, numberKeys, numberElements, false, 100);

			jobID = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// wait til the sources have emitted numberElements for each key and completed a checkpoint
			SubtaskIndexFlatMapper.workCompletedLatch.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			// verify the current state

			Set<Tuple2<Integer, Integer>> actualResult = CollectionSink.getElementsSet();

			Set<Tuple2<Integer, Integer>> expectedResult = new HashSet<>();

			for (int key = 0; key < numberKeys; key++) {
				int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);

				expectedResult.add(Tuple2.of(KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroupIndex), numberElements * key));
			}

			assertEquals(expectedResult, actualResult);

			// clear the CollectionSink set for the restarted job
			CollectionSink.clearElementsSet();

			Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobID, Option.<String>empty()), deadline.timeLeft());

			final String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess)
				Await.result(savepointPathFuture, deadline.timeLeft())).savepointPath();

			Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), deadline.timeLeft());

			Future<Object> cancellationResponseFuture = jobManager.ask(new JobManagerMessages.CancelJob(jobID), deadline.timeLeft());

			Object cancellationResponse = Await.result(cancellationResponseFuture, deadline.timeLeft());

			assertTrue(cancellationResponse instanceof JobManagerMessages.CancellationSuccess);

			Await.ready(jobRemovedFuture, deadline.timeLeft());

			jobID = null;

			JobGraph scaledJobGraph = createJobGraphWithKeyedState(parallelism2, maxParallelism, numberKeys, numberElements2, true, 100);

			scaledJobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

			jobID = scaledJobGraph.getJobID();

			cluster.submitJobAndWait(scaledJobGraph, false);

			jobID = null;

			Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

			Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

			for (int key = 0; key < numberKeys; key++) {
				int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
				expectedResult2.add(Tuple2.of(KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism2, keyGroupIndex), key * (numberElements + numberElements2)));
			}

			assertEquals(expectedResult2, actualResult2);


		} finally {
			// clear the CollectionSink set for the restarted job
			CollectionSink.clearElementsSet();

			// clear any left overs from a possibly failed job
			if (jobID != null && jobManager != null) {
				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);

				try {
					Await.ready(jobRemovedFuture, timeout);
				} catch (TimeoutException | InterruptedException ie) {
					fail("Failed while cleaning up the cluster.");
				}
			}
		}
	}

	/**
	 * Tests that a job cannot be restarted from a savepoint with a different parallelism if the
	 * rescaled operator has non-partitioned state.
	 *
	 * @throws Exception
	 */
	@Test
	public void testSavepointRescalingNonPartitionedStateCausesException() throws Exception {
		final int parallelism = numSlots / 2;
		final int parallelism2 = numSlots;
		final int maxParallelism = 13;

		FiniteDuration timeout = new FiniteDuration(3, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		JobID jobID = null;
		ActorGateway jobManager = null;

		try {
			jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createJobGraphWithOperatorState(parallelism, maxParallelism, OperatorCheckpointMethod.NON_PARTITIONED);

			jobID = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			Object savepointResponse = null;

			// wait until the operator is started
			StateSourceBase.workStartedLatch.await();

			Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobID, Option.<String>empty()), deadline.timeLeft());
			FiniteDuration waitingTime = new FiniteDuration(10, TimeUnit.SECONDS);
			savepointResponse = Await.result(savepointPathFuture, waitingTime);

			assertTrue(String.valueOf(savepointResponse), savepointResponse instanceof JobManagerMessages.TriggerSavepointSuccess);

			final String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess)savepointResponse).savepointPath();

			Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), deadline.timeLeft());

			Future<Object> cancellationResponseFuture = jobManager.ask(new JobManagerMessages.CancelJob(jobID), deadline.timeLeft());

			Object cancellationResponse = Await.result(cancellationResponseFuture, deadline.timeLeft());

			assertTrue(cancellationResponse instanceof JobManagerMessages.CancellationSuccess);

			Await.ready(jobRemovedFuture, deadline.timeLeft());

			// job successfully removed
			jobID = null;

			JobGraph scaledJobGraph = createJobGraphWithOperatorState(parallelism2, maxParallelism, OperatorCheckpointMethod.NON_PARTITIONED);

			scaledJobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

			jobID = scaledJobGraph.getJobID();

			cluster.submitJobAndWait(scaledJobGraph, false);

			jobID = null;

		} catch (JobExecutionException exception) {
			if (exception.getCause() instanceof IllegalStateException) {
				// we expect a IllegalStateException wrapped
				// in a JobExecutionException, because the job containing non-partitioned state
				// is being rescaled
			} else {
				throw exception;
			}
		} finally {
			// clear any left overs from a possibly failed job
			if (jobID != null && jobManager != null) {
				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);

				try {
					Await.ready(jobRemovedFuture, timeout);
				} catch (TimeoutException | InterruptedException ie) {
					fail("Failed while cleaning up the cluster.");
				}
			}
		}
	}

	/**
	 * Tests that a job with non partitioned state can be restarted from a savepoint with a
	 * different parallelism if the operator with non-partitioned state are not rescaled.
	 *
	 * @throws Exception
	 */
	@Test
	public void testSavepointRescalingWithKeyedAndNonPartitionedState() throws Exception {
		int numberKeys = 42;
		int numberElements = 1000;
		int numberElements2 = 500;
		int parallelism = numSlots / 2;
		int parallelism2 = numSlots;
		int maxParallelism = 13;

		FiniteDuration timeout = new FiniteDuration(3, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		ActorGateway jobManager = null;
		JobID jobID = null;

		try {
			 jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createJobGraphWithKeyedAndNonPartitionedOperatorState(
				parallelism,
				maxParallelism,
				parallelism,
				numberKeys,
				numberElements,
				false,
				100);

			jobID = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// wait til the sources have emitted numberElements for each key and completed a checkpoint
			SubtaskIndexFlatMapper.workCompletedLatch.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			// verify the current state

			Set<Tuple2<Integer, Integer>> actualResult = CollectionSink.getElementsSet();

			Set<Tuple2<Integer, Integer>> expectedResult = new HashSet<>();

			for (int key = 0; key < numberKeys; key++) {
				int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);

				expectedResult.add(Tuple2.of(KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroupIndex) , numberElements * key));
			}

			assertEquals(expectedResult, actualResult);

			// clear the CollectionSink set for the restarted job
			CollectionSink.clearElementsSet();

			Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobID, Option.<String>empty()), deadline.timeLeft());

			final String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess)
				Await.result(savepointPathFuture, deadline.timeLeft())).savepointPath();

			Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), deadline.timeLeft());

			Future<Object> cancellationResponseFuture = jobManager.ask(new JobManagerMessages.CancelJob(jobID), deadline.timeLeft());

			Object cancellationResponse = Await.result(cancellationResponseFuture, deadline.timeLeft());

			assertTrue(cancellationResponse instanceof JobManagerMessages.CancellationSuccess);

			Await.ready(jobRemovedFuture, deadline.timeLeft());

			jobID = null;

			JobGraph scaledJobGraph = createJobGraphWithKeyedAndNonPartitionedOperatorState(
				parallelism2,
				maxParallelism,
				parallelism,
				numberKeys,
				numberElements + numberElements2,
				true,
				100);

			scaledJobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

			jobID = scaledJobGraph.getJobID();

			cluster.submitJobAndWait(scaledJobGraph, false);

			jobID = null;

			Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

			Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

			for (int key = 0; key < numberKeys; key++) {
				int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
				expectedResult2.add(Tuple2.of(KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism2, keyGroupIndex), key * (numberElements + numberElements2)));
			}

			assertEquals(expectedResult2, actualResult2);

		} finally {
			// clear the CollectionSink set for the restarted job
			CollectionSink.clearElementsSet();

			// clear any left overs from a possibly failed job
			if (jobID != null && jobManager != null) {
				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);

				try {
					Await.ready(jobRemovedFuture, timeout);
				} catch (TimeoutException | InterruptedException ie) {
					fail("Failed while cleaning up the cluster.");
				}
			}
		}
	}

	@Test
	public void testSavepointRescalingInPartitionedOperatorState() throws Exception {
		testSavepointRescalingPartitionedOperatorState(false, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION);
	}

	@Test
	public void testSavepointRescalingOutPartitionedOperatorState() throws Exception {
		testSavepointRescalingPartitionedOperatorState(true, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION);
	}

	@Test
	public void testSavepointRescalingInPartitionedOperatorStateList() throws Exception {
		testSavepointRescalingPartitionedOperatorState(false, OperatorCheckpointMethod.LIST_CHECKPOINTED);
	}

	@Test
	public void testSavepointRescalingOutPartitionedOperatorStateList() throws Exception {
		testSavepointRescalingPartitionedOperatorState(true, OperatorCheckpointMethod.LIST_CHECKPOINTED);
	}


	/**
	 * Tests rescaling of partitioned operator state. More specific, we test the mechanism with {@link ListCheckpointed}
	 * as it subsumes {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction}.
	 */
	public void testSavepointRescalingPartitionedOperatorState(boolean scaleOut, OperatorCheckpointMethod checkpointMethod) throws Exception {
		final int parallelism = scaleOut ? numSlots : numSlots / 2;
		final int parallelism2 = scaleOut ? numSlots / 2 : numSlots;
		final int maxParallelism = 13;

		FiniteDuration timeout = new FiniteDuration(3, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		JobID jobID = null;
		ActorGateway jobManager = null;

		int counterSize = Math.max(parallelism, parallelism2);

		if(checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION) {
			PartitionedStateSource.CHECK_CORRECT_SNAPSHOT = new int[counterSize];
			PartitionedStateSource.CHECK_CORRECT_RESTORE = new int[counterSize];
		} else {
			PartitionedStateSourceListCheckpointed.CHECK_CORRECT_SNAPSHOT = new int[counterSize];
			PartitionedStateSourceListCheckpointed.CHECK_CORRECT_RESTORE = new int[counterSize];
		}

		try {
			jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createJobGraphWithOperatorState(parallelism, maxParallelism, checkpointMethod);

			jobID = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			Object savepointResponse = null;

			// wait until the operator is started
			StateSourceBase.workStartedLatch.await();

			while (deadline.hasTimeLeft()) {

				Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobID, Option.<String>empty()), deadline.timeLeft());
				FiniteDuration waitingTime = new FiniteDuration(10, TimeUnit.SECONDS);
				savepointResponse = Await.result(savepointPathFuture, waitingTime);

				if (savepointResponse instanceof JobManagerMessages.TriggerSavepointSuccess) {
					break;
				}
			}

			assertTrue(savepointResponse instanceof JobManagerMessages.TriggerSavepointSuccess);

			final String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess)savepointResponse).savepointPath();

			Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), deadline.timeLeft());

			Future<Object> cancellationResponseFuture = jobManager.ask(new JobManagerMessages.CancelJob(jobID), deadline.timeLeft());

			Object cancellationResponse = Await.result(cancellationResponseFuture, deadline.timeLeft());

			assertTrue(cancellationResponse instanceof JobManagerMessages.CancellationSuccess);

			Await.ready(jobRemovedFuture, deadline.timeLeft());

			// job successfully removed
			jobID = null;

			JobGraph scaledJobGraph = createJobGraphWithOperatorState(parallelism2, maxParallelism, checkpointMethod);

			scaledJobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

			jobID = scaledJobGraph.getJobID();

			cluster.submitJobAndWait(scaledJobGraph, false);

			int sumExp = 0;
			int sumAct = 0;

			if (checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION) {
				for (int c : PartitionedStateSource.CHECK_CORRECT_SNAPSHOT) {
					sumExp += c;
				}

				for (int c : PartitionedStateSource.CHECK_CORRECT_RESTORE) {
					sumAct += c;
				}
			} else {
				for (int c : PartitionedStateSourceListCheckpointed.CHECK_CORRECT_SNAPSHOT) {
					sumExp += c;
				}

				for (int c : PartitionedStateSourceListCheckpointed.CHECK_CORRECT_RESTORE) {
					sumAct += c;
				}
			}

			assertEquals(sumExp, sumAct);
			jobID = null;

		} finally {
			// clear any left overs from a possibly failed job
			if (jobID != null && jobManager != null) {
				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);

				try {
					Await.ready(jobRemovedFuture, timeout);
				} catch (TimeoutException | InterruptedException ie) {
					fail("Failed while cleaning up the cluster.");
				}
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private static JobGraph createJobGraphWithOperatorState(
			int parallelism, int maxParallelism, OperatorCheckpointMethod checkpointMethod) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.getConfig().setMaxParallelism(maxParallelism);
		env.enableCheckpointing(Long.MAX_VALUE);
		env.setRestartStrategy(RestartStrategies.noRestart());

		StateSourceBase.workStartedLatch = new CountDownLatch(parallelism);

		SourceFunction<Integer> src;

		switch (checkpointMethod) {
			case CHECKPOINTED_FUNCTION:
				src = new PartitionedStateSource();
				break;
			case LIST_CHECKPOINTED:
				src = new PartitionedStateSourceListCheckpointed();
				break;
			case NON_PARTITIONED:
				src = new NonPartitionedStateSource();
				break;
			default:
				throw new IllegalArgumentException();
		}

		DataStream<Integer> input = env.addSource(src);

		input.addSink(new DiscardingSink<Integer>());

		return env.getStreamGraph().getJobGraph();
	}

	private static JobGraph createJobGraphWithKeyedState(
		int parallelism,
		int maxParallelism,
		int numberKeys,
		int numberElements,
		boolean terminateAfterEmission,
		int checkpointingInterval) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.getConfig().setMaxParallelism(maxParallelism);
		env.enableCheckpointing(checkpointingInterval);
		env.setRestartStrategy(RestartStrategies.noRestart());

		DataStream<Integer> input = env.addSource(new SubtaskIndexSource(
			numberKeys,
			numberElements,
			terminateAfterEmission))
			.keyBy(new KeySelector<Integer, Integer>() {
				private static final long serialVersionUID = -7952298871120320940L;

				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			});

		SubtaskIndexFlatMapper.workCompletedLatch = new CountDownLatch(numberKeys);

		DataStream<Tuple2<Integer, Integer>> result = input.flatMap(new SubtaskIndexFlatMapper(numberElements));

		result.addSink(new CollectionSink<Tuple2<Integer, Integer>>());

		return env.getStreamGraph().getJobGraph();
	}

	private static JobGraph createJobGraphWithKeyedAndNonPartitionedOperatorState(
		int parallelism,
		int maxParallelism,
		int fixedParallelism,
		int numberKeys,
		int numberElements,
		boolean terminateAfterEmission,
		int checkpointingInterval) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.getConfig().setMaxParallelism(maxParallelism);
		env.enableCheckpointing(checkpointingInterval);
		env.setRestartStrategy(RestartStrategies.noRestart());

		DataStream<Integer> input = env.addSource(new SubtaskIndexNonPartitionedStateSource(
			numberKeys,
			numberElements,
			terminateAfterEmission))
			.setParallelism(fixedParallelism)
			.keyBy(new KeySelector<Integer, Integer>() {
				private static final long serialVersionUID = -7952298871120320940L;

				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			});

		SubtaskIndexFlatMapper.workCompletedLatch = new CountDownLatch(numberKeys);

		DataStream<Tuple2<Integer, Integer>> result = input.flatMap(new SubtaskIndexFlatMapper(numberElements));

		result.addSink(new CollectionSink<Tuple2<Integer, Integer>>());

		return env.getStreamGraph().getJobGraph();
	}

	private static class SubtaskIndexSource
		extends RichParallelSourceFunction<Integer> {

		private static final long serialVersionUID = -400066323594122516L;

		private final int numberKeys;
		private final int numberElements;
		private final boolean terminateAfterEmission;

		protected int counter = 0;

		private boolean running = true;

		SubtaskIndexSource(
			int numberKeys,
			int numberElements,
			boolean terminateAfterEmission) {

			this.numberKeys = numberKeys;
			this.numberElements = numberElements;
			this.terminateAfterEmission = terminateAfterEmission;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			final Object lock = ctx.getCheckpointLock();
			final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

			while (running) {

				if (counter < numberElements) {
					synchronized (lock) {
						for (int value = subtaskIndex;
							 value < numberKeys;
							 value += getRuntimeContext().getNumberOfParallelSubtasks()) {

							ctx.collect(value);
						}

						counter++;
					}
				} else {
					if (terminateAfterEmission) {
						running = false;
					} else {
						Thread.sleep(100);
					}
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class SubtaskIndexNonPartitionedStateSource extends SubtaskIndexSource implements Checkpointed<Integer> {

		private static final long serialVersionUID = 8388073059042040203L;

		SubtaskIndexNonPartitionedStateSource(int numberKeys, int numberElements, boolean terminateAfterEmission) {
			super(numberKeys, numberElements, terminateAfterEmission);
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return counter;
		}

		@Override
		public void restoreState(Integer state) throws Exception {
			counter = state;
		}
	}

	private static class SubtaskIndexFlatMapper extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>> implements CheckpointedFunction {

		private static final long serialVersionUID = 5273172591283191348L;

		private static volatile CountDownLatch workCompletedLatch = new CountDownLatch(1);

		private transient ValueState<Integer> counter;
		private transient ValueState<Integer> sum;

		private final int numberElements;

		SubtaskIndexFlatMapper(int numberElements) {
			this.numberElements = numberElements;
		}

		@Override
		public void flatMap(Integer value, Collector<Tuple2<Integer, Integer>> out) throws Exception {

			int count = counter.value() + 1;
			counter.update(count);

			int s = sum.value() + value;
			sum.update(s);

			if (count % numberElements == 0) {
				out.collect(Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), s));
				workCompletedLatch.countDown();
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			//all managed, nothing to do.
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			counter = context.getKeyedStateStore().getState(new ValueStateDescriptor<>("counter", Integer.class, 0));
			sum = context.getKeyedStateStore().getState(new ValueStateDescriptor<>("sum", Integer.class, 0));
		}
	}

	private static class CollectionSink<IN> implements SinkFunction<IN> {

		private static ConcurrentSet<Object> elements = new ConcurrentSet<Object>();

		private static final long serialVersionUID = -1652452958040267745L;

		public static <IN> Set<IN> getElementsSet() {
			return (Set<IN>) elements;
		}

		public static void clearElementsSet() {
			elements.clear();
		}

		@Override
		public void invoke(IN value) throws Exception {
			elements.add(value);
		}
	}

	private static class StateSourceBase extends RichParallelSourceFunction<Integer> {

		private static volatile CountDownLatch workStartedLatch = new CountDownLatch(1);

		protected volatile int counter = 0;
		protected volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			final Object lock = ctx.getCheckpointLock();

			while (running) {
				synchronized (lock) {
					++counter;
					ctx.collect(1);
				}

				Thread.sleep(2);
				if (counter == 10) {
					workStartedLatch.countDown();
				}

				if (counter >= 500) {
					break;
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class NonPartitionedStateSource extends StateSourceBase implements Checkpointed<Integer> {

		private static final long serialVersionUID = -8108185918123186841L;

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return counter;
		}

		@Override
		public void restoreState(Integer state) throws Exception {
			counter = state;
		}
	}

	private static class PartitionedStateSourceListCheckpointed extends StateSourceBase implements ListCheckpointed<Integer> {

		private static final long serialVersionUID = -4357864582992546L;
		private static final int NUM_PARTITIONS = 7;

		private static int[] CHECK_CORRECT_SNAPSHOT;
		private static int[] CHECK_CORRECT_RESTORE;

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {

			CHECK_CORRECT_SNAPSHOT[getRuntimeContext().getIndexOfThisSubtask()] = counter;

			int div = counter / NUM_PARTITIONS;
			int mod = counter % NUM_PARTITIONS;

			List<Integer> split = new ArrayList<>();
			for (int i = 0; i < NUM_PARTITIONS; ++i) {
				int partitionValue = div;
				if (mod > 0) {
					--mod;
					++partitionValue;
				}
				split.add(partitionValue);
			}
			return split;
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			for (Integer v : state) {
				counter += v;
			}
			CHECK_CORRECT_RESTORE[getRuntimeContext().getIndexOfThisSubtask()] = counter;
		}
	}

	private static class PartitionedStateSource extends StateSourceBase implements CheckpointedFunction {

		private static final long serialVersionUID = -359715965103593462L;
		private static final int NUM_PARTITIONS = 7;

		private ListState<Integer> counterPartitions;

		private static int[] CHECK_CORRECT_SNAPSHOT;
		private static int[] CHECK_CORRECT_RESTORE;


		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

			counterPartitions.clear();

			CHECK_CORRECT_SNAPSHOT[getRuntimeContext().getIndexOfThisSubtask()] = counter;

			int div = counter / NUM_PARTITIONS;
			int mod = counter % NUM_PARTITIONS;

			for (int i = 0; i < NUM_PARTITIONS; ++i) {
				int partitionValue = div;
				if (mod > 0) {
					--mod;
					++partitionValue;
				}
				counterPartitions.add(partitionValue);
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			this.counterPartitions =
					context.getOperatorStateStore().getSerializableListState("counter_partitions");
			if (context.isRestored()) {
				for (int v : counterPartitions.get()) {
					counter += v;
				}
				CHECK_CORRECT_RESTORE[getRuntimeContext().getIndexOfThisSubtask()] = counter;
			}
		}
	}
}
