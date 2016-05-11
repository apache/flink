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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.SavepointStoreFactory;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.state.HashKeyGroupAssigner;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RescalingITCase extends TestLogger {

	private int numTaskManagers = 2;
	private int slotsPerTaskManager = 2;
	private int numSlots = numTaskManagers * slotsPerTaskManager;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that a a job with partitioned state can be restarted with a different parallelism.
	 */
	@Test
	public void testRescalingWithPartitionedState() throws Exception {
		ForkableFlinkMiniCluster cluster = null;
		int numberKeys = 42;
		int numberElements = 1000;
		int numberElements2 = 500;
		int parallelism = numSlots / 2;
		int parallelism2 = numSlots;
		int maxParallelism = 13;

		Deadline deadline = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();

		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slotsPerTaskManager);

			final File checkpointDir = temporaryFolder.newFolder();
			final File savepointDir = temporaryFolder.newFolder();

			config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
			config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, checkpointDir.toURI().toString());
			config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
			config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, savepointDir.toURI().toString());

			cluster = new ForkableFlinkMiniCluster(config);
			cluster.start();

			ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createPartitionedStateJobGraph(parallelism, maxParallelism, numberKeys, numberElements, false, 100);

			JobID jobID = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// wait til the sources have emitted numberElements for each key and completed a checkpoint
			SubtaskIndexFlatMapper.workCompletedLatch.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			// verify the current state

			Set<Tuple2<Integer, Integer>> actualResult = CollectionSink.getElementsSet();

			Set<Tuple2<Integer, Integer>> expectedResult = new HashSet<>();

			HashKeyGroupAssigner<Integer> keyGroupAssigner = new HashKeyGroupAssigner<>(maxParallelism);

			for (int key = 0; key < numberKeys; key++) {
				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(key);

				expectedResult.add(Tuple2.of(keyGroupIndex % parallelism, numberElements * key));
			}

			assertEquals(expectedResult, actualResult);

			// clear the CollectionSink set for the restarted job
			CollectionSink.clearElementsSet();

			Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobID), deadline.timeLeft());

			final String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess)
				Await.result(savepointPathFuture, deadline.timeLeft())).savepointPath();

			Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), deadline.timeLeft());

			Future<Object> cancellationResponseFuture = jobManager.ask(new JobManagerMessages.CancelJob(jobID), deadline.timeLeft());

			Object cancellationResponse = Await.result(cancellationResponseFuture, deadline.timeLeft());

			assertTrue(cancellationResponse instanceof JobManagerMessages.CancellationSuccess);

			Await.ready(jobRemovedFuture, deadline.timeLeft());

			JobGraph scaledJobGraph = createPartitionedStateJobGraph(parallelism2, maxParallelism, numberKeys, numberElements2, true, 100);

			scaledJobGraph.setSavepointPath(savepointPath);

			cluster.submitJobAndWait(scaledJobGraph, false);

			Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

			Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

			HashKeyGroupAssigner<Integer> keyGroupAssigner2 = new HashKeyGroupAssigner<>(maxParallelism);

			for (int key = 0; key < numberKeys; key++) {
				int keyGroupIndex = keyGroupAssigner2.getKeyGroupIndex(key);
				expectedResult2.add(Tuple2.of(keyGroupIndex % parallelism2, key * (numberElements + numberElements2)));
			}

			assertEquals(expectedResult2, actualResult2);

		} finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}

	@Test
	public void testSavepointRescalingWithNonPartitionedState() throws Exception {
		ForkableFlinkMiniCluster cluster = null;
		int parallelism = numSlots / 2;
		int parallelism2 = numSlots;
		int maxParallelism = 13;

		Deadline deadline = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();

		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slotsPerTaskManager);

			final File checkpointDir = temporaryFolder.newFolder();
			final File savepointDir = temporaryFolder.newFolder();

			config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
			config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, checkpointDir.toURI().toString());
			config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
			config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, savepointDir.toURI().toString());

			cluster = new ForkableFlinkMiniCluster(config);
			cluster.start();

			ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createNonPartitionedStateJobGraph(parallelism, maxParallelism, 500);

			JobID jobID = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// wait till all tasks have been started
			SubtaskIndexNonPartitionedSource.workStartedLatch.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobID), deadline.timeLeft());

			Object savepointResponse = Await.result(savepointPathFuture, deadline.timeLeft());

			final String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess)savepointResponse).savepointPath();

			Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), deadline.timeLeft());

			Future<Object> cancellationResponseFuture = jobManager.ask(new JobManagerMessages.CancelJob(jobID), deadline.timeLeft());

			Object cancellationResponse = Await.result(cancellationResponseFuture, deadline.timeLeft());

			assertTrue(cancellationResponse instanceof JobManagerMessages.CancellationSuccess);

			Await.ready(jobRemovedFuture, deadline.timeLeft());

			JobGraph scaledJobGraph = createNonPartitionedStateJobGraph(parallelism2, maxParallelism, 500);

			scaledJobGraph.setSavepointPath(savepointPath);

			cluster.submitJobAndWait(scaledJobGraph, false);

		} catch (JobExecutionException exception) {
			if (exception.getCause() instanceof SuppressRestartsException) {
				SuppressRestartsException suppressRestartsException = (SuppressRestartsException) exception.getCause();

				if (suppressRestartsException.getCause() instanceof IllegalStateException) {
					// we expect a IllegalStateException wrapped in a SuppressRestartsException wrapped
					// in a JobExecutionException, because the job containing non-partitioned state
					// is being rescaled
				} else {
					throw exception;
				}
			} else {
				throw exception;
			}
		} finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}

	private static JobGraph createNonPartitionedStateJobGraph(int parallelism, int maxParallelism, long checkpointInterval) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.getConfig().setMaxParallelism(maxParallelism);
		env.enableCheckpointing(checkpointInterval);

		SubtaskIndexNonPartitionedSource.workStartedLatch = new CountDownLatch(parallelism);

		DataStream<Integer> input = env.addSource(new SubtaskIndexNonPartitionedSource());

		input.addSink(new DiscardingSink<Integer>());

		return env.getStreamGraph().getJobGraph();
	}

	private static JobGraph createPartitionedStateJobGraph(
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

		result.addSink(new CollectionSink());

		return env.getStreamGraph().getJobGraph();
	}

	private static class SubtaskIndexSource
		extends RichParallelSourceFunction<Integer> {

		private static final long serialVersionUID = -400066323594122516L;

		private final int numberKeys;
		private final int numberElements;
		private final boolean terminateAfterEmission;

		private int counter = 0;

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

	private static class SubtaskIndexFlatMapper extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 5273172591283191348L;

		private static volatile CountDownLatch workCompletedLatch = new CountDownLatch(1);

		private transient ValueState<Integer> counter;
		private transient ValueState<Integer> sum;

		private final int numberElements;

		SubtaskIndexFlatMapper(int numberElements) {
			this.numberElements = numberElements;
		}

		@Override
		public void open(Configuration configuration) {
			counter = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("counter", Integer.class, 0));
			sum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", Integer.class, 0));
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

	private static class SubtaskIndexNonPartitionedSource extends RichParallelSourceFunction<Integer> implements Checkpointed<Integer> {

		private static final long serialVersionUID = -8108185918123186841L;

		static CountDownLatch workStartedLatch = new CountDownLatch(1);

		private int counter = 0;
		private boolean running = true;
		private boolean started = false;

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return counter;
		}

		@Override
		public void restoreState(Integer state) throws Exception {
			counter = state;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			final Object lock = ctx.getCheckpointLock();

			while (running) {
				if (!started) {
					workStartedLatch.countDown();
					started = true;
				}

				synchronized (lock) {
					counter++;

					ctx.collect(counter * getRuntimeContext().getIndexOfThisSubtask());
				}

				Thread.sleep(100);
			}
		}

		@Override
		public void cancel() {
			running = true;
		}
	}


}
