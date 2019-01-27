/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for region failover with multi regions.
 */
public class RegionFailoverITCase extends TestLogger {

	private static int failBase = 1000;

	private static volatile AtomicInteger jobFailedCnt = new AtomicInteger(0);

	private static volatile int numCompletedCheckpoints = 0;

	private static MiniClusterResource cluster;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");

		cluster = new MiniClusterResource(
			new MiniClusterResource.MiniClusterResourceConfiguration(
				configuration,
				2,
				2),
			true);
		cluster.before();
		jobFailedCnt.set(0);
		numCompletedCheckpoints = 0;
		MapperFunction.resetForTest();
		StringGeneratingSourceFunction.resetForTest();
		MapperFunction.savepointTriggered = false;
	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}

	@Test
	public void testRegionFailover() {
		final int restartTimes = 3;
		final long numElements = failBase * 10;

		// no need to trigger savepoint
		StringGeneratingSourceFunction.waitSavepointLatch.countDown();
		MapperFunction.savepointTriggered = true;

		try {
			// there exists 3 individual regions.
			JobGraph jobGraph = createJobGraph(3, numElements, restartTimes);
			ClusterClient<?> client = cluster.getClusterClient();
			client.submitJob(jobGraph, RegionFailoverITCase.class.getClassLoader());
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testRegionFailoverWithParallelismChanged() throws IOException {
		long numElements = failBase * 3;
		Duration timeout = Duration.ofSeconds(30);

		String checkpointPathFolder = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
		String externalizedCheckpointPath = "file://" + checkpointPathFolder;
		try {
			// there exists 3 individual regions and no restart needed.
			JobGraph jobGraph1 = createJobGraph(3, numElements, 0);
			JobID jobID1 = jobGraph1.getJobID();

			ClusterClient<?> client = cluster.getClusterClient();
			client.setDetached(true);
			JobSubmissionResult submitJob = client.submitJob(jobGraph1, RegionFailoverITCase.class.getClassLoader());

			MapperFunction.triggerSavepointLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
			String savepointPath = client.triggerSavepoint(submitJob.getJobID(), externalizedCheckpointPath).get();
			StringGeneratingSourceFunction.waitSavepointLatch.countDown();
			MapperFunction.savepointTriggered = true;

			detectJobFinishedAsExpected(client, jobID1, timeout);

			int restartTimes = 3;
			jobFailedCnt.set(0);
			numElements = failBase * 10;
			// there exists 4 individual regions.
			JobGraph jobGraph2 = createJobGraph(4, numElements, restartTimes);
			jobGraph2.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));
			client.setDetached(false);
			client.submitJob(jobGraph2, RegionFailoverITCase.class.getClassLoader());
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private JobGraph createJobGraph(
		int parallelism,
		long numElements,
		int numberOfRetries) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.disableOperatorChaining();
		if (numberOfRetries == 0) {
			env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		} else {
			// this job only accepts {restartTimes - 1} times to restart the overall job.
			// Since task would failover 'restartTimes' times, if region-based failover strategy cannot handle the multi-region failover well,
			// region will try to call global failover 'restartTimes' times leading to IT failure.
			env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(numberOfRetries - 1, 0L));
		}
		env.getConfig().disableSysoutLogging();

		// there exists num of 'parallelism' individual regions.
		env.addSource(new StringGeneratingSourceFunction(numElements, numElements / 2L)).setParallelism(parallelism)
			.map(new MapperFunction(numberOfRetries)).setParallelism(parallelism);

		env.addSource(new StringGeneratingSourceFunction(numElements, numElements / 2L)).setParallelism(1)
			.map((MapFunction<Integer, Object>) value -> value).setParallelism(1);

		return env.getStreamGraph().getJobGraph();
	}

	private void detectJobFinishedAsExpected(ClusterClient<?> client, JobID jobID, Duration timeout) throws InterruptedException, ExecutionException {
		boolean jobFinished = false;
		Deadline deadline = Deadline.now().plus(timeout);
		while (deadline.hasTimeLeft()) {
			if (client.getJobStatus(jobID).get() != JobStatus.FINISHED) {
				Thread.sleep(5000);
			} else {
				jobFinished = true;
				break;
			}
		}
		if (!jobFinished) {
			Assert.fail("Region failover ITCase job (" + jobID + ") dose not finished in " + timeout.getSeconds() +
				" seconds as expected, there must exist something error, we treat it as test failure.");
		}
	}

	private static class MapperFunction extends RichMapFunction<Integer, Integer> {

		private final int restartTimes;

		private static volatile CountDownLatch triggerSavepointLatch = new CountDownLatch(1);

		private static volatile boolean savepointTriggered = false;

		MapperFunction(int restartTimes) {
			this.restartTimes = restartTimes;
		}

		@Override
		public Integer map(Integer input) throws Exception {
			if (input > failBase) {
				if (!savepointTriggered) {
					triggerSavepointLatch.countDown();
				}

				if (jobFailedCnt.get() < restartTimes && getRuntimeContext().getIndexOfThisSubtask() == 0) {
					jobFailedCnt.incrementAndGet();
					throw new TestException();
				}
			}
			return input;
		}

		static void resetForTest() {
			triggerSavepointLatch = new CountDownLatch(1);
		}
	}

	private static class StringGeneratingSourceFunction extends RichParallelSourceFunction<Integer>
		implements ListCheckpointed<Integer>, CheckpointListener {
		private static final long serialVersionUID = 1L;

		private final long numElements;
		private final long checkpointLatestAt;

		private int index = -1;

		private volatile boolean isRunning = true;

		private static volatile CountDownLatch waitSavepointLatch = new CountDownLatch(1);

		StringGeneratingSourceFunction(long numElements, long checkpointLatestAt) {
			this.numElements = numElements;
			this.checkpointLatestAt = checkpointLatestAt;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			final int step = getRuntimeContext().getNumberOfParallelSubtasks();
			if (index < 0) {
				// not been restored, so initialize
				index = getRuntimeContext().getIndexOfThisSubtask();
			}

			while (isRunning && index < numElements) {
				int result = numCompletedCheckpoints * failBase + ThreadLocalRandom.current().nextInt(failBase);

				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (ctx.getCheckpointLock()) {
					index += step;
					ctx.collect(result);
				}

				if (numCompletedCheckpoints < 2) {
					// not yet completed enough checkpoints, so slow down
					if (index < checkpointLatestAt) {
						// mild slow down
						Thread.sleep(1);
					} else {
						// wait until the checkpoints are completed
						while (isRunning && numCompletedCheckpoints < 2) {
							Thread.sleep(200);
						}
					}
				}
				if (!MapperFunction.savepointTriggered && index >= numElements / getRuntimeContext().getNumberOfParallelSubtasks()) {
					waitSavepointLatch.await();
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.index);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.index = state.get(0);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				numCompletedCheckpoints++;
			}
		}

		static void resetForTest() {
			waitSavepointLatch = new CountDownLatch(1);
		}
	}

	private static class TestException extends IOException{
		private static final long serialVersionUID = 1L;
	}
}
