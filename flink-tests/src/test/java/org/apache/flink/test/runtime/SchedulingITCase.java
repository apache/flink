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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT case for testing Flink's scheduling strategies.
 */
public class SchedulingITCase extends TestLogger {

	/**
	 * Tests that if local recovery is disabled we won't spread
	 * out tasks when recovering.
	 */
	@Test
	public void testDisablingLocalRecovery() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, false);

		executeSchedulingTest(configuration);
	}

	/**
	 * Tests that if local recovery is enabled we won't spread
	 * out tasks when recovering for global failover.
	 */
	@Test
	public void testLocalRecoveryFull() throws Exception {
		testLocalRecoveryInternal("full");
	}

	/**
	 * Tests that if local recovery is enabled we won't spread
	 * out tasks when recovering for regional failover.
	 */
	@Test
	public void testLocalRecoveryRegion() throws Exception {
		testLocalRecoveryInternal("region");
	}

	private void testLocalRecoveryInternal(String failoverStrategyValue) throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
		configuration.setString(EXECUTION_FAILOVER_STRATEGY.key(), failoverStrategyValue);

		executeSchedulingTest(configuration);
	}

	private void executeSchedulingTest(Configuration configuration) throws Exception {
		configuration.setInteger(RestOptions.PORT, 0);

		final long slotIdleTimeout = 50L;
		configuration.setLong(JobManagerOptions.SLOT_IDLE_TIMEOUT, slotIdleTimeout);

		final int parallelism = 4;
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(parallelism)
			.setNumSlotsPerTaskManager(1)
			.build();

		try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);

			JobGraph jobGraph = createJobGraph(slotIdleTimeout << 1, parallelism);
			CompletableFuture<JobSubmissionResult> submissionFuture = miniClusterClient.submitJob(jobGraph);

			// wait for the submission to succeed
			JobSubmissionResult jobSubmissionResult = submissionFuture.get();

			CompletableFuture<JobResult> resultFuture = miniClusterClient.requestJobResult(jobSubmissionResult.getJobID());

			JobResult jobResult = resultFuture.get();

			assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));
		}
	}

	@Nonnull
	private JobGraph createJobGraph(long delay, int parallelism) throws IOException {
		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(OneTimeFailingInvokable.class);
		source.setParallelism(parallelism);
		source.setSlotSharingGroup(slotSharingGroup);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		sink.setSlotSharingGroup(slotSharingGroup);

		sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		JobGraph jobGraph = new JobGraph(source, sink);

		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, delay));
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	/**
	 * Invokable which fails exactly once (one sub task of it).
	 */
	public static final class OneTimeFailingInvokable extends AbstractInvokable {

		private static final AtomicBoolean hasFailed = new AtomicBoolean(false);

		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public OneTimeFailingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			if (hasFailed.compareAndSet(false, true)) {
				throw new FlinkException("One time failure.");
			}
		}
	}
}
