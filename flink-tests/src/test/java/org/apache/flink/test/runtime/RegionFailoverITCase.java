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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT case for testing region failover strategy.
 */
public class RegionFailoverITCase extends TestLogger {

	final long JOB_EXECUTION_TIMEOUT = 120_000L;

	/**
	 * Region failover strategy for a streaming job that will have one task failure.
	 * Verify that the job can successfully finish.
	 */
	@Test
	public void testRegionFailoverForStreamingJob() throws Exception {
		testRegionFailoverInternal("streaming");
	}

	/**
	 * Region failover strategy for a streaming job that will have one task failure.
	 * Verify that the job can successfully finish.
	 */
	@Test
	public void testRegionFailoverForBatchJob() throws Exception {
		testRegionFailoverInternal("batch");
	}

	private void testRegionFailoverInternal(final String jobType) throws Exception {
		checkNotNull(jobType);

		final Configuration configuration = new Configuration();

		configuration.setString(
			EXECUTION_FAILOVER_STRATEGY.key(),
			FailoverStrategyLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME);

		final long slotIdleTimeout = 50L;
		configuration.setLong(JobManagerOptions.SLOT_IDLE_TIMEOUT, slotIdleTimeout);

		final int parallelism = 4;
		final long restartDelay = slotIdleTimeout << 1;

		final JobGraph jobGraph;
		if (jobType.equalsIgnoreCase("streaming")) {
			jobGraph = createStreamingJob(restartDelay, parallelism);
		} else if (jobType.equalsIgnoreCase("batch")) {
			jobGraph = createBatchJob(restartDelay, parallelism);
		} else {
			throw new Exception("Unsupported job type " + jobType);
		}

		executeJob(configuration, jobGraph, parallelism);
	}

	private void executeJob(
		final Configuration clusterConfiguration,
		final JobGraph jobGraph,
		final int parallelism) throws Exception {

		checkNotNull(clusterConfiguration);
		checkNotNull(jobGraph);

		clusterConfiguration.setString(RestOptions.BIND_PORT, "0");

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(clusterConfiguration)
			.setNumTaskManagers(parallelism)
			.setNumSlotsPerTaskManager(1)
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			final MiniClusterClient miniClusterClient = new MiniClusterClient(clusterConfiguration, miniCluster);

			final CompletableFuture<JobSubmissionResult> submissionFuture = miniClusterClient.submitJob(jobGraph);

			// wait for the submission to succeed
			JobSubmissionResult jobSubmissionResult = submissionFuture.get();

			final CompletableFuture<JobResult> resultFuture =
				miniClusterClient.requestJobResult(jobSubmissionResult.getJobID());

			final JobResult jobResult = resultFuture.get(JOB_EXECUTION_TIMEOUT, TimeUnit.MILLISECONDS);

			assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));
		}
	}

	/**
	 * Creates streaming job graph with multiple regions.
	 * All edges are pipelined. Schedules in eager mode.
	 * One of the source tasks will fail once.
	 * <pre>
	 *     (source) -+-> (sink)
	 *
	 *               ^
	 *               |
	 *     pipelined + pointwise
	 * </pre>
	 */
	@Nonnull
	private JobGraph createStreamingJob(final long restartDelay, final int parallelism) throws IOException {
		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(SchedulingITCase.OneTimeFailingInvokable.class);
		source.setParallelism(parallelism);
		source.setSlotSharingGroup(slotSharingGroup);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		sink.setSlotSharingGroup(slotSharingGroup);

		sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		final JobGraph jobGraph = new JobGraph(source, sink);

		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, restartDelay));
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	/**
	 * Creates batch job graph with multiple regions.
	 * All edges are blocking. Schedules in lazy mode.
	 * One of the source tasks will fail once.
	 * <pre>
	 *     (source) -+-> (sink)
	 *
	 *               ^
	 *               |
	 *      blocking + all-to-all
	 * </pre>
	 */
	@Nonnull
	private JobGraph createBatchJob(final long restartDelay, final int parallelism) throws IOException {
		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(SchedulingITCase.OneTimeFailingInvokable.class);
		source.setParallelism(parallelism);
		source.setSlotSharingGroup(slotSharingGroup);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		sink.setSlotSharingGroup(slotSharingGroup);

		sink.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		final JobGraph jobGraph = new JobGraph(source, sink);

		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, restartDelay));
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}
}
