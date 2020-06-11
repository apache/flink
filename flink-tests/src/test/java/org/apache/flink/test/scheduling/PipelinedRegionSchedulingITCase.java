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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
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
import org.apache.flink.test.runtime.SchedulingITCase;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT case for pipelined region scheduling.
 */
public class PipelinedRegionSchedulingITCase extends TestLogger {

	@Test
	public void testSuccessWithSlotsNoFewerThanRegionSize() throws Exception {
		final JobResult jobResult = executeSchedulingTest(2);
		assertThat(jobResult.getSerializedThrowable().isPresent(), is(false));
	}

	@Test
	public void testFailsOnInsufficientSlots() throws Exception {
		final JobResult jobResult = executeSchedulingTest(1);
		assertThat(jobResult.getSerializedThrowable().isPresent(), is(true));

		final Throwable jobFailure = jobResult
			.getSerializedThrowable()
			.get()
			.deserializeError(ClassLoader.getSystemClassLoader());

		Throwable rootFailure = jobFailure;
		while (rootFailure.getCause() != null) {
			rootFailure = rootFailure.getCause();
		}

		assertThat(rootFailure, instanceOf(TimeoutException.class));
		assertThat(rootFailure.getMessage(), is("Slot request bulk is not fulfillable!"));
	}

	private JobResult executeSchedulingTest(int numSlots) throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setString(RestOptions.BIND_PORT, "0");
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 5000L);

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(numSlots)
			.build();

		try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			final MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);

			final JobGraph jobGraph = createJobGraph(100);

			// wait for the submission to succeed
			final JobID jobID = miniClusterClient.submitJob(jobGraph).get();

			final CompletableFuture<JobResult> resultFuture = miniClusterClient.requestJobResult(jobID);

			final JobResult jobResult = resultFuture.get();

			return jobResult;
		}
	}

	private JobGraph createJobGraph(final int parallelism) throws IOException {
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

		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(10)));
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}
}
