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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.BlockingSink;
import org.apache.flink.test.util.IdentityMapFunction;
import org.apache.flink.test.util.InfiniteIntegerSource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;

/**
 * Integration test for operator back pressure tracking.
 */
public class BackPressureITCase extends TestLogger {

	private static final JobID TEST_JOB_ID = new JobID();
	private static final int NUM_TASKS = 3;
	private static final int BACK_PRESSURE_REQUEST_INTERVAL_MS = 5;
	private static final int TASKS_BECOMING_BACK_PRESSURED_TIMEOUT_MS = 15 * 1000;
	private static final int MAX_BACK_PRESSURE_RATIO = 1;

	private TestingMiniCluster testingMiniCluster;
	private DispatcherGateway dispatcherGateway;

	@Before
	public void setUp() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.addAll(createBackPressureSamplingConfiguration());
		configuration.addAll(createNetworkBufferConfiguration());

		final TestingMiniClusterConfiguration testingMiniClusterConfiguration = new TestingMiniClusterConfiguration.Builder()
			.setNumSlotsPerTaskManager(NUM_TASKS)
			.setConfiguration(configuration)
			.build();

		testingMiniCluster = new TestingMiniCluster(testingMiniClusterConfiguration);
		testingMiniCluster.start();
		dispatcherGateway = testingMiniCluster.getDispatcherGatewayFuture().get();
	}

	private static Configuration createBackPressureSamplingConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setInteger(WebOptions.BACKPRESSURE_REFRESH_INTERVAL, 1000);
		configuration.setInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES, 1);
		configuration.setInteger(WebOptions.BACKPRESSURE_CLEANUP_INTERVAL, Integer.MAX_VALUE);
		return configuration;
	}

	private static Configuration createNetworkBufferConfiguration() {
		final Configuration configuration = new Configuration();

		final int memorySegmentSizeKb = 32;
		final MemorySize networkBuffersMemory = MemorySize.parse(memorySegmentSizeKb * 6 + "kb");

		configuration.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse(memorySegmentSizeKb + "kb"));
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkBuffersMemory);
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkBuffersMemory);
		return configuration;
	}

	@Test
	public void operatorsBecomeBackPressured() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
			.setParallelism(1);

		env.addSource(new InfiniteIntegerSource())
			.slotSharingGroup("sourceGroup")
			.map(new IdentityMapFunction<>())
			.slotSharingGroup("mapGroup")
			.addSink(new BlockingSink<>())
			.slotSharingGroup("sinkGroup");

		final JobGraph jobGraph = env.getStreamGraph().getJobGraph(TEST_JOB_ID);

		final List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		final JobVertex sourceJobVertex = vertices.get(0);
		final JobVertex mapJobVertex = vertices.get(1);

		testingMiniCluster.submitJob(jobGraph).get();
		ClientUtils.waitUntilJobInitializationFinished(
			() -> testingMiniCluster.getJobStatus(TEST_JOB_ID).get(),
			() -> testingMiniCluster.requestJobResult(TEST_JOB_ID).get(),
			ClassLoader.getSystemClassLoader());

		assertJobVertexSubtasksAreBackPressured(mapJobVertex);
		assertJobVertexSubtasksAreBackPressured(sourceJobVertex);
	}

	private void assertJobVertexSubtasksAreBackPressured(final JobVertex jobVertex) throws Exception {
		try {
			final Deadline timeout = Deadline.fromNow(Duration.ofMillis(TASKS_BECOMING_BACK_PRESSURED_TIMEOUT_MS));
			waitUntilCondition(
				isJobVertexBackPressured(jobVertex),
				timeout,
				BACK_PRESSURE_REQUEST_INTERVAL_MS);
		} catch (final TimeoutException e) {
			final String errorMessage = String.format("Subtasks of job vertex %s were not back pressured within timeout", jobVertex);
			throw new AssertionError(errorMessage, e);
		}
	}

	private SupplierWithException<Boolean, Exception> isJobVertexBackPressured(final JobVertex sourceJobVertex) {
		return () -> {
			final OperatorBackPressureStatsResponse backPressureStatsResponse = dispatcherGateway
				.requestOperatorBackPressureStats(TEST_JOB_ID, sourceJobVertex.getID())
				.get();

			return backPressureStatsResponse.getOperatorBackPressureStats()
				.map(backPressureStats -> isBackPressured(backPressureStats))
				.orElse(false);
		};
	}

	private static boolean isBackPressured(final OperatorBackPressureStats backPressureStats) {
		for (int i = 0; i < backPressureStats.getNumberOfSubTasks(); i++) {
			final double subtaskBackPressureRatio = backPressureStats.getBackPressureRatio(i);
			if (subtaskBackPressureRatio != MAX_BACK_PRESSURE_RATIO) {
				return false;
			}
		}
		return true;
	}

	@After
	public void tearDown() throws Exception {
		testingMiniCluster.close();
	}
}
