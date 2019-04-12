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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Simple back pressured task test.
 *
 * @see BackPressureStatsTrackerImpl
 */
public class BackPressureStatsTrackerImplITCase extends TestLogger {

	private static final long TIMEOUT_SECONDS = 10;

	private static final Duration TIMEOUT = Duration.ofSeconds(TIMEOUT_SECONDS);

	private static final int BACKPRESSURE_NUM_SAMPLES = 2;

	private static final int JOB_PARALLELISM = 4;

	private static final JobID TEST_JOB_ID = new JobID();

	private static final JobVertex TEST_JOB_VERTEX = new JobVertex("Task");

	private NetworkBufferPool networkBufferPool;

	/** Shared as static variable with the test task. */
	private static BufferPool testBufferPool;

	private TestingMiniCluster testingMiniCluster;

	private DispatcherGateway dispatcherGateway;

	@Before
	public void setUp() throws Exception {
		networkBufferPool = new NetworkBufferPool(100, 8192);
		testBufferPool = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

		final Configuration configuration = new Configuration();
		configuration.setInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES, BACKPRESSURE_NUM_SAMPLES);

		testingMiniCluster = new TestingMiniCluster(new TestingMiniClusterConfiguration.Builder()
			.setNumTaskManagers(JOB_PARALLELISM)
			.setConfiguration(configuration)
			.build());
		testingMiniCluster.start();

		dispatcherGateway = testingMiniCluster.getDispatcherGatewayFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	@After
	public void tearDown() throws Exception {
		if (testingMiniCluster != null) {
			testingMiniCluster.close();
		}

		if (testBufferPool != null) {
			testBufferPool.lazyDestroy();
		}

		if (networkBufferPool != null) {
			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}

	}

	/**
	 * Tests a simple fake-back pressured task. Back pressure is assumed when
	 * sampled stack traces are in blocking buffer requests.
	 */
	@Test
	public void testBackPressureShouldBeReflectedInStats() throws Exception {
		final List<Buffer> buffers = requestAllBuffers();
		try {
			final JobGraph jobGraph = createJobWithBackPressure();
			testingMiniCluster.submitJob(jobGraph).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

			final OperatorBackPressureStats stats = getBackPressureStatsForTestVertex();

			assertThat(stats.getNumberOfSubTasks(), is(equalTo(JOB_PARALLELISM)));
			assertThat(stats, isFullyBackpressured());
		} finally {
			releaseBuffers(buffers);
		}
	}

	@Test
	public void testAbsenceOfBackPressureShouldBeReflectedInStats() throws Exception {
		final JobGraph jobGraph = createJobWithoutBackPressure();
		testingMiniCluster.submitJob(jobGraph).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		final OperatorBackPressureStats stats = getBackPressureStatsForTestVertex();

		assertThat(stats.getNumberOfSubTasks(), is(equalTo(JOB_PARALLELISM)));
		assertThat(stats, isNotBackpressured());
	}

	private static JobGraph createJobWithBackPressure() {
		final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Test Job");

		TEST_JOB_VERTEX.setInvokableClass(BackPressuredTask.class);
		TEST_JOB_VERTEX.setParallelism(JOB_PARALLELISM);

		jobGraph.addVertex(TEST_JOB_VERTEX);
		return jobGraph;
	}

	private static JobGraph createJobWithoutBackPressure() {
		final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Test Job");

		TEST_JOB_VERTEX.setInvokableClass(BlockingNoOpInvokable.class);
		TEST_JOB_VERTEX.setParallelism(JOB_PARALLELISM);

		jobGraph.addVertex(TEST_JOB_VERTEX);
		return jobGraph;
	}

	private static List<Buffer> requestAllBuffers() throws IOException {
		final List<Buffer> buffers = new ArrayList<>();
		while (true) {
			final Buffer buffer = testBufferPool.requestBuffer();
			if (buffer != null) {
				buffers.add(buffer);
			} else {
				break;
			}
		}
		return buffers;
	}

	private static void releaseBuffers(final List<Buffer> buffers) {
		for (Buffer buffer : buffers) {
			buffer.recycleBuffer();
			assertTrue(buffer.isRecycled());
		}
	}

	private OperatorBackPressureStats getBackPressureStatsForTestVertex() {
		waitUntilBackPressureStatsAvailable();

		final Optional<OperatorBackPressureStats> stats = getBackPressureStats();
		checkState(stats.isPresent());
		return stats.get();
	}

	private void waitUntilBackPressureStatsAvailable() {
		try {
			CommonTestUtils.waitUntilCondition(
				() -> {
					final Optional<OperatorBackPressureStats> stats = getBackPressureStats();
					return stats.isPresent();
					},
				Deadline.fromNow(TIMEOUT));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Optional<OperatorBackPressureStats> getBackPressureStats() {
		try {
			return dispatcherGateway.requestOperatorBackPressureStats(TEST_JOB_ID, TEST_JOB_VERTEX.getID())
				.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
				.getOperatorBackPressureStats();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * A back pressured producer sharing a {@link BufferPool} with the
	 * test driver.
	 */
	public static class BackPressuredTask extends AbstractInvokable {

		public BackPressuredTask(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			final BufferBuilder bufferBuilder = testBufferPool.requestBufferBuilderBlocking();
			// Got a buffer, yay!
			BufferBuilderTestUtils.buildSingleBuffer(bufferBuilder).recycleBuffer();

			Thread.currentThread().join();
		}
	}

	private static Matcher<OperatorBackPressureStats> isNotBackpressured() {
		return new OperatorBackPressureRatioMatcher(0);
	}

	private static Matcher<OperatorBackPressureStats> isFullyBackpressured() {
		return new OperatorBackPressureRatioMatcher(1);
	}

	private static class OperatorBackPressureRatioMatcher extends TypeSafeDiagnosingMatcher<OperatorBackPressureStats> {

		private final double expectedBackPressureRatio;

		private OperatorBackPressureRatioMatcher(final double expectedBackPressureRatio) {
			this.expectedBackPressureRatio = expectedBackPressureRatio;
		}

		@Override
		protected boolean matchesSafely(final OperatorBackPressureStats stats, final Description mismatchDescription) {
			if (!isBackPressureRatioCorrect(stats)) {
				mismatchDescription.appendText("Not all subtask back pressure ratios in " + getBackPressureRatios(stats) + " are " + expectedBackPressureRatio);
				return false;
			}
			return true;
		}

		private static List<Double> getBackPressureRatios(final OperatorBackPressureStats stats) {
			return IntStream.range(0, stats.getNumberOfSubTasks())
				.mapToObj(stats::getBackPressureRatio).collect(Collectors.toList());
		}

		private boolean isBackPressureRatioCorrect(final OperatorBackPressureStats stats) {
			return IntStream.range(0, stats.getNumberOfSubTasks())
				.mapToObj(stats::getBackPressureRatio)
				.allMatch(backpressureRatio -> backpressureRatio == expectedBackPressureRatio);
		}

		@Override
		public void describeTo(final Description description) {
			description.appendText("All subtask back pressure ratios are " + expectedBackPressureRatio);
		}
	}
}
