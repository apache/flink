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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ExecutionGraphCache}.
 */
public class ExecutionGraphCacheTest extends TestLogger {

	private static ArchivedExecutionGraph expectedExecutionGraph;
	private static final JobID expectedJobId = new JobID();

	@BeforeClass
	public static void setup() {
		expectedExecutionGraph = new ArchivedExecutionGraphBuilder().build();
	}

	/**
	 * Tests that we can cache AccessExecutionGraphs over multiple accesses.
	 */
	@Test
	public void testExecutionGraphCaching() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.hours(1L);

		final CountingRestfulGateway restfulGateway = createCountingRestfulGateway(expectedJobId, CompletableFuture.completedFuture(expectedExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> accessExecutionGraphFuture = executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway);

			assertEquals(expectedExecutionGraph, accessExecutionGraphFuture.get());

			accessExecutionGraphFuture = executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway);

			assertEquals(expectedExecutionGraph, accessExecutionGraphFuture.get());

			assertThat(restfulGateway.getNumRequestJobCalls(), Matchers.equalTo(1));
		}
	}

	/**
	 * Tests that an AccessExecutionGraph is invalidated after its TTL expired.
	 */
	@Test
	public void testExecutionGraphEntryInvalidation() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.milliseconds(1L);

		final CountingRestfulGateway restfulGateway = createCountingRestfulGateway(
			expectedJobId,
			CompletableFuture.completedFuture(expectedExecutionGraph),
			CompletableFuture.completedFuture(expectedExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway);

			assertEquals(expectedExecutionGraph, executionGraphFuture.get());

			// sleep for the TTL
			Thread.sleep(timeToLive.toMilliseconds() * 5L);

			CompletableFuture<AccessExecutionGraph> executionGraphFuture2 = executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway);

			assertEquals(expectedExecutionGraph, executionGraphFuture2.get());

			assertThat(restfulGateway.getNumRequestJobCalls(), Matchers.equalTo(2));
		}
	}


	/**
	 * Tests that a failure in requesting an AccessExecutionGraph from the gateway, will not create
	 * a cache entry --> another cache request will trigger a new gateway request.
	 */
	@Test
	public void testImmediateCacheInvalidationAfterFailure() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.hours(1L);

		// let's first answer with a JobNotFoundException and then only with the correct result
		final CountingRestfulGateway restfulGateway = createCountingRestfulGateway(
			expectedJobId,
			FutureUtils.completedExceptionally(new FlinkJobNotFoundException(expectedJobId)),
			CompletableFuture.completedFuture(expectedExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway);

			try {
				executionGraphFuture.get();

				fail("The execution graph future should have been completed exceptionally.");
			} catch (ExecutionException ee) {
				assertTrue(ee.getCause() instanceof FlinkException);
			}

			CompletableFuture<AccessExecutionGraph> executionGraphFuture2 = executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway);

			assertEquals(expectedExecutionGraph, executionGraphFuture2.get());
		}
	}

	/**
	 * Tests that cache entries are cleaned up when their TTL has expired upon
	 * calling {@link ExecutionGraphCache#cleanup()}.
	 */
	@Test
	public void testCacheEntryCleanup() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.milliseconds(1L);
		final JobID expectedJobId2 = new JobID();
		final ArchivedExecutionGraph expectedExecutionGraph2 = new ArchivedExecutionGraphBuilder().build();

		final AtomicInteger requestJobCalls = new AtomicInteger(0);
		final TestingRestfulGateway restfulGateway = new TestingRestfulGateway.Builder()
			.setRequestJobFunction(
				jobId -> {
					requestJobCalls.incrementAndGet();
					if (jobId.equals(expectedJobId)) {
						return CompletableFuture.completedFuture(expectedExecutionGraph);
					} else if (jobId.equals(expectedJobId2)) {
						return CompletableFuture.completedFuture(expectedExecutionGraph2);
					} else {
						throw new AssertionError("Invalid job id received.");
					}
				}
			)
			.build();

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {

			CompletableFuture<AccessExecutionGraph> executionGraph1Future = executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway);

			CompletableFuture<AccessExecutionGraph> executionGraph2Future = executionGraphCache.getExecutionGraph(expectedJobId2, restfulGateway);

			assertEquals(expectedExecutionGraph, executionGraph1Future.get());

			assertEquals(expectedExecutionGraph2, executionGraph2Future.get());

			assertThat(requestJobCalls.get(), Matchers.equalTo(2));

			Thread.sleep(timeToLive.toMilliseconds());

			executionGraphCache.cleanup();

			assertTrue(executionGraphCache.size() == 0);
		}
	}

	/**
	 * Tests that concurrent accesses only trigger a single AccessExecutionGraph request.
	 */
	@Test
	public void testConcurrentAccess() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.hours(1L);

		final CountingRestfulGateway restfulGateway = createCountingRestfulGateway(expectedJobId, CompletableFuture.completedFuture(expectedExecutionGraph));

		final int numConcurrentAccesses = 10;

		final ArrayList<CompletableFuture<AccessExecutionGraph>> executionGraphFutures = new ArrayList<>(numConcurrentAccesses);

		final ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(numConcurrentAccesses);

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			for (int i = 0; i < numConcurrentAccesses; i++) {
				CompletableFuture<AccessExecutionGraph> executionGraphFuture = CompletableFuture
					.supplyAsync(
						() -> executionGraphCache.getExecutionGraph(expectedJobId, restfulGateway),
						executor)
					.thenCompose(Function.identity());

				executionGraphFutures.add(executionGraphFuture);
			}

			final CompletableFuture<Collection<AccessExecutionGraph>> allExecutionGraphFutures = FutureUtils.combineAll(executionGraphFutures);

			Collection<AccessExecutionGraph> allExecutionGraphs = allExecutionGraphFutures.get();

			for (AccessExecutionGraph executionGraph : allExecutionGraphs) {
				assertEquals(expectedExecutionGraph, executionGraph);
			}

			assertThat(restfulGateway.getNumRequestJobCalls(), Matchers.equalTo(1));
		} finally {
			ExecutorUtils.gracefulShutdown(5000L, TimeUnit.MILLISECONDS, executor);
		}
	}

	private CountingRestfulGateway createCountingRestfulGateway(JobID jobId, CompletableFuture<ArchivedExecutionGraph>... accessExecutionGraphs) {
		final ConcurrentLinkedQueue<CompletableFuture<ArchivedExecutionGraph>> queue = new ConcurrentLinkedQueue<>(Arrays.asList(accessExecutionGraphs));
		return new CountingRestfulGateway(
			jobId,
			ignored -> queue.poll());
	}

	/**
	 * {@link RestfulGateway} implementation which counts the number of {@link #requestJob(JobID, Time)} calls.
	 */
	private static class CountingRestfulGateway extends TestingRestfulGateway {

		private final JobID expectedJobId;

		private AtomicInteger numRequestJobCalls = new AtomicInteger(0);

		private CountingRestfulGateway(JobID expectedJobId, Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction) {
			this.expectedJobId = Preconditions.checkNotNull(expectedJobId);
			this.requestJobFunction = Preconditions.checkNotNull(requestJobFunction);
		}

		@Override
		public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
			assertThat(jobId, Matchers.equalTo(expectedJobId));
			numRequestJobCalls.incrementAndGet();
			return super.requestJob(jobId, timeout);
		}

		public int getNumRequestJobCalls() {
			return numRequestJobCalls.get();
		}
	}

	private static final class SuspendableAccessExecutionGraph extends ArchivedExecutionGraph {

		private static final long serialVersionUID = -6796543726305778101L;

		private JobStatus jobStatus;

		public SuspendableAccessExecutionGraph(JobID jobId) {
			super(
				jobId,
				"ExecutionGraphCacheTest",
				Collections.emptyMap(),
				Collections.emptyList(),
				new long[0],
				JobStatus.RUNNING,
				new ErrorInfo(new FlinkException("Test"), 42L),
				"",
				new StringifiedAccumulatorResult[0],
				Collections.emptyMap(),
				new ArchivedExecutionConfig(new ExecutionConfig()),
				false,
				null,
				null,
				"stateBackendName");

			jobStatus = super.getState();
		}

		@Override
		public JobStatus getState() {
			return jobStatus;
		}

		public void setJobStatus(JobStatus jobStatus) {
			this.jobStatus = jobStatus;
		}
	}
}
