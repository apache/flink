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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link ExecutionGraphCache}.
 */
public class ExecutionGraphCacheTest extends TestLogger {

	/**
	 * Tests that we can cache AccessExecutionGraphs over multiple accesses.
	 */
	@Test
	public void testExecutionGraphCaching() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.hours(1L);
		final JobID jobId = new JobID();
		final AccessExecutionGraph accessExecutionGraph = mock(AccessExecutionGraph.class);

		final JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.requestJob(eq(jobId), any(Time.class))).thenReturn(CompletableFuture.completedFuture(accessExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> accessExecutionGraphFuture = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, accessExecutionGraphFuture.get());

			CompletableFuture<AccessExecutionGraph> accessExecutionGraphFuture2 = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, accessExecutionGraphFuture2.get());

			// verify that we only issued a single request to the gateway
			verify(jobManagerGateway, times(1)).requestJob(eq(jobId), any(Time.class));
		}
	}

	/**
	 * Tests that an AccessExecutionGraph is invalidated after its TTL expired.
	 */
	@Test
	public void testExecutionGraphEntryInvalidation() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.milliseconds(1L);
		final JobID jobId = new JobID();
		final AccessExecutionGraph accessExecutionGraph = mock(AccessExecutionGraph.class);

		final JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.requestJob(eq(jobId), any(Time.class))).thenReturn(CompletableFuture.completedFuture(accessExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, executionGraphFuture.get());

			// sleep for the TTL
			Thread.sleep(timeToLive.toMilliseconds());

			CompletableFuture<AccessExecutionGraph> executionGraphFuture2 = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, executionGraphFuture2.get());

			verify(jobManagerGateway, times(2)).requestJob(eq(jobId), any(Time.class));
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
		final JobID jobId = new JobID();

		final AccessExecutionGraph accessExecutionGraph = mock(AccessExecutionGraph.class);

		final JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		// let's first answer with a JobNotFoundException and then only with the correct result
		when(jobManagerGateway.requestJob(eq(jobId), any(Time.class))).thenReturn(
			FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId)),
			CompletableFuture.completedFuture(accessExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			try {
				executionGraphFuture.get();

				fail("The execution graph future should have been completed exceptionally.");
			} catch (ExecutionException ee) {
				assertTrue(ee.getCause() instanceof FlinkException);
			}

			CompletableFuture<AccessExecutionGraph> executionGraphFuture2 = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, executionGraphFuture2.get());
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
		final JobID jobId1 = new JobID();
		final JobID jobId2 = new JobID();
		final AccessExecutionGraph accessExecutionGraph1 = mock(AccessExecutionGraph.class);
		final AccessExecutionGraph accessExecutionGraph2 = mock(AccessExecutionGraph.class);

		final JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.requestJob(eq(jobId1), any(Time.class))).thenReturn(CompletableFuture.completedFuture(accessExecutionGraph1));
		when(jobManagerGateway.requestJob(eq(jobId2), any(Time.class))).thenReturn(CompletableFuture.completedFuture(accessExecutionGraph2));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {

			CompletableFuture<AccessExecutionGraph> executionGraph1Future = executionGraphCache.getExecutionGraph(jobId1, jobManagerGateway);

			CompletableFuture<AccessExecutionGraph> executionGraph2Future = executionGraphCache.getExecutionGraph(jobId2, jobManagerGateway);

			assertEquals(accessExecutionGraph1, executionGraph1Future.get());

			assertEquals(accessExecutionGraph2, executionGraph2Future.get());

			verify(jobManagerGateway, times(1)).requestJob(eq(jobId1), any(Time.class));
			verify(jobManagerGateway, times(1)).requestJob(eq(jobId2), any(Time.class));

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
		final JobID jobId = new JobID();

		final AccessExecutionGraph accessExecutionGraph = mock(AccessExecutionGraph.class);

		final JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		when(jobManagerGateway.requestJob(eq(jobId), any(Time.class))).thenReturn(CompletableFuture.completedFuture(accessExecutionGraph));

		final int numConcurrentAccesses = 10;

		final ArrayList<CompletableFuture<AccessExecutionGraph>> executionGraphFutures = new ArrayList<>(numConcurrentAccesses);

		final ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(numConcurrentAccesses);

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			for (int i = 0; i < numConcurrentAccesses; i++) {
				CompletableFuture<AccessExecutionGraph> executionGraphFuture = CompletableFuture
					.supplyAsync(
						() -> executionGraphCache.getExecutionGraph(jobId, jobManagerGateway),
						executor)
					.thenCompose(Function.identity());

				executionGraphFutures.add(executionGraphFuture);
			}

			final CompletableFuture<Collection<AccessExecutionGraph>> allExecutionGraphFutures = FutureUtils.combineAll(executionGraphFutures);

			Collection<AccessExecutionGraph> allExecutionGraphs = allExecutionGraphFutures.get();

			for (AccessExecutionGraph executionGraph : allExecutionGraphs) {
				assertEquals(accessExecutionGraph, executionGraph);
			}

			verify(jobManagerGateway, times(1)).requestJob(eq(jobId), any(Time.class));
		} finally {
			ExecutorUtils.gracefulShutdown(5000L, TimeUnit.MILLISECONDS, executor);
		}
	}

	/**
	 * Tests that a cache entry is invalidated if the retrieved {@link AccessExecutionGraph} is in
	 * state {@link JobStatus#SUSPENDED}.
	 *
	 * <p>This test can be removed once we no longer request the actual {@link ExecutionGraph} from the
	 * {@link JobManager}.
	 */
	@Test
	public void testCacheInvalidationIfSuspended() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.hours(1L);
		final JobID jobId = new JobID();

		final AccessExecutionGraph accessExecutionGraph = mock(AccessExecutionGraph.class);

		final AccessExecutionGraph suspendedExecutionGraph = mock(AccessExecutionGraph.class);
		when(suspendedExecutionGraph.getState()).thenReturn(JobStatus.SUSPENDED);

		final JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		// let's first answer with a suspended ExecutionGraph and then only with the correct result
		when(jobManagerGateway.requestJob(eq(jobId), any(Time.class))).thenReturn(
			CompletableFuture.completedFuture(suspendedExecutionGraph),
			CompletableFuture.completedFuture(accessExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(suspendedExecutionGraph, executionGraphFuture.get());

			CompletableFuture<AccessExecutionGraph> executionGraphFuture2 = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, executionGraphFuture2.get());
		}
	}

	/**
	 * Tests that a cache entry is invalidated if the retrieved {@link AccessExecutionGraph} changes its
	 * state to {@link JobStatus#SUSPENDED}.
	 *
	 * <p>This test can be removed once we no longer request the actual {@link ExecutionGraph} from the
	 * {@link JobManager}.
	 */
	@Test
	public void testCacheInvalidationIfSwitchToSuspended() throws Exception {
		final Time timeout = Time.milliseconds(100L);
		final Time timeToLive = Time.hours(1L);
		final JobID jobId = new JobID();

		final AccessExecutionGraph accessExecutionGraph = mock(AccessExecutionGraph.class);

		final SuspendableAccessExecutionGraph toBeSuspendedExecutionGraph = new SuspendableAccessExecutionGraph(jobId);

		final JobManagerGateway jobManagerGateway = mock(JobManagerGateway.class);
		// let's first answer with a JobNotFoundException and then only with the correct result
		when(jobManagerGateway.requestJob(eq(jobId), any(Time.class))).thenReturn(
			CompletableFuture.completedFuture(toBeSuspendedExecutionGraph),
			CompletableFuture.completedFuture(accessExecutionGraph));

		try (ExecutionGraphCache executionGraphCache = new ExecutionGraphCache(timeout, timeToLive)) {
			CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(toBeSuspendedExecutionGraph, executionGraphFuture.get());

			toBeSuspendedExecutionGraph.setJobStatus(JobStatus.SUSPENDED);

			// retrieve the same job from the cache again --> this should return it and invalidate the cache entry
			CompletableFuture<AccessExecutionGraph> executionGraphFuture2 = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, executionGraphFuture2.get());

			CompletableFuture<AccessExecutionGraph> executionGraphFuture3 = executionGraphCache.getExecutionGraph(jobId, jobManagerGateway);

			assertEquals(accessExecutionGraph, executionGraphFuture3.get());

			verify(jobManagerGateway, times(2)).requestJob(eq(jobId), any(Time.class));
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
				null);

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
