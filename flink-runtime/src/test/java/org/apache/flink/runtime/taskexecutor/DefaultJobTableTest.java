/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.ContextClassLoaderLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingLibraryCacheManager;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.taskmanager.NoOpCheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link DefaultJobTable}.
 */
public class DefaultJobTableTest extends TestLogger {

	private static final Supplier<LibraryCacheManager> DEFAULT_LIBRARY_SUPPLIER = () -> ContextClassLoaderLibraryCacheManager.INSTANCE;

	private final JobID jobId = new JobID();

	private DefaultJobTable jobTable;

	@Before
	public void setup() {
		jobTable = DefaultJobTable.create();
	}

	@After
	public void teardown() {
		if (jobTable != null) {
			jobTable.close();
		}
	}

	@Test
	public void getOrCreateJob_NoRegisteredJob_WillCreateNewJob() {
		final JobTable.Job newJob = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		assertThat(newJob.getJobId(), is(jobId));
		assertTrue(jobTable.getJob(jobId).isPresent());
	}

	@Test
	public void getOrCreateJob_RegisteredJob_WillReturnRegisteredJob() {
		final JobTable.Job newJob = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);
		final JobTable.Job otherJob = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		assertThat(otherJob, sameInstance(newJob));
	}

	@Test
	public void closeJob_WillShutDownLibraryCacheManager() throws InterruptedException {
		final OneShotLatch shutdownLibraryCacheManagerLatch = new OneShotLatch();
		final TestingLibraryCacheManager testingLibraryCacheManager = TestingLibraryCacheManager.newBuilder()
			.setShutdownRunnable(shutdownLibraryCacheManagerLatch::trigger)
			.build();
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, () -> testingLibraryCacheManager);

		job.close();

		shutdownLibraryCacheManagerLatch.await();
	}

	@Test
	public void closeJob_WillRemoveItFromJobTable() {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		job.close();

		assertFalse(jobTable.getJob(jobId).isPresent());
	}

	@Test
	public void connectJob_NotConnected_Succeeds() {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		final ResourceID resourceId = ResourceID.generate();
		final JobTable.Connection connection = connectJob(job, resourceId);

		assertThat(connection.getJobId(), is(jobId));
		assertThat(connection.getResourceId(), is(resourceId));
		assertTrue(jobTable.getConnection(jobId).isPresent());
		assertTrue(jobTable.getConnection(resourceId).isPresent());
	}

	private JobTable.Connection connectJob(JobTable.Job job, ResourceID resourceId) {
		return job.connect(
				resourceId,
				new TestingJobMasterGatewayBuilder().build(),
				new NoOpTaskManagerActions(),
				NoOpCheckpointResponder.INSTANCE,
				new TestGlobalAggregateManager(),
				new NoOpResultPartitionConsumableNotifier(),
				new NoOpPartitionProducerStateChecker());
	}

	@Test(expected = IllegalStateException.class)
	public void connectJob_Connected_Fails() {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		connectJob(job, ResourceID.generate());
		connectJob(job, ResourceID.generate());
	}

	@Test
	public void disconnectConnection_RemovesConnection() {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		final ResourceID resourceId = ResourceID.generate();
		final JobTable.Connection connection = connectJob(job, resourceId);

		connection.disconnect();

		assertFalse(jobTable.getConnection(jobId).isPresent());
		assertFalse(jobTable.getConnection(resourceId).isPresent());
	}

	@Test(expected = IllegalStateException.class)
	public void accessLibraryCachemanager_AfterBeingClosed_WillFail() {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		job.close();

		job.getLibraryCacheManager();
	}

	@Test(expected = IllegalStateException.class)
	public void connectJob_AfterBeingClosed_WillFail() {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		job.close();

		connectJob(job, ResourceID.generate());
	}

	@Test(expected = IllegalStateException.class)
	public void accessJobManagerGateway_AfterBeingDisconnected_WillFail() {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, DEFAULT_LIBRARY_SUPPLIER);

		final JobTable.Connection connection = connectJob(job, ResourceID.generate());

		connection.disconnect();

		connection.getJobManagerGateway();
	}

	@Test
	public void close_WillCloseAllRegisteredJobs() throws InterruptedException {
		final CountDownLatch shutdownLibraryCacheManagerLatch = new CountDownLatch(2);
		final TestingLibraryCacheManager classLoaderLease1 = TestingLibraryCacheManager.newBuilder()
			.setShutdownRunnable(shutdownLibraryCacheManagerLatch::countDown)
			.build();
		final TestingLibraryCacheManager classLoaderLease2 = TestingLibraryCacheManager.newBuilder()
			.setShutdownRunnable(shutdownLibraryCacheManagerLatch::countDown)
			.build();
		jobTable.getOrCreateJob(jobId, () -> classLoaderLease1);
		jobTable.getOrCreateJob(new JobID(), () -> classLoaderLease2);

		jobTable.close();

		shutdownLibraryCacheManagerLatch.await();
		assertTrue(jobTable.isEmpty());
	}
}
