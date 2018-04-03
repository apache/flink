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

package org.apache.flink.test.example.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests retrieval of a job from a running Flink cluster.
 */
@Category(New.class)
public class JobRetrievalITCase extends TestLogger {

	private static final Semaphore lock = new Semaphore(1);

	private static final MiniCluster CLUSTER;
	private static final RestClusterClient<StandaloneClusterId> CLIENT;

	static {
		try {
		MiniClusterConfiguration clusterConfiguration = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(4)
			.build();
		CLUSTER = new MiniCluster(clusterConfiguration);
		CLUSTER.start();

		final Configuration clientConfig = new Configuration();
		clientConfig.setString(JobManagerOptions.ADDRESS, CLUSTER.getRestAddress().getHost());
		clientConfig.setInteger(RestOptions.REST_PORT, CLUSTER.getRestAddress().getPort());
		clientConfig.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 0);
		clientConfig.setLong(RestOptions.RETRY_DELAY, 0);

		CLIENT = new RestClusterClient<>(
			clientConfig,
			StandaloneClusterId.getInstance());

		} catch (Exception e) {
			throw new AssertionError("Could not setup cluster.", e);
		}
	}

	@AfterClass
	public static void shutdownCluster() throws Exception {
		CLIENT.shutdown();
		CLUSTER.close();
	}

	@Test
	public void testJobRetrieval() throws Exception {
		final JobID jobID = new JobID();

		final JobVertex imalock = new JobVertex("imalock");
		imalock.setInvokableClass(SemaphoreInvokable.class);

		final JobGraph jobGraph = new JobGraph(jobID, "testjob", imalock);

		// acquire the lock to make sure that the job cannot complete until the job client
		// has been attached in resumingThread
		lock.acquire();

		CLIENT.setDetached(true);
		CLIENT.submitJob(jobGraph, JobRetrievalITCase.class.getClassLoader());

		final AtomicReference<Throwable> error = new AtomicReference<>();

		final Thread resumingThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					assertNotNull(CLIENT.requestJobResult(jobID).get());
				} catch (Throwable e) {
					error.set(e);
				}
			}
		}, "Flink-Job-Retriever");

		// wait until the job is running
		while (CLIENT.listJobs().get().isEmpty()) {
			Thread.sleep(50);
		}

		// kick off resuming
		resumingThread.start();

		// wait for client to connect
		while (resumingThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		// client has connected, we can release the lock
		lock.release();

		resumingThread.join();

		Throwable exception = error.get();
		if (exception != null) {
			throw new AssertionError(exception);
		}
	}

	@Test
	public void testNonExistingJobRetrieval() throws Exception {
		final JobID jobID = new JobID();

		try {
			CLIENT.requestJobResult(jobID).get();
			fail();
		} catch (Exception exception) {
			Optional<Throwable> expectedCause = ExceptionUtils.findThrowable(exception,
				candidate -> candidate.getMessage() != null && candidate.getMessage().contains("Could not find Flink job"));
			if (!expectedCause.isPresent()) {
				throw exception;
			}
		}
	}

	/**
	 * Invokable that waits on {@link #lock} to be released and finishes afterwards.
	 *
	 * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
	 */
	public static class SemaphoreInvokable extends AbstractInvokable {

		public SemaphoreInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			lock.acquire();
			lock.release();
		}
	}

}
