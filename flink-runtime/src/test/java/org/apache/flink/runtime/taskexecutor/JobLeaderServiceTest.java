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
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Tests for the {@link JobLeaderService}.
 */
public class JobLeaderServiceTest extends TestLogger {

	@Rule
	public final TestingRpcServiceResource rpcServiceResource = new TestingRpcServiceResource();

	/**
	 * Tests that we can concurrently modify the JobLeaderService and complete the leader retrieval operation.
	 * See FLINK-16373.
	 */
	@Test
	public void handlesConcurrentJobAdditionsAndLeaderChanges() throws Exception {
		final JobLeaderService jobLeaderService = new JobLeaderService(
			new LocalUnresolvedTaskManagerLocation(),
			RetryingRegistrationConfiguration.defaultConfiguration());

		final TestingJobLeaderListener jobLeaderListener = new TestingJobLeaderListener();
		final int numberOperations = 20;
		final BlockingQueue<SettableLeaderRetrievalService> instantiatedLeaderRetrievalServices = new ArrayBlockingQueue<>(numberOperations);

		final HighAvailabilityServices haServices = new TestingHighAvailabilityServicesBuilder()
			.setJobMasterLeaderRetrieverFunction(
				leaderForJobId -> {
					final SettableLeaderRetrievalService leaderRetrievalService = new SettableLeaderRetrievalService();
					instantiatedLeaderRetrievalServices.offer(leaderRetrievalService);
					return leaderRetrievalService;
				})
			.build();

		jobLeaderService.start(
			"foobar",
			rpcServiceResource.getTestingRpcService(),
			haServices,
			jobLeaderListener);

		final CheckedThread addJobAction = new CheckedThread() {
			@Override
			public void go() throws Exception {
				for (int i = 0; i < numberOperations; i++) {
					final JobID jobId = JobID.generate();
					jobLeaderService.addJob(jobId, "foobar");
					Thread.yield();
					jobLeaderService.removeJob(jobId);
				}
			}
		};
		addJobAction.start();

		for (int i = 0; i < numberOperations; i++) {
			final SettableLeaderRetrievalService leaderRetrievalService = instantiatedLeaderRetrievalServices.take();
			leaderRetrievalService.notifyListener("foobar", UUID.randomUUID());
		}

		addJobAction.sync();
	}

	/**
	 * Tests that the JobLeaderService won't try to reconnect to JobMaster after it
	 * has lost the leadership. See FLINK-16836.
	 */
	@Test
	public void doesNotReconnectAfterTargetLostLeadership() throws Exception {
		final JobLeaderService jobLeaderService = new JobLeaderService(
			new LocalUnresolvedTaskManagerLocation(),
			RetryingRegistrationConfiguration.defaultConfiguration());

		final JobID jobId = new JobID();

		final SettableLeaderRetrievalService leaderRetrievalService = new SettableLeaderRetrievalService();
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServicesBuilder()
			.setJobMasterLeaderRetrieverFunction(ignored -> leaderRetrievalService)
			.build();

		final String jmAddress = "foobar";
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder().build();
		rpcServiceResource.getTestingRpcService().registerGateway(jmAddress, jobMasterGateway);

		final TestingJobLeaderListener testingJobLeaderListener = new TestingJobLeaderListener();
		jobLeaderService.start(
			"foobar",
			rpcServiceResource.getTestingRpcService(),
			haServices,
			testingJobLeaderListener);

		try {
			jobLeaderService.addJob(jobId, jmAddress);

			leaderRetrievalService.notifyListener(jmAddress, UUID.randomUUID());

			testingJobLeaderListener.waitUntilJobManagerGainedLeadership();

			// revoke the leadership
			leaderRetrievalService.notifyListener(null, null);
			testingJobLeaderListener.waitUntilJobManagerLostLeadership();

			jobLeaderService.reconnect(jobId);
		} finally {
			jobLeaderService.stop();
		}
	}

	private static final class TestingJobLeaderListener implements JobLeaderListener {

		private final CountDownLatch jobManagerGainedLeadership = new CountDownLatch(1);
		private final CountDownLatch jobManagerLostLeadership = new CountDownLatch(1);

		@Override
		public void jobManagerGainedLeadership(JobID jobId, JobMasterGateway jobManagerGateway, JMTMRegistrationSuccess registrationMessage) {
			jobManagerGainedLeadership.countDown();
		}

		@Override
		public void jobManagerLostLeadership(JobID jobId, JobMasterId jobMasterId) {
			jobManagerLostLeadership.countDown();
		}

		@Override
		public void handleError(Throwable throwable) {
			// ignored
		}

		private void waitUntilJobManagerGainedLeadership() throws InterruptedException {
			jobManagerGainedLeadership.await();
		}

		private void waitUntilJobManagerLostLeadership() throws InterruptedException {
			jobManagerLostLeadership.await();
		}
	}
}
