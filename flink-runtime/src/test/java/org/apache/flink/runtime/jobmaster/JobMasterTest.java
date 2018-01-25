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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

@Category(Flip6.class)
public class JobMasterTest extends TestLogger {

	private final Time testingTimeout = Time.seconds(10L);

	@Test
	public void testHeartbeatTimeoutWithTaskManager() throws Exception {
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final String jobManagerAddress = "jm";
		final JobMasterId jobMasterId = JobMasterId.generate();
		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);

		final String taskManagerAddress = "tm";
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGateway();

		final CompletableFuture<ResourceID> heartbeatResourceIdFuture = new CompletableFuture<>();
		final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();

		taskExecutorGateway.setHeartbeatJobManagerConsumer(heartbeatResourceIdFuture::complete);
		taskExecutorGateway.setDisconnectJobManagerConsumer(tuple -> disconnectedJobManagerFuture.complete(tuple.f0));

		final TestingRpcService rpc = new TestingRpcService();
		rpc.registerGateway(taskManagerAddress, taskExecutorGateway);

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 5L;

		final ScheduledExecutor scheduledExecutor = rpc.getScheduledExecutor();
		final HeartbeatServices heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, scheduledExecutor);

		final JobGraph jobGraph = new JobGraph();

		Configuration configuration = new Configuration();
		try (BlobServer blobServer = new BlobServer(configuration, new VoidBlobStore())) {
			blobServer.start();

			final JobMaster jobMaster = new JobMaster(
				rpc,
				jmResourceId,
				jobGraph,
				configuration,
				haServices,
				heartbeatServices,
				Executors.newScheduledThreadPool(1),
				blobServer,
				new BlobLibraryCacheManager(
					blobServer,
					FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
					new String[0]),
				new NoRestartStrategy.NoRestartStrategyFactory(),
				testingTimeout,
				null,
				new NoOpOnCompletionActions(),
				testingFatalErrorHandler,
				FlinkUserCodeClassLoaders.parentFirst(new URL[0], JobMasterTest.class.getClassLoader()),
				null,
				null);

			CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			// register task manager will trigger monitor heartbeat target, schedule heartbeat request at interval time
			CompletableFuture<RegistrationResponse> registrationResponse = jobMasterGateway
				.registerTaskManager(taskManagerAddress, taskManagerLocation, testingTimeout);

			// wait for the completion of the registration
			registrationResponse.get();

			System.out.println("foobar");

			final ResourceID heartbeatResourceId = heartbeatResourceIdFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(heartbeatResourceId, Matchers.equalTo(jmResourceId));

			final JobID disconnectedJobManager = disconnectedJobManagerFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(disconnectedJobManager, Matchers.equalTo(jobGraph.getJobID()));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			rpc.stopService();
		}
	}

	@Test
	public void testHeartbeatTimeoutWithResourceManager() throws Exception {
		final String resourceManagerAddress = "rm";
		final String jobManagerAddress = "jm";
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final JobMasterId jobMasterId = JobMasterId.generate();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);
		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final JobGraph jobGraph = new JobGraph();

		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		final TestingRpcService rpc = new TestingRpcService();

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 5L;
		final ScheduledExecutor scheduledExecutor = rpc.getScheduledExecutor();
		final HeartbeatServices heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, scheduledExecutor);

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway(
			resourceManagerId,
			rmResourceId,
			heartbeatInterval,
			"localhost",
			"localhost");

		final CompletableFuture<Tuple3<JobMasterId, ResourceID, JobID>> jobManagerRegistrationFuture = new CompletableFuture<>();
		final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();

		resourceManagerGateway.setRegisterJobManagerConsumer(tuple -> jobManagerRegistrationFuture.complete(
			Tuple3.of(
				tuple.f0,
				tuple.f1,
				tuple.f3)));

		resourceManagerGateway.setDisconnectJobManagerConsumer(tuple -> disconnectedJobManagerFuture.complete(tuple.f0));

		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		Configuration configuration = new Configuration();
		try (BlobServer blobServer = new BlobServer(configuration, new VoidBlobStore())) {
			blobServer.start();

			final JobMaster jobMaster = new JobMaster(
				rpc,
				jmResourceId,
				jobGraph,
				configuration,
				haServices,
				heartbeatServices,
				Executors.newScheduledThreadPool(1),
				blobServer,
				new BlobLibraryCacheManager(
					blobServer,
					FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
					new String[0]),
				new NoRestartStrategy.NoRestartStrategyFactory(),
				testingTimeout,
				null,
				new NoOpOnCompletionActions(),
				testingFatalErrorHandler,
				FlinkUserCodeClassLoaders.parentFirst(new URL[0], JobMasterTest.class.getClassLoader()),
				null,
				null);

			CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

			// wait for the start operation to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// define a leader and see that a registration happens
			rmLeaderRetrievalService.notifyListener(resourceManagerAddress, resourceManagerId.toUUID());

			// register job manager success will trigger monitor heartbeat target between jm and rm
			final Tuple3<JobMasterId, ResourceID, JobID> registrationInformation = jobManagerRegistrationFuture.get(
				testingTimeout.toMilliseconds(),
				TimeUnit.MILLISECONDS);

			assertThat(registrationInformation.f0, Matchers.equalTo(jobMasterId));
			assertThat(registrationInformation.f1, Matchers.equalTo(jmResourceId));
			assertThat(registrationInformation.f2, Matchers.equalTo(jobGraph.getJobID()));

			final JobID disconnectedJobManager = disconnectedJobManagerFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// heartbeat timeout should trigger disconnect JobManager from ResourceManager
			assertThat(disconnectedJobManager, Matchers.equalTo(jobGraph.getJobID()));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			rpc.stopService();
		}
	}

	/**
	 * No op implementation of {@link OnCompletionActions}.
	 */
	private static final class NoOpOnCompletionActions implements OnCompletionActions {

		@Override
		public void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph) {

		}

		@Override
		public void jobFinishedByOther() {

		}
	}

}
