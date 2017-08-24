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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoader;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetAddress;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BlobLibraryCacheManager.class)
public class JobMasterTest extends TestLogger {

	private final Time testingTimeout = Time.seconds(10L);

	@Test
	public void testHeartbeatTimeoutWithTaskManager() throws Exception {
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		haServices.setCheckpointRecoveryFactory(mock(CheckpointRecoveryFactory.class));
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final String jobManagerAddress = "jm";
		final UUID jmLeaderId = UUID.randomUUID();
		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);

		final String taskManagerAddress = "tm";
		final ResourceID tmResourceId = new ResourceID(taskManagerAddress);
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(tmResourceId, InetAddress.getLoopbackAddress(), 1234);
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);

		final TestingRpcService rpc = new TestingRpcService();
		rpc.registerGateway(taskManagerAddress, taskExecutorGateway);

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 5L;

		final ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
		final HeartbeatServices heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, scheduledExecutor);
		BlobServer blobServer = mock(BlobServer.class);
		when(blobServer.getPort()).thenReturn(1337);

		final JobGraph jobGraph = new JobGraph();

		try {
			final JobMaster jobMaster = new JobMaster(
				rpc,
				jmResourceId,
				jobGraph,
				new Configuration(),
				haServices,
				heartbeatServices,
				Executors.newScheduledThreadPool(1),
				blobServer,
				mock(BlobLibraryCacheManager.class),
				mock(RestartStrategyFactory.class),
				Time.of(10, TimeUnit.SECONDS),
				null,
				mock(OnCompletionActions.class),
				testingFatalErrorHandler,
				new FlinkUserCodeClassLoader(new URL[0]));

			jobMaster.start(jmLeaderId);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			// register task manager will trigger monitor heartbeat target, schedule heartbeat request at interval time
			CompletableFuture<RegistrationResponse> registrationResponse = jobMasterGateway
				.registerTaskManager(taskManagerAddress, taskManagerLocation, jmLeaderId, testingTimeout);

			// wait for the completion of the registration
			registrationResponse.get();

			ArgumentCaptor<Runnable> heartbeatRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
			verify(scheduledExecutor, times(1)).scheduleAtFixedRate(
				heartbeatRunnableCaptor.capture(),
				eq(0L),
				eq(heartbeatInterval),
				eq(TimeUnit.MILLISECONDS));

			Runnable heartbeatRunnable = heartbeatRunnableCaptor.getValue();

			ArgumentCaptor<Runnable> timeoutRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
			verify(scheduledExecutor, timeout(testingTimeout.toMilliseconds())).schedule(timeoutRunnableCaptor.capture(), eq(heartbeatTimeout), eq(TimeUnit.MILLISECONDS));

			Runnable timeoutRunnable = timeoutRunnableCaptor.getValue();

			// run the first heartbeat request
			heartbeatRunnable.run();

			verify(taskExecutorGateway, times(1)).heartbeatFromJobManager(eq(jmResourceId));

			// run the timeout runnable to simulate a heartbeat timeout
			timeoutRunnable.run();

			verify(taskExecutorGateway, timeout(testingTimeout.toMilliseconds())).disconnectJobManager(eq(jobGraph.getJobID()), any(TimeoutException.class));

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
		final UUID rmLeaderId = UUID.randomUUID();
		final UUID jmLeaderId = UUID.randomUUID();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);
		final ResourceID jmResourceId = new ResourceID(jobManagerAddress);
		final JobGraph jobGraph = new JobGraph();

		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		haServices.setCheckpointRecoveryFactory(mock(CheckpointRecoveryFactory.class));

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 5L;
		final ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
		final HeartbeatServices heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, scheduledExecutor);

		final ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		when(resourceManagerGateway.registerJobManager(
			any(UUID.class),
			any(UUID.class),
			any(ResourceID.class),
			anyString(),
			any(JobID.class),
			any(Time.class)
		)).thenReturn(CompletableFuture.completedFuture(new JobMasterRegistrationSuccess(
			heartbeatInterval, rmLeaderId, rmResourceId)));

		final TestingRpcService rpc = new TestingRpcService();
		rpc.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		try {
			final JobMaster jobMaster = new JobMaster(
				rpc,
				jmResourceId,
				jobGraph,
				new Configuration(),
				haServices,
				heartbeatServices,
				Executors.newScheduledThreadPool(1),
				mock(BlobServer.class),
				mock(BlobLibraryCacheManager.class),
				mock(RestartStrategyFactory.class),
				Time.of(10, TimeUnit.SECONDS),
				null,
				mock(OnCompletionActions.class),
				testingFatalErrorHandler,
				new FlinkUserCodeClassLoader(new URL[0]));

			jobMaster.start(jmLeaderId);

			// define a leader and see that a registration happens
			rmLeaderRetrievalService.notifyListener(resourceManagerAddress, rmLeaderId);

			// register job manager success will trigger monitor heartbeat target between jm and rm
			verify(resourceManagerGateway, timeout(testingTimeout.toMilliseconds())).registerJobManager(
				eq(rmLeaderId),
				eq(jmLeaderId),
				eq(jmResourceId),
				anyString(),
				eq(jobGraph.getJobID()),
				any(Time.class));

			// heartbeat timeout should trigger disconnect JobManager from ResourceManager
			verify(resourceManagerGateway, timeout(heartbeatTimeout * 50L)).disconnectJobManager(eq(jobGraph.getJobID()), any(TimeoutException.class));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			rpc.stopService();
		}
	}


}
