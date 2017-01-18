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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoader;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerSenderImpl;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatManagerSenderImpl;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.SlotPoolGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BlobLibraryCacheManager.class)
public class JobMasterTest extends TestLogger {

	@Test
	public void testHeartbeatTimeoutWithTaskManager() throws Exception {
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService();
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

		final TestingSerialRpcService rpc = new TestingSerialRpcService();
		rpc.registerGateway(taskManagerAddress, taskExecutorGateway);

		final long heartbeatInterval = 1L;
		final long heartbeatTimeout = 5L;
		final CountDownLatch waitLatch = new CountDownLatch(1);
		final HeartbeatManagerSenderImpl<Void, Void> jmHeartbeatManager = new TestingHeartbeatManagerSenderImpl<>(
				waitLatch,
				heartbeatInterval,
				heartbeatTimeout,
				jmResourceId,
				rpc.getExecutor(),
				rpc.getScheduledExecutor(),
				log);

		try {
			final JobMaster jobMaster = new JobMaster(
					new JobGraph(),
					new Configuration(),
					rpc,
					haServices,
					Executors.newScheduledThreadPool(1),
					mock(BlobLibraryCacheManager.class),
					mock(RestartStrategyFactory.class),
					Time.of(10, TimeUnit.SECONDS),
					null,
					jmResourceId,
					jmHeartbeatManager,
					mock(OnCompletionActions.class),
					testingFatalErrorHandler,
					new FlinkUserCodeClassLoader(new URL[0]));

			// also start the heartbeat manager in job manager
			jobMaster.start(jmLeaderId);

			// register task manager will trigger monitoring heartbeat target, schedule heartbeat request in interval time
			jobMaster.registerTaskManager(taskManagerAddress, taskManagerLocation, jmLeaderId);

			verify(taskExecutorGateway, atLeast(1)).heartbeatFromJobManager(eq(jmResourceId));

			final ConcurrentHashMap<ResourceID, Object> heartbeatTargets = Whitebox.getInternalState(jmHeartbeatManager, "heartbeatTargets");
			final Map<ResourceID, Tuple2<TaskManagerLocation, TaskExecutorGateway>> registeredTMsInJM = Whitebox.getInternalState(jobMaster, "registeredTaskManagers");
			final SlotPoolGateway slotPoolGateway = mock(SlotPoolGateway.class);
			Whitebox.setInternalState(jobMaster, "slotPoolGateway", slotPoolGateway);

			// before heartbeat timeout
			assertTrue(heartbeatTargets.containsKey(tmResourceId));
			assertTrue(registeredTMsInJM.containsKey(tmResourceId));

			// continue to unmonitor heartbeat target
			waitLatch.countDown();

			// after heartbeat timeout
			verify(slotPoolGateway, timeout(heartbeatTimeout * 5)).releaseTaskManager(eq(tmResourceId));
			assertFalse(heartbeatTargets.containsKey(tmResourceId));
			assertFalse(registeredTMsInJM.containsKey(tmResourceId));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			rpc.stopService();
		}
	}
}
