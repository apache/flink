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
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatManagerSenderImpl;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.SlotPool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.powermock.reflect.Whitebox;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeast;

public class JobMasterTest extends TestLogger {

	@Rule
	public TestName name = new TestName();

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

		final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(
				Hardware.getNumberCPUCores(), ExecutorThreadFactory.INSTANCE);

		final long heartbeatInterval = 10L;
		final long heartbeatTimeout = 1000L;
		final HeartbeatManagerSenderImpl<Object, Object> jmHeartbeatManager = new HeartbeatManagerSenderImpl<>(
				heartbeatInterval,
				heartbeatTimeout,
				jmResourceId,
				executorService,
				rpc.getScheduledExecutor(),
				log);

		final JobVertex jobVertex = new JobVertex("NoOpInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		final JobGraph jobGraph = new JobGraph("test", jobVertex);

		final BlobLibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(mock(BlobService.class), 1000000000L);
		libraryCacheManager.registerJob(jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());

		final MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		final JobManagerMetricGroup jmMetricGroup = new JobManagerMetricGroup(registry, "host");

		try {
			final JobMaster jobMaster = new JobMaster(
					jobGraph,
					new Configuration(),
					rpc,
					haServices,
					executorService,
					libraryCacheManager,
					mock(RestartStrategyFactory.class),
					Time.of(10, TimeUnit.SECONDS),
					jmMetricGroup,
					jmResourceId,
					jmHeartbeatManager,
					mock(OnCompletionActions.class),
					testingFatalErrorHandler,
					libraryCacheManager.getClassLoader(jobGraph.getJobID()));

			// also start the heartbeat manager in job manager
			jobMaster.start(jmLeaderId);

			// register task manager will trigger monitoring heartbeat target, schedule heartbeat request in interval time
			jobMaster.registerTaskManager(taskManagerAddress, taskManagerLocation, jmLeaderId);

			final ConcurrentHashMap<ResourceID, Object> heartbeatTargets = Whitebox.getInternalState(jmHeartbeatManager, "heartbeatTargets");
			final Map<ResourceID, Tuple2<TaskManagerLocation, TaskExecutorGateway>> registeredTMsInJM = Whitebox.getInternalState(jobMaster, "registeredTaskManagers");
			final SlotPool slotPool = Whitebox.getInternalState(jobMaster, "slotPool");
			final HashSet<ResourceID> registeredTMsInSlotPool = Whitebox.getInternalState(slotPool, "registeredTaskManagers");

			// before heartbeat timeout
			assertTrue(heartbeatTargets.containsKey(tmResourceId));
			assertTrue(registeredTMsInJM.containsKey(tmResourceId));
			assertTrue(registeredTMsInSlotPool.contains(tmResourceId));

			// trigger heartbeat timeout in job manager side, because the task manager will not response the heartbeat
			Thread.sleep(heartbeatTimeout);

			// after heartbeat timeout
			verify(taskExecutorGateway, atLeast(1)).heartbeatFromJobManager(eq(jmResourceId));
			assertFalse(registeredTMsInJM.containsKey(tmResourceId));
			assertFalse(registeredTMsInSlotPool.contains(tmResourceId));

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			rpc.stopService();
		}
	}
}
