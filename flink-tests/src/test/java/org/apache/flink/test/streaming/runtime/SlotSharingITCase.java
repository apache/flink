/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.SessionDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.jobgraph.JobStatus.CANCELED;
import static org.apache.flink.runtime.jobgraph.JobStatus.FINISHED;
import static org.junit.Assert.assertEquals;

/**
 * Integration test for slot sharing.
 */
public class SlotSharingITCase extends TestLogger {

	@Test
	public void testSlotSharingDisabled() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setSlotSharingEnabled(false);
		env.fromElements("test").map(value -> value).setParallelism(2);

		final JobID jobID = JobID.generate();
		final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph(), jobID);

		final CountDownLatch resourceAllocatedLatch = new CountDownLatch(1);

		// we have 2 slots but we need 3 slots
		// in eager schedule mode (by default), this job would not be scheduled
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(2)
			.setNumSlotsPerTaskManager(1)
			.build();

		TestingMiniCluster miniCluster = new TestingMiniCluster(miniClusterConfiguration, resourceAllocatedLatch);

		miniCluster.start();

		try {
			final Deadline timeoutDeadline = Deadline.fromNow(Duration.ofSeconds(30));

			miniCluster.submitJob(jobGraph).get(timeoutDeadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			// should trigger allocating new worker since 2 slots are not enough
			resourceAllocatedLatch.await(timeoutDeadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			assertEquals(0, resourceAllocatedLatch.getCount());

			miniCluster.cancelJob(jobID).get(timeoutDeadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			CommonTestUtils.waitUntilCondition(() -> miniCluster.getJobStatus(jobID).get() == CANCELED, timeoutDeadline);
		} finally {
			miniCluster.close();
		}
	}

	@Test
	public void testSlotSharingEnabled() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setSlotSharingEnabled(true);
		env.fromElements("test").map(value -> value).setParallelism(2);

		final JobID jobID = JobID.generate();
		final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph(), jobID);

		final CountDownLatch resourceAllocatedLatch = new CountDownLatch(1);

		// we have 2 slots but we need 3 slots, it's OK under slot sharing
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(2)
			.setNumSlotsPerTaskManager(1)
			.build();

		TestingMiniCluster miniCluster = new TestingMiniCluster(miniClusterConfiguration, resourceAllocatedLatch);

		miniCluster.start();

		try {
			final Deadline timeoutDeadline = Deadline.fromNow(Duration.ofSeconds(30));

			miniCluster.submitJob(jobGraph).get(timeoutDeadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			CommonTestUtils.waitUntilCondition(() -> miniCluster.getJobStatus(jobID).get() == FINISHED, timeoutDeadline);

			assertEquals(1, resourceAllocatedLatch.getCount());
		} finally {
			miniCluster.close();
		}
	}

	/**
	 * A testing mini cluster that which injects testing resource manager.
	 */
	private static class TestingMiniCluster extends MiniCluster {

		private final CountDownLatch resourceAllocatedLatch;

		TestingMiniCluster(
				MiniClusterConfiguration miniClusterConfiguration,
				CountDownLatch resourceAllocatedLatch) {
			super(miniClusterConfiguration);

			this.resourceAllocatedLatch = resourceAllocatedLatch;
		}

		@Override
		protected Collection<? extends DispatcherResourceManagerComponent<?>> createDispatcherResourceManagerComponents(
				Configuration configuration,
				RpcServiceFactory rpcServiceFactory,
				HighAvailabilityServices haServices,
				BlobServer blobServer,
				HeartbeatServices heartbeatServices,
				MetricRegistry metricRegistry,
				MetricQueryServiceRetriever metricQueryServiceRetriever,
				FatalErrorHandler fatalErrorHandler) throws Exception {

			SessionDispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory =
					new SessionDispatcherResourceManagerComponentFactory(new ResourceManagerFactory() {
						@Override
						public ResourceManager createResourceManager(
								Configuration configuration,
								ResourceID resourceId,
								RpcService rpcService,
								HighAvailabilityServices highAvailabilityServices,
								HeartbeatServices heartbeatServices,
								MetricRegistry metricRegistry,
								FatalErrorHandler fatalErrorHandler,
								ClusterInformation clusterInformation,
								@Nullable String webInterfaceUrl,
								JobManagerMetricGroup jobManagerMetricGroup) throws Exception {

							final ResourceManagerRuntimeServicesConfiguration resourceManagerRuntimeServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration);
							final ResourceManagerRuntimeServices resourceManagerRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
								resourceManagerRuntimeServicesConfiguration,
								highAvailabilityServices,
								rpcService.getScheduledExecutor());

							return new TestingResourceManager(
									rpcService,
									getEndpointId(),
									resourceId,
									highAvailabilityServices,
									heartbeatServices,
									resourceManagerRuntimeServices.getSlotManager(),
									metricRegistry,
									resourceManagerRuntimeServices.getJobLeaderIdService(),
									clusterInformation,
									fatalErrorHandler,
									jobManagerMetricGroup,
									resourceAllocatedLatch);
						}
					});

			return Collections.singleton(
				dispatcherResourceManagerComponentFactory.create(
					configuration,
					rpcServiceFactory.createRpcService(),
					haServices,
					blobServer,
					heartbeatServices,
					metricRegistry,
					new MemoryArchivedExecutionGraphStore(),
					metricQueryServiceRetriever,
					fatalErrorHandler));
		}
	}

	/**
	 * A testing resource manager that providing a way to wait "startNewWorker" being invoked.
	 */
	private static class TestingResourceManager extends StandaloneResourceManager {

		private final CountDownLatch resourceAllocatedLatch;

		TestingResourceManager(
				RpcService rpcService,
				String resourceManagerEndpointId,
				ResourceID resourceId,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				MetricRegistry metricRegistry,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				JobManagerMetricGroup jobManagerMetricGroup,
				CountDownLatch resourceAllocatedLatch) {
			super(rpcService,
					resourceManagerEndpointId,
					resourceId,
					highAvailabilityServices,
					heartbeatServices,
					slotManager,
					metricRegistry,
					jobLeaderIdService,
					clusterInformation,
					fatalErrorHandler,
					jobManagerMetricGroup);

			this.resourceAllocatedLatch = resourceAllocatedLatch;
		}

		@Override
		public Collection<ResourceProfile> startNewWorker(ResourceProfile resourceProfile) {
			resourceAllocatedLatch.countDown();
			return Collections.emptyList();
		}
	}
}
