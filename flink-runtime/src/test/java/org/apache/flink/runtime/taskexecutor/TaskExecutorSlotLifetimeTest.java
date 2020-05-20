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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorBuilder;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.io.network.partition.TestingTaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link TaskExecutor TaskExecutor's} slot lifetime and its
 * dependencies.
 */
public class TaskExecutorSlotLifetimeTest extends TestLogger {

	@ClassRule
	public static final TestingRpcServiceResource TESTING_RPC_SERVICE_RESOURCE = new TestingRpcServiceResource();

	@Rule
	public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource = new TestingFatalErrorHandlerResource();

	@Before
	public void setup() {
		UserClassLoaderExtractingInvokable.clearQueue();
	}

	/**
	 * Tests that the user code class loader is bound to the lifetime of the
	 * slot. This means that it is being reused across a failover, for example.
	 * See FLINK-16408.
	 */
	@Test
	public void testUserCodeClassLoaderIsBoundToSlot() throws Exception {
		final Configuration configuration = new Configuration();
		final TestingRpcService rpcService = TESTING_RPC_SERVICE_RESOURCE.getTestingRpcService();

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		final CompletableFuture<SlotReport> firstSlotReportFuture = new CompletableFuture<>();
		resourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			firstSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f2);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		final BlockingQueue<TaskExecutionState> taskExecutionStates = new ArrayBlockingQueue<>(2);
		final OneShotLatch slotsOfferedLatch = new OneShotLatch();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setOfferSlotsFunction((resourceID, slotOffers) -> {
				slotsOfferedLatch.trigger();
				return CompletableFuture.completedFuture(slotOffers);
			})
			.setUpdateTaskExecutionStateFunction(FunctionUtils.uncheckedFunction(
				taskExecutionState -> {
					taskExecutionStates.put(taskExecutionState);
					return CompletableFuture.completedFuture(Acknowledge.get());
				}))
			.build();

		final LeaderRetrievalService resourceManagerLeaderRetriever = new SettableLeaderRetrievalService(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());
		final LeaderRetrievalService jobMasterLeaderRetriever = new SettableLeaderRetrievalService(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServicesBuilder()
			.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever)
			.setJobMasterLeaderRetrieverFunction(ignored -> jobMasterLeaderRetriever)
			.build();

		rpcService.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		final LocalUnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();

		try (final TaskExecutor taskExecutor = createTaskExecutor(configuration, rpcService, haServices, unresolvedTaskManagerLocation)) {

			taskExecutor.start();

			final SlotReport slotReport = firstSlotReportFuture.join();
			final SlotID firstSlotId = slotReport.iterator().next().getSlotID();

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			final JobID jobId = new JobID();
			final AllocationID allocationId = new AllocationID();
			taskExecutorGateway.requestSlot(
				firstSlotId,
				jobId,
				allocationId,
				ResourceProfile.ZERO,
				jobMasterGateway.getAddress(),
				resourceManagerGateway.getFencingToken(),
				RpcUtils.INF_TIMEOUT).join();

			final TaskDeploymentDescriptor tdd = TaskDeploymentDescriptorBuilder.newBuilder(jobId, UserClassLoaderExtractingInvokable.class)
				.setAllocationId(allocationId)
				.build();

			slotsOfferedLatch.await();

			taskExecutorGateway.submitTask(
				tdd,
				jobMasterGateway.getFencingToken(),
				RpcUtils.INF_TIMEOUT).join();

			final ClassLoader firstClassLoader = UserClassLoaderExtractingInvokable.take();

			// wait for the first task to finish
			TaskExecutionState taskExecutionState;
			do {
				taskExecutionState = taskExecutionStates.take();
			} while (!taskExecutionState.getExecutionState().isTerminal());

			// check that a second task will re-use the same class loader
			taskExecutorGateway.submitTask(
				tdd,
				jobMasterGateway.getFencingToken(),
				RpcUtils.INF_TIMEOUT).join();

			final ClassLoader secondClassLoader = UserClassLoaderExtractingInvokable.take();

			assertThat(firstClassLoader, sameInstance(secondClassLoader));
		}
	}

	private TaskExecutor createTaskExecutor(Configuration configuration, TestingRpcService rpcService, TestingHighAvailabilityServices haServices, LocalUnresolvedTaskManagerLocation unresolvedTaskManagerLocation) throws IOException {
		return new TaskExecutor(
			rpcService,
			TaskManagerConfiguration.fromConfiguration(
				configuration,
				TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(configuration),
				InetAddress.getLoopbackAddress().getHostAddress()),
			haServices,
			new TaskManagerServicesBuilder()
				.setTaskSlotTable(TaskSlotUtils.createTaskSlotTable(1))
				.setUnresolvedTaskManagerLocation(unresolvedTaskManagerLocation)
				.build(),
			ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
			new TestingHeartbeatServices(),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			new BlobCacheService(
				configuration,
				new VoidBlobStore(),
				null),
			testingFatalErrorHandlerResource.getFatalErrorHandler(),
			new TestingTaskExecutorPartitionTracker(),
			TaskManagerRunner.createBackPressureSampleService(configuration, rpcService.getScheduledExecutor()));
	}

	public static final class UserClassLoaderExtractingInvokable extends AbstractInvokable {

		private static BlockingQueue<ClassLoader> userCodeClassLoaders = new ArrayBlockingQueue<>(2);

		public UserClassLoaderExtractingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			userCodeClassLoaders.put(getEnvironment().getUserClassLoader());
		}

		private static void clearQueue() {
			userCodeClassLoaders.clear();
		}

		private static ClassLoader take() throws InterruptedException {
			return userCodeClassLoaders.take();
		}
	}
}
