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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ActiveResourceManager}.
 */
public class ActiveResourceManagerTest extends TestLogger {

	@ClassRule
	public static final TestingRpcServiceResource RPC_SERVICE_RESOURCE = new TestingRpcServiceResource();

	private static final long TIMEOUT_SEC = 5L;
	private static final Time TIMEOUT_TIME = Time.seconds(TIMEOUT_SEC);

	private static final WorkerResourceSpec WORKER_RESOURCE_SPEC = WorkerResourceSpec.ZERO;

	/**
	 * Tests worker successfully requested, started and registered.
	 */
	@Test
	public void testStartNewWorker() throws Exception {
		new Context() {{
			final ResourceID tmResourceId = ResourceID.generate();
			final CompletableFuture<TaskExecutorProcessSpec> requestWorkerFromDriverFuture = new CompletableFuture<>();

			driverBuilder.setRequestResourceFunction(taskExecutorProcessSpec -> {
				requestWorkerFromDriverFuture.complete(taskExecutorProcessSpec);
				return CompletableFuture.completedFuture(tmResourceId);
			});

			runTest(() -> {
				// received worker request, verify requesting from driver
				CompletableFuture<Boolean> startNewWorkerFuture = runInMainThread(() ->
						getResourceManager().startNewWorker(WORKER_RESOURCE_SPEC));
				TaskExecutorProcessSpec taskExecutorProcessSpec = requestWorkerFromDriverFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
				assertThat(taskExecutorProcessSpec,
						is(TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, WORKER_RESOURCE_SPEC)));

				// worker registered, verify registration succeeded
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture = registerTaskExecutor(tmResourceId);
				assertThat(registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Success.class));
			});
		}};
	}

	/**
	 * Tests worker failed while requesting.
	 */
	@Test
	public void testStartNewWorkerFailedRequesting() throws Exception {
		new Context() {{
			final ResourceID tmResourceId = ResourceID.generate();
			final AtomicInteger requestCount = new AtomicInteger(0);

			final List<CompletableFuture<ResourceID>> resourceIdFutures = new ArrayList<>();
			resourceIdFutures.add(new CompletableFuture<>());
			resourceIdFutures.add(new CompletableFuture<>());

			final List<CompletableFuture<TaskExecutorProcessSpec>> requestWorkerFromDriverFutures = new ArrayList<>();
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());

			driverBuilder.setRequestResourceFunction(taskExecutorProcessSpec -> {
				int idx = requestCount.getAndIncrement();
				assertThat(idx, lessThan(2));

				requestWorkerFromDriverFutures.get(idx).complete(taskExecutorProcessSpec);
				return resourceIdFutures.get(idx);
			});

			slotManagerBuilder.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

			runTest(() -> {
				// received worker request, verify requesting from driver
				CompletableFuture<Boolean> startNewWorkerFuture = runInMainThread(() ->
						getResourceManager().startNewWorker(WORKER_RESOURCE_SPEC));
				TaskExecutorProcessSpec taskExecutorProcessSpec1 = requestWorkerFromDriverFutures.get(0).get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
				assertThat(taskExecutorProcessSpec1,
						is(TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, WORKER_RESOURCE_SPEC)));

				// first request failed, verify requesting another worker from driver
				runInMainThread(() -> resourceIdFutures.get(0).completeExceptionally(new Throwable("testing error")));
				TaskExecutorProcessSpec taskExecutorProcessSpec2 =
						requestWorkerFromDriverFutures.get(1).get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

				// second request allocated, verify registration succeed
				runInMainThread(() -> resourceIdFutures.get(1).complete(tmResourceId));
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture = registerTaskExecutor(tmResourceId);
				assertThat(registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Success.class));
			});
		}};
	}

	/**
	 * Tests worker terminated after requested before registered.
	 */
	@Test
	public void testWorkerTerminatedBeforeRegister() throws Exception {
		new Context() {{
			final AtomicInteger requestCount = new AtomicInteger(0);

			final List<ResourceID> tmResourceIds = new ArrayList<>();
			tmResourceIds.add(ResourceID.generate());
			tmResourceIds.add(ResourceID.generate());

			final List<CompletableFuture<TaskExecutorProcessSpec>> requestWorkerFromDriverFutures = new ArrayList<>();
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());

			driverBuilder.setRequestResourceFunction(taskExecutorProcessSpec -> {
				int idx = requestCount.getAndIncrement();
				assertThat(idx, lessThan(2));

				requestWorkerFromDriverFutures.get(idx).complete(taskExecutorProcessSpec);
				return CompletableFuture.completedFuture(tmResourceIds.get(idx));
			});

			slotManagerBuilder.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

			runTest(() -> {
				// received worker request, verify requesting from driver
				CompletableFuture<Boolean> startNewWorkerFuture = runInMainThread(() ->
						getResourceManager().startNewWorker(WORKER_RESOURCE_SPEC));
				TaskExecutorProcessSpec taskExecutorProcessSpec1 = requestWorkerFromDriverFutures.get(0).get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
				assertThat(taskExecutorProcessSpec1,
						is(TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, WORKER_RESOURCE_SPEC)));

				// first worker failed before register, verify requesting another worker from driver
				runInMainThread(() -> getResourceManager().onWorkerTerminated(tmResourceIds.get(0), "terminate for testing"));
				TaskExecutorProcessSpec taskExecutorProcessSpec2 =
						requestWorkerFromDriverFutures.get(1).get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

				// second worker registered, verify registration succeed
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture = registerTaskExecutor(tmResourceIds.get(1));
				assertThat(registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Success.class));
			});
		}};
	}

	/**
	 * Tests worker terminated after registered.
	 */
	@Test
	public void testWorkerTerminatedAfterRegister() throws Exception {
		new Context() {{
			final AtomicInteger requestCount = new AtomicInteger(0);

			final List<ResourceID> tmResourceIds = new ArrayList<>();
			tmResourceIds.add(ResourceID.generate());
			tmResourceIds.add(ResourceID.generate());

			final List<CompletableFuture<TaskExecutorProcessSpec>> requestWorkerFromDriverFutures = new ArrayList<>();
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());

			driverBuilder.setRequestResourceFunction(taskExecutorProcessSpec -> {
				int idx = requestCount.getAndIncrement();
				assertThat(idx, lessThan(2));

				requestWorkerFromDriverFutures.get(idx).complete(taskExecutorProcessSpec);
				return CompletableFuture.completedFuture(tmResourceIds.get(idx));
			});

			slotManagerBuilder.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(WORKER_RESOURCE_SPEC, 1));

			runTest(() -> {
				// received worker request, verify requesting from driver
				CompletableFuture<Boolean> startNewWorkerFuture = runInMainThread(() ->
						getResourceManager().startNewWorker(WORKER_RESOURCE_SPEC));
				TaskExecutorProcessSpec taskExecutorProcessSpec1 = requestWorkerFromDriverFutures.get(0).get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
				assertThat(taskExecutorProcessSpec1,
						is(TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, WORKER_RESOURCE_SPEC)));

				// first worker registered, verify registration succeed
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture1 = registerTaskExecutor(tmResourceIds.get(0));
				assertThat(registerTaskExecutorFuture1.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Success.class));

				// first worker terminated, verify requesting another worker from driver
				runInMainThread(() -> getResourceManager().onWorkerTerminated(tmResourceIds.get(0), "terminate for testing"));
				TaskExecutorProcessSpec taskExecutorProcessSpec2 =
						requestWorkerFromDriverFutures.get(1).get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

				// second worker registered, verify registration succeed
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture2 = registerTaskExecutor(tmResourceIds.get(1));
				assertThat(registerTaskExecutorFuture2.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Success.class));
			});
		}};
	}

	/**
	 * Tests worker terminated and is no longer required.
	 */
	@Test
	public void testWorkerTerminatedNoLongerRequired() throws Exception {
		new Context() {{
			final ResourceID tmResourceId = ResourceID.generate();
			final AtomicInteger requestCount = new AtomicInteger(0);

			final List<CompletableFuture<TaskExecutorProcessSpec>> requestWorkerFromDriverFutures = new ArrayList<>();
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());
			requestWorkerFromDriverFutures.add(new CompletableFuture<>());

			driverBuilder.setRequestResourceFunction(taskExecutorProcessSpec -> {
				int idx = requestCount.getAndIncrement();
				assertThat(idx, lessThan(2));

				requestWorkerFromDriverFutures.get(idx).complete(taskExecutorProcessSpec);
				return CompletableFuture.completedFuture(tmResourceId);
			});

			runTest(() -> {
				// received worker request, verify requesting from driver
				CompletableFuture<Boolean> startNewWorkerFuture = runInMainThread(() ->
						getResourceManager().startNewWorker(WORKER_RESOURCE_SPEC));
				TaskExecutorProcessSpec taskExecutorProcessSpec = requestWorkerFromDriverFutures.get(0).get(TIMEOUT_SEC, TimeUnit.SECONDS);

				assertThat(startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), is(true));
				assertThat(taskExecutorProcessSpec,
						is(TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, WORKER_RESOURCE_SPEC)));

				// worker registered, verify registration succeed
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture = registerTaskExecutor(tmResourceId);
				assertThat(registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Success.class));

				// worker terminated, verify not requesting new worker
				runInMainThread(() -> {
					getResourceManager().onWorkerTerminated(tmResourceId, "terminate for testing");
					// needs to return something, so that we can use `get()` to make sure the main thread processing
					// finishes before the assertions
					return null;
				}).get(TIMEOUT_SEC, TimeUnit.SECONDS);
				assertFalse(requestWorkerFromDriverFutures.get(1).isDone());
			});
		}};
	}

	@Test
	public void testCloseTaskManagerConnectionOnWorkerTerminated() throws Exception {
		new Context() {{
			final ResourceID tmResourceId = ResourceID.generate();
			final CompletableFuture<TaskExecutorProcessSpec> requestWorkerFromDriverFuture = new CompletableFuture<>();
			final CompletableFuture<Void> disconnectResourceManagerFuture = new CompletableFuture<>();

			final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setDisconnectResourceManagerConsumer((ignore) -> disconnectResourceManagerFuture.complete(null))
				.createTestingTaskExecutorGateway();

			driverBuilder.setRequestResourceFunction(taskExecutorProcessSpec -> {
				requestWorkerFromDriverFuture.complete(taskExecutorProcessSpec);
				return CompletableFuture.completedFuture(tmResourceId);
			});

			runTest(() -> {
				// request a new worker, terminate it after registered
				runInMainThread(() -> getResourceManager().startNewWorker(WORKER_RESOURCE_SPEC))
					.thenCompose((ignore) -> registerTaskExecutor(tmResourceId, taskExecutorGateway))
					.thenRun(() -> runInMainThread(() -> getResourceManager().onWorkerTerminated(tmResourceId, "terminate for testing")));
				// verify task manager connection is closed
				disconnectResourceManagerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
			});
		}};
	}

	/**
	 * Tests workers from previous attempt successfully recovered and registered.
	 */
	@Test
	public void testRecoverWorkerFromPreviousAttempt() throws Exception {
		new Context() {{
			final ResourceID tmResourceId = ResourceID.generate();

			runTest(() -> {
				runInMainThread(() -> getResourceManager().onPreviousAttemptWorkersRecovered(Collections.singleton(tmResourceId)));
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture = registerTaskExecutor(tmResourceId);
				assertThat(registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Success.class));
			});
		}};
	}

	/**
	 * Tests decline unknown worker registration.
	 */
	@Test
	public void testRegisterUnknownWorker() throws Exception {
		new Context() {{
			runTest(() -> {
				CompletableFuture<RegistrationResponse> registerTaskExecutorFuture = registerTaskExecutor(ResourceID.generate());
				assertThat(registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS), instanceOf(RegistrationResponse.Decline.class));
			});
		}};
	}

	@Test
	public void testOnError() throws Exception {
		new Context() {{
			final Throwable fatalError = new Throwable("Testing fatal error");
			runTest(() -> {
				runInMainThread(() -> getResourceManager().onError(fatalError));
				final Throwable reportedError = getFatalErrorHandler().getErrorFuture().get(TIMEOUT_SEC, TimeUnit.SECONDS);
				assertThat(reportedError, is(fatalError));
			});
		}};
	}

	private static class Context {

		final Configuration flinkConfig = new Configuration();
		final TestingResourceManagerDriver.Builder driverBuilder = new TestingResourceManagerDriver.Builder();
		final TestingSlotManagerBuilder slotManagerBuilder = new TestingSlotManagerBuilder();

		private ActiveResourceManager<ResourceID> resourceManager;
		private TestingFatalErrorHandler fatalErrorHandler;

		ActiveResourceManager<ResourceID> getResourceManager() {
			return resourceManager;
		}

		TestingFatalErrorHandler getFatalErrorHandler() {
			return fatalErrorHandler;
		}

		void runTest(RunnableWithException testMethod) throws Exception {
			fatalErrorHandler = new TestingFatalErrorHandler();
			resourceManager = createAndStartResourceManager(
					flinkConfig,
					driverBuilder.build(),
					slotManagerBuilder.createSlotManager());

			try {
				testMethod.run();
			} finally {
				resourceManager.close();
			}
		}

		private ActiveResourceManager<ResourceID> createAndStartResourceManager(
				Configuration configuration,
				ResourceManagerDriver<ResourceID> driver,
				SlotManager slotManager) throws Exception {
			final TestingRpcService rpcService = RPC_SERVICE_RESOURCE.getTestingRpcService();
			final MockResourceManagerRuntimeServices rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT_TIME, slotManager);

			final ActiveResourceManager<ResourceID> activeResourceManager = new ActiveResourceManager<>(
					driver,
					configuration,
					rpcService,
					ResourceID.generate(),
					rmServices.highAvailabilityServices,
					rmServices.heartbeatServices,
					rmServices.slotManager,
					NoOpResourceManagerPartitionTracker::get,
					rmServices.jobLeaderIdService,
					new ClusterInformation("localhost", 1234),
					fatalErrorHandler,
					UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());

			activeResourceManager.start();
			rmServices.grantLeadership();

			return activeResourceManager;
		}

		void runInMainThread(Runnable runnable) {
			resourceManager.runInMainThread(() -> {
				runnable.run();
				return null;
			}, TIMEOUT_TIME);
		}

		<T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
			return resourceManager.runInMainThread(callable, TIMEOUT_TIME);
		}

		CompletableFuture<RegistrationResponse> registerTaskExecutor(ResourceID resourceID) {
			final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.createTestingTaskExecutorGateway();
			return registerTaskExecutor(resourceID, taskExecutorGateway);
		}

		CompletableFuture<RegistrationResponse> registerTaskExecutor(
				ResourceID resourceID, TaskExecutorGateway taskExecutorGateway) {
			RPC_SERVICE_RESOURCE.getTestingRpcService().registerGateway(resourceID.toString(), taskExecutorGateway);

			final TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
					resourceID.toString(),
					resourceID,
					1234,
					new HardwareDescription(1, 2L, 3L, 4L),
					TaskExecutorMemoryConfiguration.create(flinkConfig),
					ResourceProfile.ZERO,
					ResourceProfile.ZERO);

			return resourceManager.getSelfGateway(ResourceManagerGateway.class)
					.registerTaskExecutor(taskExecutorRegistration, TIMEOUT_TIME);
		}
	}
}
