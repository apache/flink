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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils;
import org.apache.flink.runtime.taskexecutor.slot.TestingTaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.ThreadSafeTaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;

/**
 * Simple environment setup for task executor task.
 */
class TaskSubmissionTestEnvironment implements AutoCloseable {

	private final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);
	private final TestingRpcService testingRpcService;
	private final BlobCacheService blobCacheService = new BlobCacheService(new Configuration(), new VoidBlobStore(), null);
	private final Time timeout = Time.milliseconds(10000L);
	private final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
	private final TimerService<AllocationID> timerService = new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds());

	private final TestingHighAvailabilityServices haServices;
	private final TemporaryFolder temporaryFolder;
	private final ThreadSafeTaskSlotTable<Task> threadSafeTaskSlotTable;
	private final JobMasterId jobMasterId;

	private TestingTaskExecutor taskExecutor;

	private TaskSubmissionTestEnvironment(
			JobID jobId,
			JobMasterId jobMasterId,
			int slotSize,
			TestingJobMasterGateway testingJobMasterGateway,
			Configuration configuration,
			List<Tuple3<ExecutionAttemptID, ExecutionState, CompletableFuture<Void>>> taskManagerActionListeners,
			@Nullable String metricQueryServiceAddress,
			TestingRpcService testingRpcService,
			ShuffleEnvironment<?, ?> shuffleEnvironment) throws Exception {

		this.haServices = new TestingHighAvailabilityServices();
		this.haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
		this.haServices.setJobMasterLeaderRetriever(jobId, new SettableLeaderRetrievalService());

		this.temporaryFolder = new TemporaryFolder();
		this.temporaryFolder.create();

		this.jobMasterId = jobMasterId;

		final TaskSlotTable<Task> taskSlotTable = slotSize > 0 ?
			TaskSlotUtils.createTaskSlotTable(slotSize) :
			TestingTaskSlotTable
				.<Task>newBuilder()
				.tryMarkSlotActiveReturns(true)
				.addTaskReturns(true)
				.closeAsyncReturns(CompletableFuture.completedFuture(null))
				.allocateSlotReturns(true)
				.memoryManagerGetterReturns(null)
				.build();

		JobMasterGateway jobMasterGateway;
		if (testingJobMasterGateway == null) {
			jobMasterGateway = new TestingJobMasterGatewayBuilder()
				.setFencingTokenSupplier(() -> jobMasterId)
				.build();
		} else {
			jobMasterGateway = testingJobMasterGateway;
		}

		this.testingRpcService = testingRpcService;
		final DefaultJobTable jobTable = DefaultJobTable.create();

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{temporaryFolder.newFolder()},
			Executors.directExecutor());

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setShuffleEnvironment(shuffleEnvironment)
			.setTaskSlotTable(taskSlotTable)
			.setJobTable(jobTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		taskExecutor = createTaskExecutor(taskManagerServices, metricQueryServiceAddress, configuration);

		taskExecutor.start();
		taskExecutor.waitUntilStarted();

		this.threadSafeTaskSlotTable = new ThreadSafeTaskSlotTable<>(taskSlotTable, taskExecutor.getMainThreadExecutableForTesting());

		TaskManagerActions taskManagerActions;
		if (taskManagerActionListeners.size() == 0) {
			taskManagerActions = new NoOpTaskManagerActions();
		} else {
			TestTaskManagerActions testTaskManagerActions = new TestTaskManagerActions(threadSafeTaskSlotTable, jobMasterGateway);
			for (Tuple3<ExecutionAttemptID, ExecutionState, CompletableFuture<Void>> listenerTuple : taskManagerActionListeners) {
				testTaskManagerActions.addListener(listenerTuple.f0, listenerTuple.f1, listenerTuple.f2);
			}
			taskManagerActions = testTaskManagerActions;
		}

		registerJobMasterConnection(jobTable, jobId, testingRpcService, jobMasterGateway, taskManagerActions, timeout);
	}

	static void registerJobMasterConnection(
			JobTable jobTable,
			JobID jobId,
			RpcService testingRpcService,
			JobMasterGateway jobMasterGateway,
			TaskManagerActions taskManagerActions,
			Time timeout) {
		final JobTable.Job job = jobTable.getOrCreateJob(jobId, () -> TestingJobServices.newBuilder().build());
		job.connect(
			ResourceID.generate(),
			jobMasterGateway,
			taskManagerActions,
			new TestCheckpointResponder(),
			new TestGlobalAggregateManager(),
			new RpcResultPartitionConsumableNotifier(jobMasterGateway, testingRpcService.getExecutor(), timeout),
			TestingPartitionProducerStateChecker.newBuilder()
				.setPartitionProducerStateFunction((jobID, intermediateDataSetID, resultPartitionID) -> CompletableFuture.completedFuture(ExecutionState.RUNNING))
				.build());
	}

	public TestingTaskExecutor getTaskExecutor() {
		return taskExecutor;
	}

	public TaskExecutorGateway getTaskExecutorGateway() {
		return taskExecutor.getSelfGateway(TaskExecutorGateway.class);
	}

	public TaskSlotTable<Task> getTaskSlotTable() {
		return threadSafeTaskSlotTable;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}

	public TestingFatalErrorHandler getTestingFatalErrorHandler() {
		return testingFatalErrorHandler;
	}

	@Nonnull
	private TestingTaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices, @Nullable String metricQueryServiceAddress, Configuration configuration) {
		final Configuration copiedConf = new Configuration(configuration);

		return new TestingTaskExecutor(
			testingRpcService,
			TaskManagerConfiguration.fromConfiguration(
				copiedConf,
				TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(copiedConf),
				InetAddress.getLoopbackAddress().getHostAddress()),
			haServices,
			taskManagerServices,
			ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			metricQueryServiceAddress,
			blobCacheService,
			testingFatalErrorHandler,
			new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
			TaskManagerRunner.createBackPressureSampleService(configuration, testingRpcService.getScheduledExecutor()));
	}

	private static ShuffleEnvironment<?, ?> createShuffleEnvironment(
			ResourceID taskManagerLocation,
			boolean localCommunication,
			Configuration configuration,
			RpcService testingRpcService,
			boolean mockShuffleEnvironment) throws Exception {
		final ShuffleEnvironment<?, ?> shuffleEnvironment;
		if (mockShuffleEnvironment) {
			shuffleEnvironment = mock(ShuffleEnvironment.class, Mockito.RETURNS_MOCKS);
		} else {
			final InetSocketAddress socketAddress = new InetSocketAddress(
				InetAddress.getByName(testingRpcService.getAddress()), configuration.getInteger(NettyShuffleEnvironmentOptions.DATA_PORT));

			final NettyConfig nettyConfig = new NettyConfig(socketAddress.getAddress(), socketAddress.getPort(),
				ConfigurationParserUtils.getPageSize(configuration), ConfigurationParserUtils.getSlot(configuration), configuration);

			shuffleEnvironment =  new NettyShuffleEnvironmentBuilder()
				.setTaskManagerLocation(taskManagerLocation)
				.setPartitionRequestInitialBackoff(configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL))
				.setPartitionRequestMaxBackoff(configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX))
				.setNettyConfig(localCommunication ? null : nettyConfig)
				.build();

			shuffleEnvironment.start();
		}

		return shuffleEnvironment;
	}

	@Override
	public void close() throws Exception {
		RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);

		timerService.stop();

		blobCacheService.close();

		temporaryFolder.delete();

		testingFatalErrorHandler.rethrowError();
	}

	public static final class Builder {

		private JobID jobId;
		private boolean mockShuffleEnvironment = true;
		private int slotSize;
		private JobMasterId jobMasterId = JobMasterId.generate();
		private TestingJobMasterGateway jobMasterGateway;
		private boolean localCommunication = true;
		private Configuration configuration = new Configuration();
		@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
		private Optional<ShuffleEnvironment<?, ?>> optionalShuffleEnvironment = Optional.empty();
		private ResourceID resourceID = ResourceID.generate();
		@Nullable
		private String metricQueryServiceAddress;

		private List<Tuple3<ExecutionAttemptID, ExecutionState, CompletableFuture<Void>>> taskManagerActionListeners = new ArrayList<>();

		public Builder(JobID jobId) {
			this.jobId = jobId;
		}

		public Builder setMetricQueryServiceAddress(String metricQueryServiceAddress) {
			this.metricQueryServiceAddress = metricQueryServiceAddress;
			return this;
		}

		public Builder useRealNonMockShuffleEnvironment() {
			this.optionalShuffleEnvironment = Optional.empty();
			this.mockShuffleEnvironment = false;
			return this;
		}

		public Builder setShuffleEnvironment(ShuffleEnvironment<?, ?> optionalShuffleEnvironment) {
			this.mockShuffleEnvironment = false;
			this.optionalShuffleEnvironment = Optional.of(optionalShuffleEnvironment);
			return this;
		}

		public Builder setSlotSize(int slotSize) {
			this.slotSize = slotSize;
			return this;
		}

		public Builder setJobMasterId(JobMasterId jobMasterId) {
			this.jobMasterId = jobMasterId;
			return this;
		}

		public Builder setJobMasterGateway(TestingJobMasterGateway jobMasterGateway) {
			this.jobMasterGateway = jobMasterGateway;
			return this;
		}

		public Builder setLocalCommunication(boolean localCommunication) {
			this.localCommunication = localCommunication;
			return this;
		}

		public Builder setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}

		public Builder addTaskManagerActionListener(ExecutionAttemptID eid, ExecutionState executionState, CompletableFuture<Void> future) {
			taskManagerActionListeners.add(Tuple3.of(eid, executionState, future));
			return this;
		}

		public Builder setResourceID(ResourceID resourceID) {
			this.resourceID = resourceID;
			return this;
		}

		public TaskSubmissionTestEnvironment build() throws Exception {
			final TestingRpcService testingRpcService = new TestingRpcService();
			final ShuffleEnvironment<?, ?> network = optionalShuffleEnvironment.orElseGet(() -> {
				try {
					return createShuffleEnvironment(resourceID,
						localCommunication,
						configuration,
						testingRpcService,
						mockShuffleEnvironment);
				} catch (Exception e) {
					throw new FlinkRuntimeException("Failed to build TaskSubmissionTestEnvironment", e);
				}
			});
			return new TaskSubmissionTestEnvironment(
				jobId,
				jobMasterId,
				slotSize,
				jobMasterGateway,
				configuration,
				taskManagerActionListeners,
				metricQueryServiceAddress,
				testingRpcService,
				network);
		}
	}
}
