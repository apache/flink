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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.partition.PartitionTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriConsumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the partition-lifecycle logic in the {@link TaskExecutor}.
 */
public class TaskExecutorPartitionLifecycleTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);

	private static final TestingRpcService RPC = new TestingRpcService();

	private final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
	private final SettableLeaderRetrievalService jobManagerLeaderRetriever = new SettableLeaderRetrievalService();
	private final SettableLeaderRetrievalService resourceManagerLeaderRetriever = new SettableLeaderRetrievalService();
	private final JobID jobId = new JobID();

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Before
	public void setup() {
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);
	}

	@After
	public void shutdown() {
		RPC.clearGateways();
	}

	@AfterClass
	public static void shutdownClass() throws ExecutionException, InterruptedException {
		RPC.stopService().get();
	}

	@Test
	public void testConnectionTerminationAfterExternalRelease() throws Exception {
		final CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
		final JobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setDisconnectTaskManagerFunction(resourceID -> {
				disconnectFuture.complete(null);
				return CompletableFuture.completedFuture(Acknowledge.get());
			}).build();

		final JobManagerConnection jobManagerConnection = TaskSubmissionTestEnvironment.createJobManagerConnection(
			jobId, jobMasterGateway, RPC, new NoOpTaskManagerActions(), timeout);

		final JobManagerTable jobManagerTable = new JobManagerTable();
		jobManagerTable.put(jobId, jobManagerConnection);

		final TestingShuffleEnvironment shuffleEnvironment = new TestingShuffleEnvironment();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setJobManagerTable(jobManagerTable)
			.setShuffleEnvironment(shuffleEnvironment)
			.setTaskSlotTable(createTaskSlotTable())
			.build();

		final PartitionTable<JobID> partitionTable = new PartitionTable<>();
		final ResultPartitionID resultPartitionId = new ResultPartitionID();

		final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices, partitionTable);

		try {
			taskExecutor.start();
			taskExecutor.waitUntilStarted();

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			// baseline, jobmanager was added in test setup
			runInTaskExecutorThreadAndWait(taskExecutor, () -> assertTrue(jobManagerTable.contains(jobId)));

			runInTaskExecutorThreadAndWait(taskExecutor, () -> partitionTable.startTrackingPartitions(jobId, Collections.singletonList(resultPartitionId)));

			final CompletableFuture<Collection<ResultPartitionID>> firstReleasePartitionsCallFuture = new CompletableFuture<>();
			runInTaskExecutorThreadAndWait(taskExecutor, () -> shuffleEnvironment.releasePartitionsLocallyFuture = firstReleasePartitionsCallFuture);

			taskExecutorGateway.releasePartitions(jobId, Collections.singletonList(new ResultPartitionID()));

			// at this point we only know that the TE has entered releasePartitions; we cannot be certain whether it
			// has already checked whether it should disconnect or not
			firstReleasePartitionsCallFuture.get();

			// connection should be kept alive since the table still contains partitions
			// once this returns we know that the TE has exited releasePartitions and associated connection checks
			runInTaskExecutorThreadAndWait(taskExecutor, () -> assertTrue(jobManagerTable.contains(jobId)));

			final CompletableFuture<Collection<ResultPartitionID>> secondReleasePartitionsCallFuture = new CompletableFuture<>();
			runInTaskExecutorThreadAndWait(taskExecutor, () -> shuffleEnvironment.releasePartitionsLocallyFuture = secondReleasePartitionsCallFuture);

			// the TM should check whether partitions are still stored, and afterwards terminate the connection
			taskExecutorGateway.releasePartitions(jobId, Collections.singletonList(resultPartitionId));

			disconnectFuture.get();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Test
	public void testBlockingPartitionReleaseAfterDisconnect() throws Exception {
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> taskExecutorGateway.disconnectJobManager(jobId, new Exception("test")),
			true,
			ResultPartitionType.BLOCKING);
	}

	@Test
	public void testPipelinedPartitionNotReleasedAfterDisconnect() throws Exception {
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> taskExecutorGateway.disconnectJobManager(jobId, new Exception("test")),
			false,
			ResultPartitionType.PIPELINED);
	}

	@Test
	public void testBlockingPartitionReleaseAfterReleaseCall() throws Exception {
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> taskExecutorGateway.releasePartitions(jobId, Collections.singletonList(partitionId)),
			true,
			ResultPartitionType.BLOCKING);
	}

	@Test
	public void testPipelinedPartitionReleaseAfterReleaseCall() throws Exception {
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> taskExecutorGateway.releasePartitions(jobId, Collections.singletonList(partitionId)),
			true,
			ResultPartitionType.PIPELINED);
	}

	@Test
	public void testBlockingPartitionReleaseAfterShutdown() throws Exception {
		// don't do any explicit release action, so that the partition must be cleaned up on shutdown
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> { },
			false,
			ResultPartitionType.BLOCKING);
	}

	@Test
	public void testPipelinedPartitionReleaseAfterShutdown() throws Exception {
		// don't do any explicit release action, so that the partition must be cleaned up on shutdown
		testPartitionRelease(
			(jobId, partitionId, taskExecutorGateway) -> { },
			false,
			ResultPartitionType.PIPELINED);
	}

	private void testPartitionRelease(
		TriConsumer<JobID, ResultPartitionID, TaskExecutorGateway> releaseAction,
		boolean waitForRelease,
		ResultPartitionType resultPartitionType) throws Exception {

		final ResultPartitionDeploymentDescriptor taskResultPartitionDescriptor =
			PartitionTestUtils.createPartitionDeploymentDescriptor(resultPartitionType);
		final ExecutionAttemptID eid1 = taskResultPartitionDescriptor.getShuffleDescriptor().getResultPartitionID().getProducerId();

		final TaskDeploymentDescriptor taskDeploymentDescriptor =
			TaskExecutorSubmissionTest.createTaskDeploymentDescriptor(
				jobId,
				"job",
				eid1,
				new SerializedValue<>(new ExecutionConfig()),
				"Sender",
				1,
				0,
				1,
				0,
				new Configuration(),
				new Configuration(),
				TestingInvokable.class.getName(),
				Collections.singletonList(taskResultPartitionDescriptor),
				Collections.emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				0);

		final TaskSlotTable taskSlotTable = createTaskSlotTable();

		final TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final TestingShuffleEnvironment shuffleEnvironment = new TestingShuffleEnvironment();

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(localStateStoresManager)
			.setShuffleEnvironment(shuffleEnvironment)
			.build();

		final CompletableFuture<Void> taskFinishedFuture = new CompletableFuture<>();
		final OneShotLatch slotOfferedLatch = new OneShotLatch();

		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setRegisterTaskManagerFunction((s, location) -> CompletableFuture.completedFuture(new JMTMRegistrationSuccess(ResourceID.generate())))
			.setOfferSlotsFunction((resourceID, slotOffers) -> {
				slotOfferedLatch.trigger();
				return CompletableFuture.completedFuture(slotOffers);
			})
			.setUpdateTaskExecutionStateFunction(taskExecutionState -> {
				if (taskExecutionState.getExecutionState() == ExecutionState.FINISHED) {
					taskFinishedFuture.complete(null);
				}
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.build();

		final PartitionTable<JobID> partitionTable = new PartitionTable<>();

		final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices, partitionTable);

		final CompletableFuture<SlotReport> initialSlotReportFuture = new CompletableFuture<>();

		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		testingResourceManagerGateway.setSendSlotReportFunction(resourceIDInstanceIDSlotReportTuple3 -> {
			initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f2);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});
		testingResourceManagerGateway.setRegisterTaskExecutorFunction(input -> CompletableFuture.completedFuture(
			new TaskExecutorRegistrationSuccess(
				new InstanceID(),
				testingResourceManagerGateway.getOwnResourceId(),
				new ClusterInformation("blobServerHost", 55555))));

		try {
			taskExecutor.start();
			taskExecutor.waitUntilStarted();

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			final String jobMasterAddress = "jm";
			RPC.registerGateway(jobMasterAddress, jobMasterGateway);
			RPC.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			// inform the task manager about the job leader
			taskManagerServices.getJobLeaderService().addJob(jobId, jobMasterAddress);
			jobManagerLeaderRetriever.notifyListener(jobMasterAddress, UUID.randomUUID());
			resourceManagerLeaderRetriever.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final Optional<SlotStatus> slotStatusOptional = StreamSupport.stream(initialSlotReportFuture.get().spliterator(), false)
				.findAny();

			assertTrue(slotStatusOptional.isPresent());

			final SlotStatus slotStatus = slotStatusOptional.get();

			while (true) {
				try {
					taskExecutorGateway.requestSlot(
						slotStatus.getSlotID(),
						jobId,
						taskDeploymentDescriptor.getAllocationId(),
						jobMasterAddress,
						testingResourceManagerGateway.getFencingToken(),
						timeout
					).get();
					break;
				} catch (Exception e) {
					// the proper establishment of the RM connection is tracked
					// asynchronously, so we have to poll here until it went through
					// until then, slot requests will fail with an exception
					Thread.sleep(50);
				}
			}

			TestingInvokable.sync = new BlockerSync();

			// Wait till the slot has been successfully offered before submitting the task.
			// This ensures TM has been successfully registered to JM.
			slotOfferedLatch.await();

			taskExecutorGateway.submitTask(taskDeploymentDescriptor, jobMasterGateway.getFencingToken(), timeout)
				.get();

			TestingInvokable.sync.awaitBlocker();

			// the task is still running => the partition is in in-progress
			runInTaskExecutorThreadAndWait(
				taskExecutor,
				() -> assertThat(partitionTable.hasTrackedPartitions(jobId), is(resultPartitionType.isBlocking())));

			TestingInvokable.sync.releaseBlocker();
			taskFinishedFuture.get(timeout.getSize(), timeout.getUnit());

			// the task is finished => the partition should be finished now
			runInTaskExecutorThreadAndWait(
				taskExecutor,
				() -> assertThat(partitionTable.hasTrackedPartitions(jobId), is(resultPartitionType.isBlocking())));

			final CompletableFuture<Collection<ResultPartitionID>> releasePartitionsFuture = new CompletableFuture<>();
			runInTaskExecutorThreadAndWait(
				taskExecutor,
				() -> shuffleEnvironment.releasePartitionsLocallyFuture = releasePartitionsFuture);

			releaseAction.accept(
				jobId,
				taskResultPartitionDescriptor.getShuffleDescriptor().getResultPartitionID(),
				taskExecutorGateway);

			if (waitForRelease) {
				Collection<ResultPartitionID> resultPartitionIDS = releasePartitionsFuture.get();
				assertThat(resultPartitionIDS, contains(taskResultPartitionDescriptor.getShuffleDescriptor().getResultPartitionID()));
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}

		// the shutdown of the backing shuffle environment releases all partitions
		// the book-keeping is not aware of this
		assertTrue(shuffleEnvironment.getPartitionsOccupyingLocalResources().isEmpty());
	}

	/**
	 * Test invokable which completes the given future when executed.
	 */
	public static class TestingInvokable extends AbstractInvokable {

		static BlockerSync sync;

		public TestingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			sync.block();
		}
	}

	private TestingTaskExecutor createTestingTaskExecutor(TaskManagerServices taskManagerServices, PartitionTable<JobID> partitionTable) throws IOException {
		return new TestingTaskExecutor(
			RPC,
			TaskManagerConfiguration.fromConfiguration(new Configuration()),
			haServices,
			taskManagerServices,
			new HeartbeatServices(10_000L, 30_000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			new BlobCacheService(
				new Configuration(),
				new VoidBlobStore(),
				null),
			new TestingFatalErrorHandler(),
			partitionTable);
	}

	private static TaskSlotTable createTaskSlotTable() {
		return new TaskSlotTable(
			Collections.singletonList(ResourceProfile.UNKNOWN),
			new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds()));

	}

	private static void runInTaskExecutorThreadAndWait(TaskExecutor taskExecutor, Runnable runnable) throws ExecutionException, InterruptedException {
		taskExecutor.getRpcService().scheduleRunnable(
			runnable,
			0,
			TimeUnit.SECONDS
		).get();
	}

	private static class TestingShuffleEnvironment implements ShuffleEnvironment<ResultPartition, SingleInputGate> {

		private final ShuffleEnvironment<ResultPartition, SingleInputGate> backingShuffleEnvironment =
			new NettyShuffleEnvironmentBuilder().build();

		CompletableFuture<Collection<ResultPartitionID>> releasePartitionsLocallyFuture = null;

		@Override
		public int start() throws IOException {
			return backingShuffleEnvironment.start();
		}

		@Override
		public ShuffleIOOwnerContext createShuffleIOOwnerContext(String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
			return backingShuffleEnvironment.createShuffleIOOwnerContext(ownerName, executionAttemptID, parentGroup);
		}

		@Override
		public Collection<ResultPartition> createResultPartitionWriters(ShuffleIOOwnerContext ownerContext, Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {
			return backingShuffleEnvironment.createResultPartitionWriters(ownerContext, resultPartitionDeploymentDescriptors);
		}

		@Override
		public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
			backingShuffleEnvironment.releasePartitionsLocally(partitionIds);
			if (releasePartitionsLocallyFuture != null) {
				releasePartitionsLocallyFuture.complete(partitionIds);
			}
		}

		@Override
		public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
			return backingShuffleEnvironment.getPartitionsOccupyingLocalResources();
		}

		@Override
		public Collection<SingleInputGate> createInputGates(ShuffleIOOwnerContext ownerContext, PartitionProducerStateProvider partitionProducerStateProvider, Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {
			return backingShuffleEnvironment.createInputGates(ownerContext, partitionProducerStateProvider, inputGateDeploymentDescriptors);
		}

		@Override
		public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo) throws IOException, InterruptedException {
			return backingShuffleEnvironment.updatePartitionInfo(consumerID, partitionInfo);
		}

		@Override
		public void close() throws Exception {
			backingShuffleEnvironment.close();
		}
	}
}
