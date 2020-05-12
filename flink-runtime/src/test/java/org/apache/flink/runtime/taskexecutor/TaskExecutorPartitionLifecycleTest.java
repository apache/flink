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
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.io.network.partition.TestingTaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
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
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriConsumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the partition-lifecycle logic in the {@link TaskExecutor}.
 */
public class TaskExecutorPartitionLifecycleTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);

	private static TestingRpcService rpc;

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
		rpc.clearGateways();
	}

	@BeforeClass
	public static void setupClass() {
		rpc = new TestingRpcService();
	}

	@AfterClass
	public static void shutdownClass() throws ExecutionException, InterruptedException {
		rpc.stopService().get();
	}

	@Test
	public void testJobMasterConnectionTerminationAfterExternalRelease() throws Exception {
		testJobMasterConnectionTerminationAfterExternalReleaseOrPromotion(
			((taskExecutorGateway, jobID, resultPartitionID) -> taskExecutorGateway.releaseOrPromotePartitions(jobID, Collections.singleton(resultPartitionID), Collections.emptySet()))
		);
	}

	@Test
	public void testJobMasterConnectionTerminationAfterExternalPromotion() throws Exception {
		testJobMasterConnectionTerminationAfterExternalReleaseOrPromotion(
			((taskExecutorGateway, jobID, resultPartitionID) -> taskExecutorGateway.releaseOrPromotePartitions(jobID, Collections.emptySet(), Collections.singleton(resultPartitionID)))
		);
	}

	private void testJobMasterConnectionTerminationAfterExternalReleaseOrPromotion(TriConsumer<TaskExecutorGateway, JobID, ResultPartitionID> releaseOrPromoteCall) throws Exception {
		final CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
		final JobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setDisconnectTaskManagerFunction(resourceID -> {
				disconnectFuture.complete(null);
				return CompletableFuture.completedFuture(Acknowledge.get());
			}).build();

		final DefaultJobTable jobTable = DefaultJobTable.create();
		TaskSubmissionTestEnvironment.registerJobMasterConnection(
			jobTable,
			jobId,
			rpc,
			jobMasterGateway,
			new NoOpTaskManagerActions(),
			timeout);

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setJobTable(jobTable)
			.setShuffleEnvironment(new NettyShuffleEnvironmentBuilder().build())
			.setTaskSlotTable(createTaskSlotTable())
			.build();

		final TestingTaskExecutorPartitionTracker partitionTracker = new TestingTaskExecutorPartitionTracker();

		final AtomicBoolean trackerIsTrackingPartitions = new AtomicBoolean(false);
		partitionTracker.setIsTrackingPartitionsForFunction(jobId -> trackerIsTrackingPartitions.get());

		final CompletableFuture<Collection<ResultPartitionID>> firstReleasePartitionsCallFuture = new CompletableFuture<>();
		partitionTracker.setStopTrackingAndReleasePartitionsConsumer(firstReleasePartitionsCallFuture::complete);

		final ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = PartitionTestUtils.createPartitionDeploymentDescriptor(ResultPartitionType.BLOCKING);
		final ResultPartitionID resultPartitionId = resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

		final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices, partitionTracker);

		try {
			taskExecutor.start();
			taskExecutor.waitUntilStarted();

			final TaskExecutorGateway taskExecutorGateway = taskExecutor.getSelfGateway(TaskExecutorGateway.class);

			trackerIsTrackingPartitions.set(true);
			assertThat(firstReleasePartitionsCallFuture.isDone(), is(false));

			taskExecutorGateway.releaseOrPromotePartitions(jobId, Collections.singleton(new ResultPartitionID()), Collections.emptySet());

			// at this point we only know that the TE has entered releasePartitions; we cannot be certain whether it
			// has already checked whether it should disconnect or not
			firstReleasePartitionsCallFuture.get();

			// connection should be kept alive since the table still contains partitions
			assertThat(disconnectFuture.isDone(), is(false));

			trackerIsTrackingPartitions.set(false);

			// the TM should check whether partitions are still stored, and afterwards terminate the connection
			releaseOrPromoteCall.accept(taskExecutorGateway, jobId, resultPartitionId);

			disconnectFuture.get();
		} finally {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}
	}

	@Test
	public void testPartitionReleaseAfterJobMasterDisconnect() throws Exception {
		testPartitionRelease(
			partitionTracker -> {
				final CompletableFuture<JobID> releasePartitionsForJobFuture = new CompletableFuture<>();
				partitionTracker.setStopTrackingAndReleaseAllPartitionsConsumer(releasePartitionsForJobFuture::complete);
				return releasePartitionsForJobFuture;
			},
			(jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway, releasePartitionsForJobFuture) -> {
				taskExecutorGateway.disconnectJobManager(jobId, new Exception("test"));

				assertThat(releasePartitionsForJobFuture.get(), equalTo(jobId));
			}
		);
	}

	@Test
	public void testPartitionReleaseAfterReleaseCall() throws Exception {
		testPartitionRelease(
			partitionTracker -> {
				final CompletableFuture<Collection<ResultPartitionID>> releasePartitionsFuture = new CompletableFuture<>();
				partitionTracker.setStopTrackingAndReleasePartitionsConsumer(releasePartitionsFuture::complete);
				return releasePartitionsFuture;
			},
			(jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway, releasePartitionsFuture) -> {
				final ResultPartitionID resultPartitionId = resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

				taskExecutorGateway.releaseOrPromotePartitions(jobId, Collections.singleton(resultPartitionId), Collections.emptySet());

				assertThat(releasePartitionsFuture.get(), hasItems(resultPartitionId));
			}
		);
	}

	@Test
	public void testPartitionPromotion() throws Exception {
		testPartitionRelease(
			partitionTracker -> {
				final CompletableFuture<Collection<ResultPartitionID>> promotePartitionsFuture = new CompletableFuture<>();
				partitionTracker.setPromotePartitionsConsumer(promotePartitionsFuture::complete);
				return promotePartitionsFuture;
			},
			(jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway, promotePartitionsFuture) -> {
				final ResultPartitionID resultPartitionId = resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

				taskExecutorGateway.releaseOrPromotePartitions(jobId, Collections.emptySet(), Collections.singleton(resultPartitionId));

				assertThat(promotePartitionsFuture.get(), hasItems(resultPartitionId));
			}
		);
	}

	@Test
	public void testClusterPartitionRelease() throws Exception {
		testPartitionRelease(
			partitionTracker -> {
				final CompletableFuture<Collection<IntermediateDataSetID>> releasePartitionsFuture = new CompletableFuture<>();
				partitionTracker.setReleaseClusterPartitionsConsumer(releasePartitionsFuture::complete);
				return releasePartitionsFuture;
			},
			(jobId, resultPartitionDeploymentDescriptor, taskExecutor, taskExecutorGateway, releasePartitionsFuture) -> {
				final IntermediateDataSetID dataSetId = resultPartitionDeploymentDescriptor.getResultId();

				taskExecutorGateway.releaseClusterPartitions(Collections.singleton(dataSetId), timeout);

				assertThat(releasePartitionsFuture.get(), hasItems(dataSetId));
			}
		);
	}

	private <C> void testPartitionRelease(PartitionTrackerSetup<C> partitionTrackerSetup, TestAction<C> testAction) throws Exception {

		final ResultPartitionDeploymentDescriptor taskResultPartitionDescriptor =
			PartitionTestUtils.createPartitionDeploymentDescriptor(ResultPartitionType.BLOCKING);
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

		final TaskSlotTable<Task> taskSlotTable = createTaskSlotTable();

		final TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{tmp.newFolder()},
			Executors.directExecutor());

		final ShuffleEnvironment<?, ?> shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();

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

		final TestingTaskExecutorPartitionTracker partitionTracker = new TestingTaskExecutorPartitionTracker();
		final CompletableFuture<ResultPartitionID> startTrackingFuture = new CompletableFuture<>();
		partitionTracker.setStartTrackingPartitionsConsumer((jobId, partitionInfo) -> startTrackingFuture.complete(partitionInfo.getResultPartitionId()));
		C partitionTrackerSetupResult = partitionTrackerSetup.accept(partitionTracker);

		final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices, partitionTracker);

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
			rpc.registerGateway(jobMasterAddress, jobMasterGateway);
			rpc.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

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
						ResourceProfile.ZERO,
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

			// the task is still running => the partition is in in-progress and should be tracked
			assertThat(startTrackingFuture.get(), equalTo(taskResultPartitionDescriptor.getShuffleDescriptor().getResultPartitionID()));

			TestingInvokable.sync.releaseBlocker();
			taskFinishedFuture.get(timeout.getSize(), timeout.getUnit());

			testAction.accept(
				jobId,
				taskResultPartitionDescriptor,
				taskExecutor,
				taskExecutorGateway,
				partitionTrackerSetupResult);
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

	private TestingTaskExecutor createTestingTaskExecutor(TaskManagerServices taskManagerServices, TaskExecutorPartitionTracker partitionTracker) throws IOException {
			final Configuration configuration = new Configuration();
		return new TestingTaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(configuration, TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(configuration)),
			haServices,
			taskManagerServices,
			new HeartbeatServices(10_000L, 30_000L),
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			new BlobCacheService(
				configuration,
				new VoidBlobStore(),
				null),
			new TestingFatalErrorHandler(),
			partitionTracker,
			TaskManagerRunner.createBackPressureSampleService(configuration, rpc.getScheduledExecutor()));
	}

	private static TaskSlotTable<Task> createTaskSlotTable() {
		return TaskSlotUtils.createTaskSlotTable(1, timeout);

	}

	@FunctionalInterface
	private interface PartitionTrackerSetup<C> {
		C accept(TestingTaskExecutorPartitionTracker partitionTracker) throws Exception;
	}

	@FunctionalInterface
	private interface TestAction<C> {
		void accept(JobID jobId, ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor, TaskExecutor taskExecutor, TaskExecutorGateway taskExecutorGateway, C partitionTrackerSetupResult) throws Exception;
	}
}
