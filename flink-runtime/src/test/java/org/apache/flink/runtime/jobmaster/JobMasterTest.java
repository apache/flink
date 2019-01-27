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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.StandaloneSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSlotPoolFactory;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.messages.job.JobPendingSlotRequestDetail;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link JobMaster}.
 */
public class JobMasterTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final Time testingTimeout = Time.seconds(10L);

	private static final long fastHeartbeatInterval = 1L;
	private static final long fastHeartbeatTimeout = 5L;

	private static final long heartbeatInterval = 1000L;
	private static final long heartbeatTimeout = 5000L;

	private static final JobGraph jobGraph = new JobGraph();

	private static TestingRpcService rpcService;

	private static HeartbeatServices fastHeartbeatServices;

	private static HeartbeatServices heartbeatServices;

	private BlobServer blobServer;

	private Configuration configuration;

	private ResourceID jmResourceId;

	private JobMasterId jobMasterId;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService rmLeaderRetrievalService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();

		fastHeartbeatServices = new TestingHeartbeatServices(fastHeartbeatInterval, fastHeartbeatTimeout, rpcService.getScheduledExecutor());
		heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, rpcService.getScheduledExecutor());
	}

	@Before
	public void setup() throws IOException {
		configuration = new Configuration();
		haServices = new TestingHighAvailabilityServices();
		jobMasterId = JobMasterId.generate();
		jmResourceId = ResourceID.generate();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		rmLeaderRetrievalService = new SettableLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		haServices.setSubmittedJobGraphStore(new StandaloneSubmittedJobGraphStore());

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
		blobServer = new BlobServer(configuration, new VoidBlobStore());

		blobServer.start();
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}

		if (blobServer != null) {
			blobServer.close();
		}

		rpcService.clearGateways();
	}

	@AfterClass
	public static void teardownClass() {
		if (rpcService != null) {
			rpcService.stopService();
			rpcService = null;
		}
	}

	@Test
	public void testHeartbeatTimeoutWithTaskManager() throws Exception {
		final CompletableFuture<ResourceID> heartbeatResourceIdFuture = new CompletableFuture<>();
		final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setHeartbeatJobManagerConsumer(heartbeatResourceIdFuture::complete)
			.setDisconnectJobManagerConsumer((jobId, throwable) -> disconnectedJobManagerFuture.complete(jobId))
			.createTestingTaskExecutorGateway();

		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			// register task manager will trigger monitor heartbeat target, schedule heartbeat request at interval time
			CompletableFuture<RegistrationResponse> registrationResponse = jobMasterGateway.registerTaskManager(
				taskExecutorGateway.getAddress(),
				taskManagerLocation,
				testingTimeout);

			// wait for the completion of the registration
			registrationResponse.get();

			final ResourceID heartbeatResourceId = heartbeatResourceIdFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(heartbeatResourceId, Matchers.equalTo(jmResourceId));

			final JobID disconnectedJobManager = disconnectedJobManagerFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(disconnectedJobManager, Matchers.equalTo(jobGraph.getJobID()));
		} finally {
			jobManagerSharedServices.shutdown();
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testHeartbeatTimeoutWithResourceManager() throws Exception {
		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway(
			resourceManagerId,
			rmResourceId,
			fastHeartbeatInterval,
			resourceManagerAddress,
			"localhost");

		final CompletableFuture<Tuple3<JobMasterId, ResourceID, JobID>> jobManagerRegistrationFuture = new CompletableFuture<>();
		final CompletableFuture<JobID> disconnectedJobManagerFuture = new CompletableFuture<>();
		final CountDownLatch registrationAttempts = new CountDownLatch(2);

		resourceManagerGateway.setRegisterJobManagerConsumer(tuple -> {
			jobManagerRegistrationFuture.complete(
				Tuple3.of(
					tuple.f0,
					tuple.f1,
					tuple.f3));
			registrationAttempts.countDown();
		});

		resourceManagerGateway.setDisconnectJobManagerConsumer(tuple -> disconnectedJobManagerFuture.complete(tuple.f0));

		rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
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

			// the JobMaster should try to reconnect to the RM
			registrationAttempts.await();
		} finally {
			jobManagerSharedServices.shutdown();
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that a JobMaster will restore the given JobGraph from its savepoint upon
	 * initial submission.
	 */
	@Test
	public void testRestoringFromSavepoint() throws Exception {

		// create savepoint data
		final long savepointId = 42L;
		final File savepointFile = createSavepoint(savepointId);

		// set savepoint settings
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath(
			savepointFile.getAbsolutePath(),
			true);
		final JobGraph jobGraph = createJobGraphWithCheckpointing(savepointRestoreSettings);

		final StandaloneCompletedCheckpointStore completedCheckpointStore = new StandaloneCompletedCheckpointStore(1);
		final TestingCheckpointRecoveryFactory testingCheckpointRecoveryFactory = new TestingCheckpointRecoveryFactory(completedCheckpointStore, new StandaloneCheckpointIDCounter());
		haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		try {
			// starting the JobMaster should have read the savepoint
			final CompletedCheckpoint savepointCheckpoint = completedCheckpointStore.getLatestCheckpoint();

			assertThat(savepointCheckpoint, Matchers.notNullValue());

			assertThat(savepointCheckpoint.getCheckpointID(), is(savepointId));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that an existing checkpoint will have precedence over an savepoint.
	 */
	@Test
	public void testCheckpointPrecedesSavepointRecovery() throws Exception {

		// create savepoint data
		final long savepointId = 42L;
		final File savepointFile = createSavepoint(savepointId);

		// set savepoint settings
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath("" +
				savepointFile.getAbsolutePath(),
			true);
		final JobGraph jobGraph = createJobGraphWithCheckpointing(savepointRestoreSettings);

		final long checkpointId = 1L;

		final CompletedCheckpoint completedCheckpoint = new CompletedCheckpoint(
			jobGraph.getJobID(),
			checkpointId,
			1L,
			1L,
			Collections.emptyMap(),
			null,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new DummyCheckpointStorageLocation());

		final StandaloneCompletedCheckpointStore completedCheckpointStore = new StandaloneCompletedCheckpointStore(1);
		completedCheckpointStore.addCheckpoint(completedCheckpoint);
		final TestingCheckpointRecoveryFactory testingCheckpointRecoveryFactory = new TestingCheckpointRecoveryFactory(completedCheckpointStore, new StandaloneCheckpointIDCounter());
		haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		try {
			// starting the JobMaster should have read the savepoint
			final CompletedCheckpoint savepointCheckpoint = completedCheckpointStore.getLatestCheckpoint();

			assertThat(savepointCheckpoint, Matchers.notNullValue());

			assertThat(savepointCheckpoint.getCheckpointID(), is(checkpointId));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the JobMaster retries the scheduling of a job
	 * in case of a missing slot offering from a registered TaskExecutor.
	 */
	@Test
	public void testSlotRequestTimeoutWhenNoSlotOffering() throws Exception {
		final JobGraph restartingJobGraph = createSingleVertexJobWithRestartStrategy();

		final long slotRequestTimeout = 10L;
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, slotRequestTimeout);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			restartingJobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		try {
			final long start = System.nanoTime();
			jobMaster.start(JobMasterId.generate(), testingTimeout).get();

			final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			final ArrayBlockingQueue<SlotRequest> blockingQueue = new ArrayBlockingQueue<>(2);
			resourceManagerGateway.setRequestSlotConsumer(blockingQueue::offer);

			rpcService.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
			rmLeaderRetrievalService.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			// wait for the first slot request
			blockingQueue.take();

			final CompletableFuture<TaskDeploymentDescriptor> submittedTaskFuture = new CompletableFuture<>();
			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setSubmitTaskConsumer((tdd, ignored) -> {
					submittedTaskFuture.complete(tdd);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.createTestingTaskExecutorGateway();

			rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

			jobMasterGateway.registerTaskManager(taskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			// wait for the slot request timeout
			final SlotRequest slotRequest = blockingQueue.take();
			final long end = System.nanoTime();

			// we rely on the slot request timeout to fail a stuck scheduling operation
			assertThat((end - start) / 1_000_000L, Matchers.greaterThanOrEqualTo(slotRequestTimeout));

			assertThat(submittedTaskFuture.isDone(), is(false));

			final SlotOffer slotOffer = new SlotOffer(slotRequest.getAllocationId(), 0, ResourceProfile.UNKNOWN);

			final CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout);

			final Collection<SlotOffer> acceptedSlots = acceptedSlotsFuture.get();

			assertThat(acceptedSlots, hasSize(1));
			final SlotOffer acceptedSlot = acceptedSlots.iterator().next();

			assertThat(acceptedSlot.getAllocationId(), equalTo(slotRequest.getAllocationId()));

			// wait for the deployed task
			final TaskDeploymentDescriptor taskDeploymentDescriptor = submittedTaskFuture.get();

			assertThat(taskDeploymentDescriptor.getAllocationId(), equalTo(slotRequest.getAllocationId()));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private JobGraph createSingleVertexJobWithRestartStrategy() throws IOException {
		final JobVertex jobVertex = new JobVertex("Test vertex");
		jobVertex.setInvokableClass(NoOpInvokable.class);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

		final JobGraph jobGraph = new JobGraph(jobVertex);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	/**
	 * Tests that we can close an unestablished ResourceManager connection.
	 */
	@Test
	public void testCloseUnestablishedResourceManagerConnection() throws Exception {
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		try {
			jobMaster.start(JobMasterId.generate(), testingTimeout).get();
			final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
			final String firstResourceManagerAddress = "address1";
			final String secondResourceManagerAddress = "address2";

			final TestingResourceManagerGateway firstResourceManagerGateway = new TestingResourceManagerGateway();
			final TestingResourceManagerGateway secondResourceManagerGateway = new TestingResourceManagerGateway();

			rpcService.registerGateway(firstResourceManagerAddress, firstResourceManagerGateway);
			rpcService.registerGateway(secondResourceManagerAddress, secondResourceManagerGateway);

			final OneShotLatch firstJobManagerRegistration = new OneShotLatch();
			final OneShotLatch secondJobManagerRegistration = new OneShotLatch();

			firstResourceManagerGateway.setRegisterJobManagerConsumer(
				jobMasterIdResourceIDStringJobIDTuple4 -> firstJobManagerRegistration.trigger());

			secondResourceManagerGateway.setRegisterJobManagerConsumer(
				jobMasterIdResourceIDStringJobIDTuple4 -> secondJobManagerRegistration.trigger());

			rmLeaderRetrievalService.notifyListener(firstResourceManagerAddress, resourceManagerId.toUUID());

			// wait until we have seen the first registration attempt
			firstJobManagerRegistration.await();

			// this should stop the connection attempts towards the first RM
			rmLeaderRetrievalService.notifyListener(secondResourceManagerAddress, resourceManagerId.toUUID());

			// check that we start registering at the second RM
			secondJobManagerRegistration.await();
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that we continue reconnecting to the latest known RM after a disconnection
	 * message.
	 */
	@Test
	public void testReconnectionAfterDisconnect() throws Exception {
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
			final BlockingQueue<JobMasterId> registrationsQueue = new ArrayBlockingQueue<>(1);

			testingResourceManagerGateway.setRegisterJobManagerConsumer(
				jobMasterIdResourceIDStringJobIDTuple4 -> registrationsQueue.offer(jobMasterIdResourceIDStringJobIDTuple4.f0));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final ResourceManagerId resourceManagerId = testingResourceManagerGateway.getFencingToken();
			rmLeaderRetrievalService.notifyListener(
				testingResourceManagerGateway.getAddress(),
				resourceManagerId.toUUID());

			// wait for first registration attempt
			final JobMasterId firstRegistrationAttempt = registrationsQueue.take();

			assertThat(firstRegistrationAttempt, equalTo(jobMasterId));

			assertThat(registrationsQueue.isEmpty(), is(true));
			jobMasterGateway.disconnectResourceManager(resourceManagerId, new FlinkException("Test exception"));

			// wait for the second registration attempt after the disconnect call
			assertThat(registrationsQueue.take(), equalTo(jobMasterId));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the a JM connects to the leading RM after regaining leadership.
	 */
	@Test
	public void testResourceManagerConnectionAfterRegainingLeadership() throws Exception {
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build());

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final BlockingQueue<JobMasterId> registrationQueue = new ArrayBlockingQueue<>(1);
			testingResourceManagerGateway.setRegisterJobManagerConsumer(
				jobMasterIdResourceIDStringJobIDTuple4 -> registrationQueue.offer(jobMasterIdResourceIDStringJobIDTuple4.f0));

			final String resourceManagerAddress = testingResourceManagerGateway.getAddress();
			rpcService.registerGateway(resourceManagerAddress, testingResourceManagerGateway);

			rmLeaderRetrievalService.notifyListener(resourceManagerAddress, testingResourceManagerGateway.getFencingToken().toUUID());

			final JobMasterId firstRegistrationAttempt = registrationQueue.take();

			assertThat(firstRegistrationAttempt, equalTo(jobMasterId));

			jobMaster.suspend(new FlinkException("Test exception."), testingTimeout).get();

			final JobMasterId jobMasterId2 = JobMasterId.generate();

			jobMaster.reconcile();
			jobMaster.start(jobMasterId2, testingTimeout).get();

			final JobMasterId secondRegistrationAttempt = registrationQueue.take();

			assertThat(secondRegistrationAttempt, equalTo(jobMasterId2));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests the {@link JobMaster#requestPartitionState(IntermediateDataSetID, ResultPartitionID)}
	 * call for a finished result partition.
	 */
	@Test
	public void testRequestPartitionState() throws Exception {
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph();
		final JobMaster jobMaster = createJobMaster(
			configuration,
			producerConsumerJobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
					tddFuture.complete(taskDeploymentDescriptor);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			assertThat(tdd.getProducedPartitions(), hasSize(1));
			final ResultPartitionDeploymentDescriptor partition = tdd.getProducedPartitions().iterator().next();

			final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
			final ExecutionAttemptID copiedExecutionAttemptId = new ExecutionAttemptID(executionAttemptId.getLowerPart(), executionAttemptId.getUpperPart());

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(producerConsumerJobGraph.getJobID(), executionAttemptId, ExecutionState.FINISHED)).get();

			// request the state of the result partition of the producer
			final CompletableFuture<ExecutionState> partitionStateFuture = jobMasterGateway.requestPartitionState(partition.getResultId(), new ResultPartitionID(partition.getPartitionId(), copiedExecutionAttemptId));

			assertThat(partitionStateFuture.get(), equalTo(ExecutionState.FINISHED));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testRequestNextInputSplit() throws Exception {
		// build one node JobGraph
		OperatorID sourceOperatorID = new OperatorID();
		InputSplitSource<TestingInputSplit> inputSplitSource = new TestingInputSplitSource();

		JobVertex source = new JobVertex("vertex1");
		source.setParallelism(1);
		source.setInputSplitSource(sourceOperatorID, inputSplitSource);
		source.setInvokableClass(AbstractInvokable.class);

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);

		configuration.setLong(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
		configuration.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");

		final JobManagerSharedServices jobManagerSharedServices =
			new TestingJobManagerSharedServicesBuilder()
				.setRestartStrategyFactory(RestartStrategyFactory.createRestartStrategyFactory(configuration))
				.build();

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			ExecutionGraph eg = jobMaster.getExecutionGraph();
			ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

			verifyGetNextInputSplit(0, jobMasterGateway, source, ev, sourceOperatorID);
			verifyGetNextInputSplit(1, jobMasterGateway, source, ev, sourceOperatorID);

			try {
				jobMasterGateway.requestNextInputSplit(
					source.getID(),
					new OperatorID(),
					eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt().getAttemptId())
					.get(testingTimeout.getSize(), testingTimeout.getUnit());
				fail("Should throw no InputSplitAssigner exception");
			} catch (Throwable t) {
				assertTrue(ExceptionUtils.findThrowableWithMessage(t, "No InputSplitAssigner").isPresent());
			}

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev, ExecutionState.SCHEDULED, testingTimeout.toMilliseconds());

			eg.failGlobal(new Exception("Testing exception"));

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev, ExecutionState.SCHEDULED, testingTimeout.toMilliseconds());

			verifyGetNextInputSplit(0, jobMasterGateway, source, ev, sourceOperatorID);
			verifyGetNextInputSplit(1, jobMasterGateway, source, ev, sourceOperatorID);

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testInitInputSplitFailLeadToJobFail() throws Exception {
		// build one node JobGraph
		OperatorID sourceOperatorID = new OperatorID();

		InputSplitSource<InputSplit> inputSplitSource = new InputSplitSource<InputSplit>() {
				@Override
				public InputSplit[] createInputSplits(int minNumSplits) throws Exception {
					throw new Exception("Testing exception to simulate source fail.");
				}

				@Override
				public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
					return null;
				}
		};

		JobVertex source = new JobVertex("vertex1");
		source.setParallelism(1);
		source.setInputSplitSource(sourceOperatorID, inputSplitSource);
		source.setInvokableClass(AbstractInvokable.class);

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);

		CompletableFuture<Void> jobFailedFuture = new CompletableFuture<>();

		final JobMaster jobMaster = new JobMaster(
				rpcService,
				JobMasterConfiguration.fromConfiguration(configuration),
				jmResourceId,
				jobGraph,
				haServices,
				DefaultSlotPoolFactory.fromConfiguration(configuration, rpcService),
				new TestingJobManagerSharedServicesBuilder().build(),
				heartbeatServices,
				blobServer,
				UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
				new OnCompletionActions() {
					@Override
					public void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph) {

					}

					@Override
					public void jobFinishedByOther() {
					}

					@Override
					public void jobMasterFailed(Throwable cause) {
						jobFailedFuture.complete(null);
					}
				},
				testingFatalErrorHandler,
				JobMasterTest.class.getClassLoader(),
				haServices.getSubmittedJobGraphStore());

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			jobFailedFuture.get(2, TimeUnit.SECONDS);

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testRequestEmptyNextInputSplit() throws Exception {

		JobVertex source = new JobVertex("vertex1");
		source.setParallelism(1);
		source.setInvokableClass(AbstractInvokable.class);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setExecutionConfig(executionConfig);

		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			ExecutionGraph eg = jobMaster.getExecutionGraph();

			try {
				jobMasterGateway.requestNextInputSplit(
					source.getID(),
					OperatorID.fromJobVertexID(source.getID()),
					eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt().getAttemptId())
					.get(testingTimeout.getSize(), testingTimeout.getUnit());
				fail("Should throw no InputSplitAssigner exception");
			} catch (Throwable t) {
				assertTrue(ExceptionUtils.findThrowableWithMessage(t, "No InputSplitAssigner").isPresent());
			}

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testRequestNextInputSplitForRegionFailover() throws Exception {

		OperatorID sourceOperatorID = new OperatorID();
		InputSplitSource<TestingInputSplit> inputSplitSource = new TestingInputSplitSource();

		JobVertex source = new JobVertex("vertex1");
		source.setParallelism(2);
		source.setInputSplitSource(sourceOperatorID, inputSplitSource);
		source.setInvokableClass(AbstractInvokable.class);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setExecutionConfig(executionConfig);

		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
		final JobMaster jobMaster = createJobMaster(
			configuration,
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			ExecutionGraph eg = jobMaster.getExecutionGraph();

			Iterator<ExecutionVertex> evIterator = eg.getAllExecutionVertices().iterator();
			ExecutionVertex ev1 = evIterator.next();
			ExecutionVertex ev2 = evIterator.next();

			verifyGetNextInputSplit(0, jobMasterGateway, source, ev1, sourceOperatorID);
			verifyGetNextInputSplit(1, jobMasterGateway, source, ev2, sourceOperatorID);
			verifyGetNextInputSplit(2, jobMasterGateway, source, ev2, sourceOperatorID);

			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					jobGraph.getJobID(),
					ev2.getCurrentExecutionAttempt().getAttemptId(),
					ExecutionState.FAILED,
					new Exception("Testing exception"))).get();
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev2, ExecutionState.SCHEDULED, testingTimeout.toMilliseconds());

			verifyGetNextInputSplit(3, jobMasterGateway, source, ev1, sourceOperatorID);
			verifyGetNextInputSplit(1, jobMasterGateway, source, ev2, sourceOperatorID);
			verifyGetNextInputSplit(2, jobMasterGateway, source, ev2, sourceOperatorID);
			verifyGetNextInputSplit(4, jobMasterGateway, source, ev2, sourceOperatorID);

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testSettingGraphManagerPluginClass() throws Exception {
		final JobVertex jobVertex = new JobVertex("Test vertex");
		jobVertex.setInvokableClass(NoOpInvokable.class);

		final JobGraph jobGraph = new JobGraph(jobVertex);
		jobGraph.getSchedulingConfiguration().setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, "TestGraphManagerPluginClass");

		try {
			createJobMaster(
				configuration,
				jobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build());
			fail("Should throw a exception");
		} catch (Throwable t) {
			assertTrue(t instanceof IllegalArgumentException);
			assertTrue(t.getMessage().contains("TestGraphManagerPluginClass"));
		}
	}

	@Test
	public void testSlotRequestWillBeCancelledWhenJobFailover() throws Exception {
		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway(
				resourceManagerId,
				rmResourceId,
				fastHeartbeatInterval,
				resourceManagerAddress,
				"localhost");

		// there should be two cancellation, as when offering slot to an pending request in slot pool
		// without same allocation id, it will trigger a cancellation.
		final List<CompletableFuture<AllocationID>> cancelAllocationFutures = new ArrayList<>(2);
		cancelAllocationFutures.add(new CompletableFuture<>());
		cancelAllocationFutures.add(new CompletableFuture<>());
		resourceManagerGateway.setCancelSlotConsumer(
				(allocationId) -> {
					for (CompletableFuture<AllocationID> cancelAllocationFuture : cancelAllocationFutures) {
						if (!cancelAllocationFuture.isDone()) {
							cancelAllocationFuture.complete(allocationId);
							break;
						}
					}
				});
		final List<AllocationID> slotAllocationIds = new ArrayList<>();
		resourceManagerGateway.setRequestSlotConsumer((slotRequest) -> slotAllocationIds.add(slotRequest.getAllocationId()));

		rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final String taskExecutorAddress = "tm";
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorAddress, taskExecutorGateway);

		JobVertex source = new JobVertex("vertex");
		source.setParallelism(2);
		source.setInvokableClass(AbstractInvokable.class);
		source.setSlotSharingGroup(new SlotSharingGroup());

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		configuration.setBoolean(JobManagerOptions.SLOT_ENABLE_SHARED_SLOT, false);

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().setRestartStrategyFactory(
						new NoRestartStrategy.NoRestartStrategyFactory()).build(),
				new TestingHeartbeatServices(10000, 60000, rpcService.getScheduledExecutor()));

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			rmLeaderRetrievalService.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(taskExecutorAddress, taskManagerLocation, testingTimeout).get();

			SlotOffer slotOffer = new SlotOffer(new AllocationID(), 0, new ResourceProfile(1, 100));
			jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			ExecutionGraph eg = jobMaster.getExecutionGraph();

			jobMaster.disconnectTaskManager(taskManagerLocation.getResourceID(), new Exception("Test Exception")).get();

			ExecutionGraphTestUtils.waitUntilJobStatus(eg, JobStatus.FAILED, 2000L);

			for (int i = 0; i < slotAllocationIds.size(); i++) {
				cancelAllocationFutures.get(i).get(2, TimeUnit.SECONDS);
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Test that pending request should be cleared and won't be fulfill by released slots. For BLINK-17900880.
	 *
	 * @throws Exception
	 */
	@Test
	public void testPendingSlotRequestsWillBeClearedWhenJobFailover() throws Exception {
		int parallelism = 100;
		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway(
				resourceManagerId,
				rmResourceId,
				fastHeartbeatInterval,
				resourceManagerAddress,
				"localhost");

		// Half of the allocations will be cancelled.
		final List<AllocationID> cancelAllocationIds = new ArrayList<>(parallelism / 2);
		resourceManagerGateway.setCancelSlotConsumer((allocationId) -> cancelAllocationIds.add(allocationId));

		final List<AllocationID> slotAllocationIds = new ArrayList<>(parallelism);
		resourceManagerGateway.setRequestSlotConsumer((slotRequest) -> slotAllocationIds.add(slotRequest.getAllocationId()));

		CompletableFuture<Acknowledge> requestSlotFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotFuture(requestSlotFuture);

		rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final String taskExecutorAddress = "tm";
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorAddress, taskExecutorGateway);

		JobVertex source = new JobVertex("vertex");
		source.setParallelism(parallelism);
		source.setInvokableClass(AbstractInvokable.class);
		source.setSlotSharingGroup(new SlotSharingGroup());

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		configuration.setBoolean(JobManagerOptions.SLOT_ENABLE_SHARED_SLOT, false);

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().setRestartStrategyFactory(
						new NoRestartStrategy.NoRestartStrategyFactory()).build(),
				new TestingHeartbeatServices(10000, 60000, rpcService.getScheduledExecutor()));

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			rmLeaderRetrievalService.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(taskExecutorAddress, taskManagerLocation, testingTimeout).get();

			long startTime = System.currentTimeMillis();
			while (true) {
				if (slotAllocationIds.size() == parallelism) {
					break;
				} else if (System.currentTimeMillis() - startTime > 2000) {
					fail("RM does not receive all allocation");
				}
				Thread.sleep(100);
			}

			List<SlotOffer> slotOffers = new ArrayList<>(parallelism / 2);
			// Fulfill the last half allocations.
			for (int i = parallelism / 2; i < parallelism; i++) {
				slotOffers.add(new SlotOffer(slotAllocationIds.get(i), i, new ResourceProfile(1, 100)));
			}
			jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), slotOffers, testingTimeout).get();

			ExecutionGraph eg = jobMaster.getExecutionGraph();

			requestSlotFuture.completeExceptionally(new TimeoutException("Testing timeout"));

			ExecutionGraphTestUtils.waitUntilJobStatus(eg, JobStatus.FAILED, 2000L);

			startTime = System.currentTimeMillis();
			while (true) {
				if (cancelAllocationIds.size() == parallelism / 2 && jobMaster.getSlotPool().getAvailableSlotsSize() == parallelism / 2) {
					break;
				} else if (System.currentTimeMillis() - startTime > 2000) {
					fail("RM does not receive all cancellation");
				}
				Thread.sleep(100);
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testExecutionWillUseSameLocationAfterFailover() throws Exception {
		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

		final TestingResourceManagerGateway resourceManagerGateway = spy(new TestingResourceManagerGateway(
				resourceManagerId,
				rmResourceId,
				fastHeartbeatInterval,
				resourceManagerAddress,
				"localhost"));

		rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final String taskExecutorAddress1 = "tm1";
		final TestingTaskExecutorGateway taskExecutorGateway1 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorAddress1, taskExecutorGateway1);
		final String taskExecutorAddress2 = "tm2";
		final TestingTaskExecutorGateway taskExecutorGateway2 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorAddress2, taskExecutorGateway2);
		final String taskExecutorAddress3 = "tm3";
		final TestingTaskExecutorGateway taskExecutorGateway3 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorAddress3, taskExecutorGateway3);
		final String taskExecutorAddress4 = "tm4";
		final TestingTaskExecutorGateway taskExecutorGateway4 = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorAddress4, taskExecutorGateway4);

		JobVertex source = new JobVertex("vertex");
		source.setParallelism(3);
		source.setInvokableClass(AbstractInvokable.class);

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().setRestartStrategyFactory(
						new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(1, 0)).build(),
				new TestingHeartbeatServices(10000, 60000, rpcService.getScheduledExecutor()));

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			TaskManagerLocation taskManagerLocation1 = mock(TaskManagerLocation.class);
			when(taskManagerLocation1.getFQDNHostname()).thenReturn("tm1");
			when(taskManagerLocation1.getResourceID()).thenReturn(ResourceID.generate());
			jobMasterGateway.registerTaskManager(taskExecutorAddress1, taskManagerLocation1, testingTimeout).get();
			TaskManagerLocation taskManagerLocation2 = mock(TaskManagerLocation.class);
			when(taskManagerLocation2.getFQDNHostname()).thenReturn("tm2");
			when(taskManagerLocation2.getResourceID()).thenReturn(ResourceID.generate());
			jobMasterGateway.registerTaskManager(taskExecutorAddress2, taskManagerLocation2, testingTimeout).get();
			TaskManagerLocation taskManagerLocation3 = mock(TaskManagerLocation.class);
			when(taskManagerLocation3.getFQDNHostname()).thenReturn("tm3");
			when(taskManagerLocation3.getResourceID()).thenReturn(ResourceID.generate());
			jobMasterGateway.registerTaskManager(taskExecutorAddress3, taskManagerLocation3, testingTimeout).get();
			TaskManagerLocation taskManagerLocation4 = mock(TaskManagerLocation.class);
			when(taskManagerLocation4.getFQDNHostname()).thenReturn("tm4");
			when(taskManagerLocation4.getResourceID()).thenReturn(ResourceID.generate());
			jobMasterGateway.registerTaskManager(taskExecutorAddress4, taskManagerLocation4, testingTimeout).get();

			SlotOffer slotOffer1 = new SlotOffer(new AllocationID(), 0, new ResourceProfile(1, 100));
			jobMasterGateway.offerSlots(taskManagerLocation1.getResourceID(), Collections.singleton(slotOffer1), testingTimeout).get();

			SlotOffer slotOffer2 = new SlotOffer(new AllocationID(), 0, new ResourceProfile(1, 100));
			jobMasterGateway.offerSlots(taskManagerLocation2.getResourceID(), Collections.singleton(slotOffer2), testingTimeout).get();

			SlotOffer slotOffer3 = new SlotOffer(new AllocationID(), 0, new ResourceProfile(1, 100));
			jobMasterGateway.offerSlots(taskManagerLocation3.getResourceID(), Collections.singleton(slotOffer3), testingTimeout).get();

			SlotOffer slotOffer4 = new SlotOffer(new AllocationID(), 0, new ResourceProfile(1, 100));
			jobMasterGateway.offerSlots(taskManagerLocation4.getResourceID(), Collections.singleton(slotOffer4), testingTimeout).get();

			ExecutionGraph eg = jobMaster.getExecutionGraph();

			Iterator<ExecutionVertex> evIterator = eg.getAllExecutionVertices().iterator();
			ExecutionVertex ev1 = evIterator.next();
			ExecutionVertex ev2 = evIterator.next();
			ExecutionVertex ev3 = evIterator.next();

			// ensure all executions are scheduled.
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev1, ExecutionState.DEPLOYING, 2000L);
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev2, ExecutionState.DEPLOYING, 2000L);
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev3, ExecutionState.DEPLOYING, 2000L);

			TaskManagerLocation locationForEv1 = ev1.getCurrentAssignedResourceLocation();
			TaskManagerLocation locationForEv2 = ev2.getCurrentAssignedResourceLocation();
			TaskManagerLocation locationForEv3 = ev3.getCurrentAssignedResourceLocation();

			jobMaster.disconnectTaskManager(locationForEv1.getResourceID(), new Exception("Test Exception"));

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev2, ExecutionState.CANCELING, 2000L);
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev3, ExecutionState.CANCELING, 2000L);

			jobMaster.updateTaskExecutionState(new TaskExecutionState(
					jobGraph.getJobID(),
					ev2.getCurrentExecutionAttempt().getAttemptId(),
					ExecutionState.CANCELED,
					new Exception("Job master cancel")));

			jobMaster.updateTaskExecutionState(new TaskExecutionState(
					jobGraph.getJobID(),
					ev3.getCurrentExecutionAttempt().getAttemptId(),
					ExecutionState.CANCELED,
					new Exception("Job master cancel")));

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev1, ExecutionState.DEPLOYING, 2000L);
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev2, ExecutionState.DEPLOYING, 2000L);
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev3, ExecutionState.DEPLOYING, 2000L);

			assertEquals(locationForEv2, ev2.getCurrentAssignedResourceLocation());
			assertEquals(locationForEv3, ev3.getCurrentAssignedResourceLocation());

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * This test verify that batch allocation fail will not cause a global failover if configured region failover.
	 */
	@Test
	public void testRegionFailoverWithoutGlobalFailIfBatchRequestFail() throws Exception {
		int parallelism = 3;
		final String resourceManagerAddress = "rm";
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceID rmResourceId = new ResourceID(resourceManagerAddress);

		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway(
				resourceManagerId,
				rmResourceId,
				fastHeartbeatInterval,
				resourceManagerAddress,
				"localhost");

		CompletableFuture<Acknowledge> requestSlotFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotFuture(requestSlotFuture);

		final List<AllocationID> slotAllocationIds = new ArrayList<>(parallelism);
		resourceManagerGateway.setRequestSlotConsumer((slotRequest) -> slotAllocationIds.add(slotRequest.getAllocationId()));

		rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

		final String taskExecutorAddress = "tm";
		final TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorAddress, taskExecutorGateway);

		JobVertex source = new JobVertex("vertex");
		source.setParallelism(parallelism);
		source.setInvokableClass(AbstractInvokable.class);
		source.setSlotSharingGroup(new SlotSharingGroup());

		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().setRestartStrategyFactory(
						new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(1, 0)).build(),
				new TestingHeartbeatServices(10000, 60000, rpcService.getScheduledExecutor()));

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			rmLeaderRetrievalService.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(taskExecutorAddress, taskManagerLocation, testingTimeout).get();

			long startTime = System.currentTimeMillis();
			while (true) {
				if (slotAllocationIds.size() == parallelism) {
					break;
				} else if (System.currentTimeMillis() - startTime > 2000) {
					fail("RM does not receive all allocation");
				}
				Thread.sleep(100);
			}

			ExecutionGraph eg = jobMaster.getExecutionGraph();

			requestSlotFuture.completeExceptionally(new TimeoutException("Testing timeout"));

			assertEquals(JobStatus.RUNNING, eg.getState());

			startTime = System.currentTimeMillis();
			while (true) {
				if (slotAllocationIds.size() == parallelism * 2) {
					break;
				} else if (System.currentTimeMillis() - startTime > 2000) {
					fail("RM does not receive all allocation");
				}
				Thread.sleep(100);
			}

			assertEquals(0, eg.getNumberOfFullRestarts());

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testRequestPendingSlotRequestDetails() throws Exception {
		final JobGraph jobGraph = createJobGraphWithSlotSharingGroup();
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final int expectedSlotRequestNum = 6;

		// create the resource manager
		final String resourceManagerAddress = "rm";
		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway(
				ResourceManagerId.generate(),
				new ResourceID(resourceManagerAddress),
				fastHeartbeatInterval,
				resourceManagerAddress,
				"localhost");

		CompletableFuture<Acknowledge> requestSlotFuture = new CompletableFuture<>();
		final ArrayBlockingQueue<SlotRequest> blockingQueue = new ArrayBlockingQueue<>(expectedSlotRequestNum);
		resourceManagerGateway.setRequestSlotConsumer(blockingQueue::offer);
		resourceManagerGateway.setRequestSlotFuture(requestSlotFuture);
		rpcService.registerGateway(resourceManagerAddress, resourceManagerGateway);

		// create a task manager
		final String taskExecutorAddress = "tm";
		rpcService.registerGateway(taskExecutorAddress, new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

		// create the job master
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().setRestartStrategyFactory(
						new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(1, 0)).build(),
				new TestingHeartbeatServices(10000, 60000, rpcService.getScheduledExecutor()));

		// start the job master
		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			ScheduledExecutorService retryExecutorService = Executors.newSingleThreadScheduledExecutor(
					new ExecutorThreadFactory("Flink-Test-Retry"));

			// wait for the start to complete and verify the results
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			Collection<JobPendingSlotRequestDetail> result = retrySuccesfulWithDelay(
					() -> jobMasterGateway.requestPendingSlotRequestDetails(testingTimeout),
					(collection) -> collection.size() == expectedSlotRequestNum,
					retryExecutorService).get(testingTimeout.toMilliseconds() + 4, TimeUnit.MILLISECONDS);

			verifyPendingSlotRequestDetails(result);

			// grant leader to the resource manager
			rmLeaderRetrievalService.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(taskExecutorAddress, taskManagerLocation, testingTimeout).get();

			// wait for all slot requests to arrive at the resource manager and verify the results
			retrySuccesfulWithDelay(
					() -> CompletableFuture.completedFuture(blockingQueue.size()),
					(size) -> size == expectedSlotRequestNum,
					retryExecutorService).get(testingTimeout.toMilliseconds() + 4, TimeUnit.MILLISECONDS);

			result = retrySuccesfulWithDelay(
					() -> jobMasterGateway.requestPendingSlotRequestDetails(testingTimeout),
					(collection) -> collection.size() == expectedSlotRequestNum,
					retryExecutorService).get(testingTimeout.toMilliseconds() + 4, TimeUnit.MILLISECONDS);

			verifyPendingSlotRequestDetails(result);

			// reply to the slot poll that resources allocated is completed and verify the results
			requestSlotFuture.complete(Acknowledge.get());

			List<SlotOffer> slotOffers = new ArrayList<>();
			Iterator<SlotRequest> iterator = blockingQueue.iterator();
			while (iterator.hasNext()) {
				slotOffers.add(new SlotOffer(iterator.next().getAllocationId(), 0, ResourceProfile.UNKNOWN));
			}
			jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), slotOffers, testingTimeout);

			retrySuccesfulWithDelay(
					() -> jobMasterGateway.requestPendingSlotRequestDetails(testingTimeout),
					(collection) -> collection.size() == 0,
					retryExecutorService).get(testingTimeout.toMilliseconds() + 4, TimeUnit.MILLISECONDS);

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private static <T> CompletableFuture<T> retrySuccesfulWithDelay(
			Supplier<CompletableFuture<T>> operation,
			Predicate<T> acceptancePredicate,
			ScheduledExecutorService retryExecutorService) {

		return FutureUtils.retrySuccesfulWithDelay(
				operation,
				Time.milliseconds(1),
				Deadline.now().plus(Duration.ofMillis(testingTimeout.toMilliseconds())),
				acceptancePredicate,
				new ScheduledExecutorServiceAdapter(retryExecutorService));
	}

	private void verifyPendingSlotRequestDetails(Collection<JobPendingSlotRequestDetail> result) {
		Map<String, JobPendingSlotRequestDetail> pendingSlotRequestDetailMap = new HashMap<>();
		for (JobPendingSlotRequestDetail detail : result) {
			for (JobPendingSlotRequestDetail.VertexTaskInfo taskInfo : detail.getVertexTaskInfos()) {
				pendingSlotRequestDetailMap.put(taskInfo.getTaskName() + "-" + taskInfo.getSubtaskIndex(), detail);
			}
		}

		{
			JobPendingSlotRequestDetail detail = pendingSlotRequestDetailMap.get("source1-0");
			assertEquals(1, detail.getVertexTaskInfos().size());
			assertTrue(detail.getStartTime() > 0);
			assertNull(detail.getSlotSharingGroupId());
			assertNull(detail.getCoLocationGroupId());
		}

		{
			JobPendingSlotRequestDetail detail = pendingSlotRequestDetailMap.get("slotSharingGroup1Vertex1-0");
			assertEquals(2, detail.getVertexTaskInfos().size());
			assertEquals(detail, pendingSlotRequestDetailMap.get("slotSharingGroup1Vertex2-0"));
			assertNotNull(detail.getSlotSharingGroupId());
			assertNull(detail.getCoLocationGroupId());
		}

		{
			JobPendingSlotRequestDetail detail = pendingSlotRequestDetailMap.get("slotSharingGroup1Vertex2-1");
			assertEquals(1, detail.getVertexTaskInfos().size());
			assertNotNull(detail.getSlotSharingGroupId());
			assertNull(detail.getCoLocationGroupId());
		}

		{
			JobPendingSlotRequestDetail detail = pendingSlotRequestDetailMap.get("coLocationGroupId1Vertex1-0");
			assertEquals(2, detail.getVertexTaskInfos().size());
			assertEquals(detail, pendingSlotRequestDetailMap.get("coLocationGroupId1Vertex2-0"));
			assertNotNull(detail.getSlotSharingGroupId());
			assertNotNull(detail.getCoLocationGroupId());
		}

		{
			JobPendingSlotRequestDetail detail = pendingSlotRequestDetailMap.get("coLocationGroupId1Vertex1-1");
			assertEquals(2, detail.getVertexTaskInfos().size());
			assertEquals(detail, pendingSlotRequestDetailMap.get("coLocationGroupId1Vertex2-1"));
			assertNotNull(detail.getSlotSharingGroupId());
			assertNotNull(detail.getCoLocationGroupId());
		}

		{
			JobPendingSlotRequestDetail detail = pendingSlotRequestDetailMap.get("sink1-0");
			assertEquals(1, detail.getVertexTaskInfos().size());
			assertTrue(detail.getStartTime() > 0);
			assertNotNull(detail.getSlotSharingGroupId());
			assertNull(detail.getCoLocationGroupId());
		}
	}

	private void verifyGetNextInputSplit(int expectedSplitNumber,
			final JobMasterGateway jobMasterGateway,
			final JobVertex jobVertex,
			final ExecutionVertex executionVertex,
			final OperatorID operatorID) throws Exception {

		SerializedInputSplit serializedInputSplit = jobMasterGateway.requestNextInputSplit(
			jobVertex.getID(),
			operatorID,
			executionVertex.getCurrentExecutionAttempt().getAttemptId())
			.get(testingTimeout.getSize(), testingTimeout.getUnit());

		InputSplit inputSplit = InstantiationUtil.deserializeObject(
			serializedInputSplit.getInputSplitData(), Thread.currentThread().getContextClassLoader());

		assertEquals(expectedSplitNumber, inputSplit.getSplitNumber());
	}

	/**
	 * Tests that the timeout in {@link JobMasterGateway#triggerSavepoint(String, boolean, Time)}
	 * is respected.
	 */
	@Test
	public void testTriggerSavepointTimeout() throws Exception {
		final JobMaster jobMaster = new JobMaster(
			rpcService,
			JobMasterConfiguration.fromConfiguration(configuration),
			jmResourceId,
			jobGraph,
			haServices,
			DefaultSlotPoolFactory.fromConfiguration(configuration, rpcService),
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices,
			blobServer,
			UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
			new NoOpOnCompletionActions(),
			testingFatalErrorHandler,
			JobMasterTest.class.getClassLoader(),
			haServices.getSubmittedJobGraphStore()) {

			@Override
			public CompletableFuture<String> triggerSavepoint(
					@Nullable final String targetDirectory,
					final boolean cancelJob,
					final Time timeout) {
				return new CompletableFuture<>();
			}
		};

		try {
			final CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);
			final CompletableFuture<String> savepointFutureLowTimeout = jobMasterGateway.triggerSavepoint("/tmp", false, Time.milliseconds(1));
			final CompletableFuture<String> savepointFutureHighTimeout = jobMasterGateway.triggerSavepoint("/tmp", false, RpcUtils.INF_TIMEOUT);

			try {
				savepointFutureLowTimeout.get(testingTimeout.getSize(), testingTimeout.getUnit());
				fail();
			} catch (final ExecutionException e) {
				final Throwable cause = ExceptionUtils.stripExecutionException(e);
				assertThat(cause, instanceOf(TimeoutException.class));
			}

			assertThat(savepointFutureHighTimeout.isDone(), is(equalTo(false)));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private JobGraph producerConsumerJobGraph() {
		final JobVertex producer = new JobVertex("Producer");
		producer.setInvokableClass(NoOpInvokable.class);
		final JobVertex consumer = new JobVertex("Consumer");
		consumer.setInvokableClass(NoOpInvokable.class);

		consumer.connectNewDataSetAsInput(producer, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(producer, consumer);
		jobGraph.setAllowQueuedScheduling(true);

		return jobGraph;
	}

	private File createSavepoint(long savepointId) throws IOException {
		final File savepointFile = temporaryFolder.newFile();
		final SavepointV2 savepoint = new SavepointV2(savepointId, Collections.emptyList(), Collections.emptyList());

		try (FileOutputStream fileOutputStream = new FileOutputStream(savepointFile)) {
			Checkpoints.storeCheckpointMetadata(savepoint, fileOutputStream);
		}

		return savepointFile;
	}

	@Nonnull
	private JobGraph createJobGraphWithCheckpointing(SavepointRestoreSettings savepointRestoreSettings) {
		final JobGraph jobGraph = new JobGraph();

		// enable checkpointing which is required to resume from a savepoint
		final CheckpointCoordinatorConfiguration checkpoinCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			1000L,
			1000L,
			1000L,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true);
		final JobCheckpointingSettings checkpointingSettings = new JobCheckpointingSettings(
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptyList(),
			checkpoinCoordinatorConfiguration,
			null);
		jobGraph.setSnapshotSettings(checkpointingSettings);
		jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);

		return jobGraph;
	}

	@Nonnull
	private JobGraph createJobGraphWithSlotSharingGroup() {
		// create vertex without SlotSharingGroup
		final JobVertex source1 = new JobVertex("source1");
		source1.setInvokableClass(NoOpInvokable.class);
		source1.setParallelism(1);
		source1.setSlotSharingGroup(null);
		source1.updateCoLocationGroup(null);

		// create vertices with the same SlotSharingGroup
		SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
		final JobVertex slotSharingGroup1Vertex1 = new JobVertex("slotSharingGroup1Vertex1");
		final JobVertex slotSharingGroup1Vertex2 = new JobVertex("slotSharingGroup1Vertex2");
		{
			slotSharingGroup1Vertex1.setInvokableClass(NoOpInvokable.class);
			slotSharingGroup1Vertex1.setParallelism(1);
			slotSharingGroup1Vertex1.setSlotSharingGroup(slotSharingGroup1);

			slotSharingGroup1Vertex2.setInvokableClass(NoOpInvokable.class);
			slotSharingGroup1Vertex2.setParallelism(2);
			slotSharingGroup1Vertex2.setSlotSharingGroup(slotSharingGroup1);
		}

		// create vertices with the same CoLocationGroup
		SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
		CoLocationGroup coLocationGroup1 = new CoLocationGroup();
		final JobVertex coLocationGroupId1Vertex1 = new JobVertex("coLocationGroupId1Vertex1");
		final JobVertex coLocationGroupId1Vertex2 = new JobVertex("coLocationGroupId1Vertex2");
		{
			coLocationGroupId1Vertex1.setInvokableClass(NoOpInvokable.class);
			coLocationGroupId1Vertex1.setParallelism(2);
			coLocationGroupId1Vertex1.setSlotSharingGroup(slotSharingGroup2);
			coLocationGroupId1Vertex1.updateCoLocationGroup(coLocationGroup1);

			coLocationGroupId1Vertex2.setInvokableClass(NoOpInvokable.class);
			coLocationGroupId1Vertex2.setParallelism(2);
			coLocationGroupId1Vertex2.setSlotSharingGroup(slotSharingGroup2);
			coLocationGroupId1Vertex2.updateCoLocationGroup(coLocationGroup1);
		}

		// create vertex with SlotSharingGroup
		SlotSharingGroup slotSharingGroup3 = new SlotSharingGroup();
		final JobVertex sink1 = new JobVertex("sink1");
		sink1.setInvokableClass(NoOpInvokable.class);
		sink1.setParallelism(1);
		sink1.setSlotSharingGroup(slotSharingGroup3);
		sink1.updateCoLocationGroup(null);

		// connect edges
		slotSharingGroup1Vertex1.connectNewDataSetAsInput(source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		slotSharingGroup1Vertex2.connectNewDataSetAsInput(slotSharingGroup1Vertex1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		coLocationGroupId1Vertex1.connectNewDataSetAsInput(slotSharingGroup1Vertex2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		coLocationGroupId1Vertex2.connectNewDataSetAsInput(coLocationGroupId1Vertex1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		sink1.connectNewDataSetAsInput(coLocationGroupId1Vertex2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(source1,
				slotSharingGroup1Vertex1, slotSharingGroup1Vertex2,
				coLocationGroupId1Vertex1, coLocationGroupId1Vertex2,
				sink1);

		return jobGraph;
	}

	@Nonnull
	private JobMaster createJobMaster(
			Configuration configuration,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerSharedServices jobManagerSharedServices) throws Exception {
		return createJobMaster(
			configuration,
			jobGraph,
			highAvailabilityServices,
			jobManagerSharedServices,
			fastHeartbeatServices);
	}

	@Nonnull
	private JobMaster createJobMaster(
			Configuration configuration,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices) throws Exception {

		final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

		return new JobMaster(
			rpcService,
			jobMasterConfiguration,
			jmResourceId,
			jobGraph,
			highAvailabilityServices,
			DefaultSlotPoolFactory.fromConfiguration(configuration, rpcService),
			jobManagerSharedServices,
			heartbeatServices,
			blobServer,
			UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
			new NoOpOnCompletionActions(),
			testingFatalErrorHandler,
			JobMasterTest.class.getClassLoader(),
			highAvailabilityServices.getSubmittedJobGraphStore());
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

		@Override
		public void jobMasterFailed(Throwable cause) {

		}
	}

	private static final class DummyCheckpointStorageLocation implements CompletedCheckpointStorageLocation {

		private static final long serialVersionUID = 164095949572620688L;

		@Override
		public String getExternalPointer() {
			return null;
		}

		@Override
		public StreamStateHandle getMetadataHandle() {
			return null;
		}

		@Override
		public void disposeStorageLocation() throws IOException {

		}
	}

	private static final class TestingInputSplitSource implements InputSplitSource<TestingInputSplit> {
		@Override
		public TestingInputSplit[] createInputSplits(int minNumSplits) {
			return new TestingInputSplit[0];
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(TestingInputSplit[] inputSplits) {
			return new TestingInputSplitAssigner();
		}
	}

	private static final class TestingInputSplitAssigner implements InputSplitAssigner {

		private int splitIndex = 0;

		@Override
		public InputSplit getNextInputSplit(String host, int taskId){
			return new TestingInputSplit(splitIndex++);
		}

		@Override
		public void inputSplitsAssigned(int taskId, List<InputSplit> inputSplits) {
			throw new UnsupportedOperationException("This method should not be called.");
		}
	}

	private static final class TestingInputSplit implements InputSplit {

		private final int splitNumber;

		public TestingInputSplit(int number) {
			this.splitNumber = number;
		}

		public int getSplitNumber() {
			return splitNumber;
		}
	}
}
