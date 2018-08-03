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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
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
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSlotPoolFactory;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
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
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkException;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
	 * Tests that in a streaming use case where checkpointing is enabled, a
	 * fixed delay with Integer.MAX_VALUE retries is instantiated if no other restart
	 * strategy has been specified.
	 */
	@Test
	public void testAutomaticRestartingWhenCheckpointing() throws Exception {
		// create savepoint data
		final long savepointId = 42L;
		final File savepointFile = createSavepoint(savepointId);

		// set savepoint settings
		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.forPath(
			savepointFile.getAbsolutePath(),
			true);
		final JobGraph jobGraph = createJobGraphWithCheckpointing(savepointRestoreSettings);

		final StandaloneCompletedCheckpointStore completedCheckpointStore = new StandaloneCompletedCheckpointStore(1);
		final TestingCheckpointRecoveryFactory testingCheckpointRecoveryFactory = new TestingCheckpointRecoveryFactory(
			completedCheckpointStore,
			new StandaloneCheckpointIDCounter());
		haServices.setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory);
		final JobMaster jobMaster = createJobMaster(
			new Configuration(),
			jobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder()
				.setRestartStrategyFactory(RestartStrategyFactory.createRestartStrategyFactory(configuration))
				.build());

		RestartStrategy restartStrategy = jobMaster.getRestartStrategy();

		assertNotNull(restartStrategy);
		assertTrue(restartStrategy instanceof FixedDelayRestartStrategy);
	}

	/**
	 * Tests that an existing checkpoint will have precedence over an savepoint
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
	 * in case of a missing slot offering from a registered TaskExecutor
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
			JobMasterTest.class.getClassLoader());
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

}
