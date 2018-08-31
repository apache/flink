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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.InMemorySubmittedJobGraphStore;
import org.apache.flink.runtime.testutils.RecoverableCompletedCheckpointStore;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.pf.FI;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import akka.testkit.CallingThreadDispatcher;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import scala.Int;
import scala.Option;
import scala.PartialFunction;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobManagerHARecoveryTest extends TestLogger {

	private static ActorSystem system;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	/**
	 * Tests that the persisted job is not removed from the SubmittedJobGraphStore if the JobManager
	 * loses its leadership. Furthermore, it tests that the job manager can recover the job from
	 * the SubmittedJobGraphStore and checkpoint state is recovered as well.
	 */
	@Test
	public void testJobRecoveryWhenLosingLeadership() throws Exception {
		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);
		FiniteDuration jobRecoveryTimeout = new FiniteDuration(0, TimeUnit.SECONDS);
		Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();
		Configuration flinkConfiguration = new Configuration();
		UUID leaderSessionID = UUID.randomUUID();
		UUID newLeaderSessionID = UUID.randomUUID();
		int slots = 2;
		ActorRef archive = null;
		ActorRef jobManager = null;
		ActorRef taskManager = null;

		flinkConfiguration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		flinkConfiguration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().toString());
		flinkConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slots);
		flinkConfiguration.setLong(BlobServerOptions.CLEANUP_INTERVAL, 3_600L);

		try {
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

			InMemorySubmittedJobGraphStore submittedJobGraphStore = new InMemorySubmittedJobGraphStore();
			submittedJobGraphStore.start(null);
			CompletedCheckpointStore checkpointStore = new RecoverableCompletedCheckpointStore();
			CheckpointIDCounter checkpointCounter = new StandaloneCheckpointIDCounter();
			CheckpointRecoveryFactory checkpointStateFactory = new TestingCheckpointRecoveryFactory(checkpointStore, checkpointCounter);
			TestingLeaderElectionService myLeaderElectionService = new TestingLeaderElectionService();
			SettableLeaderRetrievalService myLeaderRetrievalService = new SettableLeaderRetrievalService(
				null,
				null);
			TestingHighAvailabilityServices testingHighAvailabilityServices = new TestingHighAvailabilityServices();

			testingHighAvailabilityServices.setJobMasterLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID, myLeaderRetrievalService);

			InstanceManager instanceManager = new InstanceManager();
			instanceManager.addInstanceListener(scheduler);

			archive = system.actorOf(JobManager.getArchiveProps(MemoryArchivist.class, 10, Option.<Path>empty()));

			BlobServer blobServer = new BlobServer(
				flinkConfiguration,
				testingHighAvailabilityServices.createBlobStore());
			blobServer.start();
			Props jobManagerProps = Props.create(
				TestingJobManager.class,
				flinkConfiguration,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				instanceManager,
				scheduler,
				blobServer,
				new BlobLibraryCacheManager(blobServer, FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST, new String[0]),
				archive,
				new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(Int.MaxValue(), 100),
				timeout,
				myLeaderElectionService,
				submittedJobGraphStore,
				checkpointStateFactory,
				jobRecoveryTimeout,
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
				Option.<String>empty());

			jobManager = system.actorOf(jobManagerProps);
			ActorGateway gateway = new AkkaActorGateway(jobManager, leaderSessionID);

			taskManager = TaskManager.startTaskManagerComponentsAndActor(
				flinkConfiguration,
				ResourceID.generate(),
				system,
				testingHighAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				"localhost",
				Option.<String>apply("taskmanager"),
				true,
				TestingTaskManager.class);

			ActorGateway tmGateway = new AkkaActorGateway(taskManager, leaderSessionID);

			Future<Object> tmAlive = tmGateway.ask(TestingMessages.getAlive(), deadline.timeLeft());

			Await.ready(tmAlive, deadline.timeLeft());

			JobVertex sourceJobVertex = new JobVertex("Source");
			sourceJobVertex.setInvokableClass(BlockingStatefulInvokable.class);
			sourceJobVertex.setParallelism(slots);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceJobVertex);

			List<JobVertexID> vertexId = Collections.singletonList(sourceJobVertex.getID());
			jobGraph.setSnapshotSettings(new JobCheckpointingSettings(
					vertexId,
					vertexId,
					vertexId,
					new CheckpointCoordinatorConfiguration(
						100L,
						10L * 60L * 1000L,
						0L,
						1,
						CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
						true),
					null));

			BlockingStatefulInvokable.initializeStaticHelpers(slots);

			Future<Object> isLeader = gateway.ask(
					TestingJobManagerMessages.getNotifyWhenLeader(),
					deadline.timeLeft());

			Future<Object> isConnectedToJobManager = tmGateway.ask(
					new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager),
					deadline.timeLeft());

			// tell jobManager that he's the leader
			myLeaderElectionService.isLeader(leaderSessionID);
			// tell taskManager who's the leader
			myLeaderRetrievalService.notifyListener(gateway.path(), leaderSessionID);

			Await.ready(isLeader, deadline.timeLeft());
			Await.ready(isConnectedToJobManager, deadline.timeLeft());

			// submit blocking job
			Future<Object> jobSubmitted = gateway.ask(
					new JobManagerMessages.SubmitJob(jobGraph, ListeningBehaviour.DETACHED),
					deadline.timeLeft());

			Await.ready(jobSubmitted, deadline.timeLeft());

			// Wait for some checkpoints to complete
			BlockingStatefulInvokable.awaitCompletedCheckpoints();

			Future<Object> jobRemoved = gateway.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID()), deadline.timeLeft());

			// Revoke leadership
			myLeaderElectionService.notLeader();

			// check that the job gets removed from the JobManager
			Await.ready(jobRemoved, deadline.timeLeft());
			// but stays in the submitted job graph store
			assertTrue(submittedJobGraphStore.contains(jobGraph.getJobID()));

			Future<Object> jobRunning = gateway.ask(new TestingJobManagerMessages.NotifyWhenJobStatus(jobGraph.getJobID(), JobStatus.RUNNING), deadline.timeLeft());

			// Make JobManager again a leader
			myLeaderElectionService.isLeader(newLeaderSessionID);
			// tell the TaskManager about it
			myLeaderRetrievalService.notifyListener(gateway.path(), newLeaderSessionID);

			// wait that the job is recovered and reaches state RUNNING
			Await.ready(jobRunning, deadline.timeLeft());

			Future<Object> jobFinished = gateway.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID()), deadline.timeLeft());

			BlockingInvokable.unblock();

			// wait til the job has finished
			Await.ready(jobFinished, deadline.timeLeft());

			// check that the job has been removed from the submitted job graph store
			assertFalse(submittedJobGraphStore.contains(jobGraph.getJobID()));

			// Check that state has been recovered
			long[] recoveredStates = BlockingStatefulInvokable.getRecoveredStates();
			for (long state : recoveredStates) {
				boolean isExpected = state >= BlockingStatefulInvokable.NUM_CHECKPOINTS_TO_COMPLETE;
				assertTrue("Did not recover checkpoint state correctly, expecting >= " +
						BlockingStatefulInvokable.NUM_CHECKPOINTS_TO_COMPLETE +
						", but state was " + state, isExpected);
			}
		} finally {
			if (archive != null) {
				archive.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (jobManager != null) {
				jobManager.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (taskManager != null) {
				taskManager.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}
	}

	/**
	 * Tests that a job recovery failure terminates the {@link JobManager}.
	 */
	@Test
	public void testFailingJobRecovery() throws Exception {
		final FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);
		final FiniteDuration jobRecoveryTimeout = new FiniteDuration(0, TimeUnit.SECONDS);
		Deadline deadline = new FiniteDuration(1, TimeUnit.MINUTES).fromNow();
		final Configuration flinkConfiguration = new Configuration();
		UUID leaderSessionID = UUID.randomUUID();
		ActorRef jobManager = null;
		JobID jobId1 = new JobID();
		JobID jobId2 = new JobID();

		// set HA mode to zookeeper so that we try to recover jobs
		flinkConfiguration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

		try {
			final SubmittedJobGraphStore submittedJobGraphStore = mock(SubmittedJobGraphStore.class);

			SubmittedJobGraph submittedJobGraph = mock(SubmittedJobGraph.class);
			when(submittedJobGraph.getJobId()).thenReturn(jobId2);

			when(submittedJobGraphStore.getJobIds()).thenReturn(Arrays.asList(jobId1, jobId2));

			// fail the first job recovery
			when(submittedJobGraphStore.recoverJobGraph(eq(jobId1))).thenThrow(new Exception("Test exception"));
			// succeed the second job recovery
			when(submittedJobGraphStore.recoverJobGraph(eq(jobId2))).thenReturn(submittedJobGraph);

			final TestingLeaderElectionService myLeaderElectionService = new TestingLeaderElectionService();

			final Collection<JobID> recoveredJobs = new ArrayList<>(2);

			BlobServer blobServer = mock(BlobServer.class);
			Props jobManagerProps = Props.create(
				TestingFailingHAJobManager.class,
				flinkConfiguration,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				mock(InstanceManager.class),
				mock(Scheduler.class),
				blobServer,
				new BlobLibraryCacheManager(blobServer, FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST, new String[0]),
				ActorRef.noSender(),
				new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(Int.MaxValue(), 100),
				timeout,
				myLeaderElectionService,
				submittedJobGraphStore,
				mock(CheckpointRecoveryFactory.class),
				jobRecoveryTimeout,
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
				recoveredJobs).withDispatcher(CallingThreadDispatcher.Id());

			jobManager = system.actorOf(jobManagerProps);

			final TestProbe testProbe = new TestProbe(system);

			testProbe.watch(jobManager);

			Future<Object> started = Patterns.ask(jobManager, new Identify(42), deadline.timeLeft().toMillis());

			Await.ready(started, deadline.timeLeft());

			// make the job manager the leader --> this triggers the recovery of all jobs
			myLeaderElectionService.isLeader(leaderSessionID);

			// check that we did not recover any jobs
			assertThat(recoveredJobs, is(empty()));

			// verify that the JobManager terminated
			testProbe.expectTerminated(jobManager, timeout);
		} finally {
			TestingUtils.stopActor(jobManager);
		}
	}

	static class TestingFailingHAJobManager extends JobManager {

		private final Collection<JobID> recoveredJobs;

		public TestingFailingHAJobManager(
			Configuration flinkConfiguration,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			InstanceManager instanceManager,
			Scheduler scheduler,
			BlobServer blobServer,
			BlobLibraryCacheManager libraryCacheManager,
			ActorRef archive,
			RestartStrategyFactory restartStrategyFactory,
			FiniteDuration timeout,
			LeaderElectionService leaderElectionService,
			SubmittedJobGraphStore submittedJobGraphs,
			CheckpointRecoveryFactory checkpointRecoveryFactory,
			FiniteDuration jobRecoveryTimeout,
			JobManagerMetricGroup jobManagerMetricGroup,
			Collection<JobID> recoveredJobs) {
			super(
				flinkConfiguration,
				futureExecutor,
				ioExecutor,
				instanceManager,
				scheduler,
				blobServer,
				libraryCacheManager,
				archive,
				restartStrategyFactory,
				timeout,
				leaderElectionService,
				submittedJobGraphs,
				checkpointRecoveryFactory,
				jobRecoveryTimeout,
				jobManagerMetricGroup,
				Option.<String>empty());

			this.recoveredJobs = recoveredJobs;
		}

		@Override
		public PartialFunction<Object, BoxedUnit> handleMessage() {
			return ReceiveBuilder.match(
				JobManagerMessages.RecoverSubmittedJob.class,
				new FI.UnitApply<JobManagerMessages.RecoverSubmittedJob>() {
					@Override
					public void apply(JobManagerMessages.RecoverSubmittedJob submitJob) throws Exception {
						recoveredJobs.add(submitJob.submittedJobGraph().getJobId());
					}
				}).matchAny(new FI.UnitApply<Object>() {
				@Override
				public void apply(Object o) throws Exception {
					TestingFailingHAJobManager.super.handleMessage().apply(o);
				}
			}).build();
		}
	}

	public static class BlockingInvokable extends AbstractInvokable {

		private static final OneShotLatch LATCH = new OneShotLatch();

		public BlockingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {

			OperatorID operatorID = OperatorID.fromJobVertexID(getEnvironment().getJobVertexId());
			TaskStateManager taskStateManager = getEnvironment().getTaskStateManager();
			PrioritizedOperatorSubtaskState subtaskState = taskStateManager.prioritizedOperatorState(operatorID);

			int subtaskIndex = getIndexInSubtaskGroup();
			if (subtaskIndex < BlockingStatefulInvokable.recoveredStates.length) {
				Iterator<OperatorStateHandle> iterator =
					subtaskState.getJobManagerManagedOperatorState().iterator();

				if (iterator.hasNext()) {
					OperatorStateHandle operatorStateHandle = iterator.next();
					try (FSDataInputStream in = operatorStateHandle.openInputStream()) {
						BlockingStatefulInvokable.recoveredStates[subtaskIndex] =
							InstantiationUtil.deserializeObject(in, getUserCodeClassLoader());
					}
				}
				Assert.assertFalse(iterator.hasNext());
			}

			LATCH.await();
		}

		public static void unblock() {
			LATCH.trigger();
		}
	}

	public static class BlockingStatefulInvokable extends BlockingInvokable {

		private static final int NUM_CHECKPOINTS_TO_COMPLETE = 5;

		private static volatile CountDownLatch completedCheckpointsLatch = new CountDownLatch(1);

		static volatile long[] recoveredStates = new long[0];

		private int completedCheckpoints = 0;

		public BlockingStatefulInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
			ByteStreamStateHandle byteStreamStateHandle = new ByteStreamStateHandle(
					String.valueOf(UUID.randomUUID()),
					InstantiationUtil.serializeObject(checkpointMetaData.getCheckpointId()));

			Map<String, OperatorStateHandle.StateMetaInfo> stateNameToPartitionOffsets = new HashMap<>(1);
			stateNameToPartitionOffsets.put(
				"test-state",
				new OperatorStateHandle.StateMetaInfo(new long[]{0L}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));

			OperatorStateHandle operatorStateHandle = new OperatorStreamStateHandle(stateNameToPartitionOffsets, byteStreamStateHandle);

			TaskStateSnapshot checkpointStateHandles = new TaskStateSnapshot();
			checkpointStateHandles.putSubtaskStateByOperatorID(
				OperatorID.fromJobVertexID(getEnvironment().getJobVertexId()),
				new OperatorSubtaskState(
					StateObjectCollection.singleton(operatorStateHandle),
					StateObjectCollection.empty(),
					StateObjectCollection.empty(),
					StateObjectCollection.empty()));

			getEnvironment().acknowledgeCheckpoint(
					checkpointMetaData.getCheckpointId(),
					new CheckpointMetrics(0L, 0L, 0L, 0L),
					checkpointStateHandles);
			return true;
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
			throw new UnsupportedOperationException("should not be called!");
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
			throw new UnsupportedOperationException("should not be called!");
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			if (completedCheckpoints++ > NUM_CHECKPOINTS_TO_COMPLETE) {
				completedCheckpointsLatch.countDown();
			}
		}

		static void initializeStaticHelpers(int numSubtasks) {
			completedCheckpointsLatch = new CountDownLatch(numSubtasks);
			recoveredStates = new long[numSubtasks];
		}

		static void awaitCompletedCheckpoints() throws InterruptedException {
			completedCheckpointsLatch.await();
		}

		static long[] getRecoveredStates() {
			return recoveredStates;
		}

		private static OperatorStateHandle extractSingletonOperatorState(TaskStateSnapshot taskStateHandles) {
			Set<Map.Entry<OperatorID, OperatorSubtaskState>> subtaskStateMappings = taskStateHandles.getSubtaskStateMappings();
			Preconditions.checkNotNull(subtaskStateMappings);
			Preconditions.checkState(subtaskStateMappings.size()  == 1);
			OperatorSubtaskState subtaskState = subtaskStateMappings.iterator().next().getValue();
			Collection<OperatorStateHandle> managedOperatorState =
				Preconditions.checkNotNull(subtaskState).getManagedOperatorState();
			Preconditions.checkNotNull(managedOperatorState);
			Preconditions.checkState(managedOperatorState.size()  == 1);
			return managedOperatorState.iterator().next();
		}
	}
}
