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
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestByteStreamStateHandleDeepCompare;
import org.apache.flink.util.InstantiationUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Int;
import scala.Option;
import scala.PartialFunction;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobManagerHARecoveryTest {

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
		FiniteDuration jobRecoveryTimeout = new FiniteDuration(3, TimeUnit.SECONDS);
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
		flinkConfiguration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slots);

		ExecutorService executor = null;

		try {
			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

			MySubmittedJobGraphStore mySubmittedJobGraphStore = new MySubmittedJobGraphStore();
			MyCheckpointStore checkpointStore = new MyCheckpointStore();
			CheckpointIDCounter checkpointCounter = new StandaloneCheckpointIDCounter();
			CheckpointRecoveryFactory checkpointStateFactory = new MyCheckpointRecoveryFactory(checkpointStore, checkpointCounter);
			TestingLeaderElectionService myLeaderElectionService = new TestingLeaderElectionService();
			TestingLeaderRetrievalService myLeaderRetrievalService = new TestingLeaderRetrievalService();

			InstanceManager instanceManager = new InstanceManager();
			instanceManager.addInstanceListener(scheduler);

			archive = system.actorOf(Props.create(
					MemoryArchivist.class,
					10), "archive");

			executor = new ForkJoinPool();

			Props jobManagerProps = Props.create(
				TestingJobManager.class,
				flinkConfiguration,
				executor,
				executor,
				instanceManager,
				scheduler,
				new BlobLibraryCacheManager(new BlobServer(flinkConfiguration), 3600000),
				archive,
				new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(Int.MaxValue(), 100),
				timeout,
				myLeaderElectionService,
				mySubmittedJobGraphStore,
				checkpointStateFactory,
				jobRecoveryTimeout,
				Option.apply(null));

			jobManager = system.actorOf(jobManagerProps, "jobmanager");
			ActorGateway gateway = new AkkaActorGateway(jobManager, leaderSessionID);

			taskManager = TaskManager.startTaskManagerComponentsAndActor(
					flinkConfiguration,
					ResourceID.generate(),
					system,
					"localhost",
					Option.apply("taskmanager"),
					Option.apply((LeaderRetrievalService) myLeaderRetrievalService),
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
			jobGraph.setSnapshotSettings(new JobSnapshottingSettings(
					vertexId,
					vertexId,
					vertexId,
					100,
					10 * 60 * 1000,
					0,
					1,
					ExternalizedCheckpointSettings.none()));

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
			assertTrue(mySubmittedJobGraphStore.contains(jobGraph.getJobID()));

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
			assertFalse(mySubmittedJobGraphStore.contains(jobGraph.getJobID()));

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

			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	/**
	 * Tests that a failing job recovery won't cause other job recoveries to fail.
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

			Props jobManagerProps = Props.create(
				TestingFailingHAJobManager.class,
				flinkConfiguration,
				Executors.directExecutor(),
				Executors.directExecutor(),
				mock(InstanceManager.class),
				mock(Scheduler.class),
				new BlobLibraryCacheManager(mock(BlobService.class), 1 << 20),
				ActorRef.noSender(),
				new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(Int.MaxValue(), 100),
				timeout,
				myLeaderElectionService,
				submittedJobGraphStore,
				mock(CheckpointRecoveryFactory.class),
				jobRecoveryTimeout,
				Option.<MetricRegistry>apply(null),
				recoveredJobs).withDispatcher(CallingThreadDispatcher.Id());

			jobManager = system.actorOf(jobManagerProps, "jobmanager");

			Future<Object> started = Patterns.ask(jobManager, new Identify(42), deadline.timeLeft().toMillis());

			Await.ready(started, deadline.timeLeft());

			// make the job manager the leader --> this triggers the recovery of all jobs
			myLeaderElectionService.isLeader(leaderSessionID);

			// check that we have successfully recovered the second job
			assertThat(recoveredJobs, containsInAnyOrder(jobId2));
		} finally {
			TestingUtils.stopActor(jobManager);
		}
	}

	static class TestingFailingHAJobManager extends JobManager {

		private final Collection<JobID> recoveredJobs;

		public TestingFailingHAJobManager(
			Configuration flinkConfiguration,
			Executor futureExecutor,
			Executor ioExecutor,
			InstanceManager instanceManager,
			Scheduler scheduler,
			BlobLibraryCacheManager libraryCacheManager,
			ActorRef archive,
			RestartStrategyFactory restartStrategyFactory,
			FiniteDuration timeout,
			LeaderElectionService leaderElectionService,
			SubmittedJobGraphStore submittedJobGraphs,
			CheckpointRecoveryFactory checkpointRecoveryFactory,
			FiniteDuration jobRecoveryTimeout,
			Option<MetricRegistry> metricsRegistry,
			Collection<JobID> recoveredJobs) {
			super(
				flinkConfiguration,
				futureExecutor,
				ioExecutor,
				instanceManager,
				scheduler,
				libraryCacheManager,
				archive,
				restartStrategyFactory,
				timeout,
				leaderElectionService,
				submittedJobGraphs,
				checkpointRecoveryFactory,
				jobRecoveryTimeout,
				metricsRegistry);

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

	/**
	 * A checkpoint store, which supports shutdown and suspend. You can use this to test HA
	 * as long as the factory always returns the same store instance.
	 */
	static class MyCheckpointStore implements CompletedCheckpointStore {

		private final ArrayDeque<CompletedCheckpoint> checkpoints = new ArrayDeque<>(2);

		private final ArrayDeque<CompletedCheckpoint> suspended = new ArrayDeque<>(2);

		@Override
		public void recover() throws Exception {
			checkpoints.addAll(suspended);
			suspended.clear();
		}

		@Override
		public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
			checkpoints.addLast(checkpoint);
			if (checkpoints.size() > 1) {
				checkpoints.removeFirst().subsume();
			}
		}

		@Override
		public CompletedCheckpoint getLatestCheckpoint() throws Exception {
			return checkpoints.isEmpty() ? null : checkpoints.getLast();
		}

		@Override
		public void shutdown(JobStatus jobStatus) throws Exception {
			if (jobStatus.isGloballyTerminalState()) {
				checkpoints.clear();
				suspended.clear();
			} else {
				suspended.addAll(checkpoints);
				checkpoints.clear();
			}
		}

		@Override
		public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
			return new ArrayList<>(checkpoints);
		}

		@Override
		public int getNumberOfRetainedCheckpoints() {
			return checkpoints.size();
		}

	}

	static class MyCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

		private final CompletedCheckpointStore store;
		private final CheckpointIDCounter counter;

		public MyCheckpointRecoveryFactory(CompletedCheckpointStore store, CheckpointIDCounter counter) {
			this.store = store;
			this.counter = counter;
		}

		@Override
		public void start() {
		}

		@Override
		public void stop() {
		}

		@Override
		public CompletedCheckpointStore createCheckpointStore(JobID jobId, ClassLoader userClassLoader) throws Exception {
			return store;
		}

		@Override
		public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
			return counter;
		}
	}

	static class MySubmittedJobGraphStore implements SubmittedJobGraphStore {

		Map<JobID, SubmittedJobGraph> storedJobs = new HashMap<>();

		@Override
		public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {

		}

		@Override
		public void stop() throws Exception {

		}

		@Override
		public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
			if (storedJobs.containsKey(jobId)) {
				return storedJobs.get(jobId);
			} else {
				return null;
			}
		}

		@Override
		public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
			storedJobs.put(jobGraph.getJobId(), jobGraph);
		}

		@Override
		public void removeJobGraph(JobID jobId) throws Exception {
			storedJobs.remove(jobId);
		}

		@Override
		public Collection<JobID> getJobIds() throws Exception {
			return storedJobs.keySet();
		}

		boolean contains(JobID jobId) {
			return storedJobs.containsKey(jobId);
		}
	}

	public static class BlockingInvokable extends AbstractInvokable {

		private static boolean blocking = true;
		private static Object lock = new Object();

		@Override
		public void invoke() throws Exception {
			while (blocking) {
				synchronized (lock) {
					lock.wait();
				}
			}
		}

		public static void unblock() {
			blocking = false;

			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}

	public static class BlockingStatefulInvokable extends BlockingInvokable implements StatefulTask {

		private static final int NUM_CHECKPOINTS_TO_COMPLETE = 5;

		private static volatile CountDownLatch completedCheckpointsLatch = new CountDownLatch(1);

		private static volatile long[] recoveredStates = new long[0];

		private int completedCheckpoints = 0;

		@Override
		public void setInitialState(
				TaskStateHandles taskStateHandles) throws Exception {
			int subtaskIndex = getIndexInSubtaskGroup();
			if (subtaskIndex < recoveredStates.length) {
				try (FSDataInputStream in = taskStateHandles.getLegacyOperatorState().get(0).openInputStream()) {
					recoveredStates[subtaskIndex] = InstantiationUtil.deserializeObject(in, getUserCodeClassLoader());
				}
			}
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData) throws Exception {
			ByteStreamStateHandle byteStreamStateHandle = new TestByteStreamStateHandleDeepCompare(
					String.valueOf(UUID.randomUUID()),
					InstantiationUtil.serializeObject(checkpointMetaData.getCheckpointId()));

			ChainedStateHandle<StreamStateHandle> chainedStateHandle =
					new ChainedStateHandle<StreamStateHandle>(Collections.singletonList(byteStreamStateHandle));
			SubtaskState checkpointStateHandles =
					new SubtaskState(chainedStateHandle, null, null, null, null, 0L);

			getEnvironment().acknowledgeCheckpoint(
					new CheckpointMetaData(checkpointMetaData.getCheckpointId(), -1, 0L, 0L, 0L, 0L),
					checkpointStateHandles);
			return true;
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData) throws Exception {
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

		public static void initializeStaticHelpers(int numSubtasks) {
			completedCheckpointsLatch = new CountDownLatch(numSubtasks);
			recoveredStates = new long[numSubtasks];
		}

		public static void awaitCompletedCheckpoints() throws InterruptedException {
			completedCheckpointsLatch.await();
		}

		public static long[] getRecoveredStates() {
			return recoveredStates;
		}
	}

}
