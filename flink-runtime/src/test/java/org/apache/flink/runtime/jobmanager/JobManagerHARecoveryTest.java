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
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.HeapStateStore;
import org.apache.flink.runtime.checkpoint.SavepointStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.Preconditions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Int;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
	 * the SubmittedJobGraphStore.
	 *
	 * @throws Exception
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

		flinkConfiguration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		flinkConfiguration.setString(ConfigConstants.ZOOKEEPER_RECOVERY_PATH, temporaryFolder.newFolder().toString());
		flinkConfiguration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slots);

		try {

			Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

			MySubmittedJobGraphStore mySubmittedJobGraphStore = new MySubmittedJobGraphStore();
			TestingLeaderElectionService myLeaderElectionService = new TestingLeaderElectionService();
			TestingLeaderRetrievalService myLeaderRetrievalService = new TestingLeaderRetrievalService();

			InstanceManager instanceManager = new InstanceManager();
			instanceManager.addInstanceListener(scheduler);

			archive = system.actorOf(Props.create(
				MemoryArchivist.class,
				10), "archive");

			Props jobManagerProps = Props.create(
				TestingJobManager.class,
				flinkConfiguration,
				new ForkJoinPool(),
				instanceManager,
				scheduler,
				new BlobLibraryCacheManager(new BlobServer(flinkConfiguration), 3600000),
				archive,
				new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(Int.MaxValue(), 100),
				timeout,
				myLeaderElectionService,
				mySubmittedJobGraphStore,
				new StandaloneCheckpointRecoveryFactory(),
				new SavepointStore(new HeapStateStore()),
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
			sourceJobVertex.setInvokableClass(BlockingInvokable.class);
			sourceJobVertex.setParallelism(slots);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceJobVertex);

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
	 * Tests that the persisted job is not removed from the job graph store
	 * after the postStop method of the JobManager. Furthermore, it checks
	 * that BLOBs of the JobGraph are recovered properly and cleaned up after
	 * the job finishes.
	 */
	@Test
	public void testBlobRecoveryAfterLostJobManager() throws Exception {
		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);
		FiniteDuration jobRecoveryTimeout = new FiniteDuration(3, TimeUnit.SECONDS);
		Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();
		Configuration flinkConfiguration = new Configuration();
		UUID leaderSessionID = UUID.randomUUID();
		UUID newLeaderSessionID = UUID.randomUUID();
		int slots = 2;
		ActorRef archiveRef = null;
		ActorRef jobManagerRef = null;
		ActorRef taskManagerRef = null;

		String haStoragePath = temporaryFolder.newFolder().toString();

		flinkConfiguration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		flinkConfiguration.setString(ConfigConstants.ZOOKEEPER_RECOVERY_PATH, haStoragePath);
		flinkConfiguration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slots);

		try {
			MySubmittedJobGraphStore mySubmittedJobGraphStore = new MySubmittedJobGraphStore();
			TestingLeaderElectionService myLeaderElectionService = new TestingLeaderElectionService();
			TestingLeaderRetrievalService myLeaderRetrievalService = new TestingLeaderRetrievalService();

			archiveRef = system.actorOf(Props.create(
					MemoryArchivist.class,
					10), "archive");

			jobManagerRef = createJobManagerActor(
					"jobmanager-0",
					flinkConfiguration,
					myLeaderElectionService,
					mySubmittedJobGraphStore,
					3600000,
					timeout,
					jobRecoveryTimeout, archiveRef);

			ActorGateway jobManager = new AkkaActorGateway(jobManagerRef, leaderSessionID);

			taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
					flinkConfiguration,
					ResourceID.generate(),
					system,
					"localhost",
					Option.apply("taskmanager"),
					Option.apply((LeaderRetrievalService) myLeaderRetrievalService),
					true,
					TestingTaskManager.class);

			ActorGateway tmGateway = new AkkaActorGateway(taskManagerRef, leaderSessionID);

			Future<Object> tmAlive = tmGateway.ask(TestingMessages.getAlive(), deadline.timeLeft());

			Await.ready(tmAlive, deadline.timeLeft());

			JobVertex sourceJobVertex = new JobVertex("Source");
			sourceJobVertex.setInvokableClass(BlockingInvokable.class);
			sourceJobVertex.setParallelism(slots);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceJobVertex);

			// Upload fake JAR file to first JobManager
			File jarFile = temporaryFolder.newFile();
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(jarFile));
			out.close();

			jobGraph.addJar(new Path(jarFile.toURI()));
			JobClient.uploadJarFiles(jobGraph, jobManager, deadline.timeLeft());

			Future<Object> isLeader = jobManager.ask(
					TestingJobManagerMessages.getNotifyWhenLeader(),
					deadline.timeLeft());

			Future<Object> isConnectedToJobManager = tmGateway.ask(
					new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManagerRef),
					deadline.timeLeft());

			// tell jobManager that he's the leader
			myLeaderElectionService.isLeader(leaderSessionID);
			// tell taskManager who's the leader
			myLeaderRetrievalService.notifyListener(jobManager.path(), leaderSessionID);

			Await.ready(isLeader, deadline.timeLeft());
			Await.ready(isConnectedToJobManager, deadline.timeLeft());

			// submit blocking job
			Future<Object> jobSubmitted = jobManager.ask(
					new JobManagerMessages.SubmitJob(jobGraph, ListeningBehaviour.DETACHED),
					deadline.timeLeft());

			Await.ready(jobSubmitted, deadline.timeLeft());

			// Wait for running
			Future<Object> jobRunning = jobManager.ask(
					new TestingJobManagerMessages.NotifyWhenJobStatus(jobGraph.getJobID(), JobStatus.RUNNING),
					deadline.timeLeft());

			Await.ready(jobRunning, deadline.timeLeft());

			// terminate the job manager
			jobManagerRef.tell(PoisonPill.getInstance(), ActorRef.noSender());

			Future<Boolean> terminatedFuture = Patterns.gracefulStop(jobManagerRef, deadline.timeLeft());
			Boolean terminated = Await.result(terminatedFuture, deadline.timeLeft());
			assertTrue("Failed to stop job manager", terminated);

			// job stays in the submitted job graph store
			assertTrue(mySubmittedJobGraphStore.contains(jobGraph.getJobID()));

			// start new job manager
			myLeaderElectionService.reset();

			jobManagerRef = createJobManagerActor(
					"jobmanager-1",
					flinkConfiguration,
					myLeaderElectionService,
					mySubmittedJobGraphStore,
					500,
					timeout,
					jobRecoveryTimeout,
					archiveRef);

			jobManager = new AkkaActorGateway(jobManagerRef, newLeaderSessionID);

			Future<Object> isAlive = jobManager.ask(TestingMessages.getAlive(), deadline.timeLeft());

			isLeader = jobManager.ask(
					TestingJobManagerMessages.getNotifyWhenLeader(),
					deadline.timeLeft());

			isConnectedToJobManager = tmGateway.ask(
					new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManagerRef),
					deadline.timeLeft());

			Await.ready(isAlive, deadline.timeLeft());

			// tell new jobManager that he's the leader
			myLeaderElectionService.isLeader(newLeaderSessionID);
			// tell taskManager who's the leader
			myLeaderRetrievalService.notifyListener(jobManager.path(), newLeaderSessionID);

			Await.ready(isLeader, deadline.timeLeft());
			Await.ready(isConnectedToJobManager, deadline.timeLeft());

			jobRunning = jobManager.ask(
					new TestingJobManagerMessages.NotifyWhenJobStatus(jobGraph.getJobID(), JobStatus.RUNNING),
					deadline.timeLeft());

			// wait that the job is recovered and reaches state RUNNING
			Await.ready(jobRunning, deadline.timeLeft());

			Future<Object> jobFinished = jobManager.ask(
					new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID()),
					deadline.timeLeft());

			BlockingInvokable.unblock();

			// wait til the job has finished
			Await.ready(jobFinished, deadline.timeLeft());

			// check that the job has been removed from the submitted job graph store
			assertFalse(mySubmittedJobGraphStore.contains(jobGraph.getJobID()));

			// Check that the BLOB store files are removed
			File rootPath = new File(haStoragePath);

			boolean cleanedUpFiles = false;
			while (deadline.hasTimeLeft()) {
				if (listFiles(rootPath).isEmpty()) {
					cleanedUpFiles = true;
					break;
				} else {
					Thread.sleep(100);
				}
			}

			assertTrue("BlobStore files not cleaned up", cleanedUpFiles);
		} finally {
			if (archiveRef != null) {
				archiveRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (jobManagerRef != null) {
				jobManagerRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (taskManagerRef != null) {
				taskManagerRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}
	}

	/**
	 * Recursively lists all files under the given base path.
	 *
	 * @param basePath Base path to start listing files from.
	 * @return Collection of all files found under the base path.
	 * @throws IOException If base path not found.
	 */
	private static Collection<File> listFiles(File basePath) throws IOException {
		Preconditions.checkNotNull(basePath);

		Set<File> files = new HashSet<>();

		File[] listed = basePath.listFiles();
		if (listed == null) {
			throw new IOException(basePath + " not found");
		} else {
			for (File file : listed) {
				if (file.isFile()) {
					files.add(file);
				} else {
					files.addAll(listFiles(file));
				}
			}
			return files;
		}
	}

	/**
	 * Creates a new JobManager actor.
	 */
	private ActorRef createJobManagerActor(
			String name,
			Configuration flinkConfiguration,
			LeaderElectionService leaderElectionService,
			SubmittedJobGraphStore submittedJobGraphs,
			int blobCleanupInterval,
			FiniteDuration timeout,
			FiniteDuration jobRecoveryTimeout,
			ActorRef archive) throws IOException {

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		InstanceManager instanceManager = new InstanceManager();
		instanceManager.addInstanceListener(scheduler);

		Props jobManagerProps = Props.create(
				TestingJobManager.class,
				flinkConfiguration,
				new ForkJoinPool(),
				instanceManager,
				scheduler,
				new BlobLibraryCacheManager(new BlobServer(flinkConfiguration), blobCleanupInterval),
				archive,
				new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(Int.MaxValue(), 100),
				timeout,
				leaderElectionService,
				submittedJobGraphs,
				new StandaloneCheckpointRecoveryFactory(),
				new SavepointStore(new HeapStateStore()),
				jobRecoveryTimeout,
				Option.apply(null));

		return system.actorOf(jobManagerProps, name);
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
		public List<SubmittedJobGraph> recoverJobGraphs() throws Exception {
			return new ArrayList<>(storedJobs.values());
		}

		@Override
		public Option<SubmittedJobGraph> recoverJobGraph(JobID jobId) throws Exception {
			if (storedJobs.containsKey(jobId)) {
				return Option.apply(storedJobs.get(jobId));
			} else {
				return Option.apply(null);
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

		boolean contains(JobID jobId) {
			return storedJobs.containsKey(jobId);
		}
	}

	public static class BlockingInvokable extends AbstractInvokable {

		private static boolean blocking = true;
		private static Object lock = new Object();

		@Override
		public void invoke() throws Exception {
			while(blocking) {
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

}
