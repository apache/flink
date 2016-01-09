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
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.testkit.TestActorRef;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.JobStatusResponse;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.JobManagerMessages.SubmitJob;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils;
import org.apache.flink.runtime.testutils.JobManagerProcess;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.TestLogger;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests recovery of {@link SubmittedJobGraph} instances.
 */
public class JobManagerSubmittedJobGraphsRecoveryITCase extends TestLogger {

	private final static ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

	private final static FiniteDuration TestTimeOut = new FiniteDuration(5, TimeUnit.MINUTES);

	private static final File FileStateBackendBasePath;

	static {
		try {
			FileStateBackendBasePath = CommonTestUtils.createTempDirectory();
		}
		catch (IOException e) {
			throw new RuntimeException("Error in test setup. Could not create directory.", e);
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		ZooKeeper.shutdown();

		if (FileStateBackendBasePath != null) {
			FileUtils.deleteDirectory(FileStateBackendBasePath);
		}
	}

	@Before
	public void cleanUp() throws Exception {
		if (FileStateBackendBasePath != null) {
			FileUtils.cleanDirectory(FileStateBackendBasePath);
		}

		ZooKeeper.deleteAll();
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Tests that the recovery state is cleaned up after a JobManager stops.
	 */
	@Test
	public void testJobManagerCleanUp() throws Exception {
		Configuration config = ZooKeeperTestUtils.createZooKeeperRecoveryModeConfig(
				ZooKeeper.getConnectString(), FileStateBackendBasePath.getPath());

		// Configure the cluster
		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 1);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

		TestingCluster flink = new TestingCluster(config, false, false);

		try {
			final Deadline deadline = TestTimeOut.fromNow();

			// Start the JobManager and TaskManager
			flink.start(true);

			JobGraph jobGraph = createBlockingJobGraph();

			ActorGateway jobManager = flink.getLeaderGateway(deadline.timeLeft());

			// Submit the job
			jobManager.tell(new SubmitJob(jobGraph, ListeningBehaviour.DETACHED));

			// Wait for the job to start
			JobManagerActorTestUtils.waitForJobStatus(jobGraph.getJobID(), JobStatus.RUNNING,
					jobManager, deadline.timeLeft());
		}
		finally {
			flink.shutdown();
		}

		// Verify that everything is clean
		verifyCleanRecoveryState(config);
	}

	/**
	 * Tests that submissions to non-leaders are handled.
	 */
	@Test
	public void testSubmitJobToNonLeader() throws Exception {
		Configuration config = ZooKeeperTestUtils.createZooKeeperRecoveryModeConfig(
				ZooKeeper.getConnectString(), FileStateBackendBasePath.getPath());

		// Configure the cluster
		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 2);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

		TestingCluster flink = new TestingCluster(config, false, false);

		try {
			final Deadline deadline = TestTimeOut.fromNow();

			// Start the JobManager and TaskManager
			flink.start(true);

			JobGraph jobGraph = createBlockingJobGraph();

			List<ActorRef> bothJobManagers = flink.getJobManagersAsJava();

			ActorGateway leadingJobManager = flink.getLeaderGateway(deadline.timeLeft());

			ActorGateway nonLeadingJobManager;
			if (bothJobManagers.get(0).equals(leadingJobManager.actor())) {
				nonLeadingJobManager = new AkkaActorGateway(bothJobManagers.get(1), null);
			}
			else {
				nonLeadingJobManager = new AkkaActorGateway(bothJobManagers.get(0), null);
			}

			// Submit the job
			nonLeadingJobManager.tell(new SubmitJob(jobGraph, ListeningBehaviour.DETACHED));

			// Wait for the job to start. We are asking the *leading** JM here although we've
			// submitted the job to the non-leading JM. This is the behaviour under test.
			JobManagerActorTestUtils.waitForJobStatus(jobGraph.getJobID(), JobStatus.RUNNING,
					leadingJobManager, deadline.timeLeft());

			log.info("Wait that the non-leader removes the submitted job.");

			// Make sure that the **non-leading** JM has actually removed the job graph from its
			// local state.
			boolean success = false;
			while (!success && deadline.hasTimeLeft()) {
				JobStatusResponse jobStatusResponse = JobManagerActorTestUtils.requestJobStatus(
						jobGraph.getJobID(), nonLeadingJobManager, deadline.timeLeft());

				if (jobStatusResponse instanceof JobManagerMessages.JobNotFound) {
					success = true;
				}
				else {
					log.info(((JobManagerMessages.CurrentJobStatus)jobStatusResponse).status().toString());
					Thread.sleep(100);
				}
			}

			if (!success) {
				fail("Non-leading JM was still holding reference to the job graph.");
			}
		}
		finally {
			flink.shutdown();
		}

		// Verify that everything is clean
		verifyCleanRecoveryState(config);
	}

	/**
	 * Tests that clients receive updates after recovery by a new leader.
	 */
	@Test
	public void testClientNonDetachedListeningBehaviour() throws Exception {
		Configuration config = ZooKeeperTestUtils.createZooKeeperRecoveryModeConfig(
				ZooKeeper.getConnectString(), FileStateBackendBasePath.getPath());

		// Test actor system
		ActorSystem testSystem = null;

		// JobManager setup. Start the job managers as separate processes in order to not run the
		// actors postStop, which cleans up all running jobs.
		JobManagerProcess[] jobManagerProcess = new JobManagerProcess[2];

		LeaderRetrievalService leaderRetrievalService = null;

		ActorSystem taskManagerSystem = null;

		try {
			final Deadline deadline = TestTimeOut.fromNow();

			// Test actor system
			testSystem = AkkaUtils.createActorSystem(new Configuration(),
					new Some<>(new Tuple2<String, Object>("localhost", 0)));

			// The job managers
			jobManagerProcess[0] = new JobManagerProcess(0, config);
			jobManagerProcess[1] = new JobManagerProcess(1, config);

			jobManagerProcess[0].createAndStart();
			jobManagerProcess[1].createAndStart();

			// Leader listener
			TestingListener leaderListener = new TestingListener();
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(config);
			leaderRetrievalService.start(leaderListener);

			// The task manager
			taskManagerSystem = AkkaUtils.createActorSystem(AkkaUtils.getDefaultAkkaConfig());
			TaskManager.startTaskManagerComponentsAndActor(
					config, taskManagerSystem, "localhost",
					Option.<String>empty(), Option.<LeaderRetrievalService>empty(),
					false, TaskManager.class);

			// Client test actor
			TestActorRef<RecordingTestClient> clientRef = TestActorRef.create(
					testSystem, Props.create(RecordingTestClient.class));

			JobGraph jobGraph = createBlockingJobGraph();

			{
				// Initial submission
				leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

				String leaderAddress = leaderListener.getAddress();
				UUID leaderId = leaderListener.getLeaderSessionID();

				// The client
				AkkaActorGateway client = new AkkaActorGateway(clientRef, leaderId);

				// Get the leader ref
				ActorRef leaderRef = AkkaUtils.getActorRef(
						leaderAddress, testSystem, deadline.timeLeft());
				ActorGateway leader = new AkkaActorGateway(leaderRef, leaderId);

				// Submit the job in non-detached mode
				leader.tell(new SubmitJob(jobGraph,
						ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES), client);

				JobManagerActorTestUtils.waitForJobStatus(
						jobGraph.getJobID(), JobStatus.RUNNING, leader, deadline.timeLeft());
			}

			// Who's the boss?
			JobManagerProcess leadingJobManagerProcess;
			if (jobManagerProcess[0].getJobManagerAkkaURL().equals(leaderListener.getAddress())) {
				leadingJobManagerProcess = jobManagerProcess[0];
			}
			else {
				leadingJobManagerProcess = jobManagerProcess[1];
			}

			// Kill the leading job manager process
			leadingJobManagerProcess.destroy();

			{
				// Recovery by the standby JobManager
				leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

				String leaderAddress = leaderListener.getAddress();
				UUID leaderId = leaderListener.getLeaderSessionID();

				ActorRef leaderRef = AkkaUtils.getActorRef(
						leaderAddress, testSystem, deadline.timeLeft());
				ActorGateway leader = new AkkaActorGateway(leaderRef, leaderId);

				JobManagerActorTestUtils.waitForJobStatus(
						jobGraph.getJobID(), JobStatus.RUNNING, leader, deadline.timeLeft());

				// Cancel the job
				leader.tell(new JobManagerMessages.CancelJob(jobGraph.getJobID()));
			}

			// Wait for the execution result
			clientRef.underlyingActor().awaitJobResult(deadline.timeLeft().toMillis());

			int jobSubmitSuccessMessages = 0;
			for (Object msg : clientRef.underlyingActor().getMessages()) {
				if (msg instanceof JobManagerMessages.JobSubmitSuccess) {
					jobSubmitSuccessMessages++;
				}
			}

			// At least two submissions should be ack-ed (initial and recovery). This is quite
			// conservative, but it is still possible that these messages are overtaken by the
			// final message.
			assertEquals(2, jobSubmitSuccessMessages);
		}
		catch (Throwable t) {
			// In case of an error, print the job manager process logs.
			if (jobManagerProcess[0] != null) {
				jobManagerProcess[0].printProcessLog();
			}

			if (jobManagerProcess[1] != null) {
				jobManagerProcess[1].printProcessLog();
			}

			t.printStackTrace();
		}
		finally {
			if (jobManagerProcess[0] != null) {
				jobManagerProcess[0].destroy();
			}

			if (jobManagerProcess[1] != null) {
				jobManagerProcess[1].destroy();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			if (taskManagerSystem != null) {
				taskManagerSystem.shutdown();
			}

			if (testSystem != null) {
				testSystem.shutdown();
			}
		}
	}

	/**
	 * Simple recording client.
	 */
	private static class RecordingTestClient extends UntypedActor {

		private final Queue<Object> messages = new ConcurrentLinkedQueue<>();

		private CountDownLatch jobResultLatch = new CountDownLatch(1);

		@Override
		public void onReceive(Object message) throws Exception {
			if (message instanceof LeaderSessionMessage) {
				message = ((LeaderSessionMessage) message).message();
			}

			messages.add(message);

			// Check for job result
			if (message instanceof JobManagerMessages.JobResultFailure ||
					message instanceof JobManagerMessages.JobResultSuccess) {

				jobResultLatch.countDown();
			}
		}

		public Queue<Object> getMessages() {
			return messages;
		}

		public void awaitJobResult(long timeout) throws InterruptedException {
			jobResultLatch.await(timeout, TimeUnit.MILLISECONDS);
		}
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Creates a simple blocking JobGraph.
	 */
	private static JobGraph createBlockingJobGraph() {
		JobGraph jobGraph = new JobGraph("Blocking program");

		JobVertex jobVertex = new JobVertex("Blocking Vertex");
		jobVertex.setInvokableClass(Tasks.BlockingNoOpInvokable.class);

		jobGraph.addVertex(jobVertex);

		return jobGraph;
	}

	/**
	 * Fails the test if the recovery state (file state backend and ZooKeeper) is not clean.
	 */
	private static void verifyCleanRecoveryState(Configuration config) throws Exception {
		// File state backend empty
		Collection<File> stateHandles = FileUtils.listFiles(
				FileStateBackendBasePath, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

		if (!stateHandles.isEmpty()) {
			fail("File state backend is not clean: " + stateHandles);
		}

		// ZooKeeper
		String currentJobsPath = config.getString(
				ConfigConstants.ZOOKEEPER_JOBGRAPHS_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_JOBGRAPHS_PATH);

		Stat stat = ZooKeeper.getClient().checkExists().forPath(currentJobsPath);

		if (stat.getCversion() == 0) {
			// Sanity check: verify that some changes have been performed
			fail("ZooKeeper state for '" + currentJobsPath + "' has not been modified during " +
					"this test. What are you testing?");
		}

		if (stat.getNumChildren() != 0) {
			// Is everything clean again?
			fail("ZooKeeper path '" + currentJobsPath + "' is not clean: " +
					ZooKeeper.getClient().getChildren().forPath(currentJobsPath));
		}
	}

}
