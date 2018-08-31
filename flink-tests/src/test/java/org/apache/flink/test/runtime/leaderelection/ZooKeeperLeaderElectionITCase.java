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

package org.apache.flink.test.runtime.leaderelection;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.PoisonPill;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test the election of a new JobManager leader.
 */
public class ZooKeeperLeaderElectionITCase extends TestLogger {

	private static final FiniteDuration timeout = TestingUtils.TESTING_DURATION();

	private static TestingServer zkServer;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() throws Exception {
		zkServer = new TestingServer(true);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (zkServer != null) {
			zkServer.close();
			zkServer = null;
		}
	}

	/**
	 * Tests that the TaskManagers successfully register at the new leader once the old leader
	 * is terminated.
	 */
	@Test
	public void testTaskManagerRegistrationAtReelectedLeader() throws Exception {
		File rootFolder = tempFolder.getRoot();

		Configuration configuration = ZooKeeperTestUtils.createZooKeeperHAConfig(
			zkServer.getConnectString(),
			rootFolder.getPath());
		configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, UUID.randomUUID().toString());

		int numJMs = 10;
		int numTMs = 3;

		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, numJMs);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);

		TestingCluster cluster = new TestingCluster(configuration);

		try {
			cluster.start();

			for (int i = 0; i < numJMs; i++) {
				ActorGateway leadingJM = cluster.getLeaderGateway(timeout);

				cluster.waitForTaskManagersToBeRegisteredAtJobManager(leadingJM.actor());

				Future<Object> registeredTMs = leadingJM.ask(
					JobManagerMessages.getRequestNumberRegisteredTaskManager(),
					timeout);

				int numRegisteredTMs = (Integer) Await.result(registeredTMs, timeout);

				assertEquals(numTMs, numRegisteredTMs);

				cluster.clearLeader();
				leadingJM.tell(PoisonPill.getInstance());
			}
		} finally {
			cluster.stop();
		}
	}

	/**
	 * Tests that a job can be executed after a new leader has been elected. For all except for the
	 * last leader, the job is blocking. The JobManager will be terminated while executing the
	 * blocking job. Once only one JobManager is left, it is checked that a non-blocking can be
	 * successfully executed.
	 */
	@Test
	public void testJobExecutionOnClusterWithLeaderReelection() throws Exception {
		int numJMs = 10;
		int numTMs = 2;
		int numSlotsPerTM = 3;
		int parallelism = numTMs * numSlotsPerTM;

		File rootFolder = tempFolder.getRoot();

		Configuration configuration = ZooKeeperTestUtils.createZooKeeperHAConfig(
			zkServer.getConnectString(),
			rootFolder.getPath());
		configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, UUID.randomUUID().toString());

		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, numJMs);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlotsPerTM);

		// we "effectively" disable the automatic RecoverAllJobs message and sent it manually to make
		// sure that all TMs have registered to the JM prior to issuing the RecoverAllJobs message
		configuration.setString(AkkaOptions.ASK_TIMEOUT, AkkaUtils.INF_TIMEOUT().toString());

		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(true);

		JobVertex sender = new JobVertex("sender");
		JobVertex receiver = new JobVertex("receiver");

		sender.setInvokableClass(Tasks.Sender.class);
		receiver.setInvokableClass(Tasks.BlockingOnceReceiver.class);

		sender.setParallelism(parallelism);
		receiver.setParallelism(parallelism);

		receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE,
			ResultPartitionType.PIPELINED);

		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setSlotSharingGroup(slotSharingGroup);

		final JobGraph graph = new JobGraph("Blocking test job", sender, receiver);

		final TestingCluster cluster = new TestingCluster(configuration);

		ActorSystem clientActorSystem = null;

		Thread thread = null;

		JobSubmitterRunnable jobSubmission = null;

		try {
			cluster.start();

			clientActorSystem = cluster.startJobClientActorSystem(graph.getJobID());

			final ActorSystem clientAS = clientActorSystem;

			jobSubmission = new JobSubmitterRunnable(clientAS, cluster, graph);

			thread = new Thread(jobSubmission);

			thread.start();

			Deadline deadline = timeout.$times(3).fromNow();

			// Kill all JobManager except for two
			for (int i = 0; i < numJMs; i++) {
				ActorGateway jm = cluster.getLeaderGateway(deadline.timeLeft());

				cluster.waitForTaskManagersToBeRegisteredAtJobManager(jm.actor());

				// recover all jobs, sent manually
				log.info("Sent recover all jobs manually to job manager {}.", jm.path());
				jm.tell(JobManagerMessages.getRecoverAllJobs());

				if (i < numJMs - 1) {
					Future<Object> future = jm.ask(new WaitForAllVerticesToBeRunningOrFinished(graph.getJobID()), deadline.timeLeft());

					Await.ready(future, deadline.timeLeft());

					cluster.clearLeader();

					if (i == numJMs - 2) {
						Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(false);
					}

					log.info("Kill job manager {}.", jm.path());

					jm.tell(TestingJobManagerMessages.getDisablePostStop());
					jm.tell(Kill.getInstance());
				}
			}

			log.info("Waiting for submitter thread to terminate.");

			thread.join(deadline.timeLeft().toMillis());

			log.info("Submitter thread has terminated.");

			if (thread.isAlive()) {
				fail("The job submission thread did not stop (meaning it did not succeeded in" +
						"executing the test job.");
			}

			Await.result(jobSubmission.resultPromise.future(), deadline.timeLeft());
		}
		finally {
			if (clientActorSystem != null) {
				cluster.shutdownJobClientActorSystem(clientActorSystem);
			}

			if (thread != null && thread.isAlive()) {
				jobSubmission.finished = true;
			}
			cluster.stop();
		}
	}

	private static class JobSubmitterRunnable implements Runnable {
		private static final Logger LOG = LoggerFactory.getLogger(JobSubmitterRunnable.class);
		boolean finished = false;

		final ActorSystem clientActorSystem;
		final LocalFlinkMiniCluster cluster;
		final JobGraph graph;

		final Promise<JobExecutionResult> resultPromise = new Promise.DefaultPromise<>();

		public JobSubmitterRunnable(
				ActorSystem actorSystem,
				LocalFlinkMiniCluster cluster,
				JobGraph graph) {
			this.clientActorSystem = actorSystem;
			this.cluster = cluster;
			this.graph = graph;
		}

		@Override
		public void run() {
			try {
				JobExecutionResult result = JobClient.submitJobAndWait(
						clientActorSystem,
						cluster.configuration(),
						cluster.highAvailabilityServices(),
						graph,
						timeout,
						false,
						getClass().getClassLoader());

				resultPromise.success(result);
			} catch (Exception e) {
				// This was not expected... fail the test case
				resultPromise.failure(e);
			}
		}
	}
}
