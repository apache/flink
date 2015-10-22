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

import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.PoisonPill;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ZooKeeperLeaderElectionITCase extends TestLogger {

	private static final FiniteDuration timeout = TestingUtils.TESTING_DURATION();

	private static final File tempDirectory;

	static {
		try {
			tempDirectory = org.apache.flink.runtime.testutils
					.CommonTestUtils.createTempDirectory();
		}
		catch (IOException e) {
			throw new RuntimeException("Test setup failed", e);
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (tempDirectory != null) {
			FileUtils.deleteDirectory(tempDirectory);
		}
	}

	/**
	 * Tests that the TaskManagers successfully register at the new leader once the old leader
	 * is terminated.
	 */
	@Test
	public void testTaskManagerRegistrationAtReelectedLeader() throws Exception {
		Configuration configuration = new Configuration();

		int numJMs = 10;
		int numTMs = 3;

		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, numJMs);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
		configuration.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		configuration.setString(ConfigConstants.STATE_BACKEND_FS_RECOVERY_PATH, tempDirectory.getAbsoluteFile().toURI().toString());

		ForkableFlinkMiniCluster cluster = new ForkableFlinkMiniCluster(configuration);

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
		}
		finally {
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

		Configuration configuration = new Configuration();

		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, numJMs);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTM);
		configuration.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		configuration.setString(ConfigConstants.STATE_BACKEND_FS_RECOVERY_PATH, tempDirectory.getAbsoluteFile().toURI().toString());

		// we "effectively" disable the automatic RecoverAllJobs message and sent it manually to make
		// sure that all TMs have registered to the JM prior to issueing the RecoverAllJobs message
		configuration.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, AkkaUtils.INF_TIMEOUT().toString());

		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(true);

		JobVertex sender = new JobVertex("sender");
		JobVertex receiver = new JobVertex("receiver");

		sender.setInvokableClass(Tasks.Sender.class);
		receiver.setInvokableClass(Tasks.BlockingOnceReceiver.class);

		sender.setParallelism(parallelism);
		receiver.setParallelism(parallelism);

		receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE);

		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setSlotSharingGroup(slotSharingGroup);

		final JobGraph graph = new JobGraph("Blocking test job", sender, receiver);

		final ForkableFlinkMiniCluster cluster = new ForkableFlinkMiniCluster(configuration);

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

	public static class JobSubmitterRunnable implements Runnable {
		private static final Logger LOG = LoggerFactory.getLogger(JobSubmitterRunnable.class);
		boolean finished = false;

		final ActorSystem clientActorSystem;
		final ForkableFlinkMiniCluster cluster;
		final JobGraph graph;

		final Promise<JobExecutionResult> resultPromise = new Promise.DefaultPromise<>();

		public JobSubmitterRunnable(
				ActorSystem actorSystem,
				ForkableFlinkMiniCluster cluster,
				JobGraph graph) {
			this.clientActorSystem = actorSystem;
			this.cluster = cluster;
			this.graph = graph;
		}

		@Override
		public void run() {
			try {
				LeaderRetrievalService lrService =
						LeaderRetrievalUtils.createLeaderRetrievalService(
								cluster.configuration());

				JobExecutionResult result = JobClient.submitJobAndWait(
						clientActorSystem,
						lrService,
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
