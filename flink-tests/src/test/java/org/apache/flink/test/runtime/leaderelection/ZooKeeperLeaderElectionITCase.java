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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.*;

public class ZooKeeperLeaderElectionITCase extends TestLogger {

	private static final FiniteDuration timeout = TestingUtils.TESTING_DURATION();

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

		ForkableFlinkMiniCluster cluster = new ForkableFlinkMiniCluster(configuration);

		try {
			cluster.start();

			for(int i = 0; i < numJMs; i++) {
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
		int numTMs = 3;
		int numSlotsPerTM = 3;
		int parallelism = numTMs * numSlotsPerTM;

		Configuration configuration = new Configuration();

		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, numJMs);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTM);

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

			// Kill all JobManager except for two
			for(int i = 0; i < numJMs - 2; i++) {
				ActorGateway jm = cluster.getLeaderGateway(timeout);

				cluster.waitForTaskManagersToBeRegisteredAtJobManager(jm.actor());

				Future<Object> future = jm.ask(new WaitForAllVerticesToBeRunningOrFinished(graph.getJobID()), timeout);

				Await.ready(future, timeout);

				cluster.clearLeader();

				jm.tell(Kill.getInstance());
			}

			ActorGateway jm = cluster.getLeaderGateway(timeout);

			cluster.waitForTaskManagersToBeRegisteredAtJobManager(jm.actor());

			Future<Object> future = jm.ask(new WaitForAllVerticesToBeRunningOrFinished(graph.getJobID()), timeout);

			Await.ready(future, timeout);

			cluster.clearLeader();

			// set the BlockinOnceReceiver for the execution on the last JM to non-blocking, so
			// that it can succeed
			Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(false);

			jm.tell(PoisonPill.getInstance());

			thread.join(timeout.toMillis());

			if(thread.isAlive()) {
				jobSubmission.finished = true;
				fail("The job submission thread did not stop (meaning it did not succeeded in" +
						"executing the test job.");
			}
		} finally {
			if (clientActorSystem != null) {
				cluster.shutdownJobClientActorSystem(clientActorSystem);
			}

			if(thread != null && thread.isAlive() && jobSubmission != null) {
				jobSubmission.finished = true;
			}
			cluster.stop();
		}
	}

	public static class JobSubmitterRunnable implements Runnable {
		boolean finished = false;

		final ActorSystem clientActorSystem;
		final ForkableFlinkMiniCluster cluster;
		final JobGraph graph;

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
			while(!finished) {
				try {
					LeaderRetrievalService lrService =
							LeaderRetrievalUtils.createLeaderRetrievalService(
									cluster.configuration());

					ActorGateway jobManagerGateway =
							LeaderRetrievalUtils.retrieveLeaderGateway(
									lrService,
									clientActorSystem,
									timeout);

					JobClient.submitJobAndWait(
							clientActorSystem,
							jobManagerGateway,
							graph,
							timeout,
							false,
							getClass().getClassLoader());

					finished = true;
				} catch (JobExecutionException e) {
					// This was expected, so just try again to submit the job
				} catch (LeaderRetrievalException e) {
					// This can also happen, so just try again to submit the job
				} catch (Exception e) {
					// This was not expected... fail the test case
					e.printStackTrace();
					fail("Caught unexpected exception in job submission test case.");
				}
			}
		}
	}
}
