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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class LeaderChangeStateCleanupTest extends TestLogger {

	private static FiniteDuration timeout = TestingUtils.TESTING_DURATION();

	private int numJMs = 2;
	private int numTMs = 2;
	private int numSlotsPerTM = 2;
	private int parallelism = numTMs * numSlotsPerTM;

	private Configuration configuration;
	private LeaderElectionRetrievalTestingCluster cluster = null;
	private JobGraph job = createBlockingJob(parallelism);

	@Before
	public void before() throws Exception {
		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(true);

		configuration = new Configuration();

		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, numJMs);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTM);

		cluster = new LeaderElectionRetrievalTestingCluster(configuration, true, false);
		cluster.start(false); // TaskManagers don't have to register at the JobManager

		cluster.waitForActorsToBeAlive(); // we only wait until all actors are alive
	}

	@After
	public void after() {
		if(cluster != null) {
			cluster.stop();
		}
	}

	/**
	 * Tests that a job is properly canceled in the case of a leader change. In such an event all
	 * TaskManagers have to disconnect from the previous leader and connect to the newly elected
	 * leader.
	 */
	@Test
	public void testStateCleanupAfterNewLeaderElectionAndListenerNotification() throws Exception {
		UUID leaderSessionID1 = UUID.randomUUID();
		UUID leaderSessionID2 = UUID.randomUUID();

		// first make JM(0) the leader
		cluster.grantLeadership(0, leaderSessionID1);
		// notify all listeners
		cluster.notifyRetrievalListeners(0, leaderSessionID1);

		cluster.waitForTaskManagersToBeRegistered();

		// submit blocking job so that it is not finished when we cancel it
		cluster.submitJobDetached(job);

		ActorGateway jm = cluster.getLeaderGateway(timeout);

		Future<Object> wait = jm.ask(new WaitForAllVerticesToBeRunningOrFinished(job.getJobID()), timeout);

		Await.ready(wait, timeout);

		Future<Object> jobRemoval = jm.ask(new NotifyWhenJobRemoved(job.getJobID()), timeout);

		// make the JM(1) the new leader
		cluster.grantLeadership(1, leaderSessionID2);
		// notify all listeners about the event
		cluster.notifyRetrievalListeners(1, leaderSessionID2);

		Await.ready(jobRemoval, timeout);

		cluster.waitForTaskManagersToBeRegistered();

		ActorGateway jm2 = cluster.getLeaderGateway(timeout);

		Future<Object> futureNumberSlots = jm2.ask(JobManagerMessages.getRequestTotalNumberOfSlots(), timeout);

		// check that all TMs have registered at the new leader
		int numberSlots = (Integer)Await.result(futureNumberSlots, timeout);

		assertEquals(parallelism, numberSlots);

		// try to resubmit now the non-blocking job, it should complete successfully
		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(false);
		cluster.submitJobAndWait(job, false, timeout, new TestingLeaderRetrievalService(jm2.path(), jm2.leaderSessionID()));
	}

	/**
	 * Tests that a job is properly canceled in the case of a leader change. However, this time only the
	 * JMs are notified about the leader change and the TMs still believe the old leader to have
	 * leadership.
	 */
	@Test
	public void testStateCleanupAfterNewLeaderElection() throws Exception {
		UUID leaderSessionID = UUID.randomUUID();
		UUID newLeaderSessionID = UUID.randomUUID();

		cluster.grantLeadership(0, leaderSessionID);
		cluster.notifyRetrievalListeners(0, leaderSessionID);

		cluster.waitForTaskManagersToBeRegistered();

		// submit blocking job so that we can test job clean up
		cluster.submitJobDetached(job);

		ActorGateway jm = cluster.getLeaderGateway(timeout);

		Future<Object> wait = jm.ask(new WaitForAllVerticesToBeRunningOrFinished(job.getJobID()), timeout);

		Await.ready(wait, timeout);

		Future<Object> jobRemoval = jm.ask(new NotifyWhenJobRemoved(job.getJobID()), timeout);

		// only notify the JMs about the new leader JM(1)
		cluster.grantLeadership(1, newLeaderSessionID);

		// job should be removed anyway
		Await.ready(jobRemoval, timeout);
	}

	/**
	 * Tests that a job is properly canceled in the event of a leader change. However, this time
	 * only the TMs are notified about the changing leader. This should be enough to cancel the
	 * currently running job, though.
	 */
	@Test
	public void testStateCleanupAfterListenerNotification() throws Exception {
		UUID leaderSessionID = UUID.randomUUID();
		UUID newLeaderSessionID = UUID.randomUUID();

		cluster.grantLeadership(0, leaderSessionID);
		cluster.notifyRetrievalListeners(0, leaderSessionID);

		cluster.waitForTaskManagersToBeRegistered();

		// submit blocking job
		cluster.submitJobDetached(job);

		ActorGateway jm = cluster.getLeaderGateway(timeout);

		Future<Object> wait = jm.ask(new WaitForAllVerticesToBeRunningOrFinished(job.getJobID()), timeout);

		Await.ready(wait, timeout);

		Future<Object> jobRemoval = jm.ask(new NotifyWhenJobRemoved(job.getJobID()), timeout);

		// notify listeners (TMs) about the leader change
		cluster.notifyRetrievalListeners(1, newLeaderSessionID);

		Await.ready(jobRemoval, timeout);
	}

	/**
	 * Tests that the same JobManager can be reelected as the leader. Even though, the same JM
	 * is elected as the next leader, all currently running jobs should be canceled properly and
	 * all TMs should disconnect from the leader and then reconnect to it.
	 */
	@Test
	public void testReelectionOfSameJobManager() throws Exception {
		UUID leaderSessionID = UUID.randomUUID();
		UUID newLeaderSessionID = UUID.randomUUID();

		FiniteDuration shortTimeout = new FiniteDuration(10, TimeUnit.SECONDS);

		cluster.grantLeadership(0, leaderSessionID);
		cluster.notifyRetrievalListeners(0, leaderSessionID);

		cluster.waitForTaskManagersToBeRegistered();

		// submit blocking job
		cluster.submitJobDetached(job);

		ActorGateway jm = cluster.getLeaderGateway(timeout);

		Future<Object> wait = jm.ask(new WaitForAllVerticesToBeRunningOrFinished(job.getJobID()), timeout);

		Await.ready(wait, timeout);

		Future<Object> jobRemoval = jm.ask(new NotifyWhenJobRemoved(job.getJobID()), timeout);

		// make JM(0) again the leader --> this implies first a leadership revokal
		cluster.grantLeadership(0, newLeaderSessionID);

		Await.ready(jobRemoval, timeout);

		// The TMs should not be able to reconnect since they don't know the current leader
		// session ID
		try {
			cluster.waitForTaskManagersToBeRegistered(shortTimeout);
			fail("TaskManager should not be able to register at JobManager.");
		} catch (TimeoutException e) {
			// expected exception since the TMs have still the old leader session ID
		}

		// notify the TMs about the new (old) leader
		cluster.notifyRetrievalListeners(0, newLeaderSessionID);

		cluster.waitForTaskManagersToBeRegistered();

		ActorGateway leaderGateway = cluster.getLeaderGateway(timeout);

		// try to resubmit now the non-blocking job, it should complete successfully
		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(false);
		cluster.submitJobAndWait(job, false, timeout, new TestingLeaderRetrievalService(leaderGateway.path(), leaderGateway.leaderSessionID()));
	}

	public JobGraph createBlockingJob(int parallelism) {
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

		return new JobGraph("Blocking test job", sender, receiver);
	}
}
