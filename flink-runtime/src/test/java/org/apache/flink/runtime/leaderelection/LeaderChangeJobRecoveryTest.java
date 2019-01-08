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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingManualHighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

public class LeaderChangeJobRecoveryTest extends TestLogger {

	private static FiniteDuration timeout = FiniteDuration.apply(30, TimeUnit.SECONDS);

	private int numTMs = 1;
	private int numSlotsPerTM = 1;
	private int parallelism = numTMs * numSlotsPerTM;

	private JobID jobId;
	private TestingCluster cluster = null;
	private JobGraph job = createBlockingJob(parallelism);
	private TestingManualHighAvailabilityServices highAvailabilityServices;

	@Before
	public void before() throws TimeoutException, InterruptedException {
		jobId = HighAvailabilityServices.DEFAULT_JOB_ID;

		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(true);

		Configuration configuration = new Configuration();

		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 1);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlotsPerTM);

		configuration.setString(ConfigConstants.RESTART_STRATEGY, "fixeddelay");
		configuration.setInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 9999);
		configuration.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "100 milli");

		highAvailabilityServices = new TestingManualHighAvailabilityServices();

		cluster = new TestingCluster(
			configuration,
			highAvailabilityServices,
			true,
			false);
		cluster.start(false);

		// wait for actors to be alive so that they have started their leader retrieval service
		cluster.waitForActorsToBeAlive();
	}

	/**
	 * Tests that the job is not restarted or at least terminates eventually in case that the
	 * JobManager loses its leadership.
	 *
	 * @throws Exception
	 */
	@Test
	public void testNotRestartedWhenLosingLeadership() throws Exception {
		UUID leaderSessionID = UUID.randomUUID();

		highAvailabilityServices.grantLeadership(
			jobId,
			0,
			leaderSessionID);

		highAvailabilityServices.notifyRetrievers(
			jobId,
			0,
			leaderSessionID);

		cluster.waitForTaskManagersToBeRegistered(timeout);

		cluster.submitJobDetached(job);

		ActorGateway jm = cluster.getLeaderGateway(timeout);

		Future<Object> wait = jm.ask(new TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished(job.getJobID()), timeout);

		Await.ready(wait, timeout);

		Future<Object> futureExecutionGraph = jm.ask(new TestingJobManagerMessages.RequestExecutionGraph(job.getJobID()), timeout);

		TestingJobManagerMessages.ResponseExecutionGraph responseExecutionGraph =
			(TestingJobManagerMessages.ResponseExecutionGraph) Await.result(futureExecutionGraph, timeout);

		assertTrue(responseExecutionGraph instanceof TestingJobManagerMessages.ExecutionGraphFound);

		ExecutionGraph executionGraph = (ExecutionGraph) ((TestingJobManagerMessages.ExecutionGraphFound) responseExecutionGraph).executionGraph();

		highAvailabilityServices.revokeLeadership(jobId);

		executionGraph.getTerminationFuture().get(30, TimeUnit.SECONDS);
	}

	public JobGraph createBlockingJob(int parallelism) {
		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(true);

		JobVertex sender = new JobVertex("sender");
		JobVertex receiver = new JobVertex("receiver");

		sender.setInvokableClass(Tasks.Sender.class);
		receiver.setInvokableClass(Tasks.BlockingOnceReceiver.class);

		sender.setParallelism(parallelism);
		receiver.setParallelism(parallelism);

		receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setSlotSharingGroup(slotSharingGroup);

		return new JobGraph("Blocking test job", sender, receiver);
	}
}
