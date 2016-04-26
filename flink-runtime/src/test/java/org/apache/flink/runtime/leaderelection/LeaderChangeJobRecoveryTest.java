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

import akka.actor.ActorRef;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.util.TestLogger;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.Promise;
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

	private Configuration configuration;
	private LeaderElectionRetrievalTestingCluster cluster = null;
	private JobGraph job = createBlockingJob(parallelism);

	@Before
	public void before() throws TimeoutException, InterruptedException {
		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(true);

		configuration = new Configuration();

		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, 1);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTM);

		cluster = new LeaderElectionRetrievalTestingCluster(configuration, true, false, new FixedDelayRestartStrategy(9999, 100));
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

		cluster.grantLeadership(0, leaderSessionID);
		cluster.notifyRetrievalListeners(0, leaderSessionID);

		cluster.waitForTaskManagersToBeRegistered(timeout);

		cluster.submitJobDetached(job);

		ActorGateway jm = cluster.getLeaderGateway(timeout);

		Future<Object> wait = jm.ask(new TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished(job.getJobID()), timeout);

		Await.ready(wait, timeout);

		Future<Object> futureExecutionGraph = jm.ask(new TestingJobManagerMessages.RequestExecutionGraph(job.getJobID()), timeout);

		TestingJobManagerMessages.ResponseExecutionGraph responseExecutionGraph =
			(TestingJobManagerMessages.ResponseExecutionGraph) Await.result(futureExecutionGraph, timeout);

		assertTrue(responseExecutionGraph instanceof TestingJobManagerMessages.ExecutionGraphFound);

		ExecutionGraph executionGraph = ((TestingJobManagerMessages.ExecutionGraphFound) responseExecutionGraph).executionGraph();

		TestActorGateway testActorGateway = new TestActorGateway();

		executionGraph.registerJobStatusListener(testActorGateway);

		cluster.revokeLeadership();

		Future<Boolean> hasReachedTerminalState = testActorGateway.hasReachedTerminalState();

		assertTrue("The job should have reached a terminal state.", Await.result(hasReachedTerminalState, timeout));
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

		ExecutionConfig executionConfig = new ExecutionConfig();

		return new JobGraph("Blocking test job", executionConfig, sender, receiver);
	}

	public static class TestActorGateway implements ActorGateway {

		private static final long serialVersionUID = -736146686160538227L;
		private transient Promise<Boolean> terminalState = new scala.concurrent.impl.Promise.DefaultPromise<>();

		public Future<Boolean> hasReachedTerminalState() {
			return terminalState.future();
		}

		@Override
		public Future<Object> ask(Object message, FiniteDuration timeout) {
			return null;
		}

		@Override
		public void tell(Object message) {
			this.tell(message, new AkkaActorGateway(ActorRef.noSender(), null));
		}

		@Override
		public void tell(Object message, ActorGateway sender) {
			if (message instanceof ExecutionGraphMessages.JobStatusChanged) {
				ExecutionGraphMessages.JobStatusChanged jobStatusChanged = (ExecutionGraphMessages.JobStatusChanged) message;

				if (jobStatusChanged.newJobStatus().isTerminalState()) {
					terminalState.success(true);
				}
			}
		}

		@Override
		public void forward(Object message, ActorGateway sender) {

		}

		@Override
		public Future<Object> retry(Object message, int numberRetries, FiniteDuration timeout, ExecutionContext executionContext) {
			return null;
		}

		@Override
		public String path() {
			return null;
		}

		@Override
		public ActorRef actor() {
			return null;
		}

		@Override
		public UUID leaderSessionID() {
			return null;
		}
	}
}
