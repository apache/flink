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

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;

import com.typesafe.config.Config;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FixedDelayRestartStrategyConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.JobSubmitSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.JobManagerMessages.StopJob;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.SubmitJob;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestPartitionState;
import org.apache.flink.runtime.messages.TaskMessages.PartitionState;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;

import java.net.InetAddress;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.testingUtils.TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT;
import static org.apache.flink.runtime.testingUtils.TestingUtils.startTestingCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobManagerTest {

	private static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testNullHostnameGoesToLocalhost() {
		try {
			Tuple2<String, Object> address = new Tuple2<String, Object>(null, 1772);
			Config cfg = AkkaUtils.getAkkaConfig(new Configuration(),
					new Some<Tuple2<String, Object>>(address));

			String hostname = cfg.getString("akka.remote.netty.tcp.hostname");
			assertTrue(InetAddress.getByName(hostname).isLoopbackAddress());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests responses to partition state requests.
	 */
	@Test
	public void testRequestPartitionState() throws Exception {
		new JavaTestKit(system) {{
			// Setup
			TestingCluster cluster = null;

			try {
				cluster = startTestingCluster(2, 1, DEFAULT_AKKA_ASK_TIMEOUT());

				final IntermediateDataSetID rid = new IntermediateDataSetID();

				// Create a task
				final JobVertex sender = new JobVertex("Sender");
				sender.setParallelism(1);
				sender.setInvokableClass(Tasks.BlockingNoOpInvokable.class); // just block
				sender.createAndAddResultDataSet(rid, PIPELINED);

				final JobGraph jobGraph = new JobGraph("Blocking test job", sender);
				final JobID jid = jobGraph.getJobID();

				final ActorGateway jobManagerGateway = cluster.getLeaderGateway(
						TestingUtils.TESTING_DURATION());

				// we can set the leader session ID to None because we don't use this gateway to send messages
				final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), null);

				// Submit the job and wait for all vertices to be running
				jobManagerGateway.tell(
						new SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT),
						testActorGateway);
				expectMsgClass(JobSubmitSuccess.class);

				jobManagerGateway.tell(
						new WaitForAllVerticesToBeRunningOrFinished(jid),
						testActorGateway);

				expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

				// This is the mock execution ID of the task requesting the state of the partition
				final ExecutionAttemptID receiver = new ExecutionAttemptID();

				// Request the execution graph to get the runtime info
				jobManagerGateway.tell(new RequestExecutionGraph(jid), testActorGateway);

				final ExecutionGraph eg = expectMsgClass(ExecutionGraphFound.class)
						.executionGraph();

				final ExecutionVertex vertex = eg.getJobVertex(sender.getID())
						.getTaskVertices()[0];

				final IntermediateResultPartition partition = vertex.getProducedPartitions()
						.values().iterator().next();

				final ResultPartitionID partitionId = new ResultPartitionID(
						partition.getPartitionId(),
						vertex.getCurrentExecutionAttempt().getAttemptId());

				// - The test ----------------------------------------------------------------------

				// 1. All execution states
				RequestPartitionState request = new RequestPartitionState(
						jid, partitionId, receiver, rid);

				for (ExecutionState state : ExecutionState.values()) {
					ExecutionGraphTestUtils.setVertexState(vertex, state);

					jobManagerGateway.tell(request, testActorGateway);

					LeaderSessionMessage lsm = expectMsgClass(LeaderSessionMessage.class);

					assertEquals(PartitionState.class, lsm.message().getClass());

					PartitionState resp = (PartitionState) lsm.message();

					assertEquals(request.taskExecutionId(), resp.taskExecutionId());
					assertEquals(request.taskResultId(), resp.taskResultId());
					assertEquals(request.partitionId().getPartitionId(), resp.partitionId());
					assertEquals(state, resp.state());
				}

				// 2. Non-existing execution
				request = new RequestPartitionState(jid, new ResultPartitionID(), receiver, rid);

				jobManagerGateway.tell(request, testActorGateway);

				LeaderSessionMessage lsm = expectMsgClass(LeaderSessionMessage.class);

				assertEquals(PartitionState.class, lsm.message().getClass());

				PartitionState resp = (PartitionState) lsm.message();

				assertEquals(request.taskExecutionId(), resp.taskExecutionId());
				assertEquals(request.taskResultId(), resp.taskResultId());
				assertEquals(request.partitionId().getPartitionId(), resp.partitionId());
				assertNull(resp.state());

				// 3. Non-existing job
				request = new RequestPartitionState(
						new JobID(), new ResultPartitionID(), receiver, rid);

				jobManagerGateway.tell(request, testActorGateway);

				lsm = expectMsgClass(LeaderSessionMessage.class);

				assertEquals(PartitionState.class, lsm.message().getClass());

				resp = (PartitionState) lsm.message();

				assertEquals(request.taskExecutionId(), resp.taskExecutionId());
				assertEquals(request.taskResultId(), resp.taskResultId());
				assertEquals(request.partitionId().getPartitionId(), resp.partitionId());
				assertNull(resp.state());
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			finally {
				if (cluster != null) {
					cluster.shutdown();
				}
			}
		}};
	}

	@Test
	public void testStopSignal() throws Exception {
		new JavaTestKit(system) {{
			// Setup
			TestingCluster cluster = null;

			try {
				cluster = startTestingCluster(2, 1, DEFAULT_AKKA_ASK_TIMEOUT());

				// Create a task
				final JobVertex sender = new JobVertex("Sender");
				sender.setParallelism(2);
				sender.setInvokableClass(StoppableInvokable.class);

				final JobGraph jobGraph = new JobGraph("Stoppable streaming test job", sender);
				final JobID jid = jobGraph.getJobID();

				final ActorGateway jobManagerGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

				// we can set the leader session ID to None because we don't use this gateway to send messages
				final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), null);

				// Submit the job and wait for all vertices to be running
				jobManagerGateway.tell(
						new SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT),
						testActorGateway);
				expectMsgClass(JobSubmitSuccess.class);

				jobManagerGateway.tell(new WaitForAllVerticesToBeRunning(jid), testActorGateway);
				expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

				jobManagerGateway.tell(new StopJob(jid), testActorGateway);

				// - The test ----------------------------------------------------------------------
				expectMsgClass(StoppingSuccess.class);

				expectMsgClass(JobManagerMessages.JobResultSuccess.class);
			}
			finally {
				if (cluster != null) {
					cluster.shutdown();
				}
			}
		}};
	}

	@Test
	public void testStopSignalFail() throws Exception {
		new JavaTestKit(system) {{
			// Setup
			TestingCluster cluster = null;

			try {
				cluster = startTestingCluster(2, 1, DEFAULT_AKKA_ASK_TIMEOUT());

				// Create a task
				final JobVertex sender = new JobVertex("Sender");
				sender.setParallelism(1);
				sender.setInvokableClass(Tasks.BlockingNoOpInvokable.class); // just block

				final JobGraph jobGraph = new JobGraph("Non-Stoppable batching test job", sender);
				final JobID jid = jobGraph.getJobID();

				final ActorGateway jobManagerGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

				// we can set the leader session ID to None because we don't use this gateway to send messages
				final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), null);

				// Submit the job and wait for all vertices to be running
				jobManagerGateway.tell(
						new SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT),
						testActorGateway);
				expectMsgClass(JobSubmitSuccess.class);

				jobManagerGateway.tell(new WaitForAllVerticesToBeRunning(jid), testActorGateway);
				expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

				jobManagerGateway.tell(new StopJob(jid), testActorGateway);

				// - The test ----------------------------------------------------------------------
				expectMsgClass(StoppingFailure.class);

				jobManagerGateway.tell(new RequestExecutionGraph(jid), testActorGateway);

				expectMsgClass(ExecutionGraphFound.class);
			}
			finally {
				if (cluster != null) {
					cluster.shutdown();
				}
			}
		}};
	}

	/**
	 * Tests that execution graphs end up in a terminal state after calling
	 * cancel and clear everything -- even when a restart strategy is
	 * configured.
	 */
	@Test
	public void testCancelAndClearEverything() throws Exception {
		TestingCluster cluster = startTestingCluster(1, 1, DEFAULT_AKKA_ASK_TIMEOUT());

		try {
			Deadline deadline = TestingUtils.TESTING_DURATION().fromNow();

			JobVertex vertex = new JobVertex("Blocking Test Vertex");
			vertex.setParallelism(1);
			vertex.setInvokableClass(Tasks.BlockingNoOpInvokable.class);

			JobGraph jobGraph = new JobGraph();
			jobGraph.addVertex(vertex);

			// Very long restart delay in order to catch the job in restarting
			// state if erroneous
			jobGraph.setRestartStrategyConfiguration(RestartStrategies.fixedDelayRestart(
					Integer.MAX_VALUE, Integer.MAX_VALUE));

			ActorGateway jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			Future<?> submitFuture = jobManager.ask(new SubmitJob(
					jobGraph, ListeningBehaviour.EXECUTION_RESULT), deadline.timeLeft());
			Object resp = Await.result(submitFuture, deadline.timeLeft());
			assertTrue("Job submission failed", resp instanceof JobSubmitSuccess);

			Future<?> graphFuture = jobManager.ask(new RequestExecutionGraph(
					jobGraph.getJobID()), deadline.timeLeft());
			resp = Await.result(graphFuture, deadline.timeLeft());
			assertTrue("ExecutionGraph request failed", resp instanceof ExecutionGraphFound);

			ExecutionGraph graph = ((ExecutionGraphFound) resp).executionGraph();

			// Kill the job manager => call cancelAndClearEverything
			Patterns.gracefulStop(jobManager.actor(), deadline.timeLeft());

			// Wait for terminal state
			boolean isTerminalState = false;
			JobStatus state = null;

			while (!isTerminalState && deadline.hasTimeLeft()) {
				state = graph.getState();
				isTerminalState = state.isTerminalState();
				Thread.sleep(100);
			}

			assertTrue("Not in terminal state (" + state + ")", isTerminalState);
		} finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}
}
