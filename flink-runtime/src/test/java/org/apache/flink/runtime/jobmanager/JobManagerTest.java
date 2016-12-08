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
import akka.testkit.JavaTestKit;
import com.typesafe.config.Config;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStoreFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestPartitionProducerState;
import org.apache.flink.runtime.messages.JobManagerMessages.StopJob;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.SubmitJob;
import org.apache.flink.runtime.messages.TaskMessages.PartitionProducerState;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingMemoryArchivist;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.testingUtils.TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT;
import static org.apache.flink.runtime.testingUtils.TestingUtils.startTestingCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobManagerTest extends TestLogger {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

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
			new Within(duration("15 seconds")) {
				@Override
				protected void run() {
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
						expectMsgClass(JobManagerMessages.JobSubmitSuccess.class);

						jobManagerGateway.tell(
							new WaitForAllVerticesToBeRunningOrFinished(jid),
							testActorGateway);

						expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

						// This is the mock execution ID of the task requesting the state of the partition
						final ExecutionAttemptID receiver = new ExecutionAttemptID();

						// Request the execution graph to get the runtime info
						jobManagerGateway.tell(new RequestExecutionGraph(jid), testActorGateway);

						final ExecutionGraph eg = (ExecutionGraph) expectMsgClass(ExecutionGraphFound.class)
							.executionGraph();

						final ExecutionVertex vertex = eg.getJobVertex(sender.getID())
							.getTaskVertices()[0];

						final IntermediateResultPartition partition = vertex.getProducedPartitions()
							.values().iterator().next();

						final ResultPartitionID partitionId = new ResultPartitionID(
							partition.getPartitionId(),
							vertex.getCurrentExecutionAttempt().getAttemptId());

						// - The test ----------------------------------------------------------------------

						ExecutionAttemptID receiverId = new ExecutionAttemptID();

						// 1. All execution states
						RequestPartitionProducerState request = new RequestPartitionProducerState(
							jid, receiverId, rid, partitionId);

						for (ExecutionState state : ExecutionState.values()) {
							ExecutionGraphTestUtils.setVertexState(vertex, state);

							Future<?> futurePartitionState = jobManagerGateway
								.ask(request, getRemainingTime());

							LeaderSessionMessage wrappedMsg = (LeaderSessionMessage) Await.result(futurePartitionState, getRemainingTime());
							PartitionProducerState resp = (PartitionProducerState) (PartitionProducerState) wrappedMsg.message();
							assertEquals(receiverId, resp.receiverExecutionId());
							assertTrue("Responded with failure: " + resp, resp.result().isLeft());
							assertEquals(state, resp.result().left().get()._3());
						}

						// 2. Non-existing execution
						request = new RequestPartitionProducerState(jid, receiverId, rid, new ResultPartitionID());

						Future<?> futurePartitionState = jobManagerGateway.ask(request, getRemainingTime());
						LeaderSessionMessage wrappedMsg = (LeaderSessionMessage) Await.result(futurePartitionState, getRemainingTime());
						PartitionProducerState resp = (PartitionProducerState) wrappedMsg.message();
						assertEquals(receiverId, resp.receiverExecutionId());
						assertTrue("Responded with success: " + resp, resp.result().isRight());
						assertTrue(resp.result().right().get() instanceof RuntimeException);
						assertTrue(resp.result().right().get().getCause() instanceof IllegalArgumentException);

						// 3. Non-existing job
						request = new RequestPartitionProducerState(new JobID(), receiverId, rid, new ResultPartitionID());
						futurePartitionState = jobManagerGateway.ask(request, getRemainingTime());
						wrappedMsg = (LeaderSessionMessage) Await.result(futurePartitionState, getRemainingTime());
						resp = (PartitionProducerState) wrappedMsg.message();
						assertEquals(receiverId, resp.receiverExecutionId());
						assertTrue("Responded with success: " + resp, resp.result().isRight());
						assertTrue(resp.result().right().get() instanceof IllegalArgumentException);
					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (cluster != null) {
							cluster.shutdown();
						}
					}
				}
			};
		}};
	}

	/**
	 * Tests the JobManager response when the execution is not registered with
	 * the ExecutionGraph.
	 */
	@Test
	public void testRequestPartitionStateUnregisteredExecution() throws Exception {
		new JavaTestKit(system) {{
			new Within(duration("15 seconds")) {
				@Override
				protected void run() {
					// Setup
					TestingCluster cluster = null;

					try {
						cluster = startTestingCluster(4, 1, DEFAULT_AKKA_ASK_TIMEOUT());

						final IntermediateDataSetID rid = new IntermediateDataSetID();

						// Create a task
						final JobVertex sender = new JobVertex("Sender");
						sender.setParallelism(1);
						sender.setInvokableClass(Tasks.NoOpInvokable.class); // just finish
						sender.createAndAddResultDataSet(rid, PIPELINED);

						final JobVertex sender2 = new JobVertex("Blocking Sender");
						sender2.setParallelism(1);
						sender2.setInvokableClass(Tasks.BlockingNoOpInvokable.class); // just block
						sender2.createAndAddResultDataSet(new IntermediateDataSetID(), PIPELINED);

						final JobGraph jobGraph = new JobGraph("Fast finishing producer test job", sender, sender2);
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
						expectMsgClass(JobManagerMessages.JobSubmitSuccess.class);

						jobManagerGateway.tell(
							new WaitForAllVerticesToBeRunningOrFinished(jid),
							testActorGateway);

						expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

						Future<Object> egFuture = jobManagerGateway.ask(
							new RequestExecutionGraph(jobGraph.getJobID()), remaining());

						ExecutionGraphFound egFound = (ExecutionGraphFound) Await.result(egFuture, remaining());
						ExecutionGraph eg = egFound.executionGraph();

						ExecutionVertex vertex = eg.getJobVertex(sender.getID()).getTaskVertices()[0];
						while (vertex.getExecutionState() != ExecutionState.FINISHED) {
							Thread.sleep(1);
						}

						IntermediateResultPartition partition = vertex.getProducedPartitions()
							.values().iterator().next();

						ResultPartitionID partitionId = new ResultPartitionID(
							partition.getPartitionId(),
							vertex.getCurrentExecutionAttempt().getAttemptId());

						// Producer finished, request state
						ExecutionAttemptID receiverId = new ExecutionAttemptID();

						Future<?> producerStateFuture = jobManagerGateway.ask(
							new RequestPartitionProducerState(jid, receiverId, rid, partitionId), getRemainingTime());

						LeaderSessionMessage wrappedMsg = (LeaderSessionMessage) Await.result(producerStateFuture, getRemainingTime());
						PartitionProducerState resp = (PartitionProducerState) wrappedMsg.message();
						assertEquals(receiverId, resp.receiverExecutionId());
						assertTrue("Responded with failure: " + resp, resp.result().isLeft());
						assertEquals(ExecutionState.FINISHED, resp.result().left().get()._3());
					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (cluster != null) {
							cluster.shutdown();
						}
					}
				}
			};
		}};
	}

	/**
	 * Tests the JobManager response when the execution is not registered with
	 * the ExecutionGraph anymore and a new execution attempt is available.
	 */
	@Test
	public void testRequestPartitionStateMoreRecentExecutionAttempt() throws Exception {
		new JavaTestKit(system) {{
			new Within(duration("15 seconds")) {
				@Override
				protected void run() {
					// Setup
					TestingCluster cluster = null;

					try {
						cluster = startTestingCluster(4, 1, DEFAULT_AKKA_ASK_TIMEOUT());

						final IntermediateDataSetID rid = new IntermediateDataSetID();

						// Create a task
						final JobVertex sender = new JobVertex("Sender");
						sender.setParallelism(1);
						sender.setInvokableClass(Tasks.NoOpInvokable.class); // just finish
						sender.createAndAddResultDataSet(rid, PIPELINED);

						final JobVertex sender2 = new JobVertex("Blocking Sender");
						sender2.setParallelism(1);
						sender2.setInvokableClass(Tasks.BlockingNoOpInvokable.class); // just block
						sender2.createAndAddResultDataSet(new IntermediateDataSetID(), PIPELINED);

						final JobGraph jobGraph = new JobGraph("Fast finishing producer test job", sender, sender2);
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
						expectMsgClass(JobManagerMessages.JobSubmitSuccess.class);

						jobManagerGateway.tell(
							new WaitForAllVerticesToBeRunningOrFinished(jid),
							testActorGateway);

						expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

						Future<Object> egFuture = jobManagerGateway.ask(
							new RequestExecutionGraph(jobGraph.getJobID()), remaining());

						ExecutionGraphFound egFound = (ExecutionGraphFound) Await.result(egFuture, remaining());
						ExecutionGraph eg = (ExecutionGraph) egFound.executionGraph();

						ExecutionVertex vertex = eg.getJobVertex(sender.getID()).getTaskVertices()[0];
						while (vertex.getExecutionState() != ExecutionState.FINISHED) {
							Thread.sleep(1);
						}

						IntermediateResultPartition partition = vertex.getProducedPartitions()
							.values().iterator().next();

						ResultPartitionID partitionId = new ResultPartitionID(
							partition.getPartitionId(),
							vertex.getCurrentExecutionAttempt().getAttemptId());

						// Reset execution => new execution attempt
						vertex.resetForNewExecution();

						// Producer finished, request state
						ExecutionAttemptID receiverId = new ExecutionAttemptID();

						Object request = new RequestPartitionProducerState(jid, receiverId, rid, partitionId);

						Future<?> producerStateFuture = jobManagerGateway.ask(request, getRemainingTime());

						LeaderSessionMessage wrappedMsg = (LeaderSessionMessage) Await.result(producerStateFuture, getRemainingTime());
						PartitionProducerState resp = (PartitionProducerState) wrappedMsg.message();
						assertEquals(receiverId, resp.receiverExecutionId());
						assertTrue("Responded with success: " + resp, resp.result().isRight());
						assertTrue(resp.result().right().get() instanceof PartitionProducerDisposedException);
					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (cluster != null) {
							cluster.shutdown();
						}
					}
				}
			};
		}};
	}

	@Test
	public void testStopSignal() throws Exception {
		new JavaTestKit(system) {{
			new Within(duration("15 seconds")) {
				@Override
				protected void run() {
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
						expectMsgClass(JobManagerMessages.JobSubmitSuccess.class);

						jobManagerGateway.tell(new WaitForAllVerticesToBeRunning(jid), testActorGateway);
						expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

						jobManagerGateway.tell(new StopJob(jid), testActorGateway);

						// - The test ----------------------------------------------------------------------
						expectMsgClass(StoppingSuccess.class);

						expectMsgClass(JobManagerMessages.JobResultSuccess.class);
					} finally {
						if (cluster != null) {
							cluster.shutdown();
						}
					}
				}
			};
		}};
	}

	@Test
	public void testStopSignalFail() throws Exception {
		new JavaTestKit(system) {{
			new Within(duration("15 seconds")) {
				@Override
				protected void run() {
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
						expectMsgClass(JobManagerMessages.JobSubmitSuccess.class);

						jobManagerGateway.tell(new WaitForAllVerticesToBeRunning(jid), testActorGateway);
						expectMsgClass(TestingJobManagerMessages.AllVerticesRunning.class);

						jobManagerGateway.tell(new StopJob(jid), testActorGateway);

						// - The test ----------------------------------------------------------------------
						expectMsgClass(StoppingFailure.class);

						jobManagerGateway.tell(new RequestExecutionGraph(jid), testActorGateway);

						expectMsgClass(ExecutionGraphFound.class);
					} finally {
						if (cluster != null) {
							cluster.shutdown();
						}
					}
				}
			};
		}};
	}

	/**
	 * Tests that we can trigger a savepoint when periodic checkpointing is disabled.
	 */
	@Test
	public void testSavepointWithDeactivatedPeriodicCheckpointing() throws Exception {
		File defaultSavepointDir = tmpFolder.newFolder();

		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);

		Configuration config = new Configuration();
		config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
		config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, defaultSavepointDir.getAbsolutePath());

		ActorSystem actorSystem = null;
		ActorGateway jobManager = null;
		ActorGateway archiver = null;
		ActorGateway taskManager = null;
		try {
			actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

			Tuple2<ActorRef, ActorRef> master = JobManager.startJobManagerActors(
				config,
				actorSystem,
				actorSystem.dispatcher(),
				actorSystem.dispatcher(),
				Option.apply("jm"),
				Option.apply("arch"),
				TestingJobManager.class,
				TestingMemoryArchivist.class);

			jobManager = new AkkaActorGateway(master._1(), null);
			archiver = new AkkaActorGateway(master._2(), null);

			ActorRef taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
					new Configuration(),
					ResourceID.generate(),
					actorSystem,
					"localhost",
					Option.apply("tm"),
					Option.<LeaderRetrievalService>apply(new StandaloneLeaderRetrievalService(jobManager.path())),
					true,
					TestingTaskManager.class);

			taskManager = new AkkaActorGateway(taskManagerRef, null);

			// Wait until connected
			Object msg = new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager.actor());
			Await.ready(taskManager.ask(msg, timeout), timeout);

			// Create job graph
			JobVertex sourceVertex = new JobVertex("Source");
			sourceVertex.setInvokableClass(JobManagerHARecoveryTest.BlockingStatefulInvokable.class);
			sourceVertex.setParallelism(1);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceVertex);

			JobSnapshottingSettings snapshottingSettings = new JobSnapshottingSettings(
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					Long.MAX_VALUE, // deactivated checkpointing
					360000,
					0,
					Integer.MAX_VALUE);

			jobGraph.setSnapshotSettings(snapshottingSettings);

			// Submit job graph
			msg = new JobManagerMessages.SubmitJob(jobGraph, ListeningBehaviour.DETACHED);
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Wait for all tasks to be running
			msg = new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobGraph.getJobID());
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Trigger savepoint
			msg = new JobManagerMessages.TriggerSavepoint(jobGraph.getJobID());
			Future<Object> future = jobManager.ask(msg, timeout);
			Object result = Await.result(future, timeout);

			assertTrue("Did not trigger savepoint", result instanceof JobManagerMessages.TriggerSavepointSuccess);
			assertEquals(1, defaultSavepointDir.listFiles().length);
		} finally {
			if (actorSystem != null) {
				actorSystem.shutdown();
			}

			if (archiver != null) {
				archiver.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (jobManager != null) {
				jobManager.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (taskManager != null) {
				taskManager.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}
	}

	/**
	 * Tests that configured {@link SavepointRestoreSettings} are respected.
	 */
	@Test
	public void testSavepointRestoreSettings() throws Exception {
		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);

		ActorSystem actorSystem = null;
		ActorGateway jobManager = null;
		ActorGateway archiver = null;
		ActorGateway taskManager = null;
		try {
			actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

			Tuple2<ActorRef, ActorRef> master = JobManager.startJobManagerActors(
				new Configuration(),
				actorSystem,
				actorSystem.dispatcher(),
				actorSystem.dispatcher(),
				Option.apply("jm"),
				Option.apply("arch"),
				TestingJobManager.class,
				TestingMemoryArchivist.class);

			jobManager = new AkkaActorGateway(master._1(), null);
			archiver = new AkkaActorGateway(master._2(), null);

			Configuration tmConfig = new Configuration();
			tmConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 4);

			ActorRef taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
					tmConfig,
					ResourceID.generate(),
					actorSystem,
					"localhost",
					Option.apply("tm"),
					Option.<LeaderRetrievalService>apply(new StandaloneLeaderRetrievalService(jobManager.path())),
					true,
					TestingTaskManager.class);

			taskManager = new AkkaActorGateway(taskManagerRef, null);

			// Wait until connected
			Object msg = new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager.actor());
			Await.ready(taskManager.ask(msg, timeout), timeout);

			// Create job graph
			JobVertex sourceVertex = new JobVertex("Source");
			sourceVertex.setInvokableClass(JobManagerHARecoveryTest.BlockingStatefulInvokable.class);
			sourceVertex.setParallelism(1);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceVertex);

			JobSnapshottingSettings snapshottingSettings = new JobSnapshottingSettings(
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					Long.MAX_VALUE, // deactivated checkpointing
					360000,
					0,
					Integer.MAX_VALUE);

			jobGraph.setSnapshotSettings(snapshottingSettings);

			// Submit job graph
			msg = new JobManagerMessages.SubmitJob(jobGraph, ListeningBehaviour.DETACHED);
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Wait for all tasks to be running
			msg = new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobGraph.getJobID());
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Trigger savepoint
			msg = new JobManagerMessages.TriggerSavepoint(jobGraph.getJobID());
			Future<Object> future = jobManager.ask(msg, timeout);
			Object result = Await.result(future, timeout);

			String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess) result).savepointPath();

			// Cancel because of restarts
			msg = new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID());
			Future<?> removedFuture = jobManager.ask(msg, timeout);

			Future<?> cancelFuture = jobManager.ask(new JobManagerMessages.CancelJob(jobGraph.getJobID()), timeout);
			Object response = Await.result(cancelFuture, timeout);
			assertTrue("Unexpected response: " + response, response instanceof JobManagerMessages.CancellationSuccess);

			Await.ready(removedFuture, timeout);

			// Adjust the job (we need a new operator ID)
			JobVertex newSourceVertex = new JobVertex("Source");
			newSourceVertex.setInvokableClass(JobManagerHARecoveryTest.BlockingStatefulInvokable.class);
			newSourceVertex.setParallelism(1);

			JobGraph newJobGraph = new JobGraph("TestingJob", newSourceVertex);

			JobSnapshottingSettings newSnapshottingSettings = new JobSnapshottingSettings(
					Collections.singletonList(newSourceVertex.getID()),
					Collections.singletonList(newSourceVertex.getID()),
					Collections.singletonList(newSourceVertex.getID()),
					Long.MAX_VALUE, // deactivated checkpointing
					360000,
					0,
					Integer.MAX_VALUE);

			newJobGraph.setSnapshotSettings(newSnapshottingSettings);

			SavepointRestoreSettings restoreSettings = SavepointRestoreSettings.forPath(savepointPath, false);
			newJobGraph.setSavepointRestoreSettings(restoreSettings);

			msg = new JobManagerMessages.SubmitJob(newJobGraph, ListeningBehaviour.DETACHED);
			response = Await.result(jobManager.ask(msg, timeout), timeout);

			assertTrue("Unexpected response: " + response, response instanceof JobManagerMessages.JobResultFailure);

			JobManagerMessages.JobResultFailure failure = (JobManagerMessages.JobResultFailure) response;
			Throwable cause = failure.cause().deserializeError(ClassLoader.getSystemClassLoader());

			assertTrue(cause instanceof IllegalStateException);
			assertTrue(cause.getMessage().contains("allowNonRestoredState"));

			// Wait until removed
			msg = new TestingJobManagerMessages.NotifyWhenJobRemoved(newJobGraph.getJobID());

			Await.ready(jobManager.ask(msg, timeout), timeout);

			// Resubmit, but allow non restored state now
			restoreSettings = SavepointRestoreSettings.forPath(savepointPath, true);
			newJobGraph.setSavepointRestoreSettings(restoreSettings);

			msg = new JobManagerMessages.SubmitJob(newJobGraph, ListeningBehaviour.DETACHED);
			response = Await.result(jobManager.ask(msg, timeout), timeout);

			assertTrue("Unexpected response: " + response, response instanceof JobManagerMessages.JobSubmitSuccess);
		} finally {
			if (actorSystem != null) {
				actorSystem.shutdown();
			}

			if (archiver != null) {
				archiver.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (jobManager != null) {
				jobManager.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}

			if (taskManager != null) {
				taskManager.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}
	}

}
