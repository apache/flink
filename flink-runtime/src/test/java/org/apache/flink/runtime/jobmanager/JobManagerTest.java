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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.checkpoint.CheckpointDeclineReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.JobManagerHARecoveryTest.BlockingStatefulInvokable;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationResponse;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestPartitionProducerState;
import org.apache.flink.runtime.messages.JobManagerMessages.StopJob;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.SubmitJob;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepointSuccess;
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage.LookupKvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage.NotifyKvStateRegistered;
import org.apache.flink.runtime.query.KvStateMessage.NotifyKvStateUnregistered;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobStatus;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingMemoryArchivist;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Status;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import com.typesafe.config.Config;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.messages.JobManagerMessages.CancelJobWithSavepoint;
import static org.apache.flink.runtime.messages.JobManagerMessages.JobResultFailure;
import static org.apache.flink.runtime.messages.JobManagerMessages.JobResultSuccess;
import static org.apache.flink.runtime.messages.JobManagerMessages.JobSubmitSuccess;
import static org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.AllVerticesRunning;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.JobStatusIs;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenAtLeastNumTaskManagerAreRegistered;
import static org.apache.flink.runtime.testingUtils.TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT;
import static org.apache.flink.runtime.testingUtils.TestingUtils.TESTING_TIMEOUT;
import static org.apache.flink.runtime.testingUtils.TestingUtils.startTestingCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.mock;

public class JobManagerTest extends TestLogger {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private static ActorSystem system;

	private HighAvailabilityServices highAvailabilityServices;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Before
	public void setupTest() {
		highAvailabilityServices = new EmbeddedHaServices(TestingUtils.defaultExecutor());
	}

	@After
	public void tearDownTest() throws Exception {
		highAvailabilityServices.closeAndCleanupAllData();
		highAvailabilityServices = null;
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
						sender.setInvokableClass(BlockingNoOpInvokable.class); // just block
						sender.createAndAddResultDataSet(rid, PIPELINED);

						final JobGraph jobGraph = new JobGraph("Blocking test job", sender);
						final JobID jid = jobGraph.getJobID();

						final ActorGateway jobManagerGateway = cluster.getLeaderGateway(
							TestingUtils.TESTING_DURATION());

						// we can set the leader session ID to None because we don't use this gateway to send messages
						final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), HighAvailabilityServices.DEFAULT_LEADER_ID);

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

						expectMsgClass(AllVerticesRunning.class);

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

						// 1. All execution states
						RequestPartitionProducerState request = new RequestPartitionProducerState(
							jid, rid, partitionId);

						for (ExecutionState state : ExecutionState.values()) {
							ExecutionGraphTestUtils.setVertexState(vertex, state);

							Future<ExecutionState> futurePartitionState = jobManagerGateway
								.ask(request, getRemainingTime())
								.mapTo(ClassTag$.MODULE$.<ExecutionState>apply(ExecutionState.class));

							ExecutionState resp = Await.result(futurePartitionState, getRemainingTime());
							assertEquals(state, resp);
						}

						// 2. Non-existing execution
						request = new RequestPartitionProducerState(jid, rid, new ResultPartitionID());

						Future<?> futurePartitionState = jobManagerGateway.ask(request, getRemainingTime());
						try {
							Await.result(futurePartitionState, getRemainingTime());
							fail("Did not fail with expected RuntimeException");
						} catch (RuntimeException e) {
							assertEquals(IllegalArgumentException.class, e.getCause().getClass());
						}

						// 3. Non-existing job
						request = new RequestPartitionProducerState(new JobID(), rid, new ResultPartitionID());
						futurePartitionState = jobManagerGateway.ask(request, getRemainingTime());

						try {
							Await.result(futurePartitionState, getRemainingTime());
							fail("Did not fail with expected IllegalArgumentException");
						} catch (IllegalArgumentException ignored) {
						}
					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (cluster != null) {
							cluster.stop();
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
						sender.setInvokableClass(NoOpInvokable.class); // just finish
						sender.createAndAddResultDataSet(rid, PIPELINED);

						final JobVertex sender2 = new JobVertex("Blocking Sender");
						sender2.setParallelism(1);
						sender2.setInvokableClass(BlockingNoOpInvokable.class); // just block
						sender2.createAndAddResultDataSet(new IntermediateDataSetID(), PIPELINED);

						final JobGraph jobGraph = new JobGraph("Fast finishing producer test job", sender, sender2);
						final JobID jid = jobGraph.getJobID();

						final ActorGateway jobManagerGateway = cluster.getLeaderGateway(
							TestingUtils.TESTING_DURATION());

						// we can set the leader session ID to None because we don't use this gateway to send messages
						final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), HighAvailabilityServices.DEFAULT_LEADER_ID);

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

						expectMsgClass(AllVerticesRunning.class);

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

						// Producer finished, request state
						Object request = new RequestPartitionProducerState(jid, rid, partitionId);

						Future<ExecutionState> producerStateFuture = jobManagerGateway
							.ask(request, getRemainingTime())
							.mapTo(ClassTag$.MODULE$.<ExecutionState>apply(ExecutionState.class));

						assertEquals(ExecutionState.FINISHED, Await.result(producerStateFuture, getRemainingTime()));
					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (cluster != null) {
							cluster.stop();
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
						sender.setInvokableClass(NoOpInvokable.class); // just finish
						sender.createAndAddResultDataSet(rid, PIPELINED);

						final JobVertex sender2 = new JobVertex("Blocking Sender");
						sender2.setParallelism(1);
						sender2.setInvokableClass(BlockingNoOpInvokable.class); // just block
						sender2.createAndAddResultDataSet(new IntermediateDataSetID(), PIPELINED);

						final JobGraph jobGraph = new JobGraph("Fast finishing producer test job", sender, sender2);
						final JobID jid = jobGraph.getJobID();

						final ActorGateway jobManagerGateway = cluster.getLeaderGateway(
							TestingUtils.TESTING_DURATION());

						// we can set the leader session ID to None because we don't use this gateway to send messages
						final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), HighAvailabilityServices.DEFAULT_LEADER_ID);

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
						vertex.resetForNewExecution(System.currentTimeMillis(), 1L);

						// Producer finished, request state
						Object request = new RequestPartitionProducerState(jid, rid, partitionId);

						Future<?> producerStateFuture = jobManagerGateway.ask(request, getRemainingTime());

						try {
							Await.result(producerStateFuture, getRemainingTime());
							fail("Did not fail with expected Exception");
						} catch (PartitionProducerDisposedException ignored) {
						}
					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (cluster != null) {
							cluster.stop();
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
						final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), HighAvailabilityServices.DEFAULT_LEADER_ID);

						// Submit the job and wait for all vertices to be running
						jobManagerGateway.tell(
							new SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT),
							testActorGateway);
						expectMsgClass(JobSubmitSuccess.class);

						jobManagerGateway.tell(new WaitForAllVerticesToBeRunning(jid), testActorGateway);
						expectMsgClass(AllVerticesRunning.class);

						jobManagerGateway.tell(new StopJob(jid), testActorGateway);

						// - The test ----------------------------------------------------------------------
						expectMsgClass(StoppingSuccess.class);

						expectMsgClass(JobResultSuccess.class);
					} finally {
						if (cluster != null) {
							cluster.stop();
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
						sender.setInvokableClass(BlockingNoOpInvokable.class); // just block

						final JobGraph jobGraph = new JobGraph("Non-Stoppable batching test job", sender);
						final JobID jid = jobGraph.getJobID();

						final ActorGateway jobManagerGateway = cluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

						// we can set the leader session ID to None because we don't use this gateway to send messages
						final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(), HighAvailabilityServices.DEFAULT_LEADER_ID);

						// Submit the job and wait for all vertices to be running
						jobManagerGateway.tell(
							new SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT),
							testActorGateway);
						expectMsgClass(JobSubmitSuccess.class);

						jobManagerGateway.tell(new WaitForAllVerticesToBeRunning(jid), testActorGateway);
						expectMsgClass(AllVerticesRunning.class);

						jobManagerGateway.tell(new StopJob(jid), testActorGateway);

						// - The test ----------------------------------------------------------------------
						expectMsgClass(StoppingFailure.class);

						jobManagerGateway.tell(new RequestExecutionGraph(jid), testActorGateway);

						expectMsgClass(ExecutionGraphFound.class);
					} finally {
						if (cluster != null) {
							cluster.stop();
						}
					}
				}
			};
		}};
	}

	/**
	 * Tests that the JobManager handles {@link org.apache.flink.runtime.query.KvStateMessage}
	 * instances as expected.
	 */
	@Test
	public void testKvStateMessages() throws Exception {
		Deadline deadline = new FiniteDuration(100, TimeUnit.SECONDS).fromNow();

		Configuration config = new Configuration();
		config.setString(AkkaOptions.ASK_TIMEOUT, "100ms");

		ActorRef jobManagerActor = JobManager.startJobManagerActors(
			config,
			system,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			highAvailabilityServices,
			NoOpMetricRegistry.INSTANCE,
			Option.empty(),
			TestingJobManager.class,
			MemoryArchivist.class)._1();

		UUID leaderId = LeaderRetrievalUtils.retrieveLeaderSessionId(
			highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
			TestingUtils.TESTING_TIMEOUT());

		ActorGateway jobManager = new AkkaActorGateway(
				jobManagerActor,
				leaderId);

		Configuration tmConfig = new Configuration();
		tmConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
		tmConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 8);

		ActorRef taskManager = TaskManager.startTaskManagerComponentsAndActor(
			tmConfig,
			ResourceID.generate(),
			system,
			highAvailabilityServices,
			NoOpMetricRegistry.INSTANCE,
			"localhost",
			scala.Option.<String>empty(),
			true,
			TestingTaskManager.class);

		Future<Object> registrationFuture = jobManager
				.ask(new NotifyWhenAtLeastNumTaskManagerAreRegistered(1), deadline.timeLeft());

		Await.ready(registrationFuture, deadline.timeLeft());

		//
		// Location lookup
		//
		LookupKvStateLocation lookupNonExistingJob = new LookupKvStateLocation(
				new JobID(),
				"any-name");

		Future<KvStateLocation> lookupFuture = jobManager
				.ask(lookupNonExistingJob, deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class));

		try {
			Await.result(lookupFuture, deadline.timeLeft());
			fail("Did not throw expected Exception");
		} catch (FlinkJobNotFoundException ignored) {
			// Expected
		}

		JobGraph jobGraph = new JobGraph("croissant");
		JobVertex jobVertex1 = new JobVertex("cappuccino");
		jobVertex1.setParallelism(4);
		jobVertex1.setMaxParallelism(16);
		jobVertex1.setInvokableClass(BlockingNoOpInvokable.class);

		JobVertex jobVertex2 = new JobVertex("americano");
		jobVertex2.setParallelism(4);
		jobVertex2.setMaxParallelism(16);
		jobVertex2.setInvokableClass(BlockingNoOpInvokable.class);

		jobGraph.addVertex(jobVertex1);
		jobGraph.addVertex(jobVertex2);

		Future<JobSubmitSuccess> submitFuture = jobManager
				.ask(new SubmitJob(jobGraph, ListeningBehaviour.DETACHED), deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<JobSubmitSuccess>apply(JobSubmitSuccess.class));

		Await.result(submitFuture, deadline.timeLeft());

		Object lookupUnknownRegistrationName = new LookupKvStateLocation(
				jobGraph.getJobID(),
				"unknown");

		lookupFuture = jobManager
				.ask(lookupUnknownRegistrationName, deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class));

		try {
			Await.result(lookupFuture, deadline.timeLeft());
			fail("Did not throw expected Exception");
		} catch (UnknownKvStateLocation ignored) {
			// Expected
		}

		//
		// Registration
		//
		NotifyKvStateRegistered registerNonExistingJob = new NotifyKvStateRegistered(
				new JobID(),
				new JobVertexID(),
				new KeyGroupRange(0, 0),
				"any-name",
				new KvStateID(),
				new InetSocketAddress(InetAddress.getLocalHost(), 1233));

		jobManager.tell(registerNonExistingJob);

		LookupKvStateLocation lookupAfterRegistration = new LookupKvStateLocation(
				registerNonExistingJob.getJobId(),
				registerNonExistingJob.getRegistrationName());

		lookupFuture = jobManager
				.ask(lookupAfterRegistration, deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class));

		try {
			Await.result(lookupFuture, deadline.timeLeft());
			fail("Did not throw expected Exception");
		} catch (FlinkJobNotFoundException ignored) {
			// Expected
		}

		NotifyKvStateRegistered registerForExistingJob = new NotifyKvStateRegistered(
				jobGraph.getJobID(),
				jobVertex1.getID(),
				new KeyGroupRange(0, 0),
				"register-me",
				new KvStateID(),
				new InetSocketAddress(InetAddress.getLocalHost(), 1293));

		jobManager.tell(registerForExistingJob);

		lookupAfterRegistration = new LookupKvStateLocation(
				registerForExistingJob.getJobId(),
				registerForExistingJob.getRegistrationName());

		lookupFuture = jobManager
				.ask(lookupAfterRegistration, deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class));

		KvStateLocation location = Await.result(lookupFuture, deadline.timeLeft());
		assertNotNull(location);

		assertEquals(jobGraph.getJobID(), location.getJobId());
		assertEquals(jobVertex1.getID(), location.getJobVertexId());
		assertEquals(jobVertex1.getMaxParallelism(), location.getNumKeyGroups());
		assertEquals(1, location.getNumRegisteredKeyGroups());
		KeyGroupRange keyGroupRange = registerForExistingJob.getKeyGroupRange();
		assertEquals(1, keyGroupRange.getNumberOfKeyGroups());
		assertEquals(registerForExistingJob.getKvStateId(), location.getKvStateID(keyGroupRange.getStartKeyGroup()));
		assertEquals(registerForExistingJob.getKvStateServerAddress(), location.getKvStateServerAddress(keyGroupRange.getStartKeyGroup()));

		//
		// Unregistration
		//
		NotifyKvStateUnregistered unregister = new NotifyKvStateUnregistered(
				registerForExistingJob.getJobId(),
				registerForExistingJob.getJobVertexId(),
				registerForExistingJob.getKeyGroupRange(),
				registerForExistingJob.getRegistrationName());

		jobManager.tell(unregister);

		lookupFuture = jobManager
				.ask(lookupAfterRegistration, deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class));

		try {
			Await.result(lookupFuture, deadline.timeLeft());
			fail("Did not throw expected Exception");
		} catch (UnknownKvStateLocation ignored) {
			// Expected
		}

		//
		// Duplicate registration fails task
		//
		NotifyKvStateRegistered register = new NotifyKvStateRegistered(
				jobGraph.getJobID(),
				jobVertex1.getID(),
				new KeyGroupRange(0, 0),
				"duplicate-me",
				new KvStateID(),
				new InetSocketAddress(InetAddress.getLocalHost(), 1293));

		NotifyKvStateRegistered duplicate = new NotifyKvStateRegistered(
				jobGraph.getJobID(),
				jobVertex2.getID(), // <--- different operator, but...
				new KeyGroupRange(0, 0),
				"duplicate-me", // ...same name
				new KvStateID(),
				new InetSocketAddress(InetAddress.getLocalHost(), 1293));

		Future<TestingJobManagerMessages.JobStatusIs> failedFuture = jobManager
				.ask(new NotifyWhenJobStatus(jobGraph.getJobID(), JobStatus.FAILED), deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<JobStatusIs>apply(JobStatusIs.class));

		jobManager.tell(register);
		jobManager.tell(duplicate);

		// Wait for failure
		JobStatusIs jobStatus = Await.result(failedFuture, deadline.timeLeft());
		assertEquals(JobStatus.FAILED, jobStatus.state());

	}

	@Test
	public void testCancelWithSavepoint() throws Exception {
		File defaultSavepointDir = tmpFolder.newFolder();

		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);
		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, defaultSavepointDir.toURI().toString());

		ActorSystem actorSystem = null;
		ActorGateway jobManager = null;
		ActorGateway archiver = null;
		ActorGateway taskManager = null;
		try {
			actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

			Tuple2<ActorRef, ActorRef> master = JobManager.startJobManagerActors(
				config,
				actorSystem,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				Option.empty(),
				Option.apply("jm"),
				Option.apply("arch"),
				TestingJobManager.class,
				TestingMemoryArchivist.class);

			UUID leaderId = LeaderRetrievalUtils.retrieveLeaderSessionId(
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				TestingUtils.TESTING_TIMEOUT());

			jobManager = new AkkaActorGateway(master._1(), leaderId);
			archiver = new AkkaActorGateway(master._2(), leaderId);

			ActorRef taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
				config,
				ResourceID.generate(),
				actorSystem,
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				"localhost",
				Option.apply("tm"),
				true,
				TestingTaskManager.class);

			taskManager = new AkkaActorGateway(taskManagerRef, leaderId);

			// Wait until connected
			Object msg = new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager.actor());
			Await.ready(taskManager.ask(msg, timeout), timeout);

			// Create job graph
			JobVertex sourceVertex = new JobVertex("Source");
			sourceVertex.setInvokableClass(BlockingStatefulInvokable.class);
			sourceVertex.setParallelism(1);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceVertex);

			JobCheckpointingSettings snapshottingSettings = new JobCheckpointingSettings(
				Collections.singletonList(sourceVertex.getID()),
				Collections.singletonList(sourceVertex.getID()),
				Collections.singletonList(sourceVertex.getID()),
				new CheckpointCoordinatorConfiguration(
					3600000,
					3600000,
					0,
					Integer.MAX_VALUE,
					CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
					true),
					null);

			jobGraph.setSnapshotSettings(snapshottingSettings);

			// Submit job graph
			msg = new SubmitJob(jobGraph, ListeningBehaviour.DETACHED);
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Wait for all tasks to be running
			msg = new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobGraph.getJobID());
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Notify when cancelled
			msg = new NotifyWhenJobStatus(jobGraph.getJobID(), JobStatus.CANCELED);
			Future<Object> cancelled = jobManager.ask(msg, timeout);

			// Cancel with savepoint
			String savepointPath = null;

			for (int i = 0; i < 10; i++) {
				msg = new CancelJobWithSavepoint(jobGraph.getJobID(), null);
				CancellationResponse cancelResp = (CancellationResponse) Await.result(jobManager.ask(msg, timeout), timeout);

				if (cancelResp instanceof CancellationFailure) {
					CancellationFailure failure = (CancellationFailure) cancelResp;
					if (failure.cause().getMessage().contains(CheckpointDeclineReason.NOT_ALL_REQUIRED_TASKS_RUNNING.message())) {
						Thread.sleep(10); // wait and retry
					} else {
						failure.cause().printStackTrace();
						fail("Failed to cancel job: " + failure.cause().getMessage());
					}
				} else {
					savepointPath = ((CancellationSuccess) cancelResp).savepointPath();
					break;
				}
			}

			// Verify savepoint path
			assertNotNull("Savepoint not triggered", savepointPath);

			// Wait for job status change
			Await.ready(cancelled, timeout);

			File savepointFile = new File(new Path(savepointPath).getPath());
			assertTrue(savepointFile.exists());
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

			if (actorSystem != null) {
				actorSystem.awaitTermination(TESTING_TIMEOUT());
			}
		}
	}

	/**
	 * Tests that a failed savepoint does not cancel the job and new checkpoints are triggered
	 * after the failed cancel-with-savepoint.
	 */
	@Test
	public void testCancelJobWithSavepointFailurePeriodicCheckpoints() throws Exception {
		File savepointTarget = tmpFolder.newFolder();

		// A source that declines savepoints, simulating the behaviour of a
		// failed savepoint.
		JobVertex sourceVertex = new JobVertex("Source");
		sourceVertex.setInvokableClass(FailOnSavepointSourceTask.class);
		sourceVertex.setParallelism(1);
		JobGraph jobGraph = new JobGraph("TestingJob", sourceVertex);

		CheckpointCoordinatorConfiguration coordConfig = new CheckpointCoordinatorConfiguration(
			50,
			3600000,
			0,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true);

		JobCheckpointingSettings snapshottingSettings = new JobCheckpointingSettings(
			Collections.singletonList(sourceVertex.getID()),
			Collections.singletonList(sourceVertex.getID()),
			Collections.singletonList(sourceVertex.getID()),
			coordConfig,
			null);

		jobGraph.setSnapshotSettings(snapshottingSettings);

		final TestingCluster testingCluster = new TestingCluster(
			new Configuration(),
			highAvailabilityServices,
			true,
			false);

		try {
			testingCluster.start(true);

			FiniteDuration askTimeout = new FiniteDuration(30, TimeUnit.SECONDS);
			ActorGateway jobManager = testingCluster.getLeaderGateway(askTimeout);

			testingCluster.submitJobDetached(jobGraph);

			// Wait for the source to be running otherwise the savepoint
			// barrier will not reach the task.
			Future<Object> allTasksAlive = jobManager.ask(
				new WaitForAllVerticesToBeRunning(jobGraph.getJobID()),
				askTimeout);
			Await.ready(allTasksAlive, askTimeout);

			// Cancel with savepoint. The expected outcome is that cancellation
			// fails due to a failed savepoint. After this, periodic checkpoints
			// should resume.
			Future<Object> cancellationFuture = jobManager.ask(
				new CancelJobWithSavepoint(jobGraph.getJobID(), savepointTarget.getAbsolutePath()),
				askTimeout);
			Object cancellationResponse = Await.result(cancellationFuture, askTimeout);

			if (cancellationResponse instanceof CancellationFailure) {
				if (!FailOnSavepointSourceTask.CHECKPOINT_AFTER_SAVEPOINT_LATCH.await(30, TimeUnit.SECONDS)) {
					fail("No checkpoint was triggered after failed savepoint within expected duration");
				}
			} else {
				fail("Unexpected cancellation response from JobManager: " + cancellationResponse);
			}
		} finally {
			testingCluster.stop();
		}
	}

	/**
	 * Tests that a meaningful exception is returned if no savepoint directory is
	 * configured.
	 */
	@Test
	public void testCancelWithSavepointNoDirectoriesConfigured() throws Exception {
		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);
		Configuration config = new Configuration();

		ActorSystem actorSystem = null;
		ActorGateway jobManager = null;
		ActorGateway archiver = null;
		ActorGateway taskManager = null;
		try {
			actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

			Tuple2<ActorRef, ActorRef> master = JobManager.startJobManagerActors(
				config,
				actorSystem,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				Option.empty(),
				Option.apply("jm"),
				Option.apply("arch"),
				TestingJobManager.class,
				TestingMemoryArchivist.class);

			UUID leaderId = LeaderRetrievalUtils.retrieveLeaderSessionId(
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				TestingUtils.TESTING_TIMEOUT());

			jobManager = new AkkaActorGateway(master._1(), leaderId);
			archiver = new AkkaActorGateway(master._2(), leaderId);

			ActorRef taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
				config,
				ResourceID.generate(),
				actorSystem,
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				"localhost",
				Option.apply("tm"),
				true,
				TestingTaskManager.class);

			taskManager = new AkkaActorGateway(taskManagerRef, leaderId);

			// Wait until connected
			Object msg = new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager.actor());
			Await.ready(taskManager.ask(msg, timeout), timeout);

			// Create job graph
			JobVertex sourceVertex = new JobVertex("Source");
			sourceVertex.setInvokableClass(BlockingStatefulInvokable.class);
			sourceVertex.setParallelism(1);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceVertex);

			JobCheckpointingSettings snapshottingSettings = new JobCheckpointingSettings(
				Collections.singletonList(sourceVertex.getID()),
				Collections.singletonList(sourceVertex.getID()),
				Collections.singletonList(sourceVertex.getID()),
				new CheckpointCoordinatorConfiguration(
					3600000,
					3600000,
					0,
					Integer.MAX_VALUE,
					CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
					true),
				null);

			jobGraph.setSnapshotSettings(snapshottingSettings);

			// Submit job graph
			msg = new SubmitJob(jobGraph, ListeningBehaviour.DETACHED);
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Wait for all tasks to be running
			msg = new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobGraph.getJobID());
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Cancel with savepoint
			msg = new CancelJobWithSavepoint(jobGraph.getJobID(), null);
			CancellationResponse cancelResp = (CancellationResponse) Await.result(jobManager.ask(msg, timeout), timeout);

			if (cancelResp instanceof CancellationFailure) {
				CancellationFailure failure = (CancellationFailure) cancelResp;
				assertTrue(failure.cause() instanceof IllegalStateException);
				assertTrue(failure.cause().getMessage().contains("savepoint directory"));
			} else {
				fail("Unexpected cancellation response from JobManager: " + cancelResp);
			}
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
	 * Tests that we can trigger a savepoint when periodic checkpoints are disabled.
	 */
	@Test
	public void testSavepointWithDeactivatedPeriodicCheckpointing() throws Exception {
		File defaultSavepointDir = tmpFolder.newFolder();

		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);
		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, defaultSavepointDir.toURI().toString());

		ActorSystem actorSystem = null;
		ActorGateway jobManager = null;
		ActorGateway archiver = null;
		ActorGateway taskManager = null;
		try {
			actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

			Tuple2<ActorRef, ActorRef> master = JobManager.startJobManagerActors(
				config,
				actorSystem,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				Option.empty(),
				Option.apply("jm"),
				Option.apply("arch"),
				TestingJobManager.class,
				TestingMemoryArchivist.class);

			UUID leaderId = LeaderRetrievalUtils.retrieveLeaderSessionId(
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				TestingUtils.TESTING_TIMEOUT());

			jobManager = new AkkaActorGateway(master._1(), leaderId);
			archiver = new AkkaActorGateway(master._2(), leaderId);

			ActorRef taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
				config,
				ResourceID.generate(),
				actorSystem,
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				"localhost",
				Option.apply("tm"),
				true,
				TestingTaskManager.class);

			taskManager = new AkkaActorGateway(taskManagerRef, leaderId);

			// Wait until connected
			Object msg = new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager.actor());
			Await.ready(taskManager.ask(msg, timeout), timeout);

			// Create job graph
			JobVertex sourceVertex = new JobVertex("Source");
			sourceVertex.setInvokableClass(BlockingStatefulInvokable.class);
			sourceVertex.setParallelism(1);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceVertex);

			JobCheckpointingSettings snapshottingSettings = new JobCheckpointingSettings(
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					new CheckpointCoordinatorConfiguration(
						Long.MAX_VALUE, // deactivated checkpointing
						360000,
						0,
						Integer.MAX_VALUE,
						CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
						true),
					null);

			jobGraph.setSnapshotSettings(snapshottingSettings);

			// Submit job graph
			msg = new SubmitJob(jobGraph, ListeningBehaviour.DETACHED);
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Wait for all tasks to be running
			msg = new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobGraph.getJobID());
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Cancel with savepoint
			File targetDirectory = tmpFolder.newFolder();

			msg = new TriggerSavepoint(jobGraph.getJobID(), Option.apply(targetDirectory.getAbsolutePath()));
			Future<Object> future = jobManager.ask(msg, timeout);
			Object result = Await.result(future, timeout);

			assertTrue("Did not trigger savepoint", result instanceof TriggerSavepointSuccess);
			assertEquals(1, targetDirectory.listFiles().length);
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

			if (actorSystem != null) {
				actorSystem.awaitTermination(TestingUtils.TESTING_TIMEOUT());
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
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				Option.empty(),
				Option.apply("jm"),
				Option.apply("arch"),
				TestingJobManager.class,
				TestingMemoryArchivist.class);

			UUID leaderId = LeaderRetrievalUtils.retrieveLeaderSessionId(
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				TestingUtils.TESTING_TIMEOUT());

			jobManager = new AkkaActorGateway(master._1(), leaderId);
			archiver = new AkkaActorGateway(master._2(), leaderId);

			Configuration tmConfig = new Configuration();
			tmConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 4);

			ActorRef taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
				tmConfig,
				ResourceID.generate(),
				actorSystem,
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				"localhost",
				Option.apply("tm"),
				true,
				TestingTaskManager.class);

			taskManager = new AkkaActorGateway(taskManagerRef, leaderId);

			// Wait until connected
			Object msg = new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager.actor());
			Await.ready(taskManager.ask(msg, timeout), timeout);

			// Create job graph
			JobVertex sourceVertex = new JobVertex("Source");
			sourceVertex.setInvokableClass(BlockingStatefulInvokable.class);
			sourceVertex.setParallelism(1);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceVertex);

			JobCheckpointingSettings snapshottingSettings = new JobCheckpointingSettings(
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					Collections.singletonList(sourceVertex.getID()),
					new CheckpointCoordinatorConfiguration(
						Long.MAX_VALUE, // deactivated checkpointing
						360000,
						0,
						Integer.MAX_VALUE,
						CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
						true),
					null);

			jobGraph.setSnapshotSettings(snapshottingSettings);

			// Submit job graph
			msg = new SubmitJob(jobGraph, ListeningBehaviour.DETACHED);
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Wait for all tasks to be running
			msg = new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobGraph.getJobID());
			Await.result(jobManager.ask(msg, timeout), timeout);

			// Trigger savepoint
			File targetDirectory = tmpFolder.newFolder();
			msg = new TriggerSavepoint(jobGraph.getJobID(), Option.apply(targetDirectory.getAbsolutePath()));
			Future<Object> future = jobManager.ask(msg, timeout);
			Object result = Await.result(future, timeout);

			String savepointPath = ((TriggerSavepointSuccess) result).savepointPath();

			// Cancel because of restarts
			msg = new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID());
			Future<?> removedFuture = jobManager.ask(msg, timeout);

			Future<?> cancelFuture = jobManager.ask(new CancelJob(jobGraph.getJobID()), timeout);
			Object response = Await.result(cancelFuture, timeout);
			assertTrue("Unexpected response: " + response, response instanceof CancellationSuccess);

			Await.ready(removedFuture, timeout);

			// Adjust the job (we need a new operator ID)
			JobVertex newSourceVertex = new JobVertex("NewSource");
			newSourceVertex.setInvokableClass(BlockingStatefulInvokable.class);
			newSourceVertex.setParallelism(1);

			JobGraph newJobGraph = new JobGraph("NewTestingJob", newSourceVertex);

			JobCheckpointingSettings newSnapshottingSettings = new JobCheckpointingSettings(
					Collections.singletonList(newSourceVertex.getID()),
					Collections.singletonList(newSourceVertex.getID()),
					Collections.singletonList(newSourceVertex.getID()),
					new CheckpointCoordinatorConfiguration(
						Long.MAX_VALUE, // deactivated checkpointing
						360000,
						0,
						Integer.MAX_VALUE,
						CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
						true),
					null);

			newJobGraph.setSnapshotSettings(newSnapshottingSettings);

			SavepointRestoreSettings restoreSettings = SavepointRestoreSettings.forPath(savepointPath, false);
			newJobGraph.setSavepointRestoreSettings(restoreSettings);

			msg = new SubmitJob(newJobGraph, ListeningBehaviour.DETACHED);
			response = Await.result(jobManager.ask(msg, timeout), timeout);

			assertTrue("Unexpected response: " + response, response instanceof JobResultFailure);

			JobResultFailure failure = (JobResultFailure) response;
			Throwable cause = failure.cause().deserializeError(ClassLoader.getSystemClassLoader());

			assertTrue(cause instanceof IllegalStateException);
			assertTrue(cause.getMessage().contains("allowNonRestoredState"));

			// Wait until removed
			msg = new TestingJobManagerMessages.NotifyWhenJobRemoved(newJobGraph.getJobID());
			Await.ready(jobManager.ask(msg, timeout), timeout);

			// Resubmit, but allow non restored state now
			restoreSettings = SavepointRestoreSettings.forPath(savepointPath, true);
			newJobGraph.setSavepointRestoreSettings(restoreSettings);

			msg = new SubmitJob(newJobGraph, ListeningBehaviour.DETACHED);
			response = Await.result(jobManager.ask(msg, timeout), timeout);

			assertTrue("Unexpected response: " + response, response instanceof JobSubmitSuccess);
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

			if (actorSystem != null) {
				actorSystem.awaitTermination(TestingUtils.TESTING_TIMEOUT());
			}
		}
	}

	/**
	 * This tests makes sure that triggering a reconnection from the ResourceManager will stop after a new
	 * ResourceManager has connected. Furthermore it makes sure that there is not endless loop of reconnection
	 * commands (see FLINK-6341).
	 */
	@Test
	public void testResourceManagerConnection() throws TimeoutException, InterruptedException {
		FiniteDuration testTimeout = new FiniteDuration(30L, TimeUnit.SECONDS);
		final long reconnectionInterval = 200L;

		final Configuration configuration = new Configuration();
		configuration.setLong(JobManagerOptions.RESOURCE_MANAGER_RECONNECT_INTERVAL, reconnectionInterval);

		final ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(configuration);

		try {
			final ActorGateway jmGateway = TestingUtils.createJobManager(
				actorSystem,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				configuration,
				highAvailabilityServices);

			final TestProbe probe = TestProbe.apply(actorSystem);
			final AkkaActorGateway rmGateway = new AkkaActorGateway(probe.ref(), HighAvailabilityServices.DEFAULT_LEADER_ID);

			// wait for the JobManager to become the leader
			Future<?> leaderFuture = jmGateway.ask(TestingJobManagerMessages.getNotifyWhenLeader(), testTimeout);
			Await.ready(leaderFuture, testTimeout);

			jmGateway.tell(new RegisterResourceManager(probe.ref()), rmGateway);

			LeaderSessionMessage leaderSessionMessage = probe.expectMsgClass(LeaderSessionMessage.class);

			assertEquals(jmGateway.leaderSessionID(), leaderSessionMessage.leaderSessionID());
			assertTrue(leaderSessionMessage.message() instanceof RegisterResourceManagerSuccessful);

			jmGateway.tell(
				new RegistrationMessages.RegisterTaskManager(
					ResourceID.generate(),
					mock(TaskManagerLocation.class),
					new HardwareDescription(1, 1L, 1L, 1L),
					1));
			leaderSessionMessage = probe.expectMsgClass(LeaderSessionMessage.class);

			assertTrue(leaderSessionMessage.message() instanceof NotifyResourceStarted);

			// fail the NotifyResourceStarted so that we trigger the reconnection process on the JobManager's side
			probe.lastSender().tell(new Status.Failure(new Exception("Test exception")), ActorRef.noSender());

			Deadline reconnectionDeadline = new FiniteDuration(5L * reconnectionInterval, TimeUnit.MILLISECONDS).fromNow();
			boolean registered = false;

			while (reconnectionDeadline.hasTimeLeft()) {
				try {
					leaderSessionMessage = probe.expectMsgClass(reconnectionDeadline.timeLeft(), LeaderSessionMessage.class);
				} catch (AssertionError ignored) {
					// expected timeout after the reconnectionDeadline has been exceeded
					continue;
				}

				if (leaderSessionMessage.message() instanceof TriggerRegistrationAtJobManager) {
					if (registered) {
						fail("A successful registration should not be followed by another TriggerRegistrationAtJobManager message.");
					}

					jmGateway.tell(new RegisterResourceManager(probe.ref()), rmGateway);
				} else if (leaderSessionMessage.message() instanceof RegisterResourceManagerSuccessful) {
					// now we should no longer receive TriggerRegistrationAtJobManager messages
					registered = true;
				} else {
					fail("Received unknown message: " + leaderSessionMessage.message() + '.');
				}
			}

			assertTrue(registered);

		} finally {
			// cleanup the actor system and with it all of the started actors if not already terminated
			actorSystem.shutdown();
			actorSystem.awaitTermination();
		}
	}

	/**
	 * A blocking stateful source task that declines savepoints.
	 */
	public static class FailOnSavepointSourceTask extends AbstractInvokable {

		private static final CountDownLatch CHECKPOINT_AFTER_SAVEPOINT_LATCH = new CountDownLatch(1);

		private boolean receivedSavepoint;

		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public FailOnSavepointSourceTask(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			new CountDownLatch(1).await();
		}

		@Override
		public boolean triggerCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions) throws Exception {
			if (checkpointOptions.getCheckpointType() == CheckpointType.SAVEPOINT) {
				receivedSavepoint = true;
				return false;
			} else if (receivedSavepoint) {
				CHECKPOINT_AFTER_SAVEPOINT_LATCH.countDown();
				return true;
			}
			return true;
		}

		@Override
		public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {
			throw new UnsupportedOperationException("This is meant to be used as a source");
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
		}
	}
}
