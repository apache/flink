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
import akka.testkit.JavaTestKit;
import com.typesafe.config.Config;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestPartitionState;
import org.apache.flink.runtime.messages.JobManagerMessages.StopJob;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.StoppingSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.SubmitJob;
import org.apache.flink.runtime.messages.TaskMessages.PartitionState;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage.LookupKvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage.NotifyKvStateRegistered;
import org.apache.flink.runtime.query.KvStateMessage.NotifyKvStateUnregistered;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobStatus;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunningOrFinished;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
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
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.messages.JobManagerMessages.JobResultSuccess;
import static org.apache.flink.runtime.messages.JobManagerMessages.JobSubmitSuccess;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.AllVerticesRunning;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.JobStatusIs;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenAtLeastNumTaskManagerAreRegistered;
import static org.apache.flink.runtime.testingUtils.TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT;
import static org.apache.flink.runtime.testingUtils.TestingUtils.startTestingCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
						expectMsgClass(JobSubmitSuccess.class);

						jobManagerGateway.tell(
							new WaitForAllVerticesToBeRunningOrFinished(jid),
							testActorGateway);

						expectMsgClass(AllVerticesRunning.class);

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
						expectMsgClass(JobSubmitSuccess.class);

						jobManagerGateway.tell(new WaitForAllVerticesToBeRunning(jid), testActorGateway);
						expectMsgClass(AllVerticesRunning.class);

						jobManagerGateway.tell(new StopJob(jid), testActorGateway);

						// - The test ----------------------------------------------------------------------
						expectMsgClass(StoppingSuccess.class);

						expectMsgClass(JobResultSuccess.class);
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
							cluster.shutdown();
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
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "100ms");

		UUID leaderSessionId = null;
		ActorGateway jobManager = new AkkaActorGateway(
				JobManager.startJobManagerActors(
						config,
						system,
						TestingJobManager.class,
						MemoryArchivist.class)._1(),
				leaderSessionId);

		LeaderRetrievalService leaderRetrievalService = new StandaloneLeaderRetrievalService(
				AkkaUtils.getAkkaURL(system, jobManager.actor()));

		Configuration tmConfig = new Configuration();
		tmConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 4);
		tmConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);

		ActorRef taskManager = TaskManager.startTaskManagerComponentsAndActor(
				tmConfig,
				ResourceID.generate(),
				system,
				"localhost",
				scala.Option.<String>empty(),
				scala.Option.apply(leaderRetrievalService),
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
		} catch (IllegalStateException ignored) {
			// Expected
		}

		JobGraph jobGraph = new JobGraph("croissant");
		JobVertex jobVertex1 = new JobVertex("cappuccino");
		jobVertex1.setParallelism(4);
		jobVertex1.setInvokableClass(Tasks.BlockingNoOpInvokable.class);

		JobVertex jobVertex2 = new JobVertex("americano");
		jobVertex2.setParallelism(4);
		jobVertex2.setInvokableClass(Tasks.BlockingNoOpInvokable.class);

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
				0,
				"any-name",
				new KvStateID(),
				new KvStateServerAddress(InetAddress.getLocalHost(), 1233));

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
		} catch (IllegalStateException ignored) {
			// Expected
		}

		NotifyKvStateRegistered registerForExistingJob = new NotifyKvStateRegistered(
				jobGraph.getJobID(),
				jobVertex1.getID(),
				0,
				"register-me",
				new KvStateID(),
				new KvStateServerAddress(InetAddress.getLocalHost(), 1293));

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
		assertEquals(jobVertex1.getParallelism(), location.getNumKeyGroups());
		assertEquals(1, location.getNumRegisteredKeyGroups());
		int keyGroupIndex = registerForExistingJob.getKeyGroupIndex();
		assertEquals(registerForExistingJob.getKvStateId(), location.getKvStateID(keyGroupIndex));
		assertEquals(registerForExistingJob.getKvStateServerAddress(), location.getKvStateServerAddress(keyGroupIndex));

		//
		// Unregistration
		//
		NotifyKvStateUnregistered unregister = new NotifyKvStateUnregistered(
				registerForExistingJob.getJobId(),
				registerForExistingJob.getJobVertexId(),
				registerForExistingJob.getKeyGroupIndex(),
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
				0,
				"duplicate-me",
				new KvStateID(),
				new KvStateServerAddress(InetAddress.getLocalHost(), 1293));

		NotifyKvStateRegistered duplicate = new NotifyKvStateRegistered(
				jobGraph.getJobID(),
				jobVertex2.getID(), // <--- different operator, but...
				0,
				"duplicate-me", // ...same name
				new KvStateID(),
				new KvStateServerAddress(InetAddress.getLocalHost(), 1293));

		Future<TestingJobManagerMessages.JobStatusIs> failedFuture = jobManager
				.ask(new NotifyWhenJobStatus(jobGraph.getJobID(), JobStatus.FAILED), deadline.timeLeft())
				.mapTo(ClassTag$.MODULE$.<JobStatusIs>apply(JobStatusIs.class));

		jobManager.tell(register);
		jobManager.tell(duplicate);

		// Wait for failure
		JobStatusIs jobStatus = Await.result(failedFuture, deadline.timeLeft());
		assertEquals(JobStatus.FAILED, jobStatus.state());
	}
}
