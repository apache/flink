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
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.JobManagerHARecoveryTest.BlockingStatefulInvokable;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
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
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobStatus;
import org.apache.flink.runtime.testingUtils.TestingMemoryArchivist;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Status;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import static org.apache.flink.runtime.messages.JobManagerMessages.JobSubmitSuccess;
import static org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.JobStatusIs;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenAtLeastNumTaskManagerAreRegistered;
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
			actorSystem.terminate();
			Await.ready(actorSystem.whenTerminated(), Duration.Inf());
		}
	}
}
